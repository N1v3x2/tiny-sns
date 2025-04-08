#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include "file_utils.h"
#include "synchronizer_utils.h"

#include <algorithm>
#include <bits/fs_fwd.h>
#include <condition_variable>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg)                                                     \
    LOG(severity) << msg;                                                      \
    google::FlushLogFiles(google::severity);

using namespace std::chrono_literals;
using namespace std::this_thread;
using namespace std::filesystem;

using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using grpc::ClientContext;
using std::condition_variable;
using std::endl;
using std::ifstream, std::ofstream;
using std::mutex;
using std::stoi;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::vector;

int synchID;
int clusterID;
std::atomic_int serverID; // Server corresponding to the synchronzier
std::atomic_bool isMaster = false;
bool registered = false;
mutex registeredMtx;
condition_variable registeredCV;
string coordAddr;
unique_ptr<CoordService::Stub> coord_stub;
path dirPrefix;

vector<string> getAllUsers();
vector<string> getTimeline(int userID);
vector<string> getFollowers(int userID);
void Heartbeat(ServerInfo info);

class Synchronizer {
  protected:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    string hostname;
    int port;

  public:
    Synchronizer(const string& host, int p)
        : channel(1), hostname(host), port(p) {
        setupRabbitMQ();
        declareExchange("all_users", "fanout");
        declareExchange("relations", "fanout");
        declareExchange("timelines", "fanout");
    }

    ~Synchronizer() {
        die_on_amqp_error(amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS),
                          "Closing channel");
        die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                          "Closing connection");
        die_on_error(amqp_destroy_connection(conn), "Detroying connection");
    }

  private:
    void setupRabbitMQ() {
        conn = amqp_new_connection();
        amqp_socket_t* socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            die("Creating TCP socket");
        }

        int status = amqp_socket_open(socket, hostname.c_str(), port);
        if (status) {
            die("Opening TCP socket");
        }

        die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0,
                                     AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
                          "Logging in");

        amqp_channel_open(conn, channel);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
    }

    void declareExchange(const string& exchangeName, const string& type) {
        amqp_exchange_declare(
            conn, channel, amqp_cstring_bytes(exchangeName.c_str()),
            amqp_cstring_bytes(type.c_str()), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
    }
};

class SynchronizerProducer : public Synchronizer {
  public:
    SynchronizerProducer(const string& host, int p) : Synchronizer(host, p) {}

    void publishUserList() {
        vector<string> users = getAllUsers();
        sort(users.begin(), users.end());

        Json::Value userList;
        for (auto user : users) {
            userList["users"].append(user);
        }

        Json::FastWriter writer;
        string message = writer.write(userList);
        log(INFO, "Publishing user list");

        publishMessage("all_users", "", message);
    }

    void publishUserRelations() {
        Json::Value relations;
        vector<string> users = getAllUsers();

        for (const auto& user : users) {
            int userID = stoi(user);
            vector<string> followers = getFollowers(userID);

            Json::Value followerList(Json::arrayValue);
            for (const auto& follower : followers) {
                followerList.append(follower);
            }

            if (!followerList.empty()) {
                relations[user] = followerList;
            }
        }

        Json::FastWriter writer;
        string message = writer.write(relations);
        log(INFO, "Publishing user relations");

        publishMessage("relations", "", message);
    }

    void publishTimelines() {
        Json::Value timelineJson;
        Json::FastWriter writer;
        for (const auto& user : getAllUsers()) {
            int userID = stoi(user);
            for (const auto& line : getTimeline(userID)) {
                timelineJson[user].append(line);
            }
        }
        log(INFO, "Publishing timelines");
        string message = writer.write(timelineJson);
        publishMessage("timelines", "", message);
    }

  private:
    void publishMessage(const string& exchangeName, const string& routingKey,
                        const string& message) {
        int status = amqp_basic_publish(
            conn, channel, amqp_cstring_bytes(exchangeName.c_str()),
            !routingKey.empty() ? amqp_cstring_bytes(routingKey.c_str())
                                : amqp_empty_bytes,
            0, 0, NULL, amqp_cstring_bytes(message.c_str()));
        die_on_error(status, "Publish Message");
    }
};

class SynchronizerConsumer : public Synchronizer {
  private:
    vector<amqp_bytes_t> queues;

  public:
    SynchronizerConsumer(const string& host, int p) : Synchronizer(host, p) {
        queues.push_back(declareAndBindQueue("all_users", ""));
        queues.push_back(declareAndBindQueue("relations", ""));
        queues.push_back(declareAndBindQueue("timelines", ""));
    }

    void consumeMessages(int timeout_ms = 1000) {
        for (auto& queue : queues) {
            amqp_basic_consume(conn, channel, queue, amqp_empty_bytes, 0, 1, 0,
                               amqp_empty_table);
            die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
        }

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        while (true) {
            amqp_envelope_t envelope;
            amqp_maybe_release_buffers(conn);

            amqp_rpc_reply_t res =
                amqp_consume_message(conn, &envelope, &timeout, 0);

            switch (res.reply_type) {
            case AMQP_RESPONSE_NORMAL: {
                string message(static_cast<char*>(envelope.message.body.bytes),
                               envelope.message.body.len);
                string exchange(static_cast<char*>(envelope.exchange.bytes),
                                envelope.exchange.len);
                if (exchange == "all_users") {
                    consumeUserList(message);
                } else if (exchange == "relations") {
                    consumeUserRelations(message);
                } else if (exchange == "timelines") {
                    consumeTimelines(message);
                } else {
                    log(WARNING,
                        "Received message intended for unknown exchange");
                }
                amqp_destroy_envelope(&envelope);
                break;
            }
            case AMQP_RESPONSE_NONE:
            case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                continue;
            case AMQP_RESPONSE_SERVER_EXCEPTION:
                log(ERROR, "Received fatal message error: "
                           "AMQP_RESPONSE_SERVER_EXCEPTION");
                return;
            }
        }
    }

  private:
    void consumeUserList(const string& message) {
        log(INFO, "Consuming user lists");
        vector<string> allUsers;
        if (!message.empty()) {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root)) {
                for (const auto& user : root["users"]) {
                    allUsers.push_back(user.asString());
                }
            }
        }
        updateAllUsersFile(allUsers);
    }

    void consumeUserRelations(const string& message) {
        log(INFO, "Consuming user relations");
        if (message.empty()) {
            return;
        }
        vector<string> allUsers = getAllUsers();
        Json::Value jsonMsg;
        Json::Reader reader;
        if (!reader.parse(message, jsonMsg)) {
            return;
        }

        for (const auto& user : allUsers) {
            if (!jsonMsg.isMember(user)) {
                continue;
            }
            updateUserFollowers(user, jsonMsg);
        }
    }

    void consumeTimelines(const string& message) {
        log(INFO, "Consuming timelines");
        if (!message.empty()) {
            Json::Value root;
            Json::Reader reader;
            if (!reader.parse(message, root)) {
                return;
            }

            for (auto& user : getAllUsers()) {
                if (!root.isMember(user)) {
                    continue;
                }
                vector<string> lines;
                for (const auto& line : root[user]) {
                    lines.push_back(line.asString());
                }
                updateUserTimeline(user, lines);
            }
        }
    }

    void updateAllUsersFile(const vector<string>& users) {
        vector<string> currentUsers = getAllUsers();
        const path file = dirPrefix / "all_users.txt";
        SemGuard fileLock(file);
        ofstream fs(file, fs.app);
        for (string user : users) {
            if (std::find(currentUsers.begin(), currentUsers.end(), user) ==
                currentUsers.end()) {
                fs << user << std::endl;
            }
        }
    }

    void updateUserFollowers(const string& user, const Json::Value& jsonMsg) {
        vector<string> followers = getFollowers(stoi(user));
        const path file = dirPrefix / (user + "_followers.txt");
        SemGuard fileLock(file);
        ofstream fs(file, fs.app);
        for (const auto& follower : jsonMsg[user]) {
            if (std::find(followers.begin(), followers.end(),
                          follower.asString()) == followers.end()) {
                fs << follower.asString() << endl;
            }
        }
    }

    void updateUserTimeline(const string& user, const vector<string>& lines) {
        const path timelineFile = dirPrefix / (user + "_timeline.txt");
        SemGuard lock(timelineFile);

        time_t lastTimestamp = 0;
        std::fstream fs(timelineFile, fs.in | fs.out);
        char fill;
        std::tm postTime{};
        string postUser, postContent, empty;
        while (fs.peek() != ifstream::traits_type::eof()) {
            fs >> fill >> std::ws >>
                std::get_time(&postTime, "%a %b %d %H:%M:%S %Y") >> fill >>
                postUser >> fill >> std::ws;
            std::getline(fs, postContent);
            std::getline(fs, empty);
            lastTimestamp = std::mktime(&postTime);
        }
        fs.clear();

        std::istringstream ss;
        for (size_t i = 0; i < lines.size(); i += 4) {
            ss.clear();
            ss.str(lines[i]);
            ss >> fill >> std::ws >>
                std::get_time(&postTime, "%a %b %d %H:%M:%S %Y");
            time_t t = std::mktime(&postTime);
            if (t > lastTimestamp) {
                // Next 4 lines = single post
                for (size_t j = i; j < i + 4; ++j) {
                    fs << lines[j] << std::endl;
                }
            }
        }
    }

  private:
    amqp_bytes_t declareAndBindQueue(const string& exchangeName,
                                     const string& bindingKey) {
        amqp_queue_declare_ok_t* r = amqp_queue_declare(
            conn, channel, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

        amqp_bytes_t queueName = amqp_bytes_malloc_dup(r->queue);
        amqp_queue_bind(
            conn, channel, queueName, amqp_cstring_bytes(exchangeName.c_str()),
            !bindingKey.empty() ? amqp_cstring_bytes(bindingKey.c_str())
                                : amqp_empty_bytes,
            amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
        return queueName;
    }
};

void RunServer(string port);
void publishMessages(string port);
void consumeMessages();

int main(int argc, char** argv) {
    int opt = 0;
    string coordIP;
    string coordPort;
    string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1) {
        switch (opt) {
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'i':
            synchID = stoi(optarg);
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    clusterID = ((synchID - 1) % 3) + 1;

    string logFilename = string("synchronizer-") + port;
    google::InitGoogleLogging(logFilename.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    coord_stub = CoordService::NewStub(
        grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials()));

    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    thread hb(Heartbeat, serverInfo);

    RunServer(port);
    return 0;
}

void RunServer(string port) {
    // Wait for registration heartbeat before continuing
    std::unique_lock<mutex> lock(registeredMtx);
    registeredCV.wait(lock, [] { return registered; });
    log(INFO, "Registration complete; setting up synchronizer");

    thread producer(publishMessages, port);
    thread consumer(consumeMessages);

    producer.join();
    consumer.join();
}

void Heartbeat(ServerInfo serverInfo) {
    Confirmation conf;
    ServerInfo response;
    ID id;
    id.set_id(synchID);

    while (true) {
        {
            ClientContext context;
            log(INFO, "Sending heartbeat to coordinator");
            grpc::Status status =
                coord_stub->Heartbeat(&context, serverInfo, &conf);
            if (!status.ok()) {
                log(ERROR,
                    "Did not receive reply from coordinator. Terminating...");
                std::terminate();
            }
        }
        {
            ClientContext context;
            log(INFO, "Sending `GetFollowerServer` request to coordinator");
            coord_stub->GetFollowerServer(&context, id, &response);
        }

        serverID = response.serverid();
        isMaster = response.ismaster();
        dirPrefix =
            path("cluster_" + to_string(clusterID)) / to_string(serverID);

        if (!registered) {
            registered = true;
            registeredCV.notify_all();
        }
        sleep_for(5s);
    }
}

void publishMessages(string port) {
    SynchronizerProducer producer("rabbitmq", 5672);

    ServerInfo msg;
    Confirmation c;
    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    ServerList followerServers;
    ID id;
    id.set_id(synchID);

    while (true) {
        if (isMaster) {
            producer.publishUserList();
            producer.publishUserRelations();
            producer.publishTimelines();
        }
        sleep_for(5s);
    }
}

void consumeMessages() {
    SynchronizerConsumer consumer("rabbitmq", 5672);
    consumer.consumeMessages();
}

vector<string> getAllUsers() {
    return getLinesFromFile(dirPrefix / "all_users.txt");
}
vector<string> getTimeline(int userID) {
    return getLinesFromFile(dirPrefix / (to_string(userID) + "_timeline.txt"));
}
vector<string> getFollowers(int userID) {
    return getLinesFromFile(dirPrefix / (to_string(userID) + "_followers.txt"));
}
