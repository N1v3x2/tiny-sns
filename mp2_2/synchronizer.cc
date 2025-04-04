#include <bits/fs_fwd.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <iomanip>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include "file_utils.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

using namespace std::chrono_literals;
using namespace std::this_thread;

using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using grpc::ClientContext;
using std::ifstream, std::ofstream;
using std::string;
using std::to_string;
using std::vector;
using std::unique_ptr;
using std::stoi;
using std::mutex;
using std::condition_variable;
using std::cout;
using std::endl;
using std::lock_guard;

struct Host {
    int serverId;
    string hostname;
    string port;
    Host(int s, string h, string p) :
        serverId(s), hostname(h), port(p)
    {}
};

int synchID;
int serverID; // Server corresponding to the synchronzier
int clusterID;

unique_ptr<CoordService::Stub> coord_stub;
string coordAddr;

vector<Host> otherHosts;
mutex registeredMtx, hostsMtx;

condition_variable registeredCV;
bool registered = false, isMaster = false;

vector<string> getAllUsers();
vector<string> getTimeline(int clientID);
vector<string> getFollowers(int clientID);
string getDirPrefix();

void Heartbeat(ServerInfo serverInfo);

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
    switch (x.reply_type) {
        case AMQP_RESPONSE_NORMAL:
            return;

        case AMQP_RESPONSE_NONE:
            fprintf(stderr, "%s: missing RPC reply type!\n", context);
            break;

        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
            break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (x.reply.id) {
            case AMQP_CONNECTION_CLOSE_METHOD: {
                amqp_connection_close_t *m = (amqp_connection_close_t *)x.reply.decoded;
                fprintf(stderr, "%s: server connection error %uh, message: %.*s\n",
                    context, m->reply_code, (int)m->reply_text.len,
                    (char *)m->reply_text.bytes);
                break;
            }
            case AMQP_CHANNEL_CLOSE_METHOD: {
                amqp_channel_close_t *m = (amqp_channel_close_t *)x.reply.decoded;
                fprintf(stderr, "%s: server channel error %uh, message: %.*s\n", context,
                    m->reply_code, (int)m->reply_text.len,
                    (char *)m->reply_text.bytes);
                break;
            }
            default:
                fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context,
                      x.reply.id);
                break;
        }
        break;
    }
    // FIXME: replace with exception throwing for more robustness (otherwise destructors won't get called)
    exit(1);
}

void die_on_error(int x, char const *context) {
    if (x < 0) {
        fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x));
        exit(1);
    }
}

void die(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
    exit(1);
}

class SynchronizerRabbitMQ {
private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    amqp_bytes_t allUsersQueue, relationsQueue, timelinesQueue;
    string hostname;
    int port;

    void setupRabbitMQ() {
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            die("Creating TCP socket");
        }

        int status = amqp_socket_open(socket, hostname.c_str(), port);
        if (status) {
            die("Opening TCP socket");
        }

        die_on_amqp_error(
            amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
            "Logging in");

        amqp_channel_open(conn, channel);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
    }

    void declareExchange(const string& exchangeName, const string& type) {
        amqp_exchange_declare(conn, channel,
                              amqp_cstring_bytes(exchangeName.c_str()),
                              amqp_cstring_bytes(type.c_str()),
                              0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
    }

    amqp_bytes_t declareAndBindQueue(const string& exchangeName, const string& bindingKey) {
        amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, channel,
                                                        amqp_empty_bytes,
                                                        0, 0, 1, 1, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

        amqp_bytes_t queueName = amqp_bytes_malloc_dup(r->queue);
        amqp_queue_bind(conn, channel, queueName,
                        amqp_cstring_bytes(exchangeName.c_str()),
                        !bindingKey.empty() ? amqp_cstring_bytes(bindingKey.c_str()) : amqp_empty_bytes,
                        amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
        return queueName;
    }

    void publishMessage(const string& exchangeName,
                        const string& routingKey,
                        const string& message) {
        int status = amqp_basic_publish(
            conn, channel,
            amqp_cstring_bytes(exchangeName.c_str()),
            !routingKey.empty() ? amqp_cstring_bytes(routingKey.c_str()) : amqp_empty_bytes,
            0, 0, NULL,
            amqp_cstring_bytes(message.c_str()));
        die_on_error(status, "Publish Message");
    }

    string consumeMessage(amqp_bytes_t queueName, int timeout_ms = 5000) {
        amqp_basic_consume(conn, channel,
                           queueName,
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type != AMQP_RESPONSE_NORMAL) {
            return "";
        }

        string message(
            static_cast<char *>(envelope.message.body.bytes),
            envelope.message.body.len);
        amqp_destroy_envelope(&envelope);
        return message;
    }

public:
    SynchronizerRabbitMQ(const string &host, int p)
        : channel(1), hostname(host), port(p)
    {
        setupRabbitMQ();
        declareExchange("all_users", "direct");
        declareExchange("relations", "direct");
        declareExchange("timelines", "direct");
        allUsersQueue = declareAndBindQueue("all_users", to_string(synchID));
        relationsQueue = declareAndBindQueue("relations", to_string(synchID));
        timelinesQueue = declareAndBindQueue("timelines", to_string(synchID));
    }

    ~SynchronizerRabbitMQ() {
        die_on_amqp_error(amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS),
                          "Closing channel");
        die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                          "Closing connection");
        die_on_error(amqp_destroy_connection(conn), "Detroying connection");
    }

    void publishUserList() {
        vector<string> users = getAllUsers();
        sort(users.begin(), users.end());

        Json::Value userList;
        for (auto user : users) {
            userList["clients"].append(user);
        }

        Json::FastWriter writer;
        string message = writer.write(userList);
        log(INFO, "Publishing user list");

        lock_guard<mutex> lock(hostsMtx);
        for (auto& host : otherHosts) {
            publishMessage("all_users", to_string(host.serverId), message);
        }
    }

    void consumeUserLists() {
        log(INFO, "Consuming user lists");
        string message = consumeMessage(allUsersQueue, 1000);

        vector<string> allUsers;
        if (!message.empty()) {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root)) {
                for (const auto &user : root["clients"]) {
                    allUsers.push_back(user.asString());
                }
            }
        }

        updateAllUsersFile(allUsers);
    }

    void publishClientRelations() {
        Json::Value relations;
        vector<string> users = getAllUsers();

        for (const auto &client : users) {
            int clientId = stoi(client);
            vector<string> followers = getFollowers(clientId);

            Json::Value followerList(Json::arrayValue);
            for (const auto &follower : followers) {
                followerList.append(follower);
            }

            if (!followerList.empty()) {
                relations[client] = followerList;
            }
        }

        Json::FastWriter writer;
        string message = writer.write(relations);
        log(INFO, "Publishing client relations");

        lock_guard<mutex> lock(hostsMtx);
        for (auto& host : otherHosts) {
            publishMessage("relations", to_string(host.serverId), message);
        }
    }

    void consumeClientRelations() {
        vector<string> allUsers = getAllUsers();
        log(INFO, "Consuming client relations");
        string message = consumeMessage(relationsQueue, 1000);

        if (message.empty()) { return; }

        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(message, root)) { return; }

        for (const auto &client : allUsers) {
            if (!root.isMember(client)) { continue; }

            vector<string> followers = getFollowers(stoi(client));
            string file = getDirPrefix() + client + "_followers.txt";
            SemGuard fileLock(file);
            ofstream fs(file, fs.app);

            for (const auto &follower : root[client]) {
                if (std::find(followers.begin(), followers.end(), follower.asString()) == followers.end()) {
                    fs << follower.asString() << endl;
                }
            }
        }
    }

    void publishTimelines() {
        Json::FastWriter writer;
        for (const auto &client : getAllUsers()) {
            int clientID = stoi(client);
            /*int clientCluster = (stoi(client) - 1) % 3 + 1;*/

            Json::Value timelineJson;
            for (const auto& line : getTimeline(clientID)) {
                timelineJson["lines"].append(line);
            }

            log(INFO, "Publishing timeline for client " + client);
            timelineJson["client"] = client;
            string timelineMsg = writer.write(timelineJson);

            lock_guard<mutex> lock(hostsMtx);
            for (auto& host : otherHosts) {
                publishMessage("timelines", to_string(host.serverId), timelineMsg);
            }
        }
    }

    void consumeTimelines() {
        log(INFO, "Consuming timelines");
        string message = consumeMessage(timelinesQueue, 1000);
        if (!message.empty()) {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root)) {
                string client = root["client"].asString();
                vector<string> lines;
                for (const auto& line : root["lines"]) {
                    lines.push_back(line.asString());
                }
                updateClientTimeline(client, lines);
            }
        }
    }

private:
    void updateAllUsersFile(const vector<string> &users) {
        vector<string> allUsers = getAllUsers();
        string file = getDirPrefix() + "all_users.txt";
        SemGuard fileLock(file);
        ofstream fs(file, fs.app);
        for (string user : users) {
            if (std::find(allUsers.begin(), allUsers.end(), user) == allUsers.end()) {
                fs << user << std::endl;
            }
        }
    }

    void updateClientTimeline(const string& client, const vector<string>& lines) {
        string clientFile = getDirPrefix() + client + ".txt";
        SemGuard lock(clientFile);

        time_t lastTimestamp = 0;
        std::fstream fs(clientFile, fs.in | fs.out);
        char fill;
        std::tm postTime{};
        string postUser, postContent, dummy;
        while (fs.peek() != ifstream::traits_type::eof()) {
            fs >> fill >> std::ws >> std::get_time(&postTime, "%a %b %d %H:%M:%S %Y")
               >> fill >> postUser
               >> fill >> std::ws;
            std::getline(fs, postContent);
            std::getline(fs, dummy);
            lastTimestamp = std::mktime(&postTime);
        }
        fs.clear();

        std::istringstream ss;
        for (size_t i = 0; i < lines.size(); i += 4) {
            ss.clear();
            ss.str(lines[i]);
            ss >> fill >> std::ws >> std::get_time(&postTime, "%a %b %d %H:%M:%S %Y");
            time_t t = std::mktime(&postTime);
            if (t > lastTimestamp) {
                // Next 4 lines = single post
                for (size_t j = i; j < i + 4; ++j) {
                    fs << lines[j] << std::endl;
                }
            }
        }
    }
};

void runSynchronizer(string port, SynchronizerRabbitMQ &rabbitMQ);

void RunServer(string port_no) {
    // Wait for registration before continuing
    std::unique_lock<mutex> lock(registeredMtx);
    registeredCV.wait(lock, []{ return registered; });
    log(INFO, "Registration complete; setting up synchronizer");

    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672);

    std::thread producerThread(runSynchronizer, port_no, std::ref(rabbitMQ));
    std::thread consumerThread([&rabbitMQ]() {
        while (true) {
            rabbitMQ.consumeUserLists();
            rabbitMQ.consumeClientRelations();
            rabbitMQ.consumeTimelines();
            // Want to consume at least as fast as the producer produces
            sleep_for(1s);
        }
    });

    producerThread.join();
    consumerThread.join();
}

int main(int argc, char **argv) {
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

    string logFilename = string("synchronizer-") + port;
    google::InitGoogleLogging(logFilename.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    coord_stub = CoordService::NewStub(
        grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials()));

    clusterID = ((synchID - 1) % 3) + 1;
    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    std::thread hb(Heartbeat, serverInfo);

    RunServer(port);
    return 0;
}

void runSynchronizer(string port, SynchronizerRabbitMQ &rabbitMQ) {
    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    while (true) {
        sleep_for(10s);

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        coord_stub->GetFollowerServers(&context, id, &followerServers);
        {
            std::lock_guard<mutex> lock(hostsMtx);
            otherHosts.clear();
            for (int i = 0; i < followerServers.serverid_size(); ++i) {
                otherHosts.push_back({
                    followerServers.serverid(i),
                    followerServers.hostname(i),
                    followerServers.port(i)});
            }
        }

        if (isMaster) {
            rabbitMQ.publishUserList();
            rabbitMQ.publishClientRelations();
            rabbitMQ.publishTimelines();
        }
    }
    return;
}

void Heartbeat(ServerInfo serverInfo) {
    Confirmation conf;
    {
        ClientContext context;
        log(INFO, "Sending registration heartbeat to coordinator");
        grpc::Status status = coord_stub->Heartbeat(&context, serverInfo, &conf);
    }
    {
        ClientContext context;
        ID id;
        id.set_id(synchID);
        ServerInfo info;
        log(INFO, "Sending `GetFollowerServer` request to coordinator");
        coord_stub->GetFollowerServer(&context, id, &info);
        serverID = info.serverid();
        isMaster = info.ismaster();
    }

    if (conf.status()) {
        registered = true;
        registeredCV.notify_all();
    }

    while (true) {
        ClientContext context;
        log(INFO, "Sending heartbeat to coordinator");
        grpc::Status status = coord_stub->Heartbeat(&context, serverInfo, &conf);
        if (!conf.status()) {
            log(ERROR, "Did not receive reply from coordinator. Terminating...");
            std::terminate();
        }
        sleep_for(5s);
    }
}

vector<string> getLinesFromFile(const string& filename) {
    auto getLines = [](const string& filepath) {
        vector<string> lines;
        SemGuard fileLock(filepath);
        ifstream fs(filepath);
        string line;
        while (fs.peek() != ifstream::traits_type::eof()) {
            getline(fs, line);
            lines.push_back(line);
        }
        return lines;
    };
    string masterFile = "cluster_" + to_string(clusterID) + "/1/" + filename;
    /*string slaveFile = "cluster_" + to_string(clusterID) + "/2/" + filename;*/
    vector<string> masterLines = getLines(masterFile);
    /*vector<string> slaveLines = getLines(slaveFile);*/
    /*return masterLines.size() >= slaveLines.size() ? masterLines : slaveLines;*/
    return masterLines;
}
vector<string> getAllUsers() { return getLinesFromFile("all_users.txt"); }
vector<string> getTimeline(int clientID) { return getLinesFromFile(to_string(clientID) + ".txt"); }
vector<string> getFollowers(int clientID) { return getLinesFromFile(to_string(clientID) + "_followers.txt"); }
string getDirPrefix() { return "cluster_" + to_string(clusterID) + "/" + to_string(serverID) + "/"; }
