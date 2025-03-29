// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using std::ifstream, std::ofstream;
using std::string, std::to_string;
using std::vector;
using std::unordered_map;
using std::unique_ptr;
using std::stoi;
using std::mutex, std::lock_guard;

class SemGuard {
    sem_t* sem;

    string makeName(const string& filename) {
        string semName = filename;
        std::replace(semName.begin(), semName.end(), '/', '_');
        semName.insert(0, 1, '/');
        return semName;
    }

public:
    SemGuard(const string& name) {
        sem = sem_open(makeName(name).c_str(), O_CREAT, 0644, 1);
        sem_wait(sem);
    }
    ~SemGuard() {
        sem_post(sem);
        sem_close(sem);
    }
};

int synchID = 1;
int clusterID = 1;

unique_ptr<CoordService::Stub> coord_stub_;
int registered_synchronizers = 6; // update this by asking coordinator
string coordAddr;
string clusterSubdirectory; // Server ID this synchronizer belongs to
vector<string> otherHosts;
mutex otherHostsMtx;

vector<string> get_lines_from_file(const string&);
vector<string> get_all_users();
vector<string> get_tl_or_fl(int, bool);
vector<string> getFollowersOfUser(int);
bool file_contains_user(const string& filename, const string& user);
string get_dir_prefix();

void Heartbeat(ServerInfo serverInfo);

class SynchronizerRabbitMQ {
private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    string hostname;
    int port;
    int synchID;

    void setupRabbitMQ() {
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, hostname.c_str(), port);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
    }

    void declareQueue(const string &queueName) {
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    void publishMessage(const string &queueName, const string &message) {
        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

    string consumeMessage(const string &queueName, int timeout_ms = 5000) {
        amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

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
    SynchronizerRabbitMQ(const string &host, int p, int id)
        : channel(1), hostname(host), port(p), synchID(id)
    {
        setupRabbitMQ();
        declareQueue("synch" + to_string(synchID) + "_users_queue");
        declareQueue("synch" + to_string(synchID) + "_clients_relations_queue");
        declareQueue("synch" + to_string(synchID) + "_timeline_queue");
    }

    void publishUserList() {
        vector<string> users = get_all_users();
        sort(users.begin(), users.end());

        Json::Value userList;
        for (auto user : users) {
            userList["users"].append(user);
        }

        Json::FastWriter writer;
        string message = writer.write(userList);
        publishMessage("synch" + to_string(synchID) + "_users_queue", message);
    }

    void consumeUserLists() {
        vector<string> allUsers;
        // YOUR CODE HERE

        // TODO: while the number of synchronizers is harcorded as 6 right now, you need to change this
        // to use the correct number of follower synchronizers that exist overall
        // accomplish this by making a gRPC request to the coordinator asking for the list of all follower synchronizers registered with it
        for (int i = 1; i <= 6; i++) {
            string queueName = "synch" + to_string(i) + "_users_queue";
            string message = consumeMessage(queueName, 1000); // 1 second timeout
            if (!message.empty()) {
                Json::Value root;
                Json::Reader reader;
                if (reader.parse(message, root))
                    for (const auto &user : root["users"])
                        allUsers.push_back(user.asString());
            }
        }
        updateAllUsersFile(allUsers);
    }

    void publishClientRelations() {
        Json::Value relations;
        vector<string> users = get_all_users();

        for (const auto &client : users) {
            int clientId = stoi(client);
            vector<string> followers = getFollowersOfUser(clientId);

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
        publishMessage("synch" + to_string(synchID) + "_clients_relations_queue", message);
    }

    void consumeClientRelations() {
        vector<string> allUsers = get_all_users();

        // YOUR CODE HERE

        // TODO: hardcoding 6 here, but you need to get list of all synchronizers from coordinator as before
        for (int i = 1; i <= 6; i++) {
            string queueName = "synch" + to_string(i) + "_clients_relations_queue";
            string message = consumeMessage(queueName, 1000); // 1 second timeout

            if (!message.empty()) {
                Json::Value root;
                Json::Reader reader;
                if (reader.parse(message, root)) {
                    for (const auto &client : allUsers) {
                        string followerFile = get_dir_prefix() + client + "_followers.txt";
                        SemGuard fileLock(followerFile);

                        ofstream followerStream(followerFile, std::ios::app);
                        if (root.isMember(client)) {
                            for (const auto &follower : root[client]) {
                                if (!file_contains_user(followerFile, follower.asString())) {
                                    followerStream << follower.asString() << std::endl;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client
    void publishTimelines() {
        vector<string> users = get_all_users();

        for (const auto &client : users) {
            int clientId = stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            // only do this for clients in your own cluster
            if (client_cluster != clusterID) {
                continue;
            }

            vector<string> timeline = get_tl_or_fl(clientId, true);
            vector<string> followers = getFollowersOfUser(clientId);

            for (const auto &follower : followers) {
                // send the timeline updates of your current user to all its followers

                // YOUR CODE HERE

            }
        }
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimelines() {
        string queueName = "synch" + to_string(synchID) + "_timeline_queue";
        string message = consumeMessage(queueName, 1000); // 1 second timeout

        if (!message.empty()) {
            // consume the message from the queue and update the timeline file of the appropriate client with
            // the new updates to the timeline of the user it follows

            // YOUR CODE HERE
        }
    }

private:
    void updateAllUsersFile(const vector<string> &users) {
        string usersFile = get_dir_prefix() + "all_users.txt";
        SemGuard fileLock(usersFile);

        ofstream userStream(usersFile, std::ios::app);
        for (string user : users)
            if (!file_contains_user(usersFile, user))
                userStream << user << std::endl;
    }
};

void run_synchronizer(string port, SynchronizerRabbitMQ &rabbitMQ);

void RunServer(string port_no) {
    // Initialize RabbitMQ connection
    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672, synchID);

    std::thread t1(run_synchronizer, port_no, std::ref(rabbitMQ));

    // Create a consumer thread
    std::thread consumerThread([&rabbitMQ]() {
        while (true) {
            rabbitMQ.consumeUserLists();
            rabbitMQ.consumeClientRelations();
            rabbitMQ.consumeTimelines();
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    });

    t1.join();
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

    string log_file_name = string("synchronizer-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    coord_stub_ = CoordService::NewStub(
        grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials()));

    clusterID = ((synchID - 1) % 3) + 1;
    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    Heartbeat(serverInfo);

    RunServer(port);
    return 0;
}

void run_synchronizer(string port, SynchronizerRabbitMQ &rabbitMQ)
{

    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    // TODO: begin synchronization process
    while (true) {
        // the synchronizers sync files every 5 seconds
        sleep(5);

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        // making a request to the coordinator to see count of follower synchronizers
        coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

        {
            lock_guard<mutex> lock(otherHostsMtx);
            otherHosts.clear();
            for (int i = 0; i < followerServers.hostname_size(); ++i) {
                if (followerServers.serverid(i) == synchID) continue;
                otherHosts.push_back(followerServers.hostname(i));
            }
            registered_synchronizers = (int)otherHosts.size();
        }

        // Publish user list
        rabbitMQ.publishUserList();

        // Publish client relations
        rabbitMQ.publishClientRelations();

        // Publish timelines
        rabbitMQ.publishTimelines();
    }
    return;
}

vector<string> get_lines_from_file(const string& filename) {
    vector<string> lines;
    string line;
    ifstream fs;

    SemGuard fileLock(filename);

    fs.open(filename);
    while (fs.peek() != ifstream::traits_type::eof()) {
        getline(fs, line);
        if (!line.empty())
            lines.push_back(line);
    }
    fs.close();

    return lines;
}

void Heartbeat(ServerInfo serverInfo) {
    // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
    // servers to register the synchronizer with the coordinator and determine whether it is a master

    log(INFO, "Sending initial heartbeat to coordinator");

    // send a heartbeat to the coordinator, which registers your follower synchronizer as either a master or a slave

    // YOUR CODE HERE
}

bool file_contains_user(const string& filename, const string& user) {
    vector<string> users = get_lines_from_file(filename);
    SemGuard fileLock(filename);

    bool found = false;
    for (size_t i = 0; i < users.size(); i++) {
        if (user == users[i]) {
            found = true;
            break;
        }
    }

    return found;
}

vector<string> get_all_users() {
    string master_users_file = "cluster_" + to_string(clusterID) + "/1/all_users.txt";
    string slave_users_file = "cluster_" + to_string(clusterID) + "/2/all_users.txt";

    // take longest list and package into AllUsers message
    vector<string> master_user_list = get_lines_from_file(master_users_file);
    vector<string> slave_user_list = get_lines_from_file(slave_users_file);

    return master_user_list.size() >= slave_user_list.size() ?
        master_user_list : slave_user_list;
}

vector<string> get_tl_or_fl(int clientID, bool tl) {
    string master_fn = "cluster_" + to_string(clusterID) + "/1/" + to_string(clientID);
    string slave_fn = "cluster_" + to_string(clusterID) + "/2/" + to_string(clientID);
    if (tl) {
        master_fn.append("_timeline.txt");
        slave_fn.append("_timeline.txt");
    } else {
        master_fn.append("_followers.txt");
        slave_fn.append("_followers.txt");
    }

    vector<string> m = get_lines_from_file(master_fn);
    vector<string> s = get_lines_from_file(slave_fn);

    return m.size() >= s.size() ? m : s;
}

vector<string> getFollowersOfUser(int ID) {
    vector<string> followers;
    string clientID = to_string(ID);
    vector<string> usersInCluster = get_all_users();

    for (auto userID : usersInCluster) { // Examine each user's following file
        string file = get_dir_prefix() + userID + "_follow_list.txt";
        SemGuard fileLock(file);

        if (file_contains_user(file, clientID)) {
            followers.push_back(userID);
        }
    }

    return followers;
}

string get_dir_prefix() {
    return "cluster_" + to_string(clusterID) + "/" + clusterSubdirectory + "/";
}
