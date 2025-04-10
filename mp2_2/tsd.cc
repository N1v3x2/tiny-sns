#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <semaphore.h>
#include <stdlib.h>
#include <string>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#define log(severity, msg)                                                     \
    LOG(severity) << msg;                                                      \
    google::FlushLogFiles(google::severity);

#include "coordinator.grpc.pb.h"
#include "file_utils.h"
#include "sns.grpc.pb.h"

using namespace std::chrono_literals;
using namespace std::this_thread;
using namespace std::filesystem;

using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::ServerInfo;
using csce438::SNSService;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using std::cout;
using std::endl;
using std::ifstream;
using std::lock_guard;
using std::map;
using std::mutex;
using std::ofstream;
using std::shared_ptr;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::vector;

struct Client;
using client_ptr = shared_ptr<Client>;

/**
 * @brief This server's ID
 */
int serverID;

/**
 * @brief This server's cluster ID
 */
int clusterID;

/**
 * @brief The directory prefix for all files this server is responsible for,
 * e.g. "cluster_1/1"
 */
path dirPrefix;

/**
 * @brief The pathof the "all_users.txt" file this server uses
 */
path allUsersFile;

/**
 * @brief gRPC stub for communicating with the coordinator
 */
unique_ptr<CoordService::Stub> coordStub;

/**
 * @brief In-memory cache of `Client`s, which stores information about which
 * users are logged into the SNS and user follower/following relationships
 */
map<string, client_ptr> clientCache;
mutex clientMtx;

/**
 * @class Client
 * @brief In-memory representation of a client logged into the SNS
 *
 */
struct Client {
    string username;
    map<string, client_ptr> followers;
    map<string, client_ptr> following;

    Client(const string& uname) : username(uname) {}

    bool operator==(const Client& c1) const {
        return (username == c1.username);
    }

    path getTimelineFile() { return dirPrefix / (username + "_timeline.txt"); }
    path getFollowerFile() { return dirPrefix / (username + "_followers.txt"); }
    path getFollowingFile() {
        return dirPrefix / (username + "_following.txt");
    }
};

/**
 * @brief Gets information about this cluster's slave server from the
 * coordinator
 *
 * @return `ServerInfo` object containing information about the slave server
 */
ServerInfo GetSlaveInfo();

/**
 * @brief Checks with the coordinator whether this server is the master of its
 * cluster
 *
 * @return `true` if this server is the master, otherwise `false`
 */
bool isMaster();

/**
 * @brief Checks with the coordinator whether a slave server exists in this
 * cluster
 *
 * @return `true` if this cluster has a slave, otherwise `false`
 */
bool hasSlave();

/**
 * @brief Gets a gRPC stub to communicate with this cluster's slave server (only
 * called if this is a master)
 */
unique_ptr<SNSService::Stub> getSlaveStub();

/**
 * @brief Constructs a `Message` object (represents a timeline post)
 *
 * @param username The user who posted
 * @param msg The user's message
 * @return The constructed message
 */
Message makeMessage(const string& username, const string& msg);

class SNSServiceImpl final : public SNSService::Service {
    Status List(ServerContext* context, const Request* request,
                ListReply* response) override {
        string username = request->username();
        log(INFO, "Received `List` request " + username);

        lock_guard<mutex> lock(clientMtx);
        for (auto& [uname, _] : clientCache) {
            response->add_all_users(uname);
        }

        client_ptr client = clientCache[username];
        for (auto& [follower_uname, _] : client->followers) {
            response->add_followers(follower_uname);
        }

        log(INFO, "Sending `List` to client...");
        return Status::OK;
    }

    Status Follow(ServerContext* context, const Request* request,
                  Reply* response) override {
        string username = request->username();
        log(INFO, "Received `Follow` request from client " + username);

        if (request->arguments_size() < 1) {
            response->set_msg("Invalid command");
            return Status::OK;
        }
        string toFollow = request->arguments(0);

        client_ptr client = nullptr, clientToFollow = nullptr;
        {
            lock_guard<mutex> lock(clientMtx);
            if (!clientCache.count(toFollow)) {
                response->set_msg("Invalid username");
                return Status::OK;
            }
            client = clientCache[username];
            clientToFollow = clientCache[toFollow];

            if (client->following.count(toFollow) ||
                *client == *clientToFollow) {
                response->set_msg("Input username already exists");
                return Status::OK;
            }

            client->following[toFollow] = clientToFollow;
            clientToFollow->followers[username] = client;
        }
        {
            const string file = client->getFollowingFile();
            SemGuard fileLock(file);
            ofstream fs(file);
            fs << toFollow << endl;
        }
        {
            const string file = clientToFollow->getFollowerFile();
            SemGuard fileLock(file);
            ofstream fs(file);
            fs << username << endl;
        }

        if (isMaster() && hasSlave()) {
            unique_ptr<SNSService::Stub> slaveStub = getSlaveStub();
            ClientContext ctx;
            Request req;
            req.set_username(username);
            req.add_arguments(toFollow);
            Reply reply;
            log(INFO, "Master: replicating Follow state to slave");
            slaveStub->Follow(&ctx, req, &reply);
        }

        return Status::OK;
    }

    Status Login(ServerContext* context, const Request* request,
                 Reply* reply) override {
        string username = request->username();
        log(INFO, "Received `Login` request from client " + username);

        {
            lock_guard<mutex> lock(clientMtx);
            if (clientCache.count(username)) {
                return Status::OK;
            }

            client_ptr client = std::make_shared<Client>(username);
            clientCache[username] = client;

            ofstream fs(client->getTimelineFile());
            fs.close();
            fs.open(client->getFollowingFile());
            fs.close();
            fs.open(client->getFollowerFile());
            fs.close();

            // Add user to all_users.txt
            SemGuard fileLock(allUsersFile);
            fs.open(allUsersFile, fs.app);
            fs << username << endl;
        }

        if (isMaster() && hasSlave()) {
            unique_ptr<SNSService::Stub> slave_stub = getSlaveStub();
            ClientContext ctx;
            Request req;
            req.set_username(username);
            Reply rep;
            log(INFO, "Master: replicating Login state to slave");
            slave_stub->Login(&ctx, req, &rep);
        }

        return Status::OK;
    }

    Status Timeline(ServerContext* context,
                    ServerReaderWriter<Message, Message>* stream) override {
        Message msg;
        client_ptr client = nullptr;
        string username;

        // Set the current user based on the first message received
        if (stream->Read(&msg)) {
            username = msg.username();
            client = clientCache[username];
        } else {
            return Status::OK;
        }

        log(INFO, "Received `Timeline` request from client " + username);

        // Use background thread to monitor changes to the user's file
        thread monitorTimeline([stream, client, username]() {
            ifstream fs;
            char fill;
            std::tm postTime{};
            string postUser, postContent, dummy;
            Message timelineMsg;

            time_t lastRead = 0;
            while (true) {
                vector<Message> msgs;
                {
                    const string file = client->getTimelineFile();
                    SemGuard lock(file);
                    fs.clear();
                    fs.open(file);

                    while (fs.peek() != ifstream::traits_type::eof()) {
                        fs >> fill >> std::ws >>
                            std::get_time(&postTime, "%a %b %d %H:%M:%S "
                                                     "%Y") >>
                            fill >> postUser >> fill >> std::ws;
                        std::getline(fs, postContent);
                        std::getline(fs, dummy);

                        // Only read new posts (since
                        // last read)
                        if (std::mktime(&postTime) <= lastRead)
                            continue;

                        Timestamp* timestamp = new Timestamp();
                        timestamp->set_seconds(std::mktime(&postTime));
                        timestamp->set_nanos(0);
                        timelineMsg.set_allocated_timestamp(timestamp);
                        timelineMsg.set_username(postUser);
                        timelineMsg.set_msg(postContent);
                        msgs.push_back(timelineMsg);
                    }
                    fs.close();
                }

                if (!msgs.empty()) {
                    log(INFO, "User " + client->username +
                                  " has unread posts; sending "
                                  "latest posts...");
                    for (auto& msg : msgs)
                        stream->Write(msg);
                    lastRead =
                        TimeUtil::TimestampToTimeT(msgs.back().timestamp());
                }

                sleep_for(5s);
            }
        });

        std::shared_ptr<grpc::ClientReaderWriter<Message, Message>> slaveStream;
        ClientContext ctx;
        if (isMaster() && hasSlave()) {
            unique_ptr<SNSService::Stub> slaveStub = getSlaveStub();
            slaveStream = slaveStub->Timeline(&ctx);
            log(INFO, "Master: establishing timeline connection to "
                      "slave");
            string connMsg = "Timeline";
            slaveStream->Write(makeMessage(username, connMsg));
        }

        ofstream fs;
        while (stream->Read(&msg)) {
            // Write posts to all followers
            log(INFO, "User " + client->username +
                          " just posted; sending post to followers...");

            lock_guard<mutex> lock(clientMtx);
            for (auto& [_, follower] : client->followers) {
                {
                    const string file = follower->getTimelineFile();
                    SemGuard fileLock(file);
                    time_t curr_time = time(nullptr);
                    std::tm* t = gmtime(&curr_time);
                    fs.clear();
                    fs.open(file, fs.app);
                    fs << "T " << asctime(t) << "U " << username << '\n'
                       << "W " << msg.msg() << '\n';
                    fs.close();
                }
                if (isMaster() && hasSlave()) {
                    log(INFO, "Master: replicating "
                              "timeline post to slave");
                    slaveStream->Write(msg);
                }
            }
        }
        monitorTimeline.join();
        return Status::OK;
    }
};

bool isMaster() {
    ClientContext ctx;
    ID req;
    req.set_id(serverID);
    ServerInfo reply;
    coordStub->GetSlave(&ctx, req, &reply);
    return serverID != reply.serverid();
}

bool hasSlave() {
    ClientContext ctx;
    ID req;
    req.set_id(serverID);
    ServerInfo reply;
    coordStub->GetSlave(&ctx, req, &reply);
    return reply.serverid() != -1;
}

unique_ptr<SNSService::Stub> getSlaveStub() {
    ClientContext ctx;
    ID req;
    req.set_id(clusterID);
    ServerInfo reply;
    coordStub->GetSlave(&ctx, req, &reply);
    string slaveAddr = reply.hostname() + ":" + reply.port();
    return SNSService::NewStub(
        grpc::CreateChannel(slaveAddr, grpc::InsecureChannelCredentials()));
}

Message makeMessage(const string& username, const string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}

/**
 * @brief Sends heartbeats periodically (every 5 seconds) to the coordinator;
 * used by background thread
 *
 * @param coordIP The coordinator's IP
 * @param coordPort The coordinator's port
 * @param portNo This server's port
 */
void heartbeat(string coordIP, string coordPort, string serverPort) {
    const string hostname = "0.0.0.0";
    string coord_address = coordIP + ":" + coordPort;

    ServerInfo info;
    info.set_serverid(serverID);
    info.set_hostname(hostname);
    info.set_port(serverPort);
    info.set_type("server");
    info.set_clusterid(clusterID);

    Confirmation confirmation;

    while (true) {
        ClientContext context;
        log(INFO, "Sending heartbeat to coordinator");
        grpc::Status status =
            coordStub->Heartbeat(&context, info, &confirmation);
        if (!status.ok() || !confirmation.status()) {
            log(ERROR, "Heartbeat did not receive reply from coordinator");
            std::terminate();
        }
        log(INFO, "Received heartbeat confirmation from coordinator");
        sleep_for(5s);
    }
}

/**
 * @brief Periodically updates the in-memory client cache by reading files
 * belonging to this server's cluster (used by background thread)
 */
void UpdateClientCache() {
    string username, follower, following;

    while (true) {
        sleep_for(3s);
        lock_guard<mutex> lock(clientMtx);
        {
            SemGuard fileLock(allUsersFile);
            ifstream fs(allUsersFile);
            while (fs.peek() != ifstream::traits_type::eof()) {
                getline(fs, username);
                if (!clientCache.count(username)) {
                    client_ptr client = std::make_shared<Client>(username);
                    clientCache[username] = client;

                    ofstream newFS(client->getTimelineFile());
                    newFS.close();
                    newFS.open(client->getFollowingFile());
                    newFS.close();
                    newFS.open(client->getFollowerFile());
                    newFS.close();
                }
            }
            fs.close();
        }
        // Update follower & following information
        for (auto& [_, user] : clientCache) {
            {
                const string file = user->getFollowingFile();
                SemGuard fileLock(file);
                ifstream fs(file);
                while (fs.peek() != ifstream::traits_type::eof()) {
                    getline(fs, following);
                    if (!user->following.count(following))
                        user->following[following] = clientCache[following];
                }
            }
            {
                const string file = user->getFollowerFile();
                SemGuard fileLock(file);
                ifstream fs(file);
                while (fs.peek() != ifstream::traits_type::eof()) {
                    getline(fs, follower);
                    if (!user->followers.count(follower))
                        user->followers[follower] = clientCache[follower];
                }
            }
        }
    }
}

/**
 * @brief Starts the server
 *
 * @param coordIP The coordinator's IP
 * @param coordPort The coordinator's port
 * @param portNo This server's port
 */
void RunServer(string coordIP, string coordPort, string serverPort) {
    const string hostname = "0.0.0.0";
    string serverAddr = hostname + ":" + serverPort;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(serverAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    unique_ptr<Server> server(builder.BuildAndStart());
    cout << "Server listening on " << serverAddr << std::endl;
    log(INFO, "Server listening on " + serverAddr);

    string coordAddr = coordIP + ":" + coordPort;
    coordStub = CoordService::NewStub(
        grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials()));

    thread updateClientDB(UpdateClientCache);
    thread hb(heartbeat, coordIP, coordPort, serverPort);
    server->Wait();

    updateClientDB.join();
    hb.join();
}

int main(int argc, char** argv) {
    string coordIP, coordPort;
    string port;

    if (argc < 6) {
        cout << "Expected 6 args, got " << argc << '\n';
        cout << "Usage: tsd -c <clusterId> -s <serverId> -h "
                "<coordinatorIP> -k "
                "<coordinatorPort> -p <portNum>\n";
        return 0;
    }

    int opt = 0;
    while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
        switch (opt) {
        case 'c':
            clusterID = atoi(optarg);
            break;
        case 's':
            serverID = atoi(optarg);
            break;
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    string logFilename = string("server-") + port;
    google::InitGoogleLogging(logFilename.c_str());
    log(INFO, "Logging initialized");

    dirPrefix = path("cluster_" + to_string(clusterID)) / to_string(serverID);
    allUsersFile = dirPrefix / "all_users.txt";
    {
        SemGuard lock(allUsersFile);
        remove_all(dirPrefix);
        create_directories(dirPrefix);
        ofstream fs(allUsersFile);
    }

    RunServer(coordIP, coordPort, port);
    return 0;
}
