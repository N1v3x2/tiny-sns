#include <cstdio>
#include <ctime>

#include <chrono>
#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#define log(severity, msg)                                                     \
    LOG(severity) << msg;                                                      \
    google::FlushLogFiles(google::severity);

using namespace std::chrono_literals;
using namespace std::this_thread;

using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using std::cout;
using std::endl;
using std::lock_guard;
using std::mutex;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::vector;

struct zNode {
    int serverID;
    string hostname;
    string port;
    string type;
    time_t last_heartbeat;
    bool missed_heartbeat;
    zNode(int s, string h, string p, string t, time_t l, bool m)
        : serverID(s), hostname(h), port(p), type(t), last_heartbeat(l),
          missed_heartbeat(m) {}
    bool isActive();
};

using node_ptr = shared_ptr<zNode>;
using table = vector<vector<node_ptr>>;

const string SERVER = "server", SYNC = "synchronizer";

table routingTable{3, vector<node_ptr>()}, synchronizers{3, vector<node_ptr>()};
mutex routingTableMtx, synchronizerMtx;

node_ptr findServer(int clusterID, int serverID, string type);
node_ptr getMasterServer(int clusterID);
node_ptr getSlaveServer(int clusterID);
std::time_t getTimeNow();
void checkHeartbeat();

class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* request,
                     Confirmation* reply) override {
        int serverID = request->serverid();
        int clusterID = request->clusterid();
        string type = request->type();

        log(INFO, "Received heartbeat from " + type + " " +
                      to_string(serverID) + " in cluster " +
                      to_string(clusterID));

        lock_guard<mutex> lock(synchronizerMtx);
        lock_guard<mutex> lock2(routingTableMtx);
        node_ptr server = findServer(clusterID, serverID, type);

        if (!server) {
            // Register server after its first heartbeat
            node_ptr newserver = std::make_shared<zNode>(
                serverID, request->hostname(), request->port(), request->type(),
                getTimeNow(), false);

            if (newserver->type == SERVER) {
                routingTable[clusterID - 1].push_back(newserver);
            } else if (newserver->type == SYNC) {
                synchronizers[clusterID - 1].push_back(newserver);
            } else {
                log(WARNING, "Unknown server type attempted registration");
                reply->set_status(false);
                return Status::OK;
            }
        } else {
            // If server is already registered, simply record the heartbeat
            server->last_heartbeat = getTimeNow();
            server->missed_heartbeat = false;
        }

        reply->set_status(true);
        log(INFO, "Sending heartbeat confirmation to " + type + " " +
                      to_string(serverID) + " at cluster " +
                      to_string(clusterID));
        return Status::OK;
    }

    // Return the master
    Status GetServer(ServerContext* context, const ID* request,
                     ServerInfo* reply) override {
        int clientID = request->id();
        int clusterID = (clientID - 1) % 3 + 1;
        log(INFO,
            "Received `GetServer` request from client " + to_string(clientID));

        // First active server in the routing table is the master
        lock_guard<mutex> lock(routingTableMtx);
        node_ptr server = getMasterServer(clusterID);
        reply->set_serverid(server->serverID);
        reply->set_hostname(server->hostname);
        reply->set_port(server->port);
        reply->set_type(server->type);
        reply->set_clusterid(clusterID);
        reply->set_ismaster(true);

        log(INFO, "Sending `ServerInfo` for `GetServer` request");
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ID* request,
                    ServerInfo* reply) override {
        int clusterID = request->id();
        log(INFO,
            "Received `GetSlave` request for clutser " + to_string(clusterID));

        lock_guard<mutex> lock(routingTableMtx);
        node_ptr slave = getSlaveServer(clusterID);
        if (!slave) {
            log(INFO, "No slave server found");
            reply->set_serverid(-1);
        } else {
            reply->set_serverid(slave->serverID);
            reply->set_hostname(slave->hostname);
            reply->set_port(slave->port);
            reply->set_type(slave->type);
            reply->set_clusterid(clusterID);
            reply->set_ismaster(false);
        }

        log(INFO, "Sending `ServerInfo` from `GetSlave` request");
        return Status::OK;
    }

    Status GetFollowerServer(ServerContext* context, const ID* request,
                             ServerInfo* reply) override {
        int synchronizerID = request->id();
        int clusterID = (synchronizerID - 1) % 3 + 1;
        log(INFO, "Received `GetFollowerServer` request");

        lock_guard<mutex> lock(synchronizerMtx);
        node_ptr master = getMasterServer(clusterID);

        for (size_t i = 0; i < synchronizers[clusterID - 1].size(); ++i) {
            node_ptr synchronizer = synchronizers[clusterID - 1][i];

            if (synchronizer->serverID == synchronizerID) {
                node_ptr server = routingTable[clusterID - 1][i];
                reply->set_serverid(server->serverID);
                reply->set_hostname(server->hostname);
                reply->set_port(server->port);
                reply->set_clusterid(clusterID);
                reply->set_ismaster(server == master && server->isActive());
                break;
            }
        }
        return Status::OK;
    }

    Status GetFollowerServers(ServerContext* context, const ID* id,
                              ServerList* reply) override {
        int synchronizerID = id->id();
        log(INFO,
            "Received `GetAllFollowerServers` request from synchronizer " +
                to_string(synchronizerID));
        lock_guard<mutex> lock(synchronizerMtx);
        for (auto& cluster : synchronizers) {
            for (auto& synchronizer : cluster) {
                if (synchronizer->serverID == synchronizerID)
                    continue;
                reply->add_serverid(synchronizer->serverID);
                reply->add_hostname(synchronizer->hostname);
                reply->add_port(synchronizer->port);
                reply->add_type(synchronizer->type);
            }
        }
        log(INFO, "Sending `ServerList` from `GetAllFollowerServers` request");
        return Status::OK;
    }
};

void RunServer(string port_no) {
    std::thread hb(checkHeartbeat);

    string serverAddr("127.0.0.1:" + port_no);
    CoordServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(serverAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    cout << "Server listening on " << serverAddr << endl;
    log(INFO, "Server listening on " + serverAddr);

    server->Wait();
    hb.join();
}

int main(int argc, char** argv) {
    string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
        case 'p':
            port = optarg;
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    string logFileName = string("coordinator-") + port;
    google::InitGoogleLogging(logFileName.c_str());
    log(INFO, "Logging initialized");

    RunServer(port);
    return 0;
}

node_ptr findServer(int clusterID, int serverID, string type) {
    table servers = type == SERVER ? routingTable : synchronizers;
    for (auto& node : servers[clusterID - 1])
        if (node->serverID == serverID)
            return node;
    return nullptr;
}

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::now());
}

void checkHeartbeat() {
    while (true) {
        {
            lock_guard<mutex> lock(routingTableMtx);
            for (auto& c : routingTable) {
                for (auto& s : c) {
                    if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                        log(WARNING, "Missed heartbeat from server " +
                                         to_string(s->serverID));
                        s->missed_heartbeat = true;
                    }
                }
            }
        }
        {
            lock_guard<mutex> lock(synchronizerMtx);
            for (auto& c : synchronizers) {
                for (auto& s : c) {
                    if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                        log(WARNING, "Missed heartbeat from synchronizer " +
                                         to_string(s->serverID));
                        s->missed_heartbeat = true;
                    }
                }
            }
        }
        sleep_for(5s);
    }
}

bool zNode::isActive() { return !missed_heartbeat; }

node_ptr getMasterServer(int clusterID) {
    for (auto& s : routingTable[clusterID - 1]) {
        if (s->isActive())
            return s;
    }
    return nullptr;
}

node_ptr getSlaveServer(int clusterID) {
    if (routingTable[clusterID - 1].empty())
        return nullptr;
    node_ptr master = getMasterServer(clusterID);
    node_ptr slave = routingTable[clusterID - 1].back();
    return slave->serverID == master->serverID ? nullptr : slave;
}
