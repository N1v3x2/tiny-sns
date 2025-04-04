#include <cstdio>
#include <ctime>

#include <glog/logging.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

using namespace std::chrono_literals;
using namespace std::this_thread;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::Confirmation;
using csce438::ID;
using std::vector;
using std::string;
using std::to_string;
using std::cout;
using std::endl;
using std::mutex;
using std::lock_guard;

struct zNode {
    int serverID;
    string hostname;
    string port;
    string type;
    time_t last_heartbeat;
    bool missed_heartbeat;
    zNode(int s, string h, string p, string t, time_t l, bool m) :
        serverID(s), hostname(h), port(p), type(t), last_heartbeat(l), missed_heartbeat(m)
    {}
    bool isActive();
};

const string SERVER = "server", SYNC = "synchronizer";
using table = vector<vector<zNode*>>;
table routingTable(3, vector<zNode*>()), synchronizers(3, vector<zNode*>());
mutex routingTableMtx, synchronizerMtx;

zNode* findServer(int clusterId, int serverId, string type); 
zNode* findMaster(int clusterId);
std::time_t getTimeNow();
void checkHeartbeat();

class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* request, Confirmation* reply) override {
        int serverID = request->serverid();
        int clusterID = request->clusterid();
        string type = request->type();

        log(INFO,
            "Received heartbeat from server " +
            to_string(serverID) + " in cluster " + to_string(clusterID));

        zNode* server = findServer(clusterID - 1, serverID, type);

        if (!server) { 
            // Register server after its first heartbeat
            zNode* newserver = new zNode(
                serverID,
                request->hostname(),
                request->port(),
                request->type(),
                getTimeNow(),
                false);

            if (newserver->type == SERVER) {
                lock_guard<mutex> lock(routingTableMtx);
                routingTable[clusterID - 1].push_back(newserver);
            } else if (newserver->type == SYNC) {
                lock_guard<mutex> lock(synchronizerMtx);
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
        log(INFO, "Sending heartbeat confirmation to server " + to_string(serverID) + " at cluster " + to_string(clusterID));
        return Status::OK;
    }

    // Return the master
    Status GetServer(ServerContext* context, const ID* request, ServerInfo* reply) override {
        int clientId = request->id();
        int clusterId = (clientId - 1) % 3 + 1;
        log(INFO,
            "Received `GetServer` request from client " +
            to_string(clientId));

        // First active server in the routing table is the master
        lock_guard<mutex> lock(routingTableMtx);
        zNode* server = findMaster(clusterId);
        reply->set_serverid(server->serverID);
        reply->set_hostname(server->hostname);
        reply->set_port(server->port);
        reply->set_type(server->type);
        reply->set_clusterid(clusterId);
        reply->set_ismaster(true);

        log(INFO, "Sending `ServerInfo` for `GetServer` request");
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ID* request, ServerInfo* reply) override {
        int cluster_id = request->id();
        log(INFO, "Received `GetSlave` request for clutser " + to_string(cluster_id));

        lock_guard<mutex> lock(routingTableMtx);
        zNode* slave = routingTable[cluster_id - 1].back();
        reply->set_serverid(slave->serverID);
        reply->set_hostname(slave->hostname);
        reply->set_port(slave->port);
        reply->set_type(slave->type);
        reply->set_clusterid(cluster_id);
        reply->set_ismaster(false);

        log(INFO, "Sending `ServerInfo` from `GetSlave` request");
        return Status::OK;
    }

    Status GetFollowerServer(ServerContext* context, const ID* request, ServerInfo* reply) override {
        int syncId = request->id();
        int clusterId = (syncId - 1) % 3 + 1;

        lock_guard<mutex> lock(synchronizerMtx);
        zNode* master = routingTable[clusterId - 1].front();

        for (size_t i = 0; i < synchronizers[clusterId - 1].size(); ++i) {
            zNode* sync = synchronizers[clusterId - 1][i];
            if (sync->serverID == syncId) {
                zNode* server = routingTable[clusterId - 1][i];
                reply->set_serverid(server->serverID);
                reply->set_hostname(server->hostname);
                reply->set_port(server->port);
                reply->set_clusterid(clusterId);
                reply->set_ismaster((server == master && master->isActive()) ||
                                    (!master->isActive() && server->isActive()));
                break;
            }
        }
        return Status::OK;
    }

    Status GetFollowerServers(ServerContext* context, const ID* id, ServerList* reply) override {
        int syncId = id->id();
        log(INFO, "Received `GetAllFollowerServers` request from synchronizer " +
            to_string(syncId));
        lock_guard<mutex> lock(synchronizerMtx);
        for (auto& cluster : synchronizers) {
            for (auto& syncer : cluster) {
                if (syncer->serverID == syncId) continue;
                reply->add_serverid(syncer->serverID);
                reply->add_hostname(syncer->hostname);
                reply->add_port(syncer->port);
                reply->add_type(syncer->type);
            }
        }
        log(INFO, "Sending `ServerList` from `GetAllFollowerServers` request");
        return Status::OK;
    }
};

void RunServer(string port_no) {
    // start thread to check heartbeats
    std::thread hb(checkHeartbeat);

    string serverAddr("127.0.0.1:" + port_no);
    CoordServiceImpl service;

    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;

    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(serverAddr, grpc::InsecureServerCredentials());

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);

    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());

    cout << "Server listening on " << serverAddr << endl;
    log(INFO, "Server listening on " + serverAddr);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
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

zNode* findServer(int clusterId, int serverId, string type) {
    table servers = type == SERVER ? routingTable : synchronizers;
    for (auto& node : servers[clusterId])
        if (node->serverID == serverId) return node;
    return nullptr;
}

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

void checkHeartbeat() {
    while (true) {
        sleep_for(5s);

        // check each server for a heartbeat in the last 10 seconds
        {
            lock_guard<mutex> lock(routingTableMtx);
            for (auto& c : routingTable) {
                for (auto& s : c) {
                    if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                        log(WARNING, "Missed heartbeat from server " + to_string(s->serverID));
                        if (!s->missed_heartbeat) {
                            s->missed_heartbeat = true;
                        }
                    }
                }
            }
        }
        {
            lock_guard<mutex> lock(synchronizerMtx);
            for (auto& c : synchronizers) {
                for (auto& s : c) {
                    if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                        log(WARNING, "Missed heartbeat from synchronizer " + to_string(s->serverID));
                        if (!s->missed_heartbeat) {
                            s->missed_heartbeat = true;
                        }
                    }
                }
            }
        }
    }
}

bool zNode::isActive() {
    return !missed_heartbeat || difftime(getTimeNow(), last_heartbeat) < 10;
}

zNode* findMaster(int clusterId) {
    for (auto& s : routingTable[clusterId - 1]) {
        if (s->isActive()) return s;
    }
    return nullptr;
}
