#include <cstdio>
#include <ctime>

#include <glog/logging.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
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
using std::string, std::to_string;
using std::cout, std::endl;

struct zNode {
    int serverID;
    string hostname;
    string port;
    string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
};

const string SERVER = "server", SYNCER = "syncer";

std::mutex v_mutex;

using table = vector<vector<zNode*>>;
table routing_table(3, vector<zNode*>());
table follower_syncers(3, vector<zNode*>());


zNode* findServer(int cluster_id, int server_id); 
std::time_t getTimeNow();
void checkHeartbeat();


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Your code here
        int server_id = serverinfo->serverid();
        int cluster_id = serverinfo->clusterid();

        log(INFO,
            "Received heartbeat from server " +
            to_string(server_id) + " in cluster " + to_string(cluster_id));

        zNode* server = findServer(cluster_id - 1, server_id);

        if (!server) { 
            // Register server after its first heartbeat
            zNode* newserver = new zNode();
            newserver->serverID = server_id;
            newserver->hostname = serverinfo->hostname();
            newserver->port = serverinfo->port();
            newserver->type = serverinfo->type();
            newserver->last_heartbeat = getTimeNow();
            newserver->missed_heartbeat = false;

            if (newserver->type == SERVER)
                routing_table[cluster_id - 1].push_back(newserver);
            else if (newserver->type == SYNCER)
                follower_syncers[cluster_id - 1].push_back(newserver);
            else {
                log(WARNING, "Unknown server type attempted registration");
                confirmation->set_status(false);
                return Status::OK;
            }
        } else {
            // If server is already registered, simply record the heartbeat
            server->last_heartbeat = getTimeNow();
            server->missed_heartbeat = false;
        }

        confirmation->set_status(true);
        log(INFO, "Sending heartbeat confirmation to server " + to_string(server_id) + " at cluster " + to_string(cluster_id));
        return Status::OK;
    }

    // Return the master
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        // Your code here
        int client_id = id->id();
        log(INFO,
            "Received `GetServer` request from client " +
            to_string(client_id));

        int cluster_id = (client_id - 1) % 3 + 1;

        // First active server in the routing table is the master
        zNode* server;
        for (auto& s : routing_table[cluster_id - 1]) {
            if (s->isActive()) {
                server = s;
                break;
            }
        }

        serverinfo->set_serverid(server->serverID);
        serverinfo->set_hostname(server->hostname);
        serverinfo->set_port(server->port);
        serverinfo->set_type(server->type);
        serverinfo->set_clusterid(cluster_id);
        serverinfo->set_ismaster(true);

        log(INFO, "Sending `ServerInfo` for `GetServer` request");
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int cluster_id = id->id();
        log(INFO, "Received `GetSlave` request for clutser " + to_string(cluster_id));

        zNode* slave = routing_table[cluster_id - 1].back();

        serverinfo->set_serverid(slave->serverID);
        serverinfo->set_hostname(slave->hostname);
        serverinfo->set_port(slave->port);
        serverinfo->set_type(slave->type);
        serverinfo->set_clusterid(cluster_id);
        serverinfo->set_ismaster(false);

        log(INFO, "Sending `ServerInfo` for `GetSlave` request");
        return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext* context, const ID* id, ServerList* serverlist) override {
        throw new std::logic_error("Unimplemented");
    }

    Status GetFollowerServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        throw new std::logic_error("Unimplemented");
    }
};

void RunServer(string port_no){
    // start thread to check heartbeats
    std::thread hb(checkHeartbeat);

    string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl service;

    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;

    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);

    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());

    cout << "Server listening on " << server_address << endl;
    log(INFO, "Server listening on " + server_address);

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

    string log_file_name = string("coordinator-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging initialized");

    RunServer(port);
    return 0;
}

zNode* findServer(int cluster_id, int server_id) {
    for (auto& node : routing_table[cluster_id])
        if (node->serverID == server_id) return node;
    return nullptr;
}

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

void checkHeartbeat() {
    while (true) {
        // check each server for a heartbeat in the last 10 seconds
        std::lock_guard<std::mutex> lock(v_mutex);

        for (auto& c : routing_table) {
            for (auto& s : c) {
                if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                    log(WARNING, "Missed heartbeat from server " + to_string(s->serverID));
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
                    }
                }
            }
        }

        sleep(3);
    }
}

bool zNode::isActive() {
    return !missed_heartbeat || difftime(getTimeNow(), last_heartbeat) < 10;
}

