#include <algorithm>
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
#include <filesystem>
#include <fstream>
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

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using google::INFO;
using google::WARNING;
using google::ERROR;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;

struct zNode {
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
};

// potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = { cluster1, cluster2, cluster3 };


// func declarations
zNode* findServer(const std::vector<zNode*>& v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


zNode* findServer(const std::vector<zNode*>& v, int id) {
    for (auto& node : v) {
        if (node->serverID == id) return node;
    }
    return nullptr;
}

bool zNode::isActive(){
    bool status = false;
    if (!missed_heartbeat){
        status = true;
    } else if (difftime(getTimeNow(), last_heartbeat) < 10) {
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Your code here
        int server_id = serverinfo->serverid();
        auto it = context->client_metadata().find("clusterid");

        // The heartbeat message must include the server's cluster ID
        if (it == context->client_metadata().end()) {
            confirmation->set_status(false);
            log(ERROR, "Coordinator: received heartbeat from server without cluster ID");
            return Status::OK;
        }
        int cluster_id = atoi(it->second.data());

        log(INFO, "Coordinator: received heartbeat from server " + std::to_string(server_id) + " in cluster " + std::to_string(cluster_id));

        std::vector<zNode*>& cluster = clusters[cluster_id - 1];
        zNode* server = findServer(cluster, server_id);

        if (!server) { 
            // Register server after its first heartbeat
            zNode* newserver = new zNode();
            newserver->serverID = server_id;
            newserver->hostname = serverinfo->hostname();
            newserver->port = serverinfo->port();
            newserver->type = serverinfo->type();
            newserver->last_heartbeat = getTimeNow();
            newserver->missed_heartbeat = false;
            cluster.push_back(newserver);
        } else {
            // If server is already registered, simply record the heartbeat
            server->last_heartbeat = getTimeNow();
            server->missed_heartbeat = false;
        }

        confirmation->set_status(true);
        log(INFO, "Coordinator: sending heartbeat confirmation to server " + std::to_string(server_id) + " at cluster " + std::to_string(cluster_id));
        return Status::OK;
    }

    // function returns the server information for requested client id
    // this function assumes there are always 3 clusters and has math
    // hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        // Your code here
        int client_id = id->id();
        log(INFO, "Coordinator: received `GetServer` request from client " + client_id);

        // MP 2.1: assume each cluster only has 1 server
        int cluster_id = (client_id - 1) % 3 + 1;
        
        // Retrieve the client's server based on the calculation above
        zNode* server = clusters[cluster_id - 1][0];

        serverinfo->set_serverid(server->serverID);
        serverinfo->set_hostname(server->hostname);
        serverinfo->set_port(server->port);
        serverinfo->set_type(server->type);

        log(INFO, "Coordinator: sending `ServerInfo` to client " + client_id);
        return Status::OK;
    }
};

void RunServer(std::string port_no){
    // start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
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
    std::cout << "Server listening on " << server_address << std::endl;

    log(INFO, "Coordinator: Server listening on " + server_address);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "3010";
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

    std::string log_file_name = std::string("coordinator-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Coordinator: Logging initialized");

    RunServer(port);
    return 0;
}

void checkHeartbeat() {
    while (true) {
        // check each server for a heartbeat in the last 10 seconds
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters) {
            for (auto& s : c) {
                if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                    log(WARNING, "Coordinator: missed heartbeat from server " + std::to_string(s->serverID));
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
                    }
                }
            }
        }

        v_mutex.unlock();
        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

