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
    bool isActive() { return !missed_heartbeat; }
};

using node_ptr = shared_ptr<zNode>;
using table = vector<vector<node_ptr>>;

const string SERVER = "server";
const string SYNCHRONIZER = "synchronizer";

/**
 * @brief Routing table, represented as a 2D vector:
 * - First dimension: cluster ID
 * - Second dimension: list of pointers to `zNode` instances containing each
 * server's information
 */
table routingTable{3, vector<node_ptr>()};
mutex routingTableMtx;

/**
 * @brief Synchronizer table, represented as a 2D vector:
 * - First dimension: cluster ID
 * - Second dimension: list of pointers to `zNode` instances containing each
 * follower synchronizer's information
 */
table synchronizers{3, vector<node_ptr>()};
mutex synchronizerMtx;

/**
 * @brief Attempts to find a server that matches the parameters
 *
 * @param clusterID The cluster ID to search
 * @param serverID The server ID to look for
 * @param type The type of server to look for ("server" or "synchronizer")
 * @return A `node_ptr` to the server found or `nullptr` if it doesn't exist
 */
node_ptr findServer(int clusterID, int serverID, string type);

/**
 * @brief Get the master server in the cluster
 *
 * @param clusterID The cluster to search
 * @return A `node_ptr` to the master server or `nullptr` if none
 */
node_ptr getMasterServer(int clusterID);

/**
 * @brief Get the slave server in the cluster (assumes that a cluster has <= 2
 * servers)
 *
 * @param clusterID The cluster to search
 * @return A `node_ptr` to the slave server or `nullptr` if none
 */
node_ptr getSlaveServer(int clusterID);

/**
 * @brief Get the current time
 */
std::time_t getTimeNow();

/**
 * @brief Periodically checks heartbeats in `routingTable` and `synchronizers`
 * (every 5 seconds); used by background thread
 */
void checkHeartbeats();

class CoordServiceImpl final : public CoordService::Service {

    /**
     * @brief (RPC) Updates the client's heartbeat
     *
     * @param context The request context
     * @param request The client request containing client information
     * @param reply The reply to send to the client
     * @return RPC status
     */
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
            } else if (newserver->type == SYNCHRONIZER) {
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

    /**
     * @brief (RPC) Gets the master server in the cluster
     *
     * @param context The request context
     * @param request The client request containing the cluster ID
     * @param reply The reply containing information about the cluster's master
     * server
     * @return RPC status
     */
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

    /**
     * @brief (RPC) Gets the slave server in the cluster
     *
     * @param context The request context
     * @param request The client request containing the cluster ID
     * @param reply The reply containing information about the cluster's slave
     * server. Note: if there is no slave in the cluster, the reply's `serverid`
     * is set to -1
     * @return RPC status
     */
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

    /**
     * @brief (RPC) Gets the server corresponding to the follower synchronizer
     *
     * @param context The server context
     * @param request The client request containing the follower synchronizer ID
     * @param reply Server information about the server corresponding to the
     * client's synchronizer ID
     * @return RPC status
     */
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
};

/**
 * @brief Starts the coordinator; blocks to wait for RPCs
 *
 * @param port_no The port to connect to
 */
void RunServer(string port_no) {
    std::thread hb(checkHeartbeats);

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

void checkHeartbeats() {
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
