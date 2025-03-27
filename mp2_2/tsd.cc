/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ctime>
#include <time.h>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <map>
#include <algorithm>
#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::ClientContext;
using google::protobuf::Timestamp;
using std::string;
using std::to_string;
using std::vector;
using std::map;
using std::ifstream, std::ofstream;
using std::unique_ptr;
using std::cout, std::endl;

int server_id, cluster_id;
string cluster_dir, server_dir, all_users_file;
unique_ptr<CoordService::Stub> coord_stub;

struct Client {
    string username;
    map<string, Client*> followers;
    map<string, Client*> following;
    Client(string& uname) : username(uname) {}
    bool operator==(const Client& c1) const {
        return (username == c1.username);
    }
};

std::mutex client_mtx;
map<string, Client*> client_db;

ServerInfo GetSlaveInfo();
unique_ptr<SNSService::Stub> GetSlaveStub(string hostname, string port);
Message MakeMessage(const string& username, const string& msg);

class SNSServiceImpl final : public SNSService::Service {
    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        log(INFO, "Received `List` request " + request->username());

        std::lock_guard<std::mutex> lock(client_mtx);

        for (auto& [uname, _] : client_db)
            list_reply->add_all_users(uname);

        Client* client = client_db[request->username()];
        for (auto& [follower_uname, _] : client->followers)
            list_reply->add_followers(follower_uname);

        log(INFO, "Sending `List` to client...");
        return Status::OK;
    }

    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
        string username = request->username();
        log(INFO, "Received `Follow` request from client " + username);

        if (request->arguments_size() < 1) {
            reply->set_msg("Invalid command");
            return Status::OK;
        }
        string to_follow = request->arguments(0);

        {
            std::lock_guard<std::mutex> lock(client_mtx);

            if (!client_db.count(to_follow)) {
                reply->set_msg("Invalid username");
                return Status::OK;
            }
            Client* client = client_db[username];
            Client* client_to_follow = client_db[to_follow];

            if (client->following.count(to_follow) || client == client_to_follow) {
                reply->set_msg("Input username already exists");
                return Status::OK;
            }

            client->following[to_follow] = client_to_follow;
            client_to_follow->followers[username] = client;
        }

        // Write to files
        ofstream fs(server_dir + username + "_following.txt", std::ios::app);
        fs << to_follow << endl;
        fs.close();

        fs.open(server_dir + to_follow + "_followers.txt", std::ios::app);
        fs << username << endl;
        fs.close();

        ServerInfo slave_info = GetSlaveInfo();
        bool isMaster = slave_info.serverid() != server_id;

        // Master: replicate new user to slave
        if (isMaster) {
            unique_ptr<SNSService::Stub> slave_stub = GetSlaveStub(
                slave_info.hostname(), slave_info.port());

            ClientContext master_slave_ctx;
            Request req; req.set_username(username); req.add_arguments(to_follow);
            Reply rep;
            log(INFO, "Master: replicating Follow state to slave");
            slave_stub->Follow(&master_slave_ctx, req, &rep);
        }

        return Status::OK;
    }

    /* Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
        string username = request->username();
        log(INFO, "Received `UnFollow` request from client " + username);

        if (request->arguments_size() < 1) {
            reply->set_msg("Invalid command");
            return Status::OK;
        }

        Client* client = client_db[username];

        string username2 = request->arguments(0);
        if (!client->client_following.count(username2)) {
            reply->set_msg("Invalid username");
            return Status::OK;
        }

        client->client_following.erase(username2);

        Client* to_unfollow = client_db[username2];
        to_unfollow->client_followers.erase(username);

        return Status::OK;
    } */

    // RPC Login
    Status Login(ServerContext* context, const Request* request, Reply* reply) override {
        string username = request->username();
        log(INFO, "Received `Login` request from client " + username);

        {
            std::lock_guard<std::mutex> lock(client_mtx);

            // Check whether the user already exists
            if (client_db.count(username)) {
                reply->set_msg("Username already exists");
                return Status::OK;
            }

            client_db[username] = new Client(username);
        }

        // Create user files
        ofstream fs(server_dir + username + ".txt"); fs.close();
        fs.open(server_dir + username + "_following.txt"); fs.close();
        fs.open(server_dir + username + "_followers.txt"); fs.close();

        ServerInfo slave_info = GetSlaveInfo();
        bool isMaster = slave_info.serverid() != server_id;

        // Master: replicate to slave
        if (isMaster) {
            unique_ptr<SNSService::Stub> slave_stub = GetSlaveStub(
                slave_info.hostname(), slave_info.port());

            ClientContext master_slave_ctx;
            Request req; req.set_username(username);
            Reply rep;
            log(INFO, "Master: replicating Login state to slave");
            slave_stub->Login(&master_slave_ctx, req, &rep);
        }

        return Status::OK;
    }

    Status Timeline(ServerContext* context,
                    ServerReaderWriter<Message, Message>* stream) override {
        Message msg;
        Client* client = nullptr;
        string username, user_file;

        // Set the current user based on the first message received
        if (stream->Read(&msg)) {
            username = msg.username();
            user_file = server_dir + username + ".txt";
            std::lock_guard<std::mutex> lock(client_mtx);
            client = client_db[username];
        } else {
            return Status::OK;
        }

        log(INFO, "Received `Timeline` request from client " + username);

        // Use background thread to monitor changes to the user's file
        std::thread monitor_timeline([stream, user_file, client]() {
            ifstream fs;
            char fill;
            std::tm post_time{};
            string post_user, post_content, dummy;
            Message timeline_msg;

            time_t last_read = 0;

            while (true) {
                // TODO: synchronize access to user file
                fs.open(user_file);
                vector<Message> msgs;

                // Only read new posts (since last read)
                while (fs.peek() != ifstream::traits_type::eof()) {
                    fs >> fill >> std::ws >> std::get_time(&post_time, "%a %b %d %H:%M:%S %Y")
                       >> fill >> post_user
                       >> fill >> std::ws;
                    std::getline(fs, post_content);
                    std::getline(fs, dummy);

                    if (std::mktime(&post_time) <= last_read) break;

                    Timestamp* timestamp = new Timestamp();
                    timestamp->set_seconds(std::mktime(&post_time));
                    timestamp->set_nanos(0);

                    timeline_msg.set_allocated_timestamp(timestamp);
                    timeline_msg.set_username(post_user);
                    timeline_msg.set_msg(post_content);
                    msgs.push_back(timeline_msg);
                }

                last_read = time(nullptr);

                if (!msgs.empty()) {
                    log(INFO,
                        "User " +
                        client->username +
                        " has unread posts; sending latest posts...");
                    for (int i = (int)msgs.size() - 1; i >= 0; --i)
                        stream->Write(msgs[i]);
                }

                fs.close();
                fs.clear();
                sleep(5);
            }
        });

        // Slave replication
        ServerInfo slave_info = GetSlaveInfo();
        bool isMaster = slave_info.serverid() != server_id;

        std::shared_ptr<grpc::ClientReaderWriter<Message, Message>> slave_stream;
        ClientContext master_slave_ctx;

        if (isMaster) {
            unique_ptr<SNSService::Stub> slave_stub = GetSlaveStub(
                slave_info.hostname(), slave_info.port());
            slave_stream = slave_stub->Timeline(&master_slave_ctx);

            // Establish timeline connection to slave
            log(INFO, "Master: establishing timeline connection to slave");
            string conn_msg = "Timeline";
            slave_stream->Write(MakeMessage(username, conn_msg));
        }

        ofstream fs;
        while (stream->Read(&msg)) {
            // Write posts to all followers
            log(INFO, "User " + client->username + " just posted; sending post to followers...");

            std::lock_guard<std::mutex> lock(client_mtx);
            for (auto& [follower_uname, follower] : client->followers) {
                string follower_file = server_dir + follower_uname + ".txt";

                fs.open(follower_file, std::ios::app);

                time_t curr_time = time(NULL);
                std::tm* t = gmtime(&curr_time);

                fs << "T " << asctime(t)
                   << "U " << username << '\n'
                   << "W " << msg.msg() << '\n';

                fs.close();
                fs.clear();

                // Master: replicate to slave
                if (isMaster) {
                    log(INFO, "Master: replicating timeline post to slave");
                    slave_stream->Write(msg);
                }
            }
        }

        monitor_timeline.join();

        return Status::OK;
    }
};

ServerInfo GetSlaveInfo() {
    ClientContext master_slave_ctx;
    ID id; id.set_id(cluster_id);
    ServerInfo slave_info;
    coord_stub->GetSlave(&master_slave_ctx, id, &slave_info);
    return slave_info;
}

unique_ptr<SNSService::Stub> GetSlaveStub(string hostname, string port) {
    string slave_addr = hostname + ":" + port;
    return SNSService::NewStub(
        grpc::CreateChannel(slave_addr, grpc::InsecureChannelCredentials()));
}

Message MakeMessage(const string& username, const string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}

// Background heartbeat thread function
void Heartbeat(
    string coord_ip,
    string coord_port,
    string port_no
) {
    const string hostname = "0.0.0.0";
    string coord_address = coord_ip + ":" + coord_port;

    ServerInfo info;
    info.set_serverid(server_id);
    info.set_hostname(hostname);
    info.set_port(port_no);
    info.set_type("server");
    info.set_clusterid(cluster_id);

    Confirmation confirmation;

    // Send heartbeat every 5 seconds
    while (true) {
        ClientContext context;

        log(INFO, "Sending heartbeat to coordinator");
        grpc::Status status = coord_stub->Heartbeat(&context, info, &confirmation);
        if (!status.ok() || !confirmation.status()) {
            log(ERROR, "Heartbeat did not receive reply from coordinator");
            std::terminate();
        }
        log(INFO, "Received heartbeat confirmation from coordinator");

        sleep(5);
    }
}

void UpdateClientDB() {
    ifstream fs;
    string username, follower, following;
    while (true) {
        sleep(5);

        std::lock_guard<std::mutex> lock(client_mtx);

        // Update list of users
        fs.open(all_users_file);
        while (fs.peek() != ifstream::traits_type::eof()) {
            getline(fs, username);
            if (!client_db.count(username))
                client_db[username] = new Client(username);
        }
        fs.close();

        // Update follower & following information
        for (auto& [uname, user] : client_db) {
            fs.open(server_dir + uname + "_following.txt");
            while (fs.peek() != ifstream::traits_type::eof()) {
                getline(fs, following);
                if (!user->following.count(following))
                    user->following[following] = client_db[following];
            }
            fs.close();

            fs.open(server_dir + uname + "_followers.txt");
            while (fs.peek() != ifstream::traits_type::eof()) {
                getline(fs, follower);
                if (!user->followers.count(follower))
                    user->followers[follower] = client_db[follower];
            }
        }
    }
}

void RunServer(
    string coord_ip,
    string coord_port,
    string port_no
) {
    const string hostname = "0.0.0.0";
    string server_address = hostname + ":" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    unique_ptr<Server> server(builder.BuildAndStart());
    cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Server listening on " + server_address);

    // Establish connection to coordinator
    string coord_address = coord_ip + ":" + coord_port;
    coord_stub = CoordService::NewStub(
        grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials()));

    // Backtrack thread to update `client_db`
    std::thread update_client_db(UpdateClientDB);

    // Create background thread to send heartbeats to coordinator
    std::thread heartbeat(Heartbeat, coord_ip, coord_port, port_no);

    server->Wait();

    update_client_db.join();
    heartbeat.join();
}

int main(int argc, char** argv) {
    string coord_ip, coord_port;
    string port;

    if (argc < 6) {
        cout << "Expected 6 args, got " << argc << '\n';
        cout << "Usage: ./tsd -c <clusterId> -s <serverId> -h <coordinatorIP> -k <coordinatorPort> -p <portNum>\n";
        return 0;
    }

    int opt = 0;
    while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
        switch (opt) {
            case 'c':
                cluster_id = atoi(optarg);
                break;
            case 's':
                server_id = atoi(optarg);
                break;
            case 'h':
                coord_ip = optarg;
                break;
            case 'k':
                coord_port = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    string log_file_name = string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging initialized");

    cluster_dir = "./cluster" + to_string(cluster_id) + "/";
    server_dir = cluster_dir + to_string(server_id) + "/";
    mkdir(cluster_dir.data(), 0777);
    mkdir(server_dir.data(), 0777);

    all_users_file = server_dir + "all_users.txt";
    ofstream fs(all_users_file); fs.close();

    RunServer(coord_ip, coord_port, port);

    return 0;
}
