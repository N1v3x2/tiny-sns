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
#include <mutex>
#include <condition_variable>
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
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
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
using grpc::StatusCode;
using grpc::ClientContext;

const std::string USER_DIR = "./users/";
int server_id, cluster_id;

std::string format_log(std::string msg) {
    return "Server " + std::to_string(server_id) + " (Cluster " + std::to_string(cluster_id) + "): " + msg;
}

struct Client {
    std::string username;
    bool connected = true;
    bool has_update = false;
    std::map<std::string, Client*> client_followers;
    std::map<std::string, Client*> client_following;
    std::mutex update_mtx;
    std::condition_variable update_cv;
    // ServerReaderWriter<Message, Message>* stream = 0; // Unused for MP 2.1
    bool operator==(const Client& c1) const {
        return (username == c1.username);
    }
};

// Vector that stores every client that has been created
std::map<std::string, Client*> client_db;

class SNSServiceImpl final : public SNSService::Service {
    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        log(INFO, format_log("Received `List` request " + request->username()));

        for (auto& [uname, _] : client_db) {
            list_reply->add_all_users(uname);
        }

        Client* client = client_db[request->username()];
        for (auto& [follower_uname, _] : client->client_followers) {
            list_reply->add_followers(follower_uname);
        }

        log(INFO, format_log("Sending `List` to client..."));
        return Status::OK;
    }

    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string username = request->username();
        log(INFO, format_log("Received `Follow` request from client " + username));

        if (request->arguments_size() < 1) {
            reply->set_msg("Invalid command");
            return Status::OK;
        }

        std::string to_follow = request->arguments(0);

        if (!client_db.count(to_follow)) {
            reply->set_msg("Invalid username");
            return Status::OK;
        }
        Client* client = client_db[username];
        Client* client_to_follow = client_db[to_follow];
        
        if (client->client_following.count(to_follow) || client == client_to_follow) {
            reply->set_msg("Input username already exists");
            return Status::OK;
        }

        client->client_following[to_follow] = client_to_follow;
        client_to_follow->client_followers[username] = client;

        return Status::OK;
    }

    Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string username = request->username();
        log(INFO, format_log("Received `UnFollow` request from client " + username));

        if (request->arguments_size() < 1) {
            reply->set_msg("Invalid command");
            return Status::OK;
        }

        Client* client = client_db[username];

        std::string username2 = request->arguments(0);
        if (!client->client_following.count(username2)) {
            reply->set_msg("Invalid username");
            return Status::OK;
        }

        client->client_following.erase(username2);

        Client* to_unfollow = client_db[username2];
        to_unfollow->client_followers.erase(username);

        return Status::OK;
    }

    // RPC Login
    Status Login(ServerContext* context, const Request* request, Reply* reply) override {
        std::string username = request->username();
        log(INFO, format_log("Received `Login` request from client " + username));

        // Check whether the user already exists
        if (client_db.count(username)) {
            reply->set_msg("Username already exists");
            return Status::OK;
        }

        Client* client = new Client();
        client->username = username;
        client_db[username] = client;

        // Create user file so other users can buffer posts there
        mkdir(USER_DIR.data(), 0777);
        std::string user_file = USER_DIR + username + ".txt";
        std::fstream fs(user_file, std::ios::out);

        return Status::OK;
    }

    Status Timeline(ServerContext* context,
                    ServerReaderWriter<Message, Message>* stream) override {
        Message msg;
        Client* client = nullptr;
        std::string username, user_file;

        // Set the current user based on the first message received
        if (stream->Read(&msg)) {
            username = msg.username();
            user_file = USER_DIR + username + ".txt";
            client = client_db[username];
        } else {
            return Status::OK;
        }

        log(INFO, "Received `Timeline` request from client " + username);

        // Use background thread to monitor changes to the user's file
        std::thread monitor_posts([stream, user_file, client]() {
            std::fstream fs;
            struct stat sb;
            char fill;
            std::tm post_time {};
            std::string post_user, post_content, dummy;
            Message timeline_msg;

            while (true) {
                std::unique_lock<std::mutex> lock(client->update_mtx);
                client->update_cv.wait(lock, [client]{ return client->has_update; });
                // Holding client's update lock now
                
                fs.open(user_file, std::ios::in);
                std::vector<Message> msgs;

                // Read all buffered posts from the user's file
                while (fs.peek() != EOF) {
                    fs >> fill >> std::ws >> std::get_time(&post_time, "%a %b %d %H:%M:%S %Y")
                        >> fill >> post_user
                        >> fill >> std::ws;
                    std::getline(fs, post_content);
                    std::getline(fs, dummy);

                    if (!fs.good()) break;

                    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
                    timestamp->set_seconds(std::mktime(&post_time));
                    timestamp->set_nanos(0);
                    timeline_msg.set_allocated_timestamp(timestamp);
                    timeline_msg.set_username(post_user);
                    timeline_msg.set_msg(post_content);
                    msgs.push_back(timeline_msg);
                }

                log(INFO, format_log("User " + client->username + " has unread posts; sending latest 20 posts..."));

                // Only read up to 20 latest posts buffered in the file
                int n = msgs.size();
                for (int i = n - 1; i >= std::max(0, n - 21); --i) {
                    stream->Write(msgs[i]);
                }

                fs.close();
                fs.clear();

                client->has_update = false;
            }
        });
        
        std::fstream fs;
        while (stream->Read(&msg)) {
            // Write posts to all followers
            log(INFO, format_log("User " + client->username + " just posted; sending post to followers..."));
            for (auto& [follower_uname, follower] : client->client_followers) {
                std::string follower_file = USER_DIR + follower_uname + ".txt";

                std::lock_guard<std::mutex> lock(follower->update_mtx);
                fs.open(follower_file, std::ios::app);

                time_t curr_time = time(NULL);
                std::tm* t = gmtime(&curr_time);

                fs << "T " << asctime(t)
                    << "U " << username << '\n'
                    << "W " << msg.msg() << '\n';

                fs.close();
                fs.clear();

                follower->has_update = true;
                follower->update_cv.notify_all();
            }
        }

        monitor_posts.join();

        return Status::OK;
    }
};

// Background heartbeat thread function
void Heartbeat(
    std::string coord_ip,
    std::string coord_port,
    int cluster_id,
    int server_id,
    std::string port_no
) {
    const std::string hostname = "0.0.0.0";
    std::string coord_address = coord_ip + ":" + coord_port;
    std::string cluster_key = "clusterid";
    std::string cluster_id_str = std::to_string(cluster_id);

    std::unique_ptr<CoordService::Stub> stub_(CoordService::NewStub(
        grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials())
    ));
    
    ServerInfo info;
    info.set_serverid(server_id);
    info.set_hostname(hostname);
    info.set_port(port_no);
    info.set_type("server");

    Confirmation confirmation;

    // Send heartbeat every 5 seconds
    while (true) {
        ClientContext context;
        context.AddMetadata(cluster_key, cluster_id_str);

        log(INFO, format_log("Sending heartbeat to coordinator"));
        grpc::Status status = stub_->Heartbeat(&context, info, &confirmation);
        if (!status.ok() || !confirmation.status()) {
            log(ERROR, format_log("Heartbeat did not receive reply from coordinator"));
            std::terminate();
        }
        log(INFO, format_log("Received heartbeat confirmation from coordinator"));

        sleep(5);
    }
}

void RunServer(
    std::string coord_ip,
    std::string coord_port,
    int cluster_id,
    int server_id,
    std::string port_no
) {
    const std::string hostname = "0.0.0.0";
    std::string server_address = hostname + ":" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Server listening on " + server_address);

    // Create background thread to send heartbeats to coordinator
    std::thread heartbeat(Heartbeat, coord_ip, coord_port, cluster_id, server_id, port_no);
    heartbeat.detach();

    server->Wait();
}

int main(int argc, char** argv) {
    std::string coord_ip, coord_port;
    std::string port;

    if (argc < 6) {
        std::cout << "Expected 6 args, got " << argc << '\n';
        std::cout << "Usage: ./tsd -c <clusterId> -s <serverId> -h <coordinatorIP> -k <coordinatorPort> -p <portNum>\n";
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

    std::string log_file_name = std::string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, format_log("Logging initialized"));
    
    RunServer(coord_ip, coord_port, cluster_id, server_id, port);

    return 0;
}
