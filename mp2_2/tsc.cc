#include <glog/logging.h>
#include <grpc++/grpc++.h>
#include <google/protobuf/util/time_util.h>
#include <unistd.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sstream>
#include <stdexcept>

#include "client.h"
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::CoordService;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using google::protobuf::util::TimeUtil;
using std::string;
using std::cout;
using std::unique_ptr;

void sig_ignore(int sig) {
    cout << "Signal caught " << sig;
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

class Client : public IClient {
   public:
    Client(string coord_ip, string coord_port, string uname)
        : username(uname) {
        unique_ptr<CoordService::Stub> coord_stub_(CoordService::NewStub(
            grpc::CreateChannel(coord_ip + ":" + coord_port, grpc::InsecureChannelCredentials())
        ));

        ClientContext context;
        ID client_id;
        client_id.set_id(std::stoi(uname));
        ServerInfo info;

        grpc::Status status = coord_stub_->GetServer(&context, client_id, &info);
        if (!status.ok()) {
            throw std::runtime_error("Failed to establish connection with coordinator");
        }

        hostname = info.hostname();
        port = info.port();
    }

   protected:
    virtual int connectTo();
    virtual IReply processCommand(string& input);
    virtual void processTimeline();

   private:
    string hostname;
    string port;
    string username;

    unique_ptr<SNSService::Stub> stub_;

    IReply Login();
    IReply List();
    IReply Follow(const string& username);
    IReply UnFollow(const string& username);
    void Timeline(const string& username);
};

int Client::connectTo() {
    stub_ = SNSService::NewStub(
        grpc::CreateChannel(hostname + ":" + port, grpc::InsecureChannelCredentials()));
    if (!stub_) return -1;

    IReply status = Login();
    if (!status.grpc_status.ok()) return -1;

    return 1;
}

IReply Client::processCommand(string& input) {
    IReply ire;
    string cmd;
    std::stringstream ss(input);
    ss >> cmd;

    if (cmd == "FOLLOW") {
        string toFollow;
        if (ss >> toFollow) {
            ire = Follow(toFollow); 
        } else {
            ire.grpc_status = Status::OK;
            ire.comm_status = FAILURE_INVALID;
        }
    } else if (cmd == "UNFOLLOW") {
        string toUnfollow; ss >> toUnfollow;
        ire = UnFollow(toUnfollow); 
    } else if (cmd == "LIST") {
        ire = List();
    } else if (cmd == "TIMELINE") {
        ire = List();
    } else {
        ire.grpc_status = Status::OK;
        ire.comm_status = FAILURE_INVALID;
    }

    return ire;
}

void Client::processTimeline() {
    Timeline(username);
}

IReply Client::List() {
    IReply ire;

    ClientContext context;
    Request request;
    request.set_username(username);
    ListReply reply;

    log(INFO, "Client " + username + ": requesting `List` from server");
    ire.grpc_status = stub_->List(&context, request, &reply);

    if (ire.grpc_status.ok()) {
        log(INFO, "Client " + username + ": received `List` from server");
        ire.comm_status = SUCCESS;
        // Deserialize the user information
        for (int i = 0; i < reply.all_users_size(); ++i) {
            ire.all_users.push_back(reply.all_users(i));
        }
        for (int i = 0; i < reply.followers_size(); ++i) {
            ire.followers.push_back(reply.followers(i));
        }
    } else {
        log(ERROR, "Client " + username + ": did not receive `List` reply from server");
    }

    return ire;
}

IReply Client::Follow(const string& username2) {
    IReply ire;

    ClientContext context;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Reply reply;

    log(INFO, "Client " + username + ": requesting `Follow` from server");
    ire.grpc_status = stub_->Follow(&context, request, &reply);
    if (ire.grpc_status.ok()) {
        log(INFO, "Client " + username + ": received `Follow` reply");
        string msg = reply.msg();
        if (msg == "Invalid command") {
            ire.comm_status = FAILURE_INVALID;
        } else if (msg == "Invalid username") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
        } else if (msg == "Input username already exists") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
        } else {
            ire.comm_status = SUCCESS;
        }
    } else {
        log(ERROR, "Client " + username + ": did not receive `Follow` reply from server");
    }

    return ire;
}

IReply Client::UnFollow(const string& username2) {
    IReply ire;

    ClientContext context;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Reply reply;

    log(INFO, "Client " + username + ": requesting `UnFollow` from server");
    ire.grpc_status = stub_->UnFollow(&context, request, &reply);
    if (ire.grpc_status.ok()) {
        log(INFO, "Client " + username + ": received `UnFollow` reply");
        string msg = reply.msg();
        if (msg == "Invalid command") {
            ire.comm_status = FAILURE_INVALID;
        } else if (msg == "Invalid username") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
        } else {
            ire.comm_status = SUCCESS;
        }
    } else {
        log(ERROR, "Client " + username + ": did not receive `UnFollow` reply from server");
    }

    return ire;
}

IReply Client::Login() {
    IReply ire;

    ClientContext context;
    Request request;
    request.set_username(username);
    Reply reply;

    log(INFO, "Client " + username + ": requesting `Login` from server");
    ire.grpc_status = stub_->Login(&context, request, &reply);
    if (ire.grpc_status.ok()) {
        log(INFO, "Client " + username + ": received `Login` reply");
        if (reply.msg() == "Username already exists") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
        } else {
            ire.comm_status = SUCCESS;
        }
    } else {
        log(ERROR, "Client " + username + ": did not receive `Login` reply from server");
    }

    return ire;
}

void Client::Timeline(const string& username) {
    ClientContext context;
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
        stub_->Timeline(&context));

    string newTimelineMsg = "Timeline";
    Message newTimeline = MakeMessage(username, newTimelineMsg);

    log(INFO, "Client " + username + ": requesting timeline from server");
    stream->Write(newTimeline);

    // Process incoming posts
    std::thread reader([stream, username]() {
        Message incoming_post;
        while (stream->Read(&incoming_post)) {
            log(INFO, "Client " + username + ": received post on timeline");
            time_t time = TimeUtil::TimestampToTimeT(incoming_post.timestamp());
            displayPostMessage(
                incoming_post.username(),
                incoming_post.msg(),
                time
            );
        }
    });

    // Write posts
    while (true) {
        string msg = getPostMessage();
        Message new_post = MakeMessage(username, msg);
        log(INFO, "Client " + username + ": writing post to timeline");
        stream->Write(new_post);
    }
}

int main(int argc, char** argv) {
    string coord_ip, coord_port;
    string username;

    if (argc < 4) {
        cout << "Expected 4 args, got " << argc << '\n';
        cout << "Usage: ./tsc -h <coordinatorIP> -k <coordinatorPort> -u <userId>\n";
        return 0;
    }

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:k:")) != -1) {
        switch (opt) {
            case 'h':
                coord_ip = optarg;
                break;
            case 'u':
                username = optarg;
                break;
            case 'k':
                coord_port = optarg;
                break;
            default:
                cout << "Invalid Command Line Argument\n";
        }
    }

    string log_file_name = string("client-") + username;
    google::InitGoogleLogging(log_file_name.c_str());

    log(INFO, "Client " + username + ": Logging initialized");

    Client myc(coord_ip, coord_port, username);

    myc.run();

    return 0;
}
