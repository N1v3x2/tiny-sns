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

void sig_ignore(int sig) {
    std::cout << "Signal caught " << sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
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
    Client(std::string coord_ip, std::string coord_port, std::string uname)
        : username(uname) {
        // Establish connection with coordinator
        std::unique_ptr<CoordService::Stub> coord_stub_(CoordService::NewStub(
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
    virtual IReply processCommand(std::string& input);
    virtual void processTimeline();

   private:
    std::string hostname;
    std::string port;
    std::string username;

    // You can have an instance of the client stub
    // as a member variable.
    std::unique_ptr<SNSService::Stub> stub_;

    IReply Login();
    IReply List();
    IReply Follow(const std::string& username);
    IReply UnFollow(const std::string& username);
    void Timeline(const std::string& username);
};

///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo() {
    // ------------------------------------------------------------
    // In this function, you are supposed to create a stub so that
    // you call service methods in the processCommand/porcessTimeline
    // functions. That is, the stub should be accessible when you want
    // to call any service methods in those functions.
    // Please refer to gRpc tutorial how to create a stub.
    // ------------------------------------------------------------

    ///////////////////////////////////////////////////////////
    stub_ = SNSService::NewStub(
        grpc::CreateChannel(hostname + ":" + port, grpc::InsecureChannelCredentials())
    );
    if (!stub_) return -1;

    IReply status = Login();
    if (!status.grpc_status.ok() || status.comm_status == FAILURE_ALREADY_EXISTS) return -1;

    //////////////////////////////////////////////////////////

    return 1;
}

IReply Client::processCommand(std::string& input) {
    // ------------------------------------------------------------
    // GUIDE 1:
    // In this function, you are supposed to parse the given input
    // command and create your own message so that you call an
    // appropriate service method. The input command will be one
    // of the followings:
    //
    // FOLLOW <username>
    // UNFOLLOW <username>
    // LIST
    // TIMELINE
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // GUIDE 2:
    // Then, you should create a variable of IReply structure
    // provided by the client.h and initialize it according to
    // the result. Finally you can finish this function by returning
    // the IReply.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // HINT: How to set the IReply?
    // Suppose you have "FOLLOW" service method for FOLLOW command,
    // IReply can be set as follow:
    //
    //     // some codes for creating/initializing parameters for
    //     // service method
    //     IReply ire;
    //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
    //     ire.grpc_status = status;
    //     if (status.ok()) {
    //         ire.comm_status = SUCCESS;
    //     } else {
    //         ire.comm_status = FAILURE_NOT_EXISTS;
    //     }
    //
    //      return ire;
    //
    // IMPORTANT:
    // For the command "LIST", you should set both "all_users" and
    // "following_users" member variable of IReply.
    // ------------------------------------------------------------

    IReply ire;
    std::string cmd;
    std::stringstream ss(input);
    ss >> cmd;

    if (cmd == "FOLLOW") {
        std::string toFollow;
        if (ss >> toFollow) {
            ire = Follow(toFollow); 
        } else {
            ire.grpc_status = Status::OK;
            ire.comm_status = FAILURE_INVALID;
        }
    } else if (cmd == "UNFOLLOW") {
        std::string toUnfollow; ss >> toUnfollow;
        ire = UnFollow(toUnfollow); 
    } else if (cmd == "LIST") {
        ire = List();
    } else if (cmd == "TIMELINE") {
        ire = List(); // Just use `list` to check whether server is running
    } else {
        ire.grpc_status = Status::OK;
        ire.comm_status = FAILURE_INVALID;
    }

    return ire;
}

void Client::processTimeline() {
    Timeline(username);
}

// List Command
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

// Follow Command
IReply Client::Follow(const std::string& username2) {
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
        std::string msg = reply.msg();
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

// UNFollow Command
IReply Client::UnFollow(const std::string& username2) {
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
        std::string msg = reply.msg();
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

// Login Command
IReply Client::Login() {
    IReply ire;

    // Login
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

// Timeline Command
void Client::Timeline(const std::string& username) {
    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // in client.cc file for both getting and displaying messages
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
    ClientContext context;
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
        stub_->Timeline(&context)
    );

    std::string newTimelineMsg = "Timeline";
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
        std::string msg = getPostMessage();
        Message new_post = MakeMessage(username, msg);
        log(INFO, "Client " + username + ": writing post to timeline");
        stream->Write(new_post);
    }
}

//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {
    std::string coord_ip, coord_port;
    std::string username;

    if (argc < 4) {
        std::cout << "Expected 4 args, got " << argc << '\n';
        std::cout << "Usage: ./tsc -h <coordinatorIP> -k <coordinatorPort> -u <userId>\n";
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
                std::cout << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("client-") + username;
    google::InitGoogleLogging(log_file_name.c_str());

    log(INFO, "Client " + username + ": Logging initialized");

    Client myc(coord_ip, coord_port, username);

    myc.run();

    return 0;
}
