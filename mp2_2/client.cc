#include "client.h"

using std::cout;
using std::endl;
using std::string;

void IClient::run() {
    int ret = connectTo();
    if (ret < 0) {
        cout << "connection failed: " << ret << endl;
        exit(1);
    }
    displayTitle();
    while (1) {
        string cmd = getCommand();
        IReply reply = processCommand(cmd);
        displayCommandReply(cmd, reply);
        if (reply.grpc_status.ok() && reply.comm_status == SUCCESS &&
            cmd == "TIMELINE") {
            cout << "Now you are in the timeline" << endl;
            processTimeline();
        }
    }
}

void IClient::displayTitle() const {
    cout << "\n========= TINY SNS CLIENT =========\n";
    cout << " Command Lists and Format:\n";
    cout << " FOLLOW <username>\n";
    cout << " UNFOLLOW <username>\n";
    cout << " LIST\n";
    cout << " TIMELINE\n";
    cout << "=====================================\n";
}

string IClient::getCommand() const {
    string input;
    while (1) {
        cout << "Cmd> ";
        std::getline(std::cin, input);
        std::size_t index = input.find_first_of(" ");
        if (index != string::npos) {
            string cmd = input.substr(0, index);
            toUpperCase(cmd);
            if (input.length() == index + 1) {
                cout << "Invalid Input -- No Arguments Given\n";
                continue;
            }
            string argument = input.substr(index + 1, (input.length() - index));
            input = cmd + " " + argument;
        } else {
            toUpperCase(input);
            if (input != "LIST" && input != "TIMELINE") {
                cout << "Invalid Command\n";
                continue;
            }
        }
        break;
    }
    return input;
}

void IClient::displayCommandReply(const string& comm,
                                  const IReply& reply) const {
    if (reply.grpc_status.ok()) {
        switch (reply.comm_status) {
        case SUCCESS:
            cout << "Command completed successfully\n";
            if (comm == "LIST") {
                cout << "All users: ";
                for (string room : reply.all_users) { cout << room << ", "; }
                cout << "\nFollowers: ";
                for (string room : reply.followers) { cout << room << ", "; }
                cout << endl;
            }
            break;
        case FAILURE_ALREADY_EXISTS:
            cout << "Input username already exists, command failed\n";
            break;
        case FAILURE_NOT_EXISTS:
            cout << "Input username does not exists, command failed\n";
            break;
        case FAILURE_INVALID_USERNAME:
            cout << "Command failed with invalid username\n";
            break;
        case FAILURE_NOT_A_FOLLOWER:
            cout << "Command failed with not a follower\n";
            break;
        case FAILURE_INVALID:
            cout << "Command failed with invalid command\n";
            break;
        case FAILURE_UNKNOWN:
            cout << "Command failed with unknown reason\n";
            break;
        default:
            cout << "Invalid status\n";
            break;
        }
    } else {
        cout << "grpc failed: " << reply.grpc_status.error_message() << endl;
    }
}

void IClient::toUpperCase(string& str) const {
    std::locale loc;
    for (string::size_type i = 0; i < str.size(); i++)
        str[i] = toupper(str[i], loc);
}

/*
 * get/displayPostMessage functions will be called in chatmode
 */
string getPostMessage() {
    char buf[MAX_DATA];
    while (1) {
        fgets(buf, MAX_DATA, stdin);
        if (buf[0] != '\n') break;
    }

    string message(buf);
    return message;
}

void displayPostMessage(const string& sender, const string& message,
                        std::time_t& time) {
    string t_str(std::ctime(&time));
    t_str[t_str.size() - 1] = '\0';
    cout << sender << " (" << t_str << ") >> " << message << endl;
}

void displayReConnectionMessage(const string& host, const string& port) {
    cout << "Reconnecting to " << host << ":" << port << "..." << endl;
}
