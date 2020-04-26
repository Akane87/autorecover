#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "client.h"
#include "sns.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using tinysns::Message;
using tinysns::Posting;
using tinysns::ListReply;
using tinysns::Request;
using tinysns::ServerRequest;
using tinysns::Reply;
using tinysns::SNSService;

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

bool stress_test;
int max_posts;

class Client : public IClient
{
    public:
        Client(const std::string& hname,
               const std::string& uname,
               const std::string& p)
            :hostname(hname), username(uname), port(p)
            {}
    protected:
        virtual int connectTo();
        virtual IReply processCommand(std::string& input);
        virtual void processTimeline();
    private:
        std::string hostname;
        std::string username;
        std::string port;
        std::string routerPort;
        std::string routerHostname;
        bool reconnected = false;
        std::unique_ptr<SNSService::Stub> stub_;
        IReply Login();
        IReply Route();
        IReply List();
        IReply Follow(const std::string& username2);
        IReply UnFollow(const std::string& username2);
        void Timeline(const std::string& username);

};

int main(int argc, char** argv) {

    std::string hostname = "localhost";
    std::string username = "default";
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:p:t:")) != -1){
        switch(opt) {
            case 'h':
                hostname = optarg;break;
            case 'u':
                username = optarg;break;
            case 'p':
                port = optarg;
                break;
            case 't':
                stress_test = true;
                max_posts = atoi(optarg);
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    Client myc(hostname, username, port);
    // You MUST invoke "run_client" function to start business logic
    myc.run_client();

    return 0;
}

int Client::connectTo()
{
    std::string login_info = hostname + ":" + port;
    stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

    IReply ire = Route();
    if(!ire.grpc_status.ok()) {
		std::cout << "Failed routing" << std::endl;
        return -1;
    }
	//std::cout << "success routing" << std::endl;
    ire = Login();
    if(!ire.grpc_status.ok()) {
		std::cout << "Failed logging in" << std::endl;
        return -1;
    }
    return 1;
}

IReply Client::processCommand(std::string& input)
{
    IReply ire;
    std::size_t index = input.find_first_of(" ");
    if(index != std::string::npos) {
        std::string cmd = input.substr(0, index);
        std::string argument = input.substr(index+1, (input.length()-index));
        if (cmd == "FOLLOW") {
            ire =  Follow(argument);
        } else if(cmd == "UNFOLLOW") {
            ire = UnFollow(argument);
        }
    }else{
        if (input == "LIST") {
            ire = List();
        } else if (input == "TIMELINE") {
            ire.comm_status = SUCCESS;
            return ire;
        }
    }

    if(!ire.grpc_status.ok()){
        std::cout << "Reconnecting to server" << std::endl;
        port = routerPort;
        hostname = routerHostname;
        usleep(500000);
        int i = connectTo();
        if (i > 0){
            std::cout << "Connection Complete" << std::endl;
            ire = processCommand(input);
        }else{
            std::cout << "Connection failed" << std::endl;
        }

    }else if(ire.comm_status == SUCCESS){
		return ire;
	}else{
        ire.comm_status = FAILURE_INVALID;
    }
    return ire;
}

void Client::processTimeline()
{
    Timeline(username);
}

IReply Client::List() {
    Request request;
    request.set_username(username);
    ListReply list_reply;
    ClientContext context;
    Status status = stub_->List(&context, request, &list_reply);
    IReply ire;
    ire.grpc_status = status;
    if(status.ok()){
        ire.comm_status = SUCCESS;
        std::string all_users;
        std::string following_users;
        for(std::string s : list_reply.all_users()){
            ire.all_users.push_back(s);
        }
        for(std::string s : list_reply.followers()){
            ire.followers.push_back(s);
        }
    }else{
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

IReply Client::Follow(const std::string& username2) {
    Request request;
    request.set_username(username);
    request.add_arguments(username2);

    Reply reply;
    ClientContext context;

    Status status = stub_->Follow(&context, request, &reply);
    IReply ire; ire.grpc_status = status;
    if(reply.msg() == "Follow Failed -- Invalid Username") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }else if(reply.msg() == "Follow Failed -- Already Following User") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    }else if(reply.msg() == "Follow Successful") {
        ire.comm_status = SUCCESS;
    }else{
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

IReply Client::UnFollow(const std::string& username2) {
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Reply reply;
    ClientContext context;
    Status status = stub_->UnFollow(&context, request, &reply);
    IReply ire;
    ire.grpc_status = status;
    if(reply.msg() == "UnFollow Failed -- Invalid Username") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }else if(reply.msg() == "UnFollow Failed -- Not Following User") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }else if(reply.msg() == "UnFollow Successful") {
        ire.comm_status = SUCCESS;
    }else{
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

IReply Client::Login() {
    Request request;
    request.set_username(username);
    Reply reply;
    ClientContext context;
    Status status = stub_->Login(&context, request, &reply);
    IReply ire;
    ire.grpc_status = status;
    if (reply.msg() == "Invalid Username") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    }else{
        ire.comm_status = SUCCESS;
    }
    return ire;
}

IReply Client::Route() {
    Request request;
    request.set_username(username);
    Reply reply;
    ClientContext context;
    Status status = stub_->Route(&context, request, &reply);
    IReply ire;
    ire.grpc_status = status;
    if(reply.msg() == "noLeader"){
        std::cout << "No leader Server Found" << std::endl;
        ire.comm_status = FAILURE_UNKNOWN;
    }else{
        routerPort = port;
        routerHostname = hostname;
        std::string ip = reply.msg();
		int index = ip.find(":");
		hostname = ip.substr(0, index);
		port = ip.substr(index+1, ip.length()-hostname.length());
        ire.comm_status = SUCCESS;
        std::string login_info = ip;
        stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));
    }
    return ire;
}

void Client::Timeline(const std::string& username) {
    while (true){
        if(reconnected){
            port = routerPort;
            hostname = routerHostname;
            usleep(500000);
            int i = connectTo();
            if(i>0){
                std::cout << "Connection Complete" << std::endl;
            }else{
                std::cout << "Connection failed" << std::endl;
            }
        }else{
            reconnected = true;
        }

        ClientContext context;
    	std::shared_ptr<ClientReaderWriter<Posting, Posting>> stream = stub_->Timeline(&context);
        std::thread writer([username, stream]() {
            Posting p;
            p.set_content("--connect--");
            p.set_username(username);
            stream->Write(p);
	        stream->Write(p);

		    if(stress_test){
		        using namespace std;
		        clock_t begin = clock();

		        for(int i = 0; i < max_posts; i++) {
		            this_thread::sleep_for(chrono::milliseconds(2));
		            cout << "send message: " << i << endl;
		            p.set_content(to_string(i));
                    p.set_username(username);
		            stream->Write(p);
		        }
		        clock_t end = clock();
		        cout << "Execution time: " << double(end-begin) / CLOCKS_PER_SEC << " seconds" << endl;
		        stress_test = false;
	        }

            std::string msg;
            std::string user = username;
            while(1){
                std::getline(std::cin, msg);
                p.set_content(msg);
                p.set_username(user);
                if(stream->Write(p) == false){
		            break;
			    }
            }
            stream->WritesDone();
        });

        std::thread reader([&]() {
            Posting p;
            while(stream->Read(&p)){
                std::cout << std::endl;
                std::cout << p.content() << std::endl;
            }
        });
        reader.join();
		writer.join();
    }
}

