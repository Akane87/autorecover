#include <ctime>
#include <time.h>
#include <chrono>
#include <thread>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <sys/wait.h>
#include "sns.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using tinysns::Message;
using tinysns::Posting;
using tinysns::ListReply;
using tinysns::Request;
using tinysns::ServerRequest;
using tinysns::Reply;
using tinysns::SNSService;
using tinysns::ServerConnection;

struct PostData {
	 std::string username;
	 std::string message;
	 std::string time;
};

struct Client {
	std::string username;
	bool connected = true;
	int following_file_size = 0;
	std::vector<Client*> client_followers;
	std::vector<Client*> client_following;
	std::vector<PostData> posts;
	ServerReaderWriter<Posting, Posting>* stream = 0;
	bool operator==(const Client& c1) const{
		return (username == c1.username);
	}
};

struct Serv {
	std::string port;
	std::string host;
	std::string sourceIP; 
	std::string destinationIP;
	bool alive = false;
	std::set<std::string> clients;
	ServerReaderWriter<Posting, Posting>* stream = 0;
	bool operator==(const Serv& s1) const{
		return (host == s1.host && port == s1.port);
	}
};

std::vector<Client> client_db;
std::map<std::string, Serv> server_db;
std::mutex mtx;

int find_user(std::string username){
	int index = 0;
	for(Client c : client_db){
		if(c.username == username)
			return index;
		index++;
	}
	return -1;
}

class SNSServiceImpl final : public SNSService::Service {
    public: 
	std::string type = "error";
	std::string myServerAddress = "error";
	std::string routerPort = "error";
	std::string routerHost = "error";
	std::string headIP = "error";
	std::string tailIP = "error";

	bool hasBroadcastStream = false;
	std::unique_ptr<SNSService::Stub> stub_;
	std::unique_ptr<ClientReaderWriter<Posting, Posting>> broadcastStream = 0;

	void updateMyServer(){
		auto routerStub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel((routerHost+":"+routerPort), grpc::InsecureChannelCredentials())));
		ServerRequest request;
		ClientContext context;
		Reply pingReply;
		Status status;
		request.set_port("s");
		status = routerStub_->slavePing(&context, request, &pingReply);
	}

    private:
	Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
		Client user = client_db[find_user(request->username())];
		std::cout << "Printing list for " << user.username << std::endl;
		int index = 0;
		for(Client c : client_db){
			list_reply->add_all_users(c.username);
		}
		std::vector<Client*>::const_iterator it;
		for(it = user.client_followers.begin(); it!=user.client_followers.end(); it++){
			list_reply->add_followers((*it)->username);
		}
		return Status::OK;
	}

	Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
		std::string username1 = request->username();
		std::string username2 = request->arguments(0);
		int join_index = find_user(username2);
		if(join_index < 0 || username1 == username2)
			reply->set_msg("Follow Failed -- Invalid Username");
		else{
			Client *user1 = &client_db[find_user(username1)];
			Client *user2 = &client_db[join_index];
			if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) != user1->client_following.end()){
				reply->set_msg("Follow Failed -- Already Following User");
				return Status::OK;
			}
			user1->client_following.push_back(user2);
			user2->client_followers.push_back(user1);
			reply->set_msg("Follow Successful");
			if (type == "master"){
				updateUsers("follow", username1, username2);
			}
		}
		writeFile();
		return Status::OK; 
	}

	Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
		std::string username1 = request->username();
		std::string username2 = request->arguments(0);
		int leave_index = find_user(username2);
		if(leave_index < 0 || username1 == username2){
			reply->set_msg("UnFollow Failed -- Invalid Username");
        }else{
			Client *user1 = &client_db[find_user(username1)];
			Client *user2 = &client_db[leave_index];
			if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) == user1->client_following.end()){
				reply->set_msg("UnFollow Failed -- Not Following User");
				return Status::OK;
			}
			user1->client_following.erase(find(user1->client_following.begin(), user1->client_following.end(), user2)); 
			user2->client_followers.erase(find(user2->client_followers.begin(), user2->client_followers.end(), user1));
			reply->set_msg("UnFollow Successful");
			if (type == "master"){
				updateUsers("unfollow", username1, username2);
			}
		}
		writeFile();
		return Status::OK;
	}
	
	Status Login(ServerContext* context, const Request* request, Reply* reply) override {
		Client c;
		std::string username = request->username();
		int user_index = find_user(username);
		if(user_index < 0){
			c.username = username;
			client_db.push_back(c);
			reply->set_msg("Login Successful!");
			if(type == "master"){
				updateUsers("new", username);
			}
			writeFile();				
		}else{ 
			killClient(username);
			Client *user = &client_db[user_index];
			if(user->connected) {
				reply->set_msg("Invalid Username");
            }else{
				std::string msg = "Welcome Back " + user->username;
				reply->set_msg(msg);
				user->connected = true;
			}
		}
		return Status::OK;
	}

	void addServerLink(std::string servIP){
		bool bufferFlag = false; 
		bool alive = false;
		for(auto iter = server_db.rbegin(); iter != server_db.rend(); iter++){
		    if(iter->second.alive && !bufferFlag){
			    bufferFlag = true;
				continue;
			}else if(iter->second.alive && bufferFlag){
			    alive = true;
				break;
			}
		}
		auto currServer = server_db.find(servIP);
		auto tailServer = server_db.find(tailIP);
		auto headServer = server_db.find(headIP);
		if(alive == false){
			headIP = servIP;
			tailIP = servIP; 
			currServer->second.sourceIP = servIP; 
			currServer->second.destinationIP = servIP; 
		}else{
			currServer->second.sourceIP = tailIP; 
			currServer->second.destinationIP = headIP; 
			tailServer->second.destinationIP = servIP; 
			headServer->second.sourceIP = servIP;
			tailIP = servIP;
		}
		
		std::cout << std::endl << "New message broadcast configuration: " << std::endl;
		auto curr = server_db.find(headIP);
		std::cout << curr->first << " --> ";
		while(curr->first != tailIP){
			curr = server_db.find(curr->second.destinationIP);
			std::cout << curr->first << " --> ";
		}
		std::cout << "head" << std::endl << std::endl;
		SendServerLink();
	}

	void removeServerLink(std::string servIP){
		auto currServer = server_db.find(servIP);
		if(headIP == tailIP){
			headIP = "error";
			tailIP = "error";
			currServer->second.sourceIP = "error";
			currServer->second.destinationIP = "error";
			std::cout << "Router: Last server removed!" << std::endl; 
		}else{
			auto prevServer = server_db.find(currServer->second.sourceIP);
			auto nextServer = server_db.find(currServer->second.destinationIP);
			prevServer->second.destinationIP = nextServer->first; 
			nextServer->second.sourceIP = prevServer->first; 
			if(servIP == headIP){
				headIP = prevServer->first;
			}
			if(servIP == tailIP){
				tailIP = nextServer->first;
			}
			std::cout << "Remove Server: " << std::endl;
			auto curr = server_db.find(headIP);
			std::cout << curr->first << " --> ";
			while(curr->first != tailIP){
				curr = server_db.find(curr->second.destinationIP);
				std::cout << curr->first << " --> ";
			}
			std::cout << std::endl;
		}
		SendServerLink();
	}

	void SendServerLink() {
		for(auto i : server_db){
			if(i.second.alive) {
				std::string thisIp = i.second.host+":"+i.second.port;
				auto stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(thisIp, grpc::InsecureChannelCredentials())));
				ServerConnection request;
				request.set_source(i.second.sourceIP);
				request.set_destination(i.second.destinationIP);
				ClientContext context;
				Reply reply;
				Status status = stub_->updateServer(&context, request, &reply);
			}
		}
	}

	Status Route(ServerContext* context, const Request* request, Reply* reply) override {
		std::string username = request->username();
		usleep(100000);
		std::string leaderIp = loadBal();
		if(leaderIp == "error"){
			reply->set_msg("noLeader");
		}
		else{ 
			std::cout << "routing " << username << " to " << leaderIp << std::endl;
			auto leaderServ = server_db.find(leaderIp);
			leaderServ->second.clients.insert(username);
			reply->set_msg(leaderIp);
		}
		return Status::OK;
	}

	Status newServer(ServerContext* context, const ServerRequest* request, Reply* reply) override {
		Serv s;
		std::string ip = request->port();
		auto port_iter = server_db.find(ip);
		if(port_iter == server_db.end()){
			int index = ip.find(":");
			std::string host = ip.substr(0, index);
			std::string port = ip.substr(index+1, ip.length()-host.length());
			s.port = port;
			s.host = host;
			s.alive = true;
			server_db.insert(std::pair<std::string, Serv>(ip, s));
			addServerLink(ip);
			std::cout << "Router: Server " << ip << " successfully added" << std::endl;
		}else{ 
			if(port_iter->second.alive){
				std::cout << "Router: Duplicate server detected on " << ip << std::endl;
            }else{
				std::cout << "Router: Welcome Back " << ip << std::endl;
				port_iter->second.alive = true;
				addServerLink(ip);
			}
		}
		return Status::OK;
	}

	Status updateServer(ServerContext* context, const ServerConnection* request, Reply* reply) override {
		headIP = request->source();
		std::string temp = tailIP;
		tailIP = request->destination();
		if(temp != tailIP){
			broadcastStream = 0;
			hasBroadcastStream = false;
		}
		if(headIP == "error" || tailIP == "error"){
			std::cout << "Failed to establish place in server network" << std::endl;
		}
		return Status::OK;
	}

	void killClient(std::string username){
		for (auto iter = server_db.rbegin(); iter != server_db.rend(); iter++){
			auto it = iter->second.clients.find(username);
			if (it != iter->second.clients.end()){
				iter->second.clients.erase(it);
			}
		}
	}

	std::string loadBal(){
		bool noneAlive = true;
		std::string leaderIp;
		int lowestCount = 9999999;
		for(auto iter = server_db.rbegin(); iter != server_db.rend(); iter++){
			if (iter->second.alive && iter->second.clients.size() <= lowestCount){
				leaderIp = iter->first;
				lowestCount = iter->second.clients.size();
				noneAlive = false;
			}
		}
		if(noneAlive){
			std::cout << "Router: Failed to find avaiable master" << std::endl;
			leaderIp = "error";
		}
		return leaderIp;
	}

	Status slavePing(ServerContext* context, const ServerRequest* request, Reply* reply) override {
		std::string port = request->port();
		if(port[0] == 'd'){
			std::string ip = port.substr(1, port.length()-1);
			auto serv = server_db.find(ip);
			serv->second.alive = false;
			removeServerLink(ip);
			std::cout << "Router: Server " << ip << " died" << std::endl;		
			server_db.find(ip)->second.clients.clear();
		}else if(port[0] == 's'){
			SendServerLink();
		}else if(port[0] == 'u'|| port[0] == 'f' || port[0] == 'n'){
			std::string buf; 
			std::stringstream ss(port); 
			std::vector<std::string> tokens;
			while (ss >> buf){
				tokens.push_back(buf);
			}
			int index = tokens[1].find(':');
			std::string incomingPort = tokens[1].substr(index+1, tokens[1].length()-index-1);
			std::string host = tokens[1].substr(0, index+1);
			if(type == "router" && (port[0] == 'f'|| port[0] == 'u' || port[0] == 'n')){
				std::cout << "Routing: " << port << std::endl;
				for(auto i : server_db){
					std::string thisIp = i.second.host+":"+i.second.port;
					if (thisIp != tokens[1]  && i.second.alive){
						std::cout << "found a successful one, gonna send: " << thisIp << std::endl;
						auto stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(thisIp, grpc::InsecureChannelCredentials())));
						ServerRequest request;
						request.set_port(port);
						ClientContext context;
						Reply reply;
						Status status = stub_->slavePing(&context, request, &reply);
					}
				}
			}else if(type == "master"){
				std::cout << "Master: received " << port << std::endl;
				Client *userClient;
				Client *affectedClient;
				int user;
				int affectedUser;
				if(port[0] != 'n'){
					user = find_user(tokens[2]);
					affectedUser = find_user(tokens[3]); 
					userClient = &client_db[user]; 
					affectedClient = &client_db[affectedUser]; 
				}
				if(port[0] == 'f'){
					userClient->client_following.push_back(affectedClient);
					affectedClient->client_followers.push_back(userClient);
				}else if(port[0] == 'u'){
					int q = 0;
					for(auto i : userClient->client_following){
						if(i->username == tokens[3]){
							userClient->client_following.erase(userClient->client_following.begin()+q);
						}
						q++;
					}
					q = 0;
					for(auto i : affectedClient->client_followers){
						if(i->username == tokens[2]){
							affectedClient->client_followers.erase(affectedClient->client_followers.begin()+q);
						}
						q++;
					}
				}else if(port[0] == 'n'){
					Client c;
					c.username = tokens[2];
					client_db.push_back(c);
				}
			}
		}
		return Status::OK;
	}

	Status Timeline(ServerContext* context, 
		ServerReaderWriter<Posting, Posting>* stream) override {
		std::flush(std::cout);
		Client *c;
		std::string label = "default";
		time_t rawtime;
        struct tm * timeinfo;
        time(&rawtime);
        timeinfo = localtime(&rawtime);
        std::string outputTime = asctime(timeinfo);
        Posting p;
        stream->Read(&p);
        std::cout << "content: " << p.content() << " from Username: " << p.username() << " with ip: " << p.ip() << std::endl;
        std::string msg = p.content();
        std::string user = p.username();
        int nameIndex = find_user(user);
        Posting new_posting;
        std::string incomingIP = p.ip();
		ClientContext broadcastContext;
        Posting q;
		if (incomingIP == ""){
			label = "client";
		}else{
			label = "broadcast";
		}
		std::cout << label << ": " << "Creating broadcast stream with server at IP: " << tailIP << std::endl;
		if(broadcastStream == 0){
			stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(tailIP, grpc::InsecureChannelCredentials())));
			broadcastStream = stub_->Timeline(&broadcastContext);
			hasBroadcastStream = true;
			std::cout << label << ": " << "Created new broadcast stream" << std::endl;
		}
		
        if(nameIndex >= 0 ){
			std::cout << "Master: Writing old posts" << std::endl;
        	c = &client_db[nameIndex];
	        for(int i = 0; i < c->posts.size(); i++){
	            std::string time_data = c->posts[i].time;
	            if (c->posts[i].time[c->posts[i].time.length()-1] == '\n'){
	                time_data = c->posts[i].time.substr(0, c->posts[i].time.length()-1);
	            }
	            new_posting.set_content(c->posts[i].username + "(" + time_data + ")>> " + c->posts[i].message + '\n');
	            stream->Write(new_posting);
	        }
	        if(c->stream == 0){
				c->stream = stream;
				std::cout << label << ": Set stream for " << c->username << " " << incomingIP << std::endl;
	        }else if (label == "broadcast"){
				c->stream = stream;
				std::cout << label << ": Updated stream for " << c->username << " " << incomingIP << std::endl;
			}

	    }
        
        std::thread reader([&](){
        	if(incomingIP == ""){
        		std::cout << label << ": " << "Incoming IP: Local Client, Outgoing IP: " <<  tailIP << std::endl;
        	}else{
        		std::cout << label << ": " << "Incoming IP: "<< incomingIP << ", Outgoing IP: " <<  tailIP << std::endl;
        	}
            while(stream->Read(&p)) {
            	std::cout << "-----Post Received-----" << std::endl;
            	if(incomingIP == ""){
	        		std::cout << label << ": " << "Incoming IP: Local Client, Outgoing IP: " <<  tailIP << std::endl;
	        	}else{
	        		std::cout << label << ": " << "Incoming IP: "<< incomingIP << ", Outgoing IP: " <<  tailIP << std::endl;
	        	}
            	std::flush(std::cout);
            	q=p;
            	incomingIP = q.ip();
            	if(incomingIP == ""){
            		q.set_ip(myServerAddress);
            	}
            	if(tailIP == "" || tailIP == "error" || !hasBroadcastStream){
            		std::cout << label << ": " << "Either empty tailIP or no broadcaststream" << std::endl;
            	}else if(incomingIP == tailIP){
            		std::cout << "Finished sending to all servers" << std::endl;
            	}else if(broadcastStream == 0 || broadcastStream->Write(q) == false){
            		std::cout << label << ": " << "Broadcast stream failed" << std::endl;
            		std::cout << label << ": " << "tailIP = " << tailIP << std::endl;
            		if(incomingIP != tailIP){
            			ClientContext context;
				        stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
				           grpc::CreateChannel(
				                tailIP, grpc::InsecureChannelCredentials())));
				         std::cout << label << ": " << "Created stub" << std::endl;
				    	 broadcastStream = stub_->Timeline(&context);
				    	 std::cout << "Reconnected to inter-server stream" << std::endl;
					}
            	}else if(p.content() != "--connect--"){
		            readData(p);
				}
	            std::string time_data = outputTime;
	            if(outputTime[outputTime.length()-1] == '\n'){
	                time_data = outputTime.substr(0, outputTime.length()-1);
	            }
	            if(p.content() != "--connect--"){
		            new_posting.set_content(p.username() + "(" + time_data + ")>> " + p.content() + '\n');
		            for(int i = 0; i < c->client_followers.size(); i++){
		                if (c->client_followers[i]->stream != 0 && c->client_followers[i]->connected){
		                	std::cout << label << ": " << "Writing to: " << c->client_followers[i]->username << std::endl;
		                    c->client_followers[i]->stream->Write(new_posting);
		                }
		            }
		        }
	            std::cout << "-----------------------" << std::endl << std::endl;
				
            }
        });
        
        reader.join();
        if(label == "client"){
        	c->connected = false;
        	c->stream = 0;
        	killClient(user);
        }else{
			c->stream = 0;
			broadcastStream = 0;
		}
        std::cout << label << ": " << user << "'s timeline disconnected" << std::endl;
        return Status::OK;
	}

	void updateUsers(std::string operation, std::string user, std::string affectedUser = ""){
		auto routerStub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel((routerHost+":"+routerPort), grpc::InsecureChannelCredentials())));
		ServerRequest request;
		ClientContext context;
		Reply pingReply;
		Status status;
		if(operation == "new"){
			request.set_port("n " + myServerAddress + " " + user);
			status = routerStub_->slavePing(&context, request, &pingReply);
        }else if(operation == "follow"){
			request.set_port("f " + myServerAddress + " " + user + " " + affectedUser);
			status = routerStub_->slavePing(&context, request, &pingReply);
		}else if(operation == "unfollow"){
			request.set_port("u " + myServerAddress + " " + user + " " + affectedUser);
			status = routerStub_->slavePing(&context, request, &pingReply);
		}
		if(status.ok()){
			std::cout << "Master: Sent new data to router" << std::endl;
		}else{
			std::cout << "Master: router ded" << std::endl;
		}
	}

	void readData(Posting p){
		time_t rawtime;
		struct tm * timeinfo;
		time(&rawtime);
		timeinfo = localtime(&rawtime);
		std::string outputTime = asctime(timeinfo);
		outputTime = outputTime.substr(0, outputTime.length()-1);
		std::string msg = p.content();
        std::string user = p.username();
        int nameIndex = find_user(user);
        Client *c = &client_db[nameIndex];

		PostData tempPost; 
		tempPost.username = user;
		tempPost.message = msg;
		tempPost.time = outputTime;
		for(int i = 0; i < c->client_followers.size(); i++){
			c->client_followers[i]->posts.push_back(tempPost);
			if(c->client_followers[i]->posts.size() > 20){
				c->client_followers[i]->posts.erase(c->client_followers[i]->posts.begin());
			}
		}
		writeFile();  
		std::cout << "received \""<< msg << "\" from client: " << user << " at " << outputTime << std::endl;
	}

	void writeFile(){
		mtx.lock();
		std::ofstream write_file;
		write_file.open ("data.csv", std::fstream::out);
		write_file << std::to_string(client_db.size()) + ",\n";
		for (auto i : client_db){
			write_file << i.username + ",\n";
			for(auto j : i.client_followers){
				write_file << j->username + ',';
			}
			if(i.client_followers.size() > 0){
				write_file << "\n";
			}else{
				write_file << ",\n";
			}
			for(auto j : i.client_following){
				write_file << j->username + ',';
			}
			if(i.client_following.size() > 0){
				write_file << "\n";
			}else{
				write_file << ",\n";
			}
			write_file << std::to_string(i.posts.size()) + ",\n";
			for(auto j : i.posts){
				write_file << j.username + ',';
				write_file << j.message + ',';
				write_file << j.time + ',';
			}
			if(i.posts.size() > 0){
				write_file << '\n';
			}
		}
		write_file.close();
		mtx.unlock();
	}

	public: void readFile(){
		std::vector<std::vector<std::string>> user_followers;
		std::vector<std::vector<std::string>> user_followees;
		std::ifstream read_file ("data.csv");
		if(read_file.is_open()){
			std::string value;
			std::getline (read_file, value, ','); // read a string until next comma
			if(read_file.eof()){
				read_file.close();
				return;
			}
			int size = std::stoi(value);
			bool endLine = false;
			for(int i = 0; i < size; i++){
				Client c;
				std::getline (read_file, value, ',');
				value = value.substr(1, value.length()-1);
				c.username = value;
				std::vector<std::string> temp; 
				temp.push_back(value);
				user_followers.push_back(temp);
				user_followees.push_back(temp);
				std::getline (read_file, value, ',');
				if(value != "\n"){
					value = value.substr(1, value.length()-1);
					user_followers[i].push_back(value);
				}
				while(std::getline (read_file, value, ',')){
					if(value[0] == '\n'){
                        break;
                    }else{
						user_followers[i].push_back(value);
					}
				}
				if(value != "\n"){
					value = value.substr(1, value.length()-1);
					user_followees[i].push_back(value);
				}
				while(std::getline (read_file, value, ',')){
					if (value[0] == '\n'){
                        break;
                    }else{
						user_followees[i].push_back(value);
					}
				}
				value = value.substr(1, value.length()-1);
				int postSize = std::stoi(value);
				for(int i = 0; i < postSize; i++){
					PostData post;
					std::getline (read_file, value, ',');
					if(value[0] == '\n' || read_file.eof()){
						value = value.substr(1, value.length()-1);
						post.username = value;
						std::getline (read_file, value, ',');
						post.message = value;
						std::getline (read_file, value, ',');
						post.time = value;
						endLine = true;
					}else{
						post.username = value;
						std::getline (read_file, value, ',');
						post.message = value;
						std::getline (read_file, value, ',');
						post.time = value;
					}
					c.posts.push_back(post);
				}
				endLine = false;
				client_db.push_back(c);
			}

			for(int i = 0; i < user_followers.size(); i++){
				std::string user = user_followers[i][0];
				int userIndex = find_user(user);
				for(int j = 1; j < user_followers[i].size(); j++){
					int tempIndex = find_user(user_followers[i][j]);
					client_db[userIndex].client_followers.push_back(&client_db[tempIndex]);
				}
				for(int j = 1; j < user_followees[i].size(); j++){
					int tempIndex = find_user(user_followees[i][j]);
					client_db[userIndex].client_following.push_back(&client_db[tempIndex]);
				}
			}

		} 
		read_file.close();
	}

};

void RunServer(std::string port_no, std::string host, std::string type, std::string routerPort, std::string routerHost) {
	std::string server_address = host + ":"+port_no;
	SNSServiceImpl service;
	service.type = type;
	service.myServerAddress = server_address;
	service.routerPort = routerPort;
	service.routerHost = routerHost;
	if(type == "master") {
		service.readFile();
    }

	ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	if(type == "master") {
		service.updateMyServer();
    }
	//std::cout << "Server listening on " << server_address << std::endl;
	server->Wait();
}

void startNewProcess(std::string path, std::string port, std::string hostname, std::string type, std::string routerPort, std::string routerHost){
	std::string dummy = "";
	pid_t pid = fork();
	switch (pid) {
		case -1: 
			std::cerr << "Uh-Oh! fork() failed.\n";
			exit(1);
		case 0:
			execl(path.c_str(), dummy.c_str(), port.c_str(), hostname.c_str(), type.c_str(), routerPort.c_str(), routerHost.c_str(), (char*)NULL);
			std::cerr << "Uh-Oh! process start failed!";
			exit(1);
	}
	if(setpgid(pid, 0) != 0) {
		perror ("setpgid() error");
    }
}

void slaveStart(std::string port, std::string hostname, std::string routerPort, std::string routerHost){
	usleep(1000000);
	std::string login_info = hostname + ":" + port;
	auto stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));
	std::string routerLogin = routerHost + ":" + routerPort;
	auto routerStub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(routerLogin, grpc::InsecureChannelCredentials())));
	time_t rawtime;
	struct tm * timeStart, timeEnd;
	bool alive = true;
	bool routerAlive = true;
	Reply pingReply;
	Reply routerReply;
	ServerRequest request;
	ServerRequest routerRequest;
	Status status;
	while(routerAlive){
		request.set_port(login_info);
		routerRequest.set_port(routerLogin);
		while(1){
			ClientContext context;
			status = stub_->slavePing(&context, request, &pingReply);
			if(status.ok()){
				usleep(1000000);
			}else{
				std::cout << "Slave: Master ded" << std::endl;
				break;
			}
			
			ClientContext contextRouter;
			status = routerStub_->slavePing(&contextRouter, routerRequest, &routerReply);
			if(status.ok()){
				usleep(1000000);
			}else{
				std::cout << "Slave: Router ded" << std::endl;
				routerAlive = false;
				break;
			}
		}

		ClientContext routerContext;
		request.set_port('d'+login_info);
		status = routerStub_->slavePing(&routerContext, request, &pingReply);
		if(status.ok()){
			std::cout << "Slave: Restarting Server...";
			std::string path = "./tsd";
			std::string input1 = "-p " + std::to_string(std::stoi(port));
			std::string input2 = "-h " + hostname;
			std::string input3 = "-t master";
			std::string input4 = "-r " + routerPort;
			std::string input5 = "-q " + routerHost;
			std::cout << "RUNNING COMMAND:" << path << " " << input1 << input2 << input3 << input4 << input5 << std::endl;
			startNewProcess(path, input1, input2, input3, input4, input5);
			usleep(1000000);
			std::cout << "Restart Complete!" << std::endl;
		}else{
			std::cout << "Slave: Exiting" << std::endl;
			break;
		}
	}
}

int main(int argc, char** argv) {
	std::string port = "port failed";
	std::string hostname = "host failed";
	std::string type = "type failed";
	std::string startup = "startup failed";
	std::string routerPort = "router port failed";
	std::string routerHost = "router host failed";
	int opt = 0;
	while((opt = getopt(argc, argv, "h:t:p:c:s:r:q:")) != -1){
		switch(opt) {
			case 'h':
				hostname = optarg;break;
			case 't':
				type = optarg;break;
			case 'p':
				port = optarg;break;
			case 's':
				startup = optarg;break;
			case 'r':
				routerPort = optarg;break;
			case 'q':
				routerHost = optarg;break;
			default:
				std::cerr << "Invalid Command Line Argument\n";
			}
	}
	std::string programPath = "./tsd";
	std::string param1 = "-p " + port;
	std::string param2 = "-h " + hostname;
	std::string param3 = "-t " + startup;
	std::string param4 = "-r " + routerPort;
	std::string param5 = "-q " + routerHost;
	
	if(startup != "startup failed"){
		startNewProcess(programPath, "-p " + std::to_string(std::stoi(port)+1), param2, param3, param4, param5);
	}
	
	if(type == "slave" || type == " slave"){
		if(type == " slave"){
			type = type.substr(1, type.length()-1);
			hostname = hostname.substr(1, hostname.length()-1);
			port = port.substr(1, port.length()-1);
			routerPort = routerPort.substr(1, routerPort.length()-1);
			routerHost = routerHost.substr(1, routerHost.length()-1);
		}
		std::cout <<  "|" << type << "|" << hostname << "|" << port << "|" << std::endl;
		slaveStart(std::to_string(std::stoi(port)-1), hostname, routerPort, routerHost);
	}else if(type == " master" || type == "master"){
		if(type == " master"){
			type = type.substr(1, type.length()-1);
			hostname = hostname.substr(1, hostname.length()-1);
			port = port.substr(1, port.length()-1);
			routerPort = routerPort.substr(1, routerPort.length()-1);
			routerHost = routerHost.substr(1, routerHost.length()-1);
		}
		std::cout <<  "|" << type << "|" << hostname << "|" << port << "|" << std::endl;
		std::string routerLogin = routerHost + ":" + routerPort;
		auto routerStub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(routerLogin, grpc::InsecureChannelCredentials())));
		ServerRequest newPort;
		newPort.set_port(hostname+":"+port);
		Reply reply;
		ClientContext context;
		Status status = routerStub_->newServer(&context, newPort, &reply);
		RunServer(port, hostname, type, routerPort, routerHost);
	}else if(type == "router" || type == " router"){
		if(type == " router"){
			type = type.substr(1, type.length()-1);
			hostname = hostname.substr(1, hostname.length()-1);
			port = port.substr(1, port.length()-1);
		}
		std::cout <<  "|" << type << "|" << hostname << "|" << port << "|" << std::endl;
		RunServer(port, hostname, type, routerPort, routerHost);
	}
	return 0;
}
