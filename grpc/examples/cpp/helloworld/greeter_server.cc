/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <dirent.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::Version;
using helloworld::Know;
using helloworld::HelloReply;
using helloworld::Image;
using helloworld::Chunk;
using helloworld::Greeter;
using namespace std;

int chunk_number = 0;
string path;
string chunk_file;
map<string, int> chunk;

void GetFileNames(string path,vector<string>& filenames) {
    DIR* pDir;
    struct dirent* ptr;
    if(!(pDir = opendir(path.c_str()))) return;
    while((ptr = readdir(pDir))  != NULL) {
        filenames.push_back(ptr->d_name);
    }
    closedir(pDir);
}



bool isDirExist(const std::string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    }
    return (info.st_mode & S_IFDIR) != 0;
}



bool makePath(const std::string& path) {
    mode_t mode = 0755;
    int ret = mkdir(path.c_str(), mode);
    if (ret == 0)  return true;
    switch (errno) {
        case ENOENT:
        {
            int pos = path.find_last_of('/');
            if (pos == std::string::npos)   return false;
            if (!makePath( path.substr(0, pos) ))   return false;
        }
        return 0 == mkdir(path.c_str(), mode);

    case EEXIST:
        return isDirExist(path);

    default:
        return false;
    }
}


// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    std::string prefix("Hello ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }

  Status TellVersion(ServerContext* context, const Version* request,
                  Know* reply) override {
    chunk_number = atoi(request->size().c_str());
    cout << "Total chunk number is:" << chunk_number << endl;
    chunk_file = request->image() + "_ver" + request->version();
    reply->set_message("Okay");
    return Status::OK;
  }


  Status ChunkToSend(ServerContext* context, const Image* request,
                  Know* reply) override {
    //search and return back the number such as "ubuntuOS_ver1_1,ubuntuOS_ver1_2"
    vector<string> file_name;
    path = "/home/haojin/" + request->image() + "/"+ request->version();
    GetFileNames(path, file_name);
    
    chunk.clear(); 
    for(int i = 0; i < chunk_number; i++){
        string chunk_name = chunk_file + "_" + to_string(i);
        chunk.insert(make_pair(chunk_name, 0));
    }
    /*
    for(auto it = chunk.begin(); it != chunk.end(); it++){
        cout << it->first << endl;
    }
    */
    cout << "Get the file name from the folder <" << path  << ">" << endl;
    for(string file: file_name){
        cout << file << endl;
        for(auto it = chunk.begin(); it != chunk.end(); it++)
            if(file == it->first) it->second++;
    }
    
    string res;
    for(auto it = chunk.begin(); it != chunk.end(); it++){
        if(it->second == 0) res += (it->first + ",");
    }
    if(!res.empty()) res.erase(res.size()-1);
    cout << "The lossing chunk number is " << res << endl;
    if(!res.empty())  {
        reply->set_message(res);
    }else {
        reply->set_message("Okay");
    }
    return Status::OK;
  }


  Status SendChunk(ServerContext* context, const Chunk* request,
                  Know* reply) override {
    //store the request->data in file
    string dir = path;
    //cout << "dir:" << dir << endl;
    string filename = dir +  "/" + request->image() + "_ver" + request->version() + "_" + request->number();
    std::cout << "transfer file name is " << filename << std::endl;
    makePath(dir);
    ofstream fout(filename, ios::out | ios::trunc);
    if(!fout){
        cout << "Store " << filename << " failed." << endl;
    }
    fout << request->data();
    fout.close();
    string success = "synchronize " + filename + " success";
    reply->set_message(success);
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  GreeterServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
