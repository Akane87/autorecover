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
#include <memory>
#include <string>
#include <unistd.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif
using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Version;
using helloworld::Know;
using helloworld::Image;
using helloworld::Chunk;
using helloworld::Greeter;

class GreeterClient {
 public:
  GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->SayHello(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }

  }

  std::string TellVersion(const std::string& imageName, std::string& version, std::string& size){
    Version request;
    request.set_image(imageName);
    request.set_version(version);
    request.set_size(size);
    
    Know reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->TellVersion(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
        return reply.message();
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return "RPC failed";
    }
  }

  std::string ChunkToSend(const std::string& imageName, std::string& version){
    Image request;
    request.set_image(imageName);
    request.set_version(version);
    
    Know reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->ChunkToSend(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
        return reply.message();
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return "RPC failed";
    }
  }


  std::string SendChunk(const std::string& imageName, std::string version, std::string number, std::string data, std::string checksum){
    Chunk request;
    request.set_image(imageName);
    request.set_version(version);
    request.set_number(number);
    request.set_data(data);
    request.set_checksum(checksum);
    
    Know reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->SendChunk(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
        return reply.message();
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return "RPC failed";
    }
  }




 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GreeterClient greeter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  //std::string user("world");
  //std::string reply1 = greeter.SayHello(user);
  //std::cout << "Greeter received: " << reply1 << std::endl;
  
  //Step1: TellVersion()  
  std::string imageName = "ubuntuOS";
  std::string version = "1";
  std::string size = "10";
  std::string reply = greeter.TellVersion(imageName, version, size);
  std::cout << "Greeter received: " << reply << std::endl;

  //Step2: ChunkToSend()
  //std::string imageName = "ubuntuOS";
  reply = greeter.ChunkToSend(imageName, version);
  std::cout << "ChunkToSend: " << reply << std::endl;

  //Step3: SendChunk()
  //std::string imageName = "ubuntuOS";
  std::string number = "0";
  std::string data = "12345678";
  std::string checksum = "0x1111111";
  for(int i = 0; i < 8; i++){
    reply = greeter.SendChunk(imageName, version, to_string(i), data, checksum);
    std::cout << "SendChunk: " << reply << std::endl;
    sleep(1);
  }
  reply = greeter.ChunkToSend(imageName, version);
  std::cout << "ChunkToSend: " << reply << std::endl;
  return 0;
}
