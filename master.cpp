//
// Created by cecil on 4/26/20.
//

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>
#include <grpcpp/server_context.h>
#include "recover_service.grpc.pb.h"
#include "recover_service.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace recoverer;

#include <vector>
#include <string>
#include <unistd.h>

std::vector<std::string> addr;
std::vector<int> recv_node;
std::vector<std::unique_ptr<recover_service::Stub>> stubs;
std::vector<std::shared_ptr<Channel>> channels;

std::vector<int> delay_times;
bool recovered[32];

int main() {
    FILE* config;
    config=fopen("config.txt", "r");
    char *buf1=new char[256];
    int n;
    fscanf(config, "%d\n", &n);
    addr.resize(n+1);
    recv_node.resize(n+1);
    stubs.resize(n+1);
    channels.resize(n+1);
    delay_times.resize(n+1);
    for (int i=1; i<=n; i++){
        fscanf(config, "%s %d", buf1, &recv_node[i]);
        addr[i]=buf1;
        channels[i]=CreateChannel(buf1, grpc::InsecureChannelCredentials());
        stubs[i]=recover_service::NewStub(channels[i]);
    }
    delete[] buf1;
    for (int i=1; i<=n; i++) {
        delay_times[i]=0;
        recovered[i]=false;
    }
    while(1) {
        sleep(1);
        for (int i=1; i<=n; i++){
            ClientContext cc;
            Reply rpl, rpl0;
            rpl.set_status(9);
            stubs[i]->KeepAlive(&cc, rpl0, &rpl);
            if (rpl.status()!=8) delay_times[i]++;
            else delay_times[i]=0;
        }
        for (int i=1; i<=n; i++){
            if (delay_times[i]>=3) {
                ClientContext cc;
                Reply rpl;
                Image img;
                img.set_image(i);
                stubs[recv_node[i]]->RecoverServ(&cc, img, &rpl);
            }
        }
        //break;
    }
}

