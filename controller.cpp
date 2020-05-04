//
// Created by cecil on 4/23/20.
//

#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>

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

std::string containerID, imageName, recoverAddr;

void executeCMD(const char *cmd)
{
    std::cerr<<cmd<<std::endl;
    char* buf_ps=new char[1024];
    char* result=new char[65536];
    buf_ps[0]=0;
    result[0]=0;
    char ps[1024]={0};
    FILE *ptr;
    strcpy(ps, cmd);
    if((ptr=popen(ps, "r"))!=NULL)
    {
        while(fgets(buf_ps, 1024, ptr)!=NULL)
        {
            strcat(result, buf_ps);
            if(strlen(result)>65536)
                break;
        }
        pclose(ptr);
        ptr = NULL;
        std::cout<<result;
    }
    else
    {
        printf("popen %s error\n", ps);
    }
    delete[] buf_ps;
    delete[] result;
}

int main(int argc, char** argv) {
    if (argc!=4) {
        std::cout<<"controller [container ID] [image name] [recover node]\n";
        return 0;
    }
    containerID=argv[1];
    imageName=argv[2];
    recoverAddr=argv[3];

    auto channel=CreateChannel(recoverAddr, grpc::InsecureChannelCredentials());
    auto stub=recover_service::NewStub(channel);

    int imageN=2;

    //Iteration 0
    char commandStr[1024];
    char *buffer;
    buffer=new char[1024*1024];

    //Commit to image 0
    sprintf(commandStr, "docker commit %s %s:0", containerID.c_str(), imageName.c_str());
    std::cout<<"Committing to Image#"<<0<<"\n\n";
    executeCMD(commandStr);
    std::cout<<"\n";

    //Save Image
    sprintf(commandStr, "docker save -o img0 %s:0", imageName.c_str());
    std::cout<<"Saving Image #"<<0<<"\n\n";
    executeCMD(commandStr);
    std::cout<<"\n";

    FILE* p;
    std::string filename;
    filename="img0";
    p=fopen(filename.c_str(), "rb");
    if (p==nullptr) assert(false);

    Reply rpl;
    Version vs;
    vs.set_image(imageN);
    vs.set_version(0);
    fseek(p, 0, SEEK_END);
    int size=ftell(p);
    vs.set_size(size);
    int chunkNum=(size+1024*1024-1)/(1024*1024);

    Chunk ck;
    ChunkList ckl;

    rpl.set_status(9);
    while(rpl.status()!=8) {
        ClientContext cc2;
        stub->TellVersion(&cc2, vs, &rpl);
    }

    Image imgn;
    imgn.set_image(imageN);


    for (int ii=0;ii<chunkNum;ii++){
        int toSend=size-ii*1024*1024;
        if (toSend>1024*1024) toSend=1024*1024;
        fseek(p, ii*1024*1024, SEEK_SET);
        fread(buffer, 1, toSend, p);
        std::string bytes(buffer, toSend);
        ck.set_image(imageN);
        ck.set_version(0);
        ck.set_number(ii);
        ck.set_data(std::move(bytes));
        ck.set_checksum(0);
        ClientContext cc3;
        stub->SendChunk(&cc3, ck, &rpl);
    }

    ClientContext cc7;
    stub->Chunk2Send(&cc7, imgn, &ckl);
    while(ckl.needed_size()!=0) {
        for (auto ii:ckl.needed()){
            int toSend=size-ii*1024*1024;
            if (toSend>1024*1024) toSend=1024*1024;
            fseek(p, ii*1024*1024, SEEK_SET);
            fread(buffer, 1, toSend, p);
            std::string bytes(buffer, toSend);
            ck.set_image(imageN);
            ck.set_version(0);
            ck.set_data(std::move(bytes));
            ClientContext cc4;
            stub->SendChunk(&cc4, ck, &rpl);
        }
        ClientContext cc5;
        stub->Chunk2Send(&cc5, imgn, &ckl);
    }
    fclose(p);

    for (int i=1; i<2147483647; i++) {

        //Commit to image
        sprintf(commandStr, "docker commit %s %s:%d", containerID.c_str(), imageName.c_str(), i);
        std::cout<<"Committing to Image#"<<i<<"\n\n";
        executeCMD(commandStr);
        std::cout<<"\n";

        //Save Image
        sprintf(commandStr, "docker save -o img%d %s:%d", i, imageName.c_str(), i);
        std::cout<<"Saving Image #"<<i<<"\n\n";
        executeCMD(commandStr);
        std::cout<<"\n";

        //Diff
        sprintf(commandStr, "bsdiff img%d img%d diff%d", i-1, i, i);
        std::cout<<"Computing incremental data for Image#"<<i<<"\n\n";
        executeCMD(commandStr);
        std::cout<<"\n";

        //Removing old image
        sprintf(commandStr, "docker rmi %s:%d", imageName.c_str(), i-1);
        std::cout<<"Removing old image in docker.\n\n";
        executeCMD(commandStr);
        std::cout<<"\n";

        //Removing old image in files
        if (i!=1) {
            sprintf(commandStr, "rm img%d", i-1);
            std::cout<<"Removing old image in disk.\n\n";
            executeCMD(commandStr);
            std::cout<<"\n";
        }

        FILE* p;
        std::string filename;
        if (i==0) filename="img0"; else filename="diff"+std::to_string(i);
        p=fopen(filename.c_str(), "rb");
        if (p==nullptr) assert(false);

        Reply rpl;
        Version vs;
        vs.set_image(imageN);
        vs.set_version(i);
        fseek(p, 0, SEEK_END);
        int size=ftell(p);
        vs.set_size(size);
        int chunkNum=(size+1024*1024-1)/(1024*1024);

        rpl.set_status(9);
        while(rpl.status()!=8) {
            ClientContext cc6;
            stub->TellVersion(&cc6, vs, &rpl);
        }

        Chunk ck;
        ChunkList ckl;

        ClientContext cc8;
        stub->Chunk2Send(&cc8, imgn, &ckl);

        Image imgn;
        imgn.set_image(imageN);

        for (int ii=0;ii<chunkNum;ii++){
            int toSend=size-ii*1024*1024;
            if (toSend>1024*1024) toSend=1024*1024;
            fseek(p, ii*1024*1024, SEEK_SET);
            fread(buffer, 1, toSend, p);
            std::string bytes(buffer, toSend);
            ck.set_image(imageN);
            ck.set_version(i);
            ck.set_data(std::move(bytes));
            ClientContext cc9;
            stub->SendChunk(&cc9, ck, &rpl);
        }

        ClientContext cc10;
        stub->Chunk2Send(&cc10, imgn, &ckl);
        while(ckl.needed_size()!=0) {
            for (auto ii:ckl.needed()){
                int toSend=size-ii*1024*1024;
                if (toSend>1024*1024) toSend=1024*1024;
                fseek(p, ii*1024*1024, SEEK_SET);
                fread(buffer, 1, toSend,  p);
                std::string bytes(buffer, toSend);
                ck.set_image(imageN);
                ck.set_version(i);
                ck.set_number(ii);
                ck.set_data(std::move(bytes));
                ClientContext cc11;
                stub->SendChunk(&cc11, ck, &rpl);
            }
            ClientContext cc12;
            stub->Chunk2Send(&cc12, imgn, &ckl);
        }
        fclose(p);
    }

    delete[] buffer;
    return 0;
}
