//
// Created by cecil on 4/26/20.
//

#include <iostream>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>
#include <grpcpp/server_context.h>
#include "recover_service.pb.h"
#include "recover_service.grpc.pb.h"
#include <vector>
#include <set>

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

void recoverTheService(int img, int ver){

}

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace recoverer;

class svImpl final:public recover_service::Service{
    Status TellVersion(ServerContext* context, const Version* request, Reply* response) override;
    Status Chunk2Send(ServerContext* context, const Image* request, ChunkList* response)  override;
    Status SendChunk(ServerContext* context, const Chunk* request, Reply* response)  override;
    Status KeepAlive(ServerContext* context, const Reply* request, Reply* response) override;
    Status RecoverServ(ServerContext* context, const Image* request, Reply* response) override;
};

std::vector<int> images;
std::vector<int> steps;

std::vector<std::set<int>> chunkTable;
std::vector<FILE*> fileP;

char *zeroBuff;

Status svImpl::TellVersion(ServerContext *context, const Version *request, Reply *response) {
    int imN=request->image();
    int vN=request->version();
    if (vN!=images[imN]+1 && steps[imN]!=3) {
        response->set_status(9);
        return Status::OK;
    }
    else {
        std::string filename;
        if (vN==0) {
            filename="img_"+std::to_string(imN)+"_0";
        }
        else {
            filename="diff_"+std::to_string(imN)+"_"+std::to_string(vN);
        }
        fileP[imN]=fopen(filename.c_str(), "wb");
        int chunkN=request->size()/(1024*1024);
        for (int i=0; i<chunkN; i++) fwrite(zeroBuff, 1, 1024*1024, fileP[imN]);
        fwrite(zeroBuff, 1, request->size()%(1024*1024), fileP[imN]);
        chunkN=(request->size()+1024*1024-1)/(1024*1024);
        images[imN]++;
        steps[imN]=1;
        chunkTable[imN].clear();
        for (int i=0;i<chunkN;i++) chunkTable[imN].insert(i);
        response->set_status(8);
        return Status::OK;
    }
}

Status svImpl::Chunk2Send(ServerContext *context, const Image *request, ChunkList *response) {
    response->clear_needed();
    for (auto i:chunkTable[request->image()]){
        response->add_needed(i);
    }
    return Status::OK;
}

Status svImpl::SendChunk(ServerContext *context, const Chunk *request, Reply *response) {
    int imN=request->image();
    int vN=request->version();
    int cN=request->number();
    if (vN!=images[imN]) {
        response->set_status(9);
        return Status::OK;
    }
    if (chunkTable[imN].find(cN)==chunkTable[imN].end()) {
        response->set_status(9);
        return Status::OK;
    }
    chunkTable[imN].erase(cN);
    fseek(fileP[imN], 1024*1024*cN, SEEK_SET);
    fwrite(request->data().c_str(), 1, request->data().size(), fileP[imN]);
    if (chunkTable[imN].size()==0) {
        fclose(fileP[imN]);
        steps[imN]=2;
        if (images[imN]!=0) {
            //Patch
            char commandStr[1024];
            sprintf(commandStr, "bspatch img_%d_%d img_%d_%d diff_%d_%d", imN, vN-1, imN, vN, imN, vN);
            std::cout<<"Merging incremental data for Image#"<<imN<<", Version#"<<vN<<"\n\n";
            executeCMD(commandStr);
            std::cout<<"\n";

            //Delete Old Images
            if (vN!=1){
                sprintf(commandStr, "rm img_%d_%d", imN, vN-1);
                std::cout<<"Deleting old images\n\n";
                executeCMD(commandStr);
                std::cout<<"\n";
            }

        }
        steps[imN]=3;
    }
    return Status::OK;
}

Status svImpl::KeepAlive(ServerContext *context, const Reply *request, Reply *response) {
    response->set_status(8);
    return Status::OK;
}

Status svImpl::RecoverServ(ServerContext *context, const Image *request, Reply *response) {
    int img=request->image();
    int version=(steps[img]==3?images[img]:images[img]-1);
    if (version>=0) {
        std::cout<<"To recover "<<img<<" "<<version<<std::endl;
    }
    int vN=images[img];
    if (steps[img]!=3) vN--;
    if (vN==-1) return Status::OK;

    char commandStr[1024];
    sprintf(commandStr, "docker load --input img_%d_%d", img, vN);
    std::cout<<"Loading backup: Image#"<<img<<", Version#"<<vN<<"\n\n";
    executeCMD(commandStr);

    char servname[32]="kvrunning";
    sprintf(commandStr, "docker run --name serv_recv -p 4000:8000 %s:%d manage.py runserver 0.0.0.0:8000", servname, vN);
    std::cout<<"Loading backup: Image#"<<img<<", Version#"<<vN<<"\n\n";
    executeCMD(commandStr);
    std::cout<<"\n";

    return Status::OK;
}

int main(int argc, char** argv){
    if (argc!=2) {
        std::cout<<"recoverer [port]\n";
        return 0;
    }

    images.resize(3);
    images[1]=images[2]=-1;
    steps.resize(3);
    steps[1]=steps[2]=3;
    fileP.resize(3);
    chunkTable.resize(3);
    zeroBuff=new char[1024*1024];
    memchr(zeroBuff, 0, 1024*1024);

    svImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(std::string("0.0.0.0:")+argv[1], grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    server->Wait();
    delete[] zeroBuff;
    return 0;
}