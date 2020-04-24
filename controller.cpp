//
// Created by cecil on 4/23/20.
//

#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>

std::string containerID, imageName;

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
    if (argc!=3) {
        std::cout<<"controller [container ID] [image name] \n";
        return 0;
    }
    containerID=argv[1];
    imageName=argv[2];

    //Iteration 0
    char commandStr[1024];

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
    }

    return 0;
}
