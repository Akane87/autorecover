# Tiny Social Network Service
### Description
The objective of this assignment is to exercise your knowledge about Google Protocol Buffers and gRPC by building a Tiny SNS, 
similar in concept with posting and receiving status updates on Facebook or Twitter.

### Compile
Compile:
    make

Clean:
    make clean

### Run

Run server on the router:

    ./tsd -t router -p <router_port> -h <router_ip>
    
Master & slave connect to the router:

    ./tsd -t master -q <router_ip> -r <router_port> -s slave -h <host_addr> -p <master_port>
    
Run the client to connect to router:

    ./tsc -h <router_ip> -p <router_port> -u user1
 

