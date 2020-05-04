cmake_minimum_required(VERSION 3.15)
project(autorecoverer)

find_package(protobuf REQUIRED)
find_package (Threads)

find_package(gRPC REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

get_target_property(gRPC_CPP_PLUGIN_EXECUTABLE gRPC::grpc_cpp_plugin
        IMPORTED_LOCATION_RELEASE)

add_executable(controller controller.cpp recover_service.grpc.pb.cc recover_service.pb.cc)
add_executable(recoverer recoverer.cpp recover_service.grpc.pb.cc recover_service.pb.cc)
add_executable(master master.cpp recover_service.pb.cc recover_service.grpc.pb.cc)
target_link_libraries(controller  gRPC::grpc++ protobuf )
target_link_libraries(recoverer gRPC::grpc++ protobuf)
target_link_libraries(master gRPC::grpc++ protobuf)