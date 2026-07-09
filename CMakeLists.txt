cmake_minimum_required(VERSION 3.20)

project(message-broker)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# SHARED LIBRARY

add_library(
    message-broker-shared STATIC
    src/shared/io/SocketIO.cpp
    src/shared/io/ProtocolIO.cpp
    src/shared/io/PacketIO.cpp
)

target_include_directories(
    message-broker-shared
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# SERVER EXECUTABLE

add_executable(
    message-broker-server
    src/server/main.cpp
    src/server/ClientRegistry.cpp
    src/server/ServerSocket.cpp
    src/server/BrokerServer.cpp
    src/server/ThreadPool.cpp
)

target_link_libraries(
    message-broker-server
    PRIVATE
        message-broker-shared
)

# CLIENT LIBRARY

add_library(
    message-broker-client STATIC
    src/client/BrokerClient.cpp
    src/client/ClientSocket.cpp
)

target_link_libraries(
    message-broker-client
    PRIVATE
        message-broker-shared
)

# TESTING

enable_testing()

add_executable(
    message-broker-tests
    tests/main.cpp

    tests/SocketIOTests.cpp
    tests/ProtocolIOTests.cpp
    tests/PacketIOTests.cpp
    tests/ServerSocketTests.cpp
    tests/BrokerServerTests.cpp
    tests/BrokerClientTests.cpp

    src/server/ServerSocket.cpp
    src/server/ClientRegistry.cpp
    src/server/BrokerServer.cpp
    src/server/ThreadPool.cpp
    
    src/client/BrokerClient.cpp
    src/client/ClientSocket.cpp
)

target_link_libraries(
    message-broker-tests
    PRIVATE
        message-broker-shared
)

target_compile_features(
    message-broker-tests
    PRIVATE
        cxx_std_23
)

add_test(
    NAME ApplicationTests
    COMMAND message-broker-tests
)
