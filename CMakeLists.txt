cmake_minimum_required(VERSION 3.20)

project(message-broker)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# CLIENT LIBRARY

add_library(
    message-broker-client STATIC
    src/client/BrokerClient.cpp
    src/shared/SocketIO.cpp

    include/message_broker/IBrokerClient.hpp
    include/message_broker/ClientFactory.hpp
    include/message_broker/Guid.hpp
    include/message_broker/Message.hpp

    src/shared/Protocol.hpp
    src/shared/SocketIO.hpp
)

target_include_directories(
    message-broker-client
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

# TESTING

enable_testing()

add_executable(
    message-broker-tests
    tests/main.cpp
    tests/SocketIOTests.cpp
    src/shared/SocketIO.cpp
)

target_include_directories(
    message-broker-tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(
    message-broker-tests
    PRIVATE
        cxx_std_23
)

add_test(
    NAME SocketIOTests
    COMMAND message-broker-tests
)
