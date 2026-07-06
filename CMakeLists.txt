cmake_minimum_required(VERSION 3.20)

project(message-broker)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# CLIENT LIBRARY

add_library(
    message-broker-client STATIC
    src/client/BrokerClient.cpp
    src/shared/io/SocketIO.cpp
    src/shared/io/ProtocolIO.cpp
    src/shared/io/PacketIO.cpp

    include/message_broker/IBrokerClient.hpp
    include/message_broker/ClientFactory.hpp
    include/message_broker/Guid.hpp
    include/message_broker/Message.hpp

    src/shared/Protocol.hpp
    src/shared/io/SocketIO.hpp
    src/shared/io/ProtocolIO.hpp
    src/shared/io/PacketIO.hpp
    src/shared/exceptions/ProtocolException.hpp
)

target_include_directories(
    message-broker-client
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src
)

# TESTING

enable_testing()

add_executable(
    message-broker-tests
    tests/main.cpp

    tests/SocketIOTests.cpp
    tests/ProtocolIOTests.cpp
    tests/PacketIOTests.cpp

    src/shared/io/SocketIO.cpp
    src/shared/io/ProtocolIO.cpp
    src/shared/io/PacketIO.cpp
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
    NAME ApplicationTests
    COMMAND message-broker-tests
)
