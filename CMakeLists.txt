cmake_minimum_required(VERSION 3.20)

project(message-broker)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(
    message-broker-client STATIC
    src/client/BrokerClient.cpp
)

target_include_directories(
    message-broker-client
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)
