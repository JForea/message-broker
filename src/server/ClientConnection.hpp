#pragma once

#include <mutex>

#include <message_broker/Guid.hpp>

namespace message_broker {
    
    struct ClientConnection {
        int fd;
        Guid guid;
        std::mutex writeMutex;

        ClientConnection(int fd, const Guid& guid) : fd(fd), guid(guid) {}
    };
    
}
