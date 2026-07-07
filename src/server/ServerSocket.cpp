#include "ServerSocket.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace message_broker {
    
    ServerSocket::ServerSocket(std::string_view socketPath) {
        _serverFd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (_serverFd == -1)
            throw std::runtime_error("Failed to start server. Couldn't create server socket.");

        unlink(socketPath.data());

        sockaddr_un addr {};
        addr.sun_family = AF_UNIX;

        if (socketPath.size() >= sizeof(addr.sun_path))
            throw std::runtime_error("Socket path is too long.");

        std::strncpy(addr.sun_path, socketPath.data(), sizeof(addr.sun_path) - 1);

        if (bind(_serverFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1)
            throw std::runtime_error("Failed to bind server socket.");

        if (listen(_serverFd, SOMAXCONN) == -1)
            throw std::runtime_error("Failed to listen on server socket.");
    }

    int ServerSocket::Accept() {
        int clientFd = accept(_serverFd, nullptr, nullptr);
        if (clientFd == -1)
            throw std::runtime_error("Failed to accept client connection.");

        return clientFd;
    }

    ServerSocket::~ServerSocket() noexcept {
        if (_serverFd != -1)
            close(_serverFd);
    }

}