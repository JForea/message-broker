#include "ClientSocket.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace message_broker {

    void ClientSocket::Close() noexcept {
        if (_fd != -1)
            close(_fd);
    }
    
    ClientSocket::ClientSocket(std::string_view socketPath) {
        _fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (_fd == -1)
            throw std::runtime_error("Failed to create client socket.");

        sockaddr_un addr {};
        addr.sun_family = AF_UNIX;

        if (socketPath.size() >= sizeof(addr.sun_path))
            throw std::runtime_error("Socket path is too long.");

        std::strncpy(addr.sun_path, socketPath.data(), sizeof(addr.sun_path) - 1);

        if (connect(_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1)
            throw std::runtime_error("Failed to connect to server socket.");
    }

    ClientSocket::ClientSocket(ClientSocket&& other) noexcept : _fd(other._fd) {
        other._fd = -1;
    }
    
    ClientSocket& ClientSocket::operator=(ClientSocket&& other) noexcept {
        if (this != &other) {
            Close();

            _fd = other._fd;
            other._fd = -1;
        }

        return *this;
    }

    int ClientSocket::GetFd() const noexcept {
            return _fd;
    }

    ClientSocket::~ClientSocket() noexcept {
        Close();
    }

}