#pragma once

#include <string_view>

namespace message_broker {

    class ServerSocket {
    private:
        int _fd = -1;

    public:
        explicit ServerSocket(std::string_view socketPath);

        int GetFd() const {
            return _fd;
        }

        // Accepts new connection from client
        int Accept();

        ~ServerSocket() noexcept;
    };

}