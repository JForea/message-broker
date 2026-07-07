#pragma once

#include <string_view>

namespace message_broker {

    class ServerSocket {
    private:
        int _serverFd = -1;

    public:
        explicit ServerSocket(std::string_view socketPath);

        int Accept();

        ~ServerSocket() noexcept;
    };

}