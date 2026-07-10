#pragma once

#include <string_view>

namespace message_broker {

    class ClientSocket {
    private:
        int _fd;

        void Close() noexcept;

    public:
        explicit ClientSocket(std::string_view socketPath);

        ClientSocket(const ClientSocket&) = delete;
        ClientSocket& operator=(const ClientSocket&) = delete;

        ClientSocket(ClientSocket&& other) noexcept;
        ClientSocket& operator=(ClientSocket&& other) noexcept;

        int GetFd() const noexcept;

        ~ClientSocket() noexcept;
    };

}

