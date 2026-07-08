#pragma once

#include <string_view>

namespace message_broker {

    class ClientSocket {
    private:
        int _fd;

    public:
        explicit ClientSocket(std::string_view socketPath);

        int GetFd() const noexcept {
            return _fd;
        }

        ~ClientSocket() noexcept;
    };

}

