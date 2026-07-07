#pragma once

#include <string_view>

namespace message_broker {

    class BrokerServer {
    private:
        int _serverFd = -1;

    public:
        explicit BrokerServer(std::string_view socketPath);

        int Accept();

        ~BrokerServer() noexcept;
    };

}