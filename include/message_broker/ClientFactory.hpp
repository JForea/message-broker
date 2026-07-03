#pragma once

#include <memory>
#include <message_broker/IBrokerClient.hpp>
#include <message_broker/Guid.hpp>

namespace message_broker {
    class ClientFactory {
        static std::unique_ptr<IBrokerClient> CreateBrokerClient(Guid guid);
    };
}