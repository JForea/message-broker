#pragma once

#include <map>
#include <unordered_map>
#include <optional>
#include <vector>
#include <memory>

#include "ClientConnection.hpp"

namespace message_broker {
    
    class ClientRegistry {
    private:
        std::unordered_map<int, std::shared_ptr<ClientConnection>> _byFd;
        std::map<Guid, std::shared_ptr<ClientConnection>> _byGuid;

    public:
        void Add(int fd, const Guid& guid);
        void Remove(int fd);

        std::shared_ptr<ClientConnection> FindByFd(int fd) const;
        std::shared_ptr<ClientConnection> FindByGuid(const Guid& guid) const;

        std::vector<std::shared_ptr<ClientConnection>> GetBroadcastTargets(int senderFd) const;
    };

}


