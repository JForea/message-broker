#include "ClientRegistry.hpp"

#include <message_broker/Exceptions.hpp>

namespace message_broker {
    
    void ClientRegistry::Add(int fd, const Guid& guid) {
        if (_byFd.contains(fd) || _byGuid.contains(guid))
            throw OccupiedIdException();

        _byFd.emplace(fd, std::make_shared<ClientConnection>(fd, guid));
        _byGuid.emplace(guid, std::make_shared<ClientConnection>(fd, guid));
    }

    void ClientRegistry::Remove(int fd) {
        auto it = _byFd.find(fd);
        
        if (it == _byFd.end())
            return;

        Guid guid = it->second->guid;

        _byGuid.erase(guid);
        _byFd.erase(fd);
    }

    std::shared_ptr<ClientConnection> ClientRegistry::FindByGuid(const Guid& guid) const {
        auto it = _byGuid.find(guid);

        if (it == _byGuid.end())
            return nullptr;

        return it->second;
    }

    std::shared_ptr<ClientConnection> ClientRegistry::FindByFd(int fd) const {
        auto it = _byFd.find(fd);

        if (it == _byFd.end())
            return nullptr;

        return it->second;
    }

    std::vector<std::shared_ptr<ClientConnection>> ClientRegistry::GetBroadcastTargets(int senderFd) const {
        std::vector<std::shared_ptr<ClientConnection>> broadcastTargets;

        for (const auto& [_, conn] : _byFd) {
            if (conn->fd != senderFd)
                broadcastTargets.push_back(conn);
        }

        return broadcastTargets;
    }

}