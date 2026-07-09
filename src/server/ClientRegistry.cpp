#include "ClientRegistry.hpp"

#include <message_broker/Exceptions.hpp>

namespace message_broker {
    
    void ClientRegistry::Add(int fd, const Guid& guid) {
        std::unique_lock lock(_mutex);

        if (_byFd.contains(fd) || _byGuid.contains(guid))
            throw OccupiedIdException();

        _byFd.emplace(fd, std::make_shared<ClientConnection>(fd, guid));
        _byGuid.emplace(guid, std::make_shared<ClientConnection>(fd, guid));
    }

    std::shared_ptr<ClientConnection> ClientRegistry::Remove(int fd) {
        std::unique_lock lock(_mutex);

        auto it = _byFd.find(fd);
        
        if (it == _byFd.end())
            return nullptr;

        auto connection = it->second;

        _byGuid.erase(connection->guid);
        _byFd.erase(fd);

        return connection;
    }

    std::shared_ptr<ClientConnection> ClientRegistry::FindByGuid(const Guid& guid) const {
        std::shared_lock lock(_mutex);
        
        auto it = _byGuid.find(guid);

        if (it == _byGuid.end())
            return nullptr;

        return it->second;
    }

    std::shared_ptr<ClientConnection> ClientRegistry::FindByFd(int fd) const {
        std::shared_lock lock(_mutex);

        auto it = _byFd.find(fd);

        if (it == _byFd.end())
            return nullptr;

        return it->second;
    }

    std::vector<std::shared_ptr<ClientConnection>> ClientRegistry::GetBroadcastTargets(int senderFd) const {
        std::shared_lock lock(_mutex);
        
        std::vector<std::shared_ptr<ClientConnection>> broadcastTargets;

        for (const auto& [_, conn] : _byFd) {
            if (conn->fd != senderFd)
                broadcastTargets.push_back(conn);
        }

        return broadcastTargets;
    }

}