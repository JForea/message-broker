#include "ClientRegistry.hpp"

#include <message_broker/Exceptions.hpp>

namespace message_broker {
    
    void ClientRegistry::Add(int fd, const Guid& guid) {
        if (_guidByFd.contains(fd) || _fdByGuid.contains(guid))
            throw OccupiedIdException();

        _guidByFd.emplace(fd, guid);
        _fdByGuid.emplace(guid, fd);
    }

    void ClientRegistry::Remove(int fd) {
        auto it = _guidByFd.find(fd);
        
        if (it == _guidByFd.end())
            return;

        Guid guid = it->second;

        _fdByGuid.erase(guid);
        _guidByFd.erase(fd);
    }

    std::optional<int> ClientRegistry::FindFdByGuid(const Guid& guid) const {
        auto it = _fdByGuid.find(guid);

        if (it == _fdByGuid.end())
            return std::nullopt;

        return it->second;
    }

    std::optional<Guid> ClientRegistry::FindGuidByFd(int fd) const {
        auto it = _guidByFd.find(fd);

        if (it == _guidByFd.end())
            return std::nullopt;

        return it->second;
    }

    std::vector<int> ClientRegistry::GetBroadcastTargets(int senderFd) const {
        std::vector<int> broadcastTargets;

        for (const auto& [fd, _] : _guidByFd) {
            if (fd != senderFd)
                broadcastTargets.push_back(fd);
        }

        return broadcastTargets;
    }

}