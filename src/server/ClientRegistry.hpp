#pragma once

#include <map>
#include <unordered_map>
#include <optional>
#include <vector>

#include <message_broker/Guid.hpp>

namespace message_broker {
    
    class ClientRegistry {
    private:
        std::unordered_map<int, Guid> _guidByFd;
        std::map<Guid, int> _fdByGuid;

    public:
        void Add(int fd, const Guid& guid);
        void Remove(int fd);

        std::optional<Guid> FindGuidByFd(int fd) const;
        std::optional<int> FindFdByGuid(const Guid& guid) const;

        std::vector<int> GetBroadcastTargets(int senderFd) const;
    };

}


