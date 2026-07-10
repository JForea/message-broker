#pragma once

#include <cstddef>
#include <span>
#include <vector>
#include <unordered_set>
#include <cstdint>

#include "Pipeline.hpp"

namespace message_broker {

    // Reads exactly buf.size() bytes from the socket.
    // Throws if the connection is closed or a read error occurs.
    void ReadExact(int fd, std::span<uint8_t> buf, uint32_t size = 0);

    // Writes exactly buf.size() bytes to the socket.
    // Throws if a write error occurs.
    void WriteExact(int fd, std::span<const uint8_t> buf, uint32_t size = 0);

    // Writes exactly buf.size() bytes to targets.
    std::unordered_set<int> BroadcastExact(int fromFd, std::vector<int> toFds, uint32_t payloadSize);

    // Sends exactly size bytes from one socket to another using splice().
    // Throws if the connection is closed or a read error occurs.
    void SpliceExact(int fromFd, int toFd, uint32_t size, const Pipeline& pipeline);

}