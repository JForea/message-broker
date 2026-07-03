#pragma once

#include <cstddef>
#include <span>
#include <cstdint>

namespace message_broker {

    // Reads exactly buf.size() bytes from the socket.
    // Throws if the connection is closed or a read error occurs.
    void ReadExact(int fd, std::span<uint8_t> buf);

    // Writes exactly buf.size() bytes to the socket.
    // Throws if a write error occurs.
    void WriteExact(int fd, std::span<uint8_t> buf);

}