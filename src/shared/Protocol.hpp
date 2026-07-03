#pragma once

#include <cstdint>

namespace message_broker {

    // Identifies the type of a protocol packet.
    // Always stored as the first byte of the packet.
    enum class PacketType : uint8_t {
        // Client-side
        Register = 0x00,
        SendMessage = 0x01,
        Broadcast = 0x02,

        // Server-side
        Message = 0x80,
        Error = 0x81
    };

    enum class ErrorCode : uint8_t {
        OccupiedId = 0x00,
        UnknownTargetId = 0x01,
        NoListeners = 0x02,
        InvalidPacket = 0x03,
        PayloadTooLarge = 0x04 
    };

    inline constexpr uint32_t MaxPayloadSize = 4 * 1024 * 1024;

}