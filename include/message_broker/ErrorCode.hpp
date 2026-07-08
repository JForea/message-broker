#pragma once

#include <cstdint>

namespace message_broker {

    enum class ErrorCode : uint8_t {
        OccupiedId = 0x00,
        UnknownTargetId = 0x01,
        InvalidPacket = 0x02,
        PayloadTooLarge = 0x03
    };

} 