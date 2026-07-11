#pragma once

#include <cstdint>

enum class Command : uint8_t {
    Help = 0,
    Get = 1,
    TryGet = 2,
    Send = 3,
    Broadcast = 4,
    Unrecognized = 5,
    Guid = 6,
    Stop = 7
};