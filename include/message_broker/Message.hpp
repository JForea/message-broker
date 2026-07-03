#pragma once

#include <vector>
#include <cstdint>
#include <message_broker/Guid.hpp>

struct Message {
	Guid senderId;
	std::vector<uint8_t> data;
};