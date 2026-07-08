#pragma once

#include <vector>
#include <cstdint>
#include <message_broker/Guid.hpp>

namespace message_broker {

	struct Message {
		Guid senderId;
		std::vector<uint8_t> data;
	};
	
}