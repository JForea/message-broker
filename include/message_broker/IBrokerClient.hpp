#pragma once

#include <span>
#include <optional>
#include <message_broker/Message.hpp>

namespace message_broker {

	class IBrokerClient {

	public:
		virtual const Guid& GetClientId() = 0;

		// Get file descriptor of the created socket.
		virtual int GetSocketFd() = 0;
		
		virtual void SendMessage(const Guid& targetId, std::span<const uint8_t> data) = 0;

		virtual void Broadcast(std::span<const uint8_t> data) = 0;

		// Non-blocking get message.
		virtual std::optional<Message> TryGetMessage() = 0;

		// Blocking get message.
		virtual Message GetMessage() = 0;
		
		virtual ~IBrokerClient() {}
	};

}