#pragma once

#include <optional>
#include <memory>

#include "client/ClientSocket.hpp"
#include "shared/io/PacketIO.hpp"

namespace message_broker {

    class BrokerClient {    
    private:
        Guid _guid;
        ClientSocket _socket;
        ClientPacketReader _reader;
        ClientPacketWriter _writer;

        void Register();

        void WaitForAck();

    public:
        explicit BrokerClient(const Guid& guid, std::string_view socketPath);

        BrokerClient(const BrokerClient&) = delete;
        BrokerClient& operator=(const BrokerClient&) = delete;

        BrokerClient(BrokerClient&& other);
        BrokerClient& operator=(BrokerClient&& other);

        const Guid& GetClientId() const noexcept;

        int GetSocketFd() const;

        void SendMessage(const Guid& targetId, std::span<const uint8_t> data);

		void Broadcast(std::span<const uint8_t> data);

		std::optional<Message> TryGetMessage();

		Message GetMessage();
    };

}
