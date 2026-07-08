#pragma once

#include <message_broker/IBrokerClient.hpp>

#include "ClientSocket.hpp"
#include "shared/io/PacketIO.hpp"

namespace message_broker {

    class BrokerClient : public IBrokerClient {    
    private:
        Guid _guid;
        ClientSocket _socket;
        ClientPacketReader _reader;
        ClientPacketWriter _writer;

        void Register();

        void WaitForAck();

    public:
        explicit BrokerClient(const Guid& guid);

        const Guid& GetClientId() const noexcept override {
            return _guid;
        }

        int GetSocketFd() const noexcept override {
            return _socket.GetFd();
        }

        void SendMessage(const Guid& targetId, std::span<const uint8_t> data) override;

		void Broadcast(std::span<const uint8_t> data) override;

		std::optional<Message> TryGetMessage() override;

		Message GetMessage() override;
    };

}
