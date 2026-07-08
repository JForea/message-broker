#include "BrokerClient.hpp"

#include <poll.h>

#include <message_broker/Exceptions.hpp>

#include "shared/Defaults.hpp"

namespace message_broker {
    
    BrokerClient::BrokerClient(const Guid& guid) :
            _guid(guid),
            _socket(ClientSocket(DefaultSocketPath)),
            _reader(ClientPacketReader(_socket.GetFd())),
            _writer(ClientPacketWriter(_socket.GetFd())) 
    {
        Register();
    }

    BrokerClient::BrokerClient(const Guid& guid, std::string_view socketPath) :
            _guid(guid),
            _socket(ClientSocket(socketPath)),
            _reader(ClientPacketReader(_socket.GetFd())),
            _writer(ClientPacketWriter(_socket.GetFd())) 
    {
        Register();
    }

    void BrokerClient::Register() {
        try {
            _writer.WriteRegister(_guid);
            WaitForAck();
        } catch (const std::runtime_error& e) {
            throw ConnectionException(e.what());
        }
    }

    void BrokerClient::WaitForAck() {
        try {
            PacketType type = _reader.ReadPacketType();

            if (type == PacketType::Ack)
                return;

            if (type == PacketType::Error)
                ThrowProtocolException(_reader.ReadError());

            throw InvalidPacketException();
        } catch (const std::runtime_error& e) {
            throw ConnectionException(e.what());
        }
    }

    void BrokerClient::SendMessage(
            const Guid& targetId, 
            std::span<const uint8_t> data
    ) {
        try {
            _writer.WriteSendMessage(targetId, data);
        } catch (const std::runtime_error& e) {
            throw ConnectionException(e.what());
        }
    }

    void BrokerClient::Broadcast(std::span<const uint8_t> data) {
        try {
            _writer.WriteBroadcast(data);
        } catch (const std::runtime_error& e) {
            throw ConnectionException(e.what());
        }
    }

    std::optional<Message> BrokerClient::TryGetMessage() {
        pollfd pfd{};
        pfd.fd = _socket.GetFd();
        pfd.events = POLLIN;

        int ready = poll(&pfd, 1, 0);

        if (ready == -1)
            throw ConnectionException("poll failed.");

        if (ready == 0)
            return std::nullopt;

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
            throw ConnectionException("Client socket is closed or invalid.");

        return GetMessage();
    }

    Message BrokerClient::GetMessage() {
        try {
            PacketType type = _reader.ReadPacketType();

            if (type == PacketType::Message)
                return _reader.ReadMessage();

            if (type == PacketType::Error)
                ThrowProtocolException(_reader.ReadError());

            throw InvalidPacketException();
        }
        catch (const std::runtime_error& e) {
            throw ConnectionException(e.what());
        }
    }

}
