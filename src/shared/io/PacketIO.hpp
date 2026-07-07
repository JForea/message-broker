#pragma once

#include <message_broker/Message.hpp>
#include "ProtocolIO.hpp"

namespace message_broker {

    class PacketWriter {
    protected:
        ProtocolWriter _protocolWriter;

    public:
        explicit PacketWriter(int fd) noexcept : _protocolWriter(fd) {}
    };

    class ClientPacketWriter : public PacketWriter {
    public:
        ClientPacketWriter(int fd) noexcept : PacketWriter(fd) {}

        void WriteRegister(const Guid& guid);
        void WriteSendMessage(const Guid& targetGuid, std::span<const uint8_t> payload);
        void WriteBroadcast(std::span<const uint8_t> payload);
    };
    
    class ServerPacketWriter : public PacketWriter {
    public:
        ServerPacketWriter(int fd) noexcept : PacketWriter(fd) {}

        void WriteAck();
        void WriteError(ErrorCode code);
        void WriteMessageHeader(const Guid& senderGuid, uint32_t size);
    };

    class PacketReader {
    protected:
        ProtocolReader _protocolReader;

    public:
        explicit PacketReader(int fd) noexcept : _protocolReader(fd) {}

        PacketType ReadPacketType();
    };

    class ClientPacketReader : public PacketReader {
    public:
        ClientPacketReader(int fd) noexcept : PacketReader(fd) {}

        ErrorCode ReadError();
        Message ReadMessage();
    };

    struct SendMessageHeader {
        Guid target;
        uint32_t payloadSize;
    };

    class ServerPacketReader : public PacketReader {
    public:
        ServerPacketReader(int fd) noexcept : PacketReader(fd) {}

        Guid ReadRegister();
        SendMessageHeader ReadSendMessageHeader();
        uint32_t ReadBroadcastHeader();
    };

}