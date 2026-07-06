#include "PacketIO.hpp"
#include "SocketIO.hpp"
#include "shared/Exceptions.hpp"

namespace {

    bool IsPayloadSizeInvalid(size_t payloadSize) {
        return payloadSize > message_broker::MaxPayloadSize;
    }

}

namespace message_broker {

    // ClientPacketWriter functions

    void ClientPacketWriter::WriteRegister(const Guid& guid) {
        _protocolWriter.WritePacketType(PacketType::Register);
        _protocolWriter.WriteGuid(guid);
    }

    void ClientPacketWriter::WriteSendMessage(const Guid& targetGuid, std::span<const uint8_t> payload) {
        if (IsPayloadSizeInvalid(payload.size()))
            throw PayloadTooLargeException();

        _protocolWriter.WritePacketType(PacketType::SendMessage);
        _protocolWriter.WriteGuid(targetGuid);
        _protocolWriter.WritePayloadSize(payload.size());
        _protocolWriter.WritePayload(payload);
    }

    void ClientPacketWriter::WriteBroadcast(std::span<const uint8_t> payload) {
        if (IsPayloadSizeInvalid(payload.size()))
            throw PayloadTooLargeException();

        _protocolWriter.WritePacketType(PacketType::Broadcast);
        _protocolWriter.WritePayloadSize(payload.size());
        _protocolWriter.WritePayload(payload);
    }

    // ServerPacketWriter functions

    void ServerPacketWriter::WriteAck() {
        _protocolWriter.WritePacketType(PacketType::Ack);
    }

    void ServerPacketWriter::WriteError(ErrorCode code) {
        _protocolWriter.WritePacketType(PacketType::Error);
        _protocolWriter.WriteErrorCode(code);
    }

    void ServerPacketWriter::WriteMessageHeader(const Guid& senderGuid, uint32_t size) {
        _protocolWriter.WritePacketType(PacketType::Message);
        _protocolWriter.WriteGuid(senderGuid);
        _protocolWriter.WritePayloadSize(size);
    }

    // PacketReader functions

    PacketType PacketReader::ReadPacketType() {
        return _protocolReader.ReadPacketType();
    }

    // ClientPacketReader functions

    ErrorCode ClientPacketReader::ReadError() {
        return _protocolReader.ReadErrorCode();
    }

    Message ClientPacketReader::ReadMessage() {
        Guid senderGuid = _protocolReader.ReadGuid();
        uint32_t size = _protocolReader.ReadPayloadSize();
        
        std::vector<uint8_t> buf(size);
        _protocolReader.ReadPayload(buf);

        return { senderGuid, buf };
    }

    // ServerPacketReader functions

    Guid ServerPacketReader::ReadRegister() {
        return _protocolReader.ReadGuid();
    }

    SendMessageHeader ServerPacketReader::ReadSendMessageHeader() {
        Guid guid = _protocolReader.ReadGuid();
        uint32_t payloadSize = _protocolReader.ReadPayloadSize();

        if (IsPayloadSizeInvalid(payloadSize))
            throw PayloadTooLargeException();

        return { guid, payloadSize };
    }
    
    uint32_t ServerPacketReader::ReadBroadcastHead() {
        uint32_t payloadSize = _protocolReader.ReadPayloadSize();

        if (IsPayloadSizeInvalid(payloadSize))
            throw PayloadTooLargeException();

        return payloadSize;
    }

}