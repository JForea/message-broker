#include "ProtocolIO.hpp"
#include "SocketIO.hpp"

namespace {

    template<typename T>
    std::span<const uint8_t> AsBytes(const T& value) {
        return {
            reinterpret_cast<const uint8_t*>(&value),
            sizeof(T)
        };
    }

    template<typename T>
    std::span<uint8_t> AsWritableBytes(T& value) {
        return {
            reinterpret_cast<uint8_t*>(&value),
            sizeof(T)
        };
    }

}

namespace message_broker {

    // ProtocolWriter functions

    void ProtocolWriter::WritePacketType(PacketType type) {
        WriteExact(_fd, AsBytes(type));
    }

    void ProtocolWriter::WriteErrorCode(ErrorCode code) {
        WriteExact(_fd, AsBytes(code));
    }

    void ProtocolWriter::WriteGuid(const Guid& guid) {
        WriteExact(_fd, AsBytes(guid));
    }

    void ProtocolWriter::WritePayloadSize(uint32_t size) {
        WriteExact(_fd, AsBytes(size));
    }

    void ProtocolWriter::WritePayload(std::span<const uint8_t> data) {
        WriteExact(_fd, data);
    }

    // ProtocolReader functions
    
    PacketType ProtocolReader::ReadPacketType() {
        PacketType type {};
        ReadExact(_fd, AsWritableBytes(type));
        return type;
    }

    ErrorCode ProtocolReader::ReadErrorCode() {
        ErrorCode code {};
        ReadExact(_fd, AsWritableBytes(code));
        return code;
    }

    Guid ProtocolReader::ReadGuid() {
        Guid guid {};
        ReadExact(_fd, AsWritableBytes(guid));
        return guid;
    }

    uint32_t ProtocolReader::ReadPayloadSize() {
        uint32_t size {};
        ReadExact(_fd, AsWritableBytes(size));
        return size;
    }

    void ProtocolReader::ReadPayload(std::span<uint8_t> buf) {
        ReadExact(_fd, buf);
    }

}