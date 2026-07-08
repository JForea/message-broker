#pragma once

#include "shared/Protocol.hpp"

#include <message_broker/Guid.hpp>
#include <message_broker/ErrorCode.hpp>

#include <cstdint>
#include <span>

namespace message_broker {

    class ProtocolWriter {
    private:
        int _fd;

    public:
        ProtocolWriter(int fd) : _fd(fd) {}

        void WritePacketType(PacketType type);
        void WriteErrorCode(ErrorCode code);
        void WriteGuid(const Guid& guid);
        void WritePayloadSize(uint32_t size);
        void WritePayload(std::span<const uint8_t> payload);
    };

    class ProtocolReader {
    private:
        int _fd;

    public:
        ProtocolReader(int fd) : _fd(fd) {}
        
        PacketType ReadPacketType();
        ErrorCode ReadErrorCode();
        Guid ReadGuid();
        uint32_t ReadPayloadSize();
        void ReadPayload(std::span<uint8_t> buf);
    };
    
}