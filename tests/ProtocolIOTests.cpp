#include "shared/ProtocolIO.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <cassert>
#include <cstdint>
#include <stdexcept>

void TestProtocolReadWrite()
{
    int fds[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    message_broker::ProtocolWriter writer(fds[0]);
    message_broker::ProtocolReader reader(fds[1]);

    message_broker::Guid guid{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

    writer.WritePacketType(message_broker::PacketType::Register);
    writer.WriteGuid(guid);
    writer.WritePayloadSize(4);

    std::array<std::uint8_t, 4> payload{1,2,3,4};
    writer.WritePayload(payload);

    assert(reader.ReadPacketType() == message_broker::PacketType::Register);
    assert(reader.ReadGuid() == guid);
    assert(reader.ReadPayloadSize() == 4);

    std::array<std::uint8_t, 4> received{};
    reader.ReadPayload(received);
    assert(received == payload);

    close(fds[0]);
    close(fds[1]);
}