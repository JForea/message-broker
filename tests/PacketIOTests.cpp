#include "PacketIOTests.hpp"
#include "shared/io/PacketIO.hpp"
#include "shared/io/SocketIO.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <string_view>

void TestPacketRegister() {
    int fds[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    message_broker::Guid guid { 0, 1, 2 };

    message_broker::ClientPacketWriter writer(fds[0]);
    message_broker::ServerPacketReader reader(fds[1]);

    writer.WriteRegister(guid);

    assert(reader.ReadPacketType() == message_broker::PacketType::Register);
    assert(reader.ReadRegister() == guid);

    close(fds[0]);
    close(fds[1]);
}

void TestPacketSendMessage() {
    int fds[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    message_broker::Guid target {0, 1, 2};
    std::array<std::uint8_t, 4> payload { 0, 10, 20, 30 };

    message_broker::ClientPacketWriter writer(fds[0]);
    message_broker::ServerPacketReader reader(fds[1]);

    writer.WriteSendMessage(target, payload);

    assert(reader.ReadPacketType() == message_broker::PacketType::SendMessage);

    auto header = reader.ReadSendMessageHeader();

    assert(header.target == target);
    assert(header.payloadSize == payload.size());

    std::array<std::uint8_t, 4> received{};
    message_broker::ReadExact(fds[1], received);

    assert(received == payload);

    close(fds[0]);
    close(fds[1]);
}

void TestPacketServerMessage() {
    int fds[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    message_broker::Guid sender { 9, 8, 7 };
    std::array<std::uint8_t, 3> payload { 0, 1, 2 };

    message_broker::ServerPacketWriter writer(fds[0]);
    message_broker::ClientPacketReader reader(fds[1]);

    writer.WriteMessageHeader(sender, payload.size());
    message_broker::WriteExact(fds[0], payload);

    assert(reader.ReadPacketType() == message_broker::PacketType::Message);

    auto message = reader.ReadMessage();

    assert(message.senderId == sender);
    assert(message.data == std::vector<std::uint8_t>(payload.begin(), payload.end()));

    close(fds[0]);
    close(fds[1]);
}
