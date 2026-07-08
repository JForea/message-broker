#include "BrokerServerTests.hpp"

#include "server/BrokerServer.hpp"
#include "shared/io/PacketIO.hpp"

#include <message_broker/Guid.hpp>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>

namespace {

    constexpr const char* SocketPath = "/tmp/message-broker-integration-test.sock";

    int ConnectToServer() {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        assert(fd != -1);

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;

        strncpy(addr.sun_path, SocketPath, sizeof(addr.sun_path) - 1);

        assert(connect(
            fd,
            reinterpret_cast<sockaddr*>(&addr),
            sizeof(addr)
        ) == 0);

        return fd;
    }

}

void TestBrokerServerRegister() {
    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    std::thread serverThread([&server] {
        server.Run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    int clientFd = ConnectToServer();

    message_broker::Guid guid{0, 1, 2};

    {
        message_broker::ClientPacketWriter writer(clientFd);
        message_broker::ClientPacketReader reader(clientFd);

        writer.WriteRegister(guid);

        auto packetType = reader.ReadPacketType();
        assert(packetType == message_broker::PacketType::Ack);
    }

    close(clientFd);

    server.Stop();
    serverThread.join();

    std::filesystem::remove(SocketPath);
}
