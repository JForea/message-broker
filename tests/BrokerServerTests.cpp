#include "BrokerServerTests.hpp"
#include "server/BrokerServer.hpp"

#include <cassert>
#include <filesystem>
#include <future>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

void TestBrokerServerStart() {
    auto SocketPath = "/tmp/test.sock";

    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    assert(std::filesystem::exists(SocketPath));

    std::filesystem::remove(SocketPath);
}

void TestBrokerServerAccept() {
    constexpr auto SocketPath = "/tmp/test.sock";

    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    auto future = std::async(std::launch::async, [&] {
        return server.Accept();
    });

    int client = socket(AF_UNIX, SOCK_STREAM, 0);
    assert(client != -1);

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SocketPath, sizeof(addr.sun_path) - 1);

    assert(connect(
        client,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr)
    ) == 0);

    int accepted = future.get();

    assert(accepted != -1);

    close(client);
    close(accepted);

    std::filesystem::remove(SocketPath);
}