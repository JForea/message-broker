#include "BrokerClientTests.hpp"

#include "client/BrokerClient.hpp"
#include "server/BrokerServer.hpp"

#include <message_broker/Guid.hpp>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <thread>

namespace {

    constexpr char* SocketPath = "/tmp/message-broker-client-test.sock";

}

void TestBrokerClientRegister() {
    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    std::thread serverThread([&server] {
        server.Run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    message_broker::Guid guid{1, 2, 3};

    {
        message_broker::BrokerClient client(guid, SocketPath);
        assert(client.GetClientId() == guid);
    }

    server.Stop();
    serverThread.join();

    std::filesystem::remove(SocketPath);
}