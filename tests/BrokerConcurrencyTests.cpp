#include "BrokerConcurrencyTests.hpp"

#include "server/BrokerServer.hpp"
#include "shared/io/PacketIO.hpp"
#include "client/BrokerClient.hpp"

#include <message_broker/Guid.hpp>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <thread>

namespace {

    constexpr const char* SocketPath = "/tmp/message-broker-integration-test.sock";

}

void TestConcurrentClientRegistration() {
    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    std::thread serverThread([&server] {
        server.Run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    constexpr int ClientCount = 32;
    std::vector<std::thread> clientThreads;
    std::vector<std::unique_ptr<message_broker::BrokerClient>> clients(ClientCount);

    for (int i = 0; i < ClientCount; ++i) {
        clientThreads.emplace_back([&, i] {
            message_broker::Guid guid{
                static_cast<uint64_t>(i),
                static_cast<uint64_t>(i + 1),
                static_cast<uint64_t>(i + 2)
            };

            clients[i] = std::make_unique<message_broker::BrokerClient>(guid, SocketPath);
            assert(clients[i]->GetClientId() == guid);
        });
    }

    for (auto& thread : clientThreads)
        thread.join();

    clients.clear();

    server.Stop();
    serverThread.join();

    std::filesystem::remove(SocketPath);
}

void TestConcurrentSendMessagesToSameTarget() {
    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    std::thread serverThread([&server] {
        server.Run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    message_broker::Guid targetGuid{100, 100, 100};
    message_broker::BrokerClient target(targetGuid, SocketPath);

    constexpr int SenderCount = 8;
    constexpr int MessagesPerSender = 16;

    std::vector<std::unique_ptr<message_broker::BrokerClient>> senders;
    senders.reserve(SenderCount);

    std::vector<message_broker::Guid> senderGuids;
    senderGuids.reserve(SenderCount);

    for (int i = 0; i < SenderCount; ++i) {
        message_broker::Guid guid{
            static_cast<uint64_t>(i),
            static_cast<uint64_t>(i + 10),
            static_cast<uint64_t>(i + 20)
        };

        senderGuids.push_back(guid);
        senders.push_back(std::make_unique<message_broker::BrokerClient>(guid, SocketPath));
    }

    std::vector<std::thread> threads;

    for (int i = 0; i < SenderCount; ++i) {
        threads.emplace_back([&, i] {
            std::vector<uint8_t> payload(256, static_cast<uint8_t>(i));

            for (int j = 0; j < MessagesPerSender; ++j)
                senders[i]->SendMessage(targetGuid, payload);
        });
    }

    for (auto& thread : threads)
        thread.join();

    for (int i = 0; i < SenderCount * MessagesPerSender; ++i) {
        auto message = target.GetMessage();

        assert(message.data.size() == 256);

        uint8_t value = message.data[0];

        for (uint8_t byte : message.data)
            assert(byte == value);
    }

    senders.clear();

    server.Stop();
    serverThread.join();

    std::filesystem::remove(SocketPath);
}

void TestConcurrentBroadcastsDoNotDeadlock() {
    std::filesystem::remove(SocketPath);

    message_broker::BrokerServer server(SocketPath);

    std::thread serverThread([&server] {
        server.Run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    constexpr int ClientCount = 6;
    constexpr int BroadcastsPerClient = 8;

    std::vector<std::unique_ptr<message_broker::BrokerClient>> clients;
    clients.reserve(ClientCount);

    std::vector<message_broker::Guid> guids;
    guids.reserve(ClientCount);

    for (int i = 0; i < ClientCount; ++i) {
        message_broker::Guid guid{
            static_cast<uint64_t>(i + 1),
            static_cast<uint64_t>(i + 2),
            static_cast<uint64_t>(i + 3)
        };

        guids.push_back(guid);
        clients.push_back(std::make_unique<message_broker::BrokerClient>(guid, SocketPath));
    }

    std::vector<std::thread> threads;

    for (int i = 0; i < ClientCount; ++i) {
        threads.emplace_back([&, i] {
            std::vector<uint8_t> payload(128, static_cast<uint8_t>(i));

            for (int j = 0; j < BroadcastsPerClient; ++j)
                clients[i]->Broadcast(payload);
        });
    }

    for (auto& thread : threads)
        thread.join();

    const int expectedMessagesPerClient = (ClientCount - 1) * BroadcastsPerClient;

    for (int i = 0; i < ClientCount; ++i) {
        for (int j = 0; j < expectedMessagesPerClient; ++j) {
            auto message = clients[i]->GetMessage();

            assert(message.data.size() == 128);

            uint8_t value = message.data[0];

            for (uint8_t byte : message.data)
                assert(byte == value);
        }
    }

    clients.clear();

    server.Stop();
    serverThread.join();

    std::filesystem::remove(SocketPath);
}
