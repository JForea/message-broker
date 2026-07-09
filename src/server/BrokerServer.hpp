#pragma once

#include <atomic>

#include "ServerSocket.hpp"
#include "ClientRegistry.hpp"

#include "shared/io/PacketIO.hpp"

namespace message_broker {
    
    class BrokerServer {
    private:
        ServerSocket _socket;
        ClientRegistry _clients;
        int _epollFd = -1;
        std::atomic_bool _running { false };

        std::optional<int> AcceptClient();
        void HandleClientPacket(int fd);
        void HandleRegister(int fd);
        void HandleSendMessage(int fd, ServerPacketReader& reader);
        void HandleBroadcast(int fd, ServerPacketReader& reader);

        void UpdateEpoll(
            int fd,
            int operation,
            uint32_t events,
            const char* errorMessage
        );
        void AddServerToEpoll();
        void AddClientToEpoll(int fd);
        void RearmClientInEpoll(int fd);
        void RemoveFromEpoll(int fd) noexcept;

        void DisconnectClient(int fd) noexcept;

    public:
        explicit BrokerServer(std::string_view socketPath);
        
        void Run();

        void Stop() noexcept;

        ~BrokerServer() noexcept;
    };

}
