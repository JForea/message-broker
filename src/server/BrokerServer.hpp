#pragma once

#include "ServerSocket.hpp"
#include "ClientRegistry.hpp"

#include "shared/io/PacketIO.hpp"

namespace message_broker {
    
    class BrokerServer {
    private:
        ServerSocket _socket;
        ClientRegistry _clients;
        int _epollFd = -1;

        std::optional<int> AcceptClient();
        void HandleClientPacket(int fd);
        void HandleRegister(int fd);
        void HandleSendMessage(int fd, ServerPacketReader& reader);
        void HandleBroadcast(int fd, ServerPacketReader& reader);

        void AddToEpoll(int fd);
        void RemoveFromEpoll(int fd);
        void DisconnectClient(int fd);

    public:
        explicit BrokerServer(std::string_view socketPath);
        
        void Run();

        ~BrokerServer() noexcept;
    };

}
