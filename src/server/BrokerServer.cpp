#include "BrokerServer.hpp"

#include "shared/io/SocketIO.hpp"

#include <message_broker/Exceptions.hpp>

#include <unistd.h>
#include <sys/epoll.h>

#include <cerrno>
#include <stdexcept>

namespace message_broker {

    BrokerServer::BrokerServer(std::string_view socketPath) : _socket(socketPath) {
        _epollFd = epoll_create1(EPOLL_CLOEXEC);
        if (_epollFd == -1)
            throw std::runtime_error("Failed to create epoll instance.");

        AddToEpoll(_socket.GetFd());
    }

    BrokerServer::~BrokerServer() noexcept {
        if (_epollFd != -1)
            close(_epollFd);
    }
    
    std::optional<int> BrokerServer::AcceptClient() {
        int fd = _socket.Accept();

        try {
            HandleRegister(fd);

            return fd;
        } catch (const ProtocolException& e) {
            try {
                ServerPacketWriter writer(fd);
                writer.WriteError(e.GetErrorCode());
            } catch (...) {}

            _clients.Remove(fd);
            close(fd);

            return std::nullopt;
        }
    }

    void BrokerServer::HandleClientPacket(int fd) {
        ServerPacketReader reader(fd);

        PacketType type = reader.ReadPacketType();

        switch (type) {
        case PacketType::SendMessage:
            HandleSendMessage(fd, reader);
            break;

        case PacketType::Broadcast:
            HandleBroadcast(fd, reader);
            break;
        
        default:
            throw InvalidPacketException();
        }
    }

    void BrokerServer::HandleRegister(int fd) {
        ServerPacketReader reader(fd);

        PacketType type = reader.ReadPacketType();

        if (type != PacketType::Register)
            throw InvalidPacketException();

        Guid guid = reader.ReadRegister();

        _clients.Add(fd, guid);

        ServerPacketWriter writer(fd);

        writer.WriteAck();
    }

    void BrokerServer::HandleSendMessage(int fd, ServerPacketReader& reader) {
        SendMessageHeader header = reader.ReadSendMessageHeader();

        auto senderGuidOpt = _clients.FindGuidByFd(fd);
        if (!senderGuidOpt)
            throw std::runtime_error("Couldn't find sender GUID during SendMessage.");

        auto targetFdOpt = _clients.FindFdByGuid(header.target);
        if (!targetFdOpt)
            throw UnknownTargetIdException();

        int targetFd = targetFdOpt.value();

        try {
            ServerPacketWriter writer(targetFd);        
            writer.WriteMessageHeader(senderGuidOpt.value(), header.payloadSize);

            SpliceExact(fd, targetFd, header.payloadSize);
        } catch (const std::runtime_error&) {
            DisconnectClient(targetFd);
        }
    }

    void BrokerServer::HandleBroadcast(int fd, ServerPacketReader& reader) {
        uint32_t payloadSize = reader.ReadBroadcastHeader();

        auto senderGuidOpt = _clients.FindGuidByFd(fd);
        if (!senderGuidOpt)
            throw std::runtime_error("Couldn't find sender GUID during Broadcast.");

        auto targetFds = _clients.GetBroadcastTargets(fd);

        for (auto targetFd : targetFds) {
            ServerPacketWriter writer(targetFd);

            writer.WriteMessageHeader(senderGuidOpt.value(), payloadSize);
        }

        auto failedFds = TeeExact(fd, targetFds, payloadSize);

        for (int failedFd : failedFds)
            DisconnectClient(failedFd);
    }

    void BrokerServer::AddToEpoll(int fd) {
        epoll_event event {};
        event.events = EPOLLIN;
        event.data.fd = fd;

        if (epoll_ctl(_epollFd, EPOLL_CTL_ADD, fd, &event) == -1)
            throw std::runtime_error("Failed to add file descriptor to epoll.");
    }

    void BrokerServer::RemoveFromEpoll(int fd) {
        if (::epoll_ctl(_epollFd, EPOLL_CTL_DEL, fd, nullptr) == -1)
            throw std::runtime_error("Failed to remove file descriptor from epoll.");
    }

    void BrokerServer::DisconnectClient(int fd) {
        RemoveFromEpoll(fd);
        _clients.Remove(fd);
        close(fd);
    }

    void BrokerServer::Run() {
        const int maxEventsSize = 64;
        
        while (true) {
            epoll_event events[maxEventsSize];

            int count = epoll_wait(_epollFd, events, maxEventsSize, -1);

            if (count == -1) {
                if (errno == EINTR)
                    continue;

                throw std::runtime_error("epoll_wait failed.");
            }

            for (int i = 0; i < count; ++i) {
                int fd = events[i].data.fd;

                if (fd == _socket.GetFd()) {
                    auto clientFd = AcceptClient();

                    if (clientFd)
                        AddToEpoll(clientFd.value());
                } else {
                    try {
                        HandleClientPacket(fd);
                    } catch (const ProtocolException& e) {
                        try {
                            ServerPacketWriter writer(fd);
                            writer.WriteError(e.GetErrorCode());
                        } catch (const std::runtime_error&) {
                            DisconnectClient(fd);
                        }
                    } catch (const std::runtime_error&) {
                        // Close connection only if IO error has occured.
                        DisconnectClient(fd);
                    }
                }
            }
        }
    }

}
