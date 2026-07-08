#include "BrokerServer.hpp"

#include "shared/io/SocketIO.hpp"

#include <message_broker/Exceptions.hpp>

#include <unistd.h>
#include <sys/epoll.h>

#include <cerrno>
#include <stdexcept>
#include <algorithm>

namespace {

    std::vector<int> GetTargetFds(
        const std::vector<std::shared_ptr<message_broker::ClientConnection>>& connections
    ) {
        std::vector<int> targetFds;
        targetFds.reserve(connections.size());

        for (const auto& connection : connections)
            targetFds.push_back(connection->fd);

        return targetFds;
    }

}

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

        auto senderConnection = _clients.FindByFd(fd);
        if (!senderConnection)
            throw std::runtime_error("Couldn't find sender GUID during SendMessage.");

        auto targetConnection = _clients.FindByGuid(header.target);
        if (!targetConnection)
            throw UnknownTargetIdException();

        std::lock_guard lock(targetConnection->writeMutex);

        int targetFd = targetConnection->fd;
        try {
            ServerPacketWriter writer(targetFd);        
            writer.WriteMessageHeader(senderConnection->guid, header.payloadSize);

            SpliceExact(fd, targetFd, header.payloadSize);
        } catch (const std::runtime_error&) {
            DisconnectClient(targetFd);
        }
    }

    void BrokerServer::HandleBroadcast(int fd, ServerPacketReader& reader) {
        uint32_t payloadSize = reader.ReadBroadcastHeader();

        auto senderConnection = _clients.FindByFd(fd);
        if (!senderConnection)
            throw std::runtime_error("Couldn't find sender GUID during Broadcast.");

        auto targetConnections = _clients.GetBroadcastTargets(fd);

        // We lock multiple client write mutexes below.
        // Always acquire them in the same order to avoid deadlocks between
        // concurrent broadcasts with overlapping target sets.
        std::sort(
            targetConnections.begin(),
            targetConnections.end(),
            [](const auto& lhs, const auto& rhs) {
                return lhs->fd < rhs->fd;
            }
        );

        std::vector<std::unique_lock<std::mutex>> locks;
        locks.reserve(targetConnections.size());

        for (const auto& connection : targetConnections)
            locks.emplace_back(connection->writeMutex);

        std::vector<int> targetFds = GetTargetFds(targetConnections);

        for (const auto& connection : targetConnections) {
            ServerPacketWriter writer(connection->fd);
            writer.WriteMessageHeader(senderConnection->guid, payloadSize);
        }

        auto failedFds = TeeExact(fd, targetFds, payloadSize);

        for (int failedFd : failedFds)
            DisconnectClient(failedFd);
    }

    void BrokerServer::AddToEpoll(int fd) {
        epoll_event event {};
        event.events = EPOLLIN | EPOLLRDHUP;
        event.data.fd = fd;

        if (epoll_ctl(_epollFd, EPOLL_CTL_ADD, fd, &event) == -1)
            throw std::runtime_error("Failed to add file descriptor to epoll.");
    }

    void BrokerServer::RemoveFromEpoll(int fd) {
        if (_epollFd == -1)
            return;

        epoll_ctl(_epollFd, EPOLL_CTL_DEL, fd, nullptr);
    }

    void BrokerServer::DisconnectClient(int fd) {
        RemoveFromEpoll(fd);
        _clients.Remove(fd);
        close(fd);
    }

    void BrokerServer::Run() {
        _running.store(true);
        const int maxEventsSize = 64;
        
        while (_running.load()) {
            epoll_event events[maxEventsSize];

            int count = epoll_wait(_epollFd, events, maxEventsSize, 100);

            if (count == -1) {
                if (errno == EINTR)
                    continue;

                if (!_running && errno == EBADF)
                    return;

                throw std::runtime_error("epoll_wait failed.");
            }

            for (int i = 0; i < count; ++i) {
                int fd = events[i].data.fd;

                if (events[i].events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                    if (fd != _socket.GetFd())
                        DisconnectClient(fd);

                    continue;
                }

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
                        // Close connection only if IO error has occurred.
                        DisconnectClient(fd);
                    }
                }
            }
        }
    }

    void BrokerServer::Stop() noexcept {
        _running.store(false);
    }

}
