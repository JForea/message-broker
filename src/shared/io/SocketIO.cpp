#include "SocketIO.hpp"

#include "Pipeline.hpp"

#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

#include <cerrno>
#include <stdexcept>
#include <cmath>

namespace {

    using message_broker::Pipeline;

    constexpr size_t BufferSize = 1024 * 1024;

    inline bool ShouldRetry(ssize_t result) {
        return result == -1 && errno == EINTR;
    }

    ssize_t Read(int fd, std::span<uint8_t> buf, uint32_t size = 0) {
        while (true) {
            ssize_t res = recv(fd, buf.data(), size == 0 ? buf.size() : size, 0);

            if (ShouldRetry(res))
                continue;

            return res;
        }
    }

    ssize_t Write(int fd, std::span<const uint8_t> buf, uint32_t size = 0) {
        while (true) {
            ssize_t res = send(fd, buf.data(), size == 0 ? buf.size() : size, MSG_NOSIGNAL);

            if (ShouldRetry(res))
                continue;

            return res;
        } 
    }

    ssize_t Splice(int fromFd, int toFd, uint32_t size) {
        while (true) {
            ssize_t res = splice(
                fromFd, nullptr, 
                toFd, nullptr, 
                size, 
                0
            );

            if (ShouldRetry(res))
                continue;

            return res;
        }
    }

    ssize_t MoveBatch(
        int fromFd, int toFd,
        const Pipeline& pipeline,
        uint32_t maxBytes
    ) {
        ssize_t movedToPipe = Splice(
            fromFd, 
            pipeline.WriteEnd(), 
            maxBytes
        );
        
        if (movedToPipe <= 0)
            return movedToPipe;

        ssize_t movedToTarget = 0;

        while (movedToTarget < movedToPipe) {
            ssize_t moved = Splice(
                pipeline.ReadEnd(),
                toFd,
                movedToPipe - movedToTarget
            );

            if (moved <= 0)
                return moved;

            movedToTarget += moved;
        }

        return movedToPipe;
    }

    ssize_t Tee(int fromFd, int toFd, uint32_t size) {
        while (true) {
            ssize_t res = tee(
                fromFd, toFd, 
                size, 0
            );

            if (ShouldRetry(res))
                continue;

            return res;
        }
    }

}

namespace message_broker {

    void ReadExact(int fd, std::span<uint8_t> buf, uint32_t size) {
        size_t readTotally = 0;

        if (size == 0)
            size = buf.size();

        while (readTotally < size) {
            auto chunk = buf.subspan(readTotally);
            ssize_t res = Read(fd, chunk, size);
            
            if (res == 0)
                throw std::runtime_error("Socket closed while reading.");

            if (res < 0) 
                throw std::runtime_error("Socket read failed.");

            readTotally += res;
        }
    }

    void WriteExact(int fd, std::span<const uint8_t> buf, uint32_t size) {
        size_t writtenTotally = 0;

        if (size == 0)
            size = buf.size();

        while (writtenTotally < size) {
            auto chunk = buf.subspan(writtenTotally);
            ssize_t res = Write(fd, chunk, size);
            
            if (res == 0)
                throw std::runtime_error("Socket closed while writing.");

            if (res < 0) 
                throw std::runtime_error("Socket write failed.");

            writtenTotally += res;
        }
    }

    std::unordered_set<int> BroadcastExact(int fromFd, std::vector<int> toFds, uint32_t payloadSize) {
        std::unordered_set<int> badTargets;
        
        std::vector<uint8_t> buf(BufferSize);

        size_t broadcastedTotally = 0;
        while (broadcastedTotally < payloadSize) {
            size_t batchSize = std::min(payloadSize - broadcastedTotally, BufferSize);
            if (batchSize == 0)
                break;

            ReadExact(fromFd, buf, batchSize);

            for (int fd : toFds) {
                if (badTargets.contains(fd))
                    continue;

                try {
                    WriteExact(fd, buf, batchSize);
                } catch (...) {
                    badTargets.emplace(fd);
                }
            }

            broadcastedTotally += batchSize;
        }

        return badTargets;
    }

    // splice() cannot transfer data directly between two sockets.
    // Therefore an intermediate pipe is required.
    void SpliceExact(int fromFd, int toFd, uint32_t size, const Pipeline& pipeline) {        
        uint32_t bytesLeft = size;

        while (bytesLeft > 0) {
            ssize_t moved = MoveBatch(
                fromFd, toFd,
                pipeline,
                bytesLeft
            );

            if (moved == 0)
                throw std::runtime_error("Socket closed while splicing.");

            if (moved < 0)
                throw std::runtime_error("Splice failed.");

            bytesLeft -= moved;
        }
    }

}