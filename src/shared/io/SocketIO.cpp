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

    inline bool ShouldRetry(ssize_t result) {
        return result == -1 && errno == EINTR;
    }

    ssize_t Read(int fd, std::span<uint8_t> buf) {
        while (true) {
            ssize_t res = recv(fd, buf.data(), buf.size(), 0);

            if (ShouldRetry(res))
                continue;

            return res;
        }
    }

    ssize_t Write(int fd, std::span<const uint8_t> buf) {
        while (true) {
            ssize_t res = send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);

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

    ssize_t DrainPipeline(const Pipeline& pipeline, ssize_t size) {
        Pipeline drainPipeline;

        ssize_t drained = 0;

        while (drained < size) {
            ssize_t moved = Splice(
                pipeline.ReadEnd(),
                drainPipeline.WriteEnd(),
                size - drained
            );

            if (moved <= 0)
                return moved;

            drained += moved;
        }

        return drained;
    }

    void BroadcastToBatch(
        const Pipeline& source,
        std::span<const int> targetFds,
        std::span<Pipeline*> targetPipelines,
        uint32_t size,
        std::unordered_set<int>& failedFds
    ) {
        if (targetFds.size() != targetPipelines.size())
            throw std::invalid_argument("TargetFd and pipeline counts must match.");

        for (int i = 0; i < targetFds.size(); ++i) {
            int targetFd = targetFds[i];

            if (failedFds.contains(targetFd))
                continue;

            Pipeline& targetPipeline = *targetPipelines[i];

            ssize_t copied = Tee(
                source.ReadEnd(),
                targetPipeline.WriteEnd(),
                size
            );

            if (copied < 0) {
                failedFds.insert(targetFd);
                continue;
            }

            if (copied != static_cast<ssize_t>(size)) {
                // tee() may have copied only part of the chunk.
                // Remove that partial data before reusing the pipeline.
                if (copied > 0) {
                    ssize_t drained = DrainPipeline(targetPipeline, copied);

                    if (drained != copied)
                        throw std::runtime_error(
                            "Failed to drain target pipeline after partial tee."
                        );
                }

                failedFds.insert(targetFd);
                continue;
            }

            ssize_t sent = 0;

            while (sent < copied) {
                ssize_t moved = Splice(
                    targetPipeline.ReadEnd(),
                    targetFd,
                    static_cast<uint32_t>(copied - sent)
                );

                if (moved <= 0) {
                    ssize_t remaining = copied - sent;

                    if (remaining > 0) {
                        ssize_t drained = DrainPipeline(
                            targetPipeline,
                            remaining
                        );

                        if (drained != remaining) {
                            throw std::runtime_error(
                                "Failed to drain target pipeline after splice failure."
                            );
                        }
                    }

                    failedFds.insert(targetFd);
                    break;
                }

                sent += moved;
            }
        }
    }

}

namespace message_broker {

    void ReadExact(int fd, std::span<uint8_t> buf) {
        size_t readTotally = 0;

        while (readTotally < buf.size()) {
            auto chunk = buf.subspan(readTotally);
            ssize_t res = Read(fd, chunk);
            
            if (res == 0)
                throw std::runtime_error("Socket closed while reading.");

            if (res < 0) 
                throw std::runtime_error("Socket read failed.");

            readTotally += res;
        }
    }

    void WriteExact(int fd, std::span<const uint8_t> buf) {
        size_t writtenTotally = 0;

        while (writtenTotally < buf.size()) {
            auto chunk = buf.subspan(writtenTotally);
            ssize_t res = Write(fd, chunk);
            
            if (res == 0)
                throw std::runtime_error("Socket closed while writing.");

            if (res < 0) 
                throw std::runtime_error("Socket write failed.");

            writtenTotally += res;
        }
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

    // Best-effort function. Tries to send data to every target fd.
    // Returns the fds that failed.
    std::unordered_set<int> TeeExact(
        int fromFd, const std::vector<int>& toFds,
        uint32_t size,
        Pipeline& source,
        std::vector<Pipeline*> targetPipelines
    ) {
        // tee() cannot transfer data directly from a source socket to multiple target sockets.
        // Broadcasting is performed in three steps:
        // 1. Use splice() to move data from the sender socket to the source pipe.
        // 2. Use tee() to duplicate the data from the source pipe into each target pipe.
        // 3. Use splice() again to move the data from each target pipe to its corresponding socket.
        std::unordered_set<int> failedFds;

        uint32_t bytesLeft = size;

        while (bytesLeft > 0) {
            ssize_t movedToSource = Splice(
                fromFd,
                source.WriteEnd(),
                bytesLeft
            );

            for (int offset = 0; offset < toFds.size(); offset += targetPipelines.size()) {
                size_t batchSize = std::min(
                    targetPipelines.size(),
                    toFds.size() - offset
                );

                BroadcastToBatch(
                    source,
                    std::span<const int>(toFds).subspan(offset, batchSize),
                    std::span<Pipeline*>(targetPipelines).first(batchSize),
                    movedToSource,
                    failedFds
                );
            }

            ssize_t drained = DrainPipeline(source, movedToSource);

            if (drained != movedToSource)
                throw std::runtime_error("Failed to drain source pipeline.");

            bytesLeft -= movedToSource;
        }

        return failedFds;
    }

}