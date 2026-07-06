#include "SocketIO.hpp"

#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

#include <cerrno>
#include <stdexcept>

namespace {

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

    class Pipeline {
    private:
        int _readEnd;
        int _writeEnd;
    
    public:
        Pipeline() {
            int pipeFds[2];
            if (pipe(pipeFds) == -1)
                throw std::runtime_error("Pipe creation failed.");

            _readEnd = pipeFds[0];
            _writeEnd = pipeFds[1];
        }

        Pipeline(const Pipeline&) = delete;
        Pipeline& operator=(const Pipeline&) = delete;

        int ReadEnd() const {
            return _readEnd;
        }

        int WriteEnd() const {
            return _writeEnd;
        }

        ~Pipeline() noexcept {
            close(_readEnd);
            close(_writeEnd);
        }
    };

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

    ssize_t MoveBatch(int fromFd, int toFd, const Pipeline& pipeline, uint32_t maxBytes) {
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

    ssize_t BroadcastBatch(
        int sourceFd,
        const std::vector<int>& targetFds,
        const Pipeline& sourcePipeline,
        const std::vector<Pipeline>& targetPipelines,
        uint32_t maxBytes,
        std::unordered_set<int>& failedFds
    ) {
        ssize_t movedToSource = Splice(
            sourceFd, 
            sourcePipeline.WriteEnd(), 
            maxBytes
        );
        
        if (movedToSource <= 0)
            return movedToSource;

        for (int i = 0; i < targetFds.size(); ++i) {
            if (failedFds.contains(targetFds[i]))
                continue;

            ssize_t copied = Tee(
                sourcePipeline.ReadEnd(),
                targetPipelines[i].WriteEnd(),
                static_cast<uint32_t>(movedToSource)
            );

            if (copied != movedToSource) {
                failedFds.emplace(targetFds[i]);
                continue;
            }

            ssize_t movedToTargetFd = 0;

            while (movedToTargetFd < copied) {
                ssize_t moved = Splice(
                    targetPipelines[i].ReadEnd(),
                    targetFds[i],
                    copied - movedToTargetFd
                );

                if (moved <= 0) {
                    failedFds.emplace(targetFds[i]);
                    break;
                }

                movedToTargetFd += moved;
            }
        }

        ssize_t drained = DrainPipeline(sourcePipeline, movedToSource);

        if (drained != movedToSource)
            return -1;

        return movedToSource;
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

    void SpliceExact(int fromFd, int toFd, uint32_t size) {        
        // splice() cannot transfer data directly between two sockets.
        // Therefore an intermediate pipe is required.
        Pipeline pipeline {};

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
    std::unordered_set<int> TeeExact(int fromFd, const std::vector<int>& toFds, uint32_t size) {
        // tee() cannot transfer data directly from a source socket to multiple target sockets.
        // Broadcasting is performed in three steps:
        // 1. Use splice() to move data from the sender socket to the source pipe.
        // 2. Use tee() to duplicate the data from the source pipe into each target pipe.
        // 3. Use splice() again to move the data from each target pipe to its corresponding socket.
        Pipeline source {};
        std::vector<Pipeline> targets(toFds.size());

        std::unordered_set<int> failedFds;

        uint32_t bytesLeft = size;

        while (bytesLeft > 0) {
            ssize_t broadcasted = BroadcastBatch(
                fromFd, toFds,
                source, targets,
                bytesLeft,
                failedFds
            );

            if (broadcasted == 0)
                throw std::runtime_error("Socket closed while splicing.");

            if (broadcasted < 0)
                throw std::runtime_error("Splice failed.");

            bytesLeft -= broadcasted;
        }

        return failedFds;
    }

}