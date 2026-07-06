#include "SocketIO.hpp"

#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

#include <cerrno>
#include <stdexcept>

namespace {

    bool ShouldRetry(ssize_t result) {
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

        ~Pipeline() {
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
        size_t readTotally = 0;

        while (readTotally < buf.size()) {
            auto chunk = buf.subspan(readTotally);
            ssize_t res = Write(fd, chunk);
            
            if (res == 0)
                throw std::runtime_error("Socket closed while writing.");

            if (res < 0) 
                throw std::runtime_error("Socket write failed.");

            readTotally += res;
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
    
}