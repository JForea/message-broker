#include "SocketIO.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>

namespace {

    ssize_t Read(int fd, std::span<uint8_t> buf) {
        while (true) {
            ssize_t res = ::recv(fd, buf.data(), buf.size(), 0);

            if (res == -1 && errno == EINTR)
                continue;

            return res;
        }
    }

    ssize_t Write(int fd, std::span<uint8_t> buf) {
        while (true) {
            ssize_t res = ::send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);

            if (res == -1 && errno == EINTR)
                continue;

            return res;
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

    void WriteExact(int fd, std::span<uint8_t> buf) {
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

}