#include "shared/io/SocketIO.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <stdexcept>

void TestReadWrite() {
    int fds[2];

    int created = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    assert(created == 0);

    std::array<uint8_t, 4> output { 0, 1, 2, 3 };
    std::array<uint8_t, 4> input {};

    message_broker::WriteExact(fds[0], output);
    message_broker::ReadExact(fds[1], input);

    assert(input == output);

    close(fds[0]);
    close(fds[1]);
}

void TestReadFromClosedSocket() {
    int fds[2];

    int created = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    assert(created == 0);

    close(fds[1]);

    std::array<uint8_t, 4> buf {};

    bool error = false;

    try {
        message_broker::ReadExact(fds[0], buf);
    } catch (const std::runtime_error&) {
        error = true;
    }

    assert(error);

    close(fds[0]);
    close(fds[1]);
}

void TestWriteIntoClosedSocket() {
    int fds[2];

    int created = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    assert(created == 0);

    close(fds[1]);

    std::array<uint8_t, 4> buf {};

    bool error = false;

    try {
        message_broker::WriteExact(fds[0], buf);
    } catch (const std::runtime_error&) {
        error = true;
    }

    assert(error);

    close(fds[0]);
    close(fds[1]);
}