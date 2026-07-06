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

void TestSplice() {
    int source[2];
    int target[2];

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, source) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target) == 0);

    std::array<std::uint8_t, 4> input { 0, 1, 2, 3 };
    std::array<std::uint8_t, 4> output {};

    message_broker::WriteExact(source[0], input);

    message_broker::SpliceExact(source[1], target[0], input.size());

    message_broker::ReadExact(target[1], output);

    assert(output == input);

    close(source[0]);
    close(source[1]);
    close(target[0]);
    close(target[1]);
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

void TestSpliceFromClosedSocket() {
    int source[2];
    int target[2];

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, source) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target) == 0);

    close(source[0]);

    bool error = false;

    try {
        message_broker::SpliceExact(source[1], target[0], 4);
    } catch (const std::runtime_error&) {
        error = true;
    }

    assert(error);

    close(source[1]);
    close(target[0]);
    close(target[1]);
}

// Expecting a 5-byte payload, but only 4 bytes are available.
void TestSpliceIncompletePayload() {
    int source[2];
    int target[2];

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, source) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target) == 0);

    std::array<std::uint8_t, 4> input{0, 1, 2, 3};

    message_broker::WriteExact(source[0], input);
    close(source[0]);

    bool error = false;

    try {
        message_broker::SpliceExact(source[1], target[0], 5);
    } catch (const std::runtime_error&) {
        error = true;
    }

    assert(error);

    close(source[1]);
    close(target[0]);
    close(target[1]);
}