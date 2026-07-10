#include "SocketIOTests.hpp"

#include "shared/io/Pipeline.hpp"
#include "shared/io/SocketIO.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>
#include <thread>

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

    assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, source) == 0);
    assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, target) == 0);

    std::array<std::uint8_t, 4> input{0, 1, 2, 3};
    std::array<std::uint8_t, 4> output{};

    message_broker::WriteExact(source[0], input);

    message_broker::Pipeline pipeline;

    message_broker::SpliceExact(
        source[1],
        target[0],
        input.size(),
        pipeline
    );

    message_broker::ReadExact(target[1], output);

    assert(output == input);

    close(source[0]);
    close(source[1]);
    close(target[0]);
    close(target[1]);
}

void TestBroadcast() {
    int source[2];
    int target1[2];
    int target2[2];

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, source) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target1) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target2) == 0);

    std::array<std::uint8_t, 4> input{0, 1, 2, 3};
    std::array<std::uint8_t, 4> output1{};
    std::array<std::uint8_t, 4> output2{};

    message_broker::WriteExact(source[0], input);

    std::vector<int> targetFds{
        target1[0],
        target2[0]
    };

    auto failed = message_broker::BroadcastExact(
        source[1],
        targetFds,
        input.size()
    );

    assert(failed.empty());

    message_broker::ReadExact(target1[1], output1);
    message_broker::ReadExact(target2[1], output2);

    assert(output1 == input);
    assert(output2 == input);

    close(source[0]);
    close(source[1]);
    close(target1[0]);
    close(target1[1]);
    close(target2[0]);
    close(target2[1]);
}

void TestBroadcastByBatches() {
    int source[2];
    int target1[2];
    int target2[2];

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, source) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target1) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, target2) == 0);

    constexpr size_t InputSize =  1024 * 1024 + 1024;

    std::array<std::uint8_t, InputSize> input;

    for (int i = 0; i < InputSize; ++i)
        input[i] = i % 256;

    std::array<std::uint8_t, InputSize> output1{};
    std::array<std::uint8_t, InputSize> output2{};

    std::thread sourceThread([&source, &input] {
        message_broker::WriteExact(source[0], input);
    });

    std::thread target1Thread([&target1, &output1] {
        message_broker::ReadExact(target1[1], output1);
    });
    std::thread target2Thread([&target2, &output2] {
        message_broker::ReadExact(target2[1], output2);
    });

    std::vector<int> targetFds{
        target1[0],
        target2[0]
    };

    auto failed = message_broker::BroadcastExact(
        source[1],
        targetFds,
        input.size()
    );

    sourceThread.join();
    target1Thread.join();
    target2Thread.join();

    assert(failed.empty());
    
    assert(output1 == input);
    assert(output2 == input);

    close(source[0]);
    close(source[1]);
    close(target1[0]);
    close(target1[1]);
    close(target2[0]);
    close(target2[1]);
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

    message_broker::Pipeline pipeline;

    bool error = false;

    try {
        message_broker::SpliceExact(
            source[1],
            target[0],
            4,
            pipeline
        );
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

    message_broker::Pipeline pipeline;

    bool error = false;

    try {
        message_broker::SpliceExact(
            source[1],
            target[0],
            5,
            pipeline
        );
    } catch (const std::runtime_error&) {
        error = true;
    }

    assert(error);

    close(source[1]);
    close(target[0]);
    close(target[1]);
}
