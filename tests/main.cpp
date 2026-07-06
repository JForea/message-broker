#include "SocketIOTests.hpp"
#include "ProtocolIOTests.hpp"
#include "PacketIOTests.hpp"

#include <csignal>

int main() {
    // Prevent SIGPIPE when writing to a socket closed by the peer.
    // Socket write errors will be reported via errno instead. 
    std::signal(SIGPIPE, SIG_IGN);

    // SOCKET IO TESTS
    TestReadWrite();
    TestSplice();
    TestTee();
    TestReadFromClosedSocket();
    TestWriteIntoClosedSocket();
    TestSpliceFromClosedSocket();
    TestSpliceIncompletePayload();
    TestTeeWithFailedTarget();

    // PROTOCOL IO TESTS
    TestProtocolReadWrite();

    // PACKET IO TESTS
    TestPacketRegister();
    TestPacketSendMessage();
    TestPacketServerMessage();

}