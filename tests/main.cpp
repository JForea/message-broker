#include "SocketIOTests.hpp"
#include "ProtocolIOTests.hpp"

#include <signal.h>

int main() {

    // SOCKET IO TESTS
    TestReadWrite();
    TestSplice();
    TestReadFromClosedSocket();
    TestWriteIntoClosedSocket();
    TestSpliceFromClosedSocket();
    TestSpliceIncompletePayload();

    // PROTOCOL IO TESTS
    TestProtocolReadWrite();

}