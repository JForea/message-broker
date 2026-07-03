#include "SocketIOTests.hpp"
#include "ProtocolIOTests.hpp"

#include <signal.h>

int main() {

    // SOCKET IO TESTS
    TestReadWrite();
    TestReadFromClosedSocket();
    TestWriteIntoClosedSocket();

    // PROTOCOL IO TESTS
    TestProtocolReadWrite();

}