#include "SocketIOTests.hpp"

#include <signal.h>

int main() {

    // SOCKET IO TESTS
    TestReadWrite();
    TestReadFromClosedSocket();
    TestWriteIntoClosedSocket();

}