#include "SocketIOTests.hpp"
#include "ProtocolIOTests.hpp"
#include "PacketIOTests.hpp"
#include "ServerSocketTests.hpp"
#include "BrokerServerTests.hpp"
#include "BrokerClientTests.hpp"
#include "BrokerConcurrencyTests.hpp"

#include <csignal>

int main() {
    // Prevent SIGPIPE when writing to a socket closed by the peer.
    // Socket write errors will be reported via errno instead. 
    std::signal(SIGPIPE, SIG_IGN);

    // SOCKET IO TESTS
    TestReadWrite();
    TestSplice();
    TestBroadcast();
    TestBroadcastByBatches();
    TestReadFromClosedSocket();
    TestWriteIntoClosedSocket();
    TestSpliceFromClosedSocket();
    TestSpliceIncompletePayload();

    // PROTOCOL IO TESTS
    TestProtocolReadWrite();

    // PACKET IO TESTS
    TestPacketRegister();
    TestPacketSendMessage();
    TestPacketServerMessage();

    // SERVERSOCKET TESTS
    TestServerSocketStart();
    TestServerSocketAccept();

    // BROKERSERVER TESTS
    TestBrokerServerRegister();

    // CLIENTSERVER TESTS
    TestBrokerClientRegister();

    // BROKERSERVER CONCURRENCY TESTS
    TestConcurrentClientRegistration();
    TestConcurrentSendMessagesToSameTarget();
    TestConcurrentBroadcastsDoNotDeadlock();

}