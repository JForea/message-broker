#include <csignal>

#include "BrokerServer.hpp"

int main() {
    // Prevent SIGPIPE when writing to a socket closed by the peer.
    // Socket write errors will be reported via errno instead. 
    std::signal(SIGPIPE, SIG_IGN);

    message_broker::BrokerServer server("/tmp/message-broker.sock");
    server.Run();
}