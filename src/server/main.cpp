#include <csignal>
#include <iostream>

#include "BrokerServer.hpp"

#include "shared/Defaults.hpp"

int main(int argc, char* argv[]) {
    // Prevent SIGPIPE when writing to a socket closed by the peer.
    // Socket write errors will be reported via errno instead. 
    std::signal(SIGPIPE, SIG_IGN);

    std::string_view socketPath = message_broker::DefaultSocketPath;
    
    // TODO: Add reading threadCount as a parameter when starting server.
    constexpr size_t threadCount = 4;

    if (argc > 2) {
        std::cerr << "Usage: message-broker-server [socket-path]\n";
        return 1;
    }

    if (argc == 2)
        socketPath = argv[1];

    message_broker::BrokerServer server(socketPath, threadCount);
    server.Run();
}