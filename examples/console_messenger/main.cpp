#include <iostream>

#include <message_broker/BrokerClient.hpp>
#include <message_broker/Exceptions.hpp>

#include "Command.hpp"
#include "Helpers.hpp"

using namespace message_broker;

const std::string WrongUsage = "Wrong usage: console-messenger [guid]\n";

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << WrongUsage;
        return 1;
    }

    Guid guid = StringToGuid(argv[1]);

    if (guid[0] == 0) {
        std::cerr << WrongUsage;
        return 1;
    }

    
    try {
        BrokerClient client(guid);

        std::cout << "Connected succesfully.\n";

        while (true) {
            try {
                std::string cmdS;

                std::cin >> cmdS;

                auto command = ParseCommand(cmdS);

                switch (command) {
                    case Command::Guid: {
                        std::cout << "My Guid is: " << GuidToString(client.GetClientId()) << "\n";
                        break;
                    }

                    case Command::Help: {
                        std::cout << "help - Show command list with descriptions.\n"
                            << "get - Waits for a message in socket.\n"
                            << "try-get - Tries to get message from socket. If no message is present returns.\n"
                            << "send [targetGuid] [message] - Sends message to specified target.\n"
                            << "broadcast [message] - Sends message to every other client.\n";
                        break;
                    }

                    case Command::Get: {
                        Message message = client.GetMessage();
                        std::cout << "New message from: " << GuidToString(message.senderId) << '\n' 
                            << PayloadToString(message.data);
                        break;
                    }

                    case Command::TryGet: {
                        auto messageOpt = client.TryGetMessage();
                        if (messageOpt.has_value()) {
                            Message message = messageOpt.value();
                            std::cout << "New message from: " << GuidToString(message.senderId) << '\n' 
                                << PayloadToString(message.data);
                        } else {
                            std::cout << "No new messages for now.";
                        }
                        break;
                    }

                    case Command::Send: {
                        std::string guid, message;
                        std::cin >> guid >> message;
                        client.SendMessage(
                            StringToGuid(guid),
                            StringToPayload(message)
                        );
                        break;
                    }

                    case Command::Broadcast: {
                        std::string message;
                        std::cin >> message;
                        client.Broadcast(StringToPayload(message));
                        break;
                    }

                    case Command::Stop: {
                        return 0;
                    }
                    
                    default: {
                        std::cout << "Unknown command. Print 'help' for more information about possible commands.";
                    }
                }
            } catch (const ProtocolException& e) {
                std::cout << "Got error: " << e.what() << '\n';
            } catch (...) {
                std::cerr << "Unrecognized error." << '\n';
            }
        }
    } catch (const OccupiedIdException& e) {
        std::cerr << "Got error: " << e.what() << '\n';
    }
    
}
