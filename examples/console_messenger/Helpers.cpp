#include "Helpers.hpp"

Command ParseCommand(const std::string& s) noexcept {
    if (s == "guid")
        return Command::Guid;

    if (s == "help")
        return Command::Help;

    if (s == "get")
        return Command::Get;

    if (s == "try-get")
        return Command::TryGet;

    if (s == "send")
        return Command::Send;

    if (s == "broadcast")
        return Command::Broadcast;

    return Command::Unrecognized;
} 

std::string GuidToString(const message_broker::Guid& guid) noexcept {
    return std::to_string(guid[0]);
}

std::string PayloadToString(const std::vector<uint8_t>& payload) noexcept {
    std::string s = "";
    for (uint8_t byte : payload) {
        s += byte;
    }

    return s;
}


message_broker::Guid StringToGuid(const std::string& s) noexcept {
    message_broker::Guid guid;
    guid[0] = std::atoi(s.c_str());
    for (int i = 1; i < 16; i++) {
        guid[i] = 0;
    }

    return guid;
}

std::vector<uint8_t> StringToPayload(const std::string& s) noexcept {
    std::vector<uint8_t> payload;
    payload.reserve(s.size());
    for (char c : s) {
        payload.push_back(c);
    }

    return payload;
}
