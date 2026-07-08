#pragma once

#include <string_view>

namespace message_broker {

    inline constexpr std::string_view DefaultSocketPath = 
        "/tmp/message-broker.sock";

}
