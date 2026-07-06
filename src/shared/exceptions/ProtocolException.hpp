#pragma once

#include "shared/Protocol.hpp"

#include <stdexcept>

namespace message_broker {
    
    class ProtocolException : public std::exception {
    private:
        ErrorCode _code;
        const char* _message;

    public:
        ProtocolException(ErrorCode code, const char* message) : _code(code), _message(message) {}

        const char* what() const noexcept override {
            return _message;
        }

        ErrorCode GetErrorCode() const noexcept {
            return _code;
        }
    };

}


