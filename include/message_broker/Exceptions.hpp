#pragma once

#include <string>
#include <stdexcept>

#include <message_broker/ErrorCode.hpp>

namespace message_broker {

    class ProtocolException : public std::exception {
    private:
        ErrorCode _code;
        std::string _message;

    public:
        ProtocolException(ErrorCode code, const char* message)
            : _code(code), _message(message) {}

        const char* what() const noexcept override {
            return _message.c_str();
        }

        ErrorCode GetErrorCode() const noexcept {
            return _code;
        }
    };

    class OccupiedIdException : public ProtocolException {
    public:
        OccupiedIdException() : ProtocolException(
            ErrorCode::OccupiedId,
            "This Guid is already occupied."
        ) {}
    };

    class UnknownTargetIdException : public ProtocolException {
    public:
        UnknownTargetIdException() : ProtocolException(
            ErrorCode::UnknownTargetId,
            "Client with such Guid wasn't found."
        ) {}
    };

    class InvalidPacketException : public ProtocolException {
    public:
        InvalidPacketException() : ProtocolException(
            ErrorCode::InvalidPacket,
            "Couldn't read packet."
        ) {}
    };

    [[noreturn]]
    inline void ThrowProtocolException(ErrorCode code) {
        switch (code) {
        case ErrorCode::InvalidPacket:
            throw InvalidPacketException();

        case ErrorCode::OccupiedId:
            throw OccupiedIdException();

        case ErrorCode::UnknownTargetId:
            throw UnknownTargetIdException();
        
        default:
            throw std::runtime_error("Unknown error code.");
        }
    }

    class ConnectionException : public std::runtime_error {
    public:
        using std::runtime_error::runtime_error;
    };

}
