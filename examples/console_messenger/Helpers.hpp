#pragma once

#include <string>
#include <vector>

#include <message_broker/Guid.hpp>
#include <message_broker/ErrorCode.hpp>

#include "Command.hpp"

Command ParseCommand(const std::string&) noexcept;

std::string GuidToString(const message_broker::Guid&) noexcept;
std::string PayloadToString(const std::vector<uint8_t>&) noexcept;

message_broker::Guid StringToGuid(const std::string&) noexcept; 
std::vector<uint8_t> StringToPayload(const std::string&) noexcept;