#pragma once

#include <cstdint>
#include <span>
#include <string>

namespace lights3::util {

inline std::string to_hex(std::span<const uint8_t> data) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(data.size() * 2);
    for (uint8_t b : data) {
        out.push_back(digits[b >> 4]);
        out.push_back(digits[b & 0xf]);
    }
    return out;
}

}  // namespace lights3::util
