#include "core/util/uri.h"

#include <cstdint>

namespace lights3::util {

namespace {
int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
bool unreserved(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
           c == '-' || c == '_' || c == '.' || c == '~';
}
}  // namespace

std::string percent_decode(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            int hi = hex_val(s[i + 1]), lo = hex_val(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>(hi * 16 + lo));
                i += 2;
                continue;
            }
        }
        if (s[i] == '+') {  // query 中的空格
            out.push_back(' ');
            continue;
        }
        out.push_back(s[i]);
    }
    return out;
}

std::string aws_uri_encode(std::string_view s, bool encode_slash) {
    static const char* digits = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (unreserved(c) || (c == '/' && !encode_slash)) {
            out.push_back(c);
        } else {
            auto b = static_cast<uint8_t>(c);
            out.push_back('%');
            out.push_back(digits[b >> 4]);
            out.push_back(digits[b & 0xf]);
        }
    }
    return out;
}

}  // namespace lights3::util
