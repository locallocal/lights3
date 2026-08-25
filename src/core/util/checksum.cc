#include "core/util/checksum.h"

#include <array>

namespace lights3::util {

namespace {

// Reflected table-driven implementation (~hundreds of MB/s). RocksDB has a
// hardware-accelerated crc32c internally, but not as a public header — reusing it
// would mean adding its source tree to the include path and binding to an internal
// API across submodule upgrades; on balance, not taken. Upgrade to slicing-by-8
// when data-path throughput becomes the bottleneck
template <uint32_t kPoly>
uint32_t table_crc(uint32_t crc, std::span<const std::byte> data) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? kPoly ^ (c >> 1) : c >> 1;
            t[i] = c;
        }
        return t;
    }();
    crc = ~crc;
    for (std::byte b : data) crc = table[(crc ^ uint32_t(b)) & 0xff] ^ (crc >> 8);
    return ~crc;
}

// 64-bit twin of table_crc; same reflected table-driven shape, same ~crc chaining trick
template <uint64_t kPoly>
uint64_t table_crc64(uint64_t crc, std::span<const std::byte> data) {
    static const std::array<uint64_t, 256> table = [] {
        std::array<uint64_t, 256> t{};
        for (uint64_t i = 0; i < 256; ++i) {
            uint64_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? kPoly ^ (c >> 1) : c >> 1;
            t[i] = c;
        }
        return t;
    }();
    crc = ~crc;
    for (std::byte b : data) crc = table[(crc ^ uint64_t(b)) & 0xff] ^ (crc >> 8);
    return ~crc;
}

constexpr char kB64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

}  // namespace

uint32_t crc32c_update(uint32_t crc, std::span<const std::byte> data) {
    return table_crc<0x82F63B78u>(crc, data);  // Castagnoli, reflected polynomial
}

uint32_t crc32_update(uint32_t crc, std::span<const std::byte> data) {
    return table_crc<0xEDB88320u>(crc, data);  // IEEE 802.3, reflected polynomial
}

uint64_t crc64nvme_update(uint64_t crc, std::span<const std::byte> data) {
    return table_crc64<0x9A6C9329AC4BC9B5ull>(crc, data);  // NVMe, reflected polynomial
}

std::string base64_encode(std::span<const uint8_t> in) {
    std::string out;
    out.reserve((in.size() + 2) / 3 * 4);
    size_t i = 0;
    for (; i + 3 <= in.size(); i += 3) {
        uint32_t v = (uint32_t(in[i]) << 16) | (uint32_t(in[i + 1]) << 8) | in[i + 2];
        out.push_back(kB64[(v >> 18) & 63]);
        out.push_back(kB64[(v >> 12) & 63]);
        out.push_back(kB64[(v >> 6) & 63]);
        out.push_back(kB64[v & 63]);
    }
    size_t rem = in.size() - i;
    if (rem == 1) {
        uint32_t v = uint32_t(in[i]) << 16;
        out.push_back(kB64[(v >> 18) & 63]);
        out.push_back(kB64[(v >> 12) & 63]);
        out += "==";
    } else if (rem == 2) {
        uint32_t v = (uint32_t(in[i]) << 16) | (uint32_t(in[i + 1]) << 8);
        out.push_back(kB64[(v >> 18) & 63]);
        out.push_back(kB64[(v >> 12) & 63]);
        out.push_back(kB64[(v >> 6) & 63]);
        out += "=";
    }
    return out;
}

std::optional<std::string> base64_decode(std::string_view in) {
    if (in.empty() || in.size() % 4 != 0) return std::nullopt;
    auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::string out;
    out.reserve(in.size() / 4 * 3);
    for (size_t i = 0; i < in.size(); i += 4) {
        int pad = 0;
        int v[4];
        for (int j = 0; j < 4; ++j) {
            char c = in[i + j];
            if (c == '=' && i + 4 == in.size() && j >= 2) {
                v[j] = 0;
                ++pad;
            } else {
                v[j] = val(c);
                if (v[j] < 0 || pad > 0) return std::nullopt;  // '=' may only appear at the end
            }
        }
        uint32_t x = (uint32_t(v[0]) << 18) | (uint32_t(v[1]) << 12) | (uint32_t(v[2]) << 6) |
                     uint32_t(v[3]);
        out.push_back(char((x >> 16) & 0xff));
        if (pad < 2) out.push_back(char((x >> 8) & 0xff));
        if (pad < 1) out.push_back(char(x & 0xff));
    }
    return out;
}

}  // namespace lights3::util
