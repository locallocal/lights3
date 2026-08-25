// L4: checksums and base64. Needed by both S3's integrity headers (Content-MD5,
// x-amz-checksum-*) and ListObjectsV2's opaque tokens; lives in core/util so each
// layer does not copy its own version
#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace lights3::util {

// crc32c (Castagnoli, shared by x-amz-checksum-crc32c and duostore extent
// verification): chained incremental, crc32c_of(a||b) == crc32c_update(crc32c_of(a), b)
uint32_t crc32c_update(uint32_t crc, std::span<const std::byte> data);
inline uint32_t crc32c_of(std::span<const std::byte> data) { return crc32c_update(0, data); }
inline uint32_t crc32c_of(std::string_view s) {
    return crc32c_of(std::span(reinterpret_cast<const std::byte*>(s.data()), s.size()));
}

// crc32 (IEEE 802.3, x-amz-checksum-crc32): differs from crc32c only in the generator polynomial
uint32_t crc32_update(uint32_t crc, std::span<const std::byte> data);
inline uint32_t crc32_of(std::span<const std::byte> data) { return crc32_update(0, data); }

// crc64/nvme (x-amz-checksum-crc64nvme, the AWS SDK default algorithm since 2025):
// NVMe 1.4+ 64-bit CRC, reflected; same chained-incremental contract as the crc32 pair
uint64_t crc64nvme_update(uint64_t crc, std::span<const std::byte> data);
inline uint64_t crc64nvme_of(std::span<const std::byte> data) { return crc64nvme_update(0, data); }
inline uint64_t crc64nvme_of(std::string_view s) {
    return crc64nvme_of(std::span(reinterpret_cast<const std::byte*>(s.data()), s.size()));
}

// Standard base64 (with padding). decode is strict: a length not a multiple of 4,
// characters outside the alphabet, or a non-trailing '=' all yield nullopt —
// lenient decoding would conflate "malformed input" with "digest mismatch"
std::string base64_encode(std::span<const uint8_t> in);
inline std::string base64_encode(std::string_view s) {
    return base64_encode(std::span(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
}
std::optional<std::string> base64_decode(std::string_view in);

}  // namespace lights3::util
