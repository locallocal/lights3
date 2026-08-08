// L4: 校验和与 base64。S3 的完整性头（Content-MD5、x-amz-checksum-*）与
// ListObjectsV2 的不透明 token 都要用，放在 core/util 免得各层各抄一份
#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace lights3::util {

// crc32c（Castagnoli，x-amz-checksum-crc32c 与 duostore extent 校验共用）：
// 链式增量，crc32c_of(a||b) == crc32c_update(crc32c_of(a), b)
uint32_t crc32c_update(uint32_t crc, std::span<const std::byte> data);
inline uint32_t crc32c_of(std::span<const std::byte> data) { return crc32c_update(0, data); }
inline uint32_t crc32c_of(std::string_view s) {
    return crc32c_of(std::span(reinterpret_cast<const std::byte*>(s.data()), s.size()));
}

// crc32（IEEE 802.3，x-amz-checksum-crc32）：与 crc32c 只差生成多项式
uint32_t crc32_update(uint32_t crc, std::span<const std::byte> data);
inline uint32_t crc32_of(std::span<const std::byte> data) { return crc32_update(0, data); }

// 标准 base64（含 padding）。decode 严格：长度非 4 的倍数、字母表外字符、
// 非末尾的 '=' 一律 nullopt——宽松解码会让"格式非法"和"摘要不符"混为一谈
std::string base64_encode(std::span<const uint8_t> in);
inline std::string base64_encode(std::string_view s) {
    return base64_encode(std::span(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
}
std::optional<std::string> base64_decode(std::string_view in);

}  // namespace lights3::util
