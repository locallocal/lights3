// L3: DuoStore 的 RocksDB key/value 编解码与 crc32c（docs/duostore-backend.md §4）。
// key 编码：'\0' 分隔（共享校验层已拒绝 key 含 NUL，bucket 名限 [a-z0-9.-]，§4.1）；
// value 编码：手写小端二进制，首字节版本号（§4.2）；extent 数组经 run 编码（§4.3）。
// 损坏的持久化值统一抛 s3::S3Error(InternalError)。
#pragma once

#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "storage/duostore/meta_store.h"

namespace lights3::storage::duostore::codec {

// ---- crc32c（Castagnoli）：链式增量，crc32c_of(a||b) == crc32c_update(crc32c_of(a), b) ----
uint32_t crc32c_update(uint32_t crc, std::span<const std::byte> data);
inline uint32_t crc32c_of(std::span<const std::byte> data) { return crc32c_update(0, data); }
inline uint32_t crc32c_of(std::string_view s) {
    return crc32c_of(std::span(reinterpret_cast<const std::byte*>(s.data()), s.size()));
}

// ---- 时间戳（ObjectMeta 的 time_point ↔ 持久化的 unix ms）----
inline int64_t to_unix_ms(std::chrono::system_clock::time_point tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}
inline std::chrono::system_clock::time_point from_unix_ms(int64_t ms) {
    return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
}

// ---- key 编码（§4.1）----
std::string object_key(std::string_view bucket, std::string_view key);
std::string upload_key(std::string_view bucket, std::string_view key, std::string_view id);
std::string parts_prefix(std::string_view bucket, std::string_view key, std::string_view id);
std::string part_key(std::string_view bucket, std::string_view key, std::string_view id,
                     int part_no);
int part_no_of_key(std::string_view parts_cf_key);  // 尾部 be16

std::string be64_key(uint64_t v);       // refs / gcq 的 big-endian key
uint64_t parse_be64(std::string_view k);

// ---- extent run 编解码（§4.3；供测试观察 run 压缩效果）----
std::string encode_extents(const std::vector<Extent>& extents);
std::vector<Extent> decode_extents(std::string_view v);

// ---- value 编解码 ----
std::string encode_bucket(int64_t created_ms);
int64_t decode_bucket(std::string_view v);

std::string encode_object(const ObjectRec& rec);  // key 不入 value（在 CF key）
ObjectRec decode_object(std::string key, std::string_view v);
// 仅解码 ObjectMeta（list 用，§4.4）：算术跳过 extent runs，免物化大对象的 Extent 数组
ObjectMeta decode_object_meta(std::string key, std::string_view v);

std::string encode_upload(const UploadRec& rec);  // key/upload_id 不入 value
UploadRec decode_upload(std::string key, std::string upload_id, std::string_view v);

std::string encode_part(const PartRec& rec);  // part_no 不入 value（在 CF key）
PartRec decode_part(int part_no, std::string_view v);

std::string encode_reclaim(const Reclaim& r, int64_t enqueue_ms);
Reclaim decode_reclaim(std::string_view v, int64_t* enqueue_ms = nullptr);

// stats CF 计数器（merge operator 的操作数与全量值同格式：8B 小端 i64）
std::string encode_counter_delta(int64_t d);
int64_t decode_counter(std::string_view v);

}  // namespace lights3::storage::duostore::codec
