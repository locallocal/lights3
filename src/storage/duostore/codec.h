// L3: DuoStore's RocksDB key/value codec and crc32c (docs/duostore-backend.md §4).
// Key encoding: '\0'-separated (the shared validation layer already rejects keys
// containing NUL, and bucket names are limited to [a-z0-9.-], §4.1);
// value encoding: hand-written little-endian binary, first byte is the version (§4.2);
// the extent array uses run encoding (§4.3).
// Corrupt persisted values uniformly throw s3::S3Error(InternalError).
#pragma once

#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "storage/duostore/meta_store.h"

namespace lights3::storage::duostore::codec {

// ---- crc32c (Castagnoli): chained/incremental, crc32c_of(a||b) == crc32c_update(crc32c_of(a), b) ----
uint32_t crc32c_update(uint32_t crc, std::span<const std::byte> data);
inline uint32_t crc32c_of(std::span<const std::byte> data) { return crc32c_update(0, data); }
inline uint32_t crc32c_of(std::string_view s) {
    return crc32c_of(std::span(reinterpret_cast<const std::byte*>(s.data()), s.size()));
}

// ---- timestamps (ObjectMeta's time_point ↔ persisted unix ms) ----
inline int64_t to_unix_ms(std::chrono::system_clock::time_point tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}
inline std::chrono::system_clock::time_point from_unix_ms(int64_t ms) {
    return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
}

// ---- key encoding (§4.1) ----
std::string object_key(std::string_view bucket, std::string_view key);
std::string upload_key(std::string_view bucket, std::string_view key, std::string_view id);
std::string parts_prefix(std::string_view bucket, std::string_view key, std::string_view id);
std::string part_key(std::string_view bucket, std::string_view key, std::string_view id,
                     int part_no);
int part_no_of_key(std::string_view parts_cf_key);  // trailing be16

std::string be64_key(uint64_t v);       // big-endian key for refs / gcq
uint64_t parse_be64(std::string_view k);

// Successor seek point for delimiter group skipping (main doc §4.4): increment the
// last non-0xff byte and truncate after it; return false when all bytes are 0xff
// (the group tail is the delimiter, unreachable in practice). Shared by all meta
// store implementations.
inline bool bump_last_byte(std::string& s) {
    for (size_t i = s.size(); i-- > 0;) {
        if (uint8_t(s[i]) != 0xff) {
            ++s[i];
            s.resize(i + 1);
            return true;
        }
    }
    return false;
}

// ---- extent run codec (§4.3; exposed so tests can observe run compression) ----
std::string encode_extents(const std::vector<Extent>& extents);
std::vector<Extent> decode_extents(std::string_view v);

// ---- value codec ----
std::string encode_bucket(int64_t created_ms);
int64_t decode_bucket(std::string_view v);

std::string encode_object(const ObjectRec& rec);  // key not stored in value (it is the CF key)
ObjectRec decode_object(std::string key, std::string_view v);
// Decode only ObjectMeta (for list, §4.4): arithmetically skips the extent runs,
// avoiding materializing a large object's Extent array
ObjectMeta decode_object_meta(std::string key, std::string_view v);

std::string encode_upload(const UploadRec& rec);  // key/upload_id not stored in value
UploadRec decode_upload(std::string key, std::string upload_id, std::string_view v);

std::string encode_part(const PartRec& rec);  // part_no not stored in value (it is in the CF key)
PartRec decode_part(int part_no, std::string_view v);

std::string encode_reclaim(const Reclaim& r, int64_t enqueue_ms);
Reclaim decode_reclaim(std::string_view v, int64_t* enqueue_ms = nullptr);

// stats CF counters (merge operator operands and full values share one format: 8B little-endian i64)
std::string encode_counter_delta(int64_t d);
int64_t decode_counter(std::string_view v);

// ---- canonical parsing of pack record owner (docs/gaps.md §6.1) ----
// Three historical forms used to be hand-parsed ad hoc inside the compaction
// callbacks, unusable by offline forensics tools. Consolidated here into the one
// parser: object = "b\0k"; part (since P4) = "mpu\0b\0k\0id\0no";
// legacy part (pre-P4) = "mpu\0id\0no" (no b/k, attributability lost — kLegacyPart
// is the "conservatively do not migrate" criterion). Any new form must be
// registered here
struct PackOwner {
    enum class Kind { kObject, kPart, kLegacyPart, kUnknown } kind = Kind::kUnknown;
    std::string_view bucket, key;  // valid for kObject/kPart (points into the input string)
    std::string_view upload_id;    // valid for kPart/kLegacyPart
    int part_no = 0;               // valid for kPart/kLegacyPart
};
PackOwner parse_pack_owner(std::string_view owner);

// ---- pack record header overhead (fs_data_store §5.2: 22B fixed header + owner) ----
// The pack liveness account (live_bytes) must include the record header, same
// basis as file_size: if only payload is counted, a pack of small objects has
// live/file_size permanently below pack_gc_ratio even at 100% liveness, and
// compaction falls into a permanent "full rewrite → migrate away → rewrite in the
// new pack" loop (docs/gaps.md §2.3a).
// Header length depends on the owner form: object records are "b\0k", part
// records are "mpu\0b\0k\0id\0no". complete rebalances the selected parts'
// accounting from part basis to object basis (refs transfer in the same batch),
// preserving the "object deleted → account reaches zero" invariant; the on-disk
// record stays in mpu form, and the header-length difference vs. the account is a
// slight undercount — the conservative direction (live slightly underestimated ⇒
// one extra compaction converts it to object form, exact thereafter)
inline constexpr int64_t kPackRecHeaderFixed = 22;
inline int64_t pack_rec_overhead(std::string_view b, std::string_view k) {
    return kPackRecHeaderFixed + int64_t(b.size()) + 1 + int64_t(k.size());
}
inline int64_t pack_rec_overhead_part(std::string_view b, std::string_view k,
                                      std::string_view id, int part_no) {
    int digits = 1;
    for (int v = part_no; v >= 10; v /= 10) ++digits;
    return kPackRecHeaderFixed + 3 + 1 + int64_t(b.size()) + 1 + int64_t(k.size()) + 1 +
           int64_t(id.size()) + 1 + digits;
}

}  // namespace lights3::storage::duostore::codec
