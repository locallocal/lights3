// L3: the single coupling point between DuoStore's metadata and data sides
// (docs/duostore-backend.md §3.1). meta stores/loads DataRef as opaque location
// info; data just reads/writes according to it.
#pragma once

#include <cstdint>
#include <vector>

namespace lights3::storage::duostore {

struct Extent {
    // kRados: file_id maps to the rados object name (docs/duostore-rados-data.md §3.1)
    enum class Kind : uint8_t { kChunk = 0, kPack = 1, kRados = 2 };
    Kind kind = Kind::kChunk;
    uint64_t file_id = 0;  // chunk / pack file number (globally monotonic allocation, §4.5)
    uint64_t offset = 0;   // payload start offset within the pack; always 0 for chunk/rados
    uint64_t length = 0;   // byte count of this extent
    uint32_t crc32c = 0;   // content checksum of this extent

    bool operator==(const Extent&) const = default;
};

// Per-call batch cap for alloc_file_run (docs/gaps.md §3.9): writers grow
// geometrically up to this cap, far below each engine's id segment
// (kIdSegment=4096); a single write session wastes at most kMaxIdRun-1 ids
inline constexpr uint32_t kMaxIdRun = 64;

struct DataRef {
    std::vector<Extent> extents;  // empty = 0-byte object; persisted via run encoding (§4.3)

    uint64_t total() const {
        uint64_t sum = 0;
        for (auto& e : extents) sum += e.length;
        return sum;
    }
};

}  // namespace lights3::storage::duostore
