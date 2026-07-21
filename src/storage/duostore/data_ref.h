// L3: DuoStore 元数据/数据两侧唯一的耦合点（docs/duostore-backend.md §3.1）。
// meta 把 DataRef 当不透明定位信息存取，data 只负责按它读写。
#pragma once

#include <cstdint>
#include <vector>

namespace lights3::storage::duostore {

struct Extent {
    enum class Kind : uint8_t { kChunk = 0, kPack = 1 };  // 可扩展：kRados…
    Kind kind = Kind::kChunk;
    uint64_t file_id = 0;  // chunk / pack 文件号（全局单调分配，§4.5）
    uint64_t offset = 0;   // pack 内 payload 起始偏移；chunk 恒 0
    uint64_t length = 0;   // 本段字节数
    uint32_t crc32c = 0;   // 本段内容校验和

    bool operator==(const Extent&) const = default;
};

struct DataRef {
    std::vector<Extent> extents;  // 空 = 0 字节对象；持久化用 run 编码（§4.3）

    uint64_t total() const {
        uint64_t sum = 0;
        for (auto& e : extents) sum += e.length;
        return sum;
    }
};

}  // namespace lights3::storage::duostore
