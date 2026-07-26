// L3: DuoStore 数据侧接口（docs/duostore-backend.md §3.3）。协程 Task<T>；
// 各实现自行决定内部是否切池线程。
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "core/task.h"
#include "http/model.h"
#include "storage/duostore/data_ref.h"

namespace lights3::storage::duostore {

struct WriteHint {
    std::optional<uint64_t> content_length;  // body.length()，chunked 时 nullopt
    // pack record 内嵌的归属（§5.2："bucket\0key" 或 "mpu\0<id>\0<part_no>"）：
    // 压实顺扫反查存活与灾难恢复离线打捞用；无 pack 的引擎忽略
    std::string owner;
};

struct DataWriter {
    virtual Task<void> write(std::span<const std::byte> buf) = 0;
    virtual Task<DataRef> finish() = 0;  // 落盘后返回定位；未 finish 即析构 = 丢弃
    virtual ~DataWriter() = default;
};

// P4 压实顺扫的结果统计（docs/duostore-backend.md §9.2；P1-P3 未实现）
struct GcRewrite {
    uint64_t scanned = 0;
    uint64_t migrated = 0;
    uint64_t corrupt = 0;
};

struct IDataStore {
    virtual Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) = 0;
    // [first,last] 为 resolve_range 后的闭区间；返回流式 BodyReader（length()=last-first+1）
    virtual Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                               uint64_t last) = 0;
    virtual Task<void> remove(std::span<const Extent> extents) = 0;  // 幂等（ENOENT 忽略）
    // 整 pack 文件删除（§9.1：sealed 且 live_recs==0 的 pack）；幂等。纯虚：无 pack
    // 实体的引擎显式写 no-op override（对齐 rewrite_pack 惯例）——静默接口默认会让
    // "有 pack 但忘了实现删除"的新引擎编译通过、GC 记了账却永不释放字节
    virtual Task<void> remove_pack(uint64_t pack_id) = 0;
    virtual Task<GcRewrite> rewrite_pack(uint64_t pack_id) = 0;      // 压实顺扫（§9.2）
    virtual Task<void> close() = 0;
    virtual ~IDataStore() = default;
};

}  // namespace lights3::storage::duostore
