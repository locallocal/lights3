// L3: DuoStore 数据侧接口（docs/duostore-backend.md §3.3）。协程 Task<T>；
// 各实现自行决定内部是否切池线程。
#pragma once

#include <cstdint>
#include <functional>
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

// P4 压实顺扫的结果统计（docs/duostore-backend.md §9.2）
struct GcRewrite {
    uint64_t scanned = 0;    // 完整解析（含 crc 通过）的 record 数
    uint64_t migrated = 0;   // 存活确认并成功换 ref 的 record 数
    uint64_t corrupt = 0;    // magic/头/crc 损坏的 record 数（torn tail 不计——重启弃用的预期形态）
    uint64_t file_size = 0;  // 实际文件大小；崩溃遗留 seal(0) 的 pack 借此回填存活率分母
};

struct IDataStore;

// 压实迁移回调（§9.2，DuoStoreBackend 装配）：数据面顺扫出 (owner, from, payload)，
// 回调负责 owner 反查存活 + 追加回本 store（经 self.open_writer 进 active pack）+
// swap_extents 乐观换 ref。返回 true = 已迁移；false = 死区或存活但暂不可迁（进行
// 中 mpu 等），数据面一律不动原 record——pack 的删除恒走"live 账归零 + 空 pack
// 整删"路径，误判不丢数据。标准实现见 duostore_backend.h 的 migrate_pack_record
using PackMigrateFn = std::function<Task<bool>(
    IDataStore& self, std::string_view owner, const Extent& from,
    std::span<const std::byte> payload)>;

// 写侧 pin 钩子（§9.3）：孤儿扫描不得回收"写入中尚未提交 meta"的 chunk——慢流式
// PUT 的早期 chunk mtime 可远逾 gc_grace，仅靠 mtime 宽限不充分。writer 在分配
// file_id 时 pin、未 finish 即析构时解 pin；finish 之后的解 pin 责任移交调用方
// （DuoStoreBackend 在 meta 提交 / 兜底删除之后解除）。
// **每个产出 chunk 类实体的 data store 都必须接**：漏接的引擎会让孤儿扫描把在途
// 大对象已落地的分片当无引用文件删掉（docs/gaps.md §1.2）
struct ChunkPinHooks {
    std::function<void(uint64_t)> pin;
    std::function<void(uint64_t)> unpin;

    void pin_one(uint64_t id) const {
        if (pin) pin(id);
    }
    void unpin_one(uint64_t id) const {
        if (unpin) unpin(id);
    }
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
    // 孤儿扫描枚举（§9.3）：遍历数据面全部 chunk 类实体（fs = chunks/ 目录，rados =
    // namespace 对象列举，docs/duostore-rados-data.md §8.2——接口在 P4 定形，rados 实现
    // 排 C4），逐个回调 (file_id, mtime_ms)。孤儿判定（refs 反查/grace/pin）是调用方
    // （DuoStoreBackend）的事——数据面只枚举，不做存活判断。纯虚（对齐 remove_pack
    // 惯例）：不支持枚举的引擎显式抛错，绝不静默空扫谎报"无孤儿"
    virtual Task<void> scan_chunks(
        const std::function<void(uint64_t file_id, int64_t mtime_ms)>& cb) = 0;
    // "该 pack 是否正被某个活着的写者持有"（启动补封用，docs/gaps.md §1.4）。
    // fs 实现探测 active pack 的咨询锁；无 pack 实体或无从探测的引擎返回 false
    // （= 不阻止补封，与本改动前的行为一致）。**false 必须是保守方向**：返回
    // true 只会让补封推迟到下次启动，返回 false 却可能封掉别人正在写的 pack
    virtual bool pack_write_locked(uint64_t /*pack_id*/) { return false; }
    virtual Task<void> close() = 0;
    virtual ~IDataStore() = default;
};

}  // namespace lights3::storage::duostore
