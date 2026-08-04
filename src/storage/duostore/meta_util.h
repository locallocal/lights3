// L3: meta store 实现共享的纯计算 helper（docs/duostore-backend.md §2.1："实现间
// 重复的 S3 语义用共享 helper 压到最低"）。仅依赖 meta_store.h 的记录类型与
// multipart 工具，不含任何存储引擎耦合。
#pragma once

#include <chrono>
#include <map>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "storage/duostore/meta_store.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

// gcq 单项 extent 上限（docs/gaps.md §2.11）：删除 TB 级对象（数十万 extent）时
// 把 DataRef 拆成多条 gcq 项入队，GC 消费端单批 peek 的解码内存驻留有界。ack 逐
// 条独立、unlink 幂等，拆分不改变崩溃语义（§9.1 先物理删后销账的论证不变）
inline constexpr size_t kReclaimMaxExtents = 4096;

// complete_upload 的分片选择与对象拼装（RocksDB/Redis/SQLite 三实现原为逐字相同
// 的块）：逐项 ETag 校验（缺失/不符抛 InvalidPart）、按提交顺序拼接 extent、累加
// size、合成总 ETag 与 last_modified。selected 输出选中分片号，供调用方做 refs
// 转移与未选中分片的 GC 落账分流。version 由调用方在读到旧对象后另行设置。
inline ObjectRec assemble_completed_object(ObjectMeta meta, std::span<const PartInfo> parts,
                                           const std::map<int, PartRec>& stored,
                                           std::set<int>& selected) {
    ObjectRec rec;
    rec.meta = std::move(meta);
    std::vector<std::string> md5s;
    for (const auto& pi : parts) {
        auto sit = stored.find(pi.part_no);
        if (sit == stored.end() || sit->second.etag != strip_etag_quotes(pi.etag))
            throw s3::S3Error(s3::S3ErrorCode::InvalidPart,
                              "One or more of the specified parts could not be found or the "
                              "ETag did not match.",
                              rec.meta.key);
        md5s.push_back(sit->second.etag);
        selected.insert(pi.part_no);
        rec.meta.size += sit->second.size;
        const auto& ex = sit->second.data.extents;
        rec.data.extents.insert(rec.data.extents.end(), ex.begin(), ex.end());
    }
    rec.meta.etag = combined_etag(md5s);
    rec.meta.last_modified = std::chrono::system_clock::now();
    return rec;
}

// 压实换 ref（swap_extents，§9.2）的 refs 差集。
//
// refs 表按 file_id 是 last-wins 语义：对同一 file_id"先 Put 后 Delete"（四引擎的
// WriteBatch / Lua 顺序 / SQL 顺序 / TiKV 后者胜都如此）净效果是删除。而压实只替换
// 一个 pack extent，to 与 from **共享全部未迁移的 chunk extent**——整表 add(to) 再
// 整表 remove(from) 会抹掉这些仍被对象引用的 chunk 的 refs 表项，孤儿扫描随后
// unlink 活数据（不可恢复的数据丢失）。
//
// 只对真正新增（to−from）与真正消失（from−to）的 file_id 操作，顺序即无关，同时
// 省掉无谓 mutation。pack 存活账是加法语义（同 pack 的 +1/-1 自然抵消），不受影响，
// 故本 helper 只服务 refs 一侧；kPack extent 不入 refs，在此一并滤掉。
struct RefsDelta {
    DataRef added;    // 需 Put refs 的 extent
    DataRef removed;  // 需 Delete refs 的 extent
};

inline RefsDelta refs_delta(const DataRef& from, const DataRef& to) {
    auto ref_ids = [](const DataRef& r) {
        std::set<uint64_t> ids;
        for (const auto& e : r.extents)
            if (e.kind != Extent::Kind::kPack) ids.insert(e.file_id);
        return ids;
    };
    std::set<uint64_t> from_ids = ref_ids(from), to_ids = ref_ids(to);
    RefsDelta d;
    std::set<uint64_t> seen;
    for (const auto& e : to.extents)
        if (e.kind != Extent::Kind::kPack && !from_ids.count(e.file_id) &&
            seen.insert(e.file_id).second)
            d.added.extents.push_back(e);
    seen.clear();
    for (const auto& e : from.extents)
        if (e.kind != Extent::Kind::kPack && !to_ids.count(e.file_id) &&
            seen.insert(e.file_id).second)
            d.removed.extents.push_back(e);
    return d;
}

}  // namespace lights3::storage::duostore
