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

}  // namespace lights3::storage::duostore
