// L3: 内存后端 —— 单测与 demo 用，语义与 LocalFs 对齐。
// 它同时被注册为一等后端（storage/registry.cc），配错了就是把整个网关的数据放进
// 一个无上限的堆里；故带容量闸门与 mpu 过期清理（docs/gaps.md §6.3）
#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "storage/backend.h"

namespace lights3::storage {

struct MemoryOptions {
    // 容量上限（对象 + 在途分片的字节和）。0 = 不限——单测与 demo 的默认，
    // 保持既有行为；生产配置应显式给值，超限的写返回 503 SlowDown
    uint64_t max_bytes = 0;
    // 未 complete/abort 的 multipart 过期时长。0 = 不过期。清理在 multipart 类
    // 操作入口顺带做（本后端不接定时器：无后台线程是它作为单测夹具的一部分）
    int mpu_ttl_sec = 24 * 3600;
};

class MemoryBackend final : public IStorageBackend {
public:
    MemoryBackend() = default;
    explicit MemoryBackend(MemoryOptions opt) : opt_(opt) {}

    // 用量（测试与 registry 指标回调用）
    uint64_t used_bytes() const;
    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                               std::string_view upload_id) override;
    Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key,
                                     std::string_view upload_id,
                                     const ListPartsOptions& opt) override;
    Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket,
                                                   const ListUploadsOptions& opt) override;
    // 释放全部驻留（关停即归还内存；此前无 close，进程收尾前一直占着）
    Task<void> close() override;

private:
    // data 为不可变共享块（docs/gaps.md §3.9）：get_object 在锁内只取一次
    // shared_ptr，大对象不再在全局锁内整体拷贝。put 覆盖同 key 时旧块由仍在
    // 流式读它的 GET 持有，读完自然释放——天然的快照隔离。
    // 剩余临界区都是 O(map 操作)，故本后端不接线程池：demo/单测定位下把
    // 微秒级锁放在事件循环线程上是有意的取舍
    struct Object {
        ObjectMeta meta;
        std::shared_ptr<const std::string> data;
    };
    struct Bucket {
        BucketInfo info;
        std::map<std::string, Object> objects;  // key 有序
    };
    struct Part {
        std::string data;
        std::string etag;  // 分片内容 MD5 hex
        std::chrono::system_clock::time_point uploaded;
    };
    struct Upload {
        std::string bucket;
        std::string key;
        ObjectMeta meta;
        std::map<int, Part> parts;  // part_no 有序
        std::chrono::system_clock::time_point initiated;
    };

    Bucket& bucket_or_throw(const std::string& name);
    Upload& upload_or_throw(std::string_view bucket, std::string_view key,
                            std::string_view upload_id);
    // 容量闸门（持 m_ 调用）：delta 为本次净增字节，超限抛 SlowDown 且不改账
    void reserve_locked(int64_t delta);
    // 读 body 途中的提前闸门（不持 m_ 调用）：已用 + 本请求已缓冲超限即抛 SlowDown
    void check_inflight(size_t buffered) const;
    void expire_uploads_locked();  // mpu_ttl 过期清理（持 m_）

    MemoryOptions opt_;
    mutable std::mutex m_;
    uint64_t used_bytes_ = 0;  // 对象 + 在途分片的字节和（m_ 保护）
    std::map<std::string, Bucket> buckets_;
    std::map<std::string, Upload> uploads_;  // upload_id → 状态
};

}  // namespace lights3::storage
