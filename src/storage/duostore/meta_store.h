// L3: DuoStore 元数据侧接口（docs/duostore-backend.md §3.2）。
// 契约：同步接口，必须在池线程调用（由 DuoStoreBackend 统一在入口切池，§2.2）；
// 错误抛 s3::S3Error；提交类方法内部单事务完成
// "写新 + 旧 DataRef 入 GC 账 + 引用/统计更新"（§4.5）。
// 前提：bucket/key/upload_id 不含 NUL——'\0' 是 key 编码的分隔符（§4.1），由共享
// 校验层 validate_bucket_name/validate_object_key 保证；codec 键构造器另有防御
// 性检查（违反即抛 InternalError，绝不静默产生跨记录 key 碰撞）。
#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/backend.h"
#include "storage/duostore/data_ref.h"

namespace lights3::storage::duostore {

struct ObjectRec {
    ObjectMeta meta;       // key/size/etag/content_type/last_modified/user_meta
    DataRef data;
    uint64_t version = 0;  // 每次写 +1（实现内维护）；GC 压实换 ref 的乐观校验（§9.2）
};

struct UploadRec {
    std::string upload_id;
    ObjectMeta meta;  // key + content_type/user_meta（complete 时生效）
    int64_t initiated_ms = 0;
};

struct PartRec {
    int part_no = 0;
    uint64_t size = 0;
    std::string etag;  // 分片内容 MD5（未加引号 hex）
    int64_t modified_ms = 0;
    DataRef data;
};

struct Reclaim {
    std::vector<Extent> extents;  // 待物理回收
    int64_t enqueue_ms = 0;       // 入队时刻（unix ms）；GC 消费端据此判 gc_grace（§9.1）
};

struct PackStat {
    uint64_t pack_id = 0;
    uint64_t file_size = 0;  // 封存时由数据面回报；0 = 未知（崩溃遗留，压实时再 stat）
    int64_t live_bytes = 0;
    int64_t live_recs = 0;
    bool sealed = false;
};

struct IMetaStore {
    // ---- bucket ----
    virtual void create_bucket(std::string_view b) = 0;  // 已存在→BucketAlreadyOwnedByYou
    // 不存在→NoSuchBucket；有对象或进行中 multipart→BucketNotEmpty（对齐 AWS）
    virtual void delete_bucket(std::string_view b) = 0;
    virtual bool bucket_exists(std::string_view b) = 0;
    virtual std::vector<BucketInfo> list_buckets() = 0;

    // ---- object ----
    virtual std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) = 0;
    virtual void put_object(std::string_view b, std::string_view k, ObjectRec rec) = 0;
    virtual bool delete_object(std::string_view b, std::string_view k) = 0;  // 不存在返回 false（幂等）
    virtual ListResult list_objects(std::string_view b, const ListOptions& opt) = 0;

    // ---- multipart ----
    virtual std::string create_upload(std::string_view b, std::string_view k,
                                      ObjectMeta meta) = 0;
    virtual UploadRec require_upload(std::string_view b, std::string_view k,
                                     std::string_view id) = 0;  // 缺→NoSuchUpload
    virtual void put_part(std::string_view b, std::string_view k, std::string_view id,
                          PartRec p) = 0;  // 同号旧分片同批入 GC 账
    virtual std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                            std::string_view id) = 0;
    virtual std::vector<UploadInfo> list_uploads(std::string_view b) = 0;
    virtual std::string complete_upload(std::string_view b, std::string_view k,
                                        std::string_view id,
                                        std::span<const PartInfo> parts) = 0;  // 返回总 ETag（§8）
    virtual void abort_upload(std::string_view b, std::string_view k,
                              std::string_view id) = 0;

    // ---- 资源分配与 GC 记账（§9）----
    virtual uint64_t alloc_file_id(Extent::Kind kind) = 0;  // 持久单调，号段预留
    // 取 seq >= min_seq 的最早至多 max 项（seq 升序）。GC 消费端按 min_seq 断点
    // 续扫：被 grace/pin 跳过而未销账的队头项不会让整轮卡死或被重复统计（§9.1）
    virtual std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max,
                                                                   uint64_t min_seq = 0) = 0;
    virtual void ack_reclaim(uint64_t seq) = 0;    // 物理删除成功后销账
    // 批量销账：默认逐条转发；实现可覆写为单事务/单批提交。GC 消费端应优先走本
    // 接口——逐条 ack 的成本按实现差异巨大（SQLite 版每条是一次独立 fsync 且与
    // 业务提交争同一把写锁，RocksDB 版接近免费）。丢 ack 无害（gcq 残留重试，
    // unlink 幂等），故批量语义安全（主文档 §9.1 崩溃论证）
    virtual void ack_reclaims(std::span<const uint64_t> seqs) {
        for (uint64_t s : seqs) ack_reclaim(s);
    }
    // pack 存活账（§9.1/§9.2）：live_bytes/live_recs 随提交类事务同批增减（pack
    // extent 不入 refs，走本账），pack_stats() 返回全部有账 pack（含 live=0 与未
    // 封存项——空 pack 整删与重启弃用都依赖能看见它们）
    virtual std::vector<PackStat> pack_stats() = 0;
    // 封存（数据面轮转/close 时回调；幂等）：file_size=0 表示未知，不得覆盖已记录
    // 的非零值——崩溃遗留 pack 由 DuoStoreBackend 启动时以 0 补封（重启弃用，§5.2）
    virtual void seal_pack(uint64_t pack_id, uint64_t file_size) = 0;
    // 空 pack 整文件 unlink 成功后销账（§9.1 顺序铁律同 ack_reclaim：先物理删后销）
    virtual void drop_pack_stat(uint64_t pack_id) = 0;
    virtual bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                              const DataRef& from, const DataRef& to) = 0;  // 压实换 ref
    virtual bool chunk_referenced(uint64_t file_id) = 0;  // 孤儿扫描
    // 孤儿反向对账（§9.3）：遍历 refs 表全部 file_id（chunk/rados 同账，顺序不保证）。
    // 快照语义从宽：遍历期间的并发增删可见与否均可——调用方（孤儿扫描）对"文件在
    // refs 缺"走 chunk_referenced 现点复查、对"refs 在文件缺"只告警不删，两向都容忍
    // 弱一致快照
    virtual void scan_refs(const std::function<void(uint64_t file_id)>& cb) = 0;
    virtual void close() = 0;
    virtual ~IMetaStore() = default;
};

}  // namespace lights3::storage::duostore
