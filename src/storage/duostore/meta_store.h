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

// gcq 入账来源（docs/gaps.md §6.1）：GC 按来源分桶计数，才定位得到"回收压力来自
// 覆盖写还是批量删除还是 mpu 弃件"。落盘为 codec gcq 记录的 reason 字节（此前恒写
// 0 且解码即丢弃），旧账解出 kUnknown
enum class ReclaimReason : uint8_t {
    kUnknown = 0,        // P4 之前入队的旧账
    kOverwrite = 1,      // put_object / complete_upload 覆盖同名对象的旧版本
    kDelete = 2,         // delete_object
    kPartOverwrite = 3,  // 同号分片重传（last-write-wins）
    kAbort = 4,          // abort_upload（含 GC 的 mpu_ttl 过期清理）
    kComplete = 5,       // complete_upload 未被选中的分片
};

// 指标标签与日志用；未知取值一律回落 "unknown"（旧账/未来新增来源）
inline const char* reclaim_reason_name(ReclaimReason r) {
    switch (r) {
        case ReclaimReason::kOverwrite: return "overwrite";
        case ReclaimReason::kDelete: return "delete";
        case ReclaimReason::kPartOverwrite: return "part_overwrite";
        case ReclaimReason::kAbort: return "abort";
        case ReclaimReason::kComplete: return "complete";
        case ReclaimReason::kUnknown: break;
    }
    return "unknown";
}

struct Reclaim {
    std::vector<Extent> extents;  // 待物理回收
    int64_t enqueue_ms = 0;       // 入队时刻（unix ms）；GC 消费端据此判 gc_grace（§9.1）
    ReclaimReason reason = ReclaimReason::kUnknown;
};

struct PackStat {
    uint64_t pack_id = 0;
    uint64_t file_size = 0;  // 封存时由数据面回报；0 = 未知（崩溃遗留，压实时再 stat）
    int64_t live_bytes = 0;
    int64_t live_recs = 0;
    bool sealed = false;
};

// swap_extents_batch 的单项请求（压实按 owner 聚合后逐对象一条，§9.2）
struct SwapReq {
    std::string bucket;
    std::string key;
    uint64_t expect_version = 0;
    DataRef from;
    DataRef to;
};

// 提交结果不明（网络型引擎专有）：redis EVALSHA 后连接断、tikv primary commit
// 超时——事务**可能已经生效**。对本地引擎"抛异常 ≈ 未提交"成立，对这两个不成立。
// 调用方（commit_or_discard）据此**不得**兜底物理删数据：提交实已生效时删掉的是
// 已被对象引用的数据，产生指向已删数据的坏对象；未生效则留给孤儿扫描收敛。
// 对客户端仍是 InternalError（500，语义不变）
struct UndeterminedCommit : s3::S3Error {
    explicit UndeterminedCommit(std::string msg)
        : S3Error(s3::S3ErrorCode::InternalError, std::move(msg)) {}
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
    // 只要 meta 不要 manifest（docs/gaps.md §3.9）：HEAD/前置读走这里。
    // decode_object 会物化整份 extent vector（65 万 extent ≈ 26MB）后立刻丢弃，
    // decode_object_meta 只解码定长头
    virtual std::optional<ObjectMeta> head_object(std::string_view b, std::string_view k) = 0;
    // cond.active() 时在本事务的原子区内按 PutCondition 契约（storage/backend.h）
    // 校验旧记录：违反抛 PreconditionFailed / NoSuchKey，事务不提交（共享检查
    // 见 meta_util.h check_put_condition）
    virtual void put_object(std::string_view b, std::string_view k, ObjectRec rec,
                            PutCondition cond = {}) = 0;
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
    // 分页提示（docs/gaps.md §5.1）：返回 (key, upload_id) 严格大于
    // (key_marker, id_marker) 的项，升序；limit>0 时最多返回 limit 条。
    // 提示可被忽略——引擎无法下推时返回全量即可，调用方（DuoStoreBackend）总会
    // 再过一遍 apply_uploads_page，语义不依赖引擎是否下推。
    // 注意调用方在 delimiter 非空时传 limit=0：分组要看到全貌才能判定截断
    virtual std::vector<UploadInfo> list_uploads(std::string_view b,
                                                 std::string_view key_marker = {},
                                                 std::string_view id_marker = {},
                                                 int limit = 0) = 0;
    virtual std::string complete_upload(std::string_view b, std::string_view k,
                                        std::string_view id,
                                        std::span<const PartInfo> parts) = 0;  // 返回总 ETag（§8）
    virtual void abort_upload(std::string_view b, std::string_view k,
                              std::string_view id) = 0;

    // ---- 资源分配与 GC 记账（§9）----
    // 批派发（docs/gaps.md §3.9）：返回连续 run [first, first+n) 的首 id，持久
    // 单调、号段预留。并发写者逐个派发会让同对象的 chunk id 交错，manifest 的
    // run 编码完全失效（编码后反而膨胀 28%）；写者按几何增长的 run 批取即恢复
    // 连续性。n ≤ kMaxIdRun；run 的未用尾部弃置无害（id 只需唯一单调，不需连续）
    virtual uint64_t alloc_file_run(Extent::Kind kind, uint32_t n) = 0;
    uint64_t alloc_file_id(Extent::Kind kind) { return alloc_file_run(kind, 1); }
    // 取 seq >= min_seq 的最早至多 max 项（seq 升序）。GC 消费端按 min_seq 断点
    // 续扫：被 grace/pin 跳过而未销账的队头项不会让整轮卡死或被重复统计（§9.1）。
    // max_extents = 批内累计 extent 数上限（docs/gaps.md §2.11：按条数批 256 条最坏
    // 驻留 GB 级）：累计达到上限即提前收批，但至少返回 1 项（拆分前遗留的超大单项
    // 仍要能被消费）
    virtual std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(
        size_t max, uint64_t min_seq = 0, size_t max_extents = SIZE_MAX) = 0;
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
    // 批量换 ref（docs/gaps.md §2.13 压实批量化）：每项独立 CAS，返回逐项成败。
    // 默认逐条转发；本地引擎（rocks/sqlite）覆写为单批/单事务提交——sqlite 逐条
    // swap 是每条一次 fsync 且与业务提交争同一把写锁。网络引擎（redis/tikv）保持
    // 逐条：合并成单事务会让一个对象的 CAS 失败殃及整批（all-or-nothing），而其
    // 单条提交本就是一次 RTT
    virtual std::vector<bool> swap_extents_batch(std::span<const SwapReq> reqs) {
        std::vector<bool> out;
        out.reserve(reqs.size());
        for (const auto& r : reqs)
            out.push_back(swap_extents(r.bucket, r.key, r.expect_version, r.from, r.to));
        return out;
    }
    // 多网关 GC 租约（docs/gaps.md §6.1）：GC/孤儿扫描单实例此前只是 gc_enabled
    // **约定**，误配两台同开 GC 会互相 unlink 对方判定的空 pack。每轮开始前取
    // 租约：共享型引擎（redis/tikv）以带 TTL 的原子 CAS 实现——owner 相同即续租
    // 刷新 TTL，他人持有且未过期返回 false（本轮跳过）；本地引擎（rocks/sqlite）
    // 单进程文件锁已保证独占，默认恒 true。owner 为实例标识（进程内随机生成）。
    // 崩溃的持有者由 TTL 过期自然让位——租约不解决 pin 表不共享的问题（进程内
    // pin 他网关不可见），gc_grace ≥ 最长预期 GET 时长的部署约束仍然成立
    virtual bool try_gc_lease(std::string_view /*owner*/, int64_t /*ttl_ms*/) { return true; }
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
