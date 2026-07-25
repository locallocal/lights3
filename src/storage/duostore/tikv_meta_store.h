// L3: IMetaStore 的 TiKV 实现（docs/duostore-tikv-meta.md）。
// 提交类操作 = 快照读（start_ts）+ C++ 组 mutation 批 + 乐观 2PC 提交（tikv_client
// 侧车），WriteConflict → 取新 ts 重读重试（§4.1）；只读前置条件（bucket 存在性、
// 空检查、upload 存在性）的写偏斜用守卫分片 Op::Lock 物化（§4.3）。2PC 原子性对
// 任意进程成立——多网关共享同一 PD 即共享 meta，不持业务互斥（§4.5，仅号段小锁）。
// value 编码 100% 复用 codec.cc（§3.1）；持久化 = raft 多数派，meta_sync 无意义（§7.1）。
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/meta_store.h"
#include "storage/duostore/tikv_client.h"

namespace lights3::storage::duostore {

struct TikvMetaOptions {
    std::vector<std::string> pd_endpoints;  // meta=tikv 时必填
    std::string prefix = "duo:";            // 全部 key 前缀（多实例/测试隔离，§3.1）
    // mTLS 三件套（可选，三者同给才启用，§9）
    std::string ca_path;
    std::string cert_path;
    std::string key_path;
};

class TikvMetaStore final : public IMetaStore {
public:
    explicit TikvMetaStore(TikvMetaOptions opt);
    ~TikvMetaStore() override;
    TikvMetaStore(const TikvMetaStore&) = delete;

    void create_bucket(std::string_view b) override;
    void delete_bucket(std::string_view b) override;
    bool bucket_exists(std::string_view b) override;
    std::vector<BucketInfo> list_buckets() override;

    std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) override;
    void put_object(std::string_view b, std::string_view k, ObjectRec rec) override;
    bool delete_object(std::string_view b, std::string_view k) override;
    ListResult list_objects(std::string_view b, const ListOptions& opt) override;

    std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) override;
    UploadRec require_upload(std::string_view b, std::string_view k,
                             std::string_view id) override;
    void put_part(std::string_view b, std::string_view k, std::string_view id,
                  PartRec p) override;
    std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                    std::string_view id) override;
    std::vector<UploadInfo> list_uploads(std::string_view b) override;
    std::string complete_upload(std::string_view b, std::string_view k, std::string_view id,
                                std::span<const PartInfo> parts) override;
    void abort_upload(std::string_view b, std::string_view k, std::string_view id) override;

    uint64_t alloc_file_id(Extent::Kind kind) override;
    std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max) override;
    void ack_reclaim(uint64_t seq) override;
    void ack_reclaims(std::span<const uint64_t> seqs) override;  // 单事务批量销账
    std::vector<PackStat> pack_stats() override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    bool chunk_referenced(uint64_t file_id) override;
    void close() override;

private:
    // 号段预留（§5，与 RocksDB/Redis 版同构）：计数器 key 上的 RMW 小事务一次
    // +kIdSegment、内存派发。TiKV 提交即 raft 持久，无 Redis 版的空烧补偿
    struct IdRange {
        uint64_t next = 0, limit = 0;
    };
    static constexpr uint64_t kIdSegment = 4096;
    // 守卫分片数（§4.3）：并发 put 相撞概率 1/16，delete_bucket/complete/abort
    // 写全量 16 个分片物化写偏斜冲突
    static constexpr uint32_t kGuardShards = 16;

    // 存活客户端。close 契约 = 无在途调用（DuoStoreBackend close 顺序保证）；
    // 原子指针使违约的 close 后调用确定性地抛 InternalError 而非 TOCTOU 数据
    // 竞争（rocks 版 db_ 同型守卫）
    TikvClient& client();

    // ---- key 构造（§3.2：prefix + 单字符表标签 + codec 复合段）----
    std::string tkey(char tag, std::string_view rest) const;
    std::string bucket_key(std::string_view b) const;                  // 'B'
    std::string bucket_guard(std::string_view b, uint32_t shard) const;  // 'b'
    std::string object_key(std::string_view b, std::string_view k) const;  // 'O'
    std::string upload_key(std::string_view b, std::string_view k, std::string_view id) const;
    std::string upload_guard(std::string_view b, std::string_view k, std::string_view id,
                             uint32_t shard) const;  // 'u'
    std::string part_key(std::string_view b, std::string_view k, std::string_view id,
                         int part_no) const;                        // 'P'
    std::string refs_key(uint64_t file_id) const;                   // 'R'
    std::string gcq_key(uint64_t seq) const;                        // 'G'
    std::string counter_key(char kind) const;                       // 'C'
    // [lo, hi) 前缀区间（bucket 名/上层校验保证复合段无 NUL 歧义）
    std::pair<std::string, std::string> range_of(char tag, std::string_view rest) const;

    // ---- 事务与读辅助（.cc 内实现）----
    // 乐观重试循环（§4.1）：每轮新 start_ts → body(ts, muts) 读算组批 → commit；
    // muts 空 = 纯读/提前返回，不发事务。冲突指数退避 100µs..6.4ms，16 次上限
    template <typename Body>
    auto txn_retry(const char* what, Body&& body);
    // 纯读重试一次（§6.4）；pingcap 异常 → S3Error(InternalError) 的统一翻译
    template <typename Fn>
    auto guarded(const char* what, Fn&& fn);

    std::optional<std::string> snap_get(uint64_t ver, const std::string& key);
    // 批量快照读（一次 KvBatchGet 往返换逐 key 串行 Get），重试语义同 snap_get
    std::vector<std::optional<std::string>> snap_get_many(uint64_t ver,
                                                          const std::vector<std::string>& keys);
    // 分页扫全量 [lo, hi)（每页 1024），escape：callback 返回 false 提前停
    template <typename Fn>
    void scan_range(uint64_t ver, std::string lo, const std::string& hi, Fn&& cb);

    uint64_t alloc_id(char kind, IdRange& r);
    // gcq 入账：seq 预派发（独立小事务），入账本身保持纯写 mutation
    void enqueue_reclaim(std::vector<TikvMutation>& muts, const DataRef& ref);
    void mut_refs(std::vector<TikvMutation>& muts, const DataRef& ref, bool add,
                  std::string_view owner);
    // parts 全量读（按 part_no 升序，be16 尾缀天然有序）
    std::vector<PartRec> scan_parts(uint64_t ver, std::string_view b, std::string_view k,
                                    std::string_view id);

    TikvMetaOptions opt_;
    std::unique_ptr<TikvClient> client_owned_;
    std::atomic<TikvClient*> client_{nullptr};  // close 后置空（见 client() 注释）

    // 号段派发独立小锁（alloc 在数据面每个 chunk 打开时调用，不排队业务提交）；
    // 段耗尽的网络续段在锁外进行（alloc_id 注释）
    std::mutex alloc_mu_;
    IdRange file_ids_[2];  // 按 Extent::Kind 下标
    IdRange seqs_;         // gcq seq
};

}  // namespace lights3::storage::duostore
