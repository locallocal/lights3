// L3: IMetaStore 的 RocksDB 实现（docs/duostore-backend.md §4）。
// 提交类操作 = 单 WriteBatch（§4.5）；跨 key 复合不变量用一把 std::mutex 序列化，
// 纯读（get/list，走 snapshot）不加锁。
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/meta_store.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
class WriteBatch;
}  // namespace rocksdb

namespace lights3::storage::duostore {

struct RocksMetaOptions {
    std::string path;
    bool sync = true;                        // 提交是否 WAL fsync（§6.3 meta_sync）
    size_t block_cache_bytes = 64ull << 20;
};

class RocksMetaStore final : public IMetaStore {
public:
    explicit RocksMetaStore(RocksMetaOptions opt);
    ~RocksMetaStore() override;
    RocksMetaStore(const RocksMetaStore&) = delete;

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
    void ack_reclaims(std::span<const uint64_t> seqs) override;  // 单 WriteBatch
    std::vector<PackStat> pack_stats() override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    bool chunk_referenced(uint64_t file_id) override;
    void close() override;

private:
    // CF 下标（§4.1 表）
    enum Cf { kDefault = 0, kBuckets, kObjects, kUploads, kParts, kRefs, kGcq, kStats, kNumCf };

    // 号段预留（§4.5）：stats 计数器一次 merge +kIdSegment、内存派发
    struct IdRange {
        uint64_t next = 0, limit = 0;
    };
    static constexpr uint64_t kIdSegment = 4096;

    // close 后（db_ 为空）抛 InternalError——防御纵深，把误用变成 500 而非段错误；
    // 契约仍是 close 须在在途请求完成后调用（§9 生命周期）
    rocksdb::DB* db() const;
    std::optional<std::string> get_raw(int cf, std::string_view key);
    void commit(rocksdb::WriteBatch& batch);
    void require_bucket_locked(std::string_view b);
    std::vector<PartRec> scan_parts(std::string_view b, std::string_view k,
                                    std::string_view id);
    uint64_t alloc_id(std::string_view counter_key, IdRange& r);
    void enqueue_reclaim_locked(rocksdb::WriteBatch& batch, const DataRef& ref);
    // 同批维护 refs（chunk 引用表，§4.1）：add=写入 owner、否则删除
    void batch_refs(rocksdb::WriteBatch& batch, const DataRef& ref, bool add,
                    std::string_view owner);

    RocksMetaOptions opt_;
    std::atomic<rocksdb::DB*> db_{nullptr};
    std::vector<rocksdb::ColumnFamilyHandle*> cfs_;
    // 一把互斥序列化全部提交类操作。注意：提交（含 meta_sync=true 的 WAL fsync）
    // 在锁内执行，写路径吞吐上限 ≈ 1/fsync 延迟且 RocksDB group commit 失效——
    // P1 接受；竞争成为瓶颈时的升级路径是 TransactionDB（§4.5，不做，仅注明）
    std::mutex mu_;
    // 号段派发独立小锁：alloc 在数据面每个 chunk 打开时调用（fs_data_store），
    // 不能排在业务提交的 WAL fsync 之后。锁序恒 mu_ → alloc_mu_，无环
    std::mutex alloc_mu_;
    IdRange file_ids_[2];  // 按 Extent::Kind 下标
    IdRange seqs_;         // gcq seq
};

}  // namespace lights3::storage::duostore
