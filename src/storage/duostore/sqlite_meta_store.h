// L3: IMetaStore 的 SQLite 实现（docs/duostore-sqlite-meta.md）。
// 提交类操作 = 单 SQL 事务（BEGIN IMMEDIATE + RAII guard，§3.2），复合不变量在
// 事务隔离域内读-校验-写，零窗口；进程内一把 std::mutex 序列化写者（写连接唯一，
// §3.1/§3.4）。纯读走读连接池，WAL 模式下与写者并行。value 编码 100% 复用
// codec.cc（§2.1）；单进程独占，多进程共享 meta 为非目标（§1）。
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/meta_store.h"

namespace lights3::storage::duostore {

struct SqliteMetaOptions {
    std::string path;                  // DB 文件路径（单文件部署，§1）
    bool sync = true;                  // 提交是否持久：synchronous FULL/NORMAL（§6）
    size_t cache_bytes = 64ull << 20;  // 页缓存容量（PRAGMA cache_size，§8）
    int pool_size = 8;                 // 读连接池上限（§3.1）
};

class SqliteMetaStore final : public IMetaStore {
public:
    explicit SqliteMetaStore(SqliteMetaOptions opt);
    ~SqliteMetaStore() override;
    SqliteMetaStore(const SqliteMetaStore&) = delete;

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
    void ack_reclaims(std::span<const uint64_t> seqs) override;  // 单事务单 fsync（§3.3）
    std::vector<PackStat> pack_stats() override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    bool chunk_referenced(uint64_t file_id) override;
    void close() override;

private:
    struct Conn;  // sqlite3* + prepared statement 常驻缓存（.cc 内定义，头文件不泄漏 sqlite3 类型）
    class Stmt;
    class Txn;

    // 读连接租借 RAII（§3.1）：析构归还池；close 后归还即销毁。
    // 特殊成员在 .cc 定义（unique_ptr<Conn> 需要完整类型）
    struct Lease {
        SqliteMetaStore* store = nullptr;
        std::unique_ptr<Conn> conn;
        Lease(SqliteMetaStore* s, std::unique_ptr<Conn> c);
        Lease(Lease&&) noexcept;
        ~Lease();
        Conn& operator*() const;
    };

    // 号段预留（§4，与 RocksMetaStore 同构）：counters 一次 +kIdSegment、内存派发。
    // gcq seq 不走号段——AUTOINCREMENT rowid 随业务事务分配，天然事务性（§2.2）
    struct IdRange {
        uint64_t next = 0, limit = 0;
    };
    static constexpr uint64_t kIdSegment = 4096;

    std::unique_ptr<Conn> open_raw();      // 仅 open + busy_timeout（谱系校验前不碰文件）
    void apply_pragmas(Conn& c, bool full_sync);
    std::unique_ptr<Conn> open_conn(bool full_sync);  // open_raw + apply_pragmas
    // 谱系校验（§2.2）：在任何写入（含 WAL journal 转换）之前执行——app_id/ver 全 0
    // 但 sqlite_master 非空 = 别人的库，拒绝且不留痕
    void check_lineage(Conn& c);
    void init_schema(Conn& c);
    Lease read_conn();                     // 池取；close 后抛 InternalError
    void release(std::unique_ptr<Conn> c);
    Conn& wconn();                         // 写连接；须持 mu_；close 后抛 InternalError

    void require_bucket(Conn& c, std::string_view b);
    std::optional<std::string> object_raw(Conn& c, std::string_view b, std::string_view k);
    UploadRec require_upload_in(Conn& c, std::string_view b, std::string_view k,
                                std::string_view id);
    // 同批维护 refs（chunk 引用表）：add=写入 owner、否则删除
    void write_refs(Conn& c, const DataRef& ref, bool add, std::string_view owner);
    // gcq 入账：seq 由 AUTOINCREMENT 随事务分配，与业务写同批提交/回滚
    void enqueue_reclaim(Conn& c, const DataRef& ref);
    std::vector<PartRec> scan_parts(Conn& c, std::string_view b, std::string_view k,
                                    std::string_view id);
    uint64_t alloc_id(std::string_view counter, IdRange& r);

    SqliteMetaOptions opt_;
    // 单进程独占的 fail-fast enforcement（§1；对应 RocksDB 的 LOCK 文件）：
    // <path>.lock 上的 flock(LOCK_EX|LOCK_NB)，构造取、close 释放
    int lock_fd_ = -1;

    // 一把互斥序列化全部提交类操作（wc_ 上的事务恒在 mu_ 内，§3.4）。提交（含
    // sync=true 的 WAL fsync）在锁内完成，写吞吐上限 ≈ 1/fsync 延迟——与 RocksDB
    // 版同一取舍（P1 级接受，不做 group commit）
    std::mutex mu_;
    std::unique_ptr<Conn> wc_;  // 专用写连接（BEGIN IMMEDIATE 事务恒在此连接）
    // 号段专用连接，恒 synchronous=FULL（独立于 opt_.sync，§4）；alloc_mu_ 保护
    // IdRange 与本连接。与 mu_ 无嵌套（alloc 由数据面在业务事务之外调用）
    std::mutex alloc_mu_;
    std::unique_ptr<Conn> ac_;
    IdRange file_ids_[2];  // 按 Extent::Kind 下标

    std::mutex pool_mu_;
    std::vector<std::unique_ptr<Conn>> idle_;
    bool closed_ = false;
};

}  // namespace lights3::storage::duostore
