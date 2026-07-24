// L3: DuoStore 后端门面（docs/duostore-backend.md）：S3 语义、ETag/MD5、泵送循环；
// 元数据/数据分离到 IMetaStore / IDataStore 两个可插拔接口，DataRef 为唯一耦合点。
// P1：RocksDB meta + chunk 数据路径；pack（P2）与 GC worker（P3+）后续引入。
#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "core/thread_pool.h"
#include "storage/backend.h"
#include "storage/duostore/data_store.h"
#include "storage/duostore/meta_store.h"

namespace lights3::storage {

// meta 引擎选择（docs/duostore-redis-meta.md §8 / docs/duostore-sqlite-meta.md §8）：
// redis / sqlite 需编译期开启对应 option，否则 from_params 抛 "not compiled in"
enum class DuoMetaKind { kRocksDb, kRedis, kSqlite };

// data 引擎选择（docs/duostore-rados-data.md §10，对偶 meta_kind）：
// rados 需编译期开启 LIGHTS3_DUOSTORE_RADOS_DATA
enum class DuoDataKind { kFs, kRados };

struct DuoStoreConfig {
    std::string name;
    std::filesystem::path root;       // 必填；meta/ chunks/ packs/ 均在其下
    std::filesystem::path meta_path;  // 默认 <root>/meta（可单独指到 SSD）
    DuoMetaKind meta_kind = DuoMetaKind::kRocksDb;
    std::string redis_uri;                // meta=redis 时必填
    std::string redis_prefix = "duo:";    // key 前缀（多实例/测试隔离）
    int redis_timeout_sec = 3;            // 建连 + 单命令超时
    int redis_pool_size = 8;              // 连接池大小
    std::filesystem::path sqlite_path;    // meta=sqlite：DB 文件，默认 <root>/meta.sqlite3
    size_t sqlite_cache = 64ull << 20;    // 页缓存（PRAGMA cache_size）
    DuoDataKind data_kind = DuoDataKind::kFs;
    std::string rados_conf = "/etc/ceph/ceph.conf";  // data=rados 键（docs/duostore-rados-data.md §10）
    std::string rados_client = "client.admin";
    std::string rados_pool;                          // data=rados 时必填
    std::string rados_namespace;                     // pool 内逻辑隔离（多实例/测试）
    uint64_t rados_chunk_size = 8ull << 20;
    uint64_t rados_buffer_total = 256ull << 20;
    int rados_connect_timeout_sec = 5;
    int rados_op_timeout_sec = 0;                    // 0 = 不设 op 超时
    uint64_t chunk_size = 8ull << 20;
    uint64_t pack_threshold = 128 << 10;   // P2 生效
    uint64_t pack_max_size = 128ull << 20;
    int pack_writers = 4;
    double pack_gc_ratio = 0.5;
    int gc_interval_sec = 300;             // P3 生效
    int gc_grace_sec = 300;
    int orphan_scan_interval_sec = 86400;  // P4 生效
    int mpu_ttl_sec = 7 * 86400;
    bool meta_sync = true;
    bool verify_chunk_crc = false;
    size_t rocksdb_block_cache = 64ull << 20;

    // 集中解析 + 范围校验（docs/duostore-backend.md §11）；配置错误抛 std::runtime_error
    static DuoStoreConfig from_params(const std::string& name,
                                      const std::map<std::string, std::string>& params);
};

class DuoStoreBackend final : public IStorageBackend {
public:
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool);
    // 测试注入用：自组装 meta/data
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                    std::unique_ptr<duostore::IMetaStore> meta,
                    std::unique_ptr<duostore::IDataStore> data);
    ~DuoStoreBackend() override;

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body) override;
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
    Task<std::vector<PartMeta>> list_parts(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id) override;
    Task<std::vector<UploadInfo>> list_multipart_uploads(std::string_view bucket) override;

    Task<void> close() override;

private:
    void require_bucket(std::string_view bucket);  // 池线程调用
    // 取对象记录；缺失时区分 NoSuchBucket / NoSuchKey（GET/HEAD 错误语义必须一致）
    duostore::ObjectRec require_object(std::string_view bucket, std::string_view key);

    DuoStoreConfig cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::unique_ptr<duostore::IMetaStore> meta_;
    std::unique_ptr<duostore::IDataStore> data_;
    bool closed_ = false;
};

}  // namespace lights3::storage
