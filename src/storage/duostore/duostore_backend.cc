#include "storage/duostore/duostore_backend.h"

#include <chrono>
#include <stdexcept>

#include "core/config.h"
#include "core/log.h"
#include "core/util/crypto.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/rocks_meta_store.h"
#include "storage/multipart.h"

#ifdef LIGHTS3_DUOSTORE_REDIS_META
#include "storage/duostore/redis_meta_store.h"
#endif

#ifdef LIGHTS3_DUOSTORE_SQLITE_META
#include "storage/duostore/sqlite_meta_store.h"
#endif

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using namespace duostore;

// ---------- 配置解析（§11）----------

namespace {

// 全部标量解析统一诊断格式与严格性（拒绝尾部垃圾）；配置错误一律 runtime_error
[[noreturn]] void bad_param(const std::string& name, const char* key, const std::string& v) {
    throw std::runtime_error("duostore backend '" + name + "': invalid " + key + ": " + v);
}

bool parse_bool_param(const std::string& name, const char* key, const std::string& v) {
    try {
        return parse_bool(v);  // 共享 token 集（core/config.h）
    } catch (...) {
        bad_param(name, key, v);
    }
}

int parse_int_param(const std::string& name, const char* key, const std::string& v) {
    try {
        size_t pos = 0;
        int r = std::stoi(v, &pos);
        if (pos == v.size()) return r;
    } catch (...) {
    }
    bad_param(name, key, v);
}

double parse_double_param(const std::string& name, const char* key, const std::string& v) {
    try {
        size_t pos = 0;
        double r = std::stod(v, &pos);
        if (pos == v.size()) return r;
    } catch (...) {
    }
    bad_param(name, key, v);
}

}  // namespace

DuoStoreConfig DuoStoreConfig::from_params(const std::string& name,
                                           const std::map<std::string, std::string>& params) {
    DuoStoreConfig c;
    c.name = name;
    auto get = [&](const char* k) -> const std::string* {
        auto it = params.find(k);
        return it == params.end() ? nullptr : &it->second;
    };
    auto* root = get("root");
    if (!root || root->empty())
        throw std::runtime_error("duostore backend '" + name + "' needs root");
    c.root = *root;
    if (auto* v = get("meta_path"); v && !v->empty()) c.meta_path = *v;
    else c.meta_path = c.root / "meta";
    if (auto* v = get("chunk_size")) c.chunk_size = parse_size(*v);
    if (auto* v = get("pack_threshold")) c.pack_threshold = parse_size(*v);
    if (auto* v = get("pack_max_size")) c.pack_max_size = parse_size(*v);
    if (auto* v = get("pack_writers")) c.pack_writers = parse_int_param(name, "pack_writers", *v);
    if (auto* v = get("pack_gc_ratio"))
        c.pack_gc_ratio = parse_double_param(name, "pack_gc_ratio", *v);
    if (auto* v = get("gc_interval")) c.gc_interval_sec = parse_duration_sec(*v);
    if (auto* v = get("gc_grace")) c.gc_grace_sec = parse_duration_sec(*v);
    if (auto* v = get("orphan_scan_interval"))
        c.orphan_scan_interval_sec = parse_duration_sec(*v);
    if (auto* v = get("mpu_ttl")) c.mpu_ttl_sec = parse_duration_sec(*v);
    if (auto* v = get("meta_sync")) c.meta_sync = parse_bool_param(name, "meta_sync", *v);
    if (auto* v = get("verify_chunk_crc"))
        c.verify_chunk_crc = parse_bool_param(name, "verify_chunk_crc", *v);
    if (auto* v = get("rocksdb_block_cache")) c.rocksdb_block_cache = parse_size(*v);

    // meta 引擎选择（docs/duostore-redis-meta.md §8 / docs/duostore-sqlite-meta.md §8）
    if (auto* v = get("meta")) {
        if (*v == "rocksdb") {
            c.meta_kind = DuoMetaKind::kRocksDb;
        } else if (*v == "redis") {
#ifdef LIGHTS3_DUOSTORE_REDIS_META
            c.meta_kind = DuoMetaKind::kRedis;
#else
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta=redis not compiled in "
                                     "(build with -DLIGHTS3_DUOSTORE_REDIS_META=ON)");
#endif
        } else if (*v == "sqlite") {
#ifdef LIGHTS3_DUOSTORE_SQLITE_META
            c.meta_kind = DuoMetaKind::kSqlite;
#else
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta=sqlite not compiled in "
                                     "(build with -DLIGHTS3_DUOSTORE_SQLITE_META=ON)");
#endif
        } else {
            bad_param(name, "meta", *v);
        }
    }
    if (auto* v = get("redis_uri")) c.redis_uri = *v;
    if (auto* v = get("redis_prefix")) c.redis_prefix = *v;
    if (auto* v = get("redis_timeout")) c.redis_timeout_sec = parse_duration_sec(*v);
    if (auto* v = get("redis_pool_size"))
        c.redis_pool_size = parse_int_param(name, "redis_pool_size", *v);
    if (c.meta_kind == DuoMetaKind::kRedis) {
        if (c.redis_uri.empty())
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta=redis needs redis_uri");
        if (c.redis_pool_size < 1 || c.redis_pool_size > 256)
            throw std::runtime_error("duostore backend '" + name +
                                     "': redis_pool_size must be in [1,256]");
        if (c.redis_timeout_sec < 1)
            throw std::runtime_error("duostore backend '" + name +
                                     "': redis_timeout must be >= 1s");
    }

    // sqlite meta（docs/duostore-sqlite-meta.md §8）：meta_sync 沿用（本地引擎，
    // 持久化档位归本进程管，映射 synchronous FULL/NORMAL）
    if (auto* v = get("sqlite_path"); v && !v->empty()) c.sqlite_path = *v;
    else c.sqlite_path = c.root / "meta.sqlite3";
    if (auto* v = get("sqlite_cache")) c.sqlite_cache = parse_size(*v);
    if (c.meta_kind == DuoMetaKind::kSqlite) {
        // 进程级总预算，按连接摊分后仍需有意义（docs/duostore-sqlite-meta.md §8）
        if (c.sqlite_cache < (1ull << 20))
            throw std::runtime_error("duostore backend '" + name +
                                     "': sqlite_cache must be >= 1MiB");
    }

    // meta 引擎专属键：出现但不属于选中引擎 → WARN（键→归属表；新引擎加行即可，
    // 免去每个分支各自维护对方键清单的 O(kinds²) 漏网）。meta_sync 为 rocksdb 与
    // sqlite 共有、仅 redis 下忽略（持久化语义改由 Redis 侧 AOF 承担），单列处理
    {
        static constexpr struct {
            const char* key;
            DuoMetaKind kind;
        } kMetaOwnedKeys[] = {
            {"meta_path", DuoMetaKind::kRocksDb},
            {"rocksdb_block_cache", DuoMetaKind::kRocksDb},
            {"redis_uri", DuoMetaKind::kRedis},
            {"redis_prefix", DuoMetaKind::kRedis},
            {"redis_timeout", DuoMetaKind::kRedis},
            {"redis_pool_size", DuoMetaKind::kRedis},
            {"sqlite_path", DuoMetaKind::kSqlite},
            {"sqlite_cache", DuoMetaKind::kSqlite},
        };
        const char* kind_name = c.meta_kind == DuoMetaKind::kRocksDb  ? "rocksdb"
                                : c.meta_kind == DuoMetaKind::kRedis ? "redis"
                                                                     : "sqlite";
        for (const auto& mk : kMetaOwnedKeys)
            if (mk.kind != c.meta_kind && params.count(mk.key))
                LOG_WARN("duostore backend '{}': {} ignored with meta={}", name, mk.key,
                         kind_name);
        if (c.meta_kind == DuoMetaKind::kRedis && params.count("meta_sync"))
            LOG_WARN("duostore backend '{}': meta_sync ignored with meta=redis", name);
    }

    if (c.chunk_size < 4096)
        throw std::runtime_error("duostore backend '" + name + "': chunk_size must be >= 4KiB");
    if (c.pack_threshold > c.pack_max_size)
        throw std::runtime_error("duostore backend '" + name +
                                 "': pack_threshold must not exceed pack_max_size");
    if (c.pack_writers < 1 || c.pack_writers > 64)
        throw std::runtime_error("duostore backend '" + name +
                                 "': pack_writers must be in [1,64]");
    if (!(c.pack_gc_ratio > 0.0 && c.pack_gc_ratio <= 1.0))
        throw std::runtime_error("duostore backend '" + name +
                                 "': pack_gc_ratio must be in (0,1]");
    return c;
}

// ---------- 构造 / 关闭 ----------

DuoStoreBackend::DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool)
    : cfg_(std::move(cfg)), pool_(std::move(pool)) {
    std::filesystem::create_directories(cfg_.root);
#ifdef LIGHTS3_DUOSTORE_REDIS_META
    if (cfg_.meta_kind == DuoMetaKind::kRedis)
        meta_ = std::make_unique<RedisMetaStore>(RedisMetaOptions{
            cfg_.redis_uri, cfg_.redis_prefix, cfg_.redis_timeout_sec * 1000,
            cfg_.redis_pool_size});
#endif
#ifdef LIGHTS3_DUOSTORE_SQLITE_META
    if (cfg_.meta_kind == DuoMetaKind::kSqlite)
        meta_ = std::make_unique<SqliteMetaStore>(SqliteMetaOptions{
            cfg_.sqlite_path.string(), cfg_.meta_sync, cfg_.sqlite_cache});
#endif
    if (!meta_)
        meta_ = std::make_unique<RocksMetaStore>(RocksMetaOptions{
            cfg_.meta_path.string(), cfg_.meta_sync, cfg_.rocksdb_block_cache});
    IMetaStore* meta = meta_.get();  // 分配回调不延长 meta 生命周期：本类持有两者，先关 data
    data_ = std::make_unique<FsDataStore>(
        FsDataOptions{cfg_.root, cfg_.chunk_size, cfg_.verify_chunk_crc}, pool_,
        [meta](Extent::Kind kind) { return meta->alloc_file_id(kind); });
}

DuoStoreBackend::DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                                 std::unique_ptr<IMetaStore> meta,
                                 std::unique_ptr<IDataStore> data)
    : cfg_(std::move(cfg)), pool_(std::move(pool)), meta_(std::move(meta)),
      data_(std::move(data)) {}

DuoStoreBackend::~DuoStoreBackend() {
    // 正路是先 sync_wait(close())；兜底只同步关 meta（RocksDB 干净落盘）
    if (!closed_ && meta_) meta_->close();
}

Task<void> DuoStoreBackend::close() {
    if (closed_) co_return;
    closed_ = true;
    // P3 起：撤销 GC 定时器、等待在途 GC 协程（§9 生命周期）
    co_await data_->close();  // 封存 active pack（P2）
    co_await pool_->schedule();
    meta_->close();
}

void DuoStoreBackend::require_bucket(std::string_view bucket) {
    if (!meta_->bucket_exists(bucket))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(bucket));
}

ObjectRec DuoStoreBackend::require_object(std::string_view bucket, std::string_view key) {
    auto rec = meta_->get_object(bucket, key);
    if (!rec) {
        require_bucket(bucket);  // 区分 NoSuchBucket / NoSuchKey
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }
    return std::move(*rec);
}

// ---------- bucket ----------

Task<void> DuoStoreBackend::create_bucket(std::string_view bucket) {
    validate_bucket_name(bucket);
    co_await pool_->schedule();
    meta_->create_bucket(bucket);
}

Task<void> DuoStoreBackend::delete_bucket(std::string_view bucket) {
    co_await pool_->schedule();
    meta_->delete_bucket(bucket);
}

Task<bool> DuoStoreBackend::bucket_exists(std::string_view bucket) {
    co_await pool_->schedule();
    co_return meta_->bucket_exists(bucket);
}

Task<std::vector<BucketInfo>> DuoStoreBackend::list_buckets() {
    co_await pool_->schedule();
    co_return meta_->list_buckets();
}

// ---------- object ----------

namespace {

struct Pumped {
    DataRef ref;
    std::string md5;
};

// PUT/upload_part 共用泵送循环（§6.1 ③④）：流式写数据面，边写边算 MD5
Task<Pumped> pump_body(IDataStore& data, http::BodyReader& body) {
    auto writer = co_await data.open_writer({body.length()});
    util::HashStream md5(util::HashStream::Algo::Md5);
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        co_await writer->write(std::span<const std::byte>(buf, n));
    }
    Pumped out;
    out.ref = co_await writer->finish();
    out.md5 = md5.final_hex();
    co_return out;
}

// meta 提交失败时兜底删除已产出数据（§6.1 ⑤）；co_await 不能出现在 catch 块内，
// 清理经 exception_ptr 移出 handler。兜底失败也无害——落入孤儿扫描
template <class Commit>
Task<void> commit_or_discard(IDataStore& data, const DataRef& ref, Commit commit) {
    std::exception_ptr err;
    try {
        commit();
    } catch (...) {
        err = std::current_exception();
    }
    if (err) {
        try {
            co_await data.remove(ref.extents);
        } catch (...) {
        }
        std::rethrow_exception(err);
    }
}

}  // namespace

Task<PutResult> DuoStoreBackend::put_object(std::string_view bucket, std::string_view key,
                                            ObjectMeta meta, http::BodyReader& body) {
    validate_bucket_name(bucket);
    validate_object_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);  // 预检；正式检查在提交事务内复查（§6.1 ②）

    auto pumped = co_await pump_body(*data_, body);
    ObjectRec rec;
    rec.meta = std::move(meta);
    rec.meta.key = std::string(key);
    rec.meta.size = pumped.ref.total();
    rec.meta.etag = pumped.md5;
    rec.meta.last_modified = std::chrono::system_clock::now();
    rec.data = pumped.ref;
    // 提交点；旧 DataRef 同批入 gcq
    co_await commit_or_discard(*data_, pumped.ref,
                               [&] { meta_->put_object(bucket, key, std::move(rec)); });
    co_return PutResult{pumped.md5};
}

Task<ObjectStream> DuoStoreBackend::get_object(std::string_view bucket, std::string_view key,
                                               std::optional<ByteRange> range) {
    validate_object_key(key);
    co_await pool_->schedule();
    auto rec = require_object(bucket, key);

    ObjectStream out;
    out.meta = rec.meta;
    uint64_t first = 0, last = 0, len = rec.meta.size;
    if (range) {
        std::tie(first, last) = resolve_range(*range, rec.meta.size);
        out.range = ByteRange{first, last};
        len = last - first + 1;
    } else if (rec.meta.size > 0) {
        last = rec.meta.size - 1;
    }
    if (len == 0)
        out.body = std::make_unique<http::StringBodyReader>("");
    else
        out.body = co_await data_->open_reader(std::move(rec.data), first, last);
    co_return out;
}

Task<ObjectMeta> DuoStoreBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    co_await pool_->schedule();
    co_return require_object(bucket, key).meta;
}

Task<void> DuoStoreBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    co_await pool_->schedule();
    meta_->delete_object(bucket, key);  // 幂等（不存在返回 false）；物理回收由 GC 异步变现（§6.2）
}

Task<ListResult> DuoStoreBackend::list_objects(std::string_view bucket,
                                               const ListOptions& opt) {
    co_await pool_->schedule();
    co_return meta_->list_objects(bucket, opt);
}

// ---------- multipart（§8）----------

Task<std::string> DuoStoreBackend::create_multipart(std::string_view bucket,
                                                    std::string_view key, ObjectMeta meta) {
    validate_bucket_name(bucket);
    validate_object_key(key);
    co_await pool_->schedule();
    co_return meta_->create_upload(bucket, key, std::move(meta));
}

Task<PutResult> DuoStoreBackend::upload_part(std::string_view bucket, std::string_view key,
                                             std::string_view upload_id, int part_no,
                                             http::BodyReader& body) {
    validate_part_number(part_no);
    validate_object_key(key);  // key 进 '\0' 分隔编码（§4.1），multipart 入口同样校验
    co_await pool_->schedule();
    meta_->require_upload(bucket, key, upload_id);  // 前置校验；提交时复查

    auto pumped = co_await pump_body(*data_, body);
    PartRec p;
    p.part_no = part_no;
    p.size = pumped.ref.total();
    p.etag = pumped.md5;
    p.modified_ms = codec::to_unix_ms(std::chrono::system_clock::now());
    p.data = pumped.ref;
    // 读 body 期间上传可能已被 abort → 提交失败即兜底删数据
    co_await commit_or_discard(*data_, pumped.ref, [&] {
        meta_->put_part(bucket, key, upload_id, std::move(p));
    });
    co_return PutResult{pumped.md5};
}

Task<PutResult> DuoStoreBackend::complete_multipart(std::string_view bucket,
                                                    std::string_view key,
                                                    std::string_view upload_id,
                                                    std::span<const PartInfo> parts) {
    validate_part_order(parts);
    validate_object_key(key);
    co_await pool_->schedule();
    // 纯元数据事务，零数据搬运：O(#parts) vs localfs 串接的 O(总字节)（§8）
    co_return PutResult{meta_->complete_upload(bucket, key, upload_id, parts)};
}

Task<void> DuoStoreBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                            std::string_view upload_id) {
    validate_object_key(key);
    co_await pool_->schedule();
    meta_->abort_upload(bucket, key, upload_id);
}

Task<std::vector<PartMeta>> DuoStoreBackend::list_parts(std::string_view bucket,
                                                        std::string_view key,
                                                        std::string_view upload_id) {
    validate_object_key(key);
    co_await pool_->schedule();
    std::vector<PartMeta> out;
    for (const auto& p : meta_->list_parts(bucket, key, upload_id))
        out.push_back({p.part_no, p.size, p.etag, codec::from_unix_ms(p.modified_ms)});
    co_return out;
}

Task<std::vector<UploadInfo>> DuoStoreBackend::list_multipart_uploads(
    std::string_view bucket) {
    co_await pool_->schedule();
    co_return meta_->list_uploads(bucket);
}

}  // namespace lights3::storage
