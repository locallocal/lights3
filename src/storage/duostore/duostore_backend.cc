#include "storage/duostore/duostore_backend.h"

#include <chrono>
#include <stdexcept>
#include <unordered_set>

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

#ifdef LIGHTS3_DUOSTORE_RADOS_DATA
#include "storage/duostore/rados_data_store.h"
#endif

#ifdef LIGHTS3_DUOSTORE_TIKV_META
#include "storage/duostore/tikv_meta_store.h"
#endif

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using namespace duostore;

// ---------- PinTable（§7）----------

namespace duostore {

std::vector<uint64_t> PinTable::pin(std::span<const Extent> extents) {
    std::vector<uint64_t> ids;
    ids.reserve(extents.size());
    for (const auto& e : extents) {
        auto& s = shard_of(e.file_id);
        std::lock_guard lk(s.m);
        ++s.refs[e.file_id];
        ids.push_back(e.file_id);
    }
    return ids;
}

void PinTable::unpin(const std::vector<uint64_t>& ids) {
    for (uint64_t id : ids) unpin_id(id);
}

void PinTable::pin_id(uint64_t file_id) {
    auto& s = shard_of(file_id);
    std::lock_guard lk(s.m);
    ++s.refs[file_id];
}

void PinTable::unpin_id(uint64_t file_id) {
    auto& s = shard_of(file_id);
    std::lock_guard lk(s.m);
    auto it = s.refs.find(file_id);
    if (it == s.refs.end()) return;  // 不应发生；防御性容忍
    if (--it->second <= 0) s.refs.erase(it);
}

bool PinTable::any_pinned(std::span<const Extent> extents) {
    for (const auto& e : extents)
        if (pinned(e.file_id)) return true;
    return false;
}

bool PinTable::pinned(uint64_t file_id) {
    auto& s = shard_of(file_id);
    std::lock_guard lk(s.m);
    return s.refs.count(file_id) != 0;
}

// ---------- 压实迁移（P4 §9.2 步骤 2-3）----------

namespace {

// owner 按 '\0' 切段（各段来源 bucket/key/upload_id 均经校验不含 NUL，无歧义）
std::vector<std::string_view> split_owner(std::string_view owner) {
    std::vector<std::string_view> parts;
    size_t pos = 0;
    while (pos <= owner.size()) {
        size_t nul = owner.find('\0', pos);
        if (nul == std::string_view::npos) {
            parts.push_back(owner.substr(pos));
            break;
        }
        parts.push_back(owner.substr(pos, nul - pos));
        pos = nul + 1;
    }
    return parts;
}

}  // namespace

Task<bool> migrate_pack_record(IMetaStore& meta, IDataStore& data, PinTable* pins,
                               std::string_view owner, const Extent& from,
                               std::span<const std::byte> payload) {
    // owner 只是反查提示（§9.2）：对象 record 内嵌 "b\0k"；mpu record 内嵌
    // "mpu\0b\0k\0id\0no"——complete 后分片归属 b/k 的对象（refs 转移不改 record），
    // 提示仍然可用。进行中 mpu 的分片（对象上查不到 from）与 P4 之前的旧格式
    // "mpu\0id\0no"（无 b/k 可查）都保守返回 false：live 账不动，pack 本轮不删，
    // 分别待 complete/abort 后自然解锁与永久搁置（重写盘上旧 record 非目标）
    auto parts = split_owner(owner);
    std::string_view b, k;
    if (parts.size() == 2) {
        b = parts[0];
        k = parts[1];
    } else if (parts.size() == 5 && parts[0] == "mpu") {
        b = parts[1];
        k = parts[2];
    } else {
        co_return false;
    }
    if (b.empty() || k.empty()) co_return false;

    auto rec = meta.get_object(b, k);
    if (!rec) co_return false;
    size_t idx = rec->data.extents.size();
    for (size_t i = 0; i < rec->data.extents.size(); ++i)
        if (rec->data.extents[i] == from) {
            idx = i;
            break;
        }
    if (idx == rec->data.extents.size()) co_return false;  // 已覆盖/删除 = 死区

    // 存活：payload 追加回 active pack（open_writer 按当前配置分流——阈值缩小或
    // pack 关停时落 chunk 同样正确）；新 record 的 owner 统一写对象形态
    auto writer = co_await data.open_writer(
        {payload.size(), std::string(b) + '\0' + std::string(k)});
    co_await writer->write(payload);
    DataRef nref = co_await writer->finish();

    DataRef to;
    to.extents.reserve(rec->data.extents.size() - 1 + nref.extents.size());
    to.extents.assign(rec->data.extents.begin(), rec->data.extents.begin() + idx);
    to.extents.insert(to.extents.end(), nref.extents.begin(), nref.extents.end());
    to.extents.insert(to.extents.end(), rec->data.extents.begin() + idx + 1,
                      rec->data.extents.end());

    bool ok = meta.swap_extents(b, k, rec->version, rec->data, to);
    if (!ok) {
        // 期间被覆盖/删除：追加的新 record 成死区（无账，随未来压实回收）；chunk 类
        // 残留则显式清（refs 从未建立，删之即净）
        try {
            co_await data.remove(nref.extents);
        } catch (...) {
        }
    }
    // 写侧 pin 对称解除（追加走 chunk 路径时 ChunkPinHooks 已 pin；swap 成功即
    // refs 在账，失败则文件已删——两况都该解）
    if (pins)
        for (const auto& e : nref.extents)
            if (e.kind != Extent::Kind::kPack) pins->unpin_id(e.file_id);
    co_return ok;
}

}  // namespace duostore

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
    if (auto* v = get("rocksdb_write_buffer")) c.rocksdb_write_buffer = parse_size(*v);
    if (auto* v = get("rocksdb_max_write_buffers"))
        c.rocksdb_max_write_buffers = parse_int_param(name, "rocksdb_max_write_buffers", *v);
    if (auto* v = get("rocksdb_max_background_jobs"))
        c.rocksdb_max_background_jobs =
            parse_int_param(name, "rocksdb_max_background_jobs", *v);

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
        } else if (*v == "tikv") {
#ifdef LIGHTS3_DUOSTORE_TIKV_META
            c.meta_kind = DuoMetaKind::kTikv;
#else
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta=tikv not compiled in "
                                     "(build with -DLIGHTS3_DUOSTORE_TIKV_META=ON)");
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
    if (auto* v = get("redis_wait_replicas"))
        c.redis_wait_replicas = parse_int_param(name, "redis_wait_replicas", *v);
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
        if (c.redis_wait_replicas < 0 || c.redis_wait_replicas > 256)
            throw std::runtime_error("duostore backend '" + name +
                                     "': redis_wait_replicas must be in [0,256]");
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

    // tikv meta（docs/duostore-tikv-meta.md §9）：pd_endpoints 逗号分隔；持久化 =
    // raft 多数派，meta_sync 无意义（归属表下方单列 WARN）
    if (auto* v = get("pd_endpoints")) {
        std::string_view rest = *v;
        while (!rest.empty()) {
            auto comma = rest.find(',');
            std::string_view ep = rest.substr(0, comma);
            // 修剪空白（YAML 里 "a:2379, b:2379" 的书写习惯）
            while (!ep.empty() && (ep.front() == ' ' || ep.front() == '\t')) ep.remove_prefix(1);
            while (!ep.empty() && (ep.back() == ' ' || ep.back() == '\t')) ep.remove_suffix(1);
            if (!ep.empty()) c.pd_endpoints.emplace_back(ep);
            if (comma == std::string_view::npos) break;
            rest.remove_prefix(comma + 1);
        }
    }
    if (auto* v = get("tikv_prefix")) c.tikv_prefix = *v;
    if (auto* v = get("tikv_ca")) c.tikv_ca = *v;
    if (auto* v = get("tikv_cert")) c.tikv_cert = *v;
    if (auto* v = get("tikv_key")) c.tikv_key = *v;
    if (auto* v = get("tikv_backoff_ms"))
        c.tikv_backoff_ms = parse_int_param(name, "tikv_backoff_ms", *v);
    if (auto* v = get("tikv_gc_interval")) c.tikv_gc_interval_sec = parse_duration_sec(*v);
    if (auto* v = get("tikv_gc_retention")) c.tikv_gc_retention_sec = parse_duration_sec(*v);
    if (c.meta_kind == DuoMetaKind::kTikv) {
        if (c.pd_endpoints.empty())
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta=tikv needs pd_endpoints");
        // mTLS 三件套要么全给要么全空（ClusterConfig 以 ca 非空为启用判据）
        int given = int(!c.tikv_ca.empty()) + int(!c.tikv_cert.empty()) + int(!c.tikv_key.empty());
        if (given != 0 && given != 3)
            throw std::runtime_error("duostore backend '" + name +
                                     "': tikv_ca/tikv_cert/tikv_key must be set together");
        if (c.tikv_backoff_ms < 0)
            throw std::runtime_error("duostore backend '" + name +
                                     "': tikv_backoff_ms must be >= 0 (0 = library default)");
        // interval 0 = 关闭推进（共 TiDB 集群时由 TiDB 治理，§7.3 部署矩阵）；
        // 开启时 retention 必须为正——safepoint 推到 now 会砍掉在途快照
        if (c.tikv_gc_interval_sec < 0 || c.tikv_gc_retention_sec <= 0)
            throw std::runtime_error(
                "duostore backend '" + name +
                "': tikv_gc_interval must be >= 0 and tikv_gc_retention > 0");
    }

    // data 引擎选择（docs/duostore-rados-data.md §10，对偶 meta 分支）
    if (auto* v = get("data")) {
        if (*v == "fs") {
            c.data_kind = DuoDataKind::kFs;
        } else if (*v == "rados") {
#ifdef LIGHTS3_DUOSTORE_RADOS_DATA
            c.data_kind = DuoDataKind::kRados;
#else
            throw std::runtime_error("duostore backend '" + name +
                                     "': data=rados not compiled in "
                                     "(build with -DLIGHTS3_DUOSTORE_RADOS_DATA=ON)");
#endif
        } else {
            bad_param(name, "data", *v);
        }
    }
    if (auto* v = get("rados_conf"); v && !v->empty()) c.rados_conf = *v;
    if (auto* v = get("rados_client"); v && !v->empty()) c.rados_client = *v;
    if (auto* v = get("rados_pool")) c.rados_pool = *v;
    if (auto* v = get("rados_namespace")) c.rados_namespace = *v;
    if (auto* v = get("rados_chunk_size")) c.rados_chunk_size = parse_size(*v);
    if (auto* v = get("rados_buffer_total")) c.rados_buffer_total = parse_size(*v);
    if (auto* v = get("rados_connect_timeout"))
        c.rados_connect_timeout_sec = parse_duration_sec(*v);
    if (auto* v = get("rados_op_timeout")) c.rados_op_timeout_sec = parse_duration_sec(*v);
    if (c.data_kind == DuoDataKind::kRados) {
        if (c.rados_pool.empty())
            throw std::runtime_error("duostore backend '" + name +
                                     "': data=rados needs rados_pool");
        // 上限对齐 osd_max_object_size 默认 128MiB（docs/duostore-rados-data.md §3.4）
        if (c.rados_chunk_size < 4096 || c.rados_chunk_size > (128ull << 20))
            throw std::runtime_error("duostore backend '" + name +
                                     "': rados_chunk_size must be in [4KiB,128MiB]");
        if (c.rados_buffer_total < c.rados_chunk_size)
            throw std::runtime_error("duostore backend '" + name +
                                     "': rados_buffer_total must be >= rados_chunk_size");
        if (c.rados_connect_timeout_sec < 1)
            throw std::runtime_error("duostore backend '" + name +
                                     "': rados_connect_timeout must be >= 1s");
        if (c.rados_op_timeout_sec < 0)
            throw std::runtime_error("duostore backend '" + name +
                                     "': rados_op_timeout must be >= 0");
    }

    // data 引擎专属键：出现但不属于选中引擎 → WARN（同下 meta 键归属表的机制；
    // docs/duostore-rados-data.md §10——data=rados 下 chunk_size 被 rados_chunk_size
    // 取代、pack_* 全部忽略；verify_chunk_crc 为两引擎共有）
    {
        static constexpr struct {
            const char* key;
            DuoDataKind kind;
        } kDataOwnedKeys[] = {
            {"chunk_size", DuoDataKind::kFs},
            {"pack_threshold", DuoDataKind::kFs},
            {"pack_max_size", DuoDataKind::kFs},
            {"pack_writers", DuoDataKind::kFs},
            {"pack_gc_ratio", DuoDataKind::kFs},
            {"rados_conf", DuoDataKind::kRados},
            {"rados_client", DuoDataKind::kRados},
            {"rados_pool", DuoDataKind::kRados},
            {"rados_namespace", DuoDataKind::kRados},
            {"rados_chunk_size", DuoDataKind::kRados},
            {"rados_buffer_total", DuoDataKind::kRados},
            {"rados_connect_timeout", DuoDataKind::kRados},
            {"rados_op_timeout", DuoDataKind::kRados},
        };
        const char* kind_name = c.data_kind == DuoDataKind::kFs ? "fs" : "rados";
        for (const auto& dk : kDataOwnedKeys)
            if (dk.kind != c.data_kind && params.count(dk.key))
                LOG_WARN("duostore backend '{}': {} ignored with data={}", name, dk.key,
                         kind_name);
    }

    // meta 引擎专属键：出现但不属于选中引擎 → WARN（键→归属表；新引擎加行即可，
    // 免去每个分支各自维护对方键清单的 O(kinds²) 漏网）。meta_sync 为 rocksdb 与
    // sqlite 共有、redis / tikv 下忽略（持久化语义分别归 Redis AOF 与 raft 多数派
    // 承担），单列处理
    {
        static constexpr struct {
            const char* key;
            DuoMetaKind kind;
        } kMetaOwnedKeys[] = {
            {"meta_path", DuoMetaKind::kRocksDb},
            {"rocksdb_block_cache", DuoMetaKind::kRocksDb},
            {"rocksdb_write_buffer", DuoMetaKind::kRocksDb},
            {"rocksdb_max_write_buffers", DuoMetaKind::kRocksDb},
            {"rocksdb_max_background_jobs", DuoMetaKind::kRocksDb},
            {"redis_uri", DuoMetaKind::kRedis},
            {"redis_prefix", DuoMetaKind::kRedis},
            {"redis_timeout", DuoMetaKind::kRedis},
            {"redis_pool_size", DuoMetaKind::kRedis},
            {"redis_wait_replicas", DuoMetaKind::kRedis},
            {"sqlite_path", DuoMetaKind::kSqlite},
            {"sqlite_cache", DuoMetaKind::kSqlite},
            {"pd_endpoints", DuoMetaKind::kTikv},
            {"tikv_prefix", DuoMetaKind::kTikv},
            {"tikv_ca", DuoMetaKind::kTikv},
            {"tikv_cert", DuoMetaKind::kTikv},
            {"tikv_key", DuoMetaKind::kTikv},
            {"tikv_backoff_ms", DuoMetaKind::kTikv},
            {"tikv_gc_interval", DuoMetaKind::kTikv},
            {"tikv_gc_retention", DuoMetaKind::kTikv},
        };
        const char* kind_name = c.meta_kind == DuoMetaKind::kRocksDb   ? "rocksdb"
                                : c.meta_kind == DuoMetaKind::kRedis   ? "redis"
                                : c.meta_kind == DuoMetaKind::kSqlite  ? "sqlite"
                                                                       : "tikv";
        for (const auto& mk : kMetaOwnedKeys)
            if (mk.kind != c.meta_kind && params.count(mk.key))
                LOG_WARN("duostore backend '{}': {} ignored with meta={}", name, mk.key,
                         kind_name);
        // redis：持久化语义归 Redis 侧 AOF；tikv：提交即 raft 多数派，恒等效 sync
        if ((c.meta_kind == DuoMetaKind::kRedis || c.meta_kind == DuoMetaKind::kTikv) &&
            params.count("meta_sync"))
            LOG_WARN("duostore backend '{}': meta_sync ignored with meta={}", name, kind_name);
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
    if (c.rocksdb_max_write_buffers < 1)
        throw std::runtime_error("duostore backend '" + name +
                                 "': rocksdb_max_write_buffers must be >= 1");
    if (c.rocksdb_max_background_jobs < 1)
        throw std::runtime_error("duostore backend '" + name +
                                 "': rocksdb_max_background_jobs must be >= 1");
    return c;
}

// ---------- 构造 / 关闭 ----------

DuoStoreBackend::DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                                 MetricsScope metrics)
    : cfg_(std::move(cfg)), pool_(std::move(pool)) {
    init_metrics(metrics);
    std::filesystem::create_directories(cfg_.root);
#ifdef LIGHTS3_DUOSTORE_REDIS_META
    if (cfg_.meta_kind == DuoMetaKind::kRedis)
        meta_ = std::make_unique<RedisMetaStore>(RedisMetaOptions{
            cfg_.redis_uri, cfg_.redis_prefix, cfg_.redis_timeout_sec * 1000,
            cfg_.redis_pool_size, cfg_.redis_wait_replicas, metrics});
#endif
#ifdef LIGHTS3_DUOSTORE_SQLITE_META
    if (cfg_.meta_kind == DuoMetaKind::kSqlite) {
        SqliteMetaOptions so;
        so.path = cfg_.sqlite_path.string();
        so.sync = cfg_.meta_sync;
        so.cache_bytes = cfg_.sqlite_cache;
        so.metrics = metrics;
        meta_ = std::make_unique<SqliteMetaStore>(std::move(so));
    }
#endif
#ifdef LIGHTS3_DUOSTORE_TIKV_META
    if (cfg_.meta_kind == DuoMetaKind::kTikv) {
        TikvMetaOptions to;
        to.pd_endpoints = cfg_.pd_endpoints;
        to.prefix = cfg_.tikv_prefix;
        to.ca_path = cfg_.tikv_ca;
        to.cert_path = cfg_.tikv_cert;
        to.key_path = cfg_.tikv_key;
        to.backoff_budget_ms = cfg_.tikv_backoff_ms;
        to.gc_safepoint_interval_s = cfg_.tikv_gc_interval_sec;
        to.gc_retention_s = cfg_.tikv_gc_retention_sec;
        to.metrics = metrics;
        meta_ = std::make_unique<TikvMetaStore>(std::move(to));
    }
#endif
    if (!meta_)
        meta_ = std::make_unique<RocksMetaStore>(RocksMetaOptions{
            cfg_.meta_path.string(), cfg_.meta_sync, cfg_.rocksdb_block_cache,
            cfg_.rocksdb_write_buffer, cfg_.rocksdb_max_write_buffers,
            cfg_.rocksdb_max_background_jobs});
    IMetaStore* meta = meta_.get();  // 分配回调不延长 meta 生命周期：本类持有两者，先关 data
    auto alloc = [meta](Extent::Kind kind) { return meta->alloc_file_id(kind); };
    // 读路径 crc 失配上报（P5 corruption 指标）：只捕获计数器 shared_ptr——reader
    // 持 options 拷贝逃逸出 backend 生命周期后回调仍安全
    auto on_corruption = [c = m_read_corruption_] { c->inc(); };
#ifdef LIGHTS3_DUOSTORE_RADOS_DATA
    if (cfg_.data_kind == DuoDataKind::kRados)
        data_ = std::make_unique<RadosDataStore>(
            RadosDataOptions{cfg_.rados_conf, cfg_.rados_client, cfg_.rados_pool,
                             cfg_.rados_namespace, cfg_.rados_chunk_size,
                             cfg_.rados_buffer_total, cfg_.rados_connect_timeout_sec,
                             cfg_.rados_op_timeout_sec, cfg_.verify_chunk_crc,
                             on_corruption},
            pool_, alloc);
#endif
    if (!data_) {
        // 压实迁移与写侧 pin 钩子（P4）：迁移标准实现 + pin 表同源注入。回调不捕获
        // this（meta/pins 生命周期由本类持有与 shared_ptr 保证），测试注入组装同款
        auto pins = pins_;
        data_ = std::make_unique<FsDataStore>(
            FsDataOptions{cfg_.root, cfg_.chunk_size, cfg_.verify_chunk_crc,
                          cfg_.pack_threshold, cfg_.pack_max_size, cfg_.pack_writers,
                          on_corruption},
            pool_, alloc,
            [meta](uint64_t pack_id, uint64_t size) { meta->seal_pack(pack_id, size); },
            [meta, pins](IDataStore& ds, std::string_view owner, const Extent& from,
                         std::span<const std::byte> payload) {
                return migrate_pack_record(*meta, ds, pins.get(), owner, from, payload);
            },
            ChunkPinHooks{[pins](uint64_t id) { pins->pin_id(id); },
                          [pins](uint64_t id) { pins->unpin_id(id); }});
        write_pins_ = true;
    }
    abandon_stale_packs();
    schedule_gc();
    schedule_orphan_scan();
}

DuoStoreBackend::DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                                 std::unique_ptr<IMetaStore> meta,
                                 std::unique_ptr<IDataStore> data, MetricsScope metrics)
    : cfg_(std::move(cfg)), pool_(std::move(pool)), meta_(std::move(meta)),
      data_(std::move(data)) {
    init_metrics(metrics);
    abandon_stale_packs();
    schedule_gc();
    schedule_orphan_scan();
}

void DuoStoreBackend::init_metrics(const MetricsScope& metrics) {
    m_gc_runs_ = metrics.counter("lights3_duostore_gc_runs_total",
                                 "Completed GC rounds (manual hook + background worker)");
    m_gc_reclaims_ = metrics.counter("lights3_duostore_gc_reclaims_total",
                                     "Reclaim queue entries acked after physical removal");
    m_gc_files_removed_ = metrics.counter("lights3_duostore_gc_files_removed_total",
                                          "Chunk/rados extents physically removed by GC");
    m_gc_packs_removed_ = metrics.counter("lights3_duostore_gc_packs_removed_total",
                                          "Empty sealed packs removed by GC");
    m_gc_uploads_expired_ = metrics.counter(
        "lights3_duostore_gc_uploads_expired_total",
        "Multipart uploads aborted by GC after exceeding mpu_ttl");
    m_gc_packs_compacted_ = metrics.counter("lights3_duostore_gc_packs_compacted_total",
                                            "Low-liveness packs rewritten by GC compaction");
    m_gc_records_migrated_ = metrics.counter(
        "lights3_duostore_gc_records_migrated_total",
        "Live pack records migrated (extent swap) during GC compaction");
    m_gc_records_corrupt_ = metrics.counter(
        "lights3_duostore_pack_corrupt_records_total",
        "Corrupt pack records detected during GC compaction scans (skipped, kept on disk)");
    m_orphan_runs_ = metrics.counter("lights3_duostore_orphan_scans_total",
                                     "Completed orphan reconciliation scans");
    m_orphan_removed_ = metrics.counter("lights3_duostore_orphan_chunks_removed_total",
                                        "Unreferenced chunk files removed by orphan scans");
    m_orphan_refs_missing_ = metrics.gauge(
        "lights3_duostore_orphan_refs_missing",
        "Refs pointing at missing chunk files as of the last orphan scan (data loss signal)");
    m_read_corruption_ = metrics.counter(
        "lights3_duostore_read_corruption_total",
        "Chunk/pack crc mismatches detected on the GET read path (P5 corruption metric)");
}

// 重启弃用 active pack（§5.2）：数据面从不复用旧 active pack（新号段 + O_EXCL），
// 上代崩溃/析构遗留的 unsealed 账在此补封——否则它们永远进不了空 pack 整删与
// P4 压实的候选集。file_size 以 0 补（未知；压实顺扫时可再 stat），seal_pack
// 契约保证 0 不覆盖已知值
void DuoStoreBackend::abandon_stale_packs() {
    try {
        for (const auto& ps : meta_->pack_stats())
            if (!ps.sealed) meta_->seal_pack(ps.pack_id, 0);
    } catch (const std::exception& e) {
        // 补封失败不阻断启动（下次启动/GC 重试机会仍在），但要响亮留痕
        LOG_WARN("duostore '{}': sealing stale packs failed: {}", cfg_.name, e.what());
    }
}

DuoStoreBackend::~DuoStoreBackend() {
    // 正路是先 sync_wait(close())；兜底：撤定时器 + 等在途 GC（防回调用后释放的 this），
    // 再同步关 meta（RocksDB 干净落盘）
    if (closed_) return;
    shutdown_background();
    if (meta_) meta_->close();
}

Task<void> DuoStoreBackend::close() {
    if (closed_.exchange(true)) co_return;
    // 撤销 GC 定时器、等待在途 GC 协程结束（§9 生命周期）
    shutdown_background();
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

// PUT/upload_part 共用泵送循环（§6.1 ③④）：流式写数据面，边写边算 MD5。
// owner 进 pack record 头（§5.2）：对象 = "bucket\0key"、分片 = "mpu\0<id>\0<no>"
Task<Pumped> pump_body(IDataStore& data, http::BodyReader& body, std::string owner) {
    auto writer = co_await data.open_writer({body.length(), std::move(owner)});
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

// pin 持有的读包装（§7）：构造前 pin 已登记，析构解除。自包含——reader 随 HTTP
// 响应逃逸出 backend 生命周期，经 shared_ptr 持 pin 表
class PinnedReader final : public http::BodyReader {
public:
    PinnedReader(std::unique_ptr<http::BodyReader> inner, std::shared_ptr<PinTable> pins,
                 std::vector<uint64_t> ids)
        : inner_(std::move(inner)), pins_(std::move(pins)), ids_(std::move(ids)) {}
    ~PinnedReader() override { pins_->unpin(ids_); }

    Task<size_t> read(std::span<std::byte> buf) override { return inner_->read(buf); }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    std::shared_ptr<PinTable> pins_;
    std::vector<uint64_t> ids_;
};

// 写侧 pin 的对称解除（§9.3）：ChunkWriter 分配即 pin（孤儿扫描不回收在途写入），
// finish 后所有权移交调用方——本守卫在 meta 提交或兜底删除之后（协程帧退出时）
// 解除。仅 cfg 构造装配了 ChunkPinHooks 时生效（write_pins_）：注入构造无钩子，
// 盲解会误减并发读者的 pin
struct WritePinRelease {
    duostore::PinTable* pins = nullptr;
    std::vector<uint64_t> ids;
    WritePinRelease(bool active, duostore::PinTable* p, const DataRef& ref) {
        if (!active) return;
        pins = p;
        for (const auto& e : ref.extents)
            if (e.kind != Extent::Kind::kPack) ids.push_back(e.file_id);
    }
    WritePinRelease(const WritePinRelease&) = delete;
    ~WritePinRelease() {
        if (pins)
            for (uint64_t id : ids) pins->unpin_id(id);
    }
};

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

    auto pumped = co_await pump_body(*data_, body, codec::object_key(bucket, key));
    WritePinRelease wp(write_pins_, pins_.get(), pumped.ref);
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
    if (len == 0) {
        out.body = std::make_unique<http::StringBodyReader>("");
    } else {
        // 先 pin 后开 reader（§7）：meta 读出与 pin 之间的微窗口由 gc_grace 兜底。
        // 只 pin [first,last] 命中的 extent——Range GET 不为整对象的 manifest 买单。
        // open_reader 失败须解 pin（co_await 不能进 catch，经 exception_ptr 移出）
        std::vector<Extent> hit;
        uint64_t off = 0;
        for (const auto& e : rec.data.extents) {
            if (off > last) break;
            if (off + e.length > first) hit.push_back(e);
            off += e.length;
        }
        auto ids = pins_->pin(hit);
        std::unique_ptr<http::BodyReader> inner;
        std::exception_ptr err;
        try {
            inner = co_await data_->open_reader(std::move(rec.data), first, last);
        } catch (...) {
            err = std::current_exception();
        }
        if (err) {
            pins_->unpin(ids);
            std::rethrow_exception(err);
        }
        out.body = std::make_unique<PinnedReader>(std::move(inner), pins_, std::move(ids));
    }
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

    // mpu owner 带 b/k（P4 §9.2）：complete 后分片 record 的归属对象可凭此反查，
    // 压实不因 upload 消亡而失去提示（P4 前的旧格式 "mpu\0id\0no" 无从反查，遇之
    // 保守不迁）
    std::string owner = "mpu";
    owner += '\0';
    owner += bucket;
    owner += '\0';
    owner += key;
    owner += '\0';
    owner += upload_id;
    owner += '\0';
    owner += std::to_string(part_no);
    auto pumped = co_await pump_body(*data_, body, std::move(owner));
    WritePinRelease wp(write_pins_, pins_.get(), pumped.ref);
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

// ---------- GC 一期（§9/§9.1）----------

namespace {

constexpr size_t kGcBatch = 256;  // 单轮 peek 批量；批间 ack 后推进，防大积压单批爆内存

}  // namespace

Task<DuoGcStats> DuoStoreBackend::run_gc_once() {
    co_await pool_->schedule();
    // 登记为在途：close() 经 bg_.wait_idle() 等本轮结束后才拆 meta_/data_——
    // 手动钩子与后台 worker 同一套账，不存在"只查一次 closed_"的 TOCTOU 窗口
    BackgroundTaskGroup::Scope scope(bg_);
    DuoGcStats st;
    if (!scope.ok()) co_return st;                 // 正在关闭
    auto permit = co_await gc_sem_.acquire();      // 手动钩子 vs 后台 worker 互斥

    // 1) mpu_ttl 过期 multipart 清理（§8 末）：内部 abort，分片入 gcq 由下一步变现。
    // <=0 = 关闭（与 gc_interval 的 0 语义对齐——0 若解释为"立即过期"会把在途
    // multipart 全部静默 abort，是配置脚枪）
    const int64_t ttl_ms = int64_t(cfg_.mpu_ttl_sec) * 1000;
    if (ttl_ms > 0) {
        const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
        for (const auto& bk : meta_->list_buckets()) {
            for (const auto& u : meta_->list_uploads(bk.name)) {
                if (now - codec::to_unix_ms(u.initiated) < ttl_ms) continue;
                try {
                    meta_->abort_upload(bk.name, u.key, u.upload_id);
                    ++st.uploads_expired;
                } catch (const std::exception& e) {
                    // 与并发 complete/abort 竞争丢 NoSuchUpload 属正常；其余记 WARN 下轮重试
                    LOG_WARN("duostore '{}': gc abort expired upload {} failed: {}", cfg_.name,
                             u.upload_id, e.what());
                }
            }
        }
    }

    // 2) gcq 消费（§9.1）：逾 gc_grace 且无 pin 的项，先物理删、后销账——反序在删
    // 与销之间崩溃会产生永久孤儿的账外文件；正序崩溃只是 gcq 残留，重试 unlink 幂等。
    // 按 next_seq 断点续扫：被 grace/pin 跳过的队头项不重扫（不卡轮、不重复计数、
    // 无二次解码），扫到队尾即一轮结束
    const int64_t grace_ms = int64_t(cfg_.gc_grace_sec) * 1000;
    uint64_t next_seq = 0;
    for (;;) {
        auto batch = meta_->peek_reclaims(kGcBatch, next_seq);
        if (batch.empty()) break;
        next_seq = batch.back().first + 1;
        std::vector<uint64_t> acked;
        // 逐批取新鲜时间戳：上一步 abort 刚入队的项 enqueue_ms 晚于本函数入口时刻，
        // 用入口时刻判 grace 会把差值算成负数而误跳过（grace=0 应当立即可回收）
        const int64_t batch_now = codec::to_unix_ms(std::chrono::system_clock::now());
        for (const auto& [seq, rc] : batch) {
            if (batch_now - rc.enqueue_ms < grace_ms) {
                ++st.skipped_grace;
                continue;
            }
            if (pins_->any_pinned(rc.extents)) {
                ++st.skipped_pinned;
                continue;
            }
            try {
                // chunk/rados extent 物理 unlink；pack record 为死区随压实回收（data
                // 侧 remove 内部跳过），存活账已在业务事务扣减 → 直接销账
                co_await data_->remove(rc.extents);
            } catch (const std::exception& e) {
                LOG_WARN("duostore '{}': gc remove (seq {}) failed: {}", cfg_.name, seq,
                         e.what());
                continue;  // 不销账，gcq 残留下轮重试
            }
            for (const auto& e : rc.extents)
                if (e.kind != Extent::Kind::kPack) ++st.files_removed;
            acked.push_back(seq);
        }
        if (!acked.empty()) {
            meta_->ack_reclaims(acked);  // 批量销账（单事务/单批，接口注释的成本论证）
            st.reclaims_acked += acked.size();
        }
        if (batch.size() < kGcBatch) break;  // 队列见底
    }

    // 3) pack 压实（P4 §9.2）：sealed、live>0 且存活率低于 pack_gc_ratio（或崩溃遗留
    // file_size 未知）的 pack 顺扫迁移存活 record；live 账随 swap 归零后由第 4 步整
    // 删变现。上轮压实后 live_recs 无推进的 pack（进行中 mpu 分片 / 旧格式 owner /
    // 存活损坏 record）跳过重扫——账一有变化即自动重试
    std::vector<uint64_t> rewritten;
    const int64_t compact_now = codec::to_unix_ms(std::chrono::system_clock::now());
    for (const auto& ps : meta_->pack_stats()) {
        if (!ps.sealed || ps.live_recs <= 0) continue;
        if (ps.file_size > 0 &&
            double(ps.live_bytes) >= cfg_.pack_gc_ratio * double(ps.file_size))
            continue;
        if (auto it = compact_blocked_.find(ps.pack_id);
            it != compact_blocked_.end() && it->second.live_recs == ps.live_recs &&
            compact_now < it->second.retry_at_ms)
            continue;
        try {
            auto rw = co_await data_->rewrite_pack(ps.pack_id);
            ++st.packs_compacted;
            st.records_migrated += rw.migrated;
            st.records_corrupt += rw.corrupt;
            rewritten.push_back(ps.pack_id);
            // 崩溃遗留 seal(0) 的分母回填：下轮起存活率判定有效（§9.2）
            if (ps.file_size == 0 && rw.file_size > 0)
                meta_->seal_pack(ps.pack_id, rw.file_size);
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': gc rewrite pack {} failed: {}", cfg_.name, ps.pack_id,
                     e.what());
        }
    }

    // 4) 空 pack 整删（§9.1/§9.2 步骤 4）：sealed 且 live_recs==0，且空置已逾
    // gc_grace（延迟 unlink：服务压实/删除瞬间已读出旧 ref 未及 pin 的读者）且无
    // pin → 整文件 unlink → 销 packstat（顺序铁律同 gcq：先物理删、后销账）
    {
        const int64_t pack_now = codec::to_unix_ms(std::chrono::system_clock::now());
        auto stats = meta_->pack_stats();
        std::unordered_set<uint64_t> known;
        for (const auto& ps : stats) {
            known.insert(ps.pack_id);
            if (!ps.sealed || ps.live_recs != 0) {
                pack_empty_since_.erase(ps.pack_id);
                continue;
            }
            auto [it, first_seen] = pack_empty_since_.try_emplace(ps.pack_id, pack_now);
            (void)first_seen;
            if (pack_now - it->second < grace_ms || pins_->pinned(ps.pack_id)) continue;
            try {
                co_await data_->remove_pack(ps.pack_id);
                meta_->drop_pack_stat(ps.pack_id);
                pack_empty_since_.erase(ps.pack_id);
                ++st.packs_removed;
            } catch (const std::exception& e) {
                LOG_WARN("duostore '{}': gc remove pack {} failed: {}", cfg_.name, ps.pack_id,
                         e.what());
            }
        }
        // 压实阻塞记账：迁移后仍有 live = 本轮未能全迁 → 记当前账 + 冷却窗
        for (uint64_t pid : rewritten) {
            int64_t live = 0;
            for (const auto& ps : stats)
                if (ps.pack_id == pid) {
                    live = ps.live_recs;
                    break;
                }
            if (live > 0) compact_blocked_[pid] = {live, pack_now + grace_ms};
            else compact_blocked_.erase(pid);
        }
        // 剪枝已销账的 pack，两张簿记表不随历史无界增长
        std::erase_if(pack_empty_since_, [&](const auto& kv) { return !known.count(kv.first); });
        std::erase_if(compact_blocked_, [&](const auto& kv) { return !known.count(kv.first); });
    }

    // 完成轮才计数（关闭态的早退不计）；skip 类不设计数器——grace/pin 跳过项
    // 每轮重扫会重复累计，单调计数会虚高误导
    m_gc_runs_->inc();
    m_gc_reclaims_->inc(st.reclaims_acked);
    m_gc_files_removed_->inc(st.files_removed);
    m_gc_packs_removed_->inc(st.packs_removed);
    m_gc_uploads_expired_->inc(st.uploads_expired);
    m_gc_packs_compacted_->inc(st.packs_compacted);
    m_gc_records_migrated_->inc(st.records_migrated);
    m_gc_records_corrupt_->inc(st.records_corrupt);
    co_return st;
}

Task<duostore::DuoOrphanStats> DuoStoreBackend::run_orphan_scan_once() {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    DuoOrphanStats st;
    if (!scope.ok()) co_return st;                 // 正在关闭
    auto permit = co_await gc_sem_.acquire();      // 与 GC/后台 worker 互斥（反向对账前提）

    // refs 快照先行：反向对账"文件必先于 ref 提交存在"（§6 数据先行）要求 R 先于
    // 盘面枚举采集——R 内的每个 id 在枚举开始前文件已在盘，缺失即真丢失。持
    // gc_sem_ 排除了 gcq 的 unlink→销账窗口，业务路径不存在"有 refs 的文件被删"。
    // 注意：refs 不分 kChunk/kRados（共号段账），本扫描以当前 data 引擎的枚举为盘
    // 面真相——同一 meta 切换 data 引擎的部署形态不在孤儿扫描支持范围
    std::unordered_set<uint64_t> refs;
    meta_->scan_refs([&](uint64_t id) { refs.insert(id); });

    struct Seen {
        uint64_t id;
        int64_t mtime_ms;
    };
    std::vector<Seen> disk;
    std::unordered_set<uint64_t> on_disk;
    co_await data_->scan_chunks([&](uint64_t id, int64_t mtime_ms) {
        disk.push_back({id, mtime_ms});
        on_disk.insert(id);
    });
    st.chunks_scanned = disk.size();

    // 正向（§9.3）：无引用、mtime 逾 gc_grace、无 pin（写侧 pin 覆盖超长流式 PUT，
    // mtime 宽限对其不充分）→ unlink。快照后新提交的引用由 chunk_referenced 现点
    // 复查兜住（grace=0 的测试形态下快照必然过时）
    const int64_t grace_ms = int64_t(cfg_.gc_grace_sec) * 1000;
    const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
    for (const auto& c : disk) {
        if (refs.count(c.id)) continue;
        if (now - c.mtime_ms < grace_ms) {
            ++st.skipped_grace;
            continue;
        }
        if (pins_->pinned(c.id)) {
            ++st.skipped_pinned;
            continue;
        }
        if (meta_->chunk_referenced(c.id)) continue;  // 扫描间隙提交的新引用
        Extent e{Extent::Kind::kChunk, c.id, 0, 0, 0};
        try {
            co_await data_->remove(std::span<const Extent>(&e, 1));
            ++st.orphans_removed;
        } catch (const std::exception& ex) {
            LOG_WARN("duostore '{}': orphan unlink {:016x} failed: {}", cfg_.name, c.id,
                     ex.what());
        }
    }

    // 反向（§9.3）：refs 在而文件缺 = 数据丢失征兆 → 告警计数，绝不静默删 meta
    for (uint64_t id : refs) {
        if (on_disk.count(id)) continue;
        ++st.refs_missing;
        LOG_ERROR("duostore '{}': refs entry for chunk {:016x} but file missing "
                  "(data loss signal, keeping meta for manual inspection)", cfg_.name, id);
    }

    m_orphan_runs_->inc();
    m_orphan_removed_->inc(st.orphans_removed);
    m_orphan_refs_missing_->set(int64_t(st.refs_missing));
    co_return st;
}

Task<void> DuoStoreBackend::gc_tick() {
    // 完成后重臂（而非触发时）：GC 轮次绝不重叠/堆积——慢轮只是顺延下次触发
    std::exception_ptr err;
    try {
        co_await run_gc_once();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_gc();
    if (err) std::rethrow_exception(err);  // 交 BackgroundTaskGroup 记日志
}

void DuoStoreBackend::schedule_gc() {
    if (cfg_.gc_interval_sec <= 0) return;  // 0 = 关闭后台 GC（测试用手动钩子）
    bg_.if_open([&] {
        gc_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.gc_interval_sec),
                                               [this] { bg_.spawn(gc_tick()); });
    });
}

Task<void> DuoStoreBackend::orphan_tick() {
    // 完成后重臂（同 gc_tick）：扫描轮次绝不重叠/堆积
    std::exception_ptr err;
    try {
        co_await run_orphan_scan_once();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_orphan_scan();
    if (err) std::rethrow_exception(err);  // 交 BackgroundTaskGroup 记日志
}

void DuoStoreBackend::schedule_orphan_scan() {
    if (cfg_.orphan_scan_interval_sec <= 0) return;  // 0 = 关闭（测试用手动钩子）
    bg_.if_open([&] {
        orphan_timer_ = TimerQueue::instance().add(
            std::chrono::seconds(cfg_.orphan_scan_interval_sec),
            [this] { bg_.spawn(orphan_tick()); });
    });
}

void DuoStoreBackend::shutdown_background() {
    bg_.begin_close();
    // cancel 须在组锁外调用：TimerQueue::cancel 阻塞等在途回调，而回调内要拿组锁
    // （bg_.spawn）——begin_close 后两个 timer id 不再变更，读取无需加锁
    TimerQueue::instance().cancel(gc_timer_);
    TimerQueue::instance().cancel(orphan_timer_);
    // 阻塞等待在调用方线程上进行；在途 GC 在池线程收尾，不会互相占用（同 tiered close）
    bg_.wait_idle();
}

}  // namespace lights3::storage
