#include "storage/duostore/duostore_backend.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <stdexcept>
#include <unordered_set>

#include "core/config.h"
#include "core/log.h"
#include "core/util/crypto.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/rocks_meta_store.h"
#include "storage/listing.h"
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

std::vector<PinTable::Handle> PinTable::pin(std::span<const Extent> extents) {
    std::vector<Handle> handles;
    handles.reserve(extents.size());
    for (const auto& e : extents) {
        bool is_pack = e.kind == Extent::Kind::kPack;
        pin_key(is_pack, e.file_id);
        handles.push_back({is_pack, e.file_id});
    }
    return handles;
}

void PinTable::unpin(const std::vector<Handle>& handles) {
    for (const auto& h : handles) unpin_key(h.is_pack, h.file_id);
}

void PinTable::pin_id(uint64_t file_id) { pin_key(/*is_pack=*/false, file_id); }

void PinTable::unpin_id(uint64_t file_id) { unpin_key(/*is_pack=*/false, file_id); }

bool PinTable::any_pinned(std::span<const Extent> extents) {
    for (const auto& e : extents)
        if (pinned_key(e.kind == Extent::Kind::kPack, e.file_id)) return true;
    return false;
}

bool PinTable::pinned_chunk(uint64_t file_id) { return pinned_key(false, file_id); }

bool PinTable::pinned_pack(uint64_t pack_id) { return pinned_key(true, pack_id); }

void PinTable::pin_key(bool is_pack, uint64_t id) {
    auto& s = shard_of(id);
    std::lock_guard lk(s.m);
    ++(is_pack ? s.pack_refs : s.chunk_refs)[id];
}

void PinTable::unpin_key(bool is_pack, uint64_t id) {
    auto& s = shard_of(id);
    std::lock_guard lk(s.m);
    auto& refs = is_pack ? s.pack_refs : s.chunk_refs;
    auto it = refs.find(id);
    if (it == refs.end()) return;  // 不应发生；防御性容忍
    if (--it->second <= 0) refs.erase(it);
}

bool PinTable::pinned_key(bool is_pack, uint64_t id) {
    auto& s = shard_of(id);
    std::lock_guard lk(s.m);
    return (is_pack ? s.pack_refs : s.chunk_refs).count(id) != 0;
}

// ---------- 压实迁移（P4 §9.2 步骤 2-3）----------

Task<uint64_t> migrate_pack_records(IMetaStore& meta, IDataStore& data, PinTable* pins,
                                    std::vector<PackScanRecord> batch) {
    // owner 只是反查提示（§9.2）：对象 record 内嵌 "b\0k"；mpu record 内嵌
    // "mpu\0b\0k\0id\0no"——complete 后分片归属 b/k 的对象（refs 转移不改 record），
    // 提示仍然可用。进行中 mpu 的分片（对象上查不到 from）与 P4 之前的旧格式
    // "mpu\0id\0no"（无 b/k 可查）都保守不迁：live 账不动，pack 本轮不删，
    // 分别待 complete/abort 后自然解锁与永久搁置（重写盘上旧 record 非目标）

    // 1) 按 owner 聚合（gaps §2.13）：同一对象在同一 pack 的多条 record 一次
    // get_object + 一次换 ref，替代逐条整份 manifest 重写的 O(n²)
    struct Group {
        std::string b, k;
        std::vector<size_t> recs;  // batch 下标，扫描序
    };
    std::vector<Group> groups;
    std::map<std::string, size_t> by_owner;  // "b\0k" -> groups 下标
    for (size_t i = 0; i < batch.size(); ++i) {
        // 规范解析器（docs/gaps.md §6.1：三种 owner 形态收敛到 codec，离线取证
        // 工具复用同一入口）；kLegacyPart/kUnknown 无 b/k 可查，保守不迁
        auto po = codec::parse_pack_owner(batch[i].owner);
        if (po.kind != codec::PackOwner::Kind::kObject &&
            po.kind != codec::PackOwner::Kind::kPart)
            continue;
        std::string_view b = po.bucket, k = po.key;
        std::string gk = std::string(b) + '\0' + std::string(k);
        auto [it, fresh] = by_owner.try_emplace(std::move(gk), groups.size());
        if (fresh) groups.push_back({std::string(b), std::string(k), {}});
        groups[it->second].recs.push_back(i);
    }

    // 2) 逐组反查存活：from 与当前 manifest 逐位置配对（同 extent 不会出现两次——
    // manifest 由互异 record 拼成；防御起见仍标记已配对位置）
    struct LiveRec {
        size_t group;      // groups 下标
        size_t manifest_i; // 该组 manifest 中被替换的位置
        size_t rec_i;      // batch 下标
    };
    std::vector<LiveRec> live;
    std::vector<std::optional<ObjectRec>> group_rec(groups.size());
    for (size_t g = 0; g < groups.size(); ++g) {
        auto rec = meta.get_object(groups[g].b, groups[g].k);
        if (!rec) continue;  // 对象不存在 = 全部死区/进行中 mpu，保守不迁
        std::vector<bool> used(rec->data.extents.size(), false);
        for (size_t ri : groups[g].recs)
            for (size_t i = 0; i < rec->data.extents.size(); ++i)
                if (!used[i] && rec->data.extents[i] == batch[ri].from) {
                    used[i] = true;
                    live.push_back({g, i, ri});
                    break;
                }
        group_rec[g] = std::move(rec);
    }
    if (live.empty()) co_return 0;

    // 3) 存活 payload 批量追加（fs 实现单槽锁 + 单 fdatasync）；新 record 的 owner
    // 统一写对象形态。open_writer 同款分流语义——阈值缩小或 pack 关停时落 chunk
    // 同样正确
    std::vector<std::string> owners(live.size());
    std::vector<PackAppendItem> items(live.size());
    for (size_t i = 0; i < live.size(); ++i) {
        const auto& gr = groups[live[i].group];
        owners[i] = gr.b + '\0' + gr.k;
        items[i] = {owners[i], std::span<const std::byte>(batch[live[i].rec_i].payload)};
    }
    std::vector<DataRef> nrefs = co_await data.write_batch(items);

    // 4) 逐组拼新 manifest（多处替换一次完成）→ 批量换 ref
    std::vector<SwapReq> reqs;
    std::vector<size_t> req_group;
    reqs.reserve(groups.size());
    for (size_t g = 0; g < groups.size(); ++g) {
        if (!group_rec[g]) continue;
        std::map<size_t, size_t> repl;  // manifest 位置 -> live 下标
        for (size_t i = 0; i < live.size(); ++i)
            if (live[i].group == g) repl[live[i].manifest_i] = i;
        if (repl.empty()) continue;
        const auto& old = group_rec[g]->data;
        DataRef to;
        to.extents.reserve(old.extents.size());
        for (size_t i = 0; i < old.extents.size(); ++i) {
            auto it = repl.find(i);
            if (it == repl.end()) to.extents.push_back(old.extents[i]);
            else
                to.extents.insert(to.extents.end(), nrefs[it->second].extents.begin(),
                                  nrefs[it->second].extents.end());
        }
        reqs.push_back({groups[g].b, groups[g].k, group_rec[g]->version, old, std::move(to)});
        req_group.push_back(g);
    }
    std::vector<bool> ok = meta.swap_extents_batch(reqs);

    // 5) 失败组清理 + 写侧 pin 对称解除。期间被覆盖/删除：追加的新 record 成死区
    // （无账，随未来压实回收）；chunk 类残留显式清（refs 从未建立，删之即净）。
    // pin：swap 成功即 refs 在账，失败则文件已删——两况都解
    uint64_t migrated = 0;
    std::vector<bool> group_ok(groups.size(), false);
    for (size_t r = 0; r < reqs.size(); ++r) group_ok[req_group[r]] = ok[r];
    for (size_t i = 0; i < live.size(); ++i) {
        const DataRef& nref = nrefs[i];
        if (group_ok[live[i].group]) {
            ++migrated;
        } else {
            try {
                co_await data.remove(nref.extents);
            } catch (...) {
            }
        }
        if (pins)
            for (const auto& e : nref.extents)
                if (e.kind != Extent::Kind::kPack) pins->unpin_id(e.file_id);
    }
    co_return migrated;
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
    if (auto* v = get("pack_max_age")) c.pack_max_age_sec = parse_duration_sec(*v);
    if (auto* v = get("pack_gc_ratio"))
        c.pack_gc_ratio = parse_double_param(name, "pack_gc_ratio", *v);
    if (auto* v = get("gc_compact_max_packs"))
        c.gc_compact_max_packs = parse_int_param(name, "gc_compact_max_packs", *v);
    if (auto* v = get("gc_compact_max_bytes")) c.gc_compact_max_bytes = parse_size(*v);
    if (auto* v = get("gc_enabled")) c.gc_enabled = parse_bool_param(name, "gc_enabled", *v);
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
            {"pack_max_age", DuoDataKind::kFs},
            {"pack_gc_ratio", DuoDataKind::kFs},
            {"gc_compact_max_packs", DuoDataKind::kFs},
            {"gc_compact_max_bytes", DuoDataKind::kFs},
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
    if (c.pack_max_age_sec < 0)
        throw std::runtime_error("duostore backend '" + name +
                                 "': pack_max_age must be >= 0 (0 = never rotate on age)");
    if (c.gc_compact_max_packs < 0)
        throw std::runtime_error("duostore backend '" + name +
                                 "': gc_compact_max_packs must be >= 0 (0 = unlimited)");
    if (c.rocksdb_max_write_buffers < 1)
        throw std::runtime_error("duostore backend '" + name +
                                 "': rocksdb_max_write_buffers must be >= 1");
    if (c.rocksdb_max_background_jobs < 1)
        throw std::runtime_error("duostore backend '" + name +
                                 "': rocksdb_max_background_jobs must be >= 1");
    return c;
}

// ---------- 构造 / 关闭 ----------

// GC 租约的实例标识：随机 64bit hex；重启换新即可，旧租约由 TTL 过期让位
static std::string random_owner() {
    std::random_device rd;
    uint64_t v = (uint64_t(rd()) << 32) ^ rd();
    char buf[17];
    std::snprintf(buf, sizeof buf, "%016llx", (unsigned long long)v);
    return buf;
}

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
            cfg_.rocksdb_max_background_jobs, metrics});
    IMetaStore* meta = meta_.get();  // 分配回调不延长 meta 生命周期：本类持有两者，先关 data
    auto alloc = [meta](Extent::Kind kind, uint32_t n) { return meta->alloc_file_run(kind, n); };
    // 读路径 crc 失配上报（P5 corruption 指标）：只捕获计数器 shared_ptr——reader
    // 持 options 拷贝逃逸出 backend 生命周期后回调仍安全
    auto on_corruption = [c = m_read_corruption_] { c->inc(); };
#ifdef LIGHTS3_DUOSTORE_RADOS_DATA
    if (cfg_.data_kind == DuoDataKind::kRados) {
        RadosDataOptions ro;
        ro.conf_path = cfg_.rados_conf;
        ro.client_name = cfg_.rados_client;
        ro.pool = cfg_.rados_pool;
        ro.ns = cfg_.rados_namespace;
        ro.chunk_size = cfg_.rados_chunk_size;
        ro.buffer_total = cfg_.rados_buffer_total;
        ro.connect_timeout_sec = cfg_.rados_connect_timeout_sec;
        ro.op_timeout_sec = cfg_.rados_op_timeout_sec;
        ro.verify_chunk_crc = cfg_.verify_chunk_crc;
        ro.on_corruption = on_corruption;
        ro.metrics = metrics;  // op 延迟/错误指标（C4，docs/duostore-rados-data.md §10）
        // 写侧 pin 与 fs 路径同源注入（docs/gaps.md §1.2）：此前 rados 分支整条
        // 缺失，孤儿扫描会删掉在途大对象已落地的分片
        auto rpins = pins_;
        ro.pins = ChunkPinHooks{[rpins](uint64_t id) { rpins->pin_id(id); },
                                [rpins](uint64_t id) { rpins->unpin_id(id); }};
        data_ = std::make_unique<RadosDataStore>(std::move(ro), pool_, alloc);
        write_pins_ = true;
    }
#endif
    if (!data_) {
        // 压实迁移与写侧 pin 钩子（P4）：迁移标准实现 + pin 表同源注入。回调不捕获
        // this（meta/pins 生命周期由本类持有与 shared_ptr 保证），测试注入组装同款
        auto pins = pins_;
        data_ = std::make_unique<FsDataStore>(
            FsDataOptions{cfg_.root, cfg_.chunk_size, cfg_.verify_chunk_crc,
                          cfg_.pack_threshold, cfg_.pack_max_size, cfg_.pack_writers,
                          cfg_.pack_max_age_sec, on_corruption},
            pool_, alloc,
            [meta](uint64_t pack_id, uint64_t size) { meta->seal_pack(pack_id, size); },
            [meta, pins](IDataStore& ds, std::vector<PackScanRecord>&& batch) {
                return migrate_pack_records(*meta, ds, pins.get(), std::move(batch));
            },
            ChunkPinHooks{[pins](uint64_t id) { pins->pin_id(id); },
                          [pins](uint64_t id) { pins->unpin_id(id); }});
        write_pins_ = true;
    }
    gc_owner_ = random_owner();
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
    gc_owner_ = random_owner();
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
    m_gc_packs_sealed_aged_ = metrics.counter(
        "lights3_duostore_gc_packs_sealed_aged_total",
        "Active packs sealed by age rotation (pack_max_age) instead of by capacity");
    m_gc_compact_deferred_ = metrics.gauge(
        "lights3_duostore_gc_compact_deferred",
        "Compaction-eligible packs left over by the last round's budget (0 = budget not binding)");
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
    m_orphan_packs_removed_ = metrics.counter(
        "lights3_duostore_orphan_packs_removed_total",
        "Account-less pack files removed by orphan scans (created, no record ever committed)");
    m_orphan_refs_missing_ = metrics.gauge(
        "lights3_duostore_orphan_refs_missing",
        "Refs pointing at missing chunk files as of the last orphan scan (data loss signal)");
    m_orphan_packstats_missing_ = metrics.gauge(
        "lights3_duostore_orphan_packstats_missing",
        "Pack accounts whose file is missing as of the last orphan scan (data loss signal)");

    // 用量与空间放大（docs/gaps.md §6.1）：pack_bytes/pack_live_bytes 之比即放大率
    // ——留给查询侧算，两个 gauge 各自独立可读（gauge 是整型，先算好比值会丢精度）
    m_bytes_chunks_ = metrics.gauge("lights3_duostore_chunk_bytes",
                                    "Total bytes of chunk entities on disk (last orphan scan)");
    m_bytes_packs_ = metrics.gauge("lights3_duostore_pack_bytes",
                                   "Total bytes of pack files on disk (last orphan scan)");
    m_pack_accounted_bytes_ = metrics.gauge(
        "lights3_duostore_pack_accounted_bytes",
        "Sum of sealed pack file_size in the meta accounts (last GC round)");
    m_pack_live_bytes_ = metrics.gauge(
        "lights3_duostore_pack_live_bytes",
        "Sum of live bytes across packs (last GC round); accounted/live = space amplification");
    m_packs_total_ = metrics.gauge("lights3_duostore_packs",
                                   "Packs with an account as of the last GC round");

    // 回收是否追得上删除（§6.1）：深度只在全量轮更新——增量轮从上轮高位起扫，
    // 看不到队头积压，用它刷新会周期性地把深度谎报成 0
    m_gcq_depth_ = metrics.gauge(
        "lights3_duostore_gcq_depth",
        "Reclaim-queue entries seen by the last full GC scan (incremental rounds do not update)");
    m_gcq_oldest_age_ = metrics.gauge(
        "lights3_duostore_gcq_oldest_age_seconds",
        "Age of the oldest reclaim-queue entry at the last full GC scan (0 = queue empty)");
    // skipped 类是"本轮观测"而非累计：grace/pin 跳过项每轮重扫，单调计数会虚高
    m_gc_skipped_grace_ = metrics.gauge("lights3_duostore_gc_skipped_grace",
                                        "Reclaim entries skipped for gc_grace in the last round");
    m_gc_skipped_pinned_ = metrics.gauge("lights3_duostore_gc_skipped_pinned",
                                         "Reclaim entries skipped for pins in the last round");
    m_gc_duration_ = metrics.histogram(
        "lights3_duostore_gc_round_seconds", "Wall time of a completed GC round",
        {0.01, 0.1, 0.5, 1, 5, 15, 60, 300, 1800});
    // 回收来源分桶（codec gcq 的 reason 字节，§6.1）：定位压力来自覆盖写、批量
    // 删除还是 mpu 弃件。注册全部取值——缺失的桶在 Prometheus 里读作"没数据"而
    // 非"为 0"，会让"删除压力突然消失"和"从来没有过删除"看起来一样
    for (auto r : {ReclaimReason::kUnknown, ReclaimReason::kOverwrite, ReclaimReason::kDelete,
                   ReclaimReason::kPartOverwrite, ReclaimReason::kAbort,
                   ReclaimReason::kComplete})
        m_gc_reclaims_by_reason_[size_t(r)] = metrics.counter(
            "lights3_duostore_gc_reclaims_by_reason_total",
            "Reclaim queue entries acked, split by what enqueued them",
            {{"reason", reclaim_reason_name(r)}});
    m_read_corruption_ = metrics.counter(
        "lights3_duostore_read_corruption_total",
        "Chunk/pack crc mismatches detected on the GET read path (P5 corruption metric)");
}

// 重启弃用 active pack（§5.2）：数据面从不复用旧 active pack（新号段 + O_EXCL），
// 上代崩溃/析构遗留的 unsealed 账在此补封——否则它们永远进不了空 pack 整删与
// P4 压实的候选集。file_size 以 0 补（未知；压实顺扫时可再 stat），seal_pack
// 契约保证 0 不覆盖已知值
void DuoStoreBackend::abandon_stale_packs() {
    // 只补封"确实无人在写"的 pack（docs/gaps.md §1.4）：多网关共享同一 meta +
    // 同一 data root 时（redis/tikv meta 的误配，或滚动重启期间新旧进程重叠），
    // 无条件补封会把**另一实例正在写**的 active pack 标成 sealed → 它随即成为
    // 压实候选被整体重写，账一度归零还会被 remove_pack 整删，而对方仍持 fd 继续
    // 追加，写入落到已删 inode = 静默数据丢失。
    // 两道门：从网关（gc_enabled=false）根本不该碰别人的账；主网关则逐个探测
    // active pack 的写锁
    if (!cfg_.gc_enabled) {
        LOG_INFO("duostore '{}': gc_enabled=false, skipping stale-pack sealing "
                 "(another instance owns GC)", cfg_.name);
        return;
    }
    try {
        size_t sealed = 0, in_use = 0;
        for (const auto& ps : meta_->pack_stats()) {
            if (ps.sealed) continue;
            if (data_->pack_write_locked(ps.pack_id)) {
                ++in_use;
                continue;
            }
            meta_->seal_pack(ps.pack_id, 0);
            ++sealed;
        }
        if (in_use)
            LOG_WARN("duostore '{}': {} unsealed pack(s) are being written by another "
                     "process, left untouched ({} sealed)", cfg_.name, in_use, sealed);
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
    validate_bucket_name(bucket, kAllowReserved);
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
                 std::vector<PinTable::Handle> ids)
        : inner_(std::move(inner)), pins_(std::move(pins)), ids_(std::move(ids)) {}
    ~PinnedReader() override { pins_->unpin(ids_); }

    Task<size_t> read(std::span<std::byte> buf) override { return inner_->read(buf); }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    std::shared_ptr<PinTable> pins_;
    std::vector<PinTable::Handle> ids_;
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
// 清理经 exception_ptr 移出 handler。兜底失败也无害——落入孤儿扫描。
// 例外：UndeterminedCommit（redis 连接断 / tikv primary commit 超时）意味着事务
// **可能已生效**——此时删数据会毁掉已被对象引用的内容，产生指向已删数据的坏对象。
// 结果不明一律不删，把数据留给孤儿扫描按"refs 无引用"自然收敛
template <class Commit>
Task<void> commit_or_discard(IDataStore& data, const DataRef& ref, Commit commit) {
    std::exception_ptr err;
    bool undetermined = false;
    try {
        commit();
    } catch (const duostore::UndeterminedCommit&) {
        err = std::current_exception();
        undetermined = true;
    } catch (...) {
        err = std::current_exception();
    }
    if (err) {
        if (!undetermined) {
            try {
                co_await data.remove(ref.extents);
            } catch (...) {
            }
        }
        std::rethrow_exception(err);
    }
}

}  // namespace

Task<PutResult> DuoStoreBackend::put_object(std::string_view bucket, std::string_view key,
                                            ObjectMeta meta, http::BodyReader& body,
                                            PutCondition cond) {
    validate_bucket_name(bucket, kAllowReserved);
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
    // 提交点；旧 DataRef 同批入 gcq。条件检查在 meta 事务原子区内完成，
    // 失败抛出走 discard 路径回收已落的数据 extent
    co_await commit_or_discard(*data_, pumped.ref,
                               [&] { meta_->put_object(bucket, key, std::move(rec), cond); });
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
    // meta-only 读（docs/gaps.md §3.9）：HEAD 不为整份 manifest 买单
    auto meta = meta_->head_object(bucket, key);
    if (!meta) {
        require_bucket(bucket);  // 区分 NoSuchBucket / NoSuchKey
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }
    co_return std::move(*meta);
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
    validate_bucket_name(bucket, kAllowReserved);
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

Task<ListPartsResult> DuoStoreBackend::list_parts(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  const ListPartsOptions& opt) {
    validate_object_key(key);
    co_await pool_->schedule();
    // 分片数由协议封顶 10000，全量物化是有界的；分页只在这里裁剪
    std::vector<PartMeta> all;
    for (const auto& p : meta_->list_parts(bucket, key, upload_id))
        all.push_back({p.part_no, p.size, p.etag, codec::from_unix_ms(p.modified_ms)});
    co_return apply_parts_page(std::move(all), opt);
}

Task<ListUploadsResult> DuoStoreBackend::list_multipart_uploads(std::string_view bucket,
                                                                const ListUploadsOptions& opt) {
    co_await pool_->schedule();
    // 桶内 upload 数无上界，游标与条数下推给引擎（sqlite/rocks/tikv 真跳过，
    // redis 只能全扫）。delimiter 非空时不能限条数：分组要看到全貌才判得出截断，
    // 限了会把"还有更多"误报成到尾
    int limit = opt.delimiter.empty() && opt.max_uploads > 0 ? opt.max_uploads + 1 : 0;
    co_return apply_uploads_page(
        meta_->list_uploads(bucket, opt.key_marker, opt.upload_id_marker, limit), opt);
}

// ---------- GC 一期（§9/§9.1）----------

namespace {

constexpr size_t kGcBatch = 256;  // 单轮 peek 批量；批间 ack 后推进，防大积压单批爆内存
// 批内累计 extent 上限（gaps §2.11）：按条数批在"删过 TB 级对象"的账面下单批可达
// GB 级驻留——enqueue 侧已按 kReclaimMaxExtents 拆分，这里对拆分前遗留的旧账兜底
constexpr size_t kGcBatchExtents = 32768;

}  // namespace

Task<DuoGcStats> DuoStoreBackend::run_gc_once() {
    co_await pool_->schedule();
    // 登记为在途：close() 经 bg_.wait_idle() 等本轮结束后才拆 meta_/data_——
    // 手动钩子与后台 worker 同一套账，不存在"只查一次 closed_"的 TOCTOU 窗口
    BackgroundTaskGroup::Scope scope(bg_);
    DuoGcStats st;
    if (!scope.ok()) co_return st;                 // 正在关闭
    auto permit = co_await gc_sem_.acquire();      // 手动钩子 vs 后台 worker 互斥
    // 多网关租约（§6.1）：gc_enabled 只是约定，误配两台同开 GC 会互相 unlink 对方
    // 判定的空 pack。共享型 meta（redis/tikv）在此原子抢注；本地引擎恒 true
    const int64_t lease_ttl_ms =
        std::max<int64_t>(2 * int64_t(cfg_.gc_interval_sec), 600) * 1000;
    if (!meta_->try_gc_lease(gc_owner_, lease_ttl_ms)) {
        LOG_INFO("duostore '{}': GC lease held by another instance, skipping round", cfg_.name);
        co_return st;
    }
    const auto round_start = std::chrono::steady_clock::now();

    // 1) mpu_ttl 过期 multipart 清理（§8 末）：内部 abort，分片入 gcq 由下一步变现。
    // <=0 = 关闭（与 gc_interval 的 0 语义对齐——0 若解释为"立即过期"会把在途
    // multipart 全部静默 abort，是配置脚枪）
    const int64_t ttl_ms = int64_t(cfg_.mpu_ttl_sec) * 1000;
    if (ttl_ms > 0) {
        const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
        // list_buckets/list_uploads 亦须在 try 内：与并发 DeleteBucket 竞态时
        // list_uploads 抛 NoSuchBucket，逃出去会把本轮的 gcq 消费/压实/孤儿扫描
        // （步骤 2-4）全部跳过，回收白白顺延一个 gc_interval
        try {
            for (const auto& bk : meta_->list_buckets()) {
                std::vector<UploadInfo> uploads;
                try {
                    uploads = meta_->list_uploads(bk.name);
                } catch (const std::exception& e) {
                    LOG_WARN("duostore '{}': gc list uploads of bucket {} failed: {}",
                             cfg_.name, bk.name, e.what());
                    continue;  // 桶已删/暂时不可读：跳过该桶，不影响其余步骤
                }
                for (const auto& u : uploads) {
                    if (now - codec::to_unix_ms(u.initiated) < ttl_ms) continue;
                    try {
                        meta_->abort_upload(bk.name, u.key, u.upload_id);
                        ++st.uploads_expired;
                    } catch (const std::exception& e) {
                        // 与并发 complete/abort 竞争丢 NoSuchUpload 属正常；其余记 WARN 下轮重试
                        LOG_WARN("duostore '{}': gc abort expired upload {} failed: {}",
                                 cfg_.name, u.upload_id, e.what());
                    }
                }
            }
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': gc expired-upload sweep failed: {}", cfg_.name, e.what());
        }
    }

    // 2) gcq 消费（§9.1）：逾 gc_grace 且无 pin 的项，先物理删、后销账——反序在删
    // 与销之间崩溃会产生永久孤儿的账外文件；正序崩溃只是 gcq 残留，重试 unlink 幂等。
    // 按 next_seq 断点续扫：被 grace/pin 跳过的队头项不重扫（不卡轮、不重复计数、
    // 无二次解码），扫到队尾即一轮结束。
    // 跨轮水位（gaps §2.13）：上轮跳过项全部未到可重试时刻的轮次，从上轮高位起扫
    // ——队头 grace 积压不再每轮重复 peek+解码
    const int64_t grace_ms = int64_t(cfg_.gc_grace_sec) * 1000;
    const int64_t round_now = codec::to_unix_ms(std::chrono::system_clock::now());
    uint64_t next_seq = 0;
    if (gcq_skips_.any)
        next_seq = round_now < gcq_skips_.retry_at_ms ? gcq_hi_ : gcq_skips_.lo_seq;
    const bool full_scan = !gcq_skips_.any || next_seq == gcq_skips_.lo_seq;
    GcqSkips skips;  // 本轮新增的跳过项
    auto note_skip = [&skips](uint64_t seq, int64_t retry_at) {
        if (!skips.any || seq < skips.lo_seq) skips.lo_seq = seq;
        if (!skips.any || retry_at < skips.retry_at_ms) skips.retry_at_ms = retry_at;
        skips.any = true;
    };
    // 队列深度与队头年龄（§6.1）：全量轮的观测即整队全貌，增量轮只看新入队项
    uint64_t seen_entries = 0;
    int64_t oldest_enqueue_ms = 0;
    for (;;) {
        auto batch = meta_->peek_reclaims(kGcBatch, next_seq, kGcBatchExtents);
        if (batch.empty()) break;
        next_seq = batch.back().first + 1;
        seen_entries += batch.size();
        if (oldest_enqueue_ms == 0 && !batch.empty())
            oldest_enqueue_ms = batch.front().second.enqueue_ms;
        std::vector<uint64_t> acked;
        // 逐批取新鲜时间戳：上一步 abort 刚入队的项 enqueue_ms 晚于本函数入口时刻，
        // 用入口时刻判 grace 会把差值算成负数而误跳过（grace=0 应当立即可回收）
        const int64_t batch_now = codec::to_unix_ms(std::chrono::system_clock::now());
        for (const auto& [seq, rc] : batch) {
            if (batch_now - rc.enqueue_ms < grace_ms) {
                ++st.skipped_grace;
                note_skip(seq, rc.enqueue_ms + grace_ms);  // 确定性下界
                continue;
            }
            if (pins_->any_pinned(rc.extents)) {
                ++st.skipped_pinned;
                note_skip(seq, batch_now);  // pin 何时释放未知：下一轮即重试
                continue;
            }
            try {
                // chunk/rados extent 物理 unlink；pack record 为死区随压实回收（data
                // 侧 remove 内部跳过），存活账已在业务事务扣减 → 直接销账
                co_await data_->remove(rc.extents);
            } catch (const std::exception& e) {
                LOG_WARN("duostore '{}': gc remove (seq {}) failed: {}", cfg_.name, seq,
                         e.what());
                note_skip(seq, batch_now);  // 不销账，gcq 残留下轮重试
                continue;
            }
            for (const auto& e : rc.extents)
                if (e.kind != Extent::Kind::kPack) ++st.files_removed;
            if (auto& c = m_gc_reclaims_by_reason_[size_t(rc.reason) < 6 ? size_t(rc.reason) : 0])
                c->inc();
            acked.push_back(seq);
        }
        if (!acked.empty()) {
            meta_->ack_reclaims(acked);  // 批量销账（单事务/单批，接口注释的成本论证）
            st.reclaims_acked += acked.size();
        }
    }
    gcq_hi_ = std::max(gcq_hi_, next_seq);
    if (full_scan) {
        gcq_skips_ = skips;  // 全量轮：水位以本轮观测整体重建
        // 深度与队头年龄也只在全量轮刷新：增量轮从上轮高位起扫，看不到队头积压，
        // 用它更新会把深度周期性地谎报成 0
        m_gcq_depth_->set(int64_t(seen_entries));
        m_gcq_oldest_age_->set(oldest_enqueue_ms == 0
                                   ? 0
                                   : std::max<int64_t>(0, (round_now - oldest_enqueue_ms) / 1000));
    } else if (skips.any) {
        // 增量轮：与既有水位合并（旧跳过项仍在队头未重访）
        gcq_skips_.lo_seq = std::min(gcq_skips_.lo_seq, skips.lo_seq);
        gcq_skips_.retry_at_ms = std::min(gcq_skips_.retry_at_ms, skips.retry_at_ms);
    }

    // 2.5) active pack 老化封存（§6.1）：只按容量封存时，低写入量下 active pack
    // 永不轮转，其中被覆盖/删除的 record 进不了下面的压实候选集。放在压实之前，
    // 本轮封存的 pack 本轮就能被评估
    if (cfg_.pack_max_age_sec > 0) {
        try {
            st.packs_sealed_aged =
                co_await data_->seal_aged_packs(int64_t(cfg_.pack_max_age_sec) * 1000);
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': gc seal aged packs failed: {}", cfg_.name, e.what());
        }
    }

    // 3) pack 压实（P4 §9.2）：sealed、live>0 且存活率低于 pack_gc_ratio（或崩溃遗留
    // file_size 未知）的 pack 顺扫迁移存活 record；live 账随 swap 归零后由第 4 步整
    // 删变现。上轮压实后 live_recs 无推进的 pack（进行中 mpu 分片 / 旧格式 owner /
    // 存活损坏 record）跳过重扫——账一有变化即自动重试。
    // 预算与优先级（§6.1）：先把全部候选连同可回收字节收齐，按收益降序排，再按
    // "本轮最多 N 个 / 累计扫 M 字节"截断。此前是"一轮把符合条件的全部重写完"，
    // 批量删除后单轮 GC 可持锁数小时且与业务写抢 pack 槽；剩下的下一轮继续做，
    // 收益最高的先做保证空间尽快回落
    struct CompactCand {
        uint64_t pack_id = 0;
        uint64_t file_size = 0;   // 0 = 未知（stat 不支持且崩溃遗留 seal(0)）
        int64_t reclaimable = 0;  // file_size - live_bytes；未知大小恒 0
    };
    std::vector<CompactCand> cands;
    const int64_t compact_now = codec::to_unix_ms(std::chrono::system_clock::now());
    for (auto ps : meta_->pack_stats()) {
        if (!ps.sealed || ps.live_recs <= 0) continue;
        // 崩溃遗留 seal(0) 的分母先回填（gaps §2.3b）：一次 stat 即得，免得每次非
        // 优雅退出都把全部 active pack 无条件推进全量顺扫重写
        if (ps.file_size == 0) {
            if (uint64_t sz = data_->stat_pack(ps.pack_id); sz > 0) {
                meta_->seal_pack(ps.pack_id, sz);
                ps.file_size = sz;
            }
        }
        int64_t reclaimable = 0;
        if (ps.file_size > 0) {
            // live 计入 record 头后与 file_size 同口径（gaps §2.3a——只记 payload 时
            // 小对象 pack 100% 存活也恒低于阈值，压实永不收敛）。无可回收字节
            // （live ≥ file_size，含轻微低计的容差方向）或存活率过阈值都跳过
            reclaimable = int64_t(ps.file_size) - ps.live_bytes;
            if (reclaimable <= 0 ||
                double(ps.live_bytes) > cfg_.pack_gc_ratio * double(ps.file_size))
                continue;
        }
        if (auto it = compact_blocked_.find(ps.pack_id);
            it != compact_blocked_.end() && it->second.live_recs == ps.live_recs &&
            compact_now < it->second.retry_at_ms)
            continue;
        cands.push_back({ps.pack_id, ps.file_size, reclaimable});
    }
    // 收益降序；大小未知的排最后（无从估算收益，且只在崩溃遗留下出现）。同收益
    // 按 pack_id 定序，避免相邻轮次因排序不稳定而在同一批候选间来回摇摆
    std::sort(cands.begin(), cands.end(), [](const CompactCand& a, const CompactCand& b) {
        if ((a.file_size == 0) != (b.file_size == 0)) return b.file_size == 0;
        if (a.reclaimable != b.reclaimable) return a.reclaimable > b.reclaimable;
        return a.pack_id < b.pack_id;
    });

    std::vector<uint64_t> rewritten;
    uint64_t scanned_bytes = 0;
    for (size_t ci = 0; ci < cands.size(); ++ci) {
        const auto& cd = cands[ci];
        const bool over_count = cfg_.gc_compact_max_packs > 0 &&
                                ci >= size_t(cfg_.gc_compact_max_packs);
        // 字节预算按"已扫过的 pack 大小"计，且第一个候选恒放行——单个 pack 大于
        // 整轮预算时若一律挡下，压实就永远不推进了
        const bool over_bytes =
            cfg_.gc_compact_max_bytes > 0 && ci > 0 && scanned_bytes >= cfg_.gc_compact_max_bytes;
        if (over_count || over_bytes) {
            st.packs_compact_deferred = cands.size() - ci;
            break;
        }
        try {
            auto rw = co_await data_->rewrite_pack(cd.pack_id);
            ++st.packs_compacted;
            scanned_bytes += cd.file_size > 0 ? cd.file_size : rw.file_size;
            st.records_migrated += rw.migrated;
            st.records_corrupt += rw.corrupt;
            rewritten.push_back(cd.pack_id);
            // stat_pack 不支持的引擎（返回 0）：顺扫回报的 file_size 兜底回填（§9.2）
            if (cd.file_size == 0 && rw.file_size > 0)
                meta_->seal_pack(cd.pack_id, rw.file_size);
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': gc rewrite pack {} failed: {}", cfg_.name, cd.pack_id,
                     e.what());
        }
    }
    if (st.packs_compact_deferred)
        LOG_INFO("duostore '{}': gc compaction budget reached ({} packs / {} bytes scanned), "
                 "{} eligible pack(s) deferred to the next round", cfg_.name,
                 st.packs_compacted, scanned_bytes, st.packs_compact_deferred);

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
            if (pack_now - it->second < grace_ms || pins_->pinned_pack(ps.pack_id)) continue;
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

        // 空间放大（§6.1）：accounted/live 之比。stats 是第 4 步开头取的快照，本轮
        // 的整删已从盘上消失但仍在快照里——差一轮的滞后，对趋势观测无碍
        int64_t accounted = 0, live = 0;
        for (const auto& ps : stats) {
            accounted += int64_t(ps.file_size);
            live += ps.live_bytes;
        }
        m_pack_accounted_bytes_->set(accounted);
        m_pack_live_bytes_->set(live);
        m_packs_total_->set(int64_t(stats.size()));
    }

    // 完成轮才计数（关闭态的早退不计）；skip 类走 gauge 而非 counter——grace/pin
    // 跳过项每轮重扫会重复累计，单调计数会虚高误导，"本轮还剩多少没回收成"才是
    // 运维要看的量
    m_gc_skipped_grace_->set(int64_t(st.skipped_grace));
    m_gc_skipped_pinned_->set(int64_t(st.skipped_pinned));
    m_gc_duration_->observe(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - round_start).count());
    m_gc_runs_->inc();
    m_gc_reclaims_->inc(st.reclaims_acked);
    m_gc_files_removed_->inc(st.files_removed);
    m_gc_packs_removed_->inc(st.packs_removed);
    m_gc_uploads_expired_->inc(st.uploads_expired);
    m_gc_packs_compacted_->inc(st.packs_compacted);
    m_gc_packs_sealed_aged_->inc(st.packs_sealed_aged);
    m_gc_compact_deferred_->set(int64_t(st.packs_compact_deferred));
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
    // 与 GC 同一把租约（§6.1）：孤儿扫描的 unlink 同样不能与他网关的 GC 并发
    const int64_t lease_ttl_ms =
        std::max<int64_t>(2 * int64_t(cfg_.gc_interval_sec), 600) * 1000;
    if (!meta_->try_gc_lease(gc_owner_, lease_ttl_ms)) {
        LOG_INFO("duostore '{}': GC lease held by another instance, skipping orphan scan",
                 cfg_.name);
        co_return st;
    }

    // refs 快照先行：反向对账"文件必先于 ref 提交存在"（§6 数据先行）要求 R 先于
    // 盘面枚举采集——R 内的每个 id 在枚举开始前文件已在盘，缺失即真丢失。持
    // gc_sem_ 排除了 gcq 的 unlink→销账窗口，业务路径不存在"有 refs 的文件被删"。
    // 注意：refs 不分 kChunk/kRados（共号段账），本扫描以当前 data 引擎的枚举为盘
    // 面真相——同一 meta 切换 data 引擎的部署形态不在孤儿扫描支持范围。
    // 内存形态（docs/gaps.md §3.9）：此前 refs/on_disk 两个哈希集合 + disk 全量
    // 向量同时驻留（1 亿 chunk ≈ 4–5GB）；改为 refs 有序向量（8B/项）+ 盘面流式
    // 判定 + 命中位图（1bit/项），峰值降一个数量级
    std::vector<uint64_t> refs;
    meta_->scan_refs([&](uint64_t id) { refs.push_back(id); });
    std::sort(refs.begin(), refs.end());
    refs.erase(std::unique(refs.begin(), refs.end()), refs.end());
    std::vector<bool> ref_seen(refs.size(), false);  // 反向对账：盘面命中标记

    // 正向（§9.3）判定内联进枚举回调：无引用、mtime 逾 gc_grace、无 pin（写侧
    // pin 覆盖超长流式 PUT，mtime 宽限对其不充分）→ 记为候选。now 取扫描起点，
    // 宽限窗口只会偏严（安全方向）；快照后新提交的引用仍由删除前的
    // chunk_referenced 现点复查兜住（grace=0 的测试形态下快照必然过时）
    const int64_t grace_ms = int64_t(cfg_.gc_grace_sec) * 1000;
    const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
    std::vector<uint64_t> orphans;
    co_await data_->scan_chunks([&](uint64_t id, int64_t mtime_ms, uint64_t size) {
        ++st.chunks_scanned;
        st.chunk_bytes += size;
        auto it = std::lower_bound(refs.begin(), refs.end(), id);
        if (it != refs.end() && *it == id) {
            ref_seen[size_t(it - refs.begin())] = true;
            return;
        }
        if (now - mtime_ms < grace_ms) {
            ++st.skipped_grace;
            return;
        }
        if (pins_->pinned_chunk(id)) {
            ++st.skipped_pinned;
            return;
        }
        orphans.push_back(id);
    });

    for (uint64_t id : orphans) {
        if (meta_->chunk_referenced(id)) continue;  // 扫描间隙提交的新引用
        // kind 随 data 引擎（C4）：RadosDataStore::remove 只认 kRados（异种 extent
        // 视为引擎切换遗留跳过），kChunk 硬编码会让 rados 孤儿删除静默空转
        Extent e{cfg_.data_kind == DuoDataKind::kRados ? Extent::Kind::kRados
                                                       : Extent::Kind::kChunk,
                 id, 0, 0, 0};
        try {
            co_await data_->remove(std::span<const Extent>(&e, 1));
            ++st.orphans_removed;
        } catch (const std::exception& ex) {
            LOG_WARN("duostore '{}': orphan unlink {:016x} failed: {}", cfg_.name, id,
                     ex.what());
        }
    }

    // 反向（§9.3）：refs 在而文件缺 = 数据丢失征兆 → 告警计数，绝不静默删 meta
    for (size_t i = 0; i < refs.size(); ++i) {
        if (ref_seen[i]) continue;
        ++st.refs_missing;
        LOG_ERROR("duostore '{}': refs entry for chunk {:016x} but file missing "
                  "(data loss signal, keeping meta for manual inspection)", cfg_.name, refs[i]);
    }

    // packs/ 双向对账（docs/gaps.md §6.1）：chunk 侧的孤儿扫描此前不覆盖 pack 实体，
    // 而 pack 文件是"先建文件、首条 record 提交时才落 packstat 行"——恰在这个窗口
    // 硬崩，文件在盘上、账里没有任何行，既永不回收也不可观测。
    // pack 的存活判据比 chunk 更严：账外 ⇒ 候选，但还要过三道门——
    //   ① mtime 逾 gc_grace（刚建的 active pack 首条 record 尚未提交）
    //   ② 无 pin（在途读者）
    //   ③ 无写锁（本进程或另一实例的 active pack；flock 是"有活着的写者"的唯一
    //      可靠信号，与 abandon_stale_packs 同一判据）
    // 反向的"packstat 在而文件缺"只告警不销账——与 refs_missing 同样是数据丢失征兆
    {
        std::vector<uint64_t> known;
        for (const auto& ps : meta_->pack_stats()) known.push_back(ps.pack_id);
        std::sort(known.begin(), known.end());
        std::vector<bool> pack_seen(known.size(), false);
        std::vector<uint64_t> orphan_packs;
        co_await data_->scan_packs([&](uint64_t id, int64_t mtime_ms, uint64_t size) {
            ++st.packs_scanned;
            st.pack_bytes += size;
            auto it = std::lower_bound(known.begin(), known.end(), id);
            if (it != known.end() && *it == id) {
                pack_seen[size_t(it - known.begin())] = true;
                return;
            }
            if (now - mtime_ms < grace_ms || pins_->pinned_pack(id)) {
                ++st.packs_skipped_active;
                return;
            }
            orphan_packs.push_back(id);
        });
        for (uint64_t id : orphan_packs) {
            // 写锁探测放在枚举之外：它要开 fd + flock，不该塞进枚举回调里逐个做
            if (data_->pack_write_locked(id)) {
                ++st.packs_skipped_active;
                continue;
            }
            try {
                co_await data_->remove_pack(id);
                ++st.orphan_packs_removed;
                LOG_WARN("duostore '{}': removed account-less pack file {:016x} "
                         "(created but no record ever committed)", cfg_.name, id);
            } catch (const std::exception& ex) {
                LOG_WARN("duostore '{}': orphan pack unlink {:016x} failed: {}", cfg_.name, id,
                         ex.what());
            }
        }
        for (size_t i = 0; i < known.size(); ++i) {
            if (pack_seen[i]) continue;
            ++st.pack_stats_missing;
            LOG_ERROR("duostore '{}': packstat for {:016x} but file missing "
                      "(data loss signal, keeping meta for manual inspection)", cfg_.name,
                      known[i]);
        }
    }

    m_orphan_runs_->inc();
    m_orphan_removed_->inc(st.orphans_removed);
    m_orphan_packs_removed_->inc(st.orphan_packs_removed);
    m_orphan_refs_missing_->set(int64_t(st.refs_missing));
    m_orphan_packstats_missing_->set(int64_t(st.pack_stats_missing));
    // 用量（§6.1）：盘面实测字节，随孤儿扫描周期刷新（默认 1/d）
    m_bytes_chunks_->set(int64_t(st.chunk_bytes));
    m_bytes_packs_->set(int64_t(st.pack_bytes));
    co_return st;
}

Task<duostore::MetaDumpStats> DuoStoreBackend::run_meta_dump(std::ostream& out) {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    if (!scope.ok())
        throw S3Error(S3ErrorCode::InternalError, "duostore meta dump: backend closing");
    auto permit = co_await gc_sem_.acquire();  // 与 GC/孤儿扫描互斥（一致快照前提）
    co_return duostore::dump_meta(*meta_, out);
}

Task<duostore::MetaDumpStats> DuoStoreBackend::run_meta_load(std::istream& in) {
    co_await pool_->schedule();
    duostore::MetaDumpStats st;
    {
        BackgroundTaskGroup::Scope scope(bg_);
        if (!scope.ok())
            throw S3Error(S3ErrorCode::InternalError, "duostore meta load: backend closing");
        auto permit = co_await gc_sem_.acquire();
        st = duostore::load_meta(*meta_, in);
    }  // 信号量出块释放——下面的孤儿扫描要重取同一把
    // 恢复固化流程的收尾（meta_dump.h 运维契约）：强制孤儿扫描回收备份窗口内
    // data 侧多余的文件（gcq 与进行中 MPU 刻意不入档，其数据在此变现回收）
    LOG_INFO("duostore '{}': meta load done, running forced orphan scan", cfg_.name);
    co_await run_orphan_scan_once();
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
    if (!cfg_.gc_enabled) {
        // 多网关非指定实例（docs/duostore-rados-data.md §8.3）；手动钩子保留。
        // 构造期各到达一次（gc_tick 不会再入），响亮留痕防"忘了哪台在跑 GC"
        LOG_INFO("duostore '{}': background GC/orphan scan disabled by gc_enabled=false "
                 "(multi-gateway secondary)", cfg_.name);
        return;
    }
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
    if (!cfg_.gc_enabled) return;                    // 与 GC 同门控（§8.3 单实例执行）
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
