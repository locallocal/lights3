#include "storage/duostore/duostore_backend.h"

#include "core/util/time.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <fstream>
#include <random>
#include <stdexcept>
#include <unordered_set>

#include "core/config.h"
#include "core/log.h"
#include "core/util/crypto.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/localfs/fs_util.h"  // StubRace: shared tiered vocabulary
#include "storage/xlocalfs/uring.h"
#include "storage/duostore/rocks_meta_store.h"
#include "storage/listing.h"
#include "storage/multipart.h"
#include "storage/scrub_throttle.h"

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

// ---------- PinTable (§7) ----------

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
    if (it == refs.end()) return;  // should not happen; tolerated defensively
    if (--it->second <= 0) refs.erase(it);
}

bool PinTable::pinned_key(bool is_pack, uint64_t id) {
    auto& s = shard_of(id);
    std::lock_guard lk(s.m);
    return (is_pack ? s.pack_refs : s.chunk_refs).count(id) != 0;
}

// ---------- in-flight read registry (read lease, roadmap §3.7) ----------

uint64_t ReadClock::begin() {
    const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
    std::lock_guard lk(m_);
    uint64_t id = next_++;
    active_.emplace(id, now);
    return id;
}

void ReadClock::end(uint64_t ticket) {
    std::lock_guard lk(m_);
    active_.erase(ticket);
}

int64_t ReadClock::oldest_or(int64_t fallback) {
    std::lock_guard lk(m_);
    return active_.empty() ? fallback : active_.begin()->second;
}

// ---------- compaction migration (P4 §9.2 steps 2-3) ----------

Task<uint64_t> migrate_pack_records(IMetaStore& meta, IDataStore& data, PinTable* pins,
                                    std::vector<PackScanRecord> batch) {
    // The owner is only a reverse-lookup hint (§9.2): object records embed
    // "b\0k"; mpu records embed "mpu\0b\0k\0id\0no" — after complete, the part
    // belongs to the object at b/k (the refs transfer does not modify the
    // record), so the hint remains usable. Parts of an in-flight mpu (from not
    // found on the object) and the pre-P4 legacy format "mpu\0id\0no" (no b/k to
    // look up) are both conservatively not migrated: the live account is
    // untouched and the pack is not deleted this round; the former unblocks
    // naturally after complete/abort, the latter is shelved permanently
    // (rewriting old records on disk is a non-goal)

    // 1) Aggregate by owner (gaps §2.13): multiple records of one object in the
    // same pack do a single get_object + a single ref swap, replacing the O(n²)
    // of rewriting the whole manifest per record
    struct Group {
        std::string b, k;
        std::vector<size_t> recs;  // batch indices, in scan order
    };
    std::vector<Group> groups;
    std::map<std::string, size_t> by_owner;  // "b\0k" -> groups index
    for (size_t i = 0; i < batch.size(); ++i) {
        // Canonical parser (docs/archive/gaps.md §6.1: the three owner forms converge in
        // codec, offline forensics tools reuse the same entry point);
        // kLegacyPart/kUnknown have no b/k to look up, conservatively not migrated
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

    // 2) Reverse-check liveness per group: pair from with the current manifest
    // position by position (the same extent cannot appear twice — the manifest is
    // assembled from distinct records; positions are still marked as paired for
    // defense)
    struct LiveRec {
        size_t group;      // groups index
        size_t manifest_i; // position replaced within that group's manifest
        size_t rec_i;      // batch index
    };
    std::vector<LiveRec> live;
    std::vector<std::optional<ObjectRec>> group_rec(groups.size());
    for (size_t g = 0; g < groups.size(); ++g) {
        auto rec = meta.get_object(groups[g].b, groups[g].k);
        if (!rec) continue;  // object missing = all dead region / in-flight mpu, conservatively not migrated
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

    // 3) Batch-append live payloads (single slot lock + single fdatasync in the
    // fs implementation); new records' owners uniformly use the object form.
    // Same routing semantics as open_writer — landing in chunks after a threshold
    // shrink or with packs disabled is equally correct
    std::vector<std::string> owners(live.size());
    std::vector<PackAppendItem> items(live.size());
    for (size_t i = 0; i < live.size(); ++i) {
        const auto& gr = groups[live[i].group];
        owners[i] = gr.b + '\0' + gr.k;
        items[i] = {owners[i], std::span<const std::byte>(batch[live[i].rec_i].payload)};
    }
    std::vector<DataRef> nrefs = co_await data.write_batch(items);

    // 4) Assemble the new manifest per group (multiple replacements in one pass)
    // → batch ref swap
    std::vector<SwapReq> reqs;
    std::vector<size_t> req_group;
    reqs.reserve(groups.size());
    for (size_t g = 0; g < groups.size(); ++g) {
        if (!group_rec[g]) continue;
        std::map<size_t, size_t> repl;  // manifest position -> live index
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

    // 5) Clean up failed groups + symmetric write-side unpin. Overwritten/deleted
    // in the meantime: appended new records become dead regions (no account,
    // reclaimed by future compaction); chunk-kind residue is cleaned explicitly
    // (refs were never established, deleting them is clean).
    // Pins: on swap success refs are on the books, on failure the file is already
    // deleted — unpin in both cases
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

// ---------- configuration parsing (§11) ----------

namespace {

// All scalar parsing shares one diagnostic format and strictness (trailing
// garbage rejected); configuration errors are always runtime_error
[[noreturn]] void bad_param(const std::string& name, const char* key, const std::string& v) {
    throw std::runtime_error("duostore backend '" + name + "': invalid " + key + ": " + v);
}

bool parse_bool_param(const std::string& name, const char* key, const std::string& v) {
    try {
        return parse_bool(v);  // shared token set (core/config.h)
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
    // io_uring fs data plane (roadmap §3.4 ⑤); range checks inline because the fields are
    // unsigned (a negative would wrap before the validation section below)
    if (auto* v = get("fs_uring")) c.fs_uring = parse_bool_param(name, "fs_uring", *v);
    if (auto* v = get("fs_uring_queue_depth")) {
        int qd = parse_int_param(name, "fs_uring_queue_depth", *v);
        if (qd < 8 || qd > 65536)
            throw std::runtime_error("duostore backend '" + name +
                                     "': fs_uring_queue_depth must be in [8,65536]");
        c.fs_uring_queue_depth = unsigned(qd);
    }
    if (auto* v = get("fs_uring_sqpoll"))
        c.fs_uring_sqpoll = parse_bool_param(name, "fs_uring_sqpoll", *v);
    if (auto* v = get("fs_uring_rings")) {
        int r = parse_int_param(name, "fs_uring_rings", *v);
        if (r < 0 || r > 64)
            throw std::runtime_error("duostore backend '" + name +
                                     "': fs_uring_rings must be in [0,64] (0 = auto)");
        c.fs_uring_rings = unsigned(r);
    }
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
    if (auto* v = get("read_lease")) c.read_lease_sec = parse_duration_sec(*v);
    // Object metadata cache (roadmap §3.8); the budget default depends on the engine
    // kind resolved below, so remember whether it was configured
    std::optional<size_t> meta_cache_entries;
    if (auto* v = get("meta_cache_entries")) meta_cache_entries = parse_size(*v);
    if (auto* v = get("meta_cache_ttl")) c.meta_cache_ttl_sec = parse_duration_sec(*v);
    if (auto* v = get("meta_cache_feed"))
        c.meta_cache_feed = parse_bool_param(name, "meta_cache_feed", *v);
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

    // meta engine selection (docs/duostore-redis-meta.md §8 / docs/duostore-sqlite-meta.md §8)
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

    // sqlite meta (docs/duostore-sqlite-meta.md §8): meta_sync carries over (local
    // engine, the durability level belongs to this process, mapped to synchronous
    // FULL/NORMAL)
    if (auto* v = get("sqlite_path"); v && !v->empty()) c.sqlite_path = *v;
    else c.sqlite_path = c.root / "meta.sqlite3";
    if (auto* v = get("sqlite_cache")) c.sqlite_cache = parse_size(*v);
    if (c.meta_kind == DuoMetaKind::kSqlite) {
        // Process-wide total budget; must remain meaningful after splitting across connections (docs/duostore-sqlite-meta.md §8)
        if (c.sqlite_cache < (1ull << 20))
            throw std::runtime_error("duostore backend '" + name +
                                     "': sqlite_cache must be >= 1MiB");
    }

    // tikv meta (docs/duostore-tikv-meta.md §9): pd_endpoints comma-separated;
    // durability = raft majority, so meta_sync is meaningless (WARNed separately
    // below the ownership table)
    if (auto* v = get("pd_endpoints")) {
        std::string_view rest = *v;
        while (!rest.empty()) {
            auto comma = rest.find(',');
            std::string_view ep = rest.substr(0, comma);
            // Trim whitespace (the "a:2379, b:2379" writing habit in YAML)
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
        // The mTLS triple is either all given or all empty (ClusterConfig treats a non-empty ca as the enable criterion)
        int given = int(!c.tikv_ca.empty()) + int(!c.tikv_cert.empty()) + int(!c.tikv_key.empty());
        if (given != 0 && given != 3)
            throw std::runtime_error("duostore backend '" + name +
                                     "': tikv_ca/tikv_cert/tikv_key must be set together");
        if (c.tikv_backoff_ms < 0)
            throw std::runtime_error("duostore backend '" + name +
                                     "': tikv_backoff_ms must be >= 0 (0 = library default)");
        // interval 0 = advancing disabled (governed by TiDB when sharing a TiDB
        // cluster, §7.3 deployment matrix); when enabled, retention must be
        // positive — pushing the safepoint to now would cut off in-flight snapshots
        if (c.tikv_gc_interval_sec < 0 || c.tikv_gc_retention_sec <= 0)
            throw std::runtime_error(
                "duostore backend '" + name +
                "': tikv_gc_interval must be >= 0 and tikv_gc_retention > 0");
    }

    // data engine selection (docs/duostore-rados-data.md §10, dual of the meta branch)
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
        // Upper bound matches the osd_max_object_size default of 128MiB (docs/duostore-rados-data.md §3.4)
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

    // data-engine-owned keys: present but not belonging to the selected engine →
    // WARN (same mechanism as the meta key ownership table below;
    // docs/duostore-rados-data.md §10 — under data=rados, chunk_size is superseded
    // by rados_chunk_size and all pack_* are ignored; verify_chunk_crc is shared
    // by both engines)
    {
        static constexpr struct {
            const char* key;
            DuoDataKind kind;
        } kDataOwnedKeys[] = {
            {"chunk_size", DuoDataKind::kFs},
            {"fs_uring", DuoDataKind::kFs},
            {"fs_uring_queue_depth", DuoDataKind::kFs},
            {"fs_uring_sqpoll", DuoDataKind::kFs},
            {"fs_uring_rings", DuoDataKind::kFs},
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

    // meta-engine-owned keys: present but not belonging to the selected engine →
    // WARN (a key→ownership table; a new engine just adds rows, avoiding the
    // O(kinds²) leaks of every branch maintaining the other engines' key lists).
    // meta_sync is shared by rocksdb and sqlite, ignored under redis / tikv
    // (durability semantics are borne by Redis AOF and raft majority
    // respectively), handled separately
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
        // redis: durability semantics belong to Redis-side AOF; tikv: a commit is a raft majority, always equivalent to sync
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
    // Object metadata cache (roadmap §3.8): a shared meta engine has writers this
    // process never sees, so the cache is off there unless configured, and then only
    // with a TTL bounding the staleness window (kept below gc_grace: a stale manifest
    // must expire before the extents it names can be reclaimed by a peer's GC)
    const bool shared_meta = c.meta_kind == DuoMetaKind::kRedis || c.meta_kind == DuoMetaKind::kTikv;
    if (meta_cache_entries) c.meta_cache_entries = *meta_cache_entries;
    else if (shared_meta) c.meta_cache_entries = 0;
    if (c.meta_cache_ttl_sec < 0)
        throw std::runtime_error("duostore backend '" + name +
                                 "': meta_cache_ttl must be >= 0 (0 = no expiry)");
    if (shared_meta && c.meta_cache_entries > 0) {
        if (c.meta_cache_ttl_sec <= 0)
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta_cache_entries > 0 on a shared meta engine "
                                     "(redis/tikv) requires meta_cache_ttl > 0 -- peer "
                                     "gateways' writes are only visible after expiry");
        if (c.meta_cache_ttl_sec >= c.gc_grace_sec)
            throw std::runtime_error("duostore backend '" + name +
                                     "': meta_cache_ttl must be below gc_grace on a "
                                     "shared meta engine");
    }
    return c;
}

// ---------- construction / shutdown ----------

// Instance identity for the GC lease: random 64-bit hex; a restart simply gets a
// new one, the old lease yields via TTL expiry
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
    IMetaStore* meta = meta_.get();  // the alloc callback does not extend meta's lifetime: this class owns both, data closes first
    auto alloc = [meta](Extent::Kind kind, uint32_t n) { return meta->alloc_file_run(kind, n); };
    // Read-path crc mismatch reporting (P5 corruption metric): captures only the
    // counter shared_ptr — the callback stays safe after readers holding options
    // copies escape the backend's lifetime
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
        ro.metrics = metrics;  // op latency/error metrics (C4, docs/duostore-rados-data.md §10)
        // Write-side pins injected from the same source as the fs path
        // (docs/archive/gaps.md §1.2): the rados branch used to miss this entirely, and
        // the orphan scan would delete the already-landed parts of an in-flight
        // large object
        auto rpins = pins_;
        ro.pins = ChunkPinHooks{[rpins](uint64_t id) { rpins->pin_id(id); },
                                [rpins](uint64_t id) { rpins->unpin_id(id); }};
        data_ = std::make_unique<RadosDataStore>(std::move(ro), pool_, alloc);
        write_pins_ = true;
    }
#endif
    if (!data_) {
        // Compaction migration and write-side pin hooks (P4): the standard
        // migration implementation + pin table injected from the same source.
        // Callbacks do not capture this (meta/pins lifetimes are guaranteed by
        // this class's ownership and shared_ptr); test-injection assembly is identical
        auto pins = pins_;
        FsDataOptions fopt{cfg_.root,          cfg_.chunk_size,   cfg_.verify_chunk_crc,
                           cfg_.pack_threshold, cfg_.pack_max_size, cfg_.pack_writers,
                           cfg_.pack_max_age_sec, on_corruption};
        // io_uring fs data plane (roadmap §3.4 ⑤): opt-in; unavailability (old kernel,
        // seccomp, memlock quota) degrades to the synchronous path -- same layout, only
        // async IO is lost. Warn plus resident gauge, mirroring the xlocalfs fallback
        if (cfg_.fs_uring) {
            try {
                UringOptions uo;
                uo.entries = cfg_.fs_uring_queue_depth;
                uo.sqpoll = cfg_.fs_uring_sqpoll;
                uo.rings = cfg_.fs_uring_rings;
                fopt.uring = std::make_shared<UringEngine>(pool_, uo);
            } catch (const std::exception& e) {
                LOG_WARN("duostore backend '{}': io_uring unavailable ({}); fs data plane "
                         "falls back to synchronous IO (same layout)",
                         cfg_.name, e.what());
                metrics
                    .gauge("lights3_duostore_uring_fallback",
                           "io_uring unavailable, duostore fs data plane fell back to "
                           "synchronous IO")
                    ->set(1);
            }
        }
        data_ = std::make_unique<FsDataStore>(
            std::move(fopt), pool_, alloc,
            [meta](uint64_t pack_id, uint64_t size) { meta->seal_pack(pack_id, size); },
            [meta, pins](IDataStore& ds, std::vector<PackScanRecord>&& batch) {
                return migrate_pack_records(*meta, ds, pins.get(), std::move(batch));
            },
            ChunkPinHooks{[pins](uint64_t id) { pins->pin_id(id); },
                          [pins](uint64_t id) { pins->unpin_id(id); }});
        write_pins_ = true;
    }
    wire_cache_invalidation();
    gc_owner_ = random_owner();
    load_quarantine();
    abandon_stale_packs();
    schedule_gc();
    schedule_orphan_scan();
    schedule_read_lease();
}

DuoStoreBackend::DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                                 std::unique_ptr<IMetaStore> meta,
                                 std::unique_ptr<IDataStore> data, MetricsScope metrics)
    : cfg_(std::move(cfg)), pool_(std::move(pool)), meta_(std::move(meta)),
      data_(std::move(data)) {
    init_metrics(metrics);
    wire_cache_invalidation();
    gc_owner_ = random_owner();
    load_quarantine();
    abandon_stale_packs();
    schedule_gc();
    schedule_orphan_scan();
    schedule_read_lease();
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

    // Usage and space amplification (docs/archive/gaps.md §6.1): the ratio
    // pack_bytes/pack_live_bytes is the amplification factor — left for the query
    // side to compute, the two gauges each stay independently readable (gauges are
    // integral; a precomputed ratio would lose precision)
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

    // Whether reclamation keeps up with deletion (§6.1): depth updates only on
    // full rounds — incremental rounds scan from the previous high watermark and
    // cannot see backlog at the queue head; refreshing from them would
    // periodically misreport the depth as 0
    m_gcq_depth_ = metrics.gauge(
        "lights3_duostore_gcq_depth",
        "Reclaim-queue entries seen by the last full GC scan (incremental rounds do not update)");
    m_gcq_oldest_age_ = metrics.gauge(
        "lights3_duostore_gcq_oldest_age_seconds",
        "Age of the oldest reclaim-queue entry at the last full GC scan (0 = queue empty)");
    // The skipped family is a per-round observation, not cumulative: grace/pin skips are rescanned every round, a monotonic counter would inflate
    m_gc_skipped_grace_ = metrics.gauge("lights3_duostore_gc_skipped_grace",
                                        "Reclaim entries skipped for gc_grace in the last round");
    m_gc_skipped_pinned_ = metrics.gauge("lights3_duostore_gc_skipped_pinned",
                                         "Reclaim entries skipped for pins in the last round");
    m_gc_skipped_leased_ = metrics.gauge(
        "lights3_duostore_gc_skipped_leased",
        "Reclaim entries deferred by a peer gateway's read lease in the last round");
    m_packs_quarantined_ = metrics.gauge(
        "lights3_duostore_packs_quarantined",
        "Packs parked in the corruption quarantine (needs operator attention: "
        "`lights3 duostore quarantine list`)");
    m_gc_duration_ = metrics.histogram(
        "lights3_duostore_gc_round_seconds", "Wall time of a completed GC round",
        {0.01, 0.1, 0.5, 1, 5, 15, 60, 300, 1800});
    // Reclamation source buckets (the reason byte of codec's gcq, §6.1):
    // pinpoints whether pressure comes from overwrites, bulk deletes, or mpu
    // abandonment. Register every value — a missing bucket reads as "no data" in
    // Prometheus rather than "zero", which would make "delete pressure suddenly
    // vanished" look the same as "there were never any deletes"
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
    m_cache_peer_invalidations_ = metrics.counter(
        "lights3_duostore_meta_cache_feed_invalidations_total",
        "Object records dropped from the meta cache on a peer gateway's commit message "
        "(shared meta engine with an invalidation feed, backlog-sequence ⑤)");
    m_cache_feed_resets_ = metrics.counter(
        "lights3_duostore_meta_cache_feed_resets_total",
        "Meta cache cleared because the invalidation feed (re)connected");
    // Object metadata cache (roadmap §3.8). The from_params contract is re-checked here
    // for directly constructed configs (tests, embedding): a TTL-less cache over a
    // shared engine would serve peers' overwrites indefinitely, so it is refused
    const bool shared_meta =
        cfg_.meta_kind == DuoMetaKind::kRedis || cfg_.meta_kind == DuoMetaKind::kTikv;
    if (shared_meta && cfg_.meta_cache_entries > 0 &&
        (cfg_.meta_cache_ttl_sec <= 0 || cfg_.meta_cache_ttl_sec >= cfg_.gc_grace_sec))
        throw std::runtime_error("duostore backend '" + cfg_.name +
                                 "': meta cache on a shared meta engine needs "
                                 "0 < meta_cache_ttl < gc_grace");
    meta_cache_ = std::make_shared<ObjectRecCache>(
        MetaCacheOptions{cfg_.meta_cache_entries,
                         std::chrono::seconds(std::max(0, cfg_.meta_cache_ttl_sec))},
        metrics);
}

// Shared meta engines (backlog-sequence ⑤): when the engine can push peers' commits,
// subscribe the cache to it -- a peer's overwrite/delete then drops the local record
// within a message's latency instead of at meta_cache_ttl. The TTL stays as the
// bound for lost messages (pub/sub is fire-and-forget); a feed (re)connect clears the
// whole cache. The callbacks hold the cache by shared_ptr, never this backend
void DuoStoreBackend::wire_cache_invalidation() {
    if (!meta_cache_->enabled() || !cfg_.meta_cache_feed) return;
    const bool shared_meta =
        cfg_.meta_kind == DuoMetaKind::kRedis || cfg_.meta_kind == DuoMetaKind::kTikv;
    if (!shared_meta) return;
    auto cache = meta_cache_;
    bool ok = meta_->subscribe_invalidations(
        [cache, m = m_cache_peer_invalidations_](std::string_view b, std::string_view k) {
            cache->invalidate(b, k);
            m->inc();
        },
        [cache, m = m_cache_feed_resets_] {
            cache->clear();
            m->inc();
        });
    if (ok)
        LOG_INFO("duostore '{}': meta cache subscribed to the engine's invalidation feed "
                 "(peer commits invalidate; meta_cache_ttl={}s bounds lost messages)",
                 cfg_.name, cfg_.meta_cache_ttl_sec);
    else
        LOG_INFO("duostore '{}': meta engine has no invalidation feed; the cache relies on "
                 "meta_cache_ttl={}s (bounded staleness)",
                 cfg_.name, cfg_.meta_cache_ttl_sec);
}

// Discard active packs on restart (§5.2): the data plane never reuses old active
// packs (new id segment + O_EXCL), so unsealed accounts left by the previous
// generation's crash/destruction are catch-up sealed here — otherwise they can
// never enter the candidate sets for whole empty-pack deletion and P4 compaction.
// file_size is backfilled as 0 (unknown; the compaction scan can stat again), and
// the seal_pack contract guarantees 0 never overwrites a known value
void DuoStoreBackend::abandon_stale_packs() {
    // Only catch-up seal packs that "truly nobody is writing" (docs/archive/gaps.md §1.4):
    // with multiple gateways sharing the same meta + the same data root (a
    // redis/tikv meta misconfiguration, or old/new processes overlapping during a
    // rolling restart), unconditional sealing would mark an active pack **another
    // instance is writing** as sealed → it immediately becomes a compaction
    // candidate and is wholly rewritten, and once its account hits zero it even
    // gets remove_pack'd, while the other side still holds the fd and keeps
    // appending — writes land on a deleted inode = silent data loss.
    // Two gates: a secondary gateway (gc_enabled=false) should not touch others'
    // accounts at all; the primary gateway probes each active pack's write lock
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
        // A sealing failure does not block startup (retry chances remain at the next startup/GC), but must leave a loud trace
        LOG_WARN("duostore '{}': sealing stale packs failed: {}", cfg_.name, e.what());
    }
}

// ---------- corrupt-pack quarantine (roadmap §3.7) ----------

std::filesystem::path DuoStoreBackend::quarantine_dir() const {
    return cfg_.root / "quarantine";
}

// One tiny file per pack: <root>/quarantine/<pack_id hex> with a single
// "v1 <live> <corrupt> <since_ms> <purged>" line — restarts and the admin CLI
// (a separate process) read the same ledger the GC round writes
void DuoStoreBackend::load_quarantine() {
    std::error_code ec;
    std::filesystem::create_directories(quarantine_dir(), ec);
    for (const auto& de : std::filesystem::directory_iterator(quarantine_dir(), ec)) {
        uint64_t id = 0;
        auto name = de.path().filename().string();
        auto res = std::from_chars(name.data(), name.data() + name.size(), id, 16);
        if (res.ec != std::errc{} || res.ptr != name.data() + name.size()) continue;
        std::ifstream f(de.path());
        std::string tag;
        DuoQuarantineEntry e;
        e.pack_id = id;
        int purged = 0;
        if (!(f >> tag >> e.live_recs >> e.corrupt_records >> e.quarantined_ms >> purged) ||
            tag != "v1") {
            LOG_WARN("duostore '{}': unreadable quarantine entry {}, ignoring", cfg_.name,
                     de.path().string());
            continue;
        }
        e.purged = purged != 0;
        quarantined_.emplace(id, e);
    }
    if (!quarantined_.empty())
        LOG_WARN("duostore '{}': {} pack(s) in the corruption quarantine "
                 "(`lights3 duostore quarantine list`)", cfg_.name, quarantined_.size());
    m_packs_quarantined_->set(int64_t(quarantined_.size()));
}

void DuoStoreBackend::quarantine_save(const DuoQuarantineEntry& e) {
    char name[32];
    std::snprintf(name, sizeof name, "%016llx", (unsigned long long)e.pack_id);
    std::ofstream f(quarantine_dir() / name, std::ios::trunc);
    f << "v1 " << e.live_recs << ' ' << e.corrupt_records << ' ' << e.quarantined_ms << ' '
      << (e.purged ? 1 : 0) << '\n';
    f.flush();
    if (!f)
        LOG_ERROR("duostore '{}': cannot persist quarantine entry for pack {:016x} "
                  "(in-memory state stays authoritative until restart)", cfg_.name, e.pack_id);
}

// Caller holds q_mu_
void DuoStoreBackend::quarantine_drop(uint64_t pack_id) {
    quarantined_.erase(pack_id);
    char name[32];
    std::snprintf(name, sizeof name, "%016llx", (unsigned long long)pack_id);
    std::error_code ec;
    std::filesystem::remove(quarantine_dir() / name, ec);
    m_packs_quarantined_->set(int64_t(quarantined_.size()));
}

std::vector<DuoQuarantineEntry> DuoStoreBackend::quarantine_list() {
    std::lock_guard lk(q_mu_);
    std::vector<DuoQuarantineEntry> out;
    out.reserve(quarantined_.size());
    for (const auto& [id, e] : quarantined_) out.push_back(e);
    return out;
}

bool DuoStoreBackend::quarantine_release(uint64_t pack_id) {
    std::lock_guard lk(q_mu_);
    if (!quarantined_.count(pack_id)) return false;
    quarantine_drop(pack_id);
    LOG_INFO("duostore '{}': pack {:016x} released from quarantine; compaction retries "
             "next GC round", cfg_.name, pack_id);
    return true;
}

Task<bool> DuoStoreBackend::quarantine_purge(uint64_t pack_id) {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    if (!scope.ok()) co_return false;  // shutting down
    auto permit = co_await gc_sem_.acquire();  // no GC round while the file goes away
    {
        std::lock_guard lk(q_mu_);
        auto it = quarantined_.find(pack_id);
        if (it == quarantined_.end() || it->second.purged) co_return false;
    }
    if (pins_->pinned_pack(pack_id) || data_->pack_write_locked(pack_id))
        throw S3Error(S3ErrorCode::InternalError,
                      "pack is held by an in-flight reader/writer; retry later");
    co_await data_->remove_pack(pack_id);
    {
        std::lock_guard lk(q_mu_);
        auto it = quarantined_.find(pack_id);
        if (it != quarantined_.end()) {
            it->second.purged = true;
            quarantine_save(it->second);
        }
    }
    LOG_WARN("duostore '{}': quarantined pack {:016x} purged from disk; its remaining "
             "records are lost — delete the owning objects to drain the accounting",
             cfg_.name, pack_id);
    co_return true;
}

DuoStoreBackend::~DuoStoreBackend() {
    // The normal path is sync_wait(close()) first; fallback: cancel timers + wait
    // for in-flight GC (preventing callbacks using a freed this), then close meta
    // synchronously (RocksDB lands cleanly)
    if (closed_) return;
    shutdown_background();
    if (meta_) meta_->close();
}

Task<void> DuoStoreBackend::close() {
    if (closed_.exchange(true)) co_return;
    // Cancel the GC timers, wait for in-flight GC coroutines to end (§9 lifecycle)
    shutdown_background();
    co_await data_->close();  // seal active packs (P2)
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
        require_bucket(bucket);  // distinguish NoSuchBucket / NoSuchKey
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

// Pump loop shared by PUT/upload_part (§6.1 ③④): streams to the data plane,
// computing MD5 as it writes.
// The owner goes into the pack record header (§5.2): object = "bucket\0key",
// part = "mpu\0<id>\0<no>"
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

// Pin-holding read wrapper (§7): pins are registered before construction,
// released on destruction. Self-contained — the reader escapes the backend's
// lifetime along with the HTTP response, holding the pin table via shared_ptr
class PinnedReader final : public http::BodyReader {
public:
    PinnedReader(std::unique_ptr<http::BodyReader> inner, std::shared_ptr<PinTable> pins,
                 std::vector<PinTable::Handle> ids, std::shared_ptr<ReadClock> clock,
                 uint64_t ticket)
        : inner_(std::move(inner)), pins_(std::move(pins)), ids_(std::move(ids)),
          clock_(std::move(clock)), ticket_(ticket) {}
    ~PinnedReader() override {
        pins_->unpin(ids_);
        clock_->end(ticket_);  // the read-lease registration ends with the read (roadmap §3.7)
    }

    Task<size_t> read(std::span<std::byte> buf) override { return inner_->read(buf); }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    std::shared_ptr<PinTable> pins_;
    std::vector<PinTable::Handle> ids_;
    std::shared_ptr<ReadClock> clock_;
    uint64_t ticket_;
};

// Symmetric release of write-side pins (§9.3): ChunkWriter pins on allocation
// (the orphan scan does not reclaim in-flight writes), and after finish ownership
// transfers to the caller — this guard releases after the meta commit or fallback
// deletion (when the coroutine frame exits). Effective only when the cfg
// constructor assembled ChunkPinHooks (write_pins_): the injection constructor
// has no hooks, and blind release would wrongly decrement concurrent readers' pins
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

// Fallback deletion of produced data when the meta commit fails (§6.1 ⑤);
// co_await cannot appear inside a catch block, so cleanup is moved out of the
// handler via exception_ptr. A failed fallback is also harmless — it falls to the
// orphan scan.
// Exception: UndeterminedCommit (redis connection dropped / tikv primary commit
// timeout) means the transaction **may have taken effect** — deleting the data
// then would destroy content already referenced by the object, producing a bad
// object pointing at deleted data. When the outcome is unknown, never delete;
// leave the data for the orphan scan to converge naturally via "no refs"
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
    require_bucket(bucket);  // precheck; the authoritative check is redone inside the commit transaction (§6.1 ②)

    auto pumped = co_await pump_body(*data_, body, codec::object_key(bucket, key));
    WritePinRelease wp(write_pins_, pins_.get(), pumped.ref);
    ObjectRec rec;
    rec.meta = std::move(meta);
    rec.meta.key = std::string(key);
    rec.meta.size = pumped.ref.total();
    rec.meta.etag = pumped.md5;
    rec.meta.last_modified = std::chrono::system_clock::now();
    rec.data = pumped.ref;
    // Commit point; the old DataRef enters gcq in the same batch. The condition
    // check completes inside the meta transaction's atomic region; a throw on
    // failure takes the discard path to reclaim already-landed data extents.
    // The cached record is dropped on the way out whatever the outcome (an
    // UndeterminedCommit may have taken effect, roadmap §3.8)
    auto inv = meta_cache_->invalidate_on_exit(bucket, key);
    co_await commit_or_discard(*data_, pumped.ref,
                               [&] { meta_->put_object(bucket, key, std::move(rec), cond); });
    co_return PutResult{pumped.md5};
}

// ---------- tiered local-side hooks (roadmap §3.6 ⑥) ----------

std::optional<ObjectRec> DuoStoreBackend::tier_read(std::string_view bucket,
                                                    std::string_view key) {
    try {
        return meta_->get_object(bucket, key);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchBucket || e.code == S3ErrorCode::NoSuchKey)
            return std::nullopt;
        throw;
    }
}

Task<void> DuoStoreBackend::tier_commit_stub(std::string_view bucket, std::string_view key,
                                             const ObjectMeta& meta, const TierState& ts) {
    co_await pool_->schedule();
    ObjectRec rec;
    rec.meta = meta;
    rec.meta.key = std::string(key);
    rec.tier = ts;  // data stays empty: the meta transaction sends the old extents to the gcq
    PutCondition cond;
    cond.if_match_etag = meta.etag;
    auto inv = meta_cache_->invalidate_on_exit(bucket, key);
    meta_->put_object(bucket, key, std::move(rec), cond);
}

Task<void> DuoStoreBackend::tier_commit_cached(std::string_view bucket, std::string_view key,
                                               http::BodyReader& body, const ObjectMeta& meta,
                                               const TierState& ts) {
    co_await pool_->schedule();
    auto pumped = co_await pump_body(*data_, body, codec::object_key(bucket, key));
    WritePinRelease wp(write_pins_, pins_.get(), pumped.ref);
    if (pumped.ref.total() != meta.size) {  // the fill was verified upstream; defend anyway
        try {
            co_await data_->remove(pumped.ref.extents);
        } catch (...) {
        }
        throw S3Error(S3ErrorCode::InternalError, "tier cache fill size mismatch",
                      std::string(key));
    }
    ObjectRec rec;
    rec.meta = meta;
    rec.meta.key = std::string(key);
    rec.data = pumped.ref;
    rec.tier = ts;
    PutCondition cond;
    cond.if_match_etag = meta.etag;
    auto inv = meta_cache_->invalidate_on_exit(bucket, key);
    co_await commit_or_discard(*data_, pumped.ref,
                               [&] { meta_->put_object(bucket, key, std::move(rec), cond); });
}

namespace {
// Ends a read-clock ticket unless ownership was handed to the PinnedReader
struct TicketGuard {
    std::shared_ptr<ReadClock> clock;
    uint64_t ticket;
    ~TicketGuard() {
        if (clock) clock->end(ticket);
    }
};
}  // namespace

Task<ObjectStream> DuoStoreBackend::get_object(std::string_view bucket, std::string_view key,
                                               std::optional<ByteRange> range) {
    validate_object_key(key);
    co_await pool_->schedule();
    // Register with the read clock BEFORE fetching the manifest (roadmap §3.7):
    // the published "oldest in-flight read" must cover the meta read itself, or
    // a peer's GC could reclaim between our meta fetch and the registration
    TicketGuard ticket{read_clock_, read_clock_->begin()};
    // Cache first (roadmap §3.8): a full entry skips the meta engine; a meta-only
    // entry (left by a HEAD) or a miss goes to the engine and fills/upgrades it. The
    // token predates the meta read so a racing write cannot leave a stale record
    ObjectRec rec;
    if (auto c = meta_cache_->lookup(bucket, key); c && c->manifest) {
        rec = c->rec;
    } else {
        auto tok = meta_cache_->token_for(bucket, key);
        rec = require_object(bucket, key);
        if (rec.data.extents.size() <= kMetaCacheMaxExtents) {
            auto e = std::make_shared<CachedObject>();
            e->rec = rec;
            e->manifest = true;
            meta_cache_->insert(tok, bucket, key, std::move(e));
        }
    }
    // A tiered stub (roadmap §3.6 ⑥): the data lives in the cloud and TieredBackend
    // routes there before asking us; reachable only by direct routing to the local
    // backend or a demotion race — same signal localfs raises for its 0-length stub
    if (rec.tier.tier == TierState::kRemote) throw fsutil::StubRace(std::string(key));

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
        // Pin before opening the reader (§7): the micro-window between the meta
        // read and the pin is covered by gc_grace.
        // Only pin extents hit by [first,last] — a Range GET does not pay for the
        // whole object's manifest.
        // On open_reader failure the pins must be released (co_await cannot go in
        // a catch, moved out via exception_ptr)
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
        out.body = std::make_unique<PinnedReader>(std::move(inner), pins_, std::move(ids),
                                                  read_clock_, ticket.ticket);
        ticket.clock.reset();  // ownership handed to the reader
    }
    co_return out;
}

Task<std::optional<ObjectLayout>> DuoStoreBackend::inspect_object(std::string_view bucket,
                                                                  std::string_view key) {
    validate_object_key(key);
    co_await pool_->schedule();
    auto rec = meta_->get_object(bucket, key);  // authoritative: bypasses the cache on purpose
    if (!rec) {
        require_bucket(bucket);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }
    ObjectLayout L;
    L.engine = "duostore";
    auto& a = L.attrs;
    a.emplace_back("logical_size", std::to_string(rec->meta.size));
    a.emplace_back("etag", rec->meta.etag);
    a.emplace_back("content_type", rec->meta.content_type);
    a.emplace_back("last_modified", util::iso8601(rec->meta.last_modified));
    a.emplace_back("meta_version", std::to_string(rec->version));
    a.emplace_back("tier", rec->tier.tier == TierState::kLocal    ? "local"
                           : rec->tier.tier == TierState::kRemote ? "remote"
                                                                  : "cached");
    if (rec->tier.tier != TierState::kLocal) {
        a.emplace_back("remote_etag", rec->tier.remote_etag);
        a.emplace_back("remote_at", rec->tier.remote_at);
    }
    a.emplace_back("extents", std::to_string(rec->data.extents.size()));
    a.emplace_back("stored_bytes", std::to_string(rec->data.total()));
    for (auto& e : rec->data.extents) {
        const char* kind = e.kind == Extent::Kind::kChunk  ? "chunk"
                           : e.kind == Extent::Kind::kPack ? "pack"
                                                           : "rados";
        L.extents.push_back({kind, e.file_id, e.offset, e.length, e.crc32c});
    }
    co_return L;
}

Task<ObjectMeta> DuoStoreBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    co_await pool_->schedule();
    // Cache first (roadmap §3.8): either entry kind answers a HEAD
    ObjectRecCache::Token tok;
    if (auto c = meta_cache_->lookup(bucket, key, &tok)) co_return c->rec.meta;
    // meta-only read (docs/archive/gaps.md §3.9): HEAD does not pay for the whole manifest
    auto meta = meta_->head_object(bucket, key);
    if (!meta) {
        require_bucket(bucket);  // distinguish NoSuchBucket / NoSuchKey
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }
    auto e = std::make_shared<CachedObject>();
    e->rec.meta = *meta;
    meta_cache_->insert(tok, bucket, key, std::move(e));
    co_return std::move(*meta);
}

Task<void> DuoStoreBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    co_await pool_->schedule();
    auto inv = meta_cache_->invalidate_on_exit(bucket, key);
    meta_->delete_object(bucket, key);  // idempotent (returns false if absent); physical reclamation realized asynchronously by GC (§6.2)
}

Task<ListResult> DuoStoreBackend::list_objects(std::string_view bucket,
                                               const ListOptions& opt) {
    co_await pool_->schedule();
    co_return meta_->list_objects(bucket, opt);
}

// ---------- multipart (§8) ----------

Task<std::string> DuoStoreBackend::create_multipart(std::string_view bucket,
                                                    std::string_view key, ObjectMeta meta) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    co_await pool_->schedule();
    co_return meta_->create_upload(bucket, key, std::move(meta));
}

Task<PutResult> DuoStoreBackend::upload_part(std::string_view bucket, std::string_view key,
                                             std::string_view upload_id, int part_no,
                                             http::BodyReader& body,
                                             const std::optional<PartChecksum>& checksum) {
    validate_part_number(part_no);
    validate_object_key(key);  // the key enters the '\0'-separated encoding (§4.1), so the multipart entry validates too
    co_await pool_->schedule();
    meta_->require_upload(bucket, key, upload_id);  // precheck; redone at commit

    // The mpu owner carries b/k (P4 §9.2): after complete, the part record's
    // owning object can be reverse-looked-up from it, so compaction does not lose
    // the hint when the upload goes away (the pre-P4 legacy format "mpu\0id\0no"
    // cannot be reverse-looked-up; on encountering it, conservatively do not migrate)
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
    if (checksum) {  // resolved() only after pump_body drained the body (trailer form)
        p.checksum_algorithm = checksum->algorithm;
        p.checksum_value = checksum->resolved();
    }
    // The upload may have been aborted while reading the body → a commit failure triggers fallback data deletion
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
    // Pure metadata transaction, zero data movement: O(#parts) vs localfs concatenation's O(total bytes) (§8)
    auto inv = meta_cache_->invalidate_on_exit(bucket, key);
    PutResult r{meta_->complete_upload(bucket, key, upload_id, parts)};
    // Composite checksum echo (roadmap §2.2): assemble persisted it with the object;
    // one meta read fetches it back for the response
    if (auto m = meta_->head_object(bucket, key); m && !m->checksum_value.empty()) {
        r.checksum_algorithm = m->checksum_algorithm;
        r.checksum_value = m->checksum_value;
        r.checksum_type = m->checksum_type;
    }
    co_return r;
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
    // Part count is protocol-capped at 10000, so full materialization is bounded; pagination trims only here
    std::vector<PartMeta> all;
    for (const auto& p : meta_->list_parts(bucket, key, upload_id))
        all.push_back({p.part_no, p.size, p.etag, codec::from_unix_ms(p.modified_ms),
                       p.checksum_algorithm, p.checksum_value});
    co_return apply_parts_page(std::move(all), opt);
}

Task<ListUploadsResult> DuoStoreBackend::list_multipart_uploads(std::string_view bucket,
                                                                const ListUploadsOptions& opt) {
    co_await pool_->schedule();
    // Uploads per bucket are unbounded, so the cursor and count are pushed down to
    // the engine (sqlite/rocks/tikv truly skip, redis can only full-scan). With a
    // non-empty delimiter the count cannot be limited: grouping needs the full
    // picture to judge truncation, and limiting would misreport "there is more"
    // as end-of-list
    int limit = opt.delimiter.empty() && opt.max_uploads > 0 ? opt.max_uploads + 1 : 0;
    co_return apply_uploads_page(
        meta_->list_uploads(bucket, opt.key_marker, opt.upload_id_marker, limit, opt.prefix),
        opt);
}

// ---------- GC phase one (§9/§9.1) ----------

namespace {

constexpr size_t kGcBatch = 256;  // per-round peek batch; advance after ack between batches, preventing one batch of a large backlog from blowing memory
// Cumulative per-batch extent cap (gaps §2.11): count-based batching under a
// "deleted TB-scale objects" ledger can reach GB-scale residency per batch — the
// enqueue side already splits by kReclaimMaxExtents; this covers legacy entries
// from before the split
constexpr size_t kGcBatchExtents = 32768;

}  // namespace

Task<DuoGcStats> DuoStoreBackend::run_gc_once() {
    co_await pool_->schedule();
    // Register as in-flight: close() waits via bg_.wait_idle() for this round to
    // end before tearing down meta_/data_ — manual hooks and the background
    // worker share one ledger, so there is no "check closed_ only once" TOCTOU window
    BackgroundTaskGroup::Scope scope(bg_);
    DuoGcStats st;
    if (!scope.ok()) co_return st;                 // shutting down
    auto permit = co_await gc_sem_.acquire();      // manual hook vs background worker mutual exclusion
    // Multi-gateway lease (§6.1): gc_enabled is only a convention; two
    // misconfigured instances both running GC would unlink each other's
    // determined-empty packs. Shared meta (redis/tikv) claims atomically here;
    // local engines always return true
    const int64_t lease_ttl_ms =
        std::max<int64_t>(2 * int64_t(cfg_.gc_interval_sec), 600) * 1000;
    if (!meta_->try_gc_lease(gc_owner_, lease_ttl_ms)) {
        LOG_INFO("duostore '{}': GC lease held by another instance, skipping round", cfg_.name);
        co_return st;
    }
    // Multi-gateway read-lease floor (roadmap §3.7): only reclaim what no peer's
    // in-flight read can reference — a reader holding a ref to reclaimed extents
    // must have fetched the manifest before the deref enqueued them, so an entry
    // enqueued before every in-flight read started is provably unreachable.
    // Errors fall back to the grace-only behavior (the documented gc_grace
    // premise), never to stalling reclamation
    std::optional<int64_t> lease_floor;
    if (cfg_.read_lease_sec > 0) {
        try {
            lease_floor = meta_->min_read_lease();
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': min_read_lease failed ({}); this round is gated by "
                     "gc_grace only", cfg_.name, e.what());
        }
    }
    const auto round_start = std::chrono::steady_clock::now();

    // 1) mpu_ttl-expired multipart cleanup (end of §8): internal abort, parts
    // enter gcq to be realized in the next step. <=0 = disabled (aligned with
    // gc_interval's 0 semantics — interpreting 0 as "expire immediately" would
    // silently abort every in-flight multipart, a configuration footgun)
    const int64_t ttl_ms = int64_t(cfg_.mpu_ttl_sec) * 1000;
    if (ttl_ms > 0) {
        const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
        // list_buckets/list_uploads must also be inside the try: racing a
        // concurrent DeleteBucket, list_uploads throws NoSuchBucket; letting it
        // escape would skip this round's gcq consumption/compaction/orphan scan
        // (steps 2-4) entirely, needlessly deferring reclamation by one gc_interval
        try {
            for (const auto& bk : meta_->list_buckets()) {
                std::vector<UploadInfo> uploads;
                try {
                    uploads = meta_->list_uploads(bk.name);
                } catch (const std::exception& e) {
                    LOG_WARN("duostore '{}': gc list uploads of bucket {} failed: {}",
                             cfg_.name, bk.name, e.what());
                    continue;  // bucket deleted/temporarily unreadable: skip it without affecting the other steps
                }
                for (const auto& u : uploads) {
                    if (now - codec::to_unix_ms(u.initiated) < ttl_ms) continue;
                    try {
                        meta_->abort_upload(bk.name, u.key, u.upload_id);
                        ++st.uploads_expired;
                    } catch (const std::exception& e) {
                        // Losing to a concurrent complete/abort with NoSuchUpload is normal; anything else logs WARN and retries next round
                        LOG_WARN("duostore '{}': gc abort expired upload {} failed: {}",
                                 cfg_.name, u.upload_id, e.what());
                    }
                }
            }
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': gc expired-upload sweep failed: {}", cfg_.name, e.what());
        }
    }

    // 2) gcq consumption (§9.1): for entries beyond gc_grace and unpinned,
    // physically delete first, settle after — the reverse order crashing between
    // delete and settle would produce permanently orphaned off-ledger files;
    // crashing in this order only leaves gcq residue, and the retried unlink is
    // idempotent. Resume by next_seq: queue-head entries skipped for grace/pin are
    // not rescanned (no stuck rounds, no double counting, no second decode);
    // reaching the queue tail ends the round.
    // Cross-round watermark (gaps §2.13): in rounds where none of the previous
    // round's skips have reached their retry time, scan from the previous high
    // watermark — queue-head grace backlog no longer gets re-peeked+decoded every round
    const int64_t grace_ms = int64_t(cfg_.gc_grace_sec) * 1000;
    const int64_t round_now = codec::to_unix_ms(std::chrono::system_clock::now());
    uint64_t next_seq = 0;
    if (gcq_skips_.any)
        next_seq = round_now < gcq_skips_.retry_at_ms ? gcq_hi_ : gcq_skips_.lo_seq;
    const bool full_scan = !gcq_skips_.any || next_seq == gcq_skips_.lo_seq;
    GcqSkips skips;  // skips newly added this round
    auto note_skip = [&skips](uint64_t seq, int64_t retry_at) {
        if (!skips.any || seq < skips.lo_seq) skips.lo_seq = seq;
        if (!skips.any || retry_at < skips.retry_at_ms) skips.retry_at_ms = retry_at;
        skips.any = true;
    };
    // Queue depth and head age (§6.1): a full round's observation is the whole queue; incremental rounds see only newly enqueued entries
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
        // Take a fresh timestamp per batch: entries just enqueued by the previous
        // step's abort have enqueue_ms later than this function's entry time;
        // judging grace with the entry time would compute a negative difference
        // and wrongly skip (grace=0 should be immediately reclaimable)
        const int64_t batch_now = codec::to_unix_ms(std::chrono::system_clock::now());
        for (const auto& [seq, rc] : batch) {
            if (batch_now - rc.enqueue_ms < grace_ms) {
                ++st.skipped_grace;
                note_skip(seq, rc.enqueue_ms + grace_ms);  // deterministic lower bound
                continue;
            }
            // A peer gateway's in-flight read started before this entry was
            // enqueued could still hold the old ref (roadmap §3.7)
            if (lease_floor && rc.enqueue_ms >= *lease_floor) {
                ++st.skipped_leased;
                note_skip(seq, batch_now);  // when the peer read finishes is unknowable
                continue;
            }
            if (pins_->any_pinned(rc.extents)) {
                ++st.skipped_pinned;
                note_skip(seq, batch_now);  // when the pin releases is unknown: retry next round
                continue;
            }
            try {
                // chunk/rados extents get a physical unlink; pack records become
                // dead regions reclaimed by compaction (the data-side remove skips
                // them internally), and the liveness account was already debited in
                // the business transaction → settle directly
                co_await data_->remove(rc.extents);
            } catch (const std::exception& e) {
                LOG_WARN("duostore '{}': gc remove (seq {}) failed: {}", cfg_.name, seq,
                         e.what());
                note_skip(seq, batch_now);  // do not settle; the gcq residue retries next round
                continue;
            }
            for (const auto& e : rc.extents)
                if (e.kind != Extent::Kind::kPack) ++st.files_removed;
            if (auto& c = m_gc_reclaims_by_reason_[size_t(rc.reason) < 6 ? size_t(rc.reason) : 0])
                c->inc();
            acked.push_back(seq);
        }
        if (!acked.empty()) {
            meta_->ack_reclaims(acked);  // batch settle (one transaction/batch; cost argument in the interface comment)
            st.reclaims_acked += acked.size();
        }
    }
    gcq_hi_ = std::max(gcq_hi_, next_seq);
    if (full_scan) {
        gcq_skips_ = skips;  // full round: the watermark is rebuilt wholesale from this round's observation
        // Depth and head age also refresh only on full rounds: incremental rounds
        // scan from the previous high watermark and cannot see queue-head backlog;
        // updating from them would periodically misreport the depth as 0
        m_gcq_depth_->set(int64_t(seen_entries));
        m_gcq_oldest_age_->set(oldest_enqueue_ms == 0
                                   ? 0
                                   : std::max<int64_t>(0, (round_now - oldest_enqueue_ms) / 1000));
    } else if (skips.any) {
        // Incremental round: merge with the existing watermark (old skips still sit unvisited at the queue head)
        gcq_skips_.lo_seq = std::min(gcq_skips_.lo_seq, skips.lo_seq);
        gcq_skips_.retry_at_ms = std::min(gcq_skips_.retry_at_ms, skips.retry_at_ms);
    }

    // 2.5) Age-based sealing of active packs (§6.1): with capacity-only sealing
    // under low write volume, an active pack never rotates and its
    // overwritten/deleted records cannot enter the compaction candidate set
    // below. Placed before compaction so packs sealed this round can be evaluated
    // this round
    if (cfg_.pack_max_age_sec > 0) {
        try {
            st.packs_sealed_aged =
                co_await data_->seal_aged_packs(int64_t(cfg_.pack_max_age_sec) * 1000);
        } catch (const std::exception& e) {
            LOG_WARN("duostore '{}': gc seal aged packs failed: {}", cfg_.name, e.what());
        }
    }

    // 3) Pack compaction (P4 §9.2): packs that are sealed, live>0, with liveness
    // below pack_gc_ratio (or crash-leftover unknown file_size) get a sequential
    // scan migrating live records; once the live account hits zero via swaps, step
    // 4 realizes the whole deletion. Packs whose live_recs made no progress since
    // the last compaction (in-flight mpu parts / legacy-format owner / live
    // corrupt records) skip rescanning — any account change retries automatically.
    // Budget and priority (§6.1): first collect all candidates with their
    // reclaimable bytes, sort by yield descending, then truncate by "at most N
    // this round / cumulative M bytes scanned". Previously it was "one round
    // rewrites everything eligible"; after a bulk delete a single GC round could
    // hold the lock for hours and contend with business writes for pack slots.
    // The rest continue next round, and doing the highest-yield first ensures
    // space falls back as quickly as possible
    struct CompactCand {
        uint64_t pack_id = 0;
        uint64_t file_size = 0;   // 0 = unknown (stat unsupported and crash-leftover seal(0))
        int64_t reclaimable = 0;  // file_size - live_bytes; always 0 for unknown size
    };
    std::vector<CompactCand> cands;
    const int64_t compact_now = codec::to_unix_ms(std::chrono::system_clock::now());
    for (auto ps : meta_->pack_stats()) {
        if (!ps.sealed || ps.live_recs <= 0) continue;
        // Backfill the denominator of crash-leftover seal(0) first (gaps §2.3b):
        // one stat suffices, so every ungraceful exit does not unconditionally
        // push all active packs into full sequential-scan rewrites
        if (ps.file_size == 0) {
            if (uint64_t sz = data_->stat_pack(ps.pack_id); sz > 0) {
                meta_->seal_pack(ps.pack_id, sz);
                ps.file_size = sz;
            }
        }
        int64_t reclaimable = 0;
        if (ps.file_size > 0) {
            // live includes record headers, same basis as file_size (gaps §2.3a —
            // counting only payload, a pack of small objects stays below the
            // threshold even at 100% liveness and compaction never converges).
            // Skip when there are no reclaimable bytes (live ≥ file_size,
            // including the slight-undercount tolerance direction) or liveness is
            // above the threshold
            reclaimable = int64_t(ps.file_size) - ps.live_bytes;
            if (reclaimable <= 0 ||
                double(ps.live_bytes) > cfg_.pack_gc_ratio * double(ps.file_size))
                continue;
        }
        // Quarantined packs are parked (roadmap §3.7): no cooldown rescans until
        // the operator releases them or the live account moves (real progress —
        // deletes/mpu resolution killed records; purged entries stay parked, the
        // file is gone). live==0 needs no exception here: step 4's whole
        // deletion runs regardless and drops the ledger entry with the packstat
        {
            std::lock_guard ql(q_mu_);
            if (auto qit = quarantined_.find(ps.pack_id); qit != quarantined_.end()) {
                if (!qit->second.purged && ps.live_recs != qit->second.live_recs) {
                    LOG_INFO("duostore '{}': pack {:016x} auto-released from quarantine "
                             "(live account moved {} -> {})", cfg_.name, ps.pack_id,
                             qit->second.live_recs, ps.live_recs);
                    quarantine_drop(ps.pack_id);
                } else {
                    continue;
                }
            }
        }
        if (auto it = compact_blocked_.find(ps.pack_id);
            it != compact_blocked_.end() && it->second.live_recs == ps.live_recs &&
            compact_now < it->second.retry_at_ms)
            continue;
        cands.push_back({ps.pack_id, ps.file_size, reclaimable});
    }
    // Yield descending; unknown sizes go last (yield cannot be estimated, and they
    // only appear as crash leftovers). Ties are ordered by pack_id, so adjacent
    // rounds do not oscillate among the same candidates due to unstable sorting
    std::sort(cands.begin(), cands.end(), [](const CompactCand& a, const CompactCand& b) {
        if ((a.file_size == 0) != (b.file_size == 0)) return b.file_size == 0;
        if (a.reclaimable != b.reclaimable) return a.reclaimable > b.reclaimable;
        return a.pack_id < b.pack_id;
    });

    std::vector<uint64_t> rewritten;
    // Per-pack scan outcome for the quarantine strike accounting below (roadmap §3.7)
    std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> rw_by_pack;  // migrated, corrupt
    uint64_t scanned_bytes = 0;
    for (size_t ci = 0; ci < cands.size(); ++ci) {
        const auto& cd = cands[ci];
        const bool over_count = cfg_.gc_compact_max_packs > 0 &&
                                ci >= size_t(cfg_.gc_compact_max_packs);
        // The byte budget counts "pack sizes already scanned", and the first
        // candidate always passes — if a single pack larger than the whole round's
        // budget were always blocked, compaction would never make progress
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
            rw_by_pack[cd.pack_id] = {rw.migrated, rw.corrupt};
            // Engines without stat_pack support (returning 0): backfill from the file_size the sequential scan reports (§9.2)
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
    // Migrated records now carry new refs; cached manifests naming the old pack regions
    // stay readable until the emptied pack is unlinked in a later round (beyond
    // gc_grace), but must not outlive it. Compaction is rare and batched, so dropping
    // the whole cache is cheaper than plumbing per-object callbacks through the
    // migration hook (roadmap §3.8)
    if (st.records_migrated > 0) meta_cache_->clear();

    // 4) Whole empty-pack deletion (§9.1/§9.2 step 4): sealed with live_recs==0,
    // empty for longer than gc_grace (delayed unlink: serving readers who read
    // the old ref at the instant of compaction/deletion without having pinned
    // yet) and unpinned → unlink the whole file → drop the packstat (same iron
    // ordering as gcq: physical delete first, settle after)
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
            // Lease gate (roadmap §3.7): a peer's read that started before the
            // pack was first seen empty may hold a pre-swap manifest still
            // pointing into it; one that started after cannot (it read the
            // post-swap manifests)
            if (pack_now - it->second < grace_ms ||
                (lease_floor && it->second >= *lease_floor) || pins_->pinned_pack(ps.pack_id))
                continue;
            try {
                co_await data_->remove_pack(ps.pack_id);
                meta_->drop_pack_stat(ps.pack_id);
                pack_empty_since_.erase(ps.pack_id);
                ++st.packs_removed;
                // A quarantined pack whose live account drained to zero is fully
                // settled — retire the ledger entry with the packstat
                {
                    std::lock_guard ql(q_mu_);
                    if (quarantined_.count(ps.pack_id)) quarantine_drop(ps.pack_id);
                }
            } catch (const std::exception& e) {
                LOG_WARN("duostore '{}': gc remove pack {} failed: {}", cfg_.name, ps.pack_id,
                         e.what());
            }
        }
        // Compaction-blocked accounting: live records remaining after migration
        // means this round couldn't move everything → record the current tally
        // plus a cooldown window
        for (uint64_t pid : rewritten) {
            int64_t live = 0;
            for (const auto& ps : stats)
                if (ps.pack_id == pid) {
                    live = ps.live_recs;
                    break;
                }
            if (live > 0) {
                // Quarantine strikes (roadmap §3.7): corrupt records with zero
                // migration progress and an unmoved account, kQuarantineStrikes
                // scans in a row, prove the cooldown loop cannot converge —
                // park the pack instead of rescanning it forever
                auto [migrated, corrupt] = rw_by_pack[pid];
                int strikes = 0;
                if (corrupt > 0 && migrated == 0) {
                    auto prev = compact_blocked_.find(pid);
                    strikes = (prev != compact_blocked_.end() && prev->second.live_recs == live)
                                  ? prev->second.strikes + 1
                                  : 1;
                }
                if (strikes >= kQuarantineStrikes) {
                    DuoQuarantineEntry qe{pid, live, corrupt, pack_now, /*purged=*/false};
                    {
                        std::lock_guard ql(q_mu_);
                        quarantined_[pid] = qe;
                        quarantine_save(qe);
                        m_packs_quarantined_->set(int64_t(quarantined_.size()));
                    }
                    compact_blocked_.erase(pid);
                    ++st.packs_quarantined;
                    LOG_ERROR("duostore '{}': pack {:016x} quarantined — {} scan(s) found {} "
                              "corrupt record(s) and migrated nothing ({} live record(s) "
                              "unreclaimable). Inspect with `lights3 duostore quarantine "
                              "list`; `release` after restoring the file from backup, or "
                              "`purge` to reclaim the disk space and accept the loss",
                              cfg_.name, pid, kQuarantineStrikes, corrupt, live);
                } else {
                    compact_blocked_[pid] = {live, pack_now + grace_ms, strikes};
                }
            } else {
                compact_blocked_.erase(pid);
            }
        }
        // Prune packs whose accounting entries are gone so the two bookkeeping
        // maps don't grow without bound over history
        std::erase_if(pack_empty_since_, [&](const auto& kv) { return !known.count(kv.first); });
        std::erase_if(compact_blocked_, [&](const auto& kv) { return !known.count(kv.first); });

        // Space amplification (§6.1): the accounted/live ratio. `stats` is the
        // snapshot taken at the start of step 4, so whole-pack deletions from
        // this round are already gone from disk but still present in the
        // snapshot — a one-round lag, harmless for trend observation
        int64_t accounted = 0, live = 0;
        for (const auto& ps : stats) {
            accounted += int64_t(ps.file_size);
            live += ps.live_bytes;
        }
        m_pack_accounted_bytes_->set(accounted);
        m_pack_live_bytes_->set(live);
        m_packs_total_->set(int64_t(stats.size()));
    }

    // Only completed rounds are counted (early exits during shutdown are not);
    // the skip metrics are gauges rather than counters — grace/pin skips are
    // re-counted on every rescan, so a monotonic counter would inflate
    // misleadingly, and "how much is still left unreclaimed this round" is what
    // operators actually want to see
    m_gc_skipped_grace_->set(int64_t(st.skipped_grace));
    m_gc_skipped_pinned_->set(int64_t(st.skipped_pinned));
    m_gc_skipped_leased_->set(int64_t(st.skipped_leased));
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
    if (!scope.ok()) co_return st;                 // shutting down
    auto permit = co_await gc_sem_.acquire();      // mutual exclusion with GC/background
                                                   // workers (prerequisite for the reverse
                                                   // reconciliation)
    // Same lease as GC (§6.1): the orphan scan's unlinks likewise must not run
    // concurrently with another gateway's GC
    const int64_t lease_ttl_ms =
        std::max<int64_t>(2 * int64_t(cfg_.gc_interval_sec), 600) * 1000;
    if (!meta_->try_gc_lease(gc_owner_, lease_ttl_ms)) {
        LOG_INFO("duostore '{}': GC lease held by another instance, skipping orphan scan",
                 cfg_.name);
        co_return st;
    }

    // Refs snapshot comes first: the reverse reconciliation's invariant "files
    // must exist on disk before their ref commits" (§6 data-first) requires
    // collecting R before the on-disk enumeration — for every id in R the file
    // was already on disk before enumeration began, so a miss is a real loss.
    // Holding gc_sem_ excludes gcq's unlink→settle window, and no business path
    // ever deletes a file that still has refs.
    // Note: refs make no kChunk/kRados distinction (shared id-range accounting);
    // this scan treats the current data engine's enumeration as the on-disk
    // truth — a deployment that switches data engines under the same meta is
    // outside the orphan scan's supported scope.
    // Memory footprint (docs/archive/gaps.md §3.9): previously the refs/on_disk hash
    // sets plus a full disk vector were resident simultaneously (100M chunks
    // ≈ 4–5GB); replaced by a sorted refs vector (8B/entry) + streaming on-disk
    // classification + a hit bitmap (1 bit/entry), cutting the peak by an order
    // of magnitude
    std::vector<uint64_t> refs;
    meta_->scan_refs([&](uint64_t id) { refs.push_back(id); });
    std::sort(refs.begin(), refs.end());
    refs.erase(std::unique(refs.begin(), refs.end()), refs.end());
    std::vector<bool> ref_seen(refs.size(), false);  // reverse reconciliation: on-disk hit marks

    // Chunks with a pending gcq entry are left to the gcq consumer (roadmap
    // §3.7): every once-referenced chunk enters the gcq atomically with its
    // deref, and the gcq path is the one gated by the peer read lease —
    // unlinking such a chunk here would bypass that gate for a remote in-flight
    // reader. What stays eligible below was never referenced (crash leftovers),
    // which no reader can hold. Stable under gc_sem_ (no concurrent local
    // consumption; new business enqueues only add entries for chunks whose refs
    // still existed at the snapshot above)
    std::unordered_set<uint64_t> gcq_pending;
    for (uint64_t seq = 0;;) {
        auto batch = meta_->peek_reclaims(kGcBatch, seq, kGcBatchExtents);
        if (batch.empty()) break;
        seq = batch.back().first + 1;
        for (const auto& [s, rc] : batch)
            for (const auto& e : rc.extents)
                if (e.kind != Extent::Kind::kPack) gcq_pending.insert(e.file_id);
    }

    // Forward pass (§9.3) classification inlined into the enumeration callback:
    // no reference, mtime older than gc_grace, and no pin (the write-side pin
    // covers very long streaming PUTs, for which the mtime grace alone is
    // insufficient) → record as a candidate. `now` is taken at scan start, so
    // the grace window can only err on the strict (safe) side; references
    // committed after the snapshot are still caught by the point-in-time
    // chunk_referenced recheck before deletion (with grace=0 in test setups the
    // snapshot is necessarily stale)
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
        if (gcq_pending.count(id)) {
            ++st.skipped_gcq;  // deref'd, pending reclaim: the gcq path owns it (lease-gated)
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
        if (meta_->chunk_referenced(id)) continue;  // new reference committed in the scan gap
        // kind follows the data engine (C4): RadosDataStore::remove only
        // accepts kRados (foreign-kind extents are skipped as engine-switch
        // leftovers), so hardcoding kChunk would make rados orphan deletion
        // silently a no-op
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

    // Reverse pass (§9.3): a ref present with the file missing is a data-loss
    // signal → alert and count, never silently delete the meta
    for (size_t i = 0; i < refs.size(); ++i) {
        if (ref_seen[i]) continue;
        ++st.refs_missing;
        LOG_ERROR("duostore '{}': refs entry for chunk {:016x} but file missing "
                  "(data loss signal, keeping meta for manual inspection)", cfg_.name, refs[i]);
    }

    // packs/ two-way reconciliation (docs/archive/gaps.md §6.1): the chunk-side orphan
    // scan previously did not cover pack entities, yet a pack file is "create
    // the file first, write the packstat row only when the first record
    // commits" — a hard crash exactly in that window leaves the file on disk
    // with no accounting row at all: never reclaimed and never observable.
    // The liveness test for packs is stricter than for chunks: unaccounted ⇒
    // candidate, but it must still pass three gates —
    //   1) mtime older than gc_grace (a freshly created active pack whose first
    //      record hasn't committed yet)
    //   2) no pin (in-flight readers)
    //   3) no write lock (an active pack of this process or another instance;
    //      flock is the only reliable signal of "a live writer exists", the
    //      same criterion abandon_stale_packs uses)
    // The reverse case — packstat present but file missing — only alerts and
    // never drops the accounting, a data-loss signal just like refs_missing
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
            // Write-lock probing stays outside the enumeration: it opens an fd
            // and takes flock, which shouldn't be done per entry inside the
            // enumeration callback
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
    // Usage (§6.1): bytes measured from disk, refreshed on the orphan-scan
    // cadence (default once per day)
    m_bytes_chunks_->set(int64_t(st.chunk_bytes));
    m_bytes_packs_->set(int64_t(st.pack_bytes));
    co_return st;
}

Task<bool> DuoStoreBackend::scrub_manifest(
    const duostore::DataRef& ref, const std::string& what,
    const std::vector<uint64_t>& refs_snapshot, std::vector<bool>& ref_seen,
    const std::function<std::optional<duostore::DataRef>()>& refetch, ScrubThrottle& throttle,
    std::vector<std::byte>& buf, duostore::DuoScrubStats& st) {
    bool bad = false;
    for (const auto& e : ref.extents) {
        if (bg_.closing()) {
            st.aborted = true;
            co_return bad;
        }
        ++st.extents_checked;
        if (e.kind != Extent::Kind::kPack) {
            // Refs-ledger presence (pack extents are tracked by packstat, not
            // refs). A miss against the snapshot is rechecked point-in-time,
            // then against a refreshed manifest: a concurrent overwrite/delete
            // legitimately removed the ref, only an unchanged manifest with the
            // ref still gone is a genuine hole
            auto it = std::lower_bound(refs_snapshot.begin(), refs_snapshot.end(), e.file_id);
            if (it != refs_snapshot.end() && *it == e.file_id) {
                ref_seen[size_t(it - refs_snapshot.begin())] = true;
            } else if (!meta_->chunk_referenced(e.file_id)) {
                auto cur = refetch();
                if (cur && cur->extents == ref.extents) {
                    ++st.refs_missing;
                    bad = true;
                    LOG_ERROR("duostore '{}': scrub {}: chunk {:016x} referenced by the "
                              "manifest but absent from the refs ledger — the orphan scan "
                              "could unlink live data",
                              cfg_.name, what, e.file_id);
                }
            }
        }
        if (e.length == 0) continue;
        uint32_t crc = 0;
        uint64_t got = 0;
        bool read_ok = true;
        try {
            DataRef one;
            one.extents.push_back(e);
            auto reader = co_await data_->open_reader(one, 0, e.length - 1);
            for (;;) {
                size_t n = co_await reader->read(std::span(buf));
                if (n == 0) break;
                crc = codec::crc32c_update(crc, std::span(buf.data(), n));
                got += n;
                st.bytes_read += n;
                co_await throttle.pace(n);
            }
        } catch (const std::exception& ex) {
            read_ok = false;
            ++st.unreadable_extents;
            bad = true;
            LOG_ERROR("duostore '{}': scrub {}: extent kind={} {:016x}+{} len={} unreadable: {}",
                      cfg_.name, what, int(e.kind), e.file_id, e.offset, e.length, ex.what());
        }
        if (read_ok) {
            if (got != e.length) {
                ++st.unreadable_extents;
                bad = true;
                LOG_ERROR("duostore '{}': scrub {}: extent {:016x} short read ({} of {} bytes)",
                          cfg_.name, what, e.file_id, got, e.length);
            } else if (crc != e.crc32c) {
                ++st.corrupt_extents;
                bad = true;
                LOG_ERROR("duostore '{}': scrub {}: extent kind={} {:016x}+{} len={} crc32c "
                          "mismatch (manifest {:08x}, read {:08x})",
                          cfg_.name, what, int(e.kind), e.file_id, e.offset, e.length, e.crc32c,
                          crc);
            }
        }
    }
    co_return bad;
}

Task<duostore::DuoScrubStats> DuoStoreBackend::run_scrub_once(duostore::DuoScrubOptions opt) {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    DuoScrubStats st;
    if (!scope.ok()) {
        st.aborted = true;
        co_return st;
    }
    auto permit = co_await gc_sem_.acquire();  // our GC/orphan scan stands still: nothing
                                               // gets unlinked while manifests are read back
    // Renew the lease best-effort so a peer gateway's GC defers too. Unlike the
    // orphan scan the round is not skipped on failure — the scrub deletes
    // nothing; a racing peer GC can at worst surface overwritten-and-reclaimed
    // extents as spurious unreadable reports
    const int64_t lease_ttl_ms = std::max<int64_t>(2 * int64_t(cfg_.gc_interval_sec), 600) * 1000;
    if (!meta_->try_gc_lease(gc_owner_, lease_ttl_ms))
        LOG_WARN("duostore '{}': scrub: GC lease held by another instance; scrubbing anyway "
                 "(a racing peer GC may cause spurious unreadable reports)",
                 cfg_.name);

    // Refs snapshot before the walk (same shape as the orphan scan): ids
    // committed later are simply never judged, and every unseen leftover is
    // point-in-time rechecked before being reported
    std::vector<uint64_t> refs;
    meta_->scan_refs([&](uint64_t id) { refs.push_back(id); });
    std::sort(refs.begin(), refs.end());
    refs.erase(std::unique(refs.begin(), refs.end()), refs.end());
    std::vector<bool> ref_seen(refs.size(), false);

    ScrubThrottle throttle(opt.max_bytes_per_sec, pool_, [this] { return bg_.closing(); });
    std::vector<std::byte> buf(256 << 10);

    std::vector<BucketInfo> buckets = meta_->list_buckets();
    for (const auto& b : buckets) {
        if (st.aborted || bg_.closing()) {
            st.aborted = true;
            break;
        }
        try {
            ListOptions lo;
            lo.max_keys = 1000;
            for (;;) {
                auto page = meta_->list_objects(b.name, lo);
                for (const auto& om : page.objects) {
                    if (bg_.closing()) {
                        st.aborted = true;
                        break;
                    }
                    auto rec = meta_->get_object(b.name, om.key);
                    if (!rec) continue;  // deleted mid-walk
                    auto refetch = [&]() -> std::optional<DataRef> {
                        auto cur = meta_->get_object(b.name, om.key);
                        if (!cur) return std::nullopt;
                        return std::move(cur->data);
                    };
                    bool bad = co_await scrub_manifest(rec->data, b.name + "/" + om.key, refs,
                                                       ref_seen, refetch, throttle, buf, st);
                    ++st.objects_scanned;
                    if (bad) ++st.objects_bad;
                }
                if (st.aborted || !page.is_truncated) break;
                lo.start_after = page.next_token;
            }
            // In-flight multipart parts hold refs and live data too; a scrub
            // that skipped them would misreport their refs as stale
            for (const auto& up : meta_->list_uploads(b.name)) {
                if (st.aborted || bg_.closing()) {
                    st.aborted = true;
                    break;
                }
                std::vector<PartRec> parts;
                try {
                    parts = meta_->list_parts(b.name, up.key, up.upload_id);
                } catch (const S3Error&) {
                    continue;  // completed/aborted mid-walk
                }
                for (const auto& p : parts) {
                    auto refetch = [&]() -> std::optional<DataRef> {
                        try {
                            for (auto& q : meta_->list_parts(b.name, up.key, up.upload_id))
                                if (q.part_no == p.part_no) return std::move(q.data);
                        } catch (const S3Error&) {
                        }
                        return std::nullopt;
                    };
                    std::string what = b.name + "/" + up.key + " upload " + up.upload_id +
                                       " part " + std::to_string(p.part_no);
                    bool bad = co_await scrub_manifest(p.data, what, refs, ref_seen, refetch,
                                                       throttle, buf, st);
                    ++st.parts_scanned;
                    if (bad) ++st.objects_bad;
                }
            }
        } catch (const std::exception& ex) {
            ++st.meta_errors;
            LOG_ERROR("duostore '{}': scrub: bucket '{}' enumeration failed: {}", cfg_.name,
                      b.name, ex.what());
        }
    }

    // Reverse reconciliation: a refs entry no manifest referenced is a leak
    // suspect — its chunk is protected from the orphan scan forever. Recheck
    // point-in-time first (deleted mid-walk is normal); what remains can still
    // be an MPU that completed after its bucket page was walked, hence the
    // re-run advice rather than an ERROR
    if (!st.aborted) {
        for (size_t i = 0; i < refs.size(); ++i) {
            if (ref_seen[i]) continue;
            if (!meta_->chunk_referenced(refs[i])) continue;
            ++st.refs_stale;
            LOG_WARN("duostore '{}': scrub: refs entry {:016x} not referenced by any object or "
                     "part manifest (space-leak suspect; transient if an MPU completed "
                     "mid-scrub — re-run to confirm)",
                     cfg_.name, refs[i]);
        }
    }

    LOG_INFO("duostore '{}': scrub{}: {} objects, {} parts, {} extents, {} bytes read; "
             "corrupt {}, unreadable {}, refs missing {}, refs stale {}, meta errors {}",
             cfg_.name, st.aborted ? " (aborted)" : "", st.objects_scanned, st.parts_scanned,
             st.extents_checked, st.bytes_read, st.corrupt_extents, st.unreadable_extents,
             st.refs_missing, st.refs_stale, st.meta_errors);
    co_return st;
}

Task<duostore::MetaDumpStats> DuoStoreBackend::run_meta_dump(std::ostream& out) {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    if (!scope.ok())
        throw S3Error(S3ErrorCode::InternalError, "duostore meta dump: backend closing");
    auto permit = co_await gc_sem_.acquire();  // no GC unlinks while the dump references extents
    // Online dump (roadmap §3.7): engines with MVCC/read transactions hand out a
    // consistent point-in-time view and business writes may continue; redis
    // cannot and keeps the historical writes-stopped contract
    auto view = meta_->snapshot();
    if (!view)
        LOG_WARN("duostore '{}': meta engine cannot snapshot — the dump is only "
                 "consistent if writes are stopped for its duration", cfg_.name);
    co_return duostore::dump_meta(
        view ? *view : static_cast<duostore::IMetaReadView&>(*meta_), out);
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
        meta_cache_->clear();  // the restored records replace whatever was cached (roadmap §3.8)
    }  // semaphore released at block exit — the orphan scan below must re-acquire the same one
    // Tail end of the restore-and-solidify flow (operational contract in
    // meta_dump.h): a forced orphan scan reclaims the extra data-side files
    // from the backup window (gcq and in-flight MPUs are deliberately excluded
    // from the dump; their data is cashed in and reclaimed here)
    LOG_INFO("duostore '{}': meta load done, running forced orphan scan", cfg_.name);
    co_await run_orphan_scan_once();
    co_return st;
}

Task<void> DuoStoreBackend::gc_tick() {
    // Re-arm on completion (not on trigger): GC rounds never overlap or pile
    // up — a slow round merely pushes back the next trigger
    std::exception_ptr err;
    try {
        co_await run_gc_once();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_gc();
    if (err) std::rethrow_exception(err);  // let BackgroundTaskGroup log it
}

void DuoStoreBackend::schedule_gc() {
    if (!cfg_.gc_enabled) {
        // Multi-gateway non-designated instance (docs/duostore-rados-data.md
        // §8.3); the manual hooks remain available. Reached once per timer at
        // construction (gc_tick never re-enters), and logs loudly so nobody
        // forgets which box is running GC
        LOG_INFO("duostore '{}': background GC/orphan scan disabled by gc_enabled=false "
                 "(multi-gateway secondary)", cfg_.name);
        return;
    }
    if (cfg_.gc_interval_sec <= 0) return;  // 0 = background GC disabled (manual hooks for tests)
    bg_.if_open([&] {
        gc_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.gc_interval_sec),
                                               [this] { bg_.spawn(gc_tick()); });
    });
}

Task<void> DuoStoreBackend::orphan_tick() {
    // Re-arm on completion (same as gc_tick): scan rounds never overlap or pile up
    std::exception_ptr err;
    try {
        co_await run_orphan_scan_once();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_orphan_scan();
    if (err) std::rethrow_exception(err);  // let BackgroundTaskGroup log it
}

void DuoStoreBackend::schedule_orphan_scan() {
    if (!cfg_.gc_enabled) return;                    // same gate as GC (§8.3 single-instance execution)
    if (cfg_.orphan_scan_interval_sec <= 0) return;  // 0 = disabled (manual hooks for tests)
    bg_.if_open([&] {
        orphan_timer_ = TimerQueue::instance().add(
            std::chrono::seconds(cfg_.orphan_scan_interval_sec),
            [this] { bg_.spawn(orphan_tick()); });
    });
}

Task<void> DuoStoreBackend::lease_tick() {
    co_await pool_->schedule();
    bool supported = true;
    try {
        const int64_t now = codec::to_unix_ms(std::chrono::system_clock::now());
        // TTL = 3 publish periods: a gateway that misses two renewals in a row
        // (crash, partition) stops holding peers' reclamation back
        const int64_t ttl_ms = int64_t(cfg_.read_lease_sec) * 3000;
        // A cached manifest may be up to meta_cache_ttl old when a read starts, so
        // the lease is backdated by that much: a peer's GC then only reclaims extents
        // whose deref predates every manifest this gateway can still be holding
        // (roadmap §3.8; the TTL is bounded below gc_grace by config validation)
        int64_t oldest = read_clock_->oldest_or(now);
        if (meta_cache_->enabled() && cfg_.meta_cache_ttl_sec > 0)
            oldest -= int64_t(cfg_.meta_cache_ttl_sec) * 1000;
        supported = meta_->publish_read_lease(gc_owner_, oldest, ttl_ms);
    } catch (const std::exception& e) {
        // Transient (network): keep the timer armed; while the lease is stale
        // peers merely defer more, never reclaim more
        LOG_WARN("duostore '{}': read-lease publish failed: {}", cfg_.name, e.what());
    }
    // Local engines report unsupported — in-process pins are already exact
    // there, so the publisher stands down for the process lifetime
    if (supported) schedule_read_lease();
}

void DuoStoreBackend::schedule_read_lease() {
    if (cfg_.read_lease_sec <= 0) return;  // 0 = off (single-gateway deployments)
    bg_.if_open([&] {
        lease_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.read_lease_sec),
                                                  [this] { bg_.spawn(lease_tick()); });
    });
}

void DuoStoreBackend::shutdown_background() {
    bg_.begin_close();
    // cancel must be called outside the group lock: TimerQueue::cancel blocks
    // waiting for in-flight callbacks, and the callbacks take the group lock
    // (bg_.spawn) — after begin_close the two timer ids never change again, so
    // reading them needs no lock
    TimerQueue::instance().cancel(gc_timer_);
    TimerQueue::instance().cancel(orphan_timer_);
    TimerQueue::instance().cancel(lease_timer_);
    // The blocking wait happens on the caller's thread; in-flight GC finishes
    // up on pool threads, so neither holds up the other (same as tiered close)
    bg_.wait_idle();
}

}  // namespace lights3::storage
