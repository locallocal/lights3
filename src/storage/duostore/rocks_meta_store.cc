#include "storage/duostore/rocks_meta_store.h"

#include <array>
#include <charconv>

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/meta_util.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// Counter keys (stats CF, §4.1): file_id segments and gcq seq
constexpr const char* kCounterChunk = "c0";
constexpr const char* kCounterPack = "c1";
constexpr const char* kCounterSeq = "q";

// Pack liveness accounting keys (stats CF; §4.1's p<be64 pack_id> expanded into
// per-field sub-keys): 'b' = live_bytes (merge counter), 'r' = live_recs (merge
// counter), 's' = seal marker (plain Put, value = 8B little-endian file_size;
// presence means sealed). The three keys of one pack share a prefix and are
// adjacent, so pack_stats() aggregates them in a single prefix scan
std::string pack_stat_key(uint64_t pack_id, char field) {
    std::string k = "p";
    k += codec::be64_key(pack_id);
    k += field;
    return k;
}

[[noreturn]] void throw_status(const char* what, const rocksdb::Status& s) {
    LOG_ERROR("duostore meta: {}: {}", what, s.ToString());
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore meta: ") + what + ": " + s.ToString());
}

rocksdb::Slice slice(std::string_view s) { return {s.data(), s.size()}; }

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

// Counter merge for the stats CF: accumulates 8B little-endian i64 deltas (§4.5 id
// segment reservation; extended for pack liveness accounting in P2)
class CounterMerge final : public rocksdb::AssociativeMergeOperator {
public:
    bool Merge(const rocksdb::Slice& /*key*/, const rocksdb::Slice* existing,
               const rocksdb::Slice& value, std::string* new_value,
               rocksdb::Logger* /*logger*/) const override {
        int64_t base = 0;
        if (existing && existing->size() == 8)
            base = codec::decode_counter({existing->data(), existing->size()});
        int64_t d = value.size() == 8 ? codec::decode_counter({value.data(), value.size()}) : 0;
        *new_value = codec::encode_counter_delta(base + d);
        return true;
    }
    const char* Name() const override { return "duostore.counter"; }
};

struct SnapshotGuard {
    rocksdb::DB* db;
    const rocksdb::Snapshot* snap;
    ~SnapshotGuard() {
        if (snap) db->ReleaseSnapshot(snap);
    }
};

// User key (strips the "<bucket>\0" prefix from the CF key)
std::string_view strip_prefix(const rocksdb::Slice& k, size_t prefix_len) {
    return {k.data() + prefix_len, k.size() - prefix_len};
}

using codec::bump_last_byte;  // successor for delimiter group skipping (codec.h, shared by meta store impls)

}  // namespace

RocksMetaStore::RocksMetaStore(RocksMetaOptions opt) : opt_(std::move(opt)) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.max_background_jobs = opt_.max_background_jobs;

    rocksdb::BlockBasedTableOptions table;
    table.block_cache = rocksdb::NewLRUCache(opt_.block_cache_bytes);
    auto table_factory = std::shared_ptr<rocksdb::TableFactory>(
        rocksdb::NewBlockBasedTableFactory(table));

    rocksdb::ColumnFamilyOptions cf_opt;
    cf_opt.table_factory = table_factory;
    cf_opt.compression = rocksdb::kNoCompression;  // compression fully disabled (§13.3)
    cf_opt.write_buffer_size = opt_.write_buffer_bytes;
    cf_opt.max_write_buffer_number = opt_.max_write_buffers;
    rocksdb::ColumnFamilyOptions stats_opt = cf_opt;
    stats_opt.merge_operator = std::make_shared<CounterMerge>();

    std::vector<rocksdb::ColumnFamilyDescriptor> descs = {
        {rocksdb::kDefaultColumnFamilyName, cf_opt}, {"buckets", cf_opt},
        {"objects", cf_opt},                         {"uploads", cf_opt},
        {"parts", cf_opt},                           {"refs", cf_opt},
        {"gcq", cf_opt},                             {"stats", stats_opt},
    };
    rocksdb::DB* db = nullptr;
    auto s = rocksdb::DB::Open(options, opt_.path, descs, &cfs_, &db);
    if (!s.ok()) throw_status("open", s);
    db_.store(db, std::memory_order_release);

    // Schema version check + migration hook (§4.1 / docs/archive/gaps.md §6.1). This used to
    // be a hard "must exactly equal the current constant" check — any value-layout
    // change would make existing databases fail to start. Now:
    //   stored version == current → pass through;
    //   stored version < current  → upgrade step by step via the registered migration
    //                               chain (kSchemaMigrations, currently empty —
    //                               record-level evolution goes through codec's
    //                               read_ver compatible reads, §5.2; this chain is
    //                               reserved for future CF/key-layout changes), then
    //                               stamp the new version number;
    //   stored version > current  → the DB comes from a newer program version; reject
    //                               explicitly (running downgraded would silently
    //                               corrupt writes).
    // Any failure after a successful Open must go through close() — the destructor
    // does not run when the constructor throws, otherwise the DB handle and LOCK
    // file leak
    try {
        auto schema = get_raw(kDefault, "schema");
        if (!schema) {
            rocksdb::WriteBatch batch;
            batch.Put(cfs_[kDefault], "schema", std::to_string(kSchemaCurrent));
            batch.Put(cfs_[kDefault], "instance", new_upload_id());
            commit(batch);
        } else {
            migrate_schema(*schema);
        }
    } catch (...) {
        close();
        throw;
    }

    // Meta engine observability (docs/archive/gaps.md §6.1: the default engine rocksdb
    // previously had no metrics at all; redis/sqlite/tikv each have busy/corruption/
    // conflict counters). Property gauges are read live at render time; they return 0
    // after the db is closed (weak-pointer semantics carried by the atomic null check
    // on db_)
    auto prop = [this](const char* name) -> double {
        auto* d = db_.load(std::memory_order_acquire);
        uint64_t v = 0;
        if (d && d->GetAggregatedIntProperty(name, &v)) return double(v);
        return 0;
    };
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_estimate_num_keys",
                                "RocksDB estimated key count across all column families",
                                [prop] { return prop("rocksdb.estimate-num-keys"); });
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_block_cache_usage_bytes",
                                "RocksDB block cache memory usage",
                                [prop] { return prop("rocksdb.block-cache-usage"); });
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_sst_bytes",
                                "RocksDB total SST file size (meta store on-disk footprint)",
                                [prop] { return prop("rocksdb.total-sst-files-size"); });
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_memtable_bytes",
                                "RocksDB active+immutable memtable memory",
                                [prop] { return prop("rocksdb.cur-size-all-mem-tables"); });
}

int64_t RocksMetaStore::validate_schema_marker(const std::string& stored) {
    return parse_schema_marker(stored, /*lineage=*/"", kSchemaCurrent, "duostore meta");
}

// Migration chain (in-place transform from version n to n+1; called with the db
// already open). Register new layout changes here and bump kSchemaCurrent by 1 —
// "changing the layout without leaving a migration" is never allowed
void RocksMetaStore::migrate_schema(const std::string& stored) {
    int64_t ver = validate_schema_marker(stored);
    if (ver == kSchemaCurrent) return;
    using MigrateFn = void (*)(RocksMetaStore&);
    static constexpr std::array<std::pair<int64_t, MigrateFn>, 0> kSchemaMigrations{
        // {{1, &migrate_v1_to_v2}}  // example: once registered, v1 DBs upgrade automatically on startup
    };
    for (; ver < kSchemaCurrent; ++ver) {
        MigrateFn fn = nullptr;
        for (auto& [from, f] : kSchemaMigrations)
            if (from == ver) fn = f;
        if (!fn) throw_no_migration(ver, kSchemaCurrent, "duostore meta");
        fn(*this);
        rocksdb::WriteBatch batch;
        batch.Put(cfs_[kDefault], "schema", std::to_string(ver + 1));
        commit(batch);
        LOG_INFO("duostore meta: schema migrated v{} -> v{}", ver, ver + 1);
    }
}

RocksMetaStore::~RocksMetaStore() {
    try {
        close();
    } catch (const std::exception& e) {
        LOG_ERROR("duostore meta: close in dtor failed: {}", e.what());
    }
}

void RocksMetaStore::close() {
    std::lock_guard lk(mu_);
    // Detach db_ first: calls after close() returns fail cleanly in db() (500)
    // instead of dereferencing a dangling pointer
    rocksdb::DB* db = db_.exchange(nullptr, std::memory_order_acq_rel);
    if (!db) return;
    for (auto* h : cfs_) db->DestroyColumnFamilyHandle(h);
    cfs_.clear();
    auto s = db->Close();
    if (!s.ok()) LOG_ERROR("duostore meta: close: {}", s.ToString());
    delete db;
}

// ---------- basic wrappers ----------

rocksdb::DB* RocksMetaStore::db() const {
    auto* d = db_.load(std::memory_order_acquire);
    if (!d)
        throw S3Error(S3ErrorCode::InternalError, "duostore meta: store is closed");
    return d;
}

std::optional<std::string> RocksMetaStore::get_raw(int cf, std::string_view key) {
    auto* d = db();  // fetch the handle before touching cfs_ (close nulls db_ before clearing cfs_)
    std::string v;
    auto s = d->Get(rocksdb::ReadOptions(), cfs_[cf], slice(key), &v);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) throw_status("get", s);
    return v;
}

void RocksMetaStore::commit(rocksdb::WriteBatch& batch) {
    rocksdb::WriteOptions wo;
    wo.sync = opt_.sync;
    auto s = db()->Write(wo, &batch);
    if (!s.ok()) throw_status("commit", s);
}

void RocksMetaStore::require_bucket_locked(std::string_view b) {
    if (!get_raw(kBuckets, b))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(b));
}

void RocksMetaStore::batch_refs(rocksdb::WriteBatch& batch, const DataRef& ref, bool add,
                                std::string_view owner) {
    for (const auto& e : ref.extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack liveness goes through the stats accounting (P2);
                                                      // chunk/rados both enter refs by file_id
        if (add)
            batch.Put(cfs_[kRefs], codec::be64_key(e.file_id), slice(owner));
        else
            batch.Delete(cfs_[kRefs], codec::be64_key(e.file_id));
    }
}

void RocksMetaStore::batch_pack_delta(rocksdb::WriteBatch& batch, const DataRef& ref,
                                      int sign, int64_t rec_overhead) {
    // Aggregate multiple extents of the same pack first, then two merges per pack
    // (§9.1: incremented/decremented in the same batch as the business transaction);
    // each record counts payload + header overhead, matching the file_size accounting
    // basis (docs/archive/gaps.md §2.3a)
    std::map<uint64_t, std::pair<int64_t, int64_t>> agg;  // pack_id -> (bytes, recs)
    for (const auto& e : ref.extents) {
        if (e.kind != Extent::Kind::kPack) continue;
        auto& [bytes, recs] = agg[e.file_id];
        bytes += sign * (int64_t(e.length) + rec_overhead);
        recs += sign;
    }
    for (const auto& [id, d] : agg) {
        batch.Merge(cfs_[kStats], pack_stat_key(id, 'b'), codec::encode_counter_delta(d.first));
        batch.Merge(cfs_[kStats], pack_stat_key(id, 'r'), codec::encode_counter_delta(d.second));
    }
}

void RocksMetaStore::enqueue_reclaim_locked(rocksdb::WriteBatch& batch, const DataRef& ref,
                                            ReclaimReason reason) {
    if (ref.extents.empty()) return;
    // Split oversized DataRefs (TB-scale objects = hundreds of thousands of extents)
    // into multiple gcq entries (docs/archive/gaps.md §2.11): decoded resident memory per GC
    // batch stays bounded; acks are independent per entry, and splitting does not
    // change crash semantics (unlink is idempotent)
    const int64_t ts = now_ms();
    for (size_t i = 0; i < ref.extents.size(); i += kReclaimMaxExtents) {
        size_t n = std::min(kReclaimMaxExtents, ref.extents.size() - i);
        Reclaim r;
        r.extents.assign(ref.extents.begin() + i, ref.extents.begin() + i + n);
        r.reason = reason;
        uint64_t seq = alloc_id(kCounterSeq, seqs_);
        batch.Put(cfs_[kGcq], codec::be64_key(seq), codec::encode_reclaim(r, ts));
    }
}

uint64_t RocksMetaStore::alloc_id(std::string_view counter_key, IdRange& r, uint32_t n) {
    n = std::clamp<uint32_t>(n, 1, kMaxIdRun);  // run ≤ kMaxIdRun << kIdSegment
    std::lock_guard lk(alloc_mu_);  // independent of mu_: the common path is a pure in-memory next += n
    if (r.limit - r.next < n) {
        // The segment reservation must be persisted before ids are handed out, and
        // always with WAL fsync (independent of meta_sync) — otherwise a crash that
        // loses the reservation makes restart re-issue already-used file_ids, which
        // then collide with chunk files already on disk via O_EXCL (§6.3's "still
        // self-consistent" depends on the unconditional sync here). Wasting a segment
        // on crash is harmless; likewise the leftover discarded when switching
        // segments (run batch dispatch requires contiguity within a segment,
        // docs/archive/gaps.md §3.9)
        rocksdb::WriteBatch batch;
        batch.Merge(cfs_[kStats], slice(counter_key),
                    codec::encode_counter_delta(int64_t(kIdSegment)));
        rocksdb::WriteOptions wo;
        wo.sync = true;
        auto s = db()->Write(wo, &batch);
        if (!s.ok()) throw_status("reserve id segment", s);
        auto v = get_raw(kStats, counter_key);
        if (!v) throw S3Error(S3ErrorCode::InternalError, "duostore meta: counter vanished");
        uint64_t hi = uint64_t(codec::decode_counter(*v));
        r.limit = hi;
        r.next = hi - kIdSegment;
    }
    uint64_t first = r.next;
    r.next += n;
    return first;
}

uint64_t RocksMetaStore::alloc_file_run(Extent::Kind kind, uint32_t n) {
    // kRados shares the id segment with kChunk: the refs table accounts by bare
    // file_id without distinguishing kind, so separate counters would produce
    // cross-kind id collisions when switching the data engine (fs<->rados) on the
    // same meta store
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCounterChunk : kCounterPack,
                    file_ids_[size_t(kind)], n);
}

// ---------- bucket ----------

void RocksMetaStore::create_bucket(std::string_view b) {
    std::lock_guard lk(mu_);
    if (get_raw(kBuckets, b))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(b));
    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kBuckets], slice(b), codec::encode_bucket(now_ms()));
    commit(batch);
}

void RocksMetaStore::delete_bucket(std::string_view b) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    // Emptiness check: reject if either objects or uploads is non-empty (AWS also
    // refuses to delete a bucket with in-progress MPUs). The uploads check also
    // closes off the whole class of "put_part keeps writing after bucket deletion,
    // refs leak permanently, recreating the bucket resurrects ghost uploads"
    // problems — while an upload exists the bucket cannot be deleted, so put_part's
    // require_upload alone suffices to guarantee the bucket still exists
    std::string prefix = std::string(b) + '\0';
    auto* d = db();
    for (int cf : {int(kObjects), int(kUploads)}) {
        auto it = std::unique_ptr<rocksdb::Iterator>(
            d->NewIterator(rocksdb::ReadOptions(), cfs_[cf]));
        it->Seek(prefix);
        if (it->Valid() && it->key().starts_with(slice(prefix)))
            throw S3Error(S3ErrorCode::BucketNotEmpty,
                          "The bucket you tried to delete is not empty", std::string(b));
        if (!it->status().ok()) throw_status("delete_bucket scan", it->status());
    }
    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kBuckets], slice(b));
    commit(batch);
}

bool RocksMetaStore::bucket_exists(std::string_view b) { return get_raw(kBuckets, b).has_value(); }

std::vector<BucketInfo> RocksMetaStore::list_buckets() {
    std::vector<BucketInfo> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kBuckets]));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        int64_t created = codec::decode_bucket({it->value().data(), it->value().size()});
        out.push_back({it->key().ToString(), codec::from_unix_ms(created)});
    }
    if (!it->status().ok()) throw_status("list_buckets", it->status());
    return out;  // key byte order is lexicographic order
}

// ---------- object ----------

std::optional<ObjectRec> RocksMetaStore::get_object(std::string_view b, std::string_view k) {
    auto v = get_raw(kObjects, codec::object_key(b, k));
    if (!v) return std::nullopt;
    return codec::decode_object(std::string(k), *v);
}

std::optional<ObjectMeta> RocksMetaStore::head_object(std::string_view b, std::string_view k) {
    auto v = get_raw(kObjects, codec::object_key(b, k));
    if (!v) return std::nullopt;
    return codec::decode_object_meta(std::string(k), *v);
}

void RocksMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec,
                                PutCondition cond) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    std::string okey = codec::object_key(b, k);
    std::optional<ObjectRec> old;
    if (auto oldv = get_raw(kObjects, okey)) old = codec::decode_object(std::string(k), *oldv);
    check_put_condition(cond, old, k);  // same lock as the commit (PutCondition contract)
    rec.version = old ? old->version + 1 : 1;

    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kObjects], okey, codec::encode_object(rec));
    batch_refs(batch, rec.data, /*add=*/true, okey);
    const int64_t ov = codec::pack_rec_overhead(b, k);
    batch_pack_delta(batch, rec.data, +1, ov);
    if (old) {
        enqueue_reclaim_locked(batch, old->data, ReclaimReason::kOverwrite);
        batch_refs(batch, old->data, /*add=*/false, {});
        batch_pack_delta(batch, old->data, -1, ov);
    }
    commit(batch);
}

bool RocksMetaStore::delete_object(std::string_view b, std::string_view k) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    std::string okey = codec::object_key(b, k);
    auto oldv = get_raw(kObjects, okey);
    if (!oldv) return false;
    auto old = codec::decode_object(std::string(k), *oldv);

    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kObjects], okey);
    enqueue_reclaim_locked(batch, old.data, ReclaimReason::kDelete);
    batch_refs(batch, old.data, /*add=*/false, {});
    batch_pack_delta(batch, old.data, -1, codec::pack_rec_overhead(b, k));
    commit(batch);
    return true;
}

// §4.4: the objects CF iterates in natural sorted order; delimiter groups are
// skipped wholesale with Seek
ListResult RocksMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    require_bucket_locked(b);  // pure read; the lock-free get is idempotent and safe
    ListResult out;
    // S3: max-keys=0 returns empty with IsTruncated=false (consistent with apply_listing)
    if (opt.max_keys <= 0) return out;
    std::string base = std::string(b) + '\0';
    std::string upper = std::string(b) + '\x01';  // bucket names contain no NUL, so '\0'+1 is the bucket range upper bound

    rocksdb::ReadOptions ro;
    auto* d = db();
    SnapshotGuard snap{d, d->GetSnapshot()};
    ro.snapshot = snap.snap;
    rocksdb::Slice upper_slice(upper);
    ro.iterate_upper_bound = &upper_slice;
    auto it = std::unique_ptr<rocksdb::Iterator>(d->NewIterator(ro, cfs_[kObjects]));

    const std::string& prefix = opt.prefix;
    const std::string& delim = opt.delimiter;
    std::string seek = base + std::max(prefix, opt.start_after);
    it->Seek(seek);
    if (!opt.start_after.empty() && it->Valid() &&
        it->key() == slice(base + opt.start_after))
        it->Next();  // step past start_after when it matches itself

    std::string last_emitted;
    int count = 0;
    while (it->Valid()) {
        std::string_view uk = strip_prefix(it->key(), base.size());
        if (uk.compare(0, prefix.size(), prefix) != 0) break;  // stop once past the prefix range
        if (count >= opt.max_keys) {
            out.is_truncated = true;
            out.next_token = last_emitted;
            break;
        }
        if (!delim.empty()) {
            auto pos = uk.find(delim, prefix.size());
            if (pos != std::string_view::npos) {
                std::string group(uk.substr(0, pos + delim.size()));
                out.common_prefixes.push_back(group);
                ++count;
                // Seek skips the whole group (the substantive advantage over
                // localfs directory traversal, §4.4); token semantics must land at
                // the group tail -> Prev fetches the last key inside the group.
                // Invariant: when Seek lands on !Valid (the group is the last
                // stretch of the bucket) last_emitted keeps its old value, but the
                // loop then necessarily exits without taking the truncation branch,
                // so the stale value is never read
                std::string target = base + group;
                if (!bump_last_byte(target)) break;
                it->Seek(target);
                if (it->Valid()) {
                    it->Prev();
                    last_emitted = std::string(strip_prefix(it->key(), base.size()));
                    it->Next();
                }
                continue;
            }
        }
        out.objects.push_back(codec::decode_object_meta(
            std::string(uk), {it->value().data(), it->value().size()}));
        last_emitted = std::string(uk);
        ++count;
        it->Next();
    }
    if (!it->status().ok()) throw_status("list_objects", it->status());
    return out;
}

// ---------- multipart ----------

std::string RocksMetaStore::create_upload(std::string_view b, std::string_view k,
                                          ObjectMeta meta) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    UploadRec rec;
    rec.upload_id = new_upload_id();
    rec.meta = std::move(meta);
    rec.meta.key = std::string(k);
    rec.initiated_ms = now_ms();
    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kUploads], codec::upload_key(b, k, rec.upload_id),
              codec::encode_upload(rec));
    commit(batch);
    return rec.upload_id;
}

UploadRec RocksMetaStore::require_upload(std::string_view b, std::string_view k,
                                         std::string_view id) {
    auto missing = [&]() -> S3Error {
        return {S3ErrorCode::NoSuchUpload, "The specified multipart upload does not exist.",
                std::string(id)};
    };
    if (!is_valid_upload_id(id)) throw missing();
    auto v = get_raw(kUploads, codec::upload_key(b, k, id));
    if (!v) throw missing();
    return codec::decode_upload(std::string(k), std::string(id), *v);
}

void RocksMetaStore::put_part(std::string_view b, std::string_view k, std::string_view id,
                              PartRec p) {
    std::lock_guard lk(mu_);
    require_upload(b, k, id);
    std::string pkey = codec::part_key(b, k, id, p.part_no);
    std::optional<PartRec> old;
    if (auto oldv = get_raw(kParts, pkey)) old = codec::decode_part(p.part_no, *oldv);

    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kParts], pkey, codec::encode_part(p));
    batch_refs(batch, p.data, /*add=*/true, pkey);
    const int64_t ov = codec::pack_rec_overhead_part(b, k, id, p.part_no);
    batch_pack_delta(batch, p.data, +1, ov);
    if (old) {  // same-number re-upload is last-write-wins: the old part enters the GC ledger in the same batch
        enqueue_reclaim_locked(batch, old->data, ReclaimReason::kPartOverwrite);
        batch_refs(batch, old->data, /*add=*/false, {});
        batch_pack_delta(batch, old->data, -1, ov);
    }
    commit(batch);
}

std::vector<PartRec> RocksMetaStore::scan_parts(std::string_view b, std::string_view k,
                                                std::string_view id) {
    std::string prefix = codec::parts_prefix(b, k, id);
    std::vector<PartRec> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kParts]));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(slice(prefix)); it->Next()) {
        int no = codec::part_no_of_key({it->key().data(), it->key().size()});
        out.push_back(codec::decode_part(no, {it->value().data(), it->value().size()}));
    }
    if (!it->status().ok()) throw_status("scan parts", it->status());
    return out;  // be16 part_no guarantees ascending order (§4.1)
}

std::vector<PartRec> RocksMetaStore::list_parts(std::string_view b, std::string_view k,
                                                std::string_view id) {
    require_upload(b, k, id);
    return scan_parts(b, k, id);
}

std::vector<UploadInfo> RocksMetaStore::list_uploads(std::string_view b,
                                                    std::string_view key_marker,
                                                    std::string_view id_marker, int limit) {
    require_bucket_locked(b);  // pure read; the lock-free get is idempotent and safe
    std::string prefix = std::string(b) + '\0';
    // Cursor pushdown (docs/archive/gaps.md §5.1): the key encoding is already in
    // (key, upload_id) order, so seeking past the marker suffices — not a single
    // skipped entry is read
    std::string seek = prefix;
    if (!key_marker.empty() || !id_marker.empty()) {
        seek += std::string(key_marker);
        seek += '\0';
        seek += std::string(id_marker);
        seek += '\0';  // trailing '\0' makes the seek land just after that (key,id)
    }
    std::vector<UploadInfo> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kUploads]));
    for (it->Seek(seek); it->Valid() && it->key().starts_with(slice(prefix)); it->Next()) {
        if (limit > 0 && out.size() >= size_t(limit)) break;
        // key = <bucket>\0<key>\0<upload_id>; the prefix scan is naturally sorted by (key, upload_id)
        std::string_view rest = strip_prefix(it->key(), prefix.size());
        auto sep = rest.rfind('\0');
        if (sep == std::string_view::npos) continue;
        auto rec = codec::decode_upload(std::string(rest.substr(0, sep)),
                                        std::string(rest.substr(sep + 1)),
                                        {it->value().data(), it->value().size()});
        out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
    }
    if (!it->status().ok()) throw_status("list_uploads", it->status());
    return out;
}

// §8: complete is a pure metadata transaction with zero data movement — it
// concatenates each part's extent runs in order
std::string RocksMetaStore::complete_upload(std::string_view b, std::string_view k,
                                            std::string_view id,
                                            std::span<const PartInfo> parts) {
    std::lock_guard lk(mu_);
    auto up = require_upload(b, k, id);
    require_bucket_locked(b);

    std::map<int, PartRec> stored;
    for (auto& p : scan_parts(b, k, id)) stored.emplace(p.part_no, std::move(p));

    std::set<int> selected;
    ObjectRec rec = assemble_completed_object(std::move(up.meta), parts, stored, selected);

    std::string okey = codec::object_key(b, k);
    std::optional<ObjectRec> old;
    if (auto oldv = get_raw(kObjects, okey)) old = codec::decode_object(std::string(k), *oldv);
    rec.version = old ? old->version + 1 : 1;

    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kObjects], okey, codec::encode_object(rec));
    batch.Delete(cfs_[kUploads], codec::upload_key(b, k, id));
    // Clear parts with a single DeleteRange (avoids rebuilding key strings per part
    // and batch bloat with ten-thousand-part uploads); gcq/refs accounting still
    // needs each part's extents
    std::string pfx = codec::parts_prefix(b, k, id);
    std::string pfx_end = pfx;
    bump_last_byte(pfx_end);
    batch.DeleteRange(cfs_[kParts], pfx, pfx_end);
    for (const auto& [no, p] : stored) {
        if (selected.count(no)) {
            // refs transfer: owner is rewritten to the object. Pack liveness stays
            // unchanged, but the accounting basis is rebalanced from part to object
            // (-part header overhead +object header overhead; recs cancel out with
            // one decrement and one increment): this guarantees that a later object
            // deletion, deducting on the object basis, zeroes the ledger exactly
            batch_refs(batch, p.data, /*add=*/true, okey);
            batch_pack_delta(batch, p.data, -1,
                             codec::pack_rec_overhead_part(b, k, id, no));
            batch_pack_delta(batch, p.data, +1, codec::pack_rec_overhead(b, k));
        } else {  // unselected parts enter the GC ledger
            enqueue_reclaim_locked(batch, p.data, ReclaimReason::kComplete);
            batch_refs(batch, p.data, /*add=*/false, {});
            batch_pack_delta(batch, p.data, -1,
                             codec::pack_rec_overhead_part(b, k, id, no));
        }
    }
    if (old) {  // the old same-name object enters the GC ledger
        enqueue_reclaim_locked(batch, old->data, ReclaimReason::kOverwrite);
        batch_refs(batch, old->data, /*add=*/false, {});
        batch_pack_delta(batch, old->data, -1, codec::pack_rec_overhead(b, k));
    }
    commit(batch);
    return rec.meta.etag;
}

void RocksMetaStore::abort_upload(std::string_view b, std::string_view k,
                                  std::string_view id) {
    std::lock_guard lk(mu_);
    require_upload(b, k, id);
    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kUploads], codec::upload_key(b, k, id));
    std::string pfx = codec::parts_prefix(b, k, id);
    std::string pfx_end = pfx;
    bump_last_byte(pfx_end);
    batch.DeleteRange(cfs_[kParts], pfx, pfx_end);
    for (const auto& p : scan_parts(b, k, id)) {
        enqueue_reclaim_locked(batch, p.data, ReclaimReason::kAbort);
        batch_refs(batch, p.data, /*add=*/false, {});
        batch_pack_delta(batch, p.data, -1,
                         codec::pack_rec_overhead_part(b, k, id, p.part_no));
    }
    commit(batch);
}

// ---------- GC accounting ----------

std::vector<std::pair<uint64_t, Reclaim>> RocksMetaStore::peek_reclaims(size_t max,
                                                                        uint64_t min_seq,
                                                                        size_t max_extents) {
    std::vector<std::pair<uint64_t, Reclaim>> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kGcq]));
    std::string start = codec::be64_key(min_seq);
    size_t extents = 0;
    for (it->Seek(start); it->Valid() && out.size() < max; it->Next()) {
        uint64_t seq = codec::parse_be64({it->key().data(), it->key().size()});
        out.emplace_back(seq,
                         codec::decode_reclaim({it->value().data(), it->value().size()}));
        // Cumulative extent cap (gaps §2.11): return at least 1 entry (oversized single entries left from before splitting)
        extents += out.back().second.extents.size();
        if (extents >= max_extents) break;
    }
    if (!it->status().ok()) throw_status("peek_reclaims", it->status());
    return out;
}

void RocksMetaStore::ack_reclaim(uint64_t seq) {
    // Blind single-key delete with no cross-key invariant — does not take mu_, so GC
    // write-off does not queue behind business commits' fsync
    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kGcq], codec::be64_key(seq));
    commit(batch);
}

void RocksMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    rocksdb::WriteBatch batch;  // one batch, one commit (overrides the interface's default per-entry forwarding)
    for (uint64_t s : seqs) batch.Delete(cfs_[kGcq], codec::be64_key(s));
    commit(batch);
}

std::vector<PackStat> RocksMetaStore::pack_stats() {
    // Prefix scan 'p' over the stats CF: a pack's b/r/s sub-keys are adjacent, so we
    // aggregate while scanning. The result includes live=0 and unsealed entries
    // (whole-pack deletion of empty packs and re-sealing on restart depend on seeing
    // them)
    std::vector<PackStat> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kStats]));
    for (it->Seek("p"); it->Valid() && it->key().starts_with("p"); it->Next()) {
        std::string_view k(it->key().data(), it->key().size());
        if (k.size() != 10) continue;  // 'p' + be64 + field
        uint64_t id = codec::parse_be64(k.substr(1, 8));
        std::string_view v(it->value().data(), it->value().size());
        if (out.empty() || out.back().pack_id != id) out.push_back({.pack_id = id});
        switch (k[9]) {
            case 'b': out.back().live_bytes = codec::decode_counter(v); break;
            case 'r': out.back().live_recs = codec::decode_counter(v); break;
            case 's':
                out.back().sealed = true;
                out.back().file_size = uint64_t(codec::decode_counter(v));
                break;
            default: break;
        }
    }
    if (!it->status().ok()) throw_status("pack_stats", it->status());
    return out;
}

void RocksMetaStore::seal_pack(uint64_t pack_id, uint64_t file_size) {
    std::lock_guard lk(mu_);  // read-modify-write (keeps an already-recorded size), serialized against concurrent seals
    std::string skey = pack_stat_key(pack_id, 's');
    if (file_size == 0 && get_raw(kStats, skey)) return;  // idempotent: 0 does not overwrite a known size
    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kStats], skey, codec::encode_counter_delta(int64_t(file_size)));
    commit(batch);
}

void RocksMetaStore::drop_pack_stat(uint64_t pack_id) {
    // Blind delete of the three sub-keys with no cross-key invariant — like
    // ack_reclaim, does not take mu_
    rocksdb::WriteBatch batch;
    for (char f : {'b', 'r', 's'}) batch.Delete(cfs_[kStats], pack_stat_key(pack_id, f));
    commit(batch);
}

bool RocksMetaStore::stage_swap_locked(rocksdb::WriteBatch& batch, std::string_view b,
                                       std::string_view k, uint64_t expect_version,
                                       const DataRef& from, const DataRef& to) {
    std::string okey = codec::object_key(b, k);
    auto v = get_raw(kObjects, okey);
    if (!v) return false;
    auto rec = codec::decode_object(std::string(k), *v);
    // Optimistic check: version or extent mismatch = overwritten/deleted in the
    // meantime -> give up (§9.2)
    if (rec.version != expect_version || rec.data.extents != from.extents) return false;
    rec.data = to;
    rec.version += 1;
    batch.Put(cfs_[kObjects], okey, codec::encode_object(rec));
    // refs operate on the set difference: to/from share the not-yet-migrated
    // chunks, and add-all-then-delete-all under last-wins nets out to a delete,
    // which would wipe the refs of live data (detailed at meta_util.h refs_delta)
    auto rd = refs_delta(from, to);
    batch_refs(batch, rd.added, /*add=*/true, okey);
    batch_refs(batch, rd.removed, /*add=*/false, {});
    // Compaction ref swap: the ledger migrates with the extents (§9.2); both sides
    // use the object basis (if the migrated-out old record was in mpu form this
    // slightly under-deducts, which is the conservative direction — see the
    // pack_rec_overhead comment in codec.h)
    const int64_t ov = codec::pack_rec_overhead(b, k);
    batch_pack_delta(batch, to, +1, ov);
    batch_pack_delta(batch, from, -1, ov);
    return true;
}

bool RocksMetaStore::swap_extents(std::string_view b, std::string_view k,
                                  uint64_t expect_version, const DataRef& from,
                                  const DataRef& to) {
    std::lock_guard lk(mu_);
    rocksdb::WriteBatch batch;
    if (!stage_swap_locked(batch, b, k, expect_version, from, to)) return false;
    commit(batch);
    return true;
}

std::vector<bool> RocksMetaStore::swap_extents_batch(std::span<const SwapReq> reqs) {
    // Batched compaction (gaps §2.13): the whole batch commits in one WriteBatch.
    // Per-item CAS is independent — a failed item simply stays out of the batch and
    // does not affect the rest
    std::lock_guard lk(mu_);
    std::vector<bool> out;
    out.reserve(reqs.size());
    rocksdb::WriteBatch batch;
    bool any = false;
    for (const auto& r : reqs) {
        bool ok = stage_swap_locked(batch, r.bucket, r.key, r.expect_version, r.from, r.to);
        any = any || ok;
        out.push_back(ok);
    }
    if (any) commit(batch);
    return out;
}

bool RocksMetaStore::chunk_referenced(uint64_t file_id) {
    return get_raw(kRefs, codec::be64_key(file_id)).has_value();
}

void RocksMetaStore::scan_refs(const std::function<void(uint64_t)>& cb) {
    // Pure read via iterator snapshot (same as §4.4), does not take mu_; the orphan
    // scan tolerates a weakly consistent view
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kRefs]));
    for (it->SeekToFirst(); it->Valid(); it->Next())
        cb(codec::parse_be64({it->key().data(), it->key().size()}));
    if (!it->status().ok()) throw_status("scan_refs", it->status());
}

}  // namespace lights3::storage::duostore
