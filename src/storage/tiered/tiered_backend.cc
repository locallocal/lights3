#include "storage/tiered/tiered_backend.h"

#include <fnmatch.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <deque>
#include <fstream>
#include <stdexcept>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/time.h"
#include "storage/multipart.h"
#include "storage/tiered/tier_local_fs.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/tiered/tier_local_duo.h"
#endif

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using tier::AccessRec;
using tier::LocalObject;
using tier::Tier;
using tier::TierInfo;

namespace {

constexpr int64_t kAccessFlushSec = 300;  // access flush period (docs/tiered-storage.md §4.3)

// For demotion uploads: layers synchronous MD5 and byte counting on top of the local
// side's snapshot stream (docs/tiered-storage.md §5.2 step 3 verification)
class HashingReader final : public http::BodyReader {
public:
    explicit HashingReader(std::unique_ptr<http::BodyReader> inner) : inner_(std::move(inner)) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_->read(buf);
        if (n > 0) {
            md5_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
            bytes_ += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }

    uint64_t bytes() const { return bytes_; }
    std::string md5_hex() { return md5_.final_hex(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    uint64_t bytes_ = 0;
};

std::string ikey_rc(std::string_view bucket, std::string_view key) {
    // Range-cache single-flight marker: distinct from the demotion/fill marker so a
    // partial fill never reads as "upload in flight" to GC/reconcile
    return std::string(bucket) + "/" + std::string(key) + "\x01rc";
}

}  // namespace

// RAII release of an in-flight table entry (used by demote/promote; Tee cache fill releases
// via the reader's destructor)
struct InflightRelease {
    TieredBackend* owner;
    std::string ikey;
    ~InflightRelease() { owner->inflight_end(ikey); }
};

// Tee passthrough + cache-while-downloading (docs/tiered-storage.md §6.2): the cloud stream
// is returned to the client as usual while also being written into a local cache fill
// with incremental MD5; if verification passes at EOF, commit as cached. Local write
// failures silently degrade to pure passthrough; on client disconnect the destructor runs
// and the fill's RAII discards the half-written cache.
class TeeCacheReader final : public http::BodyReader {
public:
    TeeCacheReader(std::shared_ptr<TieredBackend> owner, std::string bucket, std::string key,
                   ObjectMeta meta, TierInfo tier, std::unique_ptr<http::BodyReader> src,
                   std::unique_ptr<tier::ICacheFill> fill)
        : owner_(std::move(owner)), bucket_(std::move(bucket)), key_(std::move(key)),
          meta_(std::move(meta)), tier_(std::move(tier)), src_(std::move(src)),
          fill_(std::move(fill)) {}

    ~TeeCacheReader() override { release_inflight(); }  // safety net for mid-transfer disconnect destruction

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await src_->read(buf);
        if (finished_) co_return n;
        if (n > 0) {
            if (!degraded_) {
                md5_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
                if (!fill_->write(buf.data(), n)) {  // ENOSPC etc.: degrade to pure passthrough, invisible to the client
                    LOG_WARN("tiered: cache fill write failed for {}/{}, passthrough only",
                             bucket_, key_);
                    degraded_ = true;
                } else {
                    written_ += n;
                }
            }
        } else {
            finished_ = true;
            if (!degraded_ && written_ == meta_.size) {
                // Single-part objects compare MD5; multipart objects can only verify byte
                // count (etag has the -N form)
                bool multipart = meta_.etag.find('-') != std::string::npos;
                if (multipart || md5_.final_hex() == meta_.etag) {
                    // The client has received all data by now: a commit failure
                    // (cancellation/ENOSPC) can only drop the cache and degrade -- the
                    // exception must not leak into response teardown and read as a
                    // "transfer failure"
                    try {
                        co_await owner_->commit_cache_fill(bucket_, key_, meta_, tier_, *fill_);
                    } catch (const std::exception& e) {
                        LOG_WARN("tiered: cache fill commit failed for {}/{}: {}, cache dropped",
                                 bucket_, key_, e.what());
                    } catch (...) {
                        LOG_WARN("tiered: cache fill commit failed for {}/{}, cache dropped",
                                 bucket_, key_);
                    }
                } else
                    LOG_WARN("tiered: cloud data checksum mismatch for {}/{}, cache dropped",
                             bucket_, key_);
            }
            // Release single-flight right at EOF: the reader may be held by the response
            // chain for a long time and must not keep blocking demotion/promotion of the
            // same key
            release_inflight();
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return meta_.size; }

private:
    void release_inflight() {
        if (released_) return;
        released_ = true;
        owner_->inflight_end(TieredBackend::make_ikey(bucket_, key_));
    }

    std::shared_ptr<TieredBackend> owner_;
    std::string bucket_, key_;
    ObjectMeta meta_;   // local metadata snapshot (external-meta invariance principle)
    TierInfo tier_;     // expected remote version, re-verified before commit
    std::unique_ptr<http::BodyReader> src_;
    std::unique_ptr<tier::ICacheFill> fill_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    uint64_t written_ = 0;
    bool degraded_ = false, finished_ = false, released_ = false;
};

// Range GET block cache filler (roadmap §3.6 ⑦): the cloud is asked for the block-aligned
// superset [af, al] of the client's [f, l]; bytes flow into the sparse cache file as they
// arrive, and only the client's window is handed on. Once the client window is exhausted
// the remaining tail (at most one block) is drained in the same read() so the last block
// completes; the presence bitmap is written at EOF. Disk failures degrade to passthrough
class RangeTeeReader final : public http::BodyReader {
public:
    RangeTeeReader(std::shared_ptr<TieredBackend> owner, std::string bucket, std::string key,
                   std::unique_ptr<tier::IRangeCache> rc, std::unique_ptr<http::BodyReader> src,
                   uint64_t af, uint64_t al, uint64_t f, uint64_t l)
        : owner_(std::move(owner)), bucket_(std::move(bucket)), key_(std::move(key)),
          rc_(std::move(rc)), src_(std::move(src)), af_(af), al_(al), f_(f), l_(l), pos_(af),
          scratch_(256 * 1024) {}
    ~RangeTeeReader() override { release(); }

    Task<size_t> read(std::span<std::byte> buf) override {
        while (pending_.empty() && !eof_) {
            size_t n = co_await src_->read(std::span(scratch_));
            if (n == 0) {
                eof_ = true;
                break;
            }
            if (!degraded_ && !rc_->write(pos_, scratch_.data(), n)) {
                LOG_WARN("tiered: range cache write failed for {}/{}, passthrough only", bucket_,
                         key_);
                degraded_ = true;
            }
            // Client window intersection
            uint64_t s = pos_, e = pos_ + n - 1;
            if (e >= f_ && s <= l_) {
                uint64_t from = std::max(s, f_), to = std::min(e, l_);
                pending_.insert(pending_.end(), scratch_.begin() + (from - s),
                                scratch_.begin() + (to - s + 1));
            }
            pos_ += n;
        }
        if (pending_.empty()) {
            finish();
            co_return 0;
        }
        size_t take = std::min(buf.size(), pending_.size());
        std::copy(pending_.begin(), pending_.begin() + take, buf.begin());
        pending_.erase(pending_.begin(), pending_.begin() + take);
        co_return take;
    }
    std::optional<uint64_t> length() const override { return l_ - f_ + 1; }

private:
    void finish() {
        if (finished_) return;
        finished_ = true;
        // Every block of [af, al] arrived in full → publish; a short stream leaves the
        // bitmap untouched (the sparse bytes stay harmless, never served)
        if (!degraded_ && pos_ == al_ + 1) {
            uint64_t bs = rc_->block_size();
            rc_->mark_present(af_ / bs, al_ / bs);
        }
        release();
    }
    void release() {
        if (released_) return;
        released_ = true;
        owner_->inflight_end(ikey_rc(bucket_, key_));
    }

    std::shared_ptr<TieredBackend> owner_;
    std::string bucket_, key_;
    std::unique_ptr<tier::IRangeCache> rc_;
    std::unique_ptr<http::BodyReader> src_;
    uint64_t af_, al_, f_, l_, pos_;
    std::vector<std::byte> scratch_;
    std::vector<std::byte> pending_;
    bool eof_ = false, degraded_ = false, finished_ = false, released_ = false;
};

// ---------- Construction / configuration ----------

TieredBackend::TieredBackend(std::shared_ptr<LocalFsBackend> local,
                             std::shared_ptr<IStorageBackend> cloud,
                             std::shared_ptr<ThreadPool> pool, TieredConfig cfg,
                             MetricsScope metrics)
    : TieredBackend(std::make_shared<tier::LocalFsTierLocal>(std::move(local)), std::move(cloud),
                    std::move(pool), std::move(cfg), std::move(metrics)) {}

TieredBackend::TieredBackend(std::shared_ptr<tier::ITierLocal> local,
                             std::shared_ptr<IStorageBackend> cloud,
                             std::shared_ptr<ThreadPool> pool, TieredConfig cfg,
                             MetricsScope metrics)
    : local_(std::move(local)), cloud_(std::move(cloud)), pool_(std::move(pool)), cfg_(cfg),
      tier_dir_(local_->state_dir()), gc_dir_(tier_dir_ / "gc"), wheel_dir_(tier_dir_ / "wheel"),
      quarantine_dir_(tier_dir_ / "quarantine"),
      transfers_(std::max(1, cfg.max_concurrent_transfers), &pool_exec_) {
    init_metrics(metrics);
    fs::create_directories(gc_dir_);
    fs::create_directories(wheel_dir_);
    fs::create_directories(quarantine_dir_);
    key_locks_.reserve(kLockStripes);
    for (size_t i = 0; i < kLockStripes; ++i)
        key_locks_.push_back(std::make_unique<AsyncSemaphore>(1, &pool_exec_));

    // The GC sequence continues from existing entries; no wraparound after restart
    uint64_t next_seq = 0;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(gc_dir_, ec)) {
        std::string name = e.path().filename().string();
        uint64_t v = 0;
        std::from_chars(name.data(), name.data() + name.size(), v);
        next_seq = std::max(next_seq, v + 1);
    }
    gc_seq_ = next_seq;
    refresh_quarantine_gauges();
    if (cfg_.range_cache && !local_->supports_range_cache())
        LOG_WARN("tiered: range_cache requested but the {} local side has no block cache; "
                 "Range GETs on remote objects pass through",
                 local_->kind());

    schedule_scan();
    schedule_flush();
    schedule_reconcile();
}

void TieredBackend::init_metrics(const MetricsScope& metrics) {
    // Pre-register the full op dimension (same rationale as duostore's reason bucketing):
    // a missing series reads as "no data" in Prometheus, not "zero". Only the four ops
    // where tiered has tiering logic are wired -- adding identical counters on purely
    // delegated paths would just duplicate the local side's own series
    static constexpr std::array<const char*, kOpCount> kOpNames = {"get", "put", "delete",
                                                                   "list"};
    for (size_t i = 0; i < kOpCount; ++i) {
        m_ops_[i] = metrics.counter("lights3_tiered_ops_total",
                                    "Tier-aware operations finished (success and failure)",
                                    {{"op", kOpNames[i]}});
        m_op_errors_[i] = metrics.counter(
            "lights3_tiered_op_errors_total",
            "Tier-aware operations that exited via an error (any exception, incl. client 4xx)",
            {{"op", kOpNames[i]}});
    }
    // GET source split: a rising cloud share = worsening cache hit rate (overly aggressive
    // coldness threshold or insufficient capacity) -- tiered's most central health signal
    const char* src_help =
        "GET data source: served from local/cached data vs streamed through from the cloud";
    m_get_local_ = metrics.counter("lights3_tiered_get_source_total", src_help,
                                   {{"source", "local"}});
    m_get_cloud_ = metrics.counter("lights3_tiered_get_source_total", src_help,
                                   {{"source", "cloud"}});
    m_demoted_ = metrics.counter("lights3_tiered_demoted_objects_total",
                                 "Objects demoted to the cloud tier (stub committed)");
    m_promoted_ = metrics.counter(
        "lights3_tiered_promoted_objects_total",
        "Objects rehydrated into the local cache (explicit promote + GET tee fill)");
    m_scan_duration_ = metrics.histogram("lights3_tiered_scan_seconds",
                                         "Wall time of a completed scan round",
                                         {0.1, 1, 5, 30, 120, 600});
    // GC observability trade-offs: runs/removed/failed are event counts (monotonic,
    // counters); deferred is "how many entries were still in backoff this round" -- the
    // same entry reappears every round, so accumulation would inflate; following duostore's
    // skipped-class precedent, store the per-round observation in a gauge. resolved gets no
    // separate series: resolved beyond removed is mostly entry invalidation / the cloud
    // never having it, with no distinct operational action attached
    m_gc_runs_ = metrics.counter("lights3_tiered_gc_runs_total",
                                 "Completed GC rounds over the orphan-copy queue");
    m_gc_removed_ = metrics.counter("lights3_tiered_gc_removed_cloud_total",
                                    "Orphan cloud copies actually deleted by GC");
    m_gc_failed_ = metrics.counter(
        "lights3_tiered_gc_failed_total",
        "GC delete attempts that failed and were re-queued with exponential backoff");
    m_gc_deferred_ = metrics.gauge(
        "lights3_tiered_gc_deferred",
        "Queue entries still in backoff as of the last GC round (not yet retried)");
    // roadmap §3.6: scan mode split, eviction volume, access flushes, range cache, quarantine
    const char* scan_help = "Completed scan rounds by mode";
    m_scan_full_ = metrics.counter("lights3_tiered_scan_rounds_total", scan_help, {{"mode", "full"}});
    m_scan_incr_ = metrics.counter("lights3_tiered_scan_rounds_total", scan_help,
                                   {{"mode", "incremental"}});
    m_evicted_bytes_ = metrics.counter("lights3_tiered_evicted_bytes_total",
                                       "Local bytes released by watermark eviction (launched)");
    m_access_flushed_ = metrics.counter("lights3_tiered_access_records_flushed_total",
                                        "Access records persisted from the write-behind buffer");
    const char* rc_help = "Range GETs on remote objects by block-cache outcome";
    m_rcache_hit_ = metrics.counter("lights3_tiered_range_cache_total", rc_help, {{"result", "hit"}});
    m_rcache_fill_ = metrics.counter("lights3_tiered_range_cache_total", rc_help, {{"result", "fill"}});
    m_rcache_pass_ = metrics.counter("lights3_tiered_range_cache_total", rc_help,
                                     {{"result", "passthrough"}});
    const char* q_help = "Reconciliation findings currently held in the quarantine ledger";
    m_q_refs_missing_ = metrics.gauge("lights3_tiered_quarantine_entries", q_help,
                                      {{"kind", "refs_missing"}});
    m_q_foreign_ = metrics.gauge("lights3_tiered_quarantine_entries", q_help, {{"kind", "foreign"}});
    // Local-tier capacity (backlog-sequence ①): the numbers the space watermark is
    // measured against, read at render time. Callbacks capture the tier-local adapter
    // and the shared books estimate rather than this backend, so a registry that
    // outlives the backend renders zeros instead of touching freed memory
    {
        std::shared_ptr<tier::ITierLocal> local = local_;
        const double high = cfg_.space_high_watermark;
        const double quota = double(cfg_.quota_bytes);
        std::shared_ptr<std::atomic<int64_t>> est = local_bytes_est_;
        metrics.gauge_callback(
            "lights3_tiered_local_used_bytes",
            "Bytes used on the filesystem holding the local tier (statvfs, everything an "
            "unprivileged writer cannot use); what space_high_watermark is measured against",
            [local] {
                auto s = local->space_usage();
                return s ? double(s->used_bytes) : 0.0;
            });
        metrics.gauge_callback("lights3_tiered_local_total_bytes",
                               "Size of the filesystem holding the local tier (statvfs)",
                               [local] {
                                   auto s = local->space_usage();
                                   return s ? double(s->total_bytes) : 0.0;
                               });
        metrics.gauge_callback(
            "lights3_tiered_local_high_watermark_bytes",
            "space_high_watermark expressed in bytes of the local filesystem; used above "
            "this = every scan evicts",
            [local, high] {
                auto s = local->space_usage();
                return s ? high * double(s->total_bytes) : 0.0;
            });
        metrics.gauge_callback(
            "lights3_tiered_local_cached_bytes",
            "Object bytes the tier books as resident locally (write-path estimate, "
            "calibrated by every full scan; 0 until the first calibration)",
            [est] { return double(std::max<int64_t>(0, est->load(std::memory_order_relaxed))); });
        metrics.gauge_callback("lights3_tiered_local_quota_bytes",
                               "Logical quota_bytes of the local tier (0 = no quota)",
                               [quota] { return quota; });
    }
}

void TieredBackend::record_op(Op op, bool ok) {
    size_t i = size_t(op);
    m_ops_[i]->inc();
    if (!ok) m_op_errors_[i]->inc();
}

TieredBackend::~TieredBackend() {
    bg_.begin_close();
    // cancel outside the group lock (callbacks take the group lock, see close()); after
    // blocking until in-flight callbacks return, the raw `this` held by background
    // coroutines is no longer referenced, and once the count reaches zero destruction is safe
    TimerQueue::instance().cancel(scan_timer_);
    TimerQueue::instance().cancel(flush_timer_);
    TimerQueue::instance().cancel(reconcile_timer_);
    bg_.wait_idle();
}

std::shared_ptr<TieredBackend> TieredBackend::from_config(
    const BackendConfig& cfg, const std::map<std::string, std::shared_ptr<IStorageBackend>>& built,
    std::shared_ptr<ThreadPool> pool, MetricsScope metrics) {
    auto param = [&](const char* k) -> std::string {
        auto it = cfg.params.find(k);
        return it == cfg.params.end() ? std::string{} : it->second;
    };
    auto parse_pct = [&](const std::string& s) {
        std::string t = s;
        bool suffixed = !t.empty() && t.back() == '%';
        if (suffixed) t.pop_back();
        double v = std::stod(t);
        // The "%" suffix must participate in the decision: "1%" means 1%. The old code
        // dropped the suffix, so 1.0 did not trigger the /100, a 1% low watermark parsed as
        // 100%, and (used-low) went negative, wrapped around, and demoted the entire bucket
        // (docs/archive/gaps.md §3.9). "85%"/"85" and "0.85" are all accepted
        if (suffixed || v > 1.0) v /= 100.0;
        if (!(v > 0.0 && v <= 1.0))
            throw std::runtime_error("tiered backend '" + cfg.name + "': watermark '" + s +
                                     "' out of range (0, 100%]");
        return v;
    };

    std::string local_name = param("local"), cloud_name = param("cloud");
    if (local_name.empty() || cloud_name.empty())
        throw std::runtime_error("tiered backend '" + cfg.name + "' needs local + cloud");
    auto local_be = built.at(local_name);
    auto cloud = built.at(cloud_name);
    if (cloud.get() == local_be.get())
        throw std::runtime_error("tiered backend '" + cfg.name + "': local and cloud must differ");
    // Local side adapters (roadmap §3.6 ⑥): localfs/xlocalfs share the disk layout
    // adapter; duostore keeps tier state in its meta engine
    std::shared_ptr<tier::ITierLocal> local;
    if (auto lf = std::dynamic_pointer_cast<LocalFsBackend>(local_be))
        local = std::make_shared<tier::LocalFsTierLocal>(std::move(lf));
#ifdef LIGHTS3_DUOSTORE
    else if (auto duo = std::dynamic_pointer_cast<DuoStoreBackend>(local_be))
        local = std::make_shared<tier::DuoStoreTierLocal>(std::move(duo));
#endif
    if (!local)
        throw std::runtime_error("tiered backend '" + cfg.name + "': local '" + local_name +
                                 "' must be a localfs/xlocalfs or duostore backend "
                                 "(docs/tiered-storage.md §2)");

    TieredConfig tc;
    if (auto v = param("cold_after"); !v.empty()) tc.cold_after_sec = parse_duration_sec(v);
    if (auto v = param("scan_interval"); !v.empty()) tc.scan_interval_sec = parse_duration_sec(v);
    if (auto v = param("space_high_watermark"); !v.empty()) tc.space_high_watermark = parse_pct(v);
    if (auto v = param("space_low_watermark"); !v.empty()) tc.space_low_watermark = parse_pct(v);
    if (auto v = param("min_free_bytes"); !v.empty()) tc.min_free_bytes = parse_size(v);
    if (auto v = param("cache_fill_on_range"); !v.empty())
        tc.cache_fill_on_range = !(v == "false" || v == "0" || v == "off");
    if (auto v = param("max_concurrent_transfers"); !v.empty())
        tc.max_concurrent_transfers = std::stoi(v);
    if (auto v = param("quota_bytes"); !v.empty()) tc.quota_bytes = parse_size(v);
    if (auto v = param("gc_retry_base"); !v.empty()) tc.gc_retry_base_sec = parse_duration_sec(v);
    if (auto v = param("gc_retry_cap"); !v.empty()) tc.gc_retry_cap_sec = parse_duration_sec(v);
    if (auto v = param("reconcile_interval"); !v.empty())
        tc.reconcile_interval_sec = parse_duration_sec(v);
    if (auto v = param("reconcile_orphans"); !v.empty()) {
        if (v == "rebuild") tc.reconcile_delete_orphans = false;
        else if (v == "delete") tc.reconcile_delete_orphans = true;
        else
            throw std::runtime_error("tiered backend '" + cfg.name +
                                     "': reconcile_orphans must be rebuild|delete");
    }
    // roadmap §3.6 knobs
    if (auto v = param("full_scan_interval"); !v.empty())
        tc.full_scan_interval_sec = parse_duration_sec(v);
    if (auto v = param("evict_size_weight"); !v.empty()) tc.evict_size_weight = std::stod(v);
    if (auto v = param("evict_frequency_weight"); !v.empty())
        tc.evict_frequency_weight = std::stod(v);
    if (auto v = param("access_buffer_max"); !v.empty())
        tc.access_buffer_max = size_t(parse_size(v));
    if (auto v = param("range_cache"); !v.empty()) tc.range_cache = parse_bool(v);
    if (auto v = param("range_cache_block"); !v.empty()) tc.range_cache_block = parse_size(v);
    // rules.N.match / rules.N.cold_after (config.cc flattens the YAML list); "never" pins
    for (int i = 0;; ++i) {
        std::string pfx = "rules." + std::to_string(i) + ".";
        std::string glob = param((pfx + "match").c_str());
        std::string ca = param((pfx + "cold_after").c_str());
        if (glob.empty() && ca.empty()) break;
        if (glob.empty() || ca.empty())
            throw std::runtime_error("tiered backend '" + cfg.name + "': rules[" +
                                     std::to_string(i) + "] needs match + cold_after");
        TierRule r{glob, ca == "never" || ca == "pin" ? -1 : parse_duration_sec(ca)};
        tc.rules.push_back(std::move(r));
    }
    if (tc.gc_retry_base_sec < 1 || tc.gc_retry_cap_sec < tc.gc_retry_base_sec)
        throw std::runtime_error("tiered backend '" + cfg.name +
                                 "': gc_retry_base must be >= 1s and <= gc_retry_cap");
    if (tc.space_low_watermark > tc.space_high_watermark)
        throw std::runtime_error("tiered backend '" + cfg.name + "': low watermark > high");
    if (tc.evict_size_weight < 0 || tc.evict_frequency_weight < 0)
        throw std::runtime_error("tiered backend '" + cfg.name + "': evict weights must be >= 0");
    if (tc.range_cache_block < 64 * 1024 || tc.range_cache_block > (1ull << 30))
        throw std::runtime_error("tiered backend '" + cfg.name +
                                 "': range_cache_block must be in [64KiB, 1GiB]");
    return std::make_shared<TieredBackend>(std::move(local), std::move(cloud), std::move(pool),
                                           tc, std::move(metrics));
}

int64_t TieredBackend::cold_after_for(std::string_view bucket, std::string_view key) const {
    if (!cfg_.rules.empty()) {
        std::string ik = make_ikey(bucket, key);
        for (const auto& r : cfg_.rules)
            if (::fnmatch(r.glob.c_str(), ik.c_str(), 0) == 0) return r.cold_after_sec;
    }
    return cfg_.cold_after_sec;
}

// ---------- bucket: delegated to local ----------

Task<void> TieredBackend::create_bucket(std::string_view bucket) {
    co_return co_await local_->backend().create_bucket(bucket);
}
Task<void> TieredBackend::delete_bucket(std::string_view bucket) {
    // local being empty means no objects (a stub is also an object); the cloud may retain
    // orphan replicas pending GC, converged by reconciliation
    co_return co_await local_->backend().delete_bucket(bucket);
}
Task<bool> TieredBackend::bucket_exists(std::string_view bucket) {
    co_return co_await local_->backend().bucket_exists(bucket);
}
Task<std::vector<BucketInfo>> TieredBackend::list_buckets() {
    co_return co_await local_->backend().list_buckets();
}

// ---------- object ----------

Task<ObjectStream> TieredBackend::get_object(std::string_view bucket, std::string_view key,
                                             std::optional<ByteRange> range) {
    OpGuard g{this, Op::kGet};
    // tiered reads local state itself, so it cannot rely solely on the local side's entry
    // validation
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    for (int attempt = 0;; ++attempt) {
        co_await pool_->schedule();
        auto obj = local_->read(bucket, key);  // absent: let local distinguish NoSuchBucket/NoSuchKey
        if (obj) touch(bucket, key);

        if (!obj || obj->tier.tier != Tier::kRemote) {
            try {
                auto os = co_await local_->backend().get_object(bucket, key, range);
                // Source counting after success: a StubRace retry that switches to the
                // cloud leaves no half-counted local entry
                m_get_local_->inc();
                g.ok = true;
                co_return os;
            } catch (const fsutil::StubRace&) {
                if (attempt >= 2) throw;
                continue;  // open raced with stubbing: re-read the tier and go to the cloud
            }
        }
        const ObjectMeta& m = obj->meta;
        const TierInfo& t = obj->tier;

        // remote + Range: block cache (roadmap §3.6 ⑦) when enabled, else passthrough
        // (docs/tiered-storage.md §6.3); external meta is always the local original
        if (range && cfg_.range_cache && local_->supports_range_cache() && m.size > 0) {
            auto [f, l] = resolve_range(*range, m.size);  // InvalidRange here matches what the cloud would say
            auto rc = local_->open_range_cache(bucket, key, *obj, cfg_.range_cache_block);
            if (rc && rc->has(f, l)) {
                if (auto body = rc->open(f, l)) {
                    ObjectStream out;
                    out.meta = m;
                    out.range = ByteRange{f, l};
                    out.body = std::move(body);
                    m_get_local_->inc();
                    m_rcache_hit_->inc();
                    g.ok = true;
                    co_return out;
                }
            }
            if (rc) {
                uint64_t bs = rc->block_size();
                uint64_t af = f / bs * bs;
                uint64_t al = std::min(m.size - 1, (l / bs + 1) * bs - 1);
                std::string rk = ikey_rc(bucket, key);
                if (cache_space_ok(al - af + 1) && inflight_try_begin(rk)) {
                    ObjectStream cs;
                    try {
                        cs = co_await cloud_->get_object(bucket, key, ByteRange{af, al});
                    } catch (...) {
                        inflight_end(rk);
                        throw;
                    }
                    m_get_cloud_->inc();
                    m_rcache_fill_->inc();
                    ObjectStream out;
                    out.meta = m;
                    out.range = ByteRange{f, l};
                    out.body = std::make_unique<RangeTeeReader>(
                        shared_from_this(), std::string(bucket), std::string(key), std::move(rc),
                        std::move(cs.body), af, al, f, l);
                    g.ok = true;
                    co_return out;
                }
                m_rcache_pass_->inc();
            }
        }

        ObjectStream cs = co_await cloud_->get_object(bucket, key, range);
        m_get_cloud_->inc();
        ObjectStream out;
        out.meta = m;
        out.range = cs.range;
        if (range) {
            out.body = std::move(cs.body);  // Range does no whole-object caching (§6.3)
            if (cfg_.cache_fill_on_range)
                bg_.spawn(promote_quiet(std::string(bucket), std::string(key)));
            g.ok = true;
            co_return out;
        }
        std::string ikey = make_ikey(bucket, key);
        if (m.size > 0 && cache_space_ok(m.size) && inflight_try_begin(ikey)) {
            auto fill = local_->begin_cache_fill(bucket, key);
            if (!fill) {  // cannot open a fill: pure-passthrough fallback (requirement 3)
                inflight_end(ikey);
                out.body = std::move(cs.body);
            } else {
                out.body = std::make_unique<TeeCacheReader>(shared_from_this(),
                                                            std::string(bucket), std::string(key),
                                                            m, t, std::move(cs.body),
                                                            std::move(fill));
            }
        } else {
            out.body = std::move(cs.body);  // insufficient space or a fill already in flight: pure passthrough
        }
        g.ok = true;
        co_return out;
    }
}

Task<PutResult> TieredBackend::put_object(std::string_view bucket, std::string_view key,
                                          ObjectMeta meta, http::BodyReader& body,
                                          PutCondition cond) {
    OpGuard g{this, Op::kPut};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    co_await pool_->schedule();
    TierInfo prior = local_->read_tier_only(bucket, key);
    int64_t prior_size = 0;
    if (auto o = local_->read(bucket, key)) prior_size = int64_t(o->local_bytes);
    // Conditions pass through to local: a stub keeps the original etag (HEAD is fully
    // local, §6.1), and the check inside local's commit section is equally authoritative
    // for demoted objects
    auto r = co_await local_->backend().put_object(bucket, key, std::move(meta), body, cond);
    touch(bucket, key);
    if (auto o = local_->read(bucket, key)) note_local_delta(int64_t(o->local_bytes) - prior_size);
    maybe_kick_quota_scan();
    // write-back: new data lands only locally and the tier returns to local; the old cloud
    // replica becomes an orphan (§7.1) and any partial block cache is stale
    if (prior.tier != Tier::kLocal) {
        enqueue_gc(bucket, key, prior.remote_etag);
        local_->drop_range_cache(bucket, key);
    }
    g.ok = true;
    co_return r;
}

Task<std::optional<ObjectLayout>> TieredBackend::inspect_object(std::string_view bucket,
                                                                std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    // The local engine raises NoSuchBucket/NoSuchKey and hops to the pool; its layout
    // is appended under local.* after the tiering view
    auto inner = co_await local_->backend().inspect_object(bucket, key);
    auto lo = local_->read(bucket, key);
    ObjectLayout L;
    L.engine = "tiered";
    auto& a = L.attrs;
    if (lo) {
        a.emplace_back("tier", lo->tier.tier == Tier::kLocal    ? "local"
                               : lo->tier.tier == Tier::kRemote ? "remote"
                                                                : "cached");
        a.emplace_back("logical_size", std::to_string(lo->meta.size));
        a.emplace_back("local_bytes", std::to_string(lo->local_bytes));
        a.emplace_back("local_mtime", std::to_string(lo->mtime));
        if (lo->tier.tier != Tier::kLocal) {
            a.emplace_back("remote_etag", lo->tier.remote_etag);
            a.emplace_back("remote_at", lo->tier.remote_at);
        }
    }
    if (inner) {
        a.emplace_back("local_engine", inner->engine);
        for (auto& [k, v] : inner->attrs) a.emplace_back("local." + k, v);
        L.extents = std::move(inner->extents);
    }
    co_return L;
}

Task<ObjectMeta> TieredBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    // The stub's metadata is complete, so HEAD finishes entirely locally (§6.1)
    auto m = co_await local_->backend().head_object(bucket, key);
    touch(bucket, key);
    co_return m;
}

Task<void> TieredBackend::delete_object(std::string_view bucket, std::string_view key) {
    OpGuard g{this, Op::kDelete};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    co_await pool_->schedule();
    TierInfo prior = local_->read_tier_only(bucket, key);
    if (auto o = local_->read(bucket, key)) note_local_delta(-int64_t(o->local_bytes));
    co_await local_->backend().delete_object(bucket, key);
    forget_access(bucket, key);
    // The response does not wait on the cloud: the cloud replica goes to GC for async deletion (§7.2)
    if (prior.tier != Tier::kLocal) {
        enqueue_gc(bucket, key, prior.remote_etag);
        local_->drop_range_cache(bucket, key);
    }
    g.ok = true;
    co_return;
}

Task<ListResult> TieredBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    OpGuard g{this, Op::kList};
    validate_bucket_name(bucket, kAllowReserved);
    auto r = co_await local_->backend().list_objects(bucket, opt);  // stubs list with their logical size
    g.ok = true;
    co_return r;
}

// ---------- multipart: delegated to local ----------

Task<std::string> TieredBackend::create_multipart(std::string_view bucket, std::string_view key,
                                                  ObjectMeta meta) {
    co_return co_await local_->backend().create_multipart(bucket, key, std::move(meta));
}
Task<void> TieredBackend::set_object_tagging(std::string_view bucket, std::string_view key,
                                             std::string tagging) {
    // Meta lives on the local side even for demoted stubs — pure delegation
    co_return co_await local_->backend().set_object_tagging(bucket, key, std::move(tagging));
}

Task<PutResult> TieredBackend::upload_part(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id, int part_no,
                                           http::BodyReader& body,
                                           const std::optional<PartChecksum>& checksum) {
    co_return co_await local_->backend().upload_part(bucket, key, upload_id, part_no, body,
                                                     checksum);
}
Task<PutResult> TieredBackend::complete_multipart(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  std::span<const PartInfo> parts) {
    validate_object_key(key);
    co_await pool_->schedule();
    TierInfo prior = local_->read_tier_only(bucket, key);
    auto r = co_await local_->backend().complete_multipart(bucket, key, upload_id, parts);
    touch(bucket, key);
    if (prior.tier != Tier::kLocal) {  // same as PUT overwrite
        enqueue_gc(bucket, key, prior.remote_etag);
        local_->drop_range_cache(bucket, key);
    }
    co_return r;
}
Task<void> TieredBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                          std::string_view upload_id) {
    co_return co_await local_->backend().abort_multipart(bucket, key, upload_id);
}
Task<ListPartsResult> TieredBackend::list_parts(std::string_view bucket, std::string_view key,
                                                std::string_view upload_id,
                                                const ListPartsOptions& opt) {
    co_return co_await local_->backend().list_parts(bucket, key, upload_id, opt);
}
Task<ListUploadsResult> TieredBackend::list_multipart_uploads(std::string_view bucket,
                                                              const ListUploadsOptions& opt) {
    co_return co_await local_->backend().list_multipart_uploads(bucket, opt);
}

// ---------- Demotion (docs/tiered-storage.md §5) ----------

Task<void> TieredBackend::demote_object(std::string bucket, std::string key) {
    auto permit = co_await transfers_.acquire();  // max_concurrent_transfers throttle
    co_await pool_->schedule();
    auto o0 = local_->read(bucket, key);
    if (!o0) co_return;  // gone
    const ObjectMeta m0 = o0->meta;
    const TierInfo t0 = o0->tier;

    if (t0.tier == Tier::kRemote) {
        // Crash recovery (between §5.2 b/c): the record already says remote but the data
        // was not reclaimed -> finish the stubbing
        if (o0->local_bytes > 0) {
            auto lk = co_await key_lock(bucket, key).acquire();
            co_await pool_->schedule();  // the lock wakeup may resume on another thread; blocking IO goes back to the pool
            auto o1 = local_->read(bucket, key);
            if (o1 && o1->tier.tier == Tier::kRemote && o1->local_bytes > 0)
                co_await local_->commit_stub(bucket, key, o1->meta, o1->tier);
        }
        co_return;
    }

    std::string ikey = make_ikey(bucket, key);
    if (!inflight_try_begin(ikey)) co_return;  // demotion/fill already in flight (also blocks erroneous GC deletion)
    InflightRelease rel{this, ikey};

    std::string remote_etag, remote_at;
    if (t0.tier == Tier::kCached) {
        // cached -> remote: if the cloud replica is still valid, stub with zero traffic (end of §5.2)
        try {
            auto cm = co_await cloud_->head_object(bucket, key);
            if (std::string(strip_etag_quotes(cm.etag)) == t0.remote_etag) {
                remote_etag = t0.remote_etag;
                remote_at = t0.remote_at;
            }
        } catch (const S3Error&) {
            // Cloud replica invalid/unreachable: do a full upload
        }
    }

    if (remote_etag.empty()) {
        co_await ensure_cloud_bucket(bucket);
        // Step 1 snapshot + step 2 streaming upload (no extra memory), recomputing MD5
        // synchronously for step 3 verification
        HashingReader body(co_await local_->open_snapshot(bucket, key, m0.size));
        ObjectMeta cloud_meta = m0;
        // Store a redundant copy of the original meta in the cloud so a lost local stub can
        // be rebuilt by reconciliation (§4.2/§9)
        cloud_meta.user_meta["lights3-etag"] = m0.etag;
        cloud_meta.user_meta["lights3-content-type"] = m0.content_type;
        PutResult pr = co_await cloud_->put_object(bucket, key, std::move(cloud_meta), body);

        std::string md5 = body.md5_hex();
        std::string cloud_etag(strip_etag_quotes(pr.etag));
        bool multipart = m0.etag.find('-') != std::string::npos;
        bool ok = body.bytes() == m0.size && (multipart || md5 == m0.etag);
        // When the cloud returns a single-part etag (pure MD5), transfer integrity can be
        // checked once more
        if (ok && cloud_etag.find('-') == std::string::npos && cloud_etag != md5) ok = false;
        if (!ok) {  // object overwritten during upload or cloud verification failed: replica goes to GC, give up this round
            enqueue_gc(bucket, key, cloud_etag);
            co_return;
        }
        remote_etag = cloud_etag;
        remote_at = util::iso8601(std::chrono::system_clock::now());
    }

    // Step 4 commit under the per-key lock: re-verify we were not beaten by a concurrent
    // write, then stub
    auto lk = co_await key_lock(bucket, key).acquire();
    co_await pool_->schedule();  // as above: the critical section is synchronous IO, must be on a pool thread
    auto o1 = local_->read(bucket, key);
    if (!o1) {  // DELETEd in the meantime: DELETE wins (§7.3)
        enqueue_gc(bucket, key, remote_etag);
        co_return;
    }
    if (o1->meta.etag != m0.etag) {  // overwritten by PUT in the meantime: PUT wins (§7.3)
        enqueue_gc(bucket, key, remote_etag);
        co_return;
    }
    if (o1->tier.tier == Tier::kRemote) co_return;  // already stubbed by someone else
    co_await local_->commit_stub(bucket, key, o1->meta,
                                 TierInfo{Tier::kRemote, remote_etag, remote_at});
    forget_access(bucket, key);  // a stub needs no access record until it is touched again
    // Count only when the stub actually lands; the crash-recovery stub completion above
    // does not count -- that demotion was already counted before the crash, and re-counting
    // after restart would inflate
    m_demoted_->inc();
    co_return;
}

// ---------- Promotion (background whole-object promotion for Range GETs + test hook, docs/tiered-storage.md §6.3) ----------

Task<void> TieredBackend::promote_object(std::string bucket, std::string key) {
    auto permit = co_await transfers_.acquire();
    co_await pool_->schedule();
    auto o0 = local_->read(bucket, key);
    if (!o0 || o0->tier.tier != Tier::kRemote) co_return;
    const ObjectMeta m0 = o0->meta;
    const TierInfo t0 = o0->tier;
    if (m0.size > 0 && !cache_space_ok(m0.size)) co_return;  // insufficient space: give up (requirement 3)
    std::string ikey = make_ikey(bucket, key);
    if (!inflight_try_begin(ikey)) co_return;  // single-flight (§6.4)
    InflightRelease rel{this, ikey};

    ObjectStream cs = co_await cloud_->get_object(bucket, key, std::nullopt);
    auto fill = local_->begin_cache_fill(bucket, key);
    if (!fill) co_return;

    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t written = 0;
    std::vector<std::byte> buf(256 * 1024);
    for (;;) {
        size_t n = co_await cs.body->read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
        if (!fill->write(buf.data(), n)) co_return;  // ENOSPC etc.: the fill's RAII discards
        written += n;
    }

    bool multipart = m0.etag.find('-') != std::string::npos;
    if (written != m0.size || (!multipart && md5.final_hex() != m0.etag)) {
        LOG_WARN("tiered: promote checksum mismatch for {}/{}, dropped", bucket, key);
        co_return;
    }
    co_await commit_cache_fill(bucket, key, m0, t0, *fill);
    co_return;
}

Task<void> TieredBackend::commit_cache_fill(std::string bucket, std::string key,
                                            ObjectMeta expect, TierInfo expect_tier,
                                            tier::ICacheFill& fill) {
    auto lk = co_await key_lock(bucket, key).acquire();
    // Critical path (docs/archive/gaps.md §2.4): this function is co_awaited by TeeCacheReader when
    // the client reads EOF; without switching back to a pool thread, the whole commit
    // (renames + fsyncs) would land directly on the HTTP response thread
    co_await pool_->schedule();
    auto o1 = local_->read(bucket, key);
    if (!o1) co_return;  // DELETEd in the meantime: discard the fill (§7.3)
    // Re-verify it is still the same remote version; discard if overwritten by PUT or
    // already filled (user writes win)
    if (o1->tier.tier != Tier::kRemote || o1->meta.etag != expect.etag ||
        o1->tier.remote_etag != expect_tier.remote_etag)
        co_return;
    co_await fill.commit(o1->meta,
                         TierInfo{Tier::kCached, o1->tier.remote_etag, o1->tier.remote_at});
    touch(bucket, key);
    // The counter sits at the commit point rather than promote_object's exit: this is the
    // single junction where "data really returned to local" (shared by explicit promotion
    // and GET Tee fill), and paths that fail re-verification and discard the fill naturally
    // do not count
    m_promoted_->inc();
    co_return;
}

// ---------- Access records + time wheel (docs/tiered-storage.md §4.3/§5.1) ----------

void TieredBackend::touch(std::string_view bucket, std::string_view key) {
    const int64_t now = ::time(nullptr);
    bool kick;
    {
        std::lock_guard lk(access_m_);
        auto& t = access_dirty_[make_ikey(bucket, key)];
        t.atime = now;
        if (t.hits < UINT32_MAX) ++t.hits;
        kick = access_dirty_.size() >= cfg_.access_buffer_max;
    }
    if (kick) maybe_kick_flush();  // bounded buffer: flush early rather than grow
}

void TieredBackend::forget_access(std::string_view bucket, std::string_view key) {
    {
        std::lock_guard lk(access_m_);
        access_dirty_.erase(make_ikey(bucket, key));
    }
    local_->erase_access(bucket, key);
}

AccessRec TieredBackend::access_of(std::string_view bucket, std::string_view key,
                                   int64_t fallback_mtime) {
    std::optional<Touch> pending;
    {
        std::lock_guard lk(access_m_);
        auto it = access_dirty_.find(make_ikey(bucket, key));
        if (it != access_dirty_.end()) pending = it->second;
    }
    AccessRec r;
    if (auto stored = local_->load_access(bucket, key)) {
        r = *stored;
    } else {
        r.atime = fallback_mtime;  // never recorded: the local record's mtime stands in
    }
    if (pending) {
        r.atime = std::max(r.atime, pending->atime);
        r.hits = uint32_t(std::min<uint64_t>(uint64_t(r.hits) + pending->hits, UINT32_MAX));
    }
    return r;
}

bool TieredBackend::persist_access(std::string_view bucket, std::string_view key,
                                   AccessRec rec) {
    bool appended = false;
    int64_t ca = cold_after_for(bucket, key);
    if (ca >= 0) {  // pinned keys are never enrolled: nothing will ever pick them up
        int64_t slot = wheel_slot_for(rec.atime, ca);
        if (slot != rec.enrolled) {
            wheel_append(slot, bucket, key);
            rec.enrolled = slot;
            appended = true;
        }
    }
    local_->store_access(bucket, key, rec);
    return appended;
}

void TieredBackend::flush_access_sync() {
    // Snapshot the pending touches without removing them (a coldness verdict taken
    // during the flush must still see them), persist, then drop exactly the entries that
    // did not move in the meantime
    std::vector<std::pair<std::string, Touch>> batch;
    {
        std::lock_guard lk(access_m_);
        batch.assign(access_dirty_.begin(), access_dirty_.end());
    }
    uint64_t flushed = 0;
    for (auto& [ik, t] : batch) {
        auto slash = ik.find('/');
        if (slash == std::string::npos) continue;
        std::string_view bucket(ik.data(), slash), key(ik.data() + slash + 1, ik.size() - slash - 1);
        AccessRec r;
        if (auto stored = local_->load_access(bucket, key)) r = *stored;
        r.atime = std::max(r.atime, t.atime);
        r.hits = uint32_t(std::min<uint64_t>(uint64_t(r.hits) + t.hits, UINT32_MAX));
        persist_access(bucket, key, r);
        ++flushed;
    }
    {
        std::lock_guard lk(access_m_);
        for (auto& [ik, t] : batch) {
            auto it = access_dirty_.find(ik);
            if (it != access_dirty_.end() && it->second.atime == t.atime && it->second.hits == t.hits)
                access_dirty_.erase(it);
        }
    }
    local_->flush_access();
    m_access_flushed_->inc(flushed);
}

Task<void> TieredBackend::flush_access() {
    co_await pool_->schedule();
    flush_access_sync();
}

void TieredBackend::wheel_append(int64_t slot, std::string_view bucket, std::string_view key) {
    // Keys that cannot be encoded on a TSV line are left to full rescans
    if (key.find('\t') != std::string_view::npos || key.find('\n') != std::string_view::npos) return;
    char name[32];
    std::snprintf(name, sizeof(name), "%020lld", static_cast<long long>(slot));
    std::string line = std::string(bucket) + "\t" + std::string(key) + "\n";
    std::lock_guard lk(wheel_m_);
    // O_APPEND, no fsync: a lost line after a crash only delays that key to the next full
    // rescan
    int fd = ::open((wheel_dir_ / name).c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return;
    ssize_t w = ::write(fd, line.data(), line.size());
    (void)w;
    ::close(fd);
}

std::vector<std::pair<int64_t, fs::path>> TieredBackend::wheel_slots() const {
    std::vector<std::pair<int64_t, fs::path>> out;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(wheel_dir_, ec)) {
        if (!e.is_regular_file()) continue;
        std::string name = e.path().filename().string();
        int64_t slot = 0;
        auto r = std::from_chars(name.data(), name.data() + name.size(), slot);
        if (r.ec != std::errc() || r.ptr != name.data() + name.size()) continue;  // consuming-* etc.
        out.emplace_back(slot, e.path());
    }
    std::sort(out.begin(), out.end());
    return out;
}

void TieredBackend::maybe_kick_flush() {
    if (flush_inflight_.exchange(true)) return;
    bool spawned = bg_.spawn(flush_task());
    if (!spawned) flush_inflight_.store(false);
}

Task<void> TieredBackend::flush_task() {
    struct Clear {
        std::atomic<bool>& f;
        ~Clear() { f.store(false); }
    } clear{flush_inflight_};
    co_await pool_->schedule();
    flush_access_sync();
}

// ---------- TierScanner (docs/tiered-storage.md §5.1) ----------

struct TieredBackend::ScanCtx {
    int64_t now = 0;
    TierScanStats st;
    std::set<std::string> chosen;      // launched this round (dedupe across passes)
    std::vector<Task<void>> batch;     // bounded coroutine frames (docs/archive/gaps.md §2.13)
    uint64_t local_bytes = 0;          // measured usage (full scan only)
    uint64_t cold_freed = 0;           // bytes the coldness-selected objects will free
    static constexpr size_t kScanBatch = 128;

    void pick(TieredBackend& self, const std::string& b, const std::string& k) {
        if (chosen.insert(TieredBackend::make_ikey(b, k)).second)
            batch.push_back(self.demote_quiet(b, k));
    }
};

Task<void> TieredBackend::drain_batch(ScanCtx& cx) {
    if (!cx.batch.empty()) {
        auto work = std::move(cx.batch);
        cx.batch.clear();
        co_await when_all(std::move(work));
    }
}

double TieredBackend::evict_score(const AccessRec& a, uint64_t size, int64_t now) const {
    double age = double(std::max<int64_t>(1, now - a.atime));
    double size_term = 1.0 + cfg_.evict_size_weight * std::log2(1.0 + double(size) / double(1 << 20));
    double freq_term = 1.0 + cfg_.evict_frequency_weight * double(a.hits);
    return age * size_term / freq_term;  // higher = better victim
}

// One candidate: coldness / crash recovery / wheel (re-)enrollment. The caller supplies
// the local state it already has (walk entry or a fresh read)
Task<void> TieredBackend::consider(ScanCtx& cx, const std::string& bucket, const std::string& key,
                                   int64_t from_slot) {
    const bool from_wheel = from_slot >= 0;
    auto o = local_->read(bucket, key);
    if (!o) {
        if (from_wheel) ++cx.st.stale;  // enrolled, since deleted
        co_return;
    }
    ++cx.st.walked;
    if (!from_wheel) cx.local_bytes += o->local_bytes;
    if (o->tier.tier == Tier::kRemote) {
        // Crash recovery: remote but the data was not reclaimed (demote_object finishes
        // the stubbing)
        if (o->local_bytes > 0) {
            cx.pick(*this, bucket, key);
            ++cx.st.recovered;
        }
        co_return;
    }
    int64_t ca = cold_after_for(bucket, key);
    if (ca < 0 || o->local_bytes == 0) co_return;  // pinned, or nothing to free
    AccessRec a = access_of(bucket, key, o->mtime);
    if (cx.now - a.atime >= ca) {  // trigger 1: coldness
        cx.pick(*this, bucket, key);
        ++cx.st.cold_picked;
        cx.cold_freed += o->local_bytes;
    } else {
        // Still hot: make sure the wheel brings it back at its deadline. The slot being
        // consumed is deleted afterwards, so a key that lives there must be re-appended
        // even if that is also its target slot (deadline later in the same hour)
        if (a.enrolled == from_slot) a.enrolled = -1;
        if (a.enrolled != wheel_slot_for(a.atime, ca)) {
            persist_access(bucket, key, a);
            ++cx.st.enrolled;
        }
    }
    if (cx.batch.size() >= ScanCtx::kScanBatch) co_await drain_batch(cx);
}

// Full enumeration (bootstrap, and every full_scan_interval): enrolls everything the
// wheel does not know about, recalibrates the quota books, finds half-done stubs and
// stale range-cache entries
Task<void> TieredBackend::scan_full(ScanCtx& cx) {
    auto walker = local_->walk();
    for (;;) {
        auto entries = co_await walker->next();
        if (entries.empty()) break;
        for (auto& e : entries) co_await consider(cx, e.bucket, e.key, /*from_slot=*/-1);
    }
    cx.local_bytes += local_->sweep_range_cache();
    local_bytes_est_->store(int64_t(cx.local_bytes), std::memory_order_relaxed);  // incremental-books calibration
    last_full_scan_ = cx.now;
}

// Wheel round (roadmap §3.6 ①): only the slots whose deadline has passed are read;
// each candidate is verified against its current access record (a touch since
// enrollment re-enrolls it further out), so the round costs O(activity), not O(objects)
Task<void> TieredBackend::scan_incremental(ScanCtx& cx) {
    const int64_t due = cx.now / kWheelSlotSec;
    std::set<std::string> seen;
    std::error_code ec;
    // A due slot is renamed aside before it is read: re-enrollments made while consuming
    // it (a hot key whose deadline is later in the same hour) go to a fresh file of the
    // same slot id instead of the one about to be deleted. Leftover "consuming-*" files
    // from a crash mid-round are processed first
    std::vector<std::pair<int64_t, fs::path>> work;
    for (auto& e : fs::directory_iterator(wheel_dir_, ec)) {
        std::string name = e.path().filename().string();
        if (name.rfind("consuming-", 0) != 0) continue;
        int64_t slot = 0;
        std::from_chars(name.data() + 10, name.data() + name.size(), slot);
        work.emplace_back(slot, e.path());
    }
    for (auto& [slot, path] : wheel_slots()) {
        if (slot > due) break;
        fs::path aside = wheel_dir_ / ("consuming-" + path.filename().string());
        {
            std::lock_guard lk(wheel_m_);  // no append may straddle the rename
            fs::rename(path, aside, ec);
        }
        if (ec) continue;
        work.emplace_back(slot, aside);
    }
    for (auto& [slot, path] : work) {
        std::ifstream in(path, std::ios::binary);
        std::string line;
        while (std::getline(in, line)) {
            auto tab = line.find('\t');
            if (tab == std::string::npos) continue;
            std::string bucket = line.substr(0, tab), key = line.substr(tab + 1);
            std::string ik = make_ikey(bucket, key);
            if (cx.chosen.count(ik) || !seen.insert(ik).second) continue;
            co_await consider(cx, bucket, key, slot);
        }
        in.close();
        fs::remove(path, ec);  // consumed: survivors were re-enrolled
    }
}

// Space-watermark eviction (trigger 2): candidates come from the wheel in ascending
// deadline order (≈ least recently used first); cached objects and range-cache residue
// rank before local objects (zero upload). The multiset keeps only the best prefix that
// covers `need`; scanning stops once enough candidate bytes have been seen, so the pass is
// proportional to the deficit rather than to the object count
Task<void> TieredBackend::scan_evict(ScanCtx& cx, uint64_t need) {
    struct Evict {
        std::string bucket, key;
        int rank;       // cached / range-cache residue = 0, local = 1
        double score;   // higher first within a rank
        uint64_t size;
        bool rcache;
        bool operator<(const Evict& o) const {
            if (rank != o.rank) return rank < o.rank;
            if (score != o.score) return score > o.score;
            return std::tie(bucket, key) < std::tie(o.bucket, o.key);
        }
    };
    std::multiset<Evict> evict;
    uint64_t evict_bytes = 0, collected = 0;
    const uint64_t enough = std::max<uint64_t>(need * 4, need + (64ull << 20));
    std::set<std::string> seen;
    bool done = false;
    for (auto& [slot, path] : wheel_slots()) {
        if (done) break;
        std::ifstream in(path, std::ios::binary);
        std::string line;
        while (!done && std::getline(in, line)) {
            auto tab = line.find('\t');
            if (tab == std::string::npos) continue;
            std::string bucket = line.substr(0, tab), key = line.substr(tab + 1);
            std::string ik = make_ikey(bucket, key);
            if (cx.chosen.count(ik) || !seen.insert(ik).second) continue;
            auto o = local_->read(bucket, key);
            if (!o || cold_after_for(bucket, key) < 0) continue;  // gone or pinned
            Evict c{bucket, key, 1, 0.0, o->local_bytes, false};
            if (o->tier.tier == Tier::kRemote) {
                c.size = local_->range_cache_bytes(bucket, key);
                c.rank = 0;
                c.rcache = true;
            } else if (o->tier.tier == Tier::kCached) {
                c.rank = 0;
            }
            if (c.size == 0) continue;
            c.score = evict_score(access_of(bucket, key, o->mtime), c.size, cx.now);
            evict_bytes += c.size;
            collected += c.size;
            evict.insert(std::move(c));
            while (!evict.empty() && evict_bytes - std::prev(evict.end())->size >= need) {
                evict_bytes -= std::prev(evict.end())->size;
                evict.erase(std::prev(evict.end()));
            }
            if (collected >= enough && evict_bytes >= need) done = true;
        }
    }
    for (auto& o : evict) {
        if (need == 0) break;
        if (o.rcache) {
            local_->drop_range_cache(o.bucket, o.key);
        } else {
            cx.pick(*this, o.bucket, o.key);
        }
        ++cx.st.evicted;
        cx.st.evicted_bytes += o.size;
        need -= std::min(need, o.size);
        if (cx.batch.size() >= ScanCtx::kScanBatch) co_await drain_batch(cx);
    }
    co_await drain_batch(cx);
    cx.st.need_remaining = need;
    m_evicted_bytes_->inc(cx.st.evicted_bytes);
    // Eviction candidates cannot cover the gap (disk consumed by things outside this
    // backend, or every object is smaller than the watermark gap): previously each
    // round silently recomputed and freed 0 bytes, completely invisible to operators
    // (docs/archive/gaps.md §4)
    if (need > 0)
        LOG_WARN("tiered: space watermark still exceeded after eviction round, "
                 "{} bytes short — disk consumed outside this backend, or no "
                 "evictable candidates left",
                 need);
}

Task<TierScanStats> TieredBackend::scan_once() {
    co_await pool_->schedule();
    // Measure only rounds that ran to completion (like duostore gc_round_seconds): the
    // duration of a round that threw midway is unrepresentative, and failures already leave
    // a trail via demote_quiet's warnings
    const auto round_start = std::chrono::steady_clock::now();
    ScanCtx cx;
    cx.now = ::time(nullptr);
    // Pending touches first: a key written seconds ago must already be enrolled when the
    // eviction pass below looks for victims
    flush_access_sync();

    cx.st.full = cfg_.full_scan_interval_sec <= 0 || last_full_scan_ == 0 ||
                 cx.now - last_full_scan_ >= cfg_.full_scan_interval_sec;
    if (cx.st.full) co_await scan_full(cx);
    else co_await scan_incremental(cx);
    co_await drain_batch(cx);

    // Trigger 2: space watermark (statvfs is authoritative, optional quota on top). The
    // filesystem is measured fresh after the coldness demotions; the quota books subtract
    // what coldness already freed
    uint64_t need = 0;
    if (auto du = local_->disk_usage()) {
        if (du->first > cfg_.space_high_watermark)
            need = uint64_t((du->first - cfg_.space_low_watermark) * double(du->second));
    }
    uint64_t books = cx.st.full ? cx.local_bytes
                                : uint64_t(std::max<int64_t>(0, local_bytes_est_->load()));
    uint64_t lb = books - std::min(books, cx.cold_freed);
    if (cfg_.quota_bytes > 0 && double(lb) > cfg_.space_high_watermark * double(cfg_.quota_bytes)) {
        uint64_t target = uint64_t(cfg_.space_low_watermark * double(cfg_.quota_bytes));
        need = std::max(need, lb > target ? lb - target : 0);
    }
    if (need > 0) co_await scan_evict(cx, need);

    flush_access_sync();
    (cx.st.full ? m_scan_full_ : m_scan_incr_)->inc();
    m_scan_duration_->observe(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - round_start).count());
    LOG_INFO("tiered: {} scan round: {} {}, cold {}, recovered {}, enrolled {}, stale {}, "
             "evicted {} ({} bytes){}",
             cx.st.full ? "full" : "incremental", cx.st.walked,
             cx.st.full ? "objects walked" : "wheel candidates", cx.st.cold_picked,
             cx.st.recovered, cx.st.enrolled, cx.st.stale, cx.st.evicted, cx.st.evicted_bytes,
             cx.st.need_remaining ? " — watermark still exceeded" : "");
    co_return cx.st;
}

Task<void> TieredBackend::demote_quiet(std::string bucket, std::string key) {
    try {
        co_await demote_object(bucket, key);
    } catch (const std::exception& e) {
        // A single-object failure is only skipped; it backs off to the next scanner round
        // for retry (§5.2 step 2)
        LOG_WARN("tiered: demote {}/{} failed: {}", bucket, key, e.what());
    }
}

Task<void> TieredBackend::promote_quiet(std::string bucket, std::string key) {
    try {
        co_await promote_object(bucket, key);
    } catch (const std::exception& e) {
        LOG_WARN("tiered: promote {}/{} failed: {}", bucket, key, e.what());
    }
}

// ---------- GC queue (docs/tiered-storage.md §7.2) ----------

void TieredBackend::enqueue_gc(std::string_view bucket, std::string_view key,
                               std::string_view remote_etag) {
    if (remote_etag.empty()) return;
    char name[32];
    std::snprintf(name, sizeof(name), "%020llu",
                  static_cast<unsigned long long>(gc_seq_.fetch_add(1)));
    try {
        fsutil::write_tsv(gc_dir_ / name, local_->tmp_dir(),
                          {{"bucket", std::string(bucket)},
                           {"key", std::string(key)},
                           {"etag", std::string(remote_etag)}});
    } catch (const std::exception& e) {
        // The cost of a failed enqueue is only a cloud orphan awaiting reconciliation (§9);
        // correctness is unaffected
        LOG_WARN("tiered: enqueue gc for {}/{} failed: {}", bucket, key, e.what());
    }
}

Task<TierGcStats> TieredBackend::run_gc_once() {
    co_await pool_->schedule();
    TierGcStats st;
    std::vector<fs::path> entries;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(gc_dir_, ec))
        if (e.is_regular_file()) entries.push_back(e.path());
    std::sort(entries.begin(), entries.end());

    const int64_t now = ::time(nullptr);
    for (auto& p : entries) {
        std::string bucket, key, etag;
        int64_t attempts = 0, retry_at = 0;
        for (auto& [k, v] : fsutil::read_tsv(p)) {
            if (k == "bucket") bucket = v;
            else if (k == "key") key = v;
            else if (k == "etag") etag = v;
            // Backoff fields (§9): old entries default to 0 = immediately eligible, backward compatible
            else if (k == "attempts") std::from_chars(v.data(), v.data() + v.size(), attempts);
            else if (k == "retry_at") std::from_chars(v.data(), v.data() + v.size(), retry_at);
        }
        if (bucket.empty() || key.empty() || etag.empty()) {
            fs::remove(p, ec);  // corrupt entry
            ++st.resolved;
            continue;
        }
        if (retry_at > now) {  // exponential backoff not yet due (§9), skip this round
            ++st.deferred;
            continue;
        }
        if (inflight_contains(make_ikey(bucket, key))) continue;  // demotion in flight, revisit next round

        auto lk = co_await key_lock(bucket, key).acquire();
        co_await pool_->schedule();  // lock wakeup thread is indeterminate; local IO goes back to the pool
        // The local record still references this cloud replica -> live reference (e.g.
        // same content re-demoted), entry invalidated
        TierInfo cur = local_->read_tier_only(bucket, key);
        if (cur.tier != Tier::kLocal && cur.remote_etag == etag) {
            fs::remove(p, ec);
            ++st.resolved;
            continue;
        }
        try {
            auto cm = co_await cloud_->head_object(bucket, key);
            // Delete only orphans whose etag matches; a mismatch means the cloud already
            // holds a new replica, so the entry is simply invalidated
            if (std::string(strip_etag_quotes(cm.etag)) == etag) {
                co_await cloud_->delete_object(bucket, key);
                ++st.removed_cloud;
            }
            fs::remove(p, ec);
            ++st.resolved;
        } catch (const S3Error& e) {
            if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket) {
                fs::remove(p, ec);  // the cloud never had it
                ++st.resolved;
                continue;
            }
            // Cloud unreachable etc.: reschedule with exponential backoff (delay =
            // base x 2^attempts clamped to cap, §9), rewriting the entry in place --
            // attempts/retry_at persist in the TSV, not reset by restart
            ++st.failed;
            int64_t delay = cfg_.gc_retry_base_sec;
            for (int64_t i = 0; i < std::min<int64_t>(attempts, 30) &&
                                delay < cfg_.gc_retry_cap_sec; ++i)
                delay *= 2;
            delay = std::min(delay, cfg_.gc_retry_cap_sec);
            try {
                fsutil::write_tsv(p, local_->tmp_dir(),
                                  {{"bucket", bucket},
                                   {"key", key},
                                   {"etag", etag},
                                   {"attempts", std::to_string(attempts + 1)},
                                   {"retry_at", std::to_string(now + delay)}});
            } catch (const std::exception& we) {
                // Rewrite failed: the entry stays as-is (without backoff fields it retries
                // immediately next round); only the backoff is lost
                LOG_WARN("tiered: gc backoff rewrite for {}/{} failed: {}", bucket, key,
                         we.what());
            }
            LOG_WARN("tiered: gc delete {}/{} failed ({}), retry in {}s (attempt {})", bucket,
                     key, e.message, delay, attempts + 1);
        }
    }
    // Event volumes are booked incrementally from this round's stats; deferred is the
    // observation "how many entries remain in backoff this round", set overwrite-style
    // (the same entry reappears every round, accumulation would inflate)
    m_gc_runs_->inc();
    m_gc_removed_->inc(st.removed_cloud);
    m_gc_failed_->inc(st.failed);
    m_gc_deferred_->set(int64_t(st.deferred));
    co_return st;
}

// ---------- Quarantine ledger (roadmap §3.6 ④) ----------
// One TSV per finding under <state>/quarantine/, named by md5(kind\0bucket\0key) so the
// same finding maps to the same file across rounds. A finding is logged loudly the first
// time only; later rounds bump last_seen/count. Findings that stop reproducing are swept
// at the end of a completed reconcile round; operators resolve the rest through
// `lights3 tier quarantine forget|purge`

fs::path TieredBackend::quarantine_path(std::string_view kind, std::string_view bucket,
                                        std::string_view key) const {
    util::HashStream h(util::HashStream::Algo::Md5);
    std::string id = std::string(kind) + '\0' + std::string(bucket) + '\0' + std::string(key);
    h.update(std::span(reinterpret_cast<const uint8_t*>(id.data()), id.size()));
    return quarantine_dir_ / h.final_hex();
}

namespace {

std::optional<QuarantineEntry> read_quarantine(const fs::path& p) {
    QuarantineEntry e;
    for (auto& [k, v] : fsutil::read_tsv(p)) {
        if (k == "kind") e.kind = v;
        else if (k == "bucket") e.bucket = v;
        else if (k == "key") e.key = v;
        else if (k == "etag") e.etag = v;
        else if (k == "first_seen") std::from_chars(v.data(), v.data() + v.size(), e.first_seen);
        else if (k == "last_seen") std::from_chars(v.data(), v.data() + v.size(), e.last_seen);
        else if (k == "count") std::from_chars(v.data(), v.data() + v.size(), e.count);
    }
    if (e.kind.empty() || e.bucket.empty() || e.key.empty()) return std::nullopt;
    return e;
}

std::vector<std::pair<std::string, std::string>> quarantine_kv(const QuarantineEntry& e) {
    return {{"kind", e.kind},
            {"bucket", e.bucket},
            {"key", e.key},
            {"etag", e.etag},
            {"first_seen", std::to_string(e.first_seen)},
            {"last_seen", std::to_string(e.last_seen)},
            {"count", std::to_string(e.count)}};
}

std::string quarantine_id(std::string_view kind, std::string_view bucket, std::string_view key) {
    return std::string(kind) + '\0' + std::string(bucket) + '\0' + std::string(key);
}

}  // namespace

bool TieredBackend::quarantine_note(std::string_view kind, std::string_view bucket,
                                    std::string_view key, std::string_view etag,
                                    std::set<std::string>& seen, TierReconcileStats& st) {
    seen.insert(quarantine_id(kind, bucket, key));
    fs::path p = quarantine_path(kind, bucket, key);
    const int64_t now = ::time(nullptr);
    bool fresh = true;
    QuarantineEntry e;
    {
        std::lock_guard lk(quarantine_m_);
        if (auto old = read_quarantine(p)) {
            e = *old;
            fresh = false;
            ++e.count;
            e.last_seen = now;
            e.etag = std::string(etag);
        } else {
            e.kind = std::string(kind);
            e.bucket = std::string(bucket);
            e.key = std::string(key);
            e.etag = std::string(etag);
            e.first_seen = e.last_seen = now;
            e.count = 1;
        }
        try {
            fsutil::write_tsv(p, local_->tmp_dir(), quarantine_kv(e));
        } catch (const std::exception& ex) {
            LOG_WARN("tiered: quarantine ledger write failed for {}/{}: {}", bucket, key,
                     ex.what());
        }
    }
    if (fresh) {
        ++st.quarantined_new;
        refresh_quarantine_gauges();
    }
    return fresh;
}

void TieredBackend::quarantine_sweep(const std::set<std::string>& seen, TierReconcileStats& st) {
    std::error_code ec;
    std::lock_guard lk(quarantine_m_);
    for (auto& f : fs::directory_iterator(quarantine_dir_, ec)) {
        if (!f.is_regular_file()) continue;
        auto e = read_quarantine(f.path());
        if (!e) {
            fs::remove(f.path(), ec);
            continue;
        }
        if (seen.count(quarantine_id(e->kind, e->bucket, e->key))) continue;
        fs::remove(f.path(), ec);
        ++st.quarantined_resolved;
        LOG_INFO("tiered reconcile: quarantine {} {}/{} resolved (finding no longer reproduces)",
                 e->kind, e->bucket, e->key);
    }
}

void TieredBackend::refresh_quarantine_gauges() {
    int64_t refs = 0, foreign = 0;
    for (const auto& e : quarantine_list()) {
        if (e.kind == "refs_missing") ++refs;
        else if (e.kind == "foreign") ++foreign;
    }
    m_q_refs_missing_->set(refs);
    m_q_foreign_->set(foreign);
}

std::vector<QuarantineEntry> TieredBackend::quarantine_list() const {
    std::vector<QuarantineEntry> out;
    std::error_code ec;
    std::lock_guard lk(quarantine_m_);
    for (auto& f : fs::directory_iterator(quarantine_dir_, ec))
        if (f.is_regular_file())
            if (auto e = read_quarantine(f.path())) out.push_back(std::move(*e));
    std::sort(out.begin(), out.end(), [](const QuarantineEntry& a, const QuarantineEntry& b) {
        return std::tie(a.first_seen, a.bucket, a.key) < std::tie(b.first_seen, b.bucket, b.key);
    });
    return out;
}

bool TieredBackend::quarantine_forget(std::string_view bucket, std::string_view key) {
    bool removed = false;
    std::error_code ec;
    {
        std::lock_guard lk(quarantine_m_);
        for (const char* kind : {"refs_missing", "foreign"}) {
            fs::path p = quarantine_path(kind, bucket, key);
            if (fs::remove(p, ec)) removed = true;
        }
    }
    if (removed) refresh_quarantine_gauges();
    return removed;
}

Task<bool> TieredBackend::quarantine_purge(std::string bucket, std::string key) {
    co_await pool_->schedule();
    fs::path p = quarantine_path("refs_missing", bucket, key);
    std::optional<QuarantineEntry> e;
    {
        std::lock_guard lk(quarantine_m_);
        e = read_quarantine(p);
    }
    if (!e) co_return false;
    auto o = local_->read(bucket, key);
    if (!o || o->tier.tier != Tier::kRemote) {  // already rewritten/deleted by the user: nothing dead left
        quarantine_forget(bucket, key);
        co_return false;
    }
    // Re-verify at the current point: a replica that came back (restored bucket,
    // finished migration) means the finding is stale, keep the stub
    bool present = false;
    try {
        auto cm = co_await cloud_->head_object(bucket, key);
        present = std::string(strip_etag_quotes(cm.etag)) == o->tier.remote_etag;
    } catch (const S3Error& ex) {
        if (ex.code != S3ErrorCode::NoSuchKey && ex.code != S3ErrorCode::NoSuchBucket) throw;
    }
    if (present) {
        LOG_INFO("tiered: quarantine purge {}/{}: cloud copy is back, keeping the stub", bucket,
                 key);
        quarantine_forget(bucket, key);
        co_return false;
    }
    LOG_WARN("tiered: quarantine purge {}/{}: deleting dead stub (cloud copy {} is gone, "
             "data loss acknowledged by operator)",
             bucket, key, o->tier.remote_etag);
    co_await delete_object(bucket, key);  // the GC entry it enqueues resolves as "cloud never had it"
    quarantine_forget(bucket, key);
    co_return true;
}

// ---------- Reconciliation (docs/tiered-storage.md §9) ----------

Task<TierReconcileStats> TieredBackend::run_reconcile_once() {
    co_await pool_->schedule();
    TierReconcileStats st;
    std::set<std::string> seen;  // quarantine findings reproduced this round
    std::error_code ec;

    // GC queue snapshot: a cloud replica already queued for deletion is not an orphan --
    // rebuilding would resurrect a just-DELETEd object (the window where the local delete
    // happened but the cloud-delete entry has not been cashed in); skip them all and let GC
    // converge
    std::set<std::string> gc_pending;  // "bucket/key\tetag"
    for (auto& e : fs::directory_iterator(gc_dir_, ec)) {
        if (!e.is_regular_file()) continue;
        std::string b, k, t;
        for (auto& [kk, vv] : fsutil::read_tsv(e.path())) {
            if (kk == "bucket") b = vv;
            else if (kk == "key") k = vv;
            else if (kk == "etag") t = vv;
        }
        if (!b.empty() && !k.empty()) gc_pending.insert(make_ikey(b, k) + "\t" + t);
    }

    for (const auto& bi : co_await local_->backend().list_buckets()) {
        const std::string& bucket = bi.name;

        // Two-cursor ordered merge (docs/archive/gaps.md §2.13): local and cloud are both paged in
        // key lexicographic order, completing both reconciliation directions in O(page)
        // memory -- previously both full key sets were materialized in memory (~1.5-2GB for
        // tens of millions of objects). The tier is read fresh per key
        std::deque<std::string> lkeys;
        std::deque<std::pair<std::string, std::string>> ckeys;  // (key, unquoted etag)
        ListOptions lopt, copt;
        lopt.max_keys = copt.max_keys = 1000;
        bool ldone = false, cdone = false;
        auto refill_local = [&]() -> Task<void> {
            while (!ldone && lkeys.empty()) {
                auto r = co_await local_->backend().list_objects(bucket, lopt);
                for (auto& o : r.objects) lkeys.push_back(o.key);
                if (!r.is_truncated || r.next_token.empty()) ldone = true;
                else lopt.start_after = r.next_token;
            }
        };
        auto refill_cloud = [&]() -> Task<void> {
            while (!cdone && ckeys.empty()) {
                ListResult r;
                try {
                    r = co_await cloud_->list_objects(bucket, copt);
                } catch (const S3Error& e) {
                    if (e.code == S3ErrorCode::NoSuchBucket) {
                        cdone = true;  // no such bucket in the cloud = never demoted, empty set
                        co_return;
                    }
                    throw;  // cloud unreachable: this round fails, the scheduler retries next round
                }
                for (auto& o : r.objects)
                    ckeys.emplace_back(o.key, std::string(strip_etag_quotes(o.etag)));
                if (!r.is_truncated || r.next_token.empty()) cdone = true;
                else copt.start_after = r.next_token;
            }
        };

        for (;;) {
            co_await refill_local();
            co_await refill_cloud();
            const bool lhas = !lkeys.empty(), chas = !ckeys.empty();
            if (!lhas && !chas) break;
            const int cmp = !lhas ? 1 : !chas ? -1 : lkeys.front().compare(ckeys.front().first);
            if (cmp == 0) {
                // Present on both sides: forward-verify the association; on etag mismatch,
                // fall through to the reverse HEAD re-verification
                std::string key = std::move(lkeys.front());
                std::string ce = std::move(ckeys.front().second);
                lkeys.pop_front();
                ckeys.pop_front();
                ++st.cloud_objects;
                TierInfo t = local_->read_tier_only(bucket, key);
                if (t.tier != Tier::kLocal) {
                    if (t.remote_etag != ce) {
                        // Cloud version disagrees with the local reference (residue of a
                        // failed demotion / in-flight overwrite): the forward direction
                        // does not touch the cloud; whether the reference still holds is
                        // adjudicated by a HEAD at the current point
                        ++st.orphans_skipped;
                        co_await reconcile_ref_missing(bucket, key, t, st, seen);
                    }
                    continue;  // association healthy
                }
                // Local is back at the local tier (a stale replica GC lost track of): the
                // full data is on hand locally, so deletion is always safe
                co_await reconcile_orphan(bucket, key, ce, /*local_is_live=*/true, st, seen);
            } else if (cmp > 0) {
                // Cloud has it, local does not: orphan candidate
                std::string key = std::move(ckeys.front().first);
                std::string ce = std::move(ckeys.front().second);
                ckeys.pop_front();
                ++st.cloud_objects;
                if (gc_pending.count(make_ikey(bucket, key) + "\t" + ce)) continue;
                if (inflight_contains(make_ikey(bucket, key))) continue;  // demotion in flight
                co_await reconcile_orphan(bucket, key, ce, /*local_is_live=*/false, st, seen);
            } else {
                // Local has it, cloud does not: missing remote/cached reference candidate
                // (§9: warn, never delete the stub)
                std::string key = std::move(lkeys.front());
                lkeys.pop_front();
                TierInfo t = local_->read_tier_only(bucket, key);
                if (t.tier != Tier::kLocal) co_await reconcile_ref_missing(bucket, key, t, st, seen);
            }
        }
    }
    // Only a complete round may declare findings resolved: an aborted round (cloud down)
    // never reaches this point, so nothing is swept on partial information
    quarantine_sweep(seen, st);
    refresh_quarantine_gauges();
    co_return st;
}

// Reverse adjudication: when a local remote/cached reference is missing from the cloud
// listing or its etag mismatches, re-verify with a HEAD at the current point (the listing
// snapshot races with state changes -- a demotion may have just completed during
// reconciliation) before deciding the warning level
Task<void> TieredBackend::reconcile_ref_missing(std::string bucket, std::string key, TierInfo t,
                                                TierReconcileStats& st,
                                                std::set<std::string>& seen) {
    if (inflight_contains(make_ikey(bucket, key))) co_return;  // demotion/fill intermediate state
    bool present = false;
    try {
        auto cm = co_await cloud_->head_object(bucket, key);
        present = std::string(strip_etag_quotes(cm.etag)) == t.remote_etag;
    } catch (const S3Error&) {
    }
    if (present) co_return;
    if (t.tier == Tier::kRemote) {
        ++st.refs_missing;
        if (quarantine_note("refs_missing", bucket, key, t.remote_etag, seen, st))
            LOG_ERROR("tiered reconcile: stub {}/{} references cloud copy (etag {}) that "
                      "is gone — data loss signal, keeping stub; quarantined (see "
                      "`lights3 tier quarantine`)",
                      bucket, key, t.remote_etag);
        else
            LOG_DEBUG("tiered reconcile: {}/{} still quarantined (refs_missing)", bucket, key);
    } else {
        // cached: the data is still local, only the cloud replica is lost -- the next
        // coldness round will re-upload
        LOG_WARN("tiered reconcile: cached {}/{} lost cloud copy (etag {}); will "
                 "re-upload on next demote",
                 bucket, key, t.remote_etag);
    }
    co_return;
}

Task<void> TieredBackend::reconcile_orphan(std::string bucket, std::string key,
                                           std::string cloud_etag, bool local_is_live,
                                           TierReconcileStats& st, std::set<std::string>& seen) {
    auto lk = co_await key_lock(bucket, key).acquire();
    co_await pool_->schedule();  // lock wakeup thread is indeterminate; inside the lock is synchronous IO
    // Re-verify local state under the lock: a PUT/demotion/DELETE may have happened during
    // the listing; if the state changed, step aside and do not adjudicate
    auto o = local_->read(bucket, key);
    bool exists = o.has_value();
    if (local_is_live ? !(exists && o->tier.tier == Tier::kLocal) : exists) co_return;

    try {
        auto cm = co_await cloud_->head_object(bucket, key);
        if (std::string(strip_etag_quotes(cm.etag)) != cloud_etag) co_return;  // cloud already rewritten
        if (local_is_live || cfg_.reconcile_delete_orphans) {
            // Deleting a stale replica whose local tier is `local` is always safe (full
            // data on hand); pure-orphan deletion follows configuration
            co_await cloud_->delete_object(bucket, key);
            ++st.orphans_deleted;
            LOG_INFO("tiered reconcile: deleted orphan cloud copy {}/{} (etag {})", bucket, key,
                     cloud_etag);
            co_return;
        }
        // Rebuild the stub (default): trust only the lights3-* redundant headers
        // (§4.2/§9) -- without them it is a foreign object, neither touched nor deleted
        // (the remote bucket may be shared with others)
        auto oe = cm.user_meta.find("lights3-etag");
        if (oe == cm.user_meta.end() || oe->second.empty()) {
            ++st.orphans_skipped;
            if (quarantine_note("foreign", bucket, key, cloud_etag, seen, st))
                LOG_WARN("tiered reconcile: cloud object {}/{} lacks lights3 redundant headers, "
                         "skipping (foreign object?); quarantined",
                         bucket, key);
            co_return;
        }
        ObjectMeta nm;
        nm.key = key;
        nm.etag = oe->second;
        nm.size = cm.size;
        nm.last_modified = cm.last_modified;
        if (auto ct = cm.user_meta.find("lights3-content-type"); ct != cm.user_meta.end())
            nm.content_type = ct->second;
        // First-class metadata are real headers (Cache-Control etc.); the cloud stores and
        // returns them verbatim, no extra redundant copy needed
        for (auto& f : kStdMetaFields) nm.*f.field = cm.*f.field;
        nm.checksum_algorithm = cm.checksum_algorithm;
        nm.checksum_value = cm.checksum_value;
        nm.checksum_type = cm.checksum_type;
        nm.part_sizes = cm.part_sizes;
        for (auto& [mk, mv] : cm.user_meta)
            if (mk.rfind("lights3-", 0) != 0) nm.user_meta.emplace(mk, mv);
        co_await local_->commit_stub(bucket, key, nm,
                                     TierInfo{Tier::kRemote, cloud_etag,
                                              util::iso8601(std::chrono::system_clock::now())});
        touch(bucket, key);
        ++st.stubs_rebuilt;
        LOG_INFO("tiered reconcile: rebuilt stub {}/{} from cloud redundant headers", bucket,
                 key);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket)
            co_return;  // gone by re-verification: someone else finished handling it
        // A single-object failure is only skipped (cloud errors from head/delete); the
        // round continues. Transient by nature: not quarantined
        ++st.orphans_skipped;
        LOG_WARN("tiered reconcile: orphan handling for {}/{} failed: {}", bucket, key,
                 e.message);
    }
    co_return;
}

// ---------- Miscellaneous ----------

Task<void> TieredBackend::ensure_cloud_bucket(std::string_view bucket) {
    // No bucket_exists check: cloudproxy treats a remote 403 as "exists" per AWS HeadBucket
    // semantics (docs/cloudproxy-backend.md §4.3), so a gateway-side permission fault would
    // make that check lie, skip bucket creation, and leave the demotion pipeline silently
    // failing every round. Create directly and treat 409 as already-exists: idempotent and
    // unambiguous
    try {
        co_await cloud_->create_bucket(bucket);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
    }
    co_return;
}

AsyncSemaphore& TieredBackend::key_lock(std::string_view bucket, std::string_view key) {
    size_t h = std::hash<std::string>{}(make_ikey(bucket, key));
    return *key_locks_[h % kLockStripes];
}

bool TieredBackend::inflight_try_begin(const std::string& ikey) {
    std::lock_guard lk(inflight_m_);
    return inflight_.insert(ikey).second;
}
void TieredBackend::inflight_end(const std::string& ikey) {
    std::lock_guard lk(inflight_m_);
    inflight_.erase(ikey);
}
bool TieredBackend::inflight_contains(const std::string& ikey) {
    std::lock_guard lk(inflight_m_);
    return inflight_.count(ikey) > 0;
}

void TieredBackend::note_local_delta(int64_t delta) {
    int64_t est = local_bytes_est_->load(std::memory_order_relaxed);
    if (est < 0) return;  // no bookkeeping before the first scan calibrates (avoids negative books)
    local_bytes_est_->fetch_add(delta, std::memory_order_relaxed);
}

void TieredBackend::maybe_kick_quota_scan() {
    if (cfg_.quota_bytes == 0) return;
    int64_t est = local_bytes_est_->load(std::memory_order_relaxed);
    if (est < 0 || double(est) <= cfg_.space_high_watermark * double(cfg_.quota_bytes)) return;
    if (quota_kick_inflight_.exchange(true)) return;  // an early round is already in flight
    bool spawned = bg_.spawn([](TieredBackend* self) -> Task<void> {
        struct Clear {
            std::atomic<bool>& f;
            ~Clear() { f.store(false); }
        } clear{self->quota_kick_inflight_};
        LOG_INFO("tiered: quota estimate above high watermark, starting early scan");
        co_await self->scan_and_gc();
    }(this));
    if (!spawned) quota_kick_inflight_.store(false);  // shutting down
}

// ---------- Background task management (core/background.h wait group) ----------

void TieredBackend::schedule_scan() {
    if (cfg_.scan_interval_sec <= 0) return;
    bg_.if_open([&] {
        scan_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.scan_interval_sec),
                                                 [this] {
                                                     bg_.spawn(scan_and_gc());
                                                     schedule_scan();
                                                 });
    });
}

void TieredBackend::schedule_flush() {
    if (cfg_.scan_interval_sec <= 0) return;
    bg_.if_open([&] {
        flush_timer_ = TimerQueue::instance().add(std::chrono::seconds(kAccessFlushSec), [this] {
            maybe_kick_flush();
            schedule_flush();
        });
    });
}

void TieredBackend::schedule_reconcile() {
    // scan_interval=0 is the master switch for background tasks (manual hook for tests);
    // reconciliation has its own independent period
    if (cfg_.scan_interval_sec <= 0 || cfg_.reconcile_interval_sec <= 0) return;
    bg_.if_open([&] {
        reconcile_timer_ = TimerQueue::instance().add(
            std::chrono::seconds(cfg_.reconcile_interval_sec), [this] {
                bg_.spawn(reconcile_task());
                schedule_reconcile();
            });
    });
}

Task<void> TieredBackend::reconcile_task() {
    try {
        co_await run_reconcile_once();
    } catch (const std::exception& e) {
        // Cloud unreachable etc.: give up this round, retry next interval (§9 low-frequency
        // task, no compensation-window requirement)
        LOG_WARN("tiered: reconcile round failed: {} (retry next interval)", e.what());
    }
}

Task<void> TieredBackend::scan_and_gc() {
    co_await run_gc_once();
    try {
        co_await scan_once();
    } catch (const std::exception& e) {
        LOG_WARN("tiered: scan round failed: {} (retry next interval)", e.what());
    }
}

Task<void> TieredBackend::close() {
    bg_.begin_close();
    // cancel must be called outside the group lock: TimerQueue::cancel blocks waiting for
    // in-flight callbacks, and callbacks take the group lock (spawn/if_open) -- after
    // begin_close the timer ids no longer change, so reading needs no lock
    TimerQueue::instance().cancel(scan_timer_);
    TimerQueue::instance().cancel(flush_timer_);
    TimerQueue::instance().cancel(reconcile_timer_);
    // The blocking wait happens on the caller's thread; background tasks wrap up on pool
    // threads, so they do not contend with each other
    bg_.wait_idle();
    co_await pool_->schedule();
    flush_access_sync();
    co_await local_->close();  // flushes the local side's tables, cancels its own timers
    co_return;
}

}  // namespace lights3::storage
