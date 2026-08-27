#include "storage/tiered/tiered_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <deque>
#include <stdexcept>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/time.h"
#include "storage/multipart.h"

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using fsutil::Tier;
using fsutil::TierInfo;

namespace {

constexpr int64_t kAtimeSnapshotSec = 300;  // atime snapshot period (docs/tiered-storage.md §4.3)

// Read only the sidecar's tier fields (data file optional; used to look up the old cloud
// replica before PUT/DELETE)
TierInfo read_tier_only(const fs::path& data_path) {
    TierInfo t;
    for (auto& [k, v] : fsutil::read_tsv(data_path.string() + fsutil::kSidecarSuffix)) {
        if (k == "tier")
            t.tier = v == "remote" ? Tier::kRemote : v == "cached" ? Tier::kCached : Tier::kLocal;
        else if (k == "remote.etag") t.remote_etag = v;
        else if (k == "remote.at") t.remote_at = v;
    }
    return t;
}

// For demotion uploads: layers synchronous MD5 and byte counting on top of FdStreamReader
// (docs/tiered-storage.md §5.2 step 3 verification)
class HashingFdReader final : public http::BodyReader {
public:
    HashingFdReader(int fd, uint64_t size, std::shared_ptr<ThreadPool> pool)
        : inner_(fd, 0, size, std::move(pool)) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_.read(buf);
        if (n > 0) {
            md5_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
            bytes_ += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_.length(); }

    uint64_t bytes() const { return bytes_; }
    std::string md5_hex() { return md5_.final_hex(); }

private:
    fsutil::FdStreamReader inner_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    uint64_t bytes_ = 0;
};

}  // namespace

// RAII release of an in-flight table entry (used by demote/promote; Tee cache fill releases
// via the reader's destructor)
struct InflightRelease {
    TieredBackend* owner;
    std::string ikey;
    ~InflightRelease() { owner->inflight_end(ikey); }
};

// Tee passthrough + cache-while-downloading (docs/tiered-storage.md §6.2): the cloud stream
// is returned to the client as usual while also being written to a staging tmp with
// incremental MD5; if verification passes at EOF, commit as cached. Disk-write failures
// silently degrade to pure passthrough; on client disconnect the destructor runs and
// TmpFile RAII discards the half-written cache.
class TeeCacheReader final : public http::BodyReader {
public:
    TeeCacheReader(std::shared_ptr<TieredBackend> owner, std::string bucket, std::string key,
                   ObjectMeta meta, TierInfo tier, std::unique_ptr<http::BodyReader> src,
                   std::unique_ptr<fsutil::TmpFile> tmp)
        : owner_(std::move(owner)), bucket_(std::move(bucket)), key_(std::move(key)),
          meta_(std::move(meta)), tier_(std::move(tier)), src_(std::move(src)),
          tmp_(std::move(tmp)) {}

    ~TeeCacheReader() override { release_inflight(); }  // safety net for mid-transfer disconnect destruction

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await src_->read(buf);
        if (finished_) co_return n;
        if (n > 0) {
            if (!degraded_) {
                md5_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
                const char* p = reinterpret_cast<const char*>(buf.data());
                size_t left = n;
                while (left > 0) {
                    ssize_t w = ::write(tmp_->fd, p, left);
                    if (w < 0) {  // ENOSPC etc.: degrade to pure passthrough, invisible to the client
                        LOG_WARN("tiered: cache fill write failed for {}/{}, passthrough only",
                                 bucket_, key_);
                        degraded_ = true;
                        break;
                    }
                    p += w;
                    left -= static_cast<size_t>(w);
                }
                written_ += n;
            }
        } else {
            finished_ = true;
            if (!degraded_ && written_ == meta_.size) {
                ::close(tmp_->fd);
                tmp_->fd = -1;
                // Single-part objects compare MD5; multipart objects can only verify byte
                // count (etag has the -N form)
                bool multipart = meta_.etag.find('-') != std::string::npos;
                if (multipart || md5_.final_hex() == meta_.etag) {
                    // The client has received all data by now: a commit failure
                    // (cancellation/ENOSPC) can only drop the cache and degrade -- the
                    // exception must not leak into response teardown and read as a
                    // "transfer failure"
                    try {
                        co_await owner_->commit_cache_fill(bucket_, key_, meta_, tier_, *tmp_);
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
    ObjectMeta meta_;   // local sidecar snapshot (external-meta invariance principle)
    TierInfo tier_;     // expected remote version, re-verified before commit
    std::unique_ptr<http::BodyReader> src_;
    std::unique_ptr<fsutil::TmpFile> tmp_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    uint64_t written_ = 0;
    bool degraded_ = false, finished_ = false, released_ = false;
};

// ---------- Construction / configuration ----------

TieredBackend::TieredBackend(std::shared_ptr<LocalFsBackend> local,
                             std::shared_ptr<IStorageBackend> cloud,
                             std::shared_ptr<ThreadPool> pool, TieredConfig cfg,
                             MetricsScope metrics)
    : local_(std::move(local)), cloud_(std::move(cloud)), pool_(std::move(pool)), cfg_(cfg),
      tier_dir_(local_->staging() / "tier"), gc_dir_(tier_dir_ / "gc"),
      transfers_(std::max(1, cfg.max_concurrent_transfers), &pool_exec_) {
    init_metrics(metrics);
    fs::create_directories(gc_dir_);
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

    load_atime_snapshot();
    schedule_scan();
    schedule_snapshot();
    schedule_reconcile();
}

void TieredBackend::init_metrics(const MetricsScope& metrics) {
    // Pre-register the full op dimension (same rationale as duostore's reason bucketing):
    // a missing series reads as "no data" in Prometheus, not "zero". Only the four ops
    // where tiered has tiering logic are wired -- adding identical counters on purely
    // delegated paths would just duplicate local_'s lights3_localfs_*
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
    TimerQueue::instance().cancel(snap_timer_);
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
    auto local = std::dynamic_pointer_cast<LocalFsBackend>(built.at(local_name));
    if (!local)
        throw std::runtime_error("tiered backend '" + cfg.name + "': local '" + local_name +
                                 "' must be a localfs/xlocalfs backend (docs/tiered-storage.md §2)");
    auto cloud = built.at(cloud_name);
    if (cloud.get() == local.get())
        throw std::runtime_error("tiered backend '" + cfg.name + "': local and cloud must differ");

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
    if (tc.gc_retry_base_sec < 1 || tc.gc_retry_cap_sec < tc.gc_retry_base_sec)
        throw std::runtime_error("tiered backend '" + cfg.name +
                                 "': gc_retry_base must be >= 1s and <= gc_retry_cap");
    if (tc.space_low_watermark > tc.space_high_watermark)
        throw std::runtime_error("tiered backend '" + cfg.name + "': low watermark > high");
    return std::make_shared<TieredBackend>(std::move(local), std::move(cloud), std::move(pool),
                                           tc, std::move(metrics));
}

// ---------- bucket: delegated to local ----------

Task<void> TieredBackend::create_bucket(std::string_view bucket) {
    co_return co_await local_->create_bucket(bucket);
}
Task<void> TieredBackend::delete_bucket(std::string_view bucket) {
    // local being empty means no objects (a stub is also an object); the cloud may retain
    // orphan replicas pending GC, converged by reconciliation
    co_return co_await local_->delete_bucket(bucket);
}
Task<bool> TieredBackend::bucket_exists(std::string_view bucket) {
    co_return co_await local_->bucket_exists(bucket);
}
Task<std::vector<BucketInfo>> TieredBackend::list_buckets() {
    co_return co_await local_->list_buckets();
}

// ---------- object ----------

Task<ObjectStream> TieredBackend::get_object(std::string_view bucket, std::string_view key,
                                             std::optional<ByteRange> range) {
    OpGuard g{this, Op::kGet};
    // tiered assembles local paths itself (object_data_path), so it cannot rely solely on
    // local_'s entry validation
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    for (int attempt = 0;; ++attempt) {
        co_await pool_->schedule();
        fs::path path = local_->object_data_path(bucket, key);
        TierInfo t;
        ObjectMeta m;
        bool have_meta = true;
        try {
            m = fsutil::load_object_meta(path, std::string(key), &t);
        } catch (const S3Error&) {
            have_meta = false;  // absent: let local distinguish NoSuchBucket/NoSuchKey
        }
        if (have_meta) touch_atime(bucket, key);

        if (!have_meta || t.tier != Tier::kRemote) {
            try {
                auto os = co_await local_->get_object(bucket, key, range);
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

        // remote: cloud stream passthrough (docs/tiered-storage.md §6.2/§6.3); external
        // meta is always the local original
        ObjectStream cs = co_await cloud_->get_object(bucket, key, range);
        m_get_cloud_->inc();
        ObjectStream out;
        out.meta = m;
        out.range = cs.range;
        if (range) {
            out.body = std::move(cs.body);  // Range does no partial caching (§6.3)
            if (cfg_.cache_fill_on_range)
                bg_.spawn(promote_quiet(std::string(bucket), std::string(key)));
            g.ok = true;
            co_return out;
        }
        std::string ikey = make_ikey(bucket, key);
        if (m.size > 0 && cache_space_ok(m.size) && inflight_try_begin(ikey)) {
            auto tmp = std::make_unique<fsutil::TmpFile>();
            tmp->path = local_->staging() / "put" / fsutil::next_tmp_name();
            tmp->fd = ::open(tmp->path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (tmp->fd < 0) {  // cannot open tmp: pure-passthrough fallback (requirement 3)
                inflight_end(ikey);
                out.body = std::move(cs.body);
            } else {
                out.body = std::make_unique<TeeCacheReader>(shared_from_this(),
                                                            std::string(bucket), std::string(key),
                                                            m, t, std::move(cs.body),
                                                            std::move(tmp));
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
    fs::path dpath = local_->object_data_path(bucket, key);
    TierInfo prior = read_tier_only(dpath);
    struct stat st0{};
    int64_t prior_size = ::stat(dpath.c_str(), &st0) == 0 ? int64_t(st0.st_size) : 0;
    // Conditions pass through to local_: a stub's sidecar keeps the original etag (HEAD is
    // fully local, §6.1), and the check inside local's commit lock is equally authoritative
    // for demoted objects
    auto r = co_await local_->put_object(bucket, key, std::move(meta), body, cond);
    touch_atime(bucket, key);
    struct stat st1{};
    if (::stat(dpath.c_str(), &st1) == 0) note_local_delta(int64_t(st1.st_size) - prior_size);
    maybe_kick_quota_scan();
    // write-back: new data lands only locally and the tier returns to local; the old cloud
    // replica becomes an orphan (§7.1)
    if (prior.tier != Tier::kLocal) enqueue_gc(bucket, key, prior.remote_etag);
    g.ok = true;
    co_return r;
}

Task<ObjectMeta> TieredBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    // The stub's sidecar is complete, so HEAD finishes entirely locally (§6.1)
    auto m = co_await local_->head_object(bucket, key);
    touch_atime(bucket, key);
    co_return m;
}

Task<void> TieredBackend::delete_object(std::string_view bucket, std::string_view key) {
    OpGuard g{this, Op::kDelete};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    co_await pool_->schedule();
    fs::path dpath = local_->object_data_path(bucket, key);
    TierInfo prior = read_tier_only(dpath);
    struct stat st0{};
    if (::stat(dpath.c_str(), &st0) == 0) note_local_delta(-int64_t(st0.st_size));
    co_await local_->delete_object(bucket, key);
    erase_atime(bucket, key);
    // The response does not wait on the cloud: the cloud replica goes to GC for async deletion (§7.2)
    if (prior.tier != Tier::kLocal) enqueue_gc(bucket, key, prior.remote_etag);
    g.ok = true;
    co_return;
}

Task<ListResult> TieredBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    OpGuard g{this, Op::kList};
    validate_bucket_name(bucket, kAllowReserved);
    auto r = co_await local_->list_objects(bucket, opt);  // a stub is a 0-length file; the walk is reused as-is
    g.ok = true;
    co_return r;
}

// ---------- multipart: delegated to local ----------

Task<std::string> TieredBackend::create_multipart(std::string_view bucket, std::string_view key,
                                                  ObjectMeta meta) {
    co_return co_await local_->create_multipart(bucket, key, std::move(meta));
}
Task<void> TieredBackend::set_object_tagging(std::string_view bucket, std::string_view key,
                                             std::string tagging) {
    // Meta lives in the local xattr/sidecar even for demoted stubs — pure delegation
    co_return co_await local_->set_object_tagging(bucket, key, std::move(tagging));
}

Task<PutResult> TieredBackend::upload_part(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id, int part_no,
                                           http::BodyReader& body,
                                           const std::optional<PartChecksum>& checksum) {
    co_return co_await local_->upload_part(bucket, key, upload_id, part_no, body, checksum);
}
Task<PutResult> TieredBackend::complete_multipart(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  std::span<const PartInfo> parts) {
    validate_object_key(key);
    co_await pool_->schedule();
    TierInfo prior = read_tier_only(local_->object_data_path(bucket, key));
    auto r = co_await local_->complete_multipart(bucket, key, upload_id, parts);
    touch_atime(bucket, key);
    if (prior.tier != Tier::kLocal) enqueue_gc(bucket, key, prior.remote_etag);  // same as PUT overwrite
    co_return r;
}
Task<void> TieredBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                          std::string_view upload_id) {
    co_return co_await local_->abort_multipart(bucket, key, upload_id);
}
Task<ListPartsResult> TieredBackend::list_parts(std::string_view bucket, std::string_view key,
                                                std::string_view upload_id,
                                                const ListPartsOptions& opt) {
    co_return co_await local_->list_parts(bucket, key, upload_id, opt);
}
Task<ListUploadsResult> TieredBackend::list_multipart_uploads(std::string_view bucket,
                                                              const ListUploadsOptions& opt) {
    co_return co_await local_->list_multipart_uploads(bucket, opt);
}

// ---------- Demotion (docs/tiered-storage.md §5) ----------

Task<void> TieredBackend::demote_object(std::string bucket, std::string key) {
    auto permit = co_await transfers_.acquire();  // max_concurrent_transfers throttle
    co_await pool_->schedule();
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t0;
    ObjectMeta m0 = fsutil::load_object_meta(path, key, &t0);

    if (t0.tier == Tier::kRemote) {
        // Crash recovery (between §5.2 b/c): the sidecar is already remote but the data
        // file was not reclaimed -> finish the stubbing
        struct stat st{};
        if (::stat(path.c_str(), &st) == 0 && st.st_size > 0) {
            auto lk = co_await key_lock(bucket, key).acquire();
            co_await pool_->schedule();  // the lock wakeup may resume on another thread; blocking IO goes back to the pool
            TierInfo t1;
            ObjectMeta m1;
            try {
                m1 = fsutil::load_object_meta(path, key, &t1);
            } catch (const S3Error&) {
                co_return;
            }
            struct stat st1{};
            if (t1.tier == Tier::kRemote && ::stat(path.c_str(), &st1) == 0 && st1.st_size > 0)
                fsutil::commit_stub(path, m1, t1, local_->staging() / "put");
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
        // Step 1 fd snapshot + step 2 streaming upload (no extra memory), recomputing MD5
        // synchronously for step 3 verification
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) fsutil::throw_errno("open for demote");
        HashingFdReader body(fd, m0.size, pool_);  // fd ownership transferred
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
    co_await pool_->schedule();  // as above: the critical section is synchronous file IO, must be on a pool thread
    TierInfo t1;
    ObjectMeta m1;
    try {
        m1 = fsutil::load_object_meta(path, key, &t1);
    } catch (const S3Error&) {  // DELETEd in the meantime: DELETE wins (§7.3)
        enqueue_gc(bucket, key, remote_etag);
        co_return;
    }
    if (m1.etag != m0.etag) {  // overwritten by PUT in the meantime: PUT wins (§7.3)
        enqueue_gc(bucket, key, remote_etag);
        co_return;
    }
    if (t1.tier == Tier::kRemote) co_return;  // already stubbed by someone else
    fsutil::commit_stub(path, m1, TierInfo{Tier::kRemote, remote_etag, remote_at},
                        local_->staging() / "put");
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
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t0;
    ObjectMeta m0;
    try {
        m0 = fsutil::load_object_meta(path, key, &t0);
    } catch (const S3Error&) {
        co_return;
    }
    if (t0.tier != Tier::kRemote) co_return;
    if (m0.size > 0 && !cache_space_ok(m0.size)) co_return;  // insufficient space: give up (requirement 3)
    std::string ikey = make_ikey(bucket, key);
    if (!inflight_try_begin(ikey)) co_return;  // single-flight (§6.4)
    InflightRelease rel{this, ikey};

    ObjectStream cs = co_await cloud_->get_object(bucket, key, std::nullopt);
    fsutil::TmpFile tmp{local_->staging() / "put" / fsutil::next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) fsutil::throw_errno("open promote tmp");

    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t written = 0;
    std::vector<std::byte> buf(256 * 1024);
    for (;;) {
        size_t n = co_await cs.body->read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
        const char* p = reinterpret_cast<const char*>(buf.data());
        size_t left = n;
        while (left > 0) {
            ssize_t w = ::write(tmp.fd, p, left);
            if (w < 0) co_return;  // ENOSPC etc.: TmpFile RAII discards
            p += w;
            left -= static_cast<size_t>(w);
        }
        written += n;
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    bool multipart = m0.etag.find('-') != std::string::npos;
    if (written != m0.size || (!multipart && md5.final_hex() != m0.etag)) {
        LOG_WARN("tiered: promote checksum mismatch for {}/{}, dropped", bucket, key);
        co_return;
    }
    co_await commit_cache_fill(bucket, key, m0, t0, tmp);
    co_return;
}

Task<void> TieredBackend::commit_cache_fill(std::string bucket, std::string key,
                                            ObjectMeta expect, TierInfo expect_tier,
                                            fsutil::TmpFile& tmp) {
    auto lk = co_await key_lock(bucket, key).acquire();
    // Critical path (docs/archive/gaps.md §2.4): this function is co_awaited by TeeCacheReader when
    // the client reads EOF; without switching back to a pool thread, the whole rename + two
    // fsyncs + sidecar write would land directly on the HTTP response thread
    co_await pool_->schedule();
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t1;
    ObjectMeta m1;
    try {
        m1 = fsutil::load_object_meta(path, key, &t1);
    } catch (const S3Error&) {
        co_return;  // DELETEd in the meantime: discard tmp (§7.3)
    }
    // Re-verify it is still the same remote version; discard if overwritten by PUT or
    // already filled (user writes win)
    if (t1.tier != Tier::kRemote || m1.etag != expect.etag ||
        t1.remote_etag != expect_tier.remote_etag)
        co_return;
    fsutil::commit_cached(path, tmp, m1, TierInfo{Tier::kCached, t1.remote_etag, t1.remote_at},
                          local_->staging() / "put");
    // The counter sits at the commit point rather than promote_object's exit: this is the
    // single junction where "data really returned to local" (shared by explicit promotion
    // and GET Tee fill), and paths that fail re-verification and discard tmp naturally do
    // not count
    m_promoted_->inc();
    co_return;
}

// ---------- TierScanner (docs/tiered-storage.md §5.1) ----------

Task<void> TieredBackend::scan_once() {
    co_await pool_->schedule();
    // Measure only rounds that ran to completion (like duostore gc_round_seconds): the
    // duration of a round that threw midway is unrepresentative, and failures already leave
    // a trail via demote_quiet's warnings
    const auto round_start = std::chrono::steady_clock::now();
    struct Evict {
        std::string bucket, key;
        int rank;  // cached=0 (zero-cost stubbing) takes priority over local=1 (docs/tiered-storage.md §5.1)
        int64_t atime;
        uint64_t size;
        bool operator<(const Evict& o) const {
            return std::tie(rank, atime, bucket, key) < std::tie(o.rank, o.atime, o.bucket, o.key);
        }
    };

    const int64_t now = ::time(nullptr);
    std::set<std::string> chosen;
    // Bounded coroutine frames (docs/archive/gaps.md §2.13): transfers_ only limits execution
    // concurrency, not frame count; this used to materialize all objects and construct N
    // frames at once. Changed to batched co_await, so the number of simultaneously live
    // frames is fixed
    constexpr size_t kScanBatch = 128;
    std::vector<Task<void>> batch;
    auto pick = [&](const std::string& b, const std::string& k) {
        if (chosen.insert(make_ikey(b, k)).second) batch.push_back(demote_quiet(b, k));
    };
    auto drain_batch = [&]() -> Task<void> {
        if (!batch.empty()) {
            auto work = std::move(batch);
            batch.clear();
            co_await when_all(std::move(work));
        }
    };

    // First pass: coldness detection + crash recovery (launches streamingly, no candidate
    // materialization), tallying the quota books along the way
    uint64_t local_bytes = 0;  // actual data-file usage (quota books)
    uint64_t cold_freed = 0;   // bytes the coldness-selected objects will free, counted toward the watermark target
    std::error_code ec;
    for (auto& be : fs::directory_iterator(local_->root(), ec)) {
        if (!be.is_directory() || !fs::exists(be.path() / fsutil::kBucketMarker)) continue;
        std::string bucket = be.path().filename().string();
        for (auto it = fs::recursive_directory_iterator(be.path(), ec);
             it != fs::recursive_directory_iterator(); ++it) {
            if (!it->is_regular_file()) continue;
            std::string name = it->path().filename().string();
            if (name == fsutil::kBucketMarker || name.ends_with(fsutil::kSidecarSuffix)) continue;
            std::string key = fs::relative(it->path(), be.path()).generic_string();
            TierInfo t;
            try {
                fsutil::load_object_meta(it->path(), key, &t);
            } catch (const S3Error&) {
                continue;  // raced with a concurrent delete
            }
            struct stat st{};
            if (::stat(it->path().c_str(), &st) != 0) continue;
            auto disk_size = static_cast<uint64_t>(st.st_size);
            local_bytes += disk_size;

            if (t.tier == Tier::kRemote) {
                // Crash recovery: remote but the data file was not reclaimed (demote_object
                // finishes the stubbing)
                if (disk_size > 0) pick(bucket, key);
            } else if (now - atime_or(make_ikey(bucket, key), int64_t(st.st_mtime)) >=
                       cfg_.cold_after_sec) {
                pick(bucket, key);  // trigger 1: coldness
                cold_freed += disk_size;
            }
            if (batch.size() >= kScanBatch) co_await drain_batch();
        }
    }
    co_await drain_batch();
    local_bytes_est_.store(int64_t(local_bytes), std::memory_order_relaxed);  // incremental-books calibration

    // Trigger 2: space watermark (statvfs is authoritative, optional quota on top). statvfs
    // is measured fresh after the coldness demotions; the quota books subtract what
    // coldness already freed
    uint64_t need = 0;
    struct statvfs sv{};
    if (::statvfs(local_->root().c_str(), &sv) == 0 && sv.f_blocks > 0) {
        double used = 1.0 - double(sv.f_bavail) / double(sv.f_blocks);
        if (used > cfg_.space_high_watermark)
            need = uint64_t((used - cfg_.space_low_watermark) * double(sv.f_blocks) *
                            double(sv.f_frsize));
    }
    uint64_t lb = local_bytes - std::min(local_bytes, cold_freed);
    if (cfg_.quota_bytes > 0 && double(lb) > cfg_.space_high_watermark * double(cfg_.quota_bytes)) {
        uint64_t target = uint64_t(cfg_.space_low_watermark * double(cfg_.quota_bytes));
        need = std::max(need, lb > target ? lb - target : 0);
    }

    if (need > 0) {
        // Second pass (reached only past the watermark): gather eviction candidates. No
        // longer fully materialized -- the reclamation target is fixed, and the multiset
        // keeps only the best (smallest rank/atime) prefix that "just covers need",
        // trimming from the worst end on overflow; memory is proportional to need, not to
        // the total object count
        std::multiset<Evict> evict;
        uint64_t evict_bytes = 0;
        for (auto& be : fs::directory_iterator(local_->root(), ec)) {
            if (!be.is_directory() || !fs::exists(be.path() / fsutil::kBucketMarker)) continue;
            std::string bucket = be.path().filename().string();
            for (auto it = fs::recursive_directory_iterator(be.path(), ec);
                 it != fs::recursive_directory_iterator(); ++it) {
                if (!it->is_regular_file()) continue;
                std::string name = it->path().filename().string();
                if (name == fsutil::kBucketMarker || name.ends_with(fsutil::kSidecarSuffix))
                    continue;
                std::string key = fs::relative(it->path(), be.path()).generic_string();
                if (chosen.count(make_ikey(bucket, key))) continue;  // already being demoted
                TierInfo t;
                try {
                    fsutil::load_object_meta(it->path(), key, &t);
                } catch (const S3Error&) {
                    continue;
                }
                if (t.tier == Tier::kRemote) continue;
                struct stat st{};
                if (::stat(it->path().c_str(), &st) != 0 || st.st_size == 0) continue;
                evict.insert({bucket, key, t.tier == Tier::kCached ? 0 : 1,
                              atime_or(make_ikey(bucket, key), int64_t(st.st_mtime)),
                              static_cast<uint64_t>(st.st_size)});
                evict_bytes += static_cast<uint64_t>(st.st_size);
                while (!evict.empty() &&
                       evict_bytes - std::prev(evict.end())->size >= need) {
                    evict_bytes -= std::prev(evict.end())->size;
                    evict.erase(std::prev(evict.end()));
                }
            }
        }
        for (auto& o : evict) {
            if (need == 0) break;
            pick(o.bucket, o.key);
            need -= std::min(need, o.size);
            if (batch.size() >= kScanBatch) co_await drain_batch();
        }
        co_await drain_batch();
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

    save_atime_snapshot();
    m_scan_duration_->observe(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - round_start).count());
    co_return;
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
        fsutil::write_tsv(gc_dir_ / name, local_->staging() / "put",
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
        co_await pool_->schedule();  // lock wakeup thread is indeterminate; sidecar IO goes back to the pool
        // The local sidecar still references this cloud replica -> live reference (e.g.
        // same content re-demoted), entry invalidated
        TierInfo cur = read_tier_only(local_->object_data_path(bucket, key));
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
                fsutil::write_tsv(p, local_->staging() / "put",
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

// ---------- Reconciliation (docs/tiered-storage.md §9) ----------

Task<TierReconcileStats> TieredBackend::run_reconcile_once() {
    co_await pool_->schedule();
    TierReconcileStats st;
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

    for (auto& be : fs::directory_iterator(local_->root(), ec)) {
        if (!be.is_directory() || !fs::exists(be.path() / fsutil::kBucketMarker)) continue;
        std::string bucket = be.path().filename().string();

        // Two-cursor ordered merge (docs/archive/gaps.md §2.13): local and cloud are both paged in
        // key lexicographic order, completing both reconciliation directions in O(page)
        // memory -- previously both full key sets were materialized in memory (~1.5-2GB for
        // tens of millions of objects). Local existence is judged by the data file (list is
        // the data-file view); the tier is read fresh from the sidecar per key
        std::deque<std::string> lkeys;
        std::deque<std::pair<std::string, std::string>> ckeys;  // (key, unquoted etag)
        ListOptions lopt, copt;
        lopt.max_keys = copt.max_keys = 1000;
        bool ldone = false, cdone = false;
        auto refill_local = [&]() -> Task<void> {
            while (!ldone && lkeys.empty()) {
                auto r = co_await local_->list_objects(bucket, lopt);
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
                TierInfo t = read_tier_only(local_->object_data_path(bucket, key));
                if (t.tier != Tier::kLocal) {
                    if (t.remote_etag != ce) {
                        // Cloud version disagrees with the local reference (residue of a
                        // failed demotion / in-flight overwrite): the forward direction
                        // does not touch the cloud; whether the reference still holds is
                        // adjudicated by a HEAD at the current point
                        LOG_WARN("tiered reconcile: {}/{} cloud etag {} != referenced {}",
                                 bucket, key, ce, t.remote_etag);
                        ++st.orphans_skipped;
                        co_await reconcile_ref_missing(bucket, key, t, st);
                    }
                    continue;  // association healthy
                }
                // Local is back at the local tier (a stale replica GC lost track of): the
                // full data is on hand locally, so deletion is always safe
                co_await reconcile_orphan(bucket, key, ce, /*local_is_live=*/true, st);
            } else if (cmp > 0) {
                // Cloud has it, local does not: orphan candidate
                std::string key = std::move(ckeys.front().first);
                std::string ce = std::move(ckeys.front().second);
                ckeys.pop_front();
                ++st.cloud_objects;
                if (gc_pending.count(make_ikey(bucket, key) + "\t" + ce)) continue;
                if (inflight_contains(make_ikey(bucket, key))) continue;  // demotion in flight
                co_await reconcile_orphan(bucket, key, ce, /*local_is_live=*/false, st);
            } else {
                // Local has it, cloud does not: missing remote/cached reference candidate
                // (§9: warn, never delete the stub)
                std::string key = std::move(lkeys.front());
                lkeys.pop_front();
                TierInfo t = read_tier_only(local_->object_data_path(bucket, key));
                if (t.tier != Tier::kLocal) co_await reconcile_ref_missing(bucket, key, t, st);
            }
        }
    }
    co_return st;
}

// Reverse adjudication: when a local remote/cached reference is missing from the cloud
// listing or its etag mismatches, re-verify with a HEAD at the current point (the listing
// snapshot races with state changes -- a demotion may have just completed during
// reconciliation) before deciding the warning level
Task<void> TieredBackend::reconcile_ref_missing(std::string bucket, std::string key, TierInfo t,
                                                TierReconcileStats& st) {
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
        LOG_ERROR("tiered reconcile: stub {}/{} references cloud copy (etag {}) that "
                  "is gone — data loss signal, keeping stub for manual inspection",
                  bucket, key, t.remote_etag);
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
                                           TierReconcileStats& st) {
    auto lk = co_await key_lock(bucket, key).acquire();
    co_await pool_->schedule();  // lock wakeup thread is indeterminate; inside the lock is synchronous file IO
    fs::path path = local_->object_data_path(bucket, key);
    // Re-verify local state under the lock: a PUT/demotion/DELETE may have happened during
    // the listing; if the state changed, step aside and do not adjudicate
    TierInfo t;
    bool exists = true;
    try {
        fsutil::load_object_meta(path, key, &t);
    } catch (const S3Error&) {
        exists = false;
    }
    if (local_is_live ? !(exists && t.tier == Tier::kLocal) : exists) co_return;

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
            LOG_WARN("tiered reconcile: cloud object {}/{} lacks lights3 redundant headers, "
                     "skipping (foreign object?)",
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
        std::error_code ec;
        fs::create_directories(path.parent_path(), ec);
        fsutil::commit_stub(path, nm,
                            TierInfo{Tier::kRemote, cloud_etag,
                                     util::iso8601(std::chrono::system_clock::now())},
                            local_->staging() / "put");
        touch_atime(bucket, key);
        ++st.stubs_rebuilt;
        LOG_INFO("tiered reconcile: rebuilt stub {}/{} from cloud redundant headers", bucket,
                 key);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket)
            co_return;  // gone by re-verification: someone else finished handling it
        // A single-object failure is only skipped (cloud errors from head/delete); the
        // round continues
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

bool TieredBackend::cache_space_ok(uint64_t size) const {
    struct statvfs sv{};
    if (::statvfs(local_->root().c_str(), &sv) != 0) return false;
    uint64_t avail = uint64_t(sv.f_bavail) * sv.f_frsize;
    return avail > size + cfg_.min_free_bytes;
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

// ---------- TierIndex: atime table (docs/tiered-storage.md §4.3) ----------

void TieredBackend::touch_atime(std::string_view bucket, std::string_view key) {
    std::lock_guard lk(atime_m_);
    atime_[make_ikey(bucket, key)] = ::time(nullptr);
    atime_dirty_ = true;
}
void TieredBackend::erase_atime(std::string_view bucket, std::string_view key) {
    std::lock_guard lk(atime_m_);
    if (atime_.erase(make_ikey(bucket, key)) > 0) atime_dirty_ = true;
}
int64_t TieredBackend::atime_or(const std::string& ikey, int64_t fallback) {
    std::lock_guard lk(atime_m_);
    auto it = atime_.find(ikey);
    return it == atime_.end() ? fallback : it->second;
}

void TieredBackend::load_atime_snapshot() {
    for (auto& [k, v] : fsutil::read_tsv(tier_dir_ / "atime.tsv")) {
        int64_t t = 0;
        std::from_chars(v.data(), v.data() + v.size(), t);
        if (t > 0) atime_[k] = t;
    }
}

void TieredBackend::save_atime_snapshot() {
    std::vector<std::pair<std::string, std::string>> kv;
    {
        // Boundedness (docs/archive/gaps.md §4: deletions bypassing this backend would grow the
        // table without bound, and the snapshot is a full rewrite): records older than the
        // coldness threshold are dropped outright -- the object has already reached
        // coldness eligibility, and after losing atime the mtime fallback reaches the same
        // conclusion. The table thus grows only with "recently accessed keys"
        const int64_t cutoff = ::time(nullptr) - cfg_.cold_after_sec;
        std::lock_guard lk(atime_m_);
        size_t before = atime_.size();
        for (auto it = atime_.begin(); it != atime_.end();) {
            if (it->second < cutoff) {
                it = atime_.erase(it);
                continue;
            }
            if (it->first.find('\t') == std::string::npos &&
                it->first.find('\n') == std::string::npos)
                kv.emplace_back(it->first, std::to_string(it->second));
            ++it;
        }
        if (atime_.size() != before) atime_dirty_ = true;  // rolling eviction itself is also a change
        // The table has not moved since the last snapshot, so the written content would be
        // byte-identical to the one on disk: skip the full rewrite + fsync. Keep a
        // "still needs writing" state so a failed write naturally retries next round
        if (!atime_dirty_) return;
        atime_dirty_ = false;
    }
    try {
        fsutil::write_tsv(tier_dir_ / "atime.tsv", local_->staging() / "put", kv);
    } catch (const std::exception& e) {
        // A failed snapshot only affects coldness accuracy at next startup (mtime
        // fallback), tolerable
        LOG_WARN("tiered: atime snapshot failed: {}", e.what());
        std::lock_guard lk(atime_m_);
        atime_dirty_ = true;  // retry next round, or this loss would be permanently masked by "not dirty"
    }
}

void TieredBackend::note_local_delta(int64_t delta) {
    int64_t est = local_bytes_est_.load(std::memory_order_relaxed);
    if (est < 0) return;  // no bookkeeping before the first scan calibrates (avoids negative books)
    local_bytes_est_.fetch_add(delta, std::memory_order_relaxed);
}

void TieredBackend::maybe_kick_quota_scan() {
    if (cfg_.quota_bytes == 0) return;
    int64_t est = local_bytes_est_.load(std::memory_order_relaxed);
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

void TieredBackend::schedule_snapshot() {
    if (cfg_.scan_interval_sec <= 0) return;
    bg_.if_open([&] {
        snap_timer_ = TimerQueue::instance().add(std::chrono::seconds(kAtimeSnapshotSec),
                                                 [this] {
                                                     bg_.spawn(snapshot_task());
                                                     schedule_snapshot();
                                                 });
    });
}

Task<void> TieredBackend::snapshot_task() {
    co_await pool_->schedule();
    save_atime_snapshot();
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
    co_await scan_once();
}

Task<void> TieredBackend::close() {
    bg_.begin_close();
    // cancel must be called outside the group lock: TimerQueue::cancel blocks waiting for
    // in-flight callbacks, and callbacks take the group lock (spawn/if_open) -- after
    // begin_close the timer ids no longer change, so reading needs no lock
    TimerQueue::instance().cancel(scan_timer_);
    TimerQueue::instance().cancel(snap_timer_);
    TimerQueue::instance().cancel(reconcile_timer_);
    // The blocking wait happens on the caller's thread; background tasks wrap up on pool
    // threads, so they do not contend with each other
    bg_.wait_idle();
    save_atime_snapshot();
    co_await local_->close();  // cancel the base localfs's periodic mpu cleanup timer
    co_return;
}

}  // namespace lights3::storage
