#include "s3/usage.h"

#include <algorithm>
#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/util/time.h"
#include "s3/errors.h"
#include "storage/backend.h"
#include "storage/multipart.h"

namespace lights3::s3 {

using nlohmann::json;

namespace {

int64_t to_unix(std::chrono::system_clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

Task<std::string> read_all(http::BodyReader& body, size_t max_size = 64 * 1024) {
    std::string out;
    std::byte buf[16 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        if (out.size() + n > max_size) throw std::runtime_error("usage object too large");
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return out;
}

}  // namespace

// ---------- persistence shape ----------

std::string UsageTracker::serialize(const BucketUsage& u) {
    json j;
    j["objects"] = u.objects;
    j["bytes"] = u.bytes;
    j["mpu_bytes"] = u.mpu_bytes;
    j["scanned_unix"] = to_unix(u.scanned_at);
    j["flushed_unix"] = to_unix(std::chrono::system_clock::now());
    return j.dump() + "\n";
}

std::optional<BucketUsage> UsageTracker::deserialize(const std::string& body) {
    try {
        json j = json::parse(body);
        BucketUsage u;
        u.objects = std::max<int64_t>(0, j.value("objects", int64_t{0}));
        u.bytes = std::max<int64_t>(0, j.value("bytes", int64_t{0}));
        u.mpu_bytes = std::max<int64_t>(0, j.value("mpu_bytes", int64_t{0}));
        u.scanned_at = std::chrono::system_clock::time_point(
            std::chrono::seconds(j.value("scanned_unix", int64_t{0})));
        return u;
    } catch (const json::exception&) {
        return std::nullopt;
    }
}

// ---------- lifecycle ----------

Task<std::shared_ptr<UsageTracker>> UsageTracker::load(storage::BucketRouter router,
                                                       UsageConfig cfg,
                                                       std::shared_ptr<MetricsRegistry> metrics) {
    auto t = std::shared_ptr<UsageTracker>(new UsageTracker());
    t->router_ = std::move(router);
    t->cfg_ = cfg;
    t->metrics_ = std::move(metrics);
    if (t->metrics_) {
        const char* help = "Writes refused by a quota, by the scope that was exceeded";
        t->m_reject_bucket_ =
            t->metrics_->counter("lights3_quota_rejections_total", help, {{"scope", "bucket"}});
        t->m_reject_tenant_ =
            t->metrics_->counter("lights3_quota_rejections_total", help, {{"scope", "tenant"}});
        t->m_scans_ = t->metrics_->counter("lights3_usage_scans_total",
                                           "Full bucket usage counts completed");
        t->m_last_scan_ = t->metrics_->gauge(
            "lights3_usage_last_scan_timestamp_seconds",
            "Unix time of the most recent full bucket usage count on this instance");
    }
    if (cfg.enabled) {
        auto persisted = co_await t->read_persisted();
        std::vector<std::string> fresh;
        {
            std::lock_guard lk(t->mu_);
            for (auto& [b, u] : persisted) {
                t->usage_[b] = u;
                if (t->claim_gauge_locked(b)) fresh.push_back(b);
            }
        }
        for (auto& b : fresh) t->register_gauges(b);
        if (!persisted.empty())
            LOG_INFO("usage: loaded counters for {} bucket(s) from {}/{}", persisted.size(),
                     storage::kSysBucketName, kPrefix);
    }
    co_return t;
}

UsageTracker::~UsageTracker() { shutdown_background(); }

Task<std::map<std::string, BucketUsage>> UsageTracker::read_persisted() {
    std::map<std::string, BucketUsage> out;
    auto backend = router_.default_backend();
    if (!backend || !co_await backend->bucket_exists(storage::kSysBucketName)) co_return out;
    sys_bucket_ready_ = true;
    storage::ListOptions opt;
    opt.prefix = std::string(kPrefix);
    for (;;) {
        auto page = co_await backend->list_objects(storage::kSysBucketName, opt);
        for (auto& obj : page.objects) {
            std::string bucket = obj.key.substr(kPrefix.size());
            try {
                auto stream =
                    co_await backend->get_object(storage::kSysBucketName, obj.key, std::nullopt);
                auto body = co_await read_all(*stream.body);
                if (auto u = deserialize(body))
                    out.emplace(std::move(bucket), *u);
                else
                    LOG_WARN("usage: skipping malformed object {}/{}", storage::kSysBucketName,
                             obj.key);
            } catch (const std::exception& e) {
                LOG_WARN("usage: failed to load {}: {}", obj.key, e.what());
            }
        }
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    co_return out;
}

Task<void> UsageTracker::ensure_sys_bucket() {
    if (sys_bucket_ready_.load()) co_return;
    try {
        co_await router_.default_backend()->create_bucket(storage::kSysBucketName);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
    }
    sys_bucket_ready_ = true;
}

Task<void> UsageTracker::persist(const std::string& bucket, BucketUsage u) {
    co_await ensure_sys_bucket();
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader body(serialize(u));
    co_await router_.default_backend()->put_object(storage::kSysBucketName, object_key(bucket),
                                                   std::move(meta), body);
}

// ---------- counters ----------

bool UsageTracker::claim_gauge_locked(const std::string& bucket) {
    if (!metrics_ || gauged_.contains(bucket) || gauged_.size() >= kMaxGaugedBuckets) return false;
    gauged_.insert(bucket);
    return true;
}

void UsageTracker::register_gauges(const std::string& bucket) {
    // Callback gauges read the map under the lock at render time; the closure only
    // captures `this` and the name, and shutdown removes the series before teardown
    metrics_->gauge_callback(
        "lights3_bucket_usage_bytes", "Committed object bytes per bucket (usage accounting)",
        [this, bucket] {
            std::lock_guard lk(mu_);
            auto it = usage_.find(bucket);
            return it == usage_.end() ? 0.0 : double(it->second.bytes);
        },
        {{"bucket", bucket}});
    metrics_->gauge_callback(
        "lights3_bucket_usage_objects", "Committed objects per bucket (usage accounting)",
        [this, bucket] {
            std::lock_guard lk(mu_);
            auto it = usage_.find(bucket);
            return it == usage_.end() ? 0.0 : double(it->second.objects);
        },
        {{"bucket", bucket}});
}

void UsageTracker::apply(const std::string& bucket, int64_t d_objects, int64_t d_bytes,
                         int64_t d_mpu_bytes) {
    if (!cfg_.enabled) return;
    bool fresh = false;
    {
        std::lock_guard lk(mu_);
        auto& u = usage_[bucket];
        u.objects = std::max<int64_t>(0, u.objects + d_objects);
        u.bytes = std::max<int64_t>(0, u.bytes + d_bytes);
        u.mpu_bytes = std::max<int64_t>(0, u.mpu_bytes + d_mpu_bytes);
        u.dirty = true;
        fresh = claim_gauge_locked(bucket);
    }
    if (fresh) register_gauges(bucket);
}

std::optional<BucketUsage> UsageTracker::get(const std::string& bucket) const {
    std::lock_guard lk(mu_);
    auto it = usage_.find(bucket);
    if (it == usage_.end()) return std::nullopt;
    return it->second;
}

std::map<std::string, BucketUsage> UsageTracker::all() const {
    std::lock_guard lk(mu_);
    return usage_;
}

BucketUsage UsageTracker::sum(const std::vector<std::string>& buckets) const {
    BucketUsage total;
    std::lock_guard lk(mu_);
    for (auto& b : buckets) {
        auto it = usage_.find(b);
        if (it == usage_.end()) continue;
        total.objects += it->second.objects;
        total.bytes += it->second.bytes;
        total.mpu_bytes += it->second.mpu_bytes;
    }
    return total;
}

void UsageTracker::quota_rejected(bool tenant_scope) {
    if (tenant_scope) {
        if (m_reject_tenant_) m_reject_tenant_->inc();
    } else if (m_reject_bucket_) {
        m_reject_bucket_->inc();
    }
}

// ---------- scans ----------

Task<BucketUsage> UsageTracker::rescan(std::string bucket) {
    {
        std::lock_guard lk(mu_);
        if (!scanning_.insert(bucket).second)
            throw S3Error(S3ErrorCode::SlowDown, "A usage scan of this bucket is already running.");
    }
    struct Unmark {
        UsageTracker* t;
        const std::string& b;
        ~Unmark() {
            std::lock_guard lk(t->mu_);
            t->scanning_.erase(b);
        }
    } unmark{this, bucket};

    auto& backend = router_.resolve(bucket);
    BucketUsage u;
    u.scanned_at = std::chrono::system_clock::now();  // taken before the walk: writes
                                                      // landing during it may or may
                                                      // not be counted, so the stamp
                                                      // must not claim them
    storage::ListOptions opt;
    opt.max_keys = 1000;
    for (;;) {
        if (bg_.closing()) throw S3Error(S3ErrorCode::SlowDown, "Shutting down.");
        auto page = co_await backend.list_objects(bucket, opt);
        for (auto& o : page.objects) {
            ++u.objects;
            u.bytes += static_cast<int64_t>(o.size);
        }
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    storage::ListUploadsOptions uopt;
    uopt.max_uploads = 1000;
    for (;;) {
        if (bg_.closing()) throw S3Error(S3ErrorCode::SlowDown, "Shutting down.");
        auto page = co_await backend.list_multipart_uploads(bucket, uopt);
        for (auto& up : page.uploads) {
            storage::ListPartsOptions popt;
            popt.max_parts = storage::kMaxParts;
            try {
                auto parts = co_await backend.list_parts(bucket, up.key, up.upload_id, popt);
                for (auto& p : parts.parts) u.mpu_bytes += static_cast<int64_t>(p.size);
            } catch (const S3Error& e) {
                if (e.code != S3ErrorCode::NoSuchUpload) throw;  // completed/aborted meanwhile
            }
        }
        if (!page.is_truncated) break;
        uopt.key_marker = page.next_key_marker;
        uopt.upload_id_marker = page.next_upload_id_marker;
    }
    bool fresh = false;
    {
        std::lock_guard lk(mu_);
        usage_[bucket] = u;
        fresh = claim_gauge_locked(bucket);
    }
    if (fresh) register_gauges(bucket);
    co_await persist(bucket, u);
    scans_.fetch_add(1);
    if (m_scans_) m_scans_->inc();
    if (m_last_scan_) m_last_scan_->set(to_unix(u.scanned_at));
    LOG_INFO("usage: bucket {} counted: {} object(s), {} byte(s), {} in-flight multipart byte(s)",
             bucket, u.objects, u.bytes, u.mpu_bytes);
    co_return u;
}

Task<size_t> UsageTracker::reconcile_all() {
    size_t n = 0;
    std::set<std::string> seen;
    for (auto& [name, backend] : router_.backends()) {
        std::vector<storage::BucketInfo> buckets;
        try {
            buckets = co_await backend->list_buckets();
        } catch (const std::exception& e) {
            LOG_WARN("usage: cannot list buckets of backend {}: {}", name, e.what());
            continue;
        }
        for (auto& b : buckets) {
            if (b.name == storage::kSysBucketName || !seen.insert(b.name).second) continue;
            // Only buckets this backend actually serves (a name present on several
            // backends is routed to exactly one; the others are shadow copies)
            if (&router_.resolve(b.name) != backend.get()) continue;
            if (bg_.closing()) co_return n;
            try {
                co_await rescan(b.name);
                ++n;
            } catch (const std::exception& e) {
                LOG_WARN("usage: scan of bucket {} failed: {}", b.name, e.what());
            }
        }
    }
    // Buckets with counters but no longer present anywhere: drop them
    std::vector<std::string> gone;
    {
        std::lock_guard lk(mu_);
        for (auto& [b, _] : usage_)
            if (!seen.contains(b)) gone.push_back(b);
    }
    for (auto& b : gone) {
        try {
            co_await remove(b);
        } catch (const std::exception& e) {
            LOG_WARN("usage: cannot drop stale counters of {}: {}", b, e.what());
        }
    }
    co_return n;
}

Task<void> UsageTracker::bootstrap_scan() {
    // Buckets that exist but have no record (fresh deployment, or created while the
    // feature was off) start from zero: count them once so quotas mean something
    std::vector<std::string> todo;
    std::set<std::string> seen;
    for (auto& [name, backend] : router_.backends()) {
        try {
            for (auto& b : co_await backend->list_buckets()) {
                if (b.name == storage::kSysBucketName || !seen.insert(b.name).second) continue;
                if (&router_.resolve(b.name) != backend.get()) continue;
                auto u = get(b.name);
                if (!u || !u->scanned()) todo.push_back(b.name);
            }
        } catch (const std::exception& e) {
            LOG_WARN("usage: cannot list buckets of backend {}: {}", name, e.what());
        }
    }
    for (auto& b : todo) {
        if (bg_.closing()) co_return;
        try {
            co_await rescan(b);
        } catch (const std::exception& e) {
            LOG_WARN("usage: bootstrap scan of bucket {} failed: {}", b, e.what());
        }
    }
}

Task<void> UsageTracker::flush() {
    std::vector<std::pair<std::string, BucketUsage>> dirty;
    {
        std::lock_guard lk(mu_);
        for (auto& [b, u] : usage_)
            if (u.dirty) dirty.emplace_back(b, u);
    }
    for (auto& [b, u] : dirty) {
        try {
            co_await persist(b, u);
            std::lock_guard lk(mu_);
            auto it = usage_.find(b);
            // Clear only if nothing changed meanwhile (compare the persisted snapshot)
            if (it != usage_.end() && it->second.objects == u.objects &&
                it->second.bytes == u.bytes && it->second.mpu_bytes == u.mpu_bytes)
                it->second.dirty = false;
        } catch (const std::exception& e) {
            LOG_WARN("usage: flush of bucket {} failed: {}", b, e.what());
        }
    }
}

Task<void> UsageTracker::sync_now() {
    auto persisted = co_await read_persisted();
    size_t adopted = 0;
    std::vector<std::string> fresh;
    {
        std::lock_guard lk(mu_);
        for (auto& [b, p] : persisted) {
            auto it = usage_.find(b);
            // Adopt a peer's scan only when it is newer than the base we already hold:
            // a peer's mere flush carries its own partial view, never a better one
            if (it == usage_.end()) {
                usage_[b] = p;
                if (claim_gauge_locked(b)) fresh.push_back(b);
                ++adopted;
            } else if (p.scanned_at > it->second.scanned_at) {
                it->second = p;
                ++adopted;
            }
        }
    }
    for (auto& b : fresh) register_gauges(b);
    if (adopted) LOG_INFO("usage sync: adopted {} newer record(s)", adopted);
}

Task<void> UsageTracker::remove(const std::string& bucket) {
    bool gauged = false;
    {
        std::lock_guard lk(mu_);
        usage_.erase(bucket);
        gauged = gauged_.erase(bucket) > 0;
    }
    if (gauged && metrics_) metrics_->remove_labeled("bucket", bucket);
    if (!cfg_.enabled) co_return;
    try {
        co_await router_.default_backend()->delete_object(storage::kSysBucketName,
                                                          object_key(bucket));
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::NoSuchKey && e.code != S3ErrorCode::NoSuchBucket) throw;
    }
}

// ---------- background ----------

void UsageTracker::start_background(std::shared_ptr<ThreadPool> pool, int sync_interval_sec) {
    if (!cfg_.enabled) return;
    pool_ = std::move(pool);
    sync_interval_sec_ = sync_interval_sec;
    schedule_flush();
    schedule_reconcile();
    schedule_sync();
    if (cfg_.reconcile)
        bg_.spawn([](UsageTracker* t) -> Task<void> {
            co_await t->pool_->schedule();
            co_await t->bootstrap_scan();
        }(this));
}

void UsageTracker::shutdown_background() {
    bg_.begin_close();
    TimerQueue::instance().cancel(flush_timer_);
    TimerQueue::instance().cancel(reconcile_timer_);
    TimerQueue::instance().cancel(sync_timer_);
    bg_.wait_idle();
    // Final flush so a clean restart resumes with the latest counters. Runs on the
    // caller's thread through the backend directly (the pool may already be draining)
    if (cfg_.enabled && pool_) {
        try {
            sync_wait(flush());
        } catch (const std::exception& e) {
            LOG_WARN("usage: final flush failed: {}", e.what());
        }
        pool_.reset();
    }
    if (metrics_) {
        std::set<std::string> gauged;
        {
            std::lock_guard lk(mu_);
            gauged.swap(gauged_);
        }
        for (auto& b : gauged) metrics_->remove_labeled("bucket", b);
    }
}

Task<void> UsageTracker::flush_tick() {
    co_await pool_->schedule();
    std::exception_ptr err;
    try {
        co_await flush();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_flush();
    if (err) std::rethrow_exception(err);
}

Task<void> UsageTracker::reconcile_tick() {
    co_await pool_->schedule();
    std::exception_ptr err;
    try {
        size_t n = co_await reconcile_all();
        LOG_INFO("usage: reconcile pass counted {} bucket(s)", n);
    } catch (...) {
        err = std::current_exception();
    }
    schedule_reconcile();
    if (err) std::rethrow_exception(err);
}

Task<void> UsageTracker::sync_tick() {
    co_await pool_->schedule();
    std::exception_ptr err;
    try {
        co_await sync_now();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_sync();
    if (err) std::rethrow_exception(err);
}

void UsageTracker::schedule_flush() {
    if (cfg_.flush_interval_sec <= 0) return;
    bg_.if_open([&] {
        flush_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.flush_interval_sec),
                                                  [this] { bg_.spawn(flush_tick()); });
    });
}

void UsageTracker::schedule_reconcile() {
    if (!cfg_.reconcile || cfg_.reconcile_interval_sec <= 0) return;
    bg_.if_open([&] {
        reconcile_timer_ =
            TimerQueue::instance().add(std::chrono::seconds(cfg_.reconcile_interval_sec),
                                       [this] { bg_.spawn(reconcile_tick()); });
    });
}

void UsageTracker::schedule_sync() {
    if (sync_interval_sec_ <= 0) return;
    bg_.if_open([&] {
        sync_timer_ = TimerQueue::instance().add(std::chrono::seconds(sync_interval_sec_),
                                                 [this] { bg_.spawn(sync_tick()); });
    });
}

}  // namespace lights3::s3
