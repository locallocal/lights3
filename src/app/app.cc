#include "app/app.h"

#include <csignal>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <thread>

#include "core/log.h"
#include "http/admission.h"
#include "s3/errors.h"
#include "storage/bucket_router.h"
#include "storage/registry.h"

namespace lights3 {

namespace {

// self-pipe: the signal handler does only a single write
// (async-signal-safe); the real shutdown is executed by a watchdog thread.
// Calling server->shutdown() directly in the handler is unsafe — the httplib
// driver's implementation takes an internal lock, and a signal landing on a
// thread already holding that lock self-deadlocks (docs/archive/gaps.md §3.9)
int g_sig_pipe[2] = {-1, -1};

void on_signal(int sig) {
    unsigned char b = static_cast<unsigned char>(sig);
    ssize_t n = ::write(g_sig_pipe[1], &b, 1);
    (void)n;  // A full pipe means a signal is already pending; dropping is fine
}

LogLevel parse_level(const std::string& s) {
    if (s == "debug") return LogLevel::Debug;
    if (s == "warn") return LogLevel::Warn;
    if (s == "error") return LogLevel::Error;
    return LogLevel::Info;
}

}  // namespace

Application::Application(const std::string& config_path) : cfg_(Config::load(config_path)) {
    Logger::init(parse_level(cfg_.log_level));
}

Application::~Application() { shutdown(); }

void Application::open_storage() {
    pool_ = std::make_shared<ThreadPool>(cfg_.runtime.io_threads);
    // Backend-level metrics registry: build hands each backend a scope
    // labeled backend=<name>, rendered appended to /-/metrics
    metrics_ = std::make_shared<MetricsRegistry>();
    backends_ = storage::StorageRegistry::build(cfg_.backends, pool_, metrics_);
}

void Application::start_server() {
    auto router = storage::BucketRouter::build(cfg_.buckets, backends_);
    auto auth = s3::SigV4Authenticator::build(cfg_.auth);
    // Dynamic credentials (docs/credential-management.md): loaded from the default backend, replacing the static lookup table
    cred_store_ = sync_wait(s3::CredentialStore::load(router.default_backend(), cfg_.auth));
    auth.set_provider(cred_store_);
    bool auth_enabled = auth.enabled();
    if (!auth_enabled)
        LOG_WARN("no credentials configured: authentication is DISABLED");
    // Static website hosting (docs/static-website.md): the store always exists — even
    // with an empty YAML list, PUT ?website can add sites at runtime. Static names get
    // the same validation gate as user requests (reserved names fail startup)
    for (auto& w : cfg_.website.buckets) storage::validate_bucket_name(w.bucket);
    website_store_ =
        sync_wait(s3::WebsiteStore::load(router.default_backend(), cfg_.website.buckets));
    // CORS rules (roadmap §2.1): dynamic-only (?cors API), persisted next to the
    // website entries in .sys
    cors_store_ = sync_wait(s3::CorsStore::load(router.default_backend()));
    // Lifecycle rules (roadmap §2.4): stored next to cors/website; the runner gets its
    // own router copy (S3Service owns the primary by value)
    lifecycle_store_ = sync_wait(s3::LifecycleStore::load(router.default_backend()));
    lifecycle_runner_ = std::make_unique<s3::LifecycleRunner>(
        storage::BucketRouter::build(cfg_.buckets, backends_), lifecycle_store_);
    // roadmap §3.9 (docs/multi-tenancy.md): audit file, usage counters, quotas,
    // tenants + bucket ownership. All persisted next to the other .sys records
    audit_ = s3::AuditLog::open(cfg_.audit);
    usage_ = sync_wait(s3::UsageTracker::load(storage::BucketRouter::build(cfg_.buckets, backends_),
                                              cfg_.usage, metrics_));
    quota_store_ = sync_wait(s3::QuotaStore::load(router.default_backend()));
    tenant_store_ = sync_wait(s3::TenantStore::load(router.default_backend()));
    owner_store_ = sync_wait(s3::OwnerStore::load(router.default_backend()));
    tenants_ = std::make_shared<s3::TenantRegistry>(tenant_store_, owner_store_);
    lifecycle_runner_->set_usage_tracker(usage_);
    if (!cfg_.usage.enabled)
        LOG_WARN("usage accounting is disabled: bucket/tenant quotas are not enforced");
    service_ = std::make_shared<s3::S3Service>(std::move(router), std::move(auth),
                                               cfg_.http.base_domain);
    service_->set_pool_stats([pool = pool_] { return pool->stats(); });
    service_->set_request_timeout(std::chrono::seconds(cfg_.http.request_timeout_sec));
    service_->set_min_part_size(cfg_.http.min_part_size);
    service_->set_backend_metrics(metrics_);
    service_->set_credential_store(cred_store_);
    service_->set_website_store(website_store_);
    service_->set_cors_store(cors_store_);
    service_->set_lifecycle_store(lifecycle_store_);
    service_->set_usage_tracker(usage_);
    service_->set_quota_store(quota_store_);
    service_->set_tenant_registry(tenants_);
    service_->set_audit_log(audit_);
    if (!cfg_.website.buckets.empty() && !auth_enabled)
        LOG_WARN("website: buckets configured but authentication is disabled; "
                 "all buckets are already anonymously accessible");
    // Phase-2 background tasks (docs/credential-management.md
    // §10.2/§10.3): credentials_file hot-reload polling + periodic
    // multi-instance incremental sync (both gated by config)
    cred_store_->start_background(pool_);
    // Website/CORS entries share the same multi-instance sync knob (docs/static-website.md §4)
    website_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    cors_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    lifecycle_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    quota_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    tenant_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    owner_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    // Counter flush / reconcile / multi-instance adoption (usage.*, auth.sync_interval)
    usage_->start_background(pool_, cfg_.auth.sync_interval_sec);
    // Enforcement scan (lifecycle.scan_interval, 0 = disabled)
    lifecycle_runner_->start_background(pool_, cfg_.lifecycle.scan_interval_sec);

    server_ = http::HttpServerFactory::create(cfg_.http.driver, cfg_.http);
    // Dispatch-entry admission control (docs/concurrency.md §6):
    // over-limit requests queue on the semaphore instead of being
    // rejected; waiters are woken via the pool executor, avoiding running
    // an entire request coroutine chain inline on the releasing call stack
    pool_exec_ = std::make_shared<ThreadPoolExecutor>(*pool_);
    inflight_ = std::make_shared<AsyncSemaphore>(cfg_.runtime.max_inflight_requests,
                                                 pool_exec_.get());
    // Observability for the admission gate and the timer thread
    // (docs/archive/gaps.md §7): under load testing, "stuck at admission" vs
    // "stuck in the pool" and "how long the timer was blocked by a slow
    // callback" can all be read straight from /-/metrics
    service_->set_admission_stats(
        [inflight = inflight_, cap = cfg_.runtime.max_inflight_requests]() -> s3::AdmissionStats {
            return {cap, inflight->available(), inflight->waiting()};
        });
    service_->set_timer_stats([] { return TimerQueue::instance().stats(); });
    // L1 connection counters + per-client rate limits (roadmap §4.2)
    service_->set_conn_stats([this]() -> http::ConnStats {
        return server_ ? server_->stats() : http::ConnStats{};
    });
    {
        auto& rl = cfg_.ratelimit;
        std::shared_ptr<s3::RateLimiter> ip, ak;
        if (rl.per_ip_rps > 0 || rl.per_ip_max_inflight > 0)
            ip = std::make_shared<s3::RateLimiter>(
                s3::RateLimiter::Limits{rl.per_ip_rps, rl.per_ip_burst, rl.per_ip_max_inflight},
                static_cast<size_t>(rl.max_tracked));
        if (rl.per_ak_rps > 0 || rl.per_ak_max_inflight > 0)
            ak = std::make_shared<s3::RateLimiter>(
                s3::RateLimiter::Limits{rl.per_ak_rps, rl.per_ak_burst, rl.per_ak_max_inflight},
                static_cast<size_t>(rl.max_tracked));
        if (ip || ak)
            LOG_INFO("rate limits: per-ip rps={} burst={} inflight={}; per-ak rps={} burst={} inflight={}",
                     rl.per_ip_rps, rl.per_ip_burst, rl.per_ip_max_inflight, rl.per_ak_rps,
                     rl.per_ak_burst, rl.per_ak_max_inflight);
        service_->set_rate_limiters(std::move(ip), std::move(ak));
    }
    // Process shutdown broadcast (the third cancel source of
    // docs/concurrency.md §5): fired after run() returns, so in-flight
    // requests converge from their nearest cancellable suspension point
    // instead of waiting out their individual request_timeouts
    shutdown_src_ = std::make_shared<CancelSource>();
    auto stall = std::chrono::seconds(cfg_.http.transfer_stall_timeout_sec);
    // Assembly of queueing / Permit lifetime / cancellation convergence lives in http/admission.h (shared with the unit tests)
    // The stall guard's progress threshold must not exceed the streaming chunk size:
    // with io_chunk_size configured below 64KiB, a single read could never count as
    // progress and every window would kill a healthy slow connection (roadmap §1.4)
    auto stall_progress = std::min<uint64_t>(http::StallGuardReader::kMinProgressBytes,
                                             cfg_.http.io_chunk_size);
    server_->set_handler(http::make_admission_handler(
        inflight_, stall, shutdown_src_,
        [service = service_](http::HttpRequest req) { return service->dispatch(std::move(req)); },
        stall_progress));
    server_->listen(cfg_.http.bind, cfg_.http.port);
}

int Application::run() {
    // Signal -> self-pipe -> watchdog-thread shutdown (see the comment on on_signal)
    if (::pipe2(g_sig_pipe, O_CLOEXEC) != 0)
        throw std::runtime_error("cannot create signal pipe");
    std::thread sig_thread([this] {
        unsigned char b = 0;
        while (::read(g_sig_pipe[0], &b, 1) == 1) {
            LOG_INFO("signal {} received, shutting down", int(b));
            server_->shutdown();
        }
    });
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);  // An uninitialized mask is an undefined blocking set
    sa.sa_flags = SA_RESTART;  // With the self-pipe scheme, no need to interrupt syscalls via EINTR
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("lights3 started: driver={} backends={} pool={}", cfg_.http.driver,
             cfg_.backends.size(), cfg_.runtime.io_threads);
    server_->run();  // Blocks until SIGINT/SIGTERM

    // Reap the shutdown watchdog thread first: only then is destroying server safe
    ::close(g_sig_pipe[1]);
    g_sig_pipe[1] = -1;
    if (sig_thread.joinable()) sig_thread.join();
    ::close(g_sig_pipe[0]);
    g_sig_pipe[0] = -1;

    // run() returning does **not** mean in-flight requests have reached
    // zero (drivers return unconditionally after grace + force close,
    // docs/archive/gaps.md §2.1). Broadcast cancellation first so in-flight
    // requests converge from their suspension points, then wait for
    // permits to return; otherwise the backend close() in shutdown() would
    // touch the same backend concurrently with still-running requests
    shutdown_src_->request_cancel();
    inflight_->close();
    {
        // The driver-side connection grace is a separate quantity
        // (drivers/common.h kShutdownGrace); what we wait for here is
        // permits returning. Write the literal in one place only: copying
        // "10s" into the log message would drift out of sync eventually
        constexpr auto kDrainDeadline = std::chrono::seconds(10);
        const int max_inflight = cfg_.runtime.max_inflight_requests;
        auto deadline = std::chrono::steady_clock::now() + kDrainDeadline;
        while (inflight_->available() < max_inflight &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        if (inflight_->available() < max_inflight)
            LOG_ERROR("{} request(s) still in flight after {}s; proceeding with shutdown",
                      max_inflight - inflight_->available(), kDrainDeadline.count());
    }

    shutdown();
    LOG_INFO("lights3 exited cleanly");
    return 0;
}

void Application::close_backends() noexcept {
    // close() must come **before** pool->join() — it internally
    // co_awaits pool->schedule() (a joined pool throws post-after-join),
    // and destructor fallback is not equivalent to close (duostore skips
    // sealing the active pack and the rados flush; tiered drops the atime
    // snapshot). Close one by one so a single failure does not block the
    // rest; calling repeatedly is harmless (close is idempotent)
    for (auto& [name, backend] : backends_) {
        try {
            sync_wait(backend->close());
        } catch (const std::exception& e) {
            LOG_ERROR("backend {} close failed: {}", name, e.what());
        }
    }
}

void Application::shutdown() noexcept {
    try {
        // Timers / in-flight sync must wind down before the thread pool
        if (cred_store_) cred_store_->shutdown_background();
        if (website_store_) website_store_->shutdown_background();
        if (cors_store_) cors_store_->shutdown_background();
        if (lifecycle_runner_) lifecycle_runner_->shutdown_background();
        if (lifecycle_store_) lifecycle_store_->shutdown_background();
        if (usage_) usage_->shutdown_background();  // final counter flush happens here
        if (quota_store_) quota_store_->shutdown_background();
        if (tenant_store_) tenant_store_->shutdown_background();
        if (owner_store_) owner_store_->shutdown_background();
    } catch (const std::exception& e) {
        LOG_ERROR("store background shutdown failed: {}", e.what());
    }
    close_backends();
    // The backends' shared_ptrs are still held by service (via router)
    // and handler (via server), so clearing backends_ alone triggers
    // no destruction. Release in reverse ownership order so backend
    // destruction happens **before** pool->join() — destructors still use
    // the pool (docs/archive/gaps.md §3.9)
    server_.reset();
    service_.reset();
    inflight_.reset();  // holds a raw pointer into pool_exec_; must go first
    pool_exec_.reset();
    shutdown_src_.reset();
    cred_store_.reset();
    website_store_.reset();
    cors_store_.reset();
    lifecycle_runner_.reset();
    lifecycle_store_.reset();
    tenants_.reset();
    owner_store_.reset();
    tenant_store_.reset();
    quota_store_.reset();
    usage_.reset();
    audit_.reset();
    backends_.clear();
    if (pool_) {
        try {
            pool_->join();
        } catch (const std::exception& e) {
            LOG_ERROR("thread pool join failed: {}", e.what());
        }
        pool_.reset();
    }
    metrics_.reset();
}

}  // namespace lights3
