#include "app/app.h"

#include <fcntl.h>
#include <unistd.h>
#include <csignal>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

#include "core/fault.h"
#include "core/log.h"
#include "core/version.h"
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
    // A full pipe means a signal is already pending; dropping is fine
    (void)n;
}

}  // namespace

Application::Application(const std::string& config_path) : config_path_(config_path), cfg_(Config::load(config_path)) {
    Logger::init(cfg_.log);
    // Fault injection (roadmap §6.1): LIGHTS3_FAULTS arms named IO failure points
    fault::arm_from_env();
    if (auto d = fault::describe(); !d.empty()) LOG_WARN("fault injection armed: {}", d);
}

Application::~Application() { shutdown(); }

void Application::open_storage() {
    pool_ = std::make_shared<ThreadPool>(cfg_.runtime.io_threads);
    // Backend-level metrics registry: build hands each backend a scope
    // labeled backend=<name>, rendered appended to /-/metrics
    metrics_ = std::make_shared<MetricsRegistry>();
    // Build identity as a constant-1 gauge (the Prometheus *_build_info idiom,
    // roadmap §6.3): `lights3_build_info{version=,commit=,build_type=}` lets a
    // fleet dashboard tell which build every instance is running
    metrics_
        ->gauge("lights3_build_info", "Build identity of this lights3 process (always 1)",
                {{"version", version()}, {"commit", git_commit()}, {"build_type", build_type()}})
        ->set(1);
    backends_ = storage::StorageRegistry::build(cfg_.backends, pool_, metrics_);
}

void Application::start_server() {
    // The data plane routes to metered decorators (roadmap §5.1: per-backend op
    // histograms + the per-request backend-time accumulator); the raw instances
    // stay in backends_ for close() and the offline admin tasks
    metered_ = storage::meter_backends(backends_, metrics_);
    auto router = storage::BucketRouter::build(cfg_.buckets, metered_);
    auto auth = s3::SigV4Authenticator::build(cfg_.auth);
    // Dynamic credentials (docs/credential-management.md): loaded from the default backend, replacing the static lookup
    // table
    cred_store_ = sync_wait(s3::CredentialStore::load(router.default_backend(), cfg_.auth));
    auth.set_provider(cred_store_);
    bool auth_enabled = auth.enabled();
    if (!auth_enabled) LOG_WARN("no credentials configured: authentication is DISABLED");
    // Static website hosting (docs/static-website.md): the store always exists — even
    // with an empty YAML list, PUT ?website can add sites at runtime. Static names get
    // the same validation gate as user requests (reserved names fail startup)
    for (auto& w : cfg_.website.buckets) storage::validate_bucket_name(w.bucket);
    website_store_ = sync_wait(s3::WebsiteStore::load(router.default_backend(), cfg_.website.buckets));
    // CORS rules (roadmap §2.1): dynamic-only (?cors API), persisted next to the
    // website entries in .sys
    cors_store_ = sync_wait(s3::CorsStore::load(router.default_backend()));
    // mTLS identity bindings (backlog-sequence ⑥, docs/tls.md §2.1): the table is
    // always loaded (root may prepare bindings before switching the mode on)
    tls_identity_store_ = sync_wait(s3::TlsIdentityStore::load(router.default_backend()));
    // Lifecycle rules (roadmap §2.4): stored next to cors/website; the runner gets its
    // own router copy (S3Service owns the primary by value)
    lifecycle_store_ = sync_wait(s3::LifecycleStore::load(router.default_backend()));
    // Copies of the one router share its table: rule swaps and backend hot add /
    // remove (roadmap §4.4, backlog-sequence ⑦) reach the runner and the usage
    // tracker through the same snapshot the service resolves against
    lifecycle_runner_ = std::make_unique<s3::LifecycleRunner>(router, lifecycle_store_);
    // roadmap §3.9 (docs/multi-tenancy.md): audit file, usage counters, quotas,
    // tenants + bucket ownership. All persisted next to the other .sys records
    audit_ = s3::AuditLog::open(cfg_.audit);
    usage_ = sync_wait(s3::UsageTracker::load(router, cfg_.usage, metrics_));
    quota_store_ = sync_wait(s3::QuotaStore::load(router.default_backend()));
    tenant_store_ = sync_wait(s3::TenantStore::load(router.default_backend()));
    owner_store_ = sync_wait(s3::OwnerStore::load(router.default_backend()));
    tenants_ = std::make_shared<s3::TenantRegistry>(tenant_store_, owner_store_);
    lifecycle_runner_->set_usage_tracker(usage_);
    if (!cfg_.usage.enabled) LOG_WARN("usage accounting is disabled: bucket/tenant quotas are not enforced");
    service_ = std::make_shared<s3::S3Service>(std::move(router), std::move(auth), cfg_.http.base_domain);
    service_->set_pool_stats([pool = pool_] { return pool->stats(); });
    service_->set_request_timeout(std::chrono::seconds(cfg_.http.request_timeout_sec));
    service_->set_min_part_size(cfg_.http.min_part_size);
    service_->set_slow_request_threshold(std::chrono::milliseconds(cfg_.log.slow_request_threshold_ms));
    service_->set_backend_metrics(metrics_);
    service_->set_credential_store(cred_store_);
    service_->set_website_store(website_store_);
    service_->set_cors_store(cors_store_);
    service_->set_tls_identity_store(tls_identity_store_);
    service_->set_tls_identity_mode(s3::S3Service::parse_tls_identity_mode(cfg_.auth.tls_identity));
    if (cfg_.auth.tls_identity != "off") {
        if (!auth_enabled)
            LOG_WARN(
                "auth.tls_identity={} but authentication is disabled: certificate "
                "bindings have nothing to map to",
                cfg_.auth.tls_identity);
        else
            LOG_INFO("mTLS identity mapping on ({}): {} binding(s) in .sys/tls-identities", cfg_.auth.tls_identity,
                     tls_identity_store_->snapshot()->size());
    }
    service_->set_lifecycle_store(lifecycle_store_);
    service_->set_usage_tracker(usage_);
    service_->set_quota_store(quota_store_);
    service_->set_tenant_registry(tenants_);
    service_->set_audit_log(audit_);
    if (!cfg_.website.buckets.empty() && !auth_enabled)
        LOG_WARN(
            "website: buckets configured but authentication is disabled; "
            "all buckets are already anonymously accessible");
    // Phase-2 background tasks (docs/credential-management.md
    // §10.2/§10.3): credentials_file hot-reload polling + periodic
    // multi-instance incremental sync (both gated by config)
    cred_store_->start_background(pool_);
    // Website/CORS entries share the same multi-instance sync knob (docs/static-website.md §4)
    website_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    cors_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
    tls_identity_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
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
    inflight_ = std::make_shared<AsyncSemaphore>(cfg_.runtime.max_inflight_requests, pool_exec_.get());
    // Observability for the admission gate and the timer thread
    // (docs/archive/gaps.md §7): under load testing, "stuck at admission" vs
    // "stuck in the pool" and "how long the timer was blocked by a slow
    // callback" can all be read straight from /-/metrics
    admission_counters_ = std::make_shared<http::AdmissionCounters>();
    service_->set_admission_stats([inflight = inflight_, ctr = admission_counters_]() -> s3::AdmissionStats {
        s3::AdmissionStats st;
        st.capacity = inflight->capacity();
        st.available = inflight->available();
        st.waiting = inflight->waiting();
        st.counters = true;
        auto ld = [](const std::atomic<uint64_t>& a) { return a.load(std::memory_order_relaxed); };
        for (size_t i = 0; i < st.wait_hist.size(); ++i) st.wait_hist[i] = ld(ctr->wait_hist[i]);
        st.wait_sum_us = ld(ctr->wait_sum_us);
        st.wait_count = ld(ctr->wait_count);
        st.queued = ld(ctr->queued);
        st.cancelled = ld(ctr->cancelled);
        st.stalls_in = ld(ctr->stalls_in);
        st.stalls_out = ld(ctr->stalls_out);
        return st;
    });
    service_->set_metrics_root_only(cfg_.http.metrics_access == "root");
    service_->set_reload_hook([this] { return reload_config(); });
    // Maintenance jobs on the live gateway (backlog-sequence ③ fsck; duostore
    // gc / scan and tier scan / gc / reconcile rounds; the quarantine ledgers):
    // the raw backends (not the metered decorators -- a round is a maintenance
    // traversal, not a request), one job per backend at a time, outcome kept for
    // polling. The service speaks (group, op) strings; the mapping to ops and the
    // 409 code (ScrubInProgress for fsck, JobInProgress for the rounds) live here
    admin_jobs_ = std::make_unique<AdminJobs>(backends_);
    auto job_failure = [](const AdminJobs::Failure& f, bool fsck) -> s3::S3Error {
        switch (f.code) {
            case AdminJobs::Error::NoSuchBackend:
                return s3::S3Error(s3::S3ErrorCode::NoSuchKey, f.message);
            case AdminJobs::Error::Unsupported:
                return s3::S3Error(s3::S3ErrorCode::InvalidRequest, f.message);
            case AdminJobs::Error::Busy:
                return s3::S3Error(fsck ? s3::S3ErrorCode::ScrubInProgress : s3::S3ErrorCode::JobInProgress, f.message);
        }
        return s3::S3Error(s3::S3ErrorCode::InternalError, f.message);
    };
    auto op_of = [](const std::string& group, const std::string& op) {
        auto o = parse_job_op(group, op);
        if (!o) throw s3::S3Error(s3::S3ErrorCode::InvalidRequest, "no operation '" + op + "' in '" + group + "'.");
        return *o;
    };
    service_->set_job_hooks(
        [this, job_failure, op_of](const std::string& backend, const std::string& group, const std::string& op,
                                   uint64_t bps) {
            JobOp o = op_of(group, op);
            try {
                nlohmann::json j = admin_jobs_->status(backend, o);
                j["job_id"] = admin_jobs_->start(backend, o, bps);
                j["running"] = true;
                j["busy"] = true;
                return j;
            } catch (const AdminJobs::Failure& f) {
                throw job_failure(f, o == JobOp::Fsck);
            }
        },
        [this, job_failure, op_of](const std::string& backend, const std::string& group, const std::string& op) {
            JobOp o = op_of(group, op);
            try {
                return admin_jobs_->status(backend, o);
            } catch (const AdminJobs::Failure& f) {
                throw job_failure(f, o == JobOp::Fsck);
            }
        },
        [this, job_failure](const std::string& backend, const std::string& group) {
            try {
                return admin_jobs_->quarantine(backend, group);
            } catch (const AdminJobs::Failure& f) {
                throw job_failure(f, false);
            }
        });
    service_->set_timer_stats([] { return TimerQueue::instance().stats(); });
    // L1 connection counters + per-client rate limits (roadmap §4.2)
    // Both listeners' counters add up (accepted/active/timeouts are per-listener
    // events; the sum is the process-wide view a dashboard wants)
    service_->set_conn_stats([this]() -> http::ConnStats {
        http::ConnStats s = server_ ? server_->stats() : http::ConnStats{};
        if (!admin_server_) return s;
        http::ConnStats a = admin_server_->stats();
        s.accepted += a.accepted;
        s.rejected_limit += a.rejected_limit;
        s.active += a.active;
        s.keepalive_closes += a.keepalive_closes;
        s.timeouts_idle += a.timeouts_idle;
        s.timeouts_header += a.timeouts_header;
        s.timeouts_body += a.timeouts_body;
        s.timeouts_write += a.timeouts_write;
        s.requests += a.requests;
        s.tls_handshakes_ok += a.tls_handshakes_ok;
        s.tls_handshakes_failed += a.tls_handshakes_failed;
        s.parse_errors += a.parse_errors;
        return s;
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
                     rl.per_ip_rps, rl.per_ip_burst, rl.per_ip_max_inflight, rl.per_ak_rps, rl.per_ak_burst,
                     rl.per_ak_max_inflight);
        service_->set_rate_limiters(std::move(ip), std::move(ak));
    }
    // Process shutdown broadcast (the third cancel source of
    // docs/concurrency.md §5): fired after run() returns, so in-flight
    // requests converge from their nearest cancellable suspension point
    // instead of waiting out their individual request_timeouts
    shutdown_src_ = std::make_shared<CancelSource>();
    stall_sec_ = std::make_shared<std::atomic<long>>(cfg_.http.transfer_stall_timeout_sec);
    // Assembly of queueing / Permit lifetime / cancellation convergence lives in http/admission.h (shared with the unit
    // tests) The stall guard's progress threshold must not exceed the streaming chunk size: with io_chunk_size
    // configured below 64KiB, a single read could never count as progress and every window would kill a healthy slow
    // connection (roadmap §1.4)
    auto stall_progress = std::min<uint64_t>(http::StallGuardReader::kMinProgressBytes, cfg_.http.io_chunk_size);
    server_->set_handler(http::make_admission_handler(
        inflight_, stall_sec_, shutdown_src_,
        [service = service_](http::HttpRequest req) { return service->dispatch(std::move(req)); }, stall_progress,
        admission_counters_));
    server_->listen(cfg_.http.bind, cfg_.http.port);

    // Separate admin listener (http.admin_port, backlog-sequence ②): the same
    // admission gate and cancellation source (its requests count toward
    // max_inflight_requests and drain under the same shutdown_grace), its own
    // driver instance; the handler flags every request as admin-face and the
    // service gates the /-/ endpoints on that flag
    if (cfg_.http.admin_port >= 0) {
        std::string admin_driver = cfg_.http.driver;
        if (admin_driver == "seastar") {
            auto drivers = http::HttpServerFactory::drivers();
            if (std::find(drivers.begin(), drivers.end(), "builtin") == drivers.end())
                throw std::runtime_error(
                    "http.admin_port with the seastar driver needs the "
                    "builtin driver compiled in for the admin listener");
            admin_driver = "builtin";
        }
        admin_server_ = http::HttpServerFactory::create(admin_driver, cfg_.http);
        admin_server_->set_handler(http::make_admission_handler(
            inflight_, stall_sec_, shutdown_src_,
            [service = service_](http::HttpRequest req) {
                req.admin_face = true;
                return service->dispatch(std::move(req));
            },
            stall_progress, admission_counters_));
        service_->set_admin_split(true);
        const std::string& abind = cfg_.http.admin_bind.empty() ? cfg_.http.bind : cfg_.http.admin_bind;
        admin_server_->listen(abind, static_cast<uint16_t>(cfg_.http.admin_port));
        LOG_INFO(
            "admin listener ({}): {}:{} serves /-/ (metrics, admin API); the data-plane "
            "port answers 404 for it",
            admin_driver, abind, admin_server_->bound_port());
    }
}

uint16_t Application::bound_port() const { return server_ ? server_->bound_port() : 0; }
uint16_t Application::admin_bound_port() const { return admin_server_ ? admin_server_->bound_port() : 0; }

int Application::run() {
    // Signal -> self-pipe -> watchdog-thread shutdown (see the comment on on_signal)
    if (::pipe2(g_sig_pipe, O_CLOEXEC) != 0) throw std::runtime_error("cannot create signal pipe");
    std::thread sig_thread([this] {
        unsigned char b = 0;
        while (::read(g_sig_pipe[0], &b, 1) == 1) {
            if (b == SIGHUP) {
                // Config hot reload (roadmap §4.4) on the watchdog thread: file IO and
                // the apply steps are all off the signal handler and off the request path
                LOG_INFO("SIGHUP received, reloading {}", config_path_);
                reload_config();
                continue;
            }
            LOG_INFO("signal {} received, shutting down", int(b));
            server_->shutdown();
            if (admin_server_) admin_server_->shutdown();
        }
    });
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    // An uninitialized mask is an undefined blocking set
    sigemptyset(&sa.sa_mask);
    // With the self-pipe scheme, no need to interrupt syscalls via EINTR
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    // reload, not terminate
    sigaction(SIGHUP, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("lights3 {} (git {}, {}) started: driver={} backends={} pool={}", version(), git_commit(), build_type(),
             cfg_.http.driver, cfg_.backends.size(), cfg_.runtime.io_threads);
    // The admin listener runs on its own thread for the lifetime of the data-plane
    // run(); both are stopped by the watchdog thread on a signal. Joined before the
    // watchdog so the order of teardown stays: listeners, watchdog, in-flight drain
    std::thread admin_thread;
    if (admin_server_) admin_thread = std::thread([this] { admin_server_->run(); });
    // Blocks until SIGINT/SIGTERM
    server_->run();
    if (admin_thread.joinable()) {
        // no-op when the watchdog already did it
        admin_server_->shutdown();
        admin_thread.join();
    }

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
    int rc = 0;
    {
        // Permit drain (roadmap §4.5): the same http.shutdown_grace that bounds the
        // driver's connection drain bounds how long we wait for permits to return
        // (streaming responses hold theirs past the driver's return). One knob,
        // one meaning; the wait is a condition variable, not a polling loop
        auto grace = std::chrono::seconds(cfg_.http.shutdown_grace_sec);
        if (!inflight_->wait_drained(grace)) {
            LOG_ERROR(
                "{} request(s) still in flight after http.shutdown_grace={}s; "
                "proceeding with shutdown",
                inflight_->capacity() - inflight_->available(), grace.count());
            rc = kExitUncleanShutdown;
        }
    }

    shutdown();
    if (!shutdown_clean()) rc = kExitUncleanShutdown;
    if (rc == 0)
        LOG_INFO("lights3 exited cleanly");
    else
        LOG_ERROR("lights3 exited with shutdown errors (exit code {})", rc);
    return rc;
}

// ---------- Config hot reload (roadmap §4.4, docs/config-reload.md) ----------

namespace {

template <class T>
std::string change(const char* key, const T& from, const T& to) {
    std::ostringstream os;
    os << key << ": " << from << " -> " << to;
    return os.str();
}

// Every key outside the reloadable subset: a change is reported as
// requires_restart and left alone. Kept as an explicit list so a new config key
// is either wired here or shows up in the report — never silently ignored
std::vector<std::string> restart_only_changes(const Config& a, const Config& b) {
    std::vector<std::string> out;
    auto cmp = [&](bool differs, const char* key) {
        if (differs) out.push_back(key);
    };
    auto& x = a.http;
    auto& y = b.http;
    cmp(x.driver != y.driver, "http.driver");
    cmp(x.bind != y.bind, "http.bind");
    cmp(x.port != y.port, "http.port");
    cmp(x.admin_bind != y.admin_bind || x.admin_port != y.admin_port, "http.admin_bind/admin_port");
    cmp(x.io_threads != y.io_threads, "http.io_threads");
    cmp(x.max_header_size != y.max_header_size, "http.max_header_size");
    cmp(x.idle_timeout_sec != y.idle_timeout_sec, "http.idle_timeout");
    cmp(x.header_timeout_sec != y.header_timeout_sec, "http.header_timeout");
    cmp(x.body_timeout_sec != y.body_timeout_sec, "http.body_timeout");
    cmp(x.write_timeout_sec != y.write_timeout_sec, "http.write_timeout");
    cmp(x.max_requests_per_connection != y.max_requests_per_connection, "http.max_requests_per_connection");
    cmp(x.max_connections != y.max_connections, "http.max_connections");
    cmp(x.base_domain != y.base_domain, "http.base_domain");
    cmp(x.tls_cert != y.tls_cert || x.tls_key != y.tls_key, "http.tls_cert/tls_key (paths)");
    cmp(x.tls_client_ca != y.tls_client_ca || x.tls_client_auth != y.tls_client_auth,
        "http.tls_client_ca/tls_client_auth");
    cmp(x.tls_min_version != y.tls_min_version || x.tls_ciphers != y.tls_ciphers ||
            x.tls_ciphersuites != y.tls_ciphersuites,
        "http.tls_min_version/tls_ciphers/tls_ciphersuites");
    cmp(x.tls_reload_interval_sec != y.tls_reload_interval_sec, "http.tls_reload_interval");
    cmp(x.tls_sni.size() != y.tls_sni.size(), "http.tls_sni");
    for (size_t i = 0; i < x.tls_sni.size() && i < y.tls_sni.size(); ++i)
        if (x.tls_sni[i].hosts != y.tls_sni[i].hosts || x.tls_sni[i].cert != y.tls_sni[i].cert ||
            x.tls_sni[i].key != y.tls_sni[i].key) {
            out.push_back("http.tls_sni");
            break;
        }
    cmp(x.drain_limit != y.drain_limit || x.trailer_max_size != y.trailer_max_size ||
            x.io_chunk_size != y.io_chunk_size || x.body_queue_cap != y.body_queue_cap,
        "http.drain_limit/trailer_max_size/io_chunk_size/body_queue_cap");
    cmp(x.shutdown_grace_sec != y.shutdown_grace_sec || x.shutdown_force_wait_sec != y.shutdown_force_wait_sec,
        "http.shutdown_grace/shutdown_force_wait");
    cmp(a.runtime.io_threads != b.runtime.io_threads, "runtime.io_threads");
    auto& p = a.auth;
    auto& q = b.auth;
    bool creds_differ = p.credentials.size() != q.credentials.size();
    for (size_t i = 0; !creds_differ && i < p.credentials.size(); ++i)
        creds_differ = p.credentials[i].access_key != q.credentials[i].access_key ||
                       static_cast<const std::string&>(p.credentials[i].secret_key) !=
                           static_cast<const std::string&>(q.credentials[i].secret_key);
    cmp(creds_differ, "auth.credentials");
    cmp(p.region != q.region || p.service != q.service, "auth.region/service");
    cmp(p.credentials_file != q.credentials_file || p.credentials_file_reload_sec != q.credentials_file_reload_sec,
        "auth.credentials_file/credentials_file_reload");
    cmp(p.sync_interval_sec != q.sync_interval_sec, "auth.sync_interval");
    cmp(p.tls_identity != q.tls_identity, "auth.tls_identity");
    // backends[] is judged entry by entry by plan_backends (add / remove apply,
    // the rest is reported there)
    cmp(a.buckets.default_backend != b.buckets.default_backend, "buckets.default_backend");
    cmp(a.website.buckets != b.website.buckets, "website");
    cmp(a.lifecycle.scan_interval_sec != b.lifecycle.scan_interval_sec, "lifecycle.scan_interval");
    cmp(a.usage.enabled != b.usage.enabled || a.usage.flush_interval_sec != b.usage.flush_interval_sec ||
            a.usage.reconcile_interval_sec != b.usage.reconcile_interval_sec || a.usage.reconcile != b.usage.reconcile,
        "usage");
    cmp(a.audit.path != b.audit.path || a.audit.data_plane != b.audit.data_plane ||
            a.audit.max_size != b.audit.max_size || a.audit.max_files != b.audit.max_files,
        "audit");
    cmp(a.ratelimit.max_tracked != b.ratelimit.max_tracked, "ratelimit.max_tracked");
    // Sink and formatter are built once at Logger::init (roadmap §5.2)
    cmp(a.log.format != b.log.format || a.log.file != b.log.file || a.log.max_size != b.log.max_size ||
            a.log.max_files != b.log.max_files || a.log.async != b.log.async ||
            a.log.async_queue != b.log.async_queue || a.log.async_overflow != b.log.async_overflow,
        "log.format/file/max_size/max_files/async*");
    return out;
}

bool rules_differ(const BucketsConfig& a, const BucketsConfig& b) {
    if (a.rules.size() != b.rules.size()) return true;
    for (size_t i = 0; i < a.rules.size(); ++i)
        if (a.rules[i].match != b.rules[i].match || a.rules[i].backend != b.rules[i].backend) return true;
    return false;
}

// Backend instance add / remove on reload (backlog-sequence ⑦): entries are
// matched by name. New names are built and routed; names gone from the file are
// removed once nothing references them; an entry whose type / parameters changed
// keeps running as configured before and is reported (a parameter change would
// mean rebuilding an instance that carries state). Removal is refused outright
// (whole reload) when the file still references the backend -- a remaining tiered
// entry naming it as local / cloud, or a maintenance job (fsck, duostore or tier
// round) running on it -- because the file describes a topology this process
// cannot run; removing the default backend is only deferred (it hosts .sys; the
// rest of the reload still applies)
struct BackendPlan {
    std::vector<BackendConfig> added;
    std::vector<std::string> removed;
    std::vector<std::string> requires_restart;
    std::string error;
};

BackendPlan plan_backends(const std::vector<BackendConfig>& running, const Config& fresh,
                          const std::string& default_name, const AdminJobs* jobs) {
    BackendPlan plan;
    auto find = [](const std::vector<BackendConfig>& v, const std::string& name) {
        for (auto& b : v)
            if (b.name == name) return &b;
        return static_cast<const BackendConfig*>(nullptr);
    };
    for (auto& b : fresh.backends) {
        auto* old = find(running, b.name);
        if (!old) {
            plan.added.push_back(b);
        } else if (old->type != b.type || old->params != b.params) {
            plan.requires_restart.push_back("backends (" + b.name +
                                            ": type/parameters changed; the running instance keeps "
                                            "its startup configuration)");
        }
    }
    for (auto& old : running) {
        if (find(fresh.backends, old.name)) continue;
        if (old.name == default_name) {
            plan.requires_restart.push_back("backends (" + old.name +
                                            ": the default backend cannot be removed at runtime)");
            continue;
        }
        for (auto& b : fresh.backends) {
            if (b.type != "tiered") continue;
            auto ref = [&](const char* k) {
                auto it = b.params.find(k);
                return it != b.params.end() && it->second == old.name;
            };
            if (ref("local") || ref("cloud")) {
                plan.error = "backends: cannot remove '" + old.name + "': tiered backend '" + b.name +
                             "' references it as " + (ref("local") ? "local" : "cloud");
                return plan;
            }
        }
        if (jobs && jobs->busy(old.name)) {
            plan.error = "backends: cannot remove '" + old.name +
                         "' while a maintenance job (fsck / duostore / tier round) runs on it (retry when it finishes)";
            return plan;
        }
        plan.removed.push_back(old.name);
    }
    return plan;
}

}  // namespace

ConfigReloadReport Application::reload_config() {
    std::lock_guard lk(reload_mu_);
    ConfigReloadReport report;
    Config fresh;
    try {
        // same parser + validation as startup
        fresh = Config::load(config_path_);
    } catch (const std::exception& e) {
        report.error = e.what();
        LOG_WARN("config reload refused, keeping the running configuration: {}", e.what());
        return report;
    }
    if (!service_) {
        report.error = "server not started";
        return report;
    }

    // Backends and bucket routing rules first: the steps that can still fail
    // (a backend that does not construct, a rule naming an unknown backend, an
    // unreachable rule) -- refused as a whole before anything else is touched.
    // New instances are built, metered and swapped into the router together with
    // the rules (one snapshot, backlog-sequence ⑦); removed ones leave the router
    // here and are closed on a retiring thread once their in-flight requests drain
    BackendPlan plan = plan_backends(cfg_.backends, fresh, cfg_.buckets.default_backend, admin_jobs_.get());
    if (!plan.error.empty()) {
        report.error = plan.error;
        LOG_WARN("config reload refused, keeping the running configuration: {}", report.error);
        return report;
    }
    bool rules_changed = rules_differ(cfg_.buckets, fresh.buckets);
    if (rules_changed || !plan.added.empty() || !plan.removed.empty()) {
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> built;
        try {
            if (!plan.added.empty()) built = storage::StorageRegistry::build(plan.added, pool_, metrics_, &backends_);
        } catch (const std::exception& e) {
            report.error = std::string("backends: ") + e.what();
            LOG_WARN("config reload refused, keeping the running configuration: {}", report.error);
            return report;
        }
        auto built_metered = storage::meter_backends(built, metrics_);
        auto candidate = metered_;
        for (auto& name : plan.removed) candidate.erase(name);
        for (auto& [name, b] : built_metered) candidate[name] = b;
        // A changed default_backend is reported by restart_only_changes; the router
        // keeps the running one (it hosts .sys), so the rules are validated against it
        BucketsConfig target = fresh.buckets;
        target.default_backend = cfg_.buckets.default_backend;
        try {
            service_->router().update(target, std::move(candidate));
        } catch (const std::exception& e) {
            // Nothing was swapped: tear the freshly built instances down again
            for (auto& [name, b] : built) {
                try {
                    sync_wait(b->close());
                } catch (const std::exception& ce) {
                    LOG_WARN("backend {} (rolled back): close failed: {}", name, ce.what());
                }
                metrics_->remove_labeled("backend", name);
            }
            report.error = std::string("buckets.rules: ") + e.what();
            LOG_WARN("config reload refused, keeping the running configuration: {}", report.error);
            return report;
        }
        if (rules_changed) {
            report.applied.push_back(change("buckets.rules", cfg_.buckets.rules.size(), fresh.buckets.rules.size()) +
                                     " rule(s)");
            cfg_.buckets.rules = fresh.buckets.rules;
        }
        for (auto& bc : plan.added) {
            backends_[bc.name] = built[bc.name];
            metered_[bc.name] = built_metered[bc.name];
            admin_jobs_->add_backend(bc.name, built[bc.name]);
            cfg_.backends.push_back(bc);
            report.applied.push_back("backends: added " + bc.name + " (" + bc.type + ")");
        }
        for (auto& name : plan.removed) {
            auto raw = backends_[name];
            auto metered = std::dynamic_pointer_cast<storage::MeteredBackend>(metered_[name]);
            backends_.erase(name);
            metered_.erase(name);
            // a job slipped in since the plan: it aborts on close
            if (!admin_jobs_->remove_backend(name))
                LOG_WARN("backend {} removed while a maintenance job runs on it; the job will abort", name);
            std::erase_if(cfg_.backends, [&](const BackendConfig& b) { return b.name == name; });
            retire_backend(name, std::move(raw), std::move(metered));
            report.applied.push_back("backends: removed " + name + " (closing after in-flight requests drain)");
        }
    }
    if (cfg_.log.level != fresh.log.level) {
        Logger::set_level(Logger::parse_level(fresh.log.level));
        report.applied.push_back(change("log.level", cfg_.log.level, fresh.log.level));
        cfg_.log.level = fresh.log.level;
    }
    if (cfg_.log.slow_request_threshold_ms != fresh.log.slow_request_threshold_ms) {
        service_->set_slow_request_threshold(std::chrono::milliseconds(fresh.log.slow_request_threshold_ms));
        report.applied.push_back(change("log.slow_request_threshold(ms)", cfg_.log.slow_request_threshold_ms,
                                        fresh.log.slow_request_threshold_ms));
        cfg_.log.slow_request_threshold_ms = fresh.log.slow_request_threshold_ms;
    }
    if (cfg_.http.request_timeout_sec != fresh.http.request_timeout_sec) {
        service_->set_request_timeout(std::chrono::seconds(fresh.http.request_timeout_sec));
        report.applied.push_back(
            change("http.request_timeout", cfg_.http.request_timeout_sec, fresh.http.request_timeout_sec));
        cfg_.http.request_timeout_sec = fresh.http.request_timeout_sec;
    }
    if (cfg_.http.transfer_stall_timeout_sec != fresh.http.transfer_stall_timeout_sec) {
        stall_sec_->store(fresh.http.transfer_stall_timeout_sec, std::memory_order_relaxed);
        report.applied.push_back(change("http.transfer_stall_timeout", cfg_.http.transfer_stall_timeout_sec,
                                        fresh.http.transfer_stall_timeout_sec));
        cfg_.http.transfer_stall_timeout_sec = fresh.http.transfer_stall_timeout_sec;
    }
    if (cfg_.http.metrics_access != fresh.http.metrics_access) {
        service_->set_metrics_root_only(fresh.http.metrics_access == "root");
        report.applied.push_back(change("http.metrics_access", cfg_.http.metrics_access, fresh.http.metrics_access));
        cfg_.http.metrics_access = fresh.http.metrics_access;
    }
    if (cfg_.http.min_part_size != fresh.http.min_part_size) {
        service_->set_min_part_size(fresh.http.min_part_size);
        report.applied.push_back(change("http.min_part_size", cfg_.http.min_part_size, fresh.http.min_part_size));
        cfg_.http.min_part_size = fresh.http.min_part_size;
    }
    if (cfg_.runtime.max_inflight_requests != fresh.runtime.max_inflight_requests) {
        inflight_->set_capacity(fresh.runtime.max_inflight_requests);
        report.applied.push_back(change("runtime.max_inflight_requests", cfg_.runtime.max_inflight_requests,
                                        fresh.runtime.max_inflight_requests));
        cfg_.runtime.max_inflight_requests = fresh.runtime.max_inflight_requests;
    }
    {
        auto& o = cfg_.ratelimit;
        auto& n = fresh.ratelimit;
        bool differs = o.per_ip_rps != n.per_ip_rps || o.per_ip_burst != n.per_ip_burst ||
                       o.per_ip_max_inflight != n.per_ip_max_inflight || o.per_ak_rps != n.per_ak_rps ||
                       o.per_ak_burst != n.per_ak_burst || o.per_ak_max_inflight != n.per_ak_max_inflight;
        if (differs) {
            std::shared_ptr<s3::RateLimiter> ip, ak;
            if (n.per_ip_rps > 0 || n.per_ip_max_inflight > 0)
                ip = std::make_shared<s3::RateLimiter>(
                    s3::RateLimiter::Limits{n.per_ip_rps, n.per_ip_burst, n.per_ip_max_inflight},
                    static_cast<size_t>(o.max_tracked));
            if (n.per_ak_rps > 0 || n.per_ak_max_inflight > 0)
                ak = std::make_shared<s3::RateLimiter>(
                    s3::RateLimiter::Limits{n.per_ak_rps, n.per_ak_burst, n.per_ak_max_inflight},
                    static_cast<size_t>(o.max_tracked));
            service_->set_rate_limiters(std::move(ip), std::move(ak));
            report.applied.push_back(
                "ratelimit: per-ip rps=" + std::to_string(n.per_ip_rps) + " burst=" + std::to_string(n.per_ip_burst) +
                " inflight=" + std::to_string(n.per_ip_max_inflight) + ", per-ak rps=" + std::to_string(n.per_ak_rps) +
                " burst=" + std::to_string(n.per_ak_burst) + " inflight=" + std::to_string(n.per_ak_max_inflight));
            o.per_ip_rps = n.per_ip_rps;
            o.per_ip_burst = n.per_ip_burst;
            o.per_ip_max_inflight = n.per_ip_max_inflight;
            o.per_ak_rps = n.per_ak_rps;
            o.per_ak_burst = n.per_ak_burst;
            o.per_ak_max_inflight = n.per_ak_max_inflight;
        }
    }
    // TLS certificate material: always re-read on an explicit reload (the periodic
    // poll may be off); the paths/knobs themselves are startup-only
    if (server_ && !cfg_.http.tls_cert.empty()) {
        // same files, its own holder
        if (admin_server_) admin_server_->reload_tls();
        if (server_->reload_tls())
            report.applied.push_back("http.tls: certificate material re-read");
        else if (cfg_.http.driver == "seastar")
            report.applied.push_back("http.tls: seastar reloads certificates on file change");
    }
    report.requires_restart = restart_only_changes(cfg_, fresh);
    for (auto& r : plan.requires_restart) report.requires_restart.push_back(r);
    report.ok = true;
    if (report.applied.empty() && report.requires_restart.empty()) LOG_INFO("config reload: no changes");
    for (auto& a : report.applied) LOG_INFO("config reload: applied {}", a);
    for (auto& r : report.requires_restart)
        LOG_WARN("config reload: {} changed on disk but needs a restart to take effect", r);
    return report;
}

void Application::retire_backend(std::string name, std::shared_ptr<storage::IStorageBackend> raw,
                                 std::shared_ptr<storage::MeteredBackend> metered) {
    std::lock_guard lk(retire_mu_);
    retiring_.emplace_back([this, name, raw, metered] {
        // Requests that resolved the old table still hold the decorator (and a
        // get_object stream its lease): wait for them in bounded slices so a
        // shutdown can cut the wait short; log while it takes long
        if (metered) {
            int waited = 0;
            while (!retire_stop_.load(std::memory_order_relaxed) && !metered->wait_idle(std::chrono::seconds(1))) {
                if (++waited % 10 == 0)
                    LOG_INFO("backend {} removed: still waiting for {} in-flight request(s)", name,
                             metered->inflight());
            }
            if (long left = metered->inflight(); left > 0)
                LOG_WARN("backend {} removed: closing with {} request(s) still in flight (shutdown)", name, left);
        }
        std::string failure;
        try {
            sync_wait(raw->close());
        } catch (const std::exception& e) {
            failure = e.what();
        }
        // Series first, log line last: the line is what operators (and the e2e)
        // wait on, so everything observable must be done by then
        if (metrics_) metrics_->remove_labeled("backend", name);
        if (failure.empty())
            LOG_INFO("backend {} removed: closed after in-flight requests drained", name);
        else
            LOG_ERROR("backend {} removed: close failed: {}", name, failure);
    });
}

void Application::join_retiring() {
    std::vector<std::thread> threads;
    {
        std::lock_guard lk(retire_mu_);
        threads.swap(retiring_);
    }
    for (auto& t : threads)
        if (t.joinable()) t.join();
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
            // surfaces as a non-zero exit code (roadmap §4.5)
            ++shutdown_errors_;
        }
    }
}

void Application::shutdown() noexcept {
    // Retiring backends first: their threads close instances through the pool,
    // which must still be alive; a lingering stream no longer holds them up
    retire_stop_.store(true, std::memory_order_relaxed);
    try {
        join_retiring();
    } catch (const std::exception& e) {
        LOG_ERROR("retiring backend join failed: {}", e.what());
        ++shutdown_errors_;
    }
    try {
        // Timers / in-flight sync must wind down before the thread pool
        if (cred_store_) cred_store_->shutdown_background();
        if (website_store_) website_store_->shutdown_background();
        if (cors_store_) cors_store_->shutdown_background();
        if (lifecycle_runner_) lifecycle_runner_->shutdown_background();
        if (lifecycle_store_) lifecycle_store_->shutdown_background();
        // final counter flush happens here
        if (usage_) usage_->shutdown_background();
        if (quota_store_) quota_store_->shutdown_background();
        if (tenant_store_) tenant_store_->shutdown_background();
        if (owner_store_) owner_store_->shutdown_background();
    } catch (const std::exception& e) {
        LOG_ERROR("store background shutdown failed: {}", e.what());
        ++shutdown_errors_;
    }
    close_backends();
    // A scrub in flight aborts once its backend is closed; join its thread before
    // the backends themselves go away
    if (admin_jobs_) admin_jobs_->shutdown();
    admin_jobs_.reset();
    // The backends' shared_ptrs are still held by service (via router)
    // and handler (via server), so clearing backends_ alone triggers
    // no destruction. Release in reverse ownership order so backend
    // destruction happens **before** pool->join() — destructors still use
    // the pool (docs/archive/gaps.md §3.9)
    admin_server_.reset();
    server_.reset();
    service_.reset();
    // holds a raw pointer into pool_exec_; must go first
    inflight_.reset();
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
    metered_.clear();
    backends_.clear();
    if (pool_) {
        try {
            pool_->join();
        } catch (const std::exception& e) {
            LOG_ERROR("thread pool join failed: {}", e.what());
            ++shutdown_errors_;
        }
        pool_.reset();
    }
    metrics_.reset();
    // Last: everything above may still log. Drains the async queue (log.async) so
    // the final lines reach the sink before the process exits; logging afterwards
    // continues synchronously on the same sink
    Logger::shutdown();
}

}  // namespace lights3
