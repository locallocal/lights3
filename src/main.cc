// Process assembly and startup flow (docs/architecture.md §4)
#include <csignal>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <thread>

#include <gflags/gflags.h>

#include "core/config.h"
#include "core/log.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "http/admission.h"
#include "http/server.h"
#include "s3/auth/credential_store.h"
#include "s3/errors.h"
#include "s3/service.h"
#include "storage/bucket_router.h"
#include "storage/registry.h"
#ifdef LIGHTS3_DUOSTORE
#include <fstream>

#include "storage/duostore/duostore_backend.h"
#endif

namespace {

// self-pipe: the signal handler does only a single write
// (async-signal-safe); the real shutdown is executed by a watchdog thread.
// Calling server->shutdown() directly in the handler is unsafe — the httplib
// driver's implementation takes an internal lock, and a signal landing on a
// thread already holding that lock self-deadlocks (docs/gaps.md §3.9)
int g_sig_pipe[2] = {-1, -1};

void on_signal(int sig) {
    unsigned char b = static_cast<unsigned char>(sig);
    ssize_t n = ::write(g_sig_pipe[1], &b, 1);
    (void)n;  // A full pipe means a signal is already pending; dropping is fine
}

lights3::LogLevel parse_level(const std::string& s) {
    if (s == "debug") return lights3::LogLevel::Debug;
    if (s == "warn") return lights3::LogLevel::Warn;
    if (s == "error") return lights3::LogLevel::Error;
    return lights3::LogLevel::Info;
}

}  // namespace

DEFINE_string(config, "config/lights3.yaml", "Path to the lights3 YAML config file");
DEFINE_string(duostore_admin, "",
              "duostore meta admin (docs/gaps.md §6.1): 'dump:<backend>:<file>' or "
              "'load:<backend>:<file>'. Runs before the server starts (no traffic) and "
              "exits; load ends with a forced orphan scan. Backup order: copy the data "
              "dir first, then dump meta; restore data first, then load.");

#ifdef LIGHTS3_DUOSTORE
namespace {

// --duostore_admin entry point: backends are built, the server has not
// started (write quiescence holds trivially). Returns the process exit code;
// any failure throws loudly and converges through main's fallback path
int run_duostore_admin(
    const std::string& spec,
    const std::map<std::string, std::shared_ptr<lights3::storage::IStorageBackend>>& backends) {
    using namespace lights3;
    auto c1 = spec.find(':');
    auto c2 = c1 == std::string::npos ? std::string::npos : spec.find(':', c1 + 1);
    if (c2 == std::string::npos)
        throw std::runtime_error("--duostore_admin expects dump:<backend>:<file> or "
                                 "load:<backend>:<file>, got: " + spec);
    std::string cmd = spec.substr(0, c1);
    std::string name = spec.substr(c1 + 1, c2 - c1 - 1);
    std::string path = spec.substr(c2 + 1);
    auto it = backends.find(name);
    if (it == backends.end())
        throw std::runtime_error("--duostore_admin: no backend named '" + name + "'");
    auto* duo = dynamic_cast<storage::DuoStoreBackend*>(it->second.get());
    if (!duo)
        throw std::runtime_error("--duostore_admin: backend '" + name + "' is not duostore");
    if (cmd == "dump") {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f) throw std::runtime_error("--duostore_admin: cannot open for write: " + path);
        auto st = sync_wait(duo->run_meta_dump(f));
        LOG_INFO("duostore admin: dumped {} buckets / {} objects / {} sealed packs to {}",
                 st.buckets, st.objects, st.sealed_packs, path);
    } else if (cmd == "load") {
        std::ifstream f(path, std::ios::binary);
        if (!f) throw std::runtime_error("--duostore_admin: cannot open for read: " + path);
        auto st = sync_wait(duo->run_meta_load(f));
        LOG_INFO("duostore admin: loaded {} buckets / {} objects / {} sealed packs from {}",
                 st.buckets, st.objects, st.sealed_packs, path);
    } else {
        throw std::runtime_error("--duostore_admin: unknown command '" + cmd + "'");
    }
    return 0;
}

}  // namespace
#endif  // LIGHTS3_DUOSTORE

int main(int argc, char** argv) {
    using namespace lights3;

    gflags::SetUsageMessage("S3-compatible object storage server.\nusage: lights3 [--config <path>]");
    gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

    // Each backend needs an individual close() on shutdown: the router only
    // routes by bucket and cannot produce the full set. Declared outside the
    // try so the exception path of a late startup failure also takes the same
    // flush logic
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> all_backends;
    auto close_backends = [&all_backends] {
        // close() must come **before** pool->join() — it internally
        // co_awaits pool->schedule() (a joined pool throws post-after-join),
        // and destructor fallback is not equivalent to close (duostore skips
        // sealing the active pack and the rados flush; tiered drops the atime
        // snapshot). Close one by one so a single failure does not block the
        // rest; calling repeatedly is harmless (close is idempotent)
        for (auto& [name, backend] : all_backends) {
            try {
                sync_wait(backend->close());
            } catch (const std::exception& e) {
                LOG_ERROR("backend {} close failed: {}", name, e.what());
            }
        }
    };

    try {
        auto cfg = Config::load(FLAGS_config);
        Logger::init(parse_level(cfg.log_level));

        auto pool = std::make_shared<ThreadPool>(cfg.runtime.io_threads);
        // Backend-level metrics registry: build hands each backend a scope
        // labeled backend=<name>, rendered appended to /-/metrics
        auto metrics = std::make_shared<MetricsRegistry>();
        auto backends = storage::StorageRegistry::build(cfg.backends, pool, metrics);
        all_backends = backends;
        if (!FLAGS_duostore_admin.empty()) {
#ifdef LIGHTS3_DUOSTORE
            int rc = run_duostore_admin(FLAGS_duostore_admin, all_backends);
            // Backend destruction must precede pool->join() (destructors still use the pool; see the normal-shutdown comment below)
            close_backends();
            backends.clear();
            all_backends.clear();
            pool->join();
            return rc;
#else
            throw std::runtime_error("--duostore_admin requires a build with LIGHTS3_DUOSTORE");
#endif
        }
        auto router = storage::BucketRouter::build(cfg.buckets, std::move(backends));
        auto auth = s3::SigV4Authenticator::build(cfg.auth);
        // Dynamic credentials (docs/credential-management.md): loaded from the default backend, replacing the static lookup table
        auto cred_store =
            sync_wait(s3::CredentialStore::load(router.default_backend(), cfg.auth));
        auth.set_provider(cred_store);
        if (!auth.enabled())
            LOG_WARN("no credentials configured: authentication is DISABLED");
        auto service = std::make_shared<s3::S3Service>(std::move(router), std::move(auth),
                                                       cfg.http.base_domain);
        service->set_pool_stats([pool] { return pool->stats(); });
        service->set_request_timeout(std::chrono::seconds(cfg.http.request_timeout_sec));
        service->set_min_part_size(cfg.http.min_part_size);
        service->set_backend_metrics(metrics);
        service->set_credential_store(cred_store);
        // Phase-2 background tasks (docs/credential-management.md
        // §10.2/§10.3): credentials_file hot-reload polling + periodic
        // multi-instance incremental sync (both gated by config)
        cred_store->start_background(pool);

        auto server = http::HttpServerFactory::create(cfg.http.driver, cfg.http);
        // Dispatch-entry admission control (docs/concurrency.md §6):
        // over-limit requests queue on the semaphore instead of being
        // rejected; waiters are woken via the pool executor, avoiding running
        // an entire request coroutine chain inline on the releasing call stack
        auto pool_exec = std::make_shared<ThreadPoolExecutor>(*pool);
        auto inflight = std::make_shared<AsyncSemaphore>(cfg.runtime.max_inflight_requests,
                                                         pool_exec.get());
        // Observability for the admission gate and the timer thread
        // (docs/gaps.md §7): under load testing, "stuck at admission" vs
        // "stuck in the pool" and "how long the timer was blocked by a slow
        // callback" can all be read straight from /-/metrics
        service->set_admission_stats(
            [inflight, cap = cfg.runtime.max_inflight_requests]() -> s3::AdmissionStats {
                return {cap, inflight->available(), inflight->waiting()};
            });
        service->set_timer_stats([] { return TimerQueue::instance().stats(); });
        // Process shutdown broadcast (the third cancel source of
        // docs/concurrency.md §5): fired after run() returns, so in-flight
        // requests converge from their nearest cancellable suspension point
        // instead of waiting out their individual request_timeouts
        auto shutdown_src = std::make_shared<CancelSource>();
        const int max_inflight = cfg.runtime.max_inflight_requests;
        auto stall = std::chrono::seconds(cfg.http.transfer_stall_timeout_sec);
        // Assembly of queueing / Permit lifetime / cancellation convergence lives in http/admission.h (shared with the unit tests)
        server->set_handler(http::make_admission_handler(
            inflight, stall, shutdown_src,
            [service](http::HttpRequest req) { return service->dispatch(std::move(req)); }));
        server->listen(cfg.http.bind, cfg.http.port);

        // Signal -> self-pipe -> watchdog-thread shutdown (see the comment on on_signal)
        if (::pipe2(g_sig_pipe, O_CLOEXEC) != 0)
            throw std::runtime_error("cannot create signal pipe");
        std::thread sig_thread([&server] {
            unsigned char b = 0;
            while (::read(g_sig_pipe[0], &b, 1) == 1) {
                LOG_INFO("signal {} received, shutting down", int(b));
                server->shutdown();
            }
        });
        struct sigaction sa{};
        sa.sa_handler = on_signal;
        sigemptyset(&sa.sa_mask);  // An uninitialized mask is an undefined blocking set
        sa.sa_flags = SA_RESTART;  // With the self-pipe scheme, no need to interrupt syscalls via EINTR
        sigaction(SIGINT, &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
        signal(SIGPIPE, SIG_IGN);

        LOG_INFO("lights3 started: driver={} backends={} pool={}", cfg.http.driver,
                 cfg.backends.size(), cfg.runtime.io_threads);
        server->run();  // Blocks until SIGINT/SIGTERM

        // Reap the shutdown watchdog thread first: only then is destroying server safe
        ::close(g_sig_pipe[1]);
        g_sig_pipe[1] = -1;
        if (sig_thread.joinable()) sig_thread.join();
        ::close(g_sig_pipe[0]);
        g_sig_pipe[0] = -1;

        // run() returning does **not** mean in-flight requests have reached
        // zero (drivers return unconditionally after grace + force close,
        // docs/gaps.md §2.1). Broadcast cancellation first so in-flight
        // requests converge from their suspension points, then wait for
        // permits to return; otherwise the close() below would touch the same
        // backend concurrently with still-running requests
        shutdown_src->request_cancel();
        inflight->close();
        {
            // The driver-side connection grace is a separate quantity
            // (drivers/common.h kShutdownGrace); what we wait for here is
            // permits returning. Write the literal in one place only: copying
            // "10s" into the log message would drift out of sync eventually
            constexpr auto kDrainDeadline = std::chrono::seconds(10);
            auto deadline = std::chrono::steady_clock::now() + kDrainDeadline;
            while (inflight->available() < max_inflight &&
                   std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            if (inflight->available() < max_inflight)
                LOG_ERROR("{} request(s) still in flight after {}s; proceeding with shutdown",
                          max_inflight - inflight->available(), kDrainDeadline.count());
        }

        cred_store->shutdown_background();  // Timers / in-flight sync must wind down before the thread pool
        close_backends();
        // The backends' shared_ptrs are still held by service (via router)
        // and handler (via server), so clearing all_backends alone triggers
        // no destruction. Release in reverse ownership order so backend
        // destruction happens **before** pool->join() — destructors still use
        // the pool (docs/gaps.md §3.9)
        server.reset();
        service.reset();
        cred_store.reset();
        all_backends.clear();
        pool->join();
        LOG_INFO("lights3 exited cleanly");
        return 0;
    } catch (const std::exception& e) {
        // Late startup failures (listen conflict, assembly throwing) used to
        // return 1 directly, skipping every close(): duostore's active pack
        // went unsealed and rados unflushed, forcing crash recovery on the
        // next startup
        close_backends();
        fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
