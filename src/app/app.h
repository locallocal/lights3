// Process assembly and lifecycle (docs/architecture.md §4). Application owns
// every process-wide component in dependency order, replacing the hand-wired
// startup/shutdown sequence that used to live in main()
#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <memory>
#include <string>

#include "core/cancel.h"
#include "core/config.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "http/server.h"
#include "s3/audit.h"
#include "s3/auth/credential_store.h"
#include "s3/quota.h"
#include "s3/service.h"
#include "s3/tenant.h"
#include "s3/usage.h"
#include "s3/website_store.h"
#include "storage/backend.h"

namespace lights3 {

class Application {
public:
    // Loads the config and initializes logging; no components are built yet
    explicit Application(const std::string& config_path);
    // Safety net: runs shutdown(), so every exit path — including an
    // exception unwinding out of main's try block after backends were built —
    // still flushes and closes them
    ~Application();

    Application(const Application&) = delete;
    Application& operator=(const Application&) = delete;

    // Stage 1: thread pool + metrics + storage backends. Split from
    // start_server() so offline admin modes (--duostore_admin) get fully
    // built backends without ever opening a listening socket
    void open_storage();

    // Stage 2: S3 service assembly, admission control and the HTTP server;
    // ends with listen(). Requires open_storage()
    void start_server();

    // Installs the signal handlers, blocks in the HTTP server until
    // SIGINT/SIGTERM, drains in-flight requests, then runs shutdown().
    // Returns the process exit code
    int run();

    // Full teardown in reverse dependency order. Idempotent, and safe after
    // a partial startup: only what exists is closed
    void shutdown() noexcept;

    const Config& config() const { return cfg_; }

    // Config hot reload (roadmap §4.4, docs/config-reload.md): re-read the file,
    // validate it as at startup, apply the runtime-changeable subset, report the
    // rest. Driven by SIGHUP and POST /-/admin/config/reload. Never partial: a
    // file that fails validation changes nothing
    ConfigReloadReport reload_config();
    const std::map<std::string, std::shared_ptr<storage::IStorageBackend>>& backends() const {
        return backends_;
    }

private:
    // Each backend needs an individual close() on shutdown: the router only
    // routes by bucket and cannot produce the full set
    void close_backends() noexcept;

    // Declaration order = construction order; shutdown() releases in reverse
    std::string config_path_;
    std::mutex reload_mu_;  // one reload at a time (SIGHUP and the admin API may race)
    std::shared_ptr<std::atomic<long>> stall_sec_;  // transfer_stall_timeout, read per request
    Config cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::shared_ptr<MetricsRegistry> metrics_;
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends_;
    std::shared_ptr<s3::CredentialStore> cred_store_;
    std::shared_ptr<s3::WebsiteStore> website_store_;
    std::shared_ptr<s3::CorsStore> cors_store_;
    std::shared_ptr<s3::LifecycleStore> lifecycle_store_;
    std::unique_ptr<s3::LifecycleRunner> lifecycle_runner_;
    // roadmap §3.9: usage accounting, quotas, tenancy, audit (docs/multi-tenancy.md)
    std::shared_ptr<s3::AuditLog> audit_;
    std::shared_ptr<s3::UsageTracker> usage_;
    std::shared_ptr<s3::QuotaStore> quota_store_;
    std::shared_ptr<s3::TenantStore> tenant_store_;
    std::shared_ptr<s3::OwnerStore> owner_store_;
    std::shared_ptr<s3::TenantRegistry> tenants_;
    std::shared_ptr<s3::S3Service> service_;
    std::shared_ptr<ThreadPoolExecutor> pool_exec_;
    std::shared_ptr<AsyncSemaphore> inflight_;
    std::shared_ptr<CancelSource> shutdown_src_;
    std::unique_ptr<http::IHttpServer> server_;
};

}  // namespace lights3
