// Process assembly and lifecycle (docs/architecture.md §4). Application owns
// every process-wide component in dependency order, replacing the hand-wired
// startup/shutdown sequence that used to live in main()
#pragma once

#include <map>
#include <memory>
#include <string>

#include "core/cancel.h"
#include "core/config.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "http/server.h"
#include "s3/auth/credential_store.h"
#include "s3/service.h"
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
    const std::map<std::string, std::shared_ptr<storage::IStorageBackend>>& backends() const {
        return backends_;
    }

private:
    // Each backend needs an individual close() on shutdown: the router only
    // routes by bucket and cannot produce the full set
    void close_backends() noexcept;

    // Declaration order = construction order; shutdown() releases in reverse
    Config cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::shared_ptr<MetricsRegistry> metrics_;
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends_;
    std::shared_ptr<s3::CredentialStore> cred_store_;
    std::shared_ptr<s3::S3Service> service_;
    std::shared_ptr<ThreadPoolExecutor> pool_exec_;
    std::shared_ptr<AsyncSemaphore> inflight_;
    std::shared_ptr<CancelSource> shutdown_src_;
    std::unique_ptr<http::IHttpServer> server_;
};

}  // namespace lights3
