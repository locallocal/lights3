// Static website configuration store (docs/static-website.md phase ③).
// Static entries come from the YAML config and are immutable via the API (same
// convention as static credentials); dynamic entries are managed through
// PUT/DELETE /bucket?website and persisted as .sys/website/<bucket> JSON
// objects, with optional periodic multi-instance sync (auth.sync_interval,
// the same knob and pattern as CredentialStore).
#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "core/background.h"
#include "core/config.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "storage/backend.h"

namespace lights3::s3 {

class WebsiteStore {
public:
    // Immutable sorted-by-bucket view. dispatch holds one snapshot for the whole
    // request: pointers into it stay valid across co_awaits even if a concurrent
    // PUT/DELETE swaps the current snapshot
    using Snapshot = std::shared_ptr<const std::vector<WebsiteBucket>>;

    // Static-only store, no persistence (tests / assemblies without a backend);
    // put/remove on non-static entries throw
    static std::shared_ptr<WebsiteStore> make_static(std::vector<WebsiteBucket> entries);

    // Startup: load .sys/website/* merged with the static entries (same-bucket:
    // static wins with a WARN). A missing .sys counts as empty; a malformed JSON
    // object is skipped with a WARN — a broken website entry must not block startup
    // (worst case one site serves 403, nothing is locked out, unlike credentials)
    static Task<std::shared_ptr<WebsiteStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend,
        std::vector<WebsiteBucket> static_entries);

    ~WebsiteStore() { shutdown_background(); }

    Snapshot snapshot() const;
    // nullptr when the bucket has no website configuration (pointer into snap)
    static const WebsiteBucket* find(const Snapshot& snap, const std::string& bucket);

    bool is_static_entry(const std::string& bucket) const;

    // Write-through (storage first, then memory — on crash storage is authoritative).
    // A static entry throws MethodNotAllowed (it belongs to the config file); no
    // backend throws InvalidRequest
    Task<void> put(WebsiteBucket entry);
    // Removing a missing entry is a no-op (DELETE ?website is idempotent, 204 either way)
    Task<void> remove(const std::string& bucket);

    // Periodic .sys re-list: entries added/changed elsewhere are picked up, dynamic
    // entries gone from storage are dropped (tombstones bridge the remove/list race,
    // same as CredentialStore). No empty-table guard: an emptied website table only
    // closes anonymous access, it cannot lock anyone out
    Task<void> sync_now();
    void start_background(std::shared_ptr<ThreadPool> pool, int sync_interval_sec);
    void shutdown_background();

private:
    WebsiteStore() = default;
    void rebuild_snapshot_locked();  // caller holds mu_ exclusively
    Task<void> ensure_sys_bucket();
    Task<void> sync_tick();
    void schedule_sync();

    std::shared_ptr<storage::IStorageBackend> backend_;  // null = static-only
    std::shared_ptr<ThreadPool> pool_;
    int sync_interval_sec_ = 0;

    mutable std::shared_mutex mu_;
    std::vector<WebsiteBucket> static_entries_;            // as configured (names unique)
    std::map<std::string, WebsiteBucket> dynamic_;         // bucket -> entry
    Snapshot snap_;                                        // rebuilt after each mutation
    std::map<std::string, std::chrono::steady_clock::time_point> tombstones_;
    std::atomic<bool> sys_bucket_ready_{false};

    TimerQueue::Id sync_timer_ = 0;
    BackgroundTaskGroup bg_{"website-store"};
};

}  // namespace lights3::s3
