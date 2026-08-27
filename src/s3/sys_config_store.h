// Generic per-bucket configuration store persisted under .sys/<prefix><bucket>
// (roadmap §2.1/§2.4). Third instantiation of the write-through + tombstone-sync
// pattern established by CredentialStore and WebsiteStore — extracted as a template
// so CORS and Lifecycle do not become the third and fourth hand-copies. WebsiteStore
// predates this template and additionally merges static YAML entries, so it stays
// as-is; stores instantiated from here are dynamic-only (API-managed).
//
// Traits contract:
//   struct T {
//     using Entry = ...;                                  // per-bucket configuration value
//     static constexpr std::string_view kPrefix;          // e.g. "cors/" (object key = prefix + bucket)
//     static constexpr const char* kName;                 // log tag, e.g. "cors"
//     static std::string serialize(const Entry&);         // JSON body
//     static std::optional<Entry> deserialize(const std::string& bucket,
//                                             const std::string& body);  // nullopt = malformed (skip + WARN)
//     static bool differs(const Entry&, const Entry&);    // sync change detection
//   };
#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "core/background.h"
#include "core/log.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "http/model.h"
#include "s3/errors.h"
#include "storage/backend.h"

namespace lights3::s3 {

// .sys bucket name (single source: credential_store.h re-exports the same constant);
// duplicated string literal avoided by using the storage-layer constant directly
inline constexpr std::string_view kSysConfigBucket = storage::kSysBucketName;

template <class Traits>
class SysConfigStore {
public:
    using Entry = typename Traits::Entry;
    // Immutable snapshot: dispatch holds one for the whole request, pointers into it
    // stay valid across co_awaits even if a concurrent PUT/DELETE swaps the current one
    using Snapshot = std::shared_ptr<const std::map<std::string, Entry>>;

    // Startup: load every .sys/<prefix>* object. A missing .sys counts as empty; a
    // malformed object is skipped with a WARN — broken per-bucket config must not block
    // startup (worst case one bucket loses the feature, nothing is locked out)
    static Task<std::shared_ptr<SysConfigStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend) {
        auto store = std::shared_ptr<SysConfigStore>(new SysConfigStore());
        store->backend_ = std::move(backend);
        if (store->backend_ && co_await store->backend_->bucket_exists(kSysConfigBucket)) {
            storage::ListOptions opt;
            opt.prefix = std::string(Traits::kPrefix);
            for (;;) {
                auto page = co_await store->backend_->list_objects(kSysConfigBucket, opt);
                for (auto& obj : page.objects) {
                    std::string bucket = obj.key.substr(Traits::kPrefix.size());
                    auto stream = co_await store->backend_->get_object(kSysConfigBucket,
                                                                       obj.key, std::nullopt);
                    auto body = co_await read_all(*stream.body);
                    if (auto e = Traits::deserialize(bucket, body))
                        store->entries_[bucket] = std::move(*e);
                    else
                        LOG_WARN("{}: skipping malformed object {}/{}", Traits::kName,
                                 kSysConfigBucket, obj.key);
                }
                if (!page.is_truncated) break;
                opt.start_after = page.next_token;
            }
        }
        store->rebuild_snapshot_locked();
        if (!store->entries_.empty())
            LOG_INFO("{}: loaded {} entries from {}/{}", Traits::kName,
                     store->entries_.size(), kSysConfigBucket, Traits::kPrefix);
        co_return store;
    }

    // Test/static assemblies without a backend: everything in memory, put/remove throw
    static std::shared_ptr<SysConfigStore> make_detached() {
        auto store = std::shared_ptr<SysConfigStore>(new SysConfigStore());
        store->rebuild_snapshot_locked();
        return store;
    }

    ~SysConfigStore() { shutdown_background(); }

    Snapshot snapshot() const {
        std::shared_lock lk(mu_);
        return snap_;
    }
    // nullptr when the bucket has no entry (pointer into snap)
    static const Entry* find(const Snapshot& snap, const std::string& bucket) {
        if (!snap) return nullptr;
        auto it = snap->find(bucket);
        return it != snap->end() ? &it->second : nullptr;
    }

    // Write-through (storage first, then memory — on crash storage is authoritative)
    Task<void> put(std::string bucket, Entry entry) {
        require_backend();
        co_await ensure_sys_bucket();
        storage::ObjectMeta meta;
        meta.content_type = "application/json";
        http::StringBodyReader body(Traits::serialize(entry));
        co_await backend_->put_object(kSysConfigBucket, object_key(bucket), std::move(meta),
                                      body);
        {
            std::unique_lock lk(mu_);
            tombstones_.erase(bucket);
            entries_[bucket] = std::move(entry);
            rebuild_snapshot_locked();
        }
    }

    // Removing a missing entry is a no-op (DELETE on the subresource is idempotent)
    Task<void> remove(const std::string& bucket) {
        require_backend();
        // Tombstone before the delete (same interleaving as credential/website remove):
        // a sync list started before the delete could otherwise pull the entry back
        {
            std::unique_lock lk(mu_);
            tombstones_[bucket] = std::chrono::steady_clock::now();
        }
        try {
            co_await backend_->delete_object(kSysConfigBucket, object_key(bucket));
        } catch (const S3Error& e) {
            if (e.code != S3ErrorCode::NoSuchKey && e.code != S3ErrorCode::NoSuchBucket) {
                std::unique_lock lk(mu_);
                tombstones_.erase(bucket);
                throw;
            }
        }
        {
            std::unique_lock lk(mu_);
            if (entries_.erase(bucket)) rebuild_snapshot_locked();
        }
    }

    // Periodic .sys re-list: entries added/changed elsewhere are picked up, entries gone
    // from storage are dropped. No empty-table guard: an emptied table only disables the
    // feature per bucket, it cannot lock anyone out (unlike credentials)
    Task<void> sync_now() {
        if (!backend_) co_return;
        std::vector<std::string> known;
        {
            std::shared_lock lk(mu_);
            for (auto& [b, _] : entries_) known.push_back(b);
        }
        if (!co_await backend_->bucket_exists(kSysConfigBucket)) co_return;

        std::map<std::string, Entry> on_storage;
        storage::ListOptions opt;
        opt.prefix = std::string(Traits::kPrefix);
        for (;;) {
            auto page = co_await backend_->list_objects(kSysConfigBucket, opt);
            for (auto& obj : page.objects) {
                std::string bucket = obj.key.substr(Traits::kPrefix.size());
                try {
                    auto stream = co_await backend_->get_object(kSysConfigBucket, obj.key,
                                                                std::nullopt);
                    auto body = co_await read_all(*stream.body);
                    if (auto e = Traits::deserialize(bucket, body))
                        on_storage.emplace(bucket, std::move(*e));
                    else
                        LOG_WARN("{} sync: skipping malformed object {}/{}", Traits::kName,
                                 kSysConfigBucket, obj.key);
                } catch (const std::exception& e) {
                    // A single failed fetch does not abort the sync; the entry keeps its old value
                    LOG_WARN("{} sync: failed to load {}: {}", Traits::kName, obj.key,
                             e.what());
                }
            }
            if (!page.is_truncated) break;
            opt.start_after = page.next_token;
        }

        size_t added = 0, removed = 0;
        {
            std::unique_lock lk(mu_);
            auto now = std::chrono::steady_clock::now();
            std::erase_if(tombstones_,
                          [&](auto& kv) { return now - kv.second > kTombstoneTtl; });
            for (auto& [b, e] : on_storage) {
                if (tombstones_.contains(b)) continue;  // just removed locally, don't resurrect
                auto it = entries_.find(b);
                if (it == entries_.end()) {
                    entries_.emplace(b, std::move(e));
                    ++added;
                } else if (Traits::differs(it->second, e)) {
                    it->second = std::move(e);
                    ++added;
                }
            }
            for (auto& b : known)
                if (!on_storage.contains(b) && entries_.erase(b)) ++removed;
            if (added || removed) rebuild_snapshot_locked();
        }
        if (added || removed)
            LOG_INFO("{} sync: {} added/updated, {} removed", Traits::kName, added, removed);
    }

    void start_background(std::shared_ptr<ThreadPool> pool, int sync_interval_sec) {
        pool_ = std::move(pool);
        sync_interval_sec_ = sync_interval_sec;
        schedule_sync();
    }
    void shutdown_background() {
        bg_.begin_close();
        // cancel outside the group lock (TimerQueue::cancel blocks on in-flight callbacks)
        TimerQueue::instance().cancel(sync_timer_);
        bg_.wait_idle();
    }

private:
    SysConfigStore() = default;

    static constexpr auto kTombstoneTtl = std::chrono::minutes(5);

    static std::string object_key(const std::string& bucket) {
        return std::string(Traits::kPrefix) + bucket;
    }

    void require_backend() const {
        if (!backend_)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          std::string("Dynamic ") + Traits::kName +
                              " configuration is not available on this deployment.");
    }

    static Task<std::string> read_all(http::BodyReader& body, size_t max_size = 256 * 1024) {
        std::string out;
        std::byte buf[16 * 1024];
        for (;;) {
            size_t n = co_await body.read(std::span(buf));
            if (n == 0) break;
            if (out.size() + n > max_size)
                throw std::runtime_error(std::string(Traits::kName) + " object too large");
            out.append(reinterpret_cast<const char*>(buf), n);
        }
        co_return out;
    }

    void rebuild_snapshot_locked() {  // caller holds mu_ exclusively (or is single-threaded)
        snap_ = std::make_shared<const std::map<std::string, Entry>>(entries_);
    }

    Task<void> ensure_sys_bucket() {
        if (sys_bucket_ready_.load()) co_return;
        try {
            co_await backend_->create_bucket(kSysConfigBucket);
        } catch (const S3Error& e) {
            if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
        }
        sys_bucket_ready_ = true;
    }

    Task<void> sync_tick() {
        co_await pool_->schedule();  // the timer thread only dispatches; IO moves to a pool thread
        std::exception_ptr err;
        try {
            co_await sync_now();
        } catch (...) {
            err = std::current_exception();
        }
        schedule_sync();
        if (err) std::rethrow_exception(err);  // hand off to BackgroundTaskGroup for logging
    }

    void schedule_sync() {
        if (sync_interval_sec_ <= 0 || !backend_) return;
        bg_.if_open([&] {
            sync_timer_ = TimerQueue::instance().add(std::chrono::seconds(sync_interval_sec_),
                                                     [this] { bg_.spawn(sync_tick()); });
        });
    }

    std::shared_ptr<storage::IStorageBackend> backend_;  // null = detached (tests)
    std::shared_ptr<ThreadPool> pool_;
    int sync_interval_sec_ = 0;

    mutable std::shared_mutex mu_;
    std::map<std::string, Entry> entries_;
    Snapshot snap_;
    std::map<std::string, std::chrono::steady_clock::time_point> tombstones_;
    std::atomic<bool> sys_bucket_ready_{false};

    TimerQueue::Id sync_timer_ = 0;
    BackgroundTaskGroup bg_{Traits::kName};
};

}  // namespace lights3::s3
