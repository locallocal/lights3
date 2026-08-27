#include "s3/website_store.h"

#include <algorithm>
#include <span>

#include <nlohmann/json.hpp>

#include "core/log.h"
#include "s3/auth/credential_store.h"  // kSysBucket
#include "s3/errors.h"
#include "http/model.h"

namespace lights3::s3 {

namespace {

using nlohmann::json;

constexpr std::string_view kWebsitePrefix = "website/";
// remove()d entries stay tombstoned this long so a concurrent sync list cannot resurrect them
constexpr auto kTombstoneTtl = std::chrono::minutes(5);

std::string object_key(const std::string& bucket) {
    return std::string(kWebsitePrefix) + bucket;
}

std::string serialize(const WebsiteBucket& w) {
    json j;
    j["index_suffix"] = w.index_suffix;
    if (!w.error_key.empty()) j["error_key"] = w.error_key;
    if (!w.redirect_all_host.empty()) {
        j["redirect_all_host"] = w.redirect_all_host;
        if (!w.redirect_all_protocol.empty())
            j["redirect_all_protocol"] = w.redirect_all_protocol;
    }
    if (!w.routing_rules.empty()) {
        json rules = json::array();
        for (auto& r : w.routing_rules) {
            json jr;
            if (!r.key_prefix_equals.empty()) jr["key_prefix_equals"] = r.key_prefix_equals;
            if (r.http_error_code_equals) jr["http_error_code_equals"] = r.http_error_code_equals;
            if (!r.protocol.empty()) jr["protocol"] = r.protocol;
            if (!r.host_name.empty()) jr["host_name"] = r.host_name;
            if (r.replace_key_prefix_with)
                jr["replace_key_prefix_with"] = *r.replace_key_prefix_with;
            if (r.replace_key_with) jr["replace_key_with"] = *r.replace_key_with;
            if (r.http_redirect_code != 301) jr["http_redirect_code"] = r.http_redirect_code;
            rules.push_back(std::move(jr));
        }
        j["routing_rules"] = std::move(rules);
    }
    if (w.max_rps) j["max_rps"] = w.max_rps;
    return j.dump();
}

// nullopt on malformed content; the same shape rules as the YAML/XML sides apply here —
// an object hand-edited into an invalid suffix must not become live config
std::optional<WebsiteBucket> deserialize(const std::string& bucket, const std::string& body) {
    try {
        auto j = json::parse(body);
        WebsiteBucket w;
        w.bucket = bucket;
        w.index_suffix = j.value("index_suffix", w.index_suffix);
        w.error_key = j.value("error_key", "");
        if (w.index_suffix.empty() || w.index_suffix.find('/') != std::string::npos)
            return std::nullopt;
        if (!w.error_key.empty() && w.error_key.front() == '/') return std::nullopt;
        w.redirect_all_host = j.value("redirect_all_host", "");
        w.redirect_all_protocol = j.value("redirect_all_protocol", "");
        if (!w.redirect_all_protocol.empty() && w.redirect_all_protocol != "http" &&
            w.redirect_all_protocol != "https")
            return std::nullopt;
        if (j.contains("routing_rules")) {
            for (auto& jr : j["routing_rules"]) {
                WebsiteRoutingRule r;
                r.key_prefix_equals = jr.value("key_prefix_equals", "");
                r.http_error_code_equals = jr.value("http_error_code_equals", 0);
                r.protocol = jr.value("protocol", "");
                r.host_name = jr.value("host_name", "");
                if (jr.contains("replace_key_prefix_with"))
                    r.replace_key_prefix_with = jr["replace_key_prefix_with"].get<std::string>();
                if (jr.contains("replace_key_with"))
                    r.replace_key_with = jr["replace_key_with"].get<std::string>();
                r.http_redirect_code = jr.value("http_redirect_code", 301);
                if (r.replace_key_prefix_with && r.replace_key_with) return std::nullopt;
                if (r.http_redirect_code < 300 || r.http_redirect_code > 399)
                    return std::nullopt;
                w.routing_rules.push_back(std::move(r));
            }
        }
        w.max_rps = j.value("max_rps", 0u);
        return w;
    } catch (...) {
        return std::nullopt;
    }
}

Task<std::string> read_all(http::BodyReader& body, size_t max_size = 64 * 1024) {
    std::string out;
    std::byte buf[16 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        if (out.size() + n > max_size) throw std::runtime_error("website object too large");
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return out;
}

}  // namespace

std::shared_ptr<WebsiteStore> WebsiteStore::make_static(std::vector<WebsiteBucket> entries) {
    auto store = std::shared_ptr<WebsiteStore>(new WebsiteStore());
    store->static_entries_ = std::move(entries);
    store->rebuild_snapshot_locked();  // single-threaded here, no lock needed yet
    return store;
}

Task<std::shared_ptr<WebsiteStore>> WebsiteStore::load(
    std::shared_ptr<storage::IStorageBackend> backend,
    std::vector<WebsiteBucket> static_entries) {
    auto store = std::shared_ptr<WebsiteStore>(new WebsiteStore());
    store->backend_ = std::move(backend);
    store->static_entries_ = std::move(static_entries);

    if (co_await store->backend_->bucket_exists(kSysBucket)) {
        store->sys_bucket_ready_ = true;
        storage::ListOptions opt;
        opt.prefix = std::string(kWebsitePrefix);
        for (;;) {
            auto page = co_await store->backend_->list_objects(kSysBucket, opt);
            for (auto& obj : page.objects) {
                std::string bucket = obj.key.substr(kWebsitePrefix.size());
                auto stream =
                    co_await store->backend_->get_object(kSysBucket, obj.key, std::nullopt);
                auto body = co_await read_all(*stream.body);
                if (auto w = deserialize(bucket, body))
                    store->dynamic_[bucket] = std::move(*w);
                else
                    LOG_WARN("skipping malformed website object {}/{}", kSysBucket, obj.key);
            }
            if (!page.is_truncated) break;
            opt.start_after = page.next_token;
        }
    }
    store->rebuild_snapshot_locked();
    if (!store->dynamic_.empty())
        LOG_INFO("website: loaded {} dynamic entries from {}/{}", store->dynamic_.size(),
                 kSysBucket, kWebsitePrefix);
    co_return store;
}

// Static wins over a same-name dynamic entry (same precedence as credentials): the
// config file is the operator's explicit intent and survives restarts unconditionally
void WebsiteStore::rebuild_snapshot_locked() {
    auto v = std::make_shared<std::vector<WebsiteBucket>>(static_entries_);
    for (auto& [b, w] : dynamic_) {
        bool shadowed = false;
        for (auto& s : static_entries_)
            if (s.bucket == b) {
                LOG_WARN("website: bucket {} is configured statically; ignoring the dynamic entry",
                         b);
                shadowed = true;
                break;
            }
        if (!shadowed) v->push_back(w);
    }
    std::sort(v->begin(), v->end(),
              [](const WebsiteBucket& a, const WebsiteBucket& b) { return a.bucket < b.bucket; });
    snap_ = std::move(v);
}

WebsiteStore::Snapshot WebsiteStore::snapshot() const {
    std::shared_lock lk(mu_);
    return snap_;
}

const WebsiteBucket* WebsiteStore::find(const Snapshot& snap, const std::string& bucket) {
    if (!snap) return nullptr;
    auto it = std::lower_bound(
        snap->begin(), snap->end(), bucket,
        [](const WebsiteBucket& w, const std::string& b) { return w.bucket < b; });
    return it != snap->end() && it->bucket == bucket ? &*it : nullptr;
}

bool WebsiteStore::is_static_entry(const std::string& bucket) const {
    std::shared_lock lk(mu_);
    for (auto& s : static_entries_)
        if (s.bucket == bucket) return true;
    return false;
}

Task<void> WebsiteStore::ensure_sys_bucket() {
    if (sys_bucket_ready_) co_return;
    try {
        co_await backend_->create_bucket(kSysBucket);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
    }
    sys_bucket_ready_ = true;
}

Task<void> WebsiteStore::put(WebsiteBucket entry) {
    if (is_static_entry(entry.bucket))
        throw S3Error(S3ErrorCode::MethodNotAllowed,
                      "The website configuration for this bucket is managed via the server "
                      "config file.");
    if (!backend_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic website configuration is not available on this deployment.");
    co_await ensure_sys_bucket();
    // Persist first, then take effect (write-through, same as credential generate)
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader body(serialize(entry));
    co_await backend_->put_object(kSysBucket, object_key(entry.bucket), std::move(meta), body);
    {
        std::unique_lock lk(mu_);
        tombstones_.erase(entry.bucket);
        dynamic_[entry.bucket] = std::move(entry);
        rebuild_snapshot_locked();
    }
}

Task<void> WebsiteStore::remove(const std::string& bucket) {
    if (is_static_entry(bucket))
        throw S3Error(S3ErrorCode::MethodNotAllowed,
                      "The website configuration for this bucket is managed via the server "
                      "config file.");
    if (!backend_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic website configuration is not available on this deployment.");
    // Tombstone before the delete (same interleaving as credential remove): a sync list
    // started before the delete could otherwise pull the entry straight back
    {
        std::unique_lock lk(mu_);
        tombstones_[bucket] = std::chrono::steady_clock::now();
    }
    try {
        co_await backend_->delete_object(kSysBucket, object_key(bucket));
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::NoSuchKey && e.code != S3ErrorCode::NoSuchBucket) {
            std::unique_lock lk(mu_);
            tombstones_.erase(bucket);
            throw;
        }
    }
    {
        std::unique_lock lk(mu_);
        if (dynamic_.erase(bucket)) rebuild_snapshot_locked();
    }
}

Task<void> WebsiteStore::sync_now() {
    if (!backend_) co_return;
    // Memory snapshot before the list (write-through makes "in snapshot + not on
    // storage" mean "removed elsewhere", same reasoning as credential sync)
    std::vector<std::string> known;
    {
        std::shared_lock lk(mu_);
        for (auto& [b, _] : dynamic_) known.push_back(b);
    }
    if (!co_await backend_->bucket_exists(kSysBucket)) co_return;

    std::map<std::string, WebsiteBucket> on_storage;
    storage::ListOptions opt;
    opt.prefix = std::string(kWebsitePrefix);
    for (;;) {
        auto page = co_await backend_->list_objects(kSysBucket, opt);
        for (auto& obj : page.objects) {
            std::string bucket = obj.key.substr(kWebsitePrefix.size());
            try {
                auto stream = co_await backend_->get_object(kSysBucket, obj.key, std::nullopt);
                auto body = co_await read_all(*stream.body);
                if (auto w = deserialize(bucket, body))
                    on_storage.emplace(bucket, std::move(*w));
                else
                    LOG_WARN("website sync: skipping malformed object {}/{}", kSysBucket,
                             obj.key);
            } catch (const std::exception& e) {
                // A single failed fetch does not abort the sync; the entry keeps its old value
                LOG_WARN("website sync: failed to load {}: {}", obj.key, e.what());
            }
        }
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }

    size_t added = 0, removed = 0;
    {
        std::unique_lock lk(mu_);
        auto now = std::chrono::steady_clock::now();
        std::erase_if(tombstones_, [&](auto& kv) { return now - kv.second > kTombstoneTtl; });
        for (auto& [b, w] : on_storage) {
            if (tombstones_.contains(b)) continue;  // just removed locally, don't resurrect
            auto it = dynamic_.find(b);
            if (it == dynamic_.end()) {
                dynamic_.emplace(b, std::move(w));
                ++added;
            } else if (!(it->second == w)) {  // full-entry compare: rules/redirect/rate too
                it->second = std::move(w);
                ++added;
            }
        }
        for (auto& b : known) {
            if (!on_storage.contains(b) && dynamic_.erase(b)) ++removed;
        }
        if (added || removed) rebuild_snapshot_locked();
    }
    if (added || removed)
        LOG_INFO("website sync: {} added/updated, {} removed", added, removed);
}

Task<void> WebsiteStore::sync_tick() {
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

void WebsiteStore::schedule_sync() {
    if (sync_interval_sec_ <= 0 || !backend_) return;
    bg_.if_open([&] {
        sync_timer_ = TimerQueue::instance().add(std::chrono::seconds(sync_interval_sec_),
                                                 [this] { bg_.spawn(sync_tick()); });
    });
}

void WebsiteStore::start_background(std::shared_ptr<ThreadPool> pool, int sync_interval_sec) {
    pool_ = std::move(pool);
    sync_interval_sec_ = sync_interval_sec;
    schedule_sync();
}

void WebsiteStore::shutdown_background() {
    bg_.begin_close();
    // cancel outside the group lock (TimerQueue::cancel blocks on in-flight callbacks)
    TimerQueue::instance().cancel(sync_timer_);
    bg_.wait_idle();
}

}  // namespace lights3::s3
