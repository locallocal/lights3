// L3 cross-cutting: per-backend object metadata cache (roadmap §3.8).
// Every HEAD/GET used to pay one metadata round trip -- stat+getxattr (+sidecar) on
// localfs, one meta-engine RTT on duostore -- even for objects served thousands of times
// per second. This is a sharded LRU keyed by (bucket, key) that a backend consults before
// its authoritative meta read and fills afterwards; the backend's own write paths
// invalidate (put / delete / complete / copy / tag rewrite / tier commits).
//
// Consistency model:
// - Exact within one process: every mutation goes through the owning backend, which
//   invalidates *after* its commit point (also on an undetermined commit). A fill that
//   raced with an invalidation is dropped by the generation token: the token is taken on
//   the miss, and insert refuses when the shard's invalidation generation moved since.
//   Without it a reader could read the old record, get preempted across the writer's
//   commit + invalidate, and then insert the stale record for good.
// - Bounded when the source of truth is shared (duostore redis/tikv, or a localfs root
//   modified by another process): a TTL caps how long a peer's write stays invisible.
//   The caller decides the bound (duostore requires a TTL on shared engines; localfs can
//   additionally validate a hit against a fresh stat(2) stamp, see localfs_backend.h).
// Cross-gateway invalidation messaging is a later phase (roadmap §3.8 "可分期").
//
// The value type is opaque here (localfs caches ObjectMeta + a stat stamp, duostore the
// full ObjectRec incl. manifest); values are shared_ptr<const V> so a hit hands out an
// immutable snapshot without copying under the lock.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "core/metrics.h"

namespace lights3::storage {

struct MetaCacheOptions {
    size_t max_entries = 0;               // total budget across shards; 0 = disabled
    std::chrono::milliseconds ttl{0};     // 0 = never expires (invalidation only)
    size_t shards = 64;                   // lock striping (the fill token is per shard)
};

struct MetaCacheStats {
    uint64_t hits = 0, misses = 0, invalidations = 0;
    uint64_t stale = 0;          // present but rejected by the caller's validation (counted as a miss too)
    uint64_t fills_dropped = 0;  // inserts refused by the generation token
    size_t entries = 0;
};

template <class V>
class MetaCache {
public:
    using Ptr = std::shared_ptr<const V>;

    // Fill token (see header comment): obtained from a miss, consumed by insert
    struct Token {
        uint32_t shard = 0;
        uint64_t gen = 0;
    };

    explicit MetaCache(MetaCacheOptions opt, const MetricsScope& metrics = {})
        : opt_(opt) {
        if (opt_.shards == 0) opt_.shards = 1;
        if (opt_.max_entries > 0 && opt_.max_entries < opt_.shards) opt_.shards = opt_.max_entries;
        nshards_ = opt_.shards;
        shards_ = std::make_unique<Shard[]>(nshards_);
        per_shard_ = opt_.max_entries / opt_.shards;
        if (opt_.max_entries > 0 && per_shard_ == 0) per_shard_ = 1;
        m_hits_ = metrics.counter("lights3_meta_cache_lookups_total",
                                  "Object metadata cache lookups", {{"result", "hit"}});
        m_misses_ = metrics.counter("lights3_meta_cache_lookups_total",
                                    "Object metadata cache lookups", {{"result", "miss"}});
        m_stale_ = metrics.counter("lights3_meta_cache_lookups_total",
                                   "Object metadata cache lookups", {{"result", "stale"}});
        m_invalidations_ = metrics.counter("lights3_meta_cache_invalidations_total",
                                           "Object metadata cache entries dropped by writes");
        m_entries_ = metrics.gauge("lights3_meta_cache_entries",
                                   "Object metadata records resident in the cache");
    }

    bool enabled() const { return opt_.max_entries > 0; }
    const MetaCacheOptions& options() const { return opt_; }

    // Returns the cached value or null; a miss also fills *tok for the later insert.
    // valid (optional) is the caller's freshness check against evidence it already holds
    // (localfs: the inode stamp of a stat/fstat taken *before* this call); a present
    // record it rejects is dropped, bumps the generation, and counts as stale + miss. It
    // runs under the shard lock: keep it to a comparison, never a syscall
    Ptr lookup(std::string_view bucket, std::string_view key, Token* tok = nullptr,
               const std::function<bool(const V&)>& valid = {}) {
        if (!enabled()) return nullptr;
        std::string k = ckey(bucket, key);
        uint32_t si = shard_of(k);
        Shard& s = shards_[si];
        std::lock_guard lk(s.m);
        auto it = s.map.find(k);
        if (it != s.map.end() && !expired(it->second)) {
            if (!valid || valid(*it->second.value)) {
                s.lru.splice(s.lru.begin(), s.lru, it->second.lru);
                s.hits++;
                m_hits_->inc();
                return it->second.value;
            }
            s.stale++;
            m_stale_->inc();
            s.gen++;  // like an invalidation: a fill racing this rejection is refused
        }
        if (it != s.map.end()) erase_locked(s, it);  // expired or stale: drop now
        s.misses++;
        m_misses_->inc();
        if (tok) *tok = Token{si, s.gen};
        return nullptr;
    }

    // Token without a lookup (warming from a listing page): counts neither hit nor miss
    Token token_for(std::string_view bucket, std::string_view key) const {
        if (!enabled()) return {};
        std::string k = ckey(bucket, key);
        uint32_t si = shard_of(k);
        const Shard& s = shards_[si];
        std::lock_guard lk(s.m);
        return Token{si, s.gen};
    }

    // Insert v under (bucket, key); silently dropped when tok is stale (an invalidation
    // touched this shard since the token was taken) or the cache is off. Replaces an
    // existing entry
    void insert(const Token& tok, std::string_view bucket, std::string_view key, Ptr v) {
        if (!enabled() || !v) return;
        std::string k = ckey(bucket, key);
        uint32_t si = shard_of(k);
        Shard& s = shards_[si];
        std::lock_guard lk(s.m);
        if (tok.shard != si || tok.gen != s.gen) {
            s.fills_dropped++;
            return;
        }
        if (auto it = s.map.find(k); it != s.map.end()) erase_locked(s, it);
        while (s.map.size() >= per_shard_ && !s.lru.empty()) {
            auto victim = s.map.find(s.lru.back());
            erase_locked(s, victim);
        }
        s.lru.push_front(k);
        Node n;
        n.value = std::move(v);
        n.lru = s.lru.begin();
        n.expires = opt_.ttl.count() > 0 ? std::chrono::steady_clock::now() + opt_.ttl
                                         : std::chrono::steady_clock::time_point::max();
        s.map.emplace(std::move(k), std::move(n));
        entries_.fetch_add(1, std::memory_order_relaxed);
        m_entries_->set(int64_t(entries_.load(std::memory_order_relaxed)));
    }

    // Drop one key and bump the shard's generation (in-flight fills for that shard are
    // refused). Call *after* the write's commit point
    void invalidate(std::string_view bucket, std::string_view key) {
        if (!enabled()) return;
        std::string k = ckey(bucket, key);
        Shard& s = shards_[shard_of(k)];
        std::lock_guard lk(s.m);
        s.gen++;
        s.invalidations++;
        m_invalidations_->inc();
        if (auto it = s.map.find(k); it != s.map.end()) erase_locked(s, it);
    }

    // Drop everything (meta restore, compaction ref swaps, bucket removal)
    void clear() {
        if (!enabled()) return;
        for (size_t i = 0; i < nshards_; ++i) {
            Shard& s = shards_[i];
            std::lock_guard lk(s.m);
            s.gen++;
            s.invalidations += s.map.size();
            m_invalidations_->inc(s.map.size());
            entries_.fetch_sub(s.map.size(), std::memory_order_relaxed);
            s.map.clear();
            s.lru.clear();
        }
        m_entries_->set(int64_t(entries_.load(std::memory_order_relaxed)));
    }

    // RAII invalidation on scope exit: declare right before a commit section so the
    // record is dropped after the commit point on every exit path -- success, throw,
    // and "commit outcome unknown" alike. In a coroutine the frame's locals are destroyed
    // before the awaiting caller resumes, so the invalidation is ordered before the
    // write is observed as complete
    struct InvalidateGuard {
        MetaCache* cache;
        std::string bucket, key;
        InvalidateGuard(MetaCache* c, std::string_view b, std::string_view k)
            : cache(c), bucket(b), key(k) {}
        InvalidateGuard(const InvalidateGuard&) = delete;
        InvalidateGuard& operator=(const InvalidateGuard&) = delete;
        ~InvalidateGuard() { cache->invalidate(bucket, key); }
    };
    [[nodiscard]] InvalidateGuard invalidate_on_exit(std::string_view bucket,
                                                     std::string_view key) {
        return InvalidateGuard(this, bucket, key);
    }

    MetaCacheStats stats() const {
        MetaCacheStats st;
        for (size_t i = 0; i < nshards_; ++i) {
            const Shard& s = shards_[i];
            std::lock_guard lk(s.m);
            st.hits += s.hits;
            st.misses += s.misses;
            st.invalidations += s.invalidations;
            st.stale += s.stale;
            st.fills_dropped += s.fills_dropped;
            st.entries += s.map.size();
        }
        return st;
    }

private:
    struct Node {
        Ptr value;
        std::list<std::string>::iterator lru;
        std::chrono::steady_clock::time_point expires;
    };
    struct Shard {
        mutable std::mutex m;
        std::unordered_map<std::string, Node> map;
        std::list<std::string> lru;  // front = most recent
        uint64_t gen = 0;
        uint64_t hits = 0, misses = 0, invalidations = 0, stale = 0, fills_dropped = 0;
    };

    static std::string ckey(std::string_view bucket, std::string_view key) {
        std::string k;
        k.reserve(bucket.size() + 1 + key.size());
        k.append(bucket);
        k.push_back('\0');  // neither side may contain NUL (validated upstream)
        k.append(key);
        return k;
    }
    uint32_t shard_of(const std::string& k) const {
        return uint32_t(std::hash<std::string>{}(k) % nshards_);
    }
    bool expired(const Node& n) const {
        return opt_.ttl.count() > 0 && std::chrono::steady_clock::now() >= n.expires;
    }
    void erase_locked(Shard& s, typename std::unordered_map<std::string, Node>::iterator it) {
        s.lru.erase(it->second.lru);
        s.map.erase(it);
        entries_.fetch_sub(1, std::memory_order_relaxed);
        m_entries_->set(int64_t(entries_.load(std::memory_order_relaxed)));
    }

    MetaCacheOptions opt_;
    size_t per_shard_ = 0;
    size_t nshards_ = 1;
    std::unique_ptr<Shard[]> shards_;
    std::atomic<uint64_t> entries_{0};
    std::shared_ptr<MetricCounter> m_hits_, m_misses_, m_stale_, m_invalidations_;
    std::shared_ptr<MetricGauge> m_entries_;
};

}  // namespace lights3::storage
