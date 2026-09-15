#include "s3/ratelimit.h"

#include <algorithm>

namespace lights3::s3 {

RateLimiter::RateLimiter(Limits limits, size_t max_tracked)
    : limits_(limits), max_tracked_(std::max<size_t>(1, max_tracked)) {
    if (limits_.burst <= 0) limits_.burst = limits_.rps;
}

std::optional<RateLimiter::Token> RateLimiter::admit(std::string_view key, Clock::time_point now) {
    if (!enabled() || key.empty()) return Token{};
    std::lock_guard lk(mu_);
    // Heterogeneous lookup: the hit path (every request of an already-seen client) does
    // not materialize the key at all
    auto it = table_.find(key);
    if (it == table_.end()) {
        // Make room BEFORE inserting: eviction only drops keys with nothing in
        // flight, and the key being admitted is exactly such a key until the slot
        // below is taken — evicting after the insert could free `it` under us
        if (table_.size() >= max_tracked_) evict_locked();
        lru_.emplace_front(key);
        // Entry holds an atomic and is therefore neither copyable nor movable: insert it
        // in place and fill the fields afterwards
        it = table_.try_emplace(std::string(key)).first;
        Entry& fresh = it->second;
        // a new key starts with a full bucket
        fresh.tokens = limits_.burst;
        fresh.last = now;
        fresh.lru = lru_.begin();
    } else {
        lru_.splice(lru_.begin(), lru_, it->second.lru);
    }
    Entry& e = it->second;
    if (limits_.rps > 0) {
        double dt = std::chrono::duration<double>(now - e.last).count();
        if (dt > 0) {
            e.tokens = std::min<double>(limits_.burst, e.tokens + dt * limits_.rps);
            e.last = now;
        }
        if (e.tokens < 1.0) return std::nullopt;
    }
    // Reading the counter the Token decrements without the lock: a release that lands
    // between this load and the increment below only ever makes the limiter admit one
    // request it could have admitted a moment later — the bucket above is the sustained
    // bound, this cap is the concurrency one, and neither is exact by construction
    if (limits_.max_inflight > 0 && e.inflight.load(std::memory_order_acquire) >= limits_.max_inflight)
        return std::nullopt;
    if (limits_.rps > 0) e.tokens -= 1.0;
    e.inflight.fetch_add(1, std::memory_order_acq_rel);
    return Token{&e};
}

size_t RateLimiter::tracked() const {
    std::lock_guard lk(mu_);
    return table_.size();
}

// Drop least-recently-seen keys with nothing in flight until the table fits. A
// key that comes back later simply starts with a full bucket again — the bound
// protects memory, exactness for evicted stragglers is not the goal
void RateLimiter::evict_locked() {
    while (table_.size() >= max_tracked_ && !lru_.empty()) {
        auto victim = std::prev(lru_.end());
        bool removed = false;
        for (auto it = victim;; --it) {
            auto tit = table_.find(*it);
            if (tit != table_.end() && tit->second.inflight.load(std::memory_order_acquire) == 0) {
                table_.erase(tit);
                lru_.erase(it);
                removed = true;
                break;
            }
            if (it == lru_.begin()) break;
        }
        // everything tracked is in flight: nothing to evict
        if (!removed) break;
    }
}

}  // namespace lights3::s3
