// L2: per-client rate limiting (docs/architecture/http-adapter.md §2.3). One
// RateLimiter per key space — source IP (decided before signature verification,
// so a flood never reaches the HMAC) and access key (after it). Each key gets a
// token bucket (sustained rps, burst capacity) plus a concurrency cap; the table
// is bounded (LRU eviction of keys with nothing in flight) so a scan of random
// sources cannot grow it without limit. Rejections surface as 503 SlowDown with
// Retry-After: 1 — the throttling signal every S3 SDK already retries on.
#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace lights3::s3 {

class RateLimiter {
public:
    struct Limits {
        // 0 = no rate limit
        int rps = 0;
        // bucket capacity; 0 = rps
        int burst = 0;
        // 0 = no concurrency cap
        int max_inflight = 0;
        bool any() const { return rps > 0 || max_inflight > 0; }
    };
    using Clock = std::chrono::steady_clock;

    RateLimiter(Limits limits, size_t max_tracked);

    bool enabled() const { return limits_.any(); }

private:
    struct Entry {
        double tokens = 0;
        Clock::time_point last{};
        // Decremented by Token without taking mu_. An entry is only ever evicted while
        // this is zero (checked under mu_, and admit increments under mu_), so a live
        // Token's pointer to it cannot dangle: unordered_map keeps element addresses
        // stable across rehashing, and the only thing that invalidates one is the erase
        // that this counter prevents
        std::atomic<int> inflight{0};
        std::list<std::string>::iterator lru;
    };

    // Transparent hashing so the hot path looks up by string_view: admit used to copy the
    // key into a std::string before even knowing whether it was already tracked
    struct KeyHash {
        using is_transparent = void;
        size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
    };

public:
    // Holds the in-flight slot of one admitted request; releasing it returns the slot.
    // Carries the entry itself rather than a copy of the key: releasing is then a single
    // atomic decrement instead of "copy the key, take the process-wide lock, hash it again"
    class Token {
    public:
        Token() = default;
        explicit Token(Entry* e) : e_(e) {}
        Token(Token&& o) noexcept : e_(o.e_) { o.e_ = nullptr; }
        Token& operator=(Token&& o) noexcept {
            if (this != &o) {
                reset();
                e_ = o.e_;
                o.e_ = nullptr;
            }
            return *this;
        }
        Token(const Token&) = delete;
        Token& operator=(const Token&) = delete;
        ~Token() { reset(); }
        void reset() {
            if (e_) e_->inflight.fetch_sub(1, std::memory_order_acq_rel);
            e_ = nullptr;
        }

    private:
        Entry* e_ = nullptr;
    };

    // nullopt = over the limit (rate or concurrency). An empty key is always admitted
    // without accounting (unknown source / auth disabled)
    std::optional<Token> admit(std::string_view key, Clock::time_point now = Clock::now());

    size_t tracked() const;

private:
    void evict_locked();

    Limits limits_;
    size_t max_tracked_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry, KeyHash, std::equal_to<>> table_;
    // front = most recently used
    std::list<std::string> lru_;
};

}  // namespace lights3::s3
