// L2: per-client rate limiting (roadmap §4.2, docs/http-adapter.md §2.3). One
// RateLimiter per key space — source IP (decided before signature verification,
// so a flood never reaches the HMAC) and access key (after it). Each key gets a
// token bucket (sustained rps, burst capacity) plus a concurrency cap; the table
// is bounded (LRU eviction of keys with nothing in flight) so a scan of random
// sources cannot grow it without limit. Rejections surface as 503 SlowDown with
// Retry-After: 1 — the throttling signal every S3 SDK already retries on.
#pragma once

#include <chrono>
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
        int rps = 0;           // 0 = no rate limit
        int burst = 0;         // bucket capacity; 0 = rps
        int max_inflight = 0;  // 0 = no concurrency cap
        bool any() const { return rps > 0 || max_inflight > 0; }
    };
    using Clock = std::chrono::steady_clock;

    RateLimiter(Limits limits, size_t max_tracked);

    bool enabled() const { return limits_.any(); }

    // Holds the in-flight slot of one admitted request; releasing it returns the slot
    class Token {
    public:
        Token() = default;
        Token(RateLimiter* l, std::string key) : l_(l), key_(std::move(key)) {}
        Token(Token&& o) noexcept : l_(o.l_), key_(std::move(o.key_)) { o.l_ = nullptr; }
        Token& operator=(Token&& o) noexcept {
            if (this != &o) {
                reset();
                l_ = o.l_;
                key_ = std::move(o.key_);
                o.l_ = nullptr;
            }
            return *this;
        }
        Token(const Token&) = delete;
        Token& operator=(const Token&) = delete;
        ~Token() { reset(); }
        void reset() {
            if (l_) l_->release(key_);
            l_ = nullptr;
        }

    private:
        RateLimiter* l_ = nullptr;
        std::string key_;
    };

    // nullopt = over the limit (rate or concurrency). An empty key is always admitted
    // without accounting (unknown source / auth disabled)
    std::optional<Token> admit(std::string_view key, Clock::time_point now = Clock::now());
    void release(const std::string& key);

    size_t tracked() const;

private:
    struct Entry {
        double tokens = 0;
        Clock::time_point last{};
        int inflight = 0;
        std::list<std::string>::iterator lru;
    };
    void evict_locked();

    Limits limits_;
    size_t max_tracked_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> table_;
    std::list<std::string> lru_;  // front = most recently used
};

}  // namespace lights3::s3
