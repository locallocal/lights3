// per-IP / per-access-key rate limiting (s3/ratelimit.h) — token
// bucket + concurrency cap + bounded table, and the dispatch integration
// (503 SlowDown + Retry-After, metrics), plus the timeout/ratelimit config surface
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "core/config.h"
#include "core/util/crypto.h"
#include "s3/auth/credential_store.h"
#include "s3/ratelimit.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

namespace {

using Clock = RateLimiter::Clock;

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

AuthConfig root_cfg() {
    AuthConfig a;
    a.credentials = {{"RLROOT", "root-sk"}};
    return a;
}

struct Env {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    AuthConfig acfg = root_cfg();
    std::shared_ptr<CredentialStore> store;
    std::unique_ptr<S3Service> svc;
    SigV4Authenticator signer = SigV4Authenticator::build(root_cfg());
    Credential root{"RLROOT", util::SecretString(std::string("root-sk"))};

    Env() {
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        store = sync_wait(CredentialStore::load(backend, acfg));
        auto auth = SigV4Authenticator::build(acfg);
        auth.set_provider(store);
        svc = std::make_unique<S3Service>(storage::BucketRouter::build(bcfg, backends), std::move(auth));
        svc->set_credential_store(store);
    }
    http::HttpResponse call(const std::string& path, const std::string& ip, const Credential* cred) {
        http::HttpRequest req;
        req.method = "GET";
        req.raw_path = path;
        req.path = path;
        req.remote_addr = ip;
        req.headers.add("Host", "localhost");
        req.headers.add("Content-Length", "0");
        if (cred) signer.sign(req, *cred, util::sha256_hex(""));
        return sync_wait(svc->dispatch(std::move(req)));
    }
};

}  // namespace

TEST(ratelimit_token_bucket_refills_at_rps) {
    RateLimiter rl({.rps = 2, .burst = 4}, 100);
    auto t0 = Clock::now();
    std::vector<RateLimiter::Token> held;
    for (int i = 0; i < 4; ++i) {
        auto t = rl.admit("a", t0);
        CHECK(t.has_value());
        held.push_back(std::move(*t));
    }
    // burst spent
    CHECK(!rl.admit("a", t0));
    // other keys unaffected
    CHECK(rl.admit("b", t0));
    // 0.8 tokens: not yet
    CHECK(!rl.admit("a", t0 + std::chrono::milliseconds(400)));
    // 1.2 tokens
    CHECK(rl.admit("a", t0 + std::chrono::milliseconds(600)));
    // A long pause refills to the burst cap, never beyond
    held.clear();
    for (int i = 0; i < 4; ++i) CHECK(rl.admit("a", t0 + std::chrono::seconds(100)));
    CHECK(!rl.admit("a", t0 + std::chrono::seconds(100)));
    // empty key: never accounted
    CHECK(rl.admit("", t0));
}

TEST(ratelimit_inflight_cap_and_release) {
    RateLimiter rl({.max_inflight = 2}, 100);
    auto a = rl.admit("k"), b = rl.admit("k");
    CHECK(a && b);
    CHECK(!rl.admit("k"));
    // slot returned
    a->reset();
    auto c = rl.admit("k");
    CHECK(c.has_value());
    {
        // move keeps exactly one release
        RateLimiter::Token moved = std::move(*c);
    }
    CHECK(rl.admit("k").has_value());
}

TEST(ratelimit_table_is_bounded) {
    RateLimiter rl({.rps = 1}, 3);
    auto t0 = Clock::now();
    for (int i = 0; i < 10; ++i) (void)rl.admit("k" + std::to_string(i), t0);
    CHECK(rl.tracked() <= size_t(3));
    // Keys with requests in flight are never evicted
    RateLimiter rl2({.max_inflight = 5}, 2);
    auto x = rl2.admit("x"), y = rl2.admit("y");
    (void)rl2.admit("z", t0);
    (void)rl2.admit("w", t0);
    // still tracked, still has its slot accounting
    CHECK(rl2.admit("x").has_value());
    CHECK(rl2.tracked() <= size_t(4));
    // Every tracked key in flight + a table already full: the newcomer must still be
    // admitted safely (it is never evicted out from under its own admission)
    RateLimiter rl3({.max_inflight = 2}, 1);
    auto p = rl3.admit("p");
    auto q = rl3.admit("q");
    CHECK(p && q);
    auto r = rl3.admit("r");
    CHECK(r.has_value());
    r->reset();
    CHECK(rl3.admit("r").has_value());
}

TEST(ratelimit_dispatch_per_ip_and_per_ak) {
    Env env;
    env.svc->set_rate_limiters(std::make_shared<RateLimiter>(RateLimiter::Limits{.rps = 1, .burst = 2}, 100),
                               std::make_shared<RateLimiter>(RateLimiter::Limits{.rps = 1, .burst = 1}, 100));
    // Per-IP: 2 from one address, the third is throttled with Retry-After; another address is fine
    auto r1 = env.call("/", "10.0.0.1", nullptr);
    auto r2 = env.call("/", "10.0.0.1", nullptr);
    // unsigned: rejected by auth, but admitted by the limiter
    CHECK_EQ(r1.status, 403);
    CHECK_EQ(r2.status, 403);
    auto r3 = env.call("/", "10.0.0.1", nullptr);
    CHECK_EQ(r3.status, 503);
    CHECK(contains(r3.small_body, "SlowDown"));
    CHECK_EQ(r3.headers.get("Retry-After").value_or(""), std::string("1"));
    CHECK_EQ(env.call("/", "10.0.0.2", nullptr).status, 403);
    // Probes are exempt from the IP limiter
    CHECK_EQ(env.call("/-/healthz", "10.0.0.1", nullptr).status, 200);
    // Per-AK: the first signed request admits, the second (same second) is throttled
    auto s1 = env.call("/", "10.0.0.3", &env.root);
    CHECK_EQ(s1.status, 200);
    auto s2 = env.call("/", "10.0.0.4", &env.root);
    CHECK_EQ(s2.status, 503);
    // Both scopes show up on /-/metrics
    auto m = env.call("/-/metrics", "10.0.0.9", nullptr);
    CHECK(contains(m.small_body, "lights3_ratelimit_rejections_total{scope=\"ip\"} 1"));
    CHECK(contains(m.small_body, "lights3_ratelimit_rejections_total{scope=\"ak\"} 1"));
}

TEST(ratelimit_and_timeout_config_surface) {
    std::string base = "backends:\n  - name: m\n    type: memory\n";
    auto cfg = Config::from_string(base);
    CHECK_EQ(cfg.http.header_timeout_sec, 30);
    CHECK_EQ(cfg.http.body_timeout_sec, 60);
    CHECK_EQ(cfg.http.write_timeout_sec, 60);
    CHECK_EQ(cfg.http.max_requests_per_connection, 1024);
    CHECK_EQ(cfg.ratelimit.per_ip_rps, 0);
    cfg = Config::from_string(base +
                              "http:\n  header_timeout: 10s\n  body_timeout: 5m\n  write_timeout: 2m\n"
                              "  max_requests_per_connection: 0\n"
                              "ratelimit:\n  per_ip_rps: 100\n  per_ip_burst: 200\n  per_ip_max_inflight: 8\n"
                              "  per_ak_rps: 50\n  per_ak_max_inflight: 4\n  max_tracked: 500\n");
    CHECK_EQ(cfg.http.header_timeout_sec, 10);
    CHECK_EQ(cfg.http.body_timeout_sec, 300);
    CHECK_EQ(cfg.http.write_timeout_sec, 120);
    CHECK_EQ(cfg.http.max_requests_per_connection, 0);
    CHECK_EQ(cfg.ratelimit.per_ip_rps, 100);
    CHECK_EQ(cfg.ratelimit.per_ip_burst, 200);
    CHECK_EQ(cfg.ratelimit.per_ip_max_inflight, 8);
    CHECK_EQ(cfg.ratelimit.per_ak_rps, 50);
    CHECK_EQ(cfg.ratelimit.per_ak_burst, 0);
    CHECK_EQ(cfg.ratelimit.max_tracked, 500);
    auto rejects = [&](const std::string& extra) {
        try {
            Config::from_string(base + extra);
        } catch (const std::runtime_error&) {
            return true;
        }
        return false;
    };
    CHECK(rejects("http:\n  header_timeout: 0s\n"));
    CHECK(rejects("http:\n  body_timeout: 2d\n"));
    CHECK(rejects("http:\n  max_requests_per_connection: -1\n"));
    // burst without rps
    CHECK(rejects("ratelimit:\n  per_ip_burst: 10\n"));
    CHECK(rejects("ratelimit:\n  max_tracked: 0\n"));
}

// The Token carries a pointer to the table entry rather than a copy of the key, so
// releasing is one atomic decrement instead of "copy the key, lock, hash it again". What
// keeps that pointer valid is the eviction rule: an entry with requests in flight is never
// erased, and unordered_map keeps element addresses stable across rehashing. This drives
// the two things that could break it -- rehashing under a live token, and eviction
// pressure while tokens are held -- and is worth running under ASan
TEST(ratelimit_token_survives_rehash_and_eviction) {
    RateLimiter rl({.max_inflight = 4}, 4096);
    std::vector<RateLimiter::Token> held;
    // Hold a slot on the first key, then grow the table far past its initial bucket count
    auto first = rl.admit("key-0");
    CHECK(first.has_value());
    for (int i = 1; i < 2000; ++i) {
        auto t = rl.admit("key-" + std::to_string(i));
        CHECK(t.has_value());
        if (i % 3 == 0) held.push_back(std::move(*t));
    }
    // The held tokens were handed out before hundreds of rehashes; releasing them now must
    // still land on their own entries
    first->reset();
    held.clear();
    CHECK(rl.admit("key-0").has_value());

    // Eviction pressure with tokens outstanding: the table is one slot wide, and every
    // newcomer forces an eviction sweep past entries that must not be touched
    RateLimiter small({.max_inflight = 2}, 1);
    std::vector<RateLimiter::Token> pinned;
    for (int i = 0; i < 64; ++i) {
        auto t = small.admit("pinned-" + std::to_string(i));
        CHECK(t.has_value());
        pinned.push_back(std::move(*t));
    }
    // releasing in reverse order, long after each entry stopped being the LRU head
    while (!pinned.empty()) pinned.pop_back();
    CHECK(small.tracked() >= size_t(1));
}

// Concurrent admit/release on one key: the counter is decremented without the lock, so
// the invariant to hold is "never more than max_inflight admitted at once, and the slots
// all come back"
TEST(ratelimit_concurrent_admit_release) {
    RateLimiter rl({.max_inflight = 8}, 64);
    std::atomic<int> live{0}, peak{0}, admitted{0}, refused{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < 8; ++t)
        ts.emplace_back([&] {
            for (int i = 0; i < 2000; ++i) {
                auto tok = rl.admit("hot");
                if (!tok) {
                    refused.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                admitted.fetch_add(1, std::memory_order_relaxed);
                int now = live.fetch_add(1, std::memory_order_acq_rel) + 1;
                int seen = peak.load(std::memory_order_relaxed);
                while (now > seen && !peak.compare_exchange_weak(seen, now)) {
                }
                std::this_thread::yield();
                live.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    for (auto& t : ts) t.join();
    CHECK(admitted.load() + refused.load() == 8 * 2000);
    CHECK(peak.load() <= 8);
    // every slot came back: the limiter admits a full set again
    std::vector<RateLimiter::Token> all;
    for (int i = 0; i < 8; ++i) {
        auto tok = rl.admit("hot");
        CHECK(tok.has_value());
        all.push_back(std::move(*tok));
    }
    CHECK(!rl.admit("hot"));
}
