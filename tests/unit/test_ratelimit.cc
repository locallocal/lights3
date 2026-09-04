// roadmap §4.2: per-IP / per-access-key rate limiting (s3/ratelimit.h) — token
// bucket + concurrency cap + bounded table, and the dispatch integration
// (503 SlowDown + Retry-After, metrics), plus the timeout/ratelimit config surface
#include <chrono>

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
    CHECK(!rl.admit("a", t0));                                     // burst spent
    CHECK(rl.admit("b", t0));                                      // other keys unaffected
    CHECK(!rl.admit("a", t0 + std::chrono::milliseconds(400)));   // 0.8 tokens: not yet
    CHECK(rl.admit("a", t0 + std::chrono::milliseconds(600)));    // 1.2 tokens
    // A long pause refills to the burst cap, never beyond
    held.clear();
    for (int i = 0; i < 4; ++i) CHECK(rl.admit("a", t0 + std::chrono::seconds(100)));
    CHECK(!rl.admit("a", t0 + std::chrono::seconds(100)));
    CHECK(rl.admit("", t0));  // empty key: never accounted
}

TEST(ratelimit_inflight_cap_and_release) {
    RateLimiter rl({.max_inflight = 2}, 100);
    auto a = rl.admit("k"), b = rl.admit("k");
    CHECK(a && b);
    CHECK(!rl.admit("k"));
    a->reset();  // slot returned
    auto c = rl.admit("k");
    CHECK(c.has_value());
    {
        RateLimiter::Token moved = std::move(*c);  // move keeps exactly one release
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
    CHECK(rl2.admit("x").has_value());  // still tracked, still has its slot accounting
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
    CHECK_EQ(r1.status, 403);  // unsigned: rejected by auth, but admitted by the limiter
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
    CHECK(rejects("ratelimit:\n  per_ip_burst: 10\n"));  // burst without rps
    CHECK(rejects("ratelimit:\n  max_tracked: 0\n"));
}
