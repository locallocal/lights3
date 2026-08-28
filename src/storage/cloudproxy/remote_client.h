// cloudproxy internal header: ClientPool (httplib::Client connection pool) + generic
// signing pipeline + error mapping and retry (docs/cloudproxy-backend.md §2.2/§5/§8.1).
// Includes httplib; may only be included by src/storage/cloudproxy/*.cc, TUs internal
// to lights3_core, and unit tests (tests/unit/test_cloudproxy.cc drives ClientPool
// directly).
#pragma once

#include <httplib/httplib.h>

#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "core/task.h"
#include "core/timer.h"
#include "s3/auth/sigv4.h"
#include "s3/errors.h"
#include "storage/cloudproxy/aws_credentials.h"
#include "storage/cloudproxy/cloudproxy_backend.h"

namespace lights3::storage::cloudproxy {

// SigV4's UNSIGNED-PAYLOAD literal (docs/cloudproxy-backend.md §3.2)
inline constexpr const char* kUnsignedPayload = "UNSIGNED-PAYLOAD";

struct Endpoint {
    bool https = false;
    std::string host;
    int port = 0;             // explicit, or defaulted per scheme
    std::string signed_host;  // byte-identical to the Host header httplib actually sends (docs/cloudproxy-backend.md §2.2)
    std::string base_url;     // scheme://host:port, input to httplib's universal Client

    static Endpoint parse(const std::string& url);  // throws std::runtime_error on invalid input
};

// Addressing target (docs/cloudproxy-backend.md §7): path-style = "/bucket/..." + endpoint
// Host; virtual-hosted = "/..." + "<bucket>.<endpoint-host>" Host. In both styles the TCP
// connection and SNI always point at the endpoint (ClientPool does not specialize); only
// Host/signature and path vary
struct Target {
    std::string prefix;  // "/<rb>" (already encoded) for path-style; empty for vhost
    std::string host;    // value that goes into the Host header and SigV4
    std::string bucket_path() const { return prefix.empty() ? "/" : prefix; }
    std::string object_path(std::string_view encoded_key_path) const {
        return prefix + std::string(encoded_key_path);
    }
};

// Remote observability metrics (docs/cloudproxy-backend.md §8.2): with an empty scope all
// are orphan instances and calls are harmless. Error counts are registered dynamically per
// remote code (get-or-create); the code set is bounded: the wire-code vocabulary +
// "http_<status>" + "transport"
struct RemoteMetrics {
    explicit RemoteMetrics(const MetricsScope& scope);
    std::shared_ptr<MetricHistogram> op_seconds(const char* op) const;  // cached registration per op
    void count_retry(const char* op) const;
    void count_error(const std::string& code) const;
    std::shared_ptr<MetricCounter> etag_mismatch;
    std::shared_ptr<MetricHistogram> pool_wait;

private:
    MetricsScope scope_;
    mutable std::mutex m_;  // op/code -> instance cache (avoids the registry's global lock on every call)
    mutable std::map<std::string, std::shared_ptr<MetricHistogram>> ops_;
    mutable std::map<std::string, std::shared_ptr<MetricCounter>> retries_, errors_;
};

// Mutex-protected idle-deque connection pool; httplib::Client is not thread-safe, so
// leases are exclusive (docs/cloudproxy-backend.md §8.1). Hygiene (roadmap §3.3): idle
// entries carry timestamps — a connection idle beyond pool_idle_timeout is never reused
// (a NAT/remote that silently dropped it would surface as first-request retry spikes)
// and a light TimerQueue reaper closes them during quiet periods; pool_max_lifetime
// retires connections by age at release. total_ shrinks accordingly.
class ClientPool {
public:
    ClientPool(const CloudProxyConfig& cfg, const Endpoint& ep,
               std::shared_ptr<MetricHistogram> wait_hist = nullptr);
    ~ClientPool();  // stops the reaper (blocks on an in-flight tick)

    // Creation time travels with the client so max-lifetime survives lease cycles
    struct PooledClient {
        std::unique_ptr<httplib::Client> c;
        std::chrono::steady_clock::time_point created{};
    };

    class Lease {
    public:
        Lease(ClientPool* pool, PooledClient pc) : pool_(pool), pc_(std::move(pc)) {}
        Lease(Lease&& o) noexcept = default;
        Lease(const Lease&) = delete;
        ~Lease() {
            if (pc_.c) pool_->release(std::move(pc_));
        }
        httplib::Client& client() { return *pc_.c; }

    private:
        ClientPool* pool_;
        PooledClient pc_;
    };

    // Blocking acquire for the pump threads (private per-transfer threads); throws
    // SlowDown if the wait exceeds request_timeout
    Lease acquire();
    // Coroutine acquire for control-plane paths (roadmap §3.3): a pool thread is never
    // parked in cv_.wait — at capacity the awaiter queues and release() hands the
    // connection over directly; a TimerQueue timer enforces the same request_timeout /
    // SlowDown contract. Resumes via the executor set below (or inline on the
    // releasing/timer thread when unset — callers hop to a pool right after anyway)
    Task<Lease> acquire_async();
    void set_resume_executor(IExecutor* ex) { resume_ex_ = ex; }

    struct Stats {
        int total = 0;
        size_t idle = 0;
    };
    Stats stats();  // tests/observability

private:
    friend class Lease;

    // Waiter state shared with its timeout timer (own lock; the timer callback never
    // touches the pool, so pool teardown cannot race it)
    struct Waiter {
        std::mutex m;
        std::coroutine_handle<> h;
        IExecutor* ex = nullptr;
        bool done = false;
        bool timed_out = false;
        bool create_new = false;  // granted a capacity slot: make the client after resume
        PooledClient granted;
    };

    struct IdleEntry {
        PooledClient pc;
        std::chrono::steady_clock::time_point since{};
    };

    std::unique_ptr<httplib::Client> make_client() const;
    void release(PooledClient pc);
    void reap_stale_locked();  // drop idles beyond pool_idle_timeout (requires m_)
    // Capacity slot freed (lifetime retirement / creation rollback): --total_, then hand
    // the slot to an async waiter or wake a cv waiter; resumes outside the pool lock
    void retire_slot();
    void schedule_reaper();

    const CloudProxyConfig cfg_;
    const Endpoint ep_;
    std::shared_ptr<MetricHistogram> wait_hist_;  // acquire wait duration (§8.2; may be null)
    IExecutor* resume_ex_ = nullptr;
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<IdleEntry> idle_;  // back = most recently used; stale ones age out at the front
    std::deque<std::shared_ptr<Waiter>> waiters_;
    int total_ = 0;
    bool stopping_ = false;
    TimerQueue::Id reaper_ = 0;
};

// 404 context for error mapping (docs/cloudproxy-backend.md §5.1: on a 404 with an
// unparsable body, fill in semantics based on the operation)
enum class ErrCtx { None, Key, Bucket, Upload };

// Remote wire code -> local S3ErrorCode, including near-synonym aliases
// (BucketAlreadyExists, TooManyRequests, etc.); shared by throw_remote_error and
// complete's 200-with-error-body path
std::optional<s3::S3ErrorCode> map_remote_code(std::string_view wire);

struct RemoteContext {
    RemoteContext(CloudProxyConfig cfg_in, Endpoint ep_in, MetricsScope scope = {})
        : cfg(std::move(cfg_in)),
          ep(ep_in),
          metrics(scope),
          pool(cfg, ep, metrics.pool_wait),
          auth(s3::SigV4Authenticator::build(
              AuthConfig{.credentials = {}, .region = cfg.region, .service = "s3"})),
          cred{cfg.access_key, cfg.secret_key} {
        // Credential chain (roadmap §3.3): only when no static keys are configured —
        // env → container endpoint → EC2 IMDSv2, resolved lazily on first signing
        if (cfg.access_key.empty() || cfg.secret_key.empty())
            cred_chain = std::make_unique<CredentialProvider>(cfg.endpoint, cfg.imds_endpoint);
    }

    // Addressing (docs/cloudproxy-backend.md §7): yields the path prefix and Host per force_path_style
    Target target(const std::string& remote_bucket) const;

    // Build a minimal HttpRequest solely for signing, then carry it over as
    // httplib::Headers (docs/cloudproxy-backend.md §2.2). x-amz-* entries in extra
    // automatically enter SignedHeaders; Content-Type goes through the httplib parameter,
    // do not put it here. Empty host = endpoint Host; vhost requests pass Target::host
    httplib::Headers signed_headers(
        const std::string& method, const std::string& raw_path, const std::string& raw_query,
        const std::vector<std::pair<std::string, std::string>>& extra,
        const std::string& payload_hash, const std::string& host = "") const;

    // Remote error -> local S3Error (single-point implementation of the
    // docs/cloudproxy-backend.md §5.1 mapping matrix)
    [[noreturn]] void throw_remote_error(int status, const std::string& body, ErrCtx ctx,
                                         std::string_view resource) const;
    [[noreturn]] void throw_transport_error(httplib::Error err) const;

    bool retryable_status(int status) const {
        return status == 429 || status == 500 || status == 502 || status == 503 ||
               status == 504;
    }
    static bool retryable_transport(httplib::Error e) {
        return e == httplib::Error::Connection || e == httplib::Error::ConnectionTimeout ||
               e == httplib::Error::SSLConnection || e == httplib::Error::Read ||
               e == httplib::Error::Write;
    }
    // Connection-establishment-stage errors (the subset safely retryable for PUT-like ops,
    // docs/cloudproxy-backend.md §5.2)
    static bool connection_stage_error(httplib::Error e) {
        return e == httplib::Error::Connection || e == httplib::Error::ConnectionTimeout ||
               e == httplib::Error::SSLConnection;
    }
    // ---- Backoff and Retry-After (roadmap §3.3) ----
    // 429/503 responses may carry Retry-After (integer seconds or HTTP-date); the hint,
    // clamped to [0, 60s], replaces the exponential formula for that wait
    static std::optional<int64_t> retry_after_hint(const httplib::Result& r);
    // Delay for this retry in ms: the hint verbatim, else base x 2^n + jitter (clamped)
    int64_t backoff_delay_ms(int attempt, std::optional<int64_t> retry_after_ms = {}) const;
    // Blocking wait for the pump threads (private per-transfer threads; a coroutine path
    // must use CloudProxyBackend::async_backoff instead — never sleep on a pool thread)
    void backoff(int attempt, std::optional<int64_t> retry_after_ms = {}) const;

    // ---- Circuit breaker (roadmap §3.3) ----
    // False = open (fail fast); true may hand this caller the single half-open probe.
    // Every definitive attempt outcome must be reported back via breaker_observe —
    // transport error / 5xx count as failures, <500 as success, 429 is neutral
    bool breaker_allow();
    void breaker_observe(const httplib::Result& r);
    void breaker_report(bool ok);
    // Convenience gate: throws SlowDown (and counts code "breaker_open") when open
    void breaker_gate();

    // ---- Per-op deadline (roadmap §3.3) ----
    // Steady deadline for one operation's whole retry loop; max() when disabled
    std::chrono::steady_clock::time_point op_deadline() const {
        return cfg.op_deadline_ms > 0
                   ? std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(cfg.op_deadline_ms)
                   : std::chrono::steady_clock::time_point::max();
    }
    static bool deadline_allows(std::chrono::steady_clock::time_point deadline,
                                int64_t next_delay_ms) {
        return deadline == std::chrono::steady_clock::time_point::max() ||
               std::chrono::steady_clock::now() + std::chrono::milliseconds(next_delay_ms) <
                   deadline;
    }

    // One measured attempt on a leased client (blocking; the caller owns retry policy —
    // see CloudProxyBackend::retry_io for the coroutine driver)
    template <class F>
    httplib::Result attempt(const std::shared_ptr<MetricHistogram>& hist,
                            httplib::Client& client, F&& fn) {
        auto t0 = std::chrono::steady_clock::now();
        auto res = fn(client);
        hist->observe(
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count());
        return res;
    }

    CloudProxyConfig cfg;
    Endpoint ep;
    RemoteMetrics metrics;
    ClientPool pool;
    s3::SigV4Authenticator auth;
    Credential cred;                                 // static keys (may be empty)
    std::unique_ptr<CredentialProvider> cred_chain;  // set when static keys are absent

private:
    // Breaker state: consecutive definitive failures; >= threshold = open until the
    // cooldown passes, then a single probe (probe_inflight_) decides
    std::mutex breaker_m_;
    int consec_failures_ = 0;
    bool probe_inflight_ = false;
    std::chrono::steady_clock::time_point breaker_open_until_{};
};

// start-after value that skips an entire common-prefix group (docs/cloudproxy-backend.md
// §4.2): the prefix is padded with 0xff up to the key length limit. Under exclusive
// semantics every key inside the group is <= this value and gets skipped, while every
// successor key outside the group is > it and none are missed. (The old "last char +1"
// scheme would also skip a literal key identical to the boundary string)
std::string group_skip_token(std::string_view prefix);

}  // namespace lights3::storage::cloudproxy
