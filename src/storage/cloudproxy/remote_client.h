// cloudproxy internal header: ClientPool (httplib::Client connection pool) + generic
// signing pipeline + error mapping and retry (docs/cloudproxy-backend.md §2.2/§5/§8.1).
// Includes httplib; may only be included by src/storage/cloudproxy/*.cc and TUs internal
// to lights3_core.
#pragma once

#include <httplib/httplib.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "s3/auth/sigv4.h"
#include "s3/errors.h"
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

// Mutex-protected idle-stack connection pool; httplib::Client is not thread-safe, so
// leases are exclusive (docs/cloudproxy-backend.md §8.1)
class ClientPool {
public:
    ClientPool(const CloudProxyConfig& cfg, const Endpoint& ep,
               std::shared_ptr<MetricHistogram> wait_hist = nullptr);

    class Lease {
    public:
        Lease(ClientPool* pool, std::unique_ptr<httplib::Client> c)
            : pool_(pool), client_(std::move(c)) {}
        Lease(Lease&& o) noexcept = default;
        Lease(const Lease&) = delete;
        ~Lease() {
            if (client_) pool_->release(std::move(client_));
        }
        httplib::Client& client() { return *client_; }

    private:
        ClientPool* pool_;
        std::unique_ptr<httplib::Client> client_;
    };

    Lease acquire();  // blocks when at the cap; throws SlowDown if the wait exceeds request_timeout

private:
    friend class Lease;
    std::unique_ptr<httplib::Client> make_client() const;
    void release(std::unique_ptr<httplib::Client> c);

    const CloudProxyConfig cfg_;
    const Endpoint ep_;
    std::shared_ptr<MetricHistogram> wait_hist_;  // acquire wait duration (§8.2; may be null)
    std::mutex m_;
    std::condition_variable cv_;
    std::vector<std::unique_ptr<httplib::Client>> idle_;
    int total_ = 0;
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
          cred{cfg.access_key, cfg.secret_key} {}

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
    void backoff(int attempt) const;  // base x 2^n + jitter

    // Unified retry execution for idempotent requests: fn takes the leased client and sends
    // one request; transport errors / 5xx / 429 are retried per policy, and after exhaustion
    // the last Result is returned. op feeds the §8.2 metrics: each remote round trip records
    // its own latency, retries are counted separately
    template <class F>
    httplib::Result with_retry(const char* op, F&& fn) {
        auto hist = metrics.op_seconds(op);
        for (int attempt = 0;; ++attempt) {
            httplib::Result r = [&] {
                auto lease = pool.acquire();
                auto t0 = std::chrono::steady_clock::now();
                auto res = fn(lease.client());
                hist->observe(std::chrono::duration<double>(
                                  std::chrono::steady_clock::now() - t0)
                                  .count());
                return res;
            }();
            bool retry = !r ? retryable_transport(r.error()) : retryable_status(r->status);
            if (!retry || attempt >= cfg.retry_max) return r;
            metrics.count_retry(op);
            backoff(attempt);
        }
    }

    CloudProxyConfig cfg;
    Endpoint ep;
    RemoteMetrics metrics;
    ClientPool pool;
    s3::SigV4Authenticator auth;
    Credential cred;
};

// start-after value that skips an entire common-prefix group (docs/cloudproxy-backend.md
// §4.2): the prefix is padded with 0xff up to the key length limit. Under exclusive
// semantics every key inside the group is <= this value and gets skipped, while every
// successor key outside the group is > it and none are missed. (The old "last char +1"
// scheme would also skip a literal key identical to the boundary string)
std::string group_skip_token(std::string_view prefix);

}  // namespace lights3::storage::cloudproxy
