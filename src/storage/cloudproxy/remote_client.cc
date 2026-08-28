#include "storage/cloudproxy/remote_client.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <stdexcept>
#include <thread>

#include "core/config.h"
#include "core/log.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/xml.h"

namespace lights3::storage {

// ---------- Configuration ----------

namespace {

const std::string* find(const std::map<std::string, std::string>& p, const char* k) {
    auto it = p.find(k);
    return it == p.end() ? nullptr : &it->second;
}

int int_param(const std::map<std::string, std::string>& p, const char* k, int def,
              const std::string& name) {
    auto* v = find(p, k);
    if (!v) return def;
    try {
        return std::stoi(*v);
    } catch (...) {
        throw std::runtime_error("cloudproxy backend '" + name + "': invalid " + k + ": " + *v);
    }
}

bool bool_param(const std::map<std::string, std::string>& p, const char* k, bool def,
                const std::string& name) {
    auto* v = find(p, k);
    if (!v) return def;
    try {
        return parse_bool(*v);  // shared token set (core/config.h), consistent with duostore
    } catch (...) {
        throw std::runtime_error("cloudproxy backend '" + name + "': invalid " + k + ": " + *v);
    }
}

}  // namespace

CloudProxyConfig CloudProxyConfig::from_params(
    const std::string& name, const std::map<std::string, std::string>& params) {
    CloudProxyConfig c;
    if (auto* v = find(params, "endpoint")) c.endpoint = *v;
    if (c.endpoint.empty())
        throw std::runtime_error("cloudproxy backend '" + name + "' needs endpoint");
    if (auto* v = find(params, "region")) c.region = *v;
    if (auto* v = find(params, "access_key")) c.access_key = *v;
    if (auto* v = find(params, "secret_key")) c.secret_key = *v;
    if (auto* v = find(params, "bucket_prefix")) c.bucket_prefix = *v;
    if (auto* v = find(params, "ca_cert")) c.ca_cert = *v;
    c.force_path_style = bool_param(params, "force_path_style", true, name);
    c.control_in_pump = bool_param(params, "control_in_pump", false, name);
    c.tls_verify = bool_param(params, "tls_verify", true, name);
    c.verify_etag = bool_param(params, "verify_etag", true, name);
    c.connect_timeout_ms = int_param(params, "connect_timeout_ms", 5000, name);
    c.request_timeout_ms = int_param(params, "request_timeout_ms", 60000, name);
    c.retry_max = int_param(params, "retry_max", 3, name);
    c.retry_base_ms = int_param(params, "retry_base_ms", 100, name);
    c.max_connections = int_param(params, "max_connections", 16, name);
    c.pool_idle_timeout_ms = int_param(params, "pool_idle_timeout_ms", 60'000, name);
    c.pool_max_lifetime_ms = int_param(params, "pool_max_lifetime_ms", 0, name);
    c.breaker_threshold = int_param(params, "breaker_threshold", 10, name);
    c.breaker_cooldown_ms = int_param(params, "breaker_cooldown_ms", 10'000, name);
    c.op_deadline_ms = int_param(params, "op_deadline_ms", 0, name);
    if (auto* v = find(params, "imds_endpoint")) c.imds_endpoint = *v;
    if (auto* v = find(params, "queue_cap")) {
        try {
            c.queue_cap_bytes = parse_size(*v);
        } catch (...) {
            throw std::runtime_error("cloudproxy backend '" + name + "': invalid queue_cap: " +
                                     *v);
        }
    }
    if (auto* v = find(params, "spool_max_bytes")) {
        try {
            c.spool_max_bytes = parse_size(*v);
        } catch (...) {
            throw std::runtime_error("cloudproxy backend '" + name +
                                     "': invalid spool_max_bytes: " + *v);
        }
    }
    if (auto* v = find(params, "spool_dir")) c.spool_dir = *v;
    // Numeric ranges are pinned down at load time, ruling out runtime arithmetic surprises
    // (e.g. backoff overflow)
    auto require_range = [&](const char* k, int64_t v, int64_t lo, int64_t hi) {
        if (v < lo || v > hi)
            throw std::runtime_error("cloudproxy backend '" + name + "': " + k + "=" +
                                     std::to_string(v) + " out of range [" + std::to_string(lo) +
                                     ", " + std::to_string(hi) + "]");
    };
    require_range("connect_timeout_ms", c.connect_timeout_ms, 1, 600'000);
    require_range("request_timeout_ms", c.request_timeout_ms, 1, 3'600'000);
    require_range("retry_max", c.retry_max, 0, 16);
    require_range("retry_base_ms", c.retry_base_ms, 1, 60'000);
    require_range("max_connections", c.max_connections, 1, 4096);
    require_range("pool_idle_timeout_ms", c.pool_idle_timeout_ms, 0, 3'600'000);
    require_range("pool_max_lifetime_ms", c.pool_max_lifetime_ms, 0, 86'400'000);
    require_range("breaker_threshold", c.breaker_threshold, 0, 10'000);
    require_range("breaker_cooldown_ms", c.breaker_cooldown_ms, 1, 3'600'000);
    require_range("op_deadline_ms", c.op_deadline_ms, 0, 3'600'000);
    require_range("queue_cap", static_cast<int64_t>(c.queue_cap_bytes), 4096, 1 << 30);
    if (!c.imds_endpoint.empty() && c.imds_endpoint.rfind("http://", 0) != 0 &&
        c.imds_endpoint.rfind("https://", 0) != 0)
        throw std::runtime_error("cloudproxy backend '" + name +
                                 "': imds_endpoint must be an http(s) URL");
    // virtual-hosted style (force_path_style=false): connection and SNI always point at the
    // endpoint; only Host/signature and path vary per bucket (docs/cloudproxy-backend.md §7);
    // bucket names containing '.' will mismatch under TLS wildcard certificates -- a
    // deployment-side constraint, not blocked here
    // The concatenated name is validated as a whole against S3 rules
    // (docs/cloudproxy-backend.md §4.3): "aaa" stands for the shortest legal local name,
    // covering charset/leading-char/".."/length issues the prefix introduces; a prefix that
    // is doomed to be invalid errors out at load time
    if (!c.bucket_prefix.empty()) {
        try {
            validate_bucket_name(c.bucket_prefix + "aaa");
        } catch (const s3::S3Error& e) {
            throw std::runtime_error("cloudproxy backend '" + name + "': invalid bucket_prefix '" +
                                     c.bucket_prefix + "': " + e.message);
        }
    }
    cloudproxy::Endpoint::parse(c.endpoint);  // validate early, surfacing errors at load time
    return c;
}

}  // namespace lights3::storage

namespace lights3::storage::cloudproxy {

using s3::S3Error;
using s3::S3ErrorCode;

// ---------- Endpoint ----------

Endpoint Endpoint::parse(const std::string& url) {
    Endpoint ep;
    std::string rest;
    if (url.rfind("https://", 0) == 0) {
        ep.https = true;
        rest = url.substr(8);
    } else if (url.rfind("http://", 0) == 0) {
        ep.https = false;
        rest = url.substr(7);
    } else {
        throw std::runtime_error("cloudproxy endpoint must start with http:// or https://: " +
                                 url);
    }
    if (!rest.empty() && rest.back() == '/') rest.pop_back();
    if (rest.empty() || rest.find('/') != std::string::npos)
        throw std::runtime_error("cloudproxy endpoint must be scheme://host[:port]: " + url);
    auto colon = rest.find(':');
    if (colon == std::string::npos) {
        ep.host = rest;
        ep.port = ep.https ? 443 : 80;
    } else {
        ep.host = rest.substr(0, colon);
        try {
            ep.port = std::stoi(rest.substr(colon + 1));
        } catch (...) {
            throw std::runtime_error("cloudproxy endpoint has invalid port: " + url);
        }
        if (ep.port < 1 || ep.port > 65535)
            throw std::runtime_error("cloudproxy endpoint has invalid port: " + url);
    }
    if (ep.host.empty())
        throw std::runtime_error("cloudproxy endpoint has empty host: " + url);
    // httplib's Host header: default port sends host only, otherwise host:port (the
    // consistency trap of docs/cloudproxy-backend.md §2.2)
    bool default_port = ep.port == (ep.https ? 443 : 80);
    ep.signed_host = default_port ? ep.host : ep.host + ":" + std::to_string(ep.port);
    ep.base_url = std::string(ep.https ? "https://" : "http://") + ep.host + ":" +
                  std::to_string(ep.port);
    return ep;
}

// ---------- Metrics (docs/cloudproxy-backend.md §8.2) ----------

RemoteMetrics::RemoteMetrics(const MetricsScope& scope) : scope_(scope) {
    etag_mismatch = scope.counter(
        "lights3_cloudproxy_etag_mismatch_total",
        "Uploads where remote ETag disagreed with locally computed MD5 (in-transit corruption)");
    pool_wait = scope.histogram("lights3_cloudproxy_pool_wait_seconds",
                                "Time spent waiting for a free remote connection lease",
                                {0.001, 0.01, 0.1, 1, 10});
}

std::shared_ptr<MetricHistogram> RemoteMetrics::op_seconds(const char* op) const {
    std::lock_guard lk(m_);
    auto& h = ops_[op];
    if (!h)
        h = scope_.histogram("lights3_cloudproxy_remote_request_seconds",
                             "Remote round-trip duration per attempt (data plane = full "
                             "transfer)",
                             {0.005, 0.02, 0.1, 0.5, 2, 10, 60}, {{"op", op}});
    return h;
}

void RemoteMetrics::count_retry(const char* op) const {
    std::lock_guard lk(m_);
    auto& c = retries_[op];
    if (!c)
        c = scope_.counter("lights3_cloudproxy_retries_total",
                           "Remote request retries (backoff taken)", {{"op", op}});
    c->inc();
}

void RemoteMetrics::count_error(const std::string& code) const {
    std::lock_guard lk(m_);
    auto& c = errors_[code];
    if (!c)
        c = scope_.counter("lights3_cloudproxy_remote_errors_total",
                           "Remote failures mapped to local errors, by remote code",
                           {{"code", code}});
    c->inc();
}

// ---------- ClientPool ----------

ClientPool::ClientPool(const CloudProxyConfig& cfg, const Endpoint& ep,
                       std::shared_ptr<MetricHistogram> wait_hist)
    : cfg_(cfg), ep_(ep), wait_hist_(std::move(wait_hist)) {
    schedule_reaper();
}

ClientPool::~ClientPool() {
    TimerQueue::Id id;
    {
        std::lock_guard lk(m_);
        stopping_ = true;
        id = reaper_;
    }
    // cancel blocks on an in-flight tick; the tick takes m_, so cancel outside the lock
    TimerQueue::instance().cancel(id);
}

std::unique_ptr<httplib::Client> ClientPool::make_client() const {
    auto c = std::make_unique<httplib::Client>(ep_.base_url);
    c->set_connection_timeout(std::chrono::milliseconds(cfg_.connect_timeout_ms));
    c->set_read_timeout(std::chrono::milliseconds(cfg_.request_timeout_ms));
    c->set_write_timeout(std::chrono::milliseconds(cfg_.request_timeout_ms));
    c->set_keep_alive(true);
    c->set_tcp_nodelay(true);
    if (ep_.https) {
        c->enable_server_certificate_verification(cfg_.tls_verify);
        if (!cfg_.ca_cert.empty()) c->set_ca_cert_path(cfg_.ca_cert);
    }
    return c;
}

// Idle entries age at the front (back = most recently used); anything idle beyond
// pool_idle_timeout is dropped — the remote/NAT likely closed it already, and reusing
// it would surface as a first-request transport error and retry spike (roadmap §3.3).
// Invariant: waiters exist only while idle_ is empty (release hands off before pushing
// idle), so reaping never needs to wake anyone — it only shrinks total_
void ClientPool::reap_stale_locked() {
    if (cfg_.pool_idle_timeout_ms <= 0) return;
    auto now = std::chrono::steady_clock::now();
    auto ttl = std::chrono::milliseconds(cfg_.pool_idle_timeout_ms);
    while (!idle_.empty() && now - idle_.front().since > ttl) {
        idle_.pop_front();
        --total_;
    }
}

void ClientPool::schedule_reaper() {
    if (cfg_.pool_idle_timeout_ms <= 0) return;
    auto interval = std::chrono::milliseconds(
        std::max(cfg_.pool_idle_timeout_ms / 2, 1000));
    std::lock_guard lk(m_);
    if (stopping_) return;
    reaper_ = TimerQueue::instance().add(interval, [this] {
        {
            std::lock_guard g(m_);
            if (stopping_) return;
            reap_stale_locked();
        }
        schedule_reaper();  // re-arm after completion (never overlaps)
    });
}

// Capacity slot freed (lifetime retirement / creation rollback): prefer handing the
// slot to an async waiter — who re-takes it and creates a fresh client after resuming —
// else wake a cv waiter. Waiters resume outside the pool lock: an inline resume under
// m_ could re-enter the pool (creation-failure rollback) and self-deadlock
void ClientPool::retire_slot() {
    std::unique_lock lk(m_);
    --total_;
    for (;;) {
        if (waiters_.empty()) {
            cv_.notify_one();
            return;
        }
        auto w = std::move(waiters_.front());
        waiters_.pop_front();
        {
            std::lock_guard g(w->m);
            if (w->done) continue;  // timed out; zombie entry
            w->done = true;
            w->create_new = true;
        }
        ++total_;  // the slot transfers to the waiter's upcoming client
        lk.unlock();
        if (w->ex)
            w->ex->post(w->h);
        else
            w->h.resume();
        return;
    }
}

ClientPool::Lease ClientPool::acquire() {
    auto start = std::chrono::steady_clock::now();
    auto deadline = start + std::chrono::milliseconds(cfg_.request_timeout_ms);
    // Wait-time observation (§8.2): recorded both when a Lease is issued and when SlowDown
    // is thrown -- worsening queueing is the direct signal for tuning max_connections
    auto observe = [&] {
        if (wait_hist_)
            wait_hist_->observe(std::chrono::duration<double>(
                                    std::chrono::steady_clock::now() - start)
                                    .count());
    };
    std::unique_lock lk(m_);
    for (;;) {
        reap_stale_locked();
        if (!idle_.empty()) {
            auto pc = std::move(idle_.back().pc);
            idle_.pop_back();
            observe();
            return Lease(this, std::move(pc));
        }
        if (total_ < cfg_.max_connections) {
            ++total_;
            lk.unlock();
            observe();
            try {
                return Lease(this, {make_client(), std::chrono::steady_clock::now()});
            } catch (...) {
                // Roll back the slot and wake waiters; an exception (e.g. bad_alloc) must
                // not shrink the pool permanently
                retire_slot();
                throw;
            }
        }
        if (!cv_.wait_until(lk, deadline,
                            [&] { return !idle_.empty() || total_ < cfg_.max_connections; })) {
            observe();
            throw S3Error(S3ErrorCode::SlowDown,
                          "cloudproxy: all remote connections busy, try again later");
        }
    }
}

Task<ClientPool::Lease> ClientPool::acquire_async() {
    auto start = std::chrono::steady_clock::now();
    auto observe = [&] {
        if (wait_hist_)
            wait_hist_->observe(std::chrono::duration<double>(
                                    std::chrono::steady_clock::now() - start)
                                    .count());
    };

    struct Acquire {
        ClientPool* pool;
        PooledClient granted;
        bool create_new = false;
        std::shared_ptr<Waiter> w;

        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk(pool->m_);
            pool->reap_stale_locked();
            if (!pool->idle_.empty()) {
                granted = std::move(pool->idle_.back().pc);
                pool->idle_.pop_back();
                return false;  // fast path: resume synchronously
            }
            if (pool->total_ < pool->cfg_.max_connections) {
                ++pool->total_;
                create_new = true;
                return false;
            }
            w = std::make_shared<Waiter>();
            w->h = h;
            w->ex = pool->resume_ex_;
            pool->waiters_.push_back(w);
            // Timeout keeps the sync contract (SlowDown after request_timeout). The
            // callback touches only the shared Waiter — no cancel bookkeeping needed;
            // release() skips entries already marked done
            auto wp = w;
            TimerQueue::instance().add(
                std::chrono::milliseconds(pool->cfg_.request_timeout_ms), [wp] {
                    std::coroutine_handle<> hh{};
                    {
                        std::lock_guard g(wp->m);
                        if (!wp->done) {
                            wp->done = true;
                            wp->timed_out = true;
                            hh = wp->h;
                        }
                    }
                    if (!hh) return;
                    if (wp->ex)
                        wp->ex->post(hh);
                    else
                        hh.resume();
                });
            return true;
        }
        void await_resume() {
            if (!w) return;
            std::lock_guard g(w->m);
            if (w->timed_out)
                throw S3Error(S3ErrorCode::SlowDown,
                              "cloudproxy: all remote connections busy, try again later");
            granted = std::move(w->granted);
            create_new = w->create_new;
        }
    };

    Acquire aw{this};
    try {
        co_await aw;
    } catch (...) {
        observe();
        throw;
    }
    if (aw.create_new) {
        try {
            aw.granted = {make_client(), std::chrono::steady_clock::now()};
        } catch (...) {
            retire_slot();
            throw;
        }
    }
    observe();
    co_return Lease(this, std::move(aw.granted));
}

void ClientPool::release(PooledClient pc) {
    auto now = std::chrono::steady_clock::now();
    // Age retirement (roadmap §3.3): drop instead of pooling; the connection closes
    // when pc goes out of scope below, outside any handoff
    if (cfg_.pool_max_lifetime_ms > 0 &&
        now - pc.created > std::chrono::milliseconds(cfg_.pool_max_lifetime_ms)) {
        pc.c.reset();  // close the socket before any waiter bookkeeping
        retire_slot();
        return;
    }
    std::unique_lock lk(m_);
    // Direct handoff to the oldest live async waiter (skipping timed-out zombies)
    while (!waiters_.empty()) {
        auto w = std::move(waiters_.front());
        waiters_.pop_front();
        std::coroutine_handle<> h{};
        {
            std::lock_guard g(w->m);
            if (w->done) continue;
            w->done = true;
            w->granted = std::move(pc);
            h = w->h;
        }
        lk.unlock();  // resume outside the pool lock
        if (w->ex)
            w->ex->post(h);
        else
            h.resume();
        return;
    }
    idle_.push_back({std::move(pc), now});
    cv_.notify_one();
}

ClientPool::Stats ClientPool::stats() {
    std::lock_guard lk(m_);
    return {total_, idle_.size()};
}

// ---------- Addressing and signing pipeline ----------

Target RemoteContext::target(const std::string& remote_bucket) const {
    if (cfg.force_path_style)
        return {"/" + util::aws_uri_encode(remote_bucket, /*encode_slash=*/false),
                ep.signed_host};
    // virtual-hosted (§7): Host = <rb>.<endpoint-host>[:port], path excludes the bucket.
    // httplib only sets Host itself when it is absent; this pipeline always carries the
    // signed Host explicitly, so no connection changes are needed
    return {"", remote_bucket + "." + ep.signed_host};
}

httplib::Headers RemoteContext::signed_headers(
    const std::string& method, const std::string& raw_path, const std::string& raw_query,
    const std::vector<std::pair<std::string, std::string>>& extra,
    const std::string& payload_hash, const std::string& host) const {
    http::HttpRequest req;
    req.method = method;
    req.raw_path = raw_path;
    req.raw_query = raw_query;
    req.headers.set("Host", host.empty() ? ep.signed_host : host);
    for (auto& [k, v] : extra) req.headers.set(k, v);
    if (cred_chain) {
        // Chain credentials (roadmap §3.3): may block briefly on a refresh — always on a
        // pool/pump thread here. The session token is set before signing so it enters
        // SignedHeaders (x-amz-* are swept in automatically)
        auto c = cred_chain->get();
        if (!c.session_token.empty()) req.headers.set("x-amz-security-token", c.session_token);
        Credential dyn{c.access_key, util::SecretString(std::move(c.secret_key))};
        auth.sign(req, dyn, payload_hash);
    } else {
        auth.sign(req, cred, payload_hash);
    }
    httplib::Headers out;
    for (auto& [k, v] : req.headers.items()) out.emplace(k, v);
    return out;
}

// ---------- Error mapping (docs/cloudproxy-backend.md §5.1) ----------

std::optional<S3ErrorCode> map_remote_code(std::string_view wire) {
    if (auto c = s3::code_from_wire(wire)) return c;
    // Near-synonym codes absent from the local vocabulary
    if (wire == "BucketAlreadyExists") return S3ErrorCode::BucketAlreadyOwnedByYou;
    if (wire == "TooManyRequests" || wire == "RequestLimitExceeded")
        return S3ErrorCode::SlowDown;
    return std::nullopt;
}

void RemoteContext::throw_remote_error(int status, const std::string& body, ErrCtx ctx,
                                       std::string_view resource) const {
    std::string remote_code, remote_msg;
    if (!body.empty()) {
        try {
            auto root = s3::xml_parse(body);
            if (root.name == "Error") {
                remote_code = root.get("Code");
                remote_msg = root.get("Message");
            }
        } catch (...) {
            // Unparsable body: fall back to the status code
        }
    }
    auto res = std::string(resource);
    // Error-mapping counters (§8.2): prefer the remote wire code; bucket by status code when unparsable
    metrics.count_error(remote_code.empty() ? "http_" + std::to_string(status) : remote_code);

    // 429/503/SlowDown -> local 503; clients may back off and retry
    if (status == 429 || status == 503 || remote_code == "SlowDown")
        throw S3Error(S3ErrorCode::SlowDown,
                      remote_msg.empty() ? "remote replied slow down" : remote_msg, res);

    // A 403 is a gateway cloud-credential/permission fault; do not pass AccessDenied
    // through and mislead clients into debugging their own credentials
    if (status == 403) {
        LOG_WARN("cloudproxy: remote returned 403 ({}) for {} — check gateway cloud "
                 "credentials",
                 remote_code.empty() ? "unparsable body" : remote_code, res);
        throw S3Error(S3ErrorCode::InternalError,
                      "remote access failure (gateway-side cloud credentials)", res);
    }

    if (status >= 400 && status < 500) {
        if (!remote_code.empty()) {
            if (auto code = map_remote_code(remote_code))
                throw S3Error(*code, remote_msg.empty() ? remote_code : remote_msg, res);
        }
        // Conditional write rejected upstream (If-None-Match/If-Match passthrough,
        // backend.h PutCondition): even with an unparsable body it must be mapped by
        // semantics, not dropped into InternalError
        if (status == 412)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          remote_msg.empty()
                              ? "At least one of the pre-conditions you specified did not hold"
                              : remote_msg,
                          res);
        if (status == 404) {
            switch (ctx) {
                case ErrCtx::Key:
                    throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist.",
                                  res);
                case ErrCtx::Bucket:
                    throw S3Error(S3ErrorCode::NoSuchBucket,
                                  "The specified bucket does not exist.", res);
                case ErrCtx::Upload:
                    throw S3Error(S3ErrorCode::NoSuchUpload,
                                  "The specified upload does not exist.", res);
                case ErrCtx::None:
                    break;
            }
        }
        // Unknown 4xx must not collapse into 500 (docs/archive/gaps.md §3.9): SDKs auto-retry 500s,
        // turning deterministic rejections like InvalidObjectState into infinite retry
        // loops. Map to a local 400 (InvalidRequest is not scrubbed by public_error), with
        // the remote code and original text carried in the message
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "remote rejected request: " + std::to_string(status) +
                          (remote_code.empty() ? "" : " " + remote_code) +
                          (remote_msg.empty() ? "" : " — " + remote_msg),
                      res);
    }

    // 5xx / everything else: local 500 (no 502 introduced; the S3 error vocabulary has no BadGateway)
    throw S3Error(S3ErrorCode::InternalError,
                  "remote returned " + std::to_string(status) +
                      (remote_code.empty() ? "" : " (" + remote_code + ")"),
                  res);
}

void RemoteContext::throw_transport_error(httplib::Error err) const {
    metrics.count_error("transport");  // §8.2: connection/DNS/timeout classes share one bucket, details go into the message
    throw S3Error(S3ErrorCode::InternalError,
                  "cloudproxy: request to " + cfg.endpoint +
                      " failed: " + httplib::to_string(err));
}

std::optional<int64_t> RemoteContext::retry_after_hint(const httplib::Result& r) {
    if (!r || (r->status != 429 && r->status != 503)) return std::nullopt;
    if (!r->has_header("Retry-After")) return std::nullopt;
    const std::string v = r->get_header_value("Retry-After");
    if (v.empty()) return std::nullopt;
    // Integer-seconds form; the alternative is an HTTP-date (RFC 9110 §10.2.3)
    if (v.find_first_not_of("0123456789") == std::string::npos) {
        try {
            return std::clamp<int64_t>(std::stoll(v), 0, 60) * 1000;
        } catch (...) {
            return std::nullopt;
        }
    }
    if (auto t = util::parse_http_date(v)) {
        auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
                         *t - std::chrono::system_clock::now())
                         .count();
        return std::clamp<int64_t>(delta, 0, 60'000);
    }
    return std::nullopt;
}

int64_t RemoteContext::backoff_delay_ms(int attempt,
                                        std::optional<int64_t> retry_after_ms) const {
    // The server's own hint wins over the formula (roadmap §3.3): it knows its
    // overload horizon; the clamp in retry_after_hint bounds a hostile value
    if (retry_after_ms) return *retry_after_ms;
    thread_local std::mt19937 rng{std::random_device{}()};
    // 64-bit arithmetic + upper clamp: extreme config values must not overflow negative and
    // feed uniform_int_distribution (UB)
    int64_t base = static_cast<int64_t>(cfg.retry_base_ms) << std::min(attempt, 10);
    base = std::clamp<int64_t>(base, 0, 60'000);
    std::uniform_int_distribution<int64_t> jitter(0, base);
    return base + jitter(rng);
}

void RemoteContext::backoff(int attempt, std::optional<int64_t> retry_after_ms) const {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(backoff_delay_ms(attempt, retry_after_ms)));
}

// ---------- Circuit breaker (roadmap §3.3) ----------

bool RemoteContext::breaker_allow() {
    if (cfg.breaker_threshold <= 0) return true;
    std::lock_guard lk(breaker_m_);
    if (consec_failures_ < cfg.breaker_threshold) return true;
    auto now = std::chrono::steady_clock::now();
    if (now < breaker_open_until_) return false;
    if (probe_inflight_) return false;  // half-open: exactly one probe decides
    probe_inflight_ = true;
    return true;
}

void RemoteContext::breaker_report(bool ok) {
    if (cfg.breaker_threshold <= 0) return;
    std::lock_guard lk(breaker_m_);
    probe_inflight_ = false;
    if (ok) {
        if (consec_failures_ >= cfg.breaker_threshold)
            LOG_INFO("cloudproxy: remote {} recovered, circuit breaker closed", cfg.endpoint);
        consec_failures_ = 0;
        return;
    }
    ++consec_failures_;
    if (consec_failures_ >= cfg.breaker_threshold) {
        breaker_open_until_ = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(cfg.breaker_cooldown_ms);
        if (consec_failures_ == cfg.breaker_threshold)
            LOG_WARN("cloudproxy: {} consecutive remote failures, circuit breaker open for "
                     "{}ms (endpoint {})",
                     consec_failures_, cfg.breaker_cooldown_ms, cfg.endpoint);
    }
}

void RemoteContext::breaker_observe(const httplib::Result& r) {
    if (!r) {
        breaker_report(false);
    } else if (r->status >= 500) {
        breaker_report(false);
    } else if (r->status != 429) {  // 429 = throttling, neither up nor down
        breaker_report(true);
    }
}

void RemoteContext::breaker_gate() {
    if (breaker_allow()) return;
    metrics.count_error("breaker_open");  // §8.2: shed load is visible per remote
    throw S3Error(S3ErrorCode::SlowDown,
                  "cloudproxy: circuit breaker open (remote " + cfg.endpoint +
                      " failing), request shed");
}

// ---------- Pagination helpers ----------

std::string group_skip_token(std::string_view prefix) {
    // 1024 = the key byte limit shared by S3 / validate_object_key: every legal key inside
    // the group is <= prefix+0xff... (1024 bytes), while any key outside the group whose
    // first divergent byte is > the corresponding prefix byte is necessarily > this value
    // -- no duplicates, no omissions
    constexpr size_t kMaxKeyBytes = 1024;
    std::string t(prefix);
    if (t.size() < kMaxKeyBytes) t.append(kMaxKeyBytes - t.size(), '\xff');
    return t;
}

}  // namespace lights3::storage::cloudproxy
