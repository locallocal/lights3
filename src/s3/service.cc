#include "s3/service.h"
#include "storage/metered_backend.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstring>
#include <random>

#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/util/hex.h"
#include "core/util/uri.h"
#include "s3/auth/credential_store.h"
#include "s3/checksum_guard.h"
#include "s3/errors.h"
#include "s3/handlers/common.h"
#include "s3/router.h"

namespace lights3::s3 {

namespace {

std::mt19937_64& id_rng() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    return rng;
}

std::string make_request_id() {
    uint64_t v = id_rng()();
    uint8_t bytes[8];
    memcpy(bytes, &v, 8);
    std::string hex = util::to_hex(std::span(bytes, 8));
    for (char& c : hex) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    return hex;
}

// x-amz-id-2 is longer than the request id (AWS uses a base64 string): hex here as well, 24 bytes
std::string make_host_id() {
    uint8_t bytes[24];
    for (size_t i = 0; i < sizeof(bytes); i += 8) {
        uint64_t v = id_rng()();
        memcpy(bytes + i, &v, 8);
    }
    return util::to_hex(std::span(bytes, sizeof(bytes)));
}

http::HttpResponse error_response(const S3Error& e, const RequestContext& ctx, bool head_only) {
    http::HttpResponse resp;
    resp.status = http_status(e.code);
    resp.headers.set("Content-Type", "application/xml");
    for (auto& [k, v] : e.headers) resp.headers.set(k, v);
    if (!head_only) resp.small_body = error_xml(e, ctx.request_id, ctx.host_id);
    return resp;
}

// Raw InternalError/SlowDown text may contain internal topology such as upstream endpoints (cloudproxy
// transport errors embed the endpoint directly): the original goes to the log only (correlated via request_id), the response body uses fixed wording
S3Error public_error(const S3Error& e, const std::string& request_id,
                     const http::HttpRequest& req) {
    if (e.code == S3ErrorCode::InternalError) {
        LOG_ERROR("req {} {} {} internal error: {}", request_id, req.method, req.path,
                  e.message);
        return S3Error(e.code, "We encountered an internal error. Please try again.");
    }
    if (e.code == S3ErrorCode::SlowDown) {
        LOG_WARN("req {} {} {} slow down: {}", request_id, req.method, req.path, e.message);
        // Fixed wording, but the throttling headers (Retry-After from the rate
        // limiter, roadmap §4.2) must survive the scrub
        S3Error scrubbed(e.code, "Please reduce your request rate.");
        scrubbed.headers = e.headers;
        return scrubbed;
    }
    return e;
}

// RAII pairing of request_start/request_end: request_end also runs when the driver destroys the request
// coroutine early (client disconnect, shutdown), so the inflight count does not leak; that path is recorded as 499 in the status distribution
struct MetricsEndGuard {
    Metrics& m;
    std::string method;
    std::chrono::steady_clock::time_point start;
    bool done = false;

    ~MetricsEndGuard() {
        if (!done) finish(499);
    }
    double finish(int status) {
        done = true;
        double secs =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        m.request_end(method, status, secs);
        return secs;
    }
};

// Access log record (roadmap §5.2, docs/s3-protocol.md §7). Filled at dispatch end;
// emitted right away for buffered responses, and at end of body for streaming ones
// (the driver pulls the body after dispatch returns, so total time and the bytes
// actually sent are only known then). Self-contained: emission may run on a driver
// thread after the service is gone
struct AccessRecord {
    std::chrono::steady_clock::time_point start;
    std::string request_id, remote, access_key, method, path, query, bucket, key, user_agent,
        api, backend;
    std::string trace_id, span_id, parent_span_id;  // roadmap §5.4
    int status = 0;
    double auth_ms = 0, handler_ms = 0, backend_ms = 0, ttfb_ms = 0;
    uint32_t backend_calls = 0;
    int64_t slow_threshold_ms = 0;
};

// Text-mode quoting: the path/UA may contain spaces (the historical unquoted path
// broke whitespace splitting), quotes and control characters
std::string quote_field(std::string_view v) {
    std::string out;
    out.reserve(v.size() + 2);
    out.push_back('"');
    for (unsigned char c : v) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
            out.push_back(static_cast<char>(c));
        } else if (c < 0x20 || c == 0x7f) {
            char buf[8];
            std::snprintf(buf, sizeof(buf), "\\x%02x", c);
            out += buf;
        } else {
            out.push_back(static_cast<char>(c));
        }
    }
    out.push_back('"');
    return out;
}

double ms_since(std::chrono::steady_clock::time_point t0, std::chrono::steady_clock::time_point t1) {
    return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

// One line per request. INFO normally; WARN with the per-stage breakdown once the
// total reaches log.slow_request_threshold, so a production deployment running at
// warn still sees the requests worth looking at. Text: fixed leading fields (aligned
// with the pre-§5.2 line, path now quoted) plus key=value slots. JSON: a flat object
// spliced into the JSON envelope by the formatter (core/log.cc)
void emit_access(const AccessRecord& r, uint64_t bytes, bool truncated) {
    double total_ms = ms_since(r.start, std::chrono::steady_clock::now());
    bool slow = r.slow_threshold_ms > 0 && total_ms >= static_cast<double>(r.slow_threshold_ms);
    auto level = slow ? spdlog::level::warn : spdlog::level::info;
    auto& log = Logger::access();
    if (!log.should_log(level)) return;
    auto dash = [](const std::string& v) -> const std::string& {
        static const std::string kDash = "-";
        return v.empty() ? kDash : v;
    };
    if (Logger::json()) {
        auto ms3 = [](double v) { return std::round(v * 1000.0) / 1000.0; };  // 3 decimals: no float noise
        nlohmann::json j;
        j["request_id"] = r.request_id;
        if (!r.remote.empty()) j["remote"] = r.remote;
        if (!r.access_key.empty()) j["ak"] = r.access_key;
        j["method"] = r.method;
        j["path"] = r.path;
        if (!r.query.empty()) j["query"] = r.query;
        if (!r.bucket.empty()) j["bucket"] = r.bucket;
        if (!r.key.empty()) j["key"] = r.key;
        j["status"] = r.status;
        j["bytes"] = bytes;
        j["ms"] = ms3(total_ms);
        j["ttfb_ms"] = ms3(r.ttfb_ms);
        j["auth_ms"] = ms3(r.auth_ms);
        j["handler_ms"] = ms3(r.handler_ms);
        j["backend_ms"] = ms3(r.backend_ms);
        j["backend_calls"] = r.backend_calls;
        j["api"] = dash(r.api);
        j["backend"] = dash(r.backend);
        if (!r.user_agent.empty()) j["ua"] = r.user_agent;
        j["trace_id"] = r.trace_id;
        j["span_id"] = r.span_id;
        if (!r.parent_span_id.empty()) j["parent_span_id"] = r.parent_span_id;
        if (slow) j["slow"] = true;
        if (truncated) j["truncated"] = true;
        log.log(level, "{}", j.dump());
        return;
    }
    std::string line = spdlog::fmt_lib::format(
        "access {} {} {} {} {} {} {}ms api={} backend={}:{:.1f}ms remote={} bucket={} ttfb={:.1f}ms ua={} trace={}/{}",
        r.request_id, dash(r.access_key), r.method, quote_field(r.path), r.status, bytes,
        static_cast<uint64_t>(total_ms), dash(r.api), dash(r.backend), r.backend_ms,
        dash(r.remote), dash(r.bucket), r.ttfb_ms, quote_field(r.user_agent), r.trace_id,
        r.span_id);
    if (!r.parent_span_id.empty()) line += " parent=" + r.parent_span_id;
    if (truncated) line += " truncated=1";
    if (slow)
        line += spdlog::fmt_lib::format(" slow=1 auth={:.1f}ms handler={:.1f}ms backend_calls={}", r.auth_ms,
                            r.handler_ms, r.backend_calls);
    log.log(level, "{}", line);
}

// Byte-counting decorators (docs/archive/gaps.md §7): inbound wraps outside the checksum/de-framing decorators
// (counting the payload bytes the handler actually consumes); outbound wraps outside stream_body (counting the
// bytes the driver actually pulls -- streaming responses are written after dispatch returns, and only a decorator can see them).
// The outbound side also carries the access record (roadmap §5.2): emitted at EOF, or from the destructor
// when the driver stopped pulling early (client gone, backend read failure, HEAD) -- then flagged truncated
// whenever fewer bytes than announced went out
class CountingBodyReader final : public http::BodyReader {
public:
    CountingBodyReader(std::unique_ptr<http::BodyReader> inner, Metrics* m, std::string bucket,
                       bool inbound, std::unique_ptr<AccessRecord> access = nullptr)
        : inner_(std::move(inner)),
          m_(m),
          bucket_(std::move(bucket)),
          inbound_(inbound),
          access_(std::move(access)) {}

    ~CountingBodyReader() override {
        if (!access_) return;
        auto len = inner_->length();
        bool truncated = len ? total_ < *len : !eof_;
        if (access_->method == "HEAD") truncated = false;
        emit_access(*access_, total_, truncated);
    }

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_->read(buf);
        if (inbound_) m_->add_bytes_in(bucket_, n);
        else m_->add_bytes_out(bucket_, n);
        total_ += n;
        if (n == 0) eof_ = true;
        if (access_ && (eof_ || (inner_->length() && total_ >= *inner_->length()))) {
            emit_access(*access_, total_, /*truncated=*/false);
            access_.reset();
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    Metrics* m_;
    std::string bucket_;
    bool inbound_;
    std::unique_ptr<AccessRecord> access_;
    uint64_t total_ = 0;
    bool eof_ = false;
};

// Explicitly unsupported subresources (docs/s3-protocol.md §1): explicit 501, avoiding wrong answers from falling into the List/Get fallback
constexpr std::string_view kUnsupportedSubresources[] = {
    "acl",         "policy",       "versioning",     "versions",
    "encryption",  "object-lock",
    "legal-hold",  "retention",    "torrent",        "replication",    "logging",
    "notification", "requestPayment", "accelerate",  "analytics",      "inventory",
    "intelligent-tiering", "metrics", "ownershipControls", "publicAccessBlock",
    "restore",     "select",       "policyStatus",   "versionId",
};

void reject_unsupported_subresource(const http::HttpRequest& req) {
    for (auto& sub : kUnsupportedSubresources)
        if (req.query_has(sub))
            throw S3Error(S3ErrorCode::NotImplemented,
                          "The requested sub-resource '" + std::string(sub) +
                              "' is not implemented.");
}

// Explicitly unsupported **request headers** (docs/archive/gaps.md §3.4): SSE/SSE-C, tagging, object-lock, and
// ACL-grant classes used to be silently swallowed -- 200 with the semantics unfulfilled; in compliance scenarios
// clients would conclude the object is encrypted/locked. A hit is 501; x-amz-acl alone admits private (this implementation's actual semantics)
void reject_unsupported_headers(const http::HttpRequest& req) {
    constexpr std::string_view kPrefixes[] = {
        "x-amz-server-side-encryption",  // the whole SSE and SSE-C family (including -customer-*, -aws-kms-*)
        "x-amz-copy-source-server-side-encryption",  // the three SSE-C headers on the copy source side
        "x-amz-object-lock-",                        // mode / retain-until-date / legal-hold
        "x-amz-grant-",                              // the five ACL grant headers, same class as x-amz-acl
    };
    // x-amz-website-redirect-location left this list with docs/static-website.md phase ③,
    // x-amz-tagging with roadmap §2.5: both are first-class metadata fields now
    for (auto& [k, v] : req.headers.items()) {
        std::string lk;
        lk.reserve(k.size());
        for (char c : k) lk.push_back(http::HeaderMap::lower(c));
        auto refuse = [&] {
            throw S3Error(S3ErrorCode::NotImplemented,
                          "The request header '" + lk + "' is not implemented.");
        };
        if (lk == "x-amz-acl") {
            // private = this implementation's only semantics, accepted; silently accepting the rest
            // (public-read etc.) would falsely claim "publicly granted"
            if (!http::HeaderMap::ieq(v, "private")) refuse();
            continue;
        }
        if (lk == "x-amz-storage-class") {
            // Likewise (docs/archive/gaps.md §5.2): STANDARD is the only storage class; accepting GLACIER and echoing
            // it back would lie on behalf of the storage layer -- the object never entered any archive tier
            if (!http::HeaderMap::ieq(v, "STANDARD")) refuse();
            continue;
        }
        for (auto p : kPrefixes)
            if (lk.rfind(p, 0) == 0) refuse();
    }
}

// Query allowlist (docs/archive/gaps.md §3.5): keys permitted on all routes -- the presigned signature parameter
// family + SDK tracing parameters. Keys are case-sensitive (consistent with the SigV4 canonical query).
// X-Amz-Security-Token joined the list with real STS support (roadmap §2.6): presigned
// URLs minted from session credentials carry it, and verify validates it
constexpr std::string_view kCommonQueryKeys[] = {
    "X-Amz-Algorithm",     "X-Amz-Credential", "X-Amz-Date",
    "X-Amz-Expires",       "X-Amz-Signature",  "X-Amz-SignedHeaders",
    "X-Amz-Content-Sha256", "X-Amz-Security-Token",
    "x-id",  // tracing parameter aws-sdk-js v3 attaches to every operation, no semantics
};

bool word_in(std::string_view list, std::string_view w) {
    size_t pos = 0;
    while (pos <= list.size()) {
        size_t sp = list.find(' ', pos);
        if (sp == std::string_view::npos) sp = list.size();
        if (list.substr(pos, sp - pos) == w) return true;
        pos = sp + 1;
    }
    return false;
}

void enforce_query_whitelist(const http::HttpRequest& req, const S3Service::Route& r) {
    std::string_view flag_key = r.flag.substr(0, r.flag.find('='));
    for (auto& [k, v] : req.query) {
        if (!flag_key.empty() && k == flag_key) continue;
        if (word_in(r.extra_query, k)) continue;
        bool common = false;
        for (auto c : kCommonQueryKeys)
            if (k == c) {
                common = true;
                break;
            }
        if (common) continue;
        throw S3Error(S3ErrorCode::NotImplemented,
                      "The query parameter '" + k + "' is not implemented.");
    }
}

}  // namespace

// ---------- virtual-host style（docs/s3-protocol.md §2）----------

S3Service::Address S3Service::resolve_address(const http::HttpRequest& req) const {
    if (!base_domain_.empty()) {
        if (auto host = req.headers.get("Host")) {
            std::string h = *host;
            // Domain names are case-insensitive (RFC 4343): without normalization, Host: B.GW.EXAMPLE.COM
            // silently degrades to path-style, the same URL in two cases points at different resources, and the
            // bucket input to policy decisions is client-controlled
            for (char& c : h)
                if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
            // Strip the port: Host may be "name:port" or "[v6]:port", and ':' inside an IPv6 literal is not a
            // port separator (rfind would truncate "[::1]" to "[:")
            if (!h.empty() && h.front() == '[') {
                if (auto rb = h.find(']'); rb != std::string::npos) h.resize(rb + 1);
            } else if (auto colon = h.rfind(':'); colon != std::string::npos) {
                h.resize(colon);
            }
            std::string suffix = "." + base_domain_;
            if (h.size() > suffix.size() && h.ends_with(suffix)) {
                std::string bucket = h.substr(0, h.size() - suffix.size());
                std::string key = req.path;
                if (!key.empty() && key.front() == '/') key.erase(0, 1);
                return {std::move(bucket), std::move(key), /*vhost=*/true};
            }
        }
    }
    auto [bucket, key] = parse_bucket_key(req.path);
    return {std::move(bucket), std::move(key), /*vhost=*/false};
}

// ---------- Static website hosting (docs/static-website.md) ----------

void S3Service::set_website_buckets(std::vector<WebsiteBucket> buckets) {
    // Same gate as user requests (allow_reserved defaults to false): an entry for .sys or
    // a malformed name is a config error and must fail at startup, not lie dormant
    for (auto& b : buckets) storage::validate_bucket_name(b.bucket);
    website_store_ = WebsiteStore::make_static(std::move(buckets));
}

namespace {
// The built-in error page embeds the S3Error message, and some messages quote request
// input (query parameter names, keys) — unescaped they would be an XSS surface on the
// bucket's own origin
std::string html_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            default: out += c;
        }
    }
    return out;
}
}  // namespace

// ---- Website redirects (roadmap §2.3: RedirectAllRequestsTo / RoutingRules / slash) ----

namespace {

// Scheme can only be relayed by a reverse proxy (same reasoning as request_base_url in
// multipart.cc): direct connections are plaintext HTTP here
std::string req_scheme(const http::HttpRequest& req) {
    if (auto p = req.headers.get("X-Forwarded-Proto"); p && !p->empty()) return *p;
    return "http";
}

http::HttpResponse redirect_response(int status, std::string location) {
    http::HttpResponse r;
    r.status = status;
    r.headers.set("Location", std::move(location));
    return r;
}

// Location for a website redirect target. Empty host+protocol stays on this gateway as
// a relative path (bucket prefix added under path-style addressing); anything else
// becomes an absolute URL. The key is percent-encoded — it flows into a header
std::string website_location(const http::HttpRequest& req, const std::string& bucket,
                             bool vhost, const std::string& protocol, const std::string& host,
                             const std::string& key) {
    std::string path = "/" + util::aws_uri_encode(key, /*encode_slash=*/false);
    if (host.empty() && protocol.empty()) {
        return vhost ? path : "/" + bucket + path;
    }
    std::string h = host.empty() ? req.headers.get("Host").value_or("") : host;
    std::string p = protocol.empty() ? req_scheme(req) : protocol;
    // Redirecting back to this gateway keeps its own addressing style; an external
    // host is addressed at its root
    if (host.empty() && !vhost) path = "/" + bucket + path;
    return p + "://" + h + path;
}

// error_status == 0 selects the pre-request phase (rules without an error-code
// condition); a non-zero status selects error-phase rules with that exact code.
// First match wins (AWS evaluates rules in order)
const WebsiteRoutingRule* match_routing_rule(const std::vector<WebsiteRoutingRule>& rules,
                                             const std::string& key, int error_status) {
    for (auto& r : rules) {
        if (r.http_error_code_equals != error_status) continue;
        if (!r.key_prefix_equals.empty() && key.rfind(r.key_prefix_equals, 0) != 0) continue;
        return &r;
    }
    return nullptr;
}

http::HttpResponse routing_redirect(const http::HttpRequest& req,
                                    const WebsiteRoutingRule& rule, const std::string& key,
                                    const std::string& bucket, bool vhost) {
    std::string new_key = key;
    if (rule.replace_key_with) {
        new_key = *rule.replace_key_with;
    } else if (rule.replace_key_prefix_with) {
        // substr is safe: the rule matched, so key starts with the condition prefix
        new_key = *rule.replace_key_prefix_with + key.substr(rule.key_prefix_equals.size());
    }
    return redirect_response(
        rule.http_redirect_code,
        website_location(req, bucket, vhost, rule.protocol, rule.host_name, new_key));
}

// Pre-request redirects, evaluated on the ORIGINAL key before the index rewrite:
// RedirectAllRequestsTo first (exclusive with everything else by construction), then
// prefix-only routing rules. nullopt = proceed with the normal read
std::optional<http::HttpResponse> website_redirect_response(const http::HttpRequest& req,
                                                            const WebsiteBucket& site,
                                                            const std::string& key,
                                                            const std::string& bucket,
                                                            bool vhost) {
    if (!site.redirect_all_host.empty())
        return redirect_response(301,
                                 website_location(req, bucket, vhost, site.redirect_all_protocol,
                                                  site.redirect_all_host, key));
    if (const auto* r = match_routing_rule(site.routing_rules, key, 0))
        return routing_redirect(req, *r, key, bucket, vhost);
    return std::nullopt;
}

}  // namespace

bool S3Service::website_rate_admit(const std::string& bucket, uint32_t rps) {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lk(rate_mu_);
    auto& b = rate_[bucket];
    if (b.last.time_since_epoch().count() == 0) {
        b.tokens = rps;  // fresh bucket starts full (burst = rps)
    } else {
        b.tokens = std::min<double>(
            rps, b.tokens + std::chrono::duration<double>(now - b.last).count() * rps);
    }
    b.last = now;
    if (b.tokens < 1.0) return false;
    b.tokens -= 1.0;
    return true;
}

// Anonymous website errors keep the ORIGINAL status code while serving the configured
// error object as the body (a 404 that answered 200 would poison caches and mislead
// crawlers). A missing/unreadable error object falls back to the built-in page — the
// site owner's misconfiguration must not turn a 404 into a 500 — and the error object
// is read directly from the backend, so this never re-enters dispatch (no recursion)
Task<http::HttpResponse> S3Service::website_error_page(const S3Error& e,
                                                       const WebsiteBucket& site,
                                                       bool head_only) {
    http::HttpResponse resp;
    resp.status = http_status(e.code);
    for (auto& [k, v] : e.headers) resp.headers.set(k, v);  // e.g. Allow on 405
    if (!site.error_key.empty()) {
        try {
            auto& backend = router_.resolve(site.bucket);
            if (head_only) {
                auto meta = co_await backend.head_object(site.bucket, site.error_key);
                resp.headers.set("Content-Type", meta.content_type);
                resp.content_length = meta.size;
            } else {
                auto stream =
                    co_await backend.get_object(site.bucket, site.error_key, std::nullopt);
                resp.headers.set("Content-Type", stream.meta.content_type);
                resp.content_length = stream.meta.size;
                resp.stream_body = std::move(stream.body);
            }
            co_return resp;
        } catch (const std::exception& err) {
            LOG_WARN("website: error document {}/{} unreadable ({}), serving built-in page",
                     site.bucket, site.error_key, err.what());
        }
    }
    resp.headers.set("Content-Type", "text/html; charset=utf-8");
    if (!head_only) {
        std::string title = std::to_string(resp.status) + " " + wire_code(e.code);
        resp.small_body = "<!DOCTYPE html><html><head><title>" + title +
                          "</title></head><body><h1>" + title + "</h1><p>" +
                          html_escape(e.message) + "</p></body></html>\n";
    }
    co_return resp;
}

bool S3Service::anonymous_website_read(const http::HttpRequest& req, const Address& addr,
                                       const WebsiteStore::Snapshot& snap) const {
    if (!snap || snap->empty()) return false;
    if (req.method != "GET" && req.method != "HEAD") return false;
    // Any signature material at all — Authorization header or any presigned query
    // parameter, even a partial/malformed set — always goes through verification: a bad
    // signature must stay an auth error, not silently degrade into an anonymous success
    // (which would both mask client misconfiguration and let expired links "work")
    if (req.headers.has("Authorization")) return false;
    if (req.query_has("X-Amz-Algorithm") || req.query_has("X-Amz-Signature") ||
        req.query_has("X-Amz-Credential"))
        return false;
    // Service scope stays out (ListBuckets must never be anonymous); an empty key is
    // admitted — the index rewrite in dispatch resolves it to an object read
    if (addr.bucket.empty()) return false;
    return WebsiteStore::find(snap, addr.bucket) != nullptr;
}

// ---------- Top-level entry ----------

Task<http::HttpResponse> S3Service::dispatch(http::HttpRequest req) {
    RequestContext ctx{make_request_id(), make_host_id(), req.cancel,
                       TraceContext::from_headers(req.headers.get("traceparent"),
                                                  req.headers.get("tracestate"))};
    bool head = req.method == "HEAD";
    auto start = std::chrono::steady_clock::now();
    metrics_.request_start();
    MetricsEndGuard mguard{metrics_, req.method, start};
    // Stage marks for the access/slow-request line (roadmap §5.2): auth = up to the
    // verified identity, handler = the route() call (backend time is a subset of it,
    // accumulated separately). Requests short-circuited before either stage keep 0
    std::chrono::steady_clock::time_point auth_done = start;
    std::optional<std::chrono::steady_clock::time_point> route_start, route_end;

    std::string access_key;
    std::string bucket, key;
    http::HttpResponse resp;
    // Set when the request took the anonymous website path: its errors render as the
    // site's error page instead of XML. The page fetch itself must happen after the
    // catch blocks — co_await is not allowed inside a coroutine's catch handler.
    // web_snap keeps anon_site's pointee alive: a concurrent ?website PUT swaps the
    // store's snapshot mid-request, but never mutates a published one
    WebsiteStore::Snapshot web_snap;
    const WebsiteBucket* anon_site = nullptr;
    std::optional<S3Error> website_err;
    // Original (pre-index-rewrite) key + addressing style, kept for the redirect
    // evaluation in the error path below (addr itself lives inside the try block)
    std::string anon_orig_key;
    bool vhost = false;
    std::string tenant_for_log;  // actor's tenant for the audit record (empty = none)
    // API x backend dimension (roadmap §5.1): the route name becomes the api label,
    // the routed backend the backend label; the accumulator travels on the
    // request's cancellation token and collects the backend share of the latency
    std::string_view api_name;
    std::string backend_name;
    auto backend_stats = std::make_shared<storage::RequestBackendStats>();
    backend_stats->trace = ctx.trace;  // outbound hops forward it as traceparent (roadmap §5.4)
    // Rate-limit slots (roadmap §4.2) held for the whole dispatch; released on return
    // The limiter instances are pinned here so a hot-reload swap cannot destroy
    // one while this request still holds a slot in it (declared before the slots:
    // the slots release first at scope exit)
    std::shared_ptr<RateLimiter> ip_lim = ip_limiter_.load(), ak_lim = ak_limiter_.load();
    std::optional<RateLimiter::Token> ip_slot, ak_slot;
    auto throttle = [&](bool by_ak) {
        metrics_.ratelimit_rejected(by_ak);
        throw S3Error(S3ErrorCode::SlowDown,
                      by_ak ? "Request rate limit exceeded for this access key."
                            : "Request rate limit exceeded for this client address.")
            .with_header("Retry-After", "1");
    };
    try {
        // Resolve addressing before steering to internal endpoints (docs/archive/gaps.md §3.8): under vhost, req.path is
        // the key, and "/-/metrics" may be a legitimate object in mybucket -- exact path comparison would turn a
        // GET into anonymous metrics and a PUT into "200 but the object was never written" silent data loss.
        // Only the /-/ prefix under path addressing (non-vhost) enters the internal branch
        auto addr = resolve_address(req);
        vhost = addr.vhost;
        bool internal = !addr.vhost && req.path.rfind("/-/", 0) == 0;
        // Per-IP limit before anything costly (signature verification, backend
        // access); the internal read endpoints (health/metrics probes) stay exempt
        bool probe = internal && (req.path == "/-/healthz" || req.path == "/-/readyz" ||
                                  req.path == "/-/metrics");
        if (ip_lim && !probe) {
            ip_slot = ip_lim->admit(req.remote_addr);
            if (!ip_slot) throttle(false);
        }
        // Read endpoints accept only GET/HEAD (probes commonly use HEAD); previously PUT /-/metrics also returned 200
        auto internal_get = [&](std::string_view ep) {
            if (req.path != ep) return false;
            if (req.method != "GET" && req.method != "HEAD")
                throw S3Error(S3ErrorCode::MethodNotAllowed,
                              "The specified method is not allowed against this resource.");
            return true;
        };
        if (internal && internal_get("/-/healthz")) {
            api_name = "healthz";
            resp.small_body = "ok\n";
            resp.headers.set("Content-Type", "text/plain");
        } else if (internal && internal_get("/-/metrics")) {
            api_name = "metrics";
            // Root gate (roadmap §5.3): the same credential class as the admin plane
            if (metrics_root_.load(std::memory_order_relaxed) && auth_.enabled()) {
                auto ident = auth_.verify(req);
                access_key = ident.access_key;
                if (!is_root(access_key))
                    throw S3Error(S3ErrorCode::AccessDenied,
                                  "Reading metrics requires a root (statically configured) "
                                  "credential on this deployment (http.metrics_access: root).");
            }
            resp.small_body =
                metrics_.render(pool_stats_, admission_stats_, timer_stats_, conn_stats_);
            // The backend-level registry is appended after the L2 request metrics
            if (backend_metrics_) resp.small_body += backend_metrics_->render();
            resp.headers.set("Content-Type", "text/plain; version=0.0.4");
        } else if (internal && internal_get("/-/readyz")) {
            api_name = "readyz";
            resp = co_await readyz();
        } else if (internal && (req.path == "/-/admin/credentials" ||
                                req.path.rfind("/-/admin/credentials/", 0) == 0)) {
            // The boundary must land on '/': bare prefix matching would let /-/admin/credentialsXYZ into the admin plane too
            api_name = "AdminCredentials";
            resp = co_await admin_credentials(req, access_key);
        } else if (internal && req.path == "/-/admin/config/reload") {
            // Config hot reload (roadmap §4.4, docs/config-reload.md)
            api_name = "AdminConfigReload";
            resp = co_await admin_config_reload(req, access_key, ctx);
        } else if (internal && (req.path == "/-/admin/tenants" || req.path == "/-/admin/usage" ||
                                req.path.rfind("/-/admin/tenants/", 0) == 0 ||
                                req.path.rfind("/-/admin/usage/", 0) == 0)) {
            // Tenancy + usage admin plane (docs/multi-tenancy.md §6), same JSON conventions
            api_name = "AdminTenancy";
            resp = co_await admin_tenancy(req, access_key, ctx);
        } else if (!addr.vhost && req.path == "/" && req.method == "POST") {
            // STS AssumeRole (roadmap §2.6): SDKs pointed at this gateway as their STS
            // endpoint POST a form body to the service root. Path-style only — under
            // vhost addressing "/" is a bucket root and stays on the S3 plane. The
            // handler does its own verification (service scope "sts") and renders
            // errors in the STS XML shape
            api_name = "AssumeRole";
            resp = co_await sts_endpoint(req, ctx, access_key);
        } else if (req.method == "OPTIONS") {
            // CORS preflight (roadmap §2.1): decided before signature verification —
            // browsers attach no signature material to preflights; the preflighted
            // request itself is verified as usual when it arrives. The handler answers
            // purely from the rule table (no object access), so nothing is disclosed
            // beyond "this origin/method pair is allowed"
            bucket = addr.bucket;
            if (!bucket.empty()) storage::validate_bucket_name(bucket);
            api_name = "Preflight";
            resp = co_await cors_preflight(req, bucket);
        } else {
            // Static website hosting phase 1 (docs/static-website.md): requests with no
            // signature material may read objects from explicitly listed website buckets
            // anonymously. Decided before verify -- verify treats a missing Authorization
            // header as AccessDenied; when auth is globally disabled verify() admits
            // everything anyway and the anonymous branch changes nothing (the synthesized
            // read-only policy would only be stricter than "unrestricted")
            if (website_store_) web_snap = website_store_->snapshot();
            bool anon = auth_.enabled() && anonymous_website_read(req, addr, web_snap);
            // Authorization uses the verify-time policy snapshot (docs/archive/gaps.md §3.7): with a second store lookup
            // after verification, the policy would vanish entirely in the race window where sync/remove deletes
            // the credential -- a readonly credential becomes unrestricted within the window. The snapshot makes
            // in-flight requests complete strictly with verify-time semantics
            auto ident = anon ? VerifiedIdentity{} : auth_.verify(req);
            access_key = ident.access_key;
            auth_done = std::chrono::steady_clock::now();
            // Per-access-key limit (after verification: the key is authenticated, so a
            // forged header cannot burn another tenant's budget)
            if (ak_lim && !access_key.empty()) {
                ak_slot = ak_lim->admit(access_key);
                if (!ak_slot) throttle(true);
            }
            // Content-MD5 / x-amz-checksum-* (docs/archive/gaps.md §5.6): installed after verify, hence wrapping outside
            // the sha256/aws-chunked decorators -- digests are computed over the de-framed plaintext, the same
            // bytes the client computed over. Independent of the signature; also effective with auth disabled
            install_checksum_guard(req);
            bucket = std::move(addr.bucket);
            key = std::move(addr.key);
            // Inbound byte counting (docs/archive/gaps.md §7): bucket already resolved, decorated at the outermost layer
            if (req.body)
                req.body = std::make_unique<CountingBodyReader>(std::move(req.body), &metrics_,
                                                                bucket, /*inbound=*/true);
            // User-requested bucket names pass full validation here, the **single** authoritative gate
            // (docs/archive/gaps.md §1.1). Previously only the first character was checked for '.', while under vhost
            // addressing the bucket comes entirely from the Host header and may contain '/' or even start with
            // '/', which combined with localfs's root_/bucket/key concatenation (fs::path replaces the whole path
            // on an absolute component) means arbitrary file reads; on the path-style side, %00 could turn the
            // first character into NUL to bypass the reserved-name check. validate_bucket_name's character-set
            // rules close both entrances at once, and reserved names (.sys) are only available to callers with
            // allow_reserved=true -- user requests never get that parameter
            if (!bucket.empty()) storage::validate_bucket_name(bucket);
            {
                Scope scope = bucket.empty() ? Scope::Service
                              : key.empty()  ? Scope::Bucket
                                             : Scope::Object;
                if (const Route* r = match_route(req, scope)) {
                    api_name = r->name;
                    bool copy = req.headers.has("x-amz-copy-source");
                    if (copy && r->name == "PutObject") api_name = "CopyObject";
                    if (copy && r->name == "UploadPart") api_name = "UploadPartCopy";
                }
                backend_name = bucket.empty() ? std::string() : router_.backend_name(bucket);
            }
            // Set when the anonymous plane answered with a redirect before any object
            // access (RedirectAllRequestsTo / prefix RoutingRules, roadmap §2.3):
            // routing and policy are skipped entirely
            std::optional<http::HttpResponse> early;
            if (anon) {
                const WebsiteBucket* site = WebsiteStore::find(web_snap, bucket);
                // Per-bucket anonymous rate limit (roadmap §2.3): decided before
                // anon_site is set, so the rejection stays a cheap XML 503 — serving
                // the error document would spend the very backend read the limiter
                // exists to protect
                if (site->max_rps && !website_rate_admit(bucket, site->max_rps)) {
                    metrics_.website(WebsiteEvent::Throttled);
                    throw S3Error(S3ErrorCode::SlowDown, "Please reduce your request rate.");
                }
                metrics_.website(WebsiteEvent::AnonRead);
                anon_site = site;
                anon_orig_key = key;
                // RedirectAllRequestsTo + prefix-only RoutingRules: evaluated on the
                // original key, before the index rewrite and before any object access
                if (auto redirect = website_redirect_response(req, *site, key, bucket, vhost)) {
                    metrics_.website(WebsiteEvent::Redirect);
                    early = std::move(*redirect);
                } else {
                // Index document (docs/static-website.md phase ②): an empty key (bucket
                // root, with or without trailing slash) or a directory-style key
                // ("docs/") maps to the index object. Rewriting before the route gate
                // also turns what would be a bucket-scope listing into a plain object
                // read -- anonymous listing stays impossible by construction
                if (key.empty() || key.back() == '/') {
                    key += anon_site->index_suffix;
                    metrics_.website(WebsiteEvent::IndexRewrite);
                }
                // Anonymous scope is pinned by route, not just policy: only the bare
                // GET/HEAD object routes (flag == "", Action::Read) qualify -- a query
                // flag steers to a different operation (?uploadId is ListParts), and
                // those stay authenticated-only along with all listing
                const Route* r = match_route(req, Scope::Object);
                if (!r || !r->flag.empty() || r->action != Action::Read)
                    throw S3Error(S3ErrorCode::AccessDenied,
                                  "Anonymous access is limited to object reads.");
                // response-* overrides are refused for anonymous requests (AWS does the
                // same): on a public bucket a crafted link could otherwise hang an
                // arbitrary Content-Disposition off the bucket's domain (objects.cc §5.3)
                if (handlers::has_response_override(req))
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "Request specific response headers cannot be used for "
                                  "anonymous requests.");
                // Defense in depth: the standard policy block below re-checks bucket/key
                // through the same allows() path every credential goes through
                CredentialPolicy p;
                p.buckets = {bucket};
                p.readonly = true;
                ident.policy = std::move(p);
                }
            }
            if (early) {
                resp = std::move(*early);
            } else {
            // per-credential policy (docs/credential-management.md §10.4): the action comes from the matched
            // route, not the HTTP method (docs/archive/gaps.md §5.10) -- DeleteObjects is a POST yet a delete,
            // CreateMultipartUpload is also a POST yet a write; the method dimension cannot separate the two.
            // The decision input is the snapshot verify returned, never a store lookup (§3.7)
            RequestAuth auth{access_key, ident.policy ? &*ident.policy : nullptr, ident.tenant,
                             ident.tenant_admin, ctx.request_id};
            tenant_for_log = ident.tenant;
            if (ident.policy) {
                auto deny = [] {
                    throw S3Error(S3ErrorCode::AccessDenied,
                                  "Access denied by credential policy.");
                };
                Scope scope = bucket.empty() ? Scope::Service
                              : key.empty()  ? Scope::Bucket
                                             : Scope::Object;
                const Route* r = match_route(req, scope);
                // No matched route means no action to decide on: leave it to route() to return 405;
                // unsupported methods are not a privilege-escalation surface anyway
                if (r) {
                    if (!ident.policy->allows(bucket, key, r->action)) deny();
                    // CopyObject / UploadPartCopy carry the source in a header, bypassing the check above:
                    // do a separate read authorization for the source bucket+key, so policy credentials cannot use copy to read data outside the allowlist
                    if (auto src = req.headers.get("x-amz-copy-source")) {
                        auto [sb, sk] = handlers::parse_copy_source(*src);
                        if (!ident.policy->allows(sb, sk, Action::Read)) deny();
                    }
                }
            }
            // Tenant ownership (docs/multi-tenancy.md §4.3): a tenant credential is
            // confined to the buckets its tenant owns, on top of its policy. Service
            // scope (ListBuckets) filters in the handler instead. Decided on the
            // verify-time snapshot like the policy; the owner table is a snapshot too
            if (!ident.tenant.empty() && tenants_ && !bucket.empty()) {
                Scope scope = key.empty() ? Scope::Bucket : Scope::Object;
                const Route* r = match_route(req, scope);
                if (r) {
                    bool creating = scope == Scope::Bucket && req.method == "PUT" && r->flag.empty();
                    co_await require_tenant_bucket(bucket, ident.tenant, creating);
                    if (auto src = req.headers.get("x-amz-copy-source")) {
                        auto [sb, sk] = handlers::parse_copy_source(*src);
                        co_await require_tenant_bucket(sb, ident.tenant, false);
                    }
                }
            }
            // Per-request timeout + cancellation wiring (docs/archive/gaps.md §3.1/§3.3): req_src is dedicated to this
            // request; external tokens (process shutdown, plus client disconnect once the driver is wired) attach
            // to the same source -- any trigger converges the whole L2/L3 chain with OperationCancelled from the
            // nearest cancellable suspension point (pool.schedule / semaphore.acquire). The token propagates down
            // the Task promise automatically, no per-handler/backend signature changes needed
            CancelSource req_src;
            req_src.set_data(backend_stats);  // reachable from the metered backends (roadmap §5.1)
            CancelRegistration link;
            if (ctx.cancel.valid()) {
                link = ctx.cancel.on_cancel([&req_src] { req_src.request_cancel(); });
                if (ctx.cancel.cancelled()) req_src.request_cancel();
            }
            std::chrono::milliseconds request_timeout(
                request_timeout_ms_.load(std::memory_order_relaxed));
            route_start = std::chrono::steady_clock::now();
            if (request_timeout.count() > 0)
                resp = co_await with_timeout(route(req, bucket, key, auth), request_timeout, req_src);
            else
                resp = co_await std::move(route(req, bucket, key, auth).with_cancel(req_src.token()));
            route_end = std::chrono::steady_clock::now();
            // Object-level website redirect (docs/static-website.md phase ③): on the
            // anonymous plane, x-amz-website-redirect-location turns the response into a
            // 301 — the header value was prefix-validated at PUT, so it is Location-safe.
            // Signed (REST) requests keep the object body + echo header, matching AWS
            if (anon_site && (resp.status == 200 || resp.status == 206)) {
                if (auto loc = resp.headers.get("x-amz-website-redirect-location")) {
                    http::HttpResponse redirect;
                    redirect.status = 301;
                    redirect.headers.set("Location", *loc);
                    resp = std::move(redirect);
                    metrics_.website(WebsiteEvent::Redirect);
                }
            }
            }  // !early
        }
    } catch (const OperationCancelled&) {
        // Timeout/disconnect/shutdown: 503 lets SDKs retry. Blocking syscalls already running on pool threads are
        // not preempted; this response only means "the gateway stops waiting for it" (the cooperative semantics of docs/concurrency.md §5)
        LOG_WARN("req {} {} {} cancelled (timeout or shutdown) trace={}", ctx.request_id,
                 req.method, req.path, ctx.trace.trace_id);
        metrics_.s3_error(S3ErrorCode::SlowDown);
        resp = error_response(
            S3Error(S3ErrorCode::SlowDown, "Request cancelled: timed out or server shutting down."),
            ctx, head);
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        if (anon_site)
            website_err = public_error(e, ctx.request_id, req);
        else
            resp = error_response(public_error(e, ctx.request_id, req), ctx, head);
    } catch (const std::exception& e) {
        LOG_ERROR("req {} {} {} internal error: {} trace={}", ctx.request_id, req.method,
                  req.path, e.what(), ctx.trace.trace_id);
        metrics_.s3_error(S3ErrorCode::InternalError);
        S3Error internal(S3ErrorCode::InternalError, "We encountered an internal error.");
        if (anon_site)
            website_err = std::move(internal);
        else
            resp = error_response(internal, ctx, head);
    }
    // Anonymous website errors render as the site's error page (original status kept,
    // XML only for signed requests). Fetched here because catch blocks cannot co_await;
    // the cancelled path above intentionally stays XML -- 503/SlowDown is retry signaling
    if (website_err) {
        // Error-phase RoutingRules first (roadmap §2.3): explicit configuration wins
        // over both the slash redirect and the error document
        if (const auto* r = match_routing_rule(anon_site->routing_rules, anon_orig_key,
                                               http_status(website_err->code))) {
            resp = routing_redirect(req, *r, anon_orig_key, bucket, vhost);
            website_err.reset();
            metrics_.website(WebsiteEvent::Redirect);
        } else if (website_err->code == S3ErrorCode::NoSuchKey && !anon_orig_key.empty() &&
                   anon_orig_key.back() != '/') {
            // AWS website-endpoint behavior (roadmap §2.3): GET /prefix without the
            // trailing slash 302-redirects to /prefix/ when the directory-style index
            // object exists — the most common felt difference from AWS for real sites
            bool have_index = false;
            try {
                co_await router_.resolve(bucket).head_object(
                    bucket, anon_orig_key + "/" + anon_site->index_suffix);
                have_index = true;
            } catch (const std::exception&) {
                // any failure (NoSuchKey included) keeps the original error
            }
            if (have_index) {
                std::string loc = (vhost ? "/" : "/" + bucket + "/") +
                                  util::aws_uri_encode(anon_orig_key, /*encode_slash=*/false) +
                                  "/";
                resp = redirect_response(302, std::move(loc));
                website_err.reset();
                metrics_.website(WebsiteEvent::Redirect);
            }
        }
        if (website_err) {
            metrics_.website(WebsiteEvent::ErrorDocument);
            resp = co_await website_error_page(*website_err, *anon_site, head);
        }
    }
    // CORS actual-request headers (roadmap §2.1): injected on success and error alike —
    // the browser needs Allow-Origin to surface either response to the page. Preflight
    // (OPTIONS) builds its own full header set in the handler
    if (req.method != "OPTIONS") apply_cors_headers(req, bucket, resp);
    resp.headers.set("x-amz-request-id", ctx.request_id);
    resp.headers.set("x-amz-id-2", ctx.host_id);
    resp.headers.set("Server", "lights3");
    // Trace Context response header (W3C draft `traceresponse`): the trace id the
    // gateway used, so a client without its own tracer can still quote it
    resp.headers.set("traceresponse", ctx.trace.traceparent());

    // Metrics close here: the histograms measure time-to-headers-ready, the same
    // quantity the access line reports as ttfb (docs/s3-protocol.md §7)
    double secs = mguard.finish(resp.status);
    uint64_t bytes = resp.content_length.value_or(resp.small_body.size());
    metrics_.record_api(api_name, backend_name.empty() ? "-" : backend_name, resp.status, secs);
    // Access log (roadmap §5.2, docs/s3-protocol.md §7): one line per request; a
    // route that never ran (short-circuited before/at auth) reports handler=0
    auto access = std::make_unique<AccessRecord>();
    access->start = start;
    access->request_id = ctx.request_id;
    access->remote = req.remote_addr;
    access->access_key = access_key;
    access->method = req.method;
    access->path = req.path;
    access->query = req.raw_query;
    access->bucket = bucket;
    access->key = key;
    if (auto ua = req.headers.get("User-Agent")) access->user_agent = *ua;
    access->api = std::string(api_name);
    access->backend = backend_name;
    access->trace_id = ctx.trace.trace_id;
    access->span_id = ctx.trace.span_id;
    access->parent_span_id = ctx.trace.parent_span_id;
    access->status = resp.status;
    access->auth_ms = ms_since(start, auth_done);
    if (route_start) access->handler_ms = ms_since(*route_start, route_end.value_or(std::chrono::steady_clock::now()));
    access->backend_ms = backend_stats->millis();
    access->backend_calls = backend_stats->calls.load(std::memory_order_relaxed);
    access->ttfb_ms = secs * 1000.0;
    access->slow_threshold_ms = slow_request_ms_.load(std::memory_order_relaxed);
    // per-bucket request distribution and outbound bytes (docs/archive/gaps.md §7). Streaming response bytes are pulled
    // by the driver after dispatch returns and counted via the decorator (which then also emits the access line with
    // the bytes actually sent and the full wall time); small responses have a known length by now
    metrics_.record_bucket_request(bucket);
    if (resp.stream_body) {
        resp.stream_body = std::make_unique<CountingBodyReader>(std::move(resp.stream_body),
                                                                &metrics_, bucket,
                                                                /*inbound=*/false,
                                                                std::move(access));
    } else {
        metrics_.add_bytes_out(bucket, resp.small_body.size());
        emit_access(*access, bytes, /*truncated=*/false);
    }
    // Data-plane audit record (roadmap §3.9 ④): structured twin of the access line
    if (audit_ && audit_->data_plane()) {
        AuditEvent e;
        e.event = "access";
        e.actor = access_key;
        e.tenant = tenant_for_log;
        e.request_id = ctx.request_id;
        e.trace_id = ctx.trace.trace_id;
        e.bucket = bucket;
        e.key = key;
        e.method = req.method;
        e.path = req.path;
        e.status = resp.status;
        e.bytes = static_cast<int64_t>(bytes);
        e.action = api_name;
        audit_->access(e);
    }
    co_return resp;
}

// ---------- Explicit dispatch table (docs/s3-protocol.md §2) ----------

namespace {

bool flag_matches(const http::HttpRequest& req, std::string_view flag) {
    if (flag.empty()) return true;
    auto eq = flag.find('=');
    if (eq == std::string_view::npos) return req.query_has(flag);
    auto v = req.query_get(flag.substr(0, eq));
    return v && *v == flag.substr(eq + 1);
}

}  // namespace

// The table lives in route() so the lambdas can reach private handlers; match_route shares the same static
// table (authorization must know the action before the handler, see dispatch)
const S3Service::Route* S3Service::match_route(const http::HttpRequest& req, Scope scope) const {
    for (auto& r : route_table())
        if (r.method == req.method && r.scope == scope && flag_matches(req, r.flag)) return &r;
    return nullptr;
}

// Dispatch table: inside a member function so capture-free lambdas can access private handlers.
// match_route and route share this one copy -- authorization must know the action before the handler runs
std::span<const S3Service::Route> S3Service::route_table() {
    using Scope = S3Service::Scope;
    static constexpr Route kRoutes[] = {
    // Service level
    {"GET", Scope::Service, "", "",
     Action::Read, "ListBuckets",
     [](S3Service& s, http::HttpRequest&, std::string, std::string,
        const RequestAuth& auth) {
         return s.list_buckets(auth);
     }},

    // Bucket level
    {"GET", Scope::Bucket, "location", "",
     Action::Read, "GetBucketLocation",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth&) {
         return s.get_bucket_location(std::move(b));
     }},
    // ?website subresource (docs/static-website.md phase ③): flagged routes must precede
    // the flagless fallbacks of the same method, or PUT /bucket?website would create a bucket
    {"GET", Scope::Bucket, "website", "",
     Action::Read, "GetBucketWebsite",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.get_bucket_website(std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "website", "",
     Action::Write, "PutBucketWebsite",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.put_bucket_website(req, std::move(b), auth);
     }},
    {"DELETE", Scope::Bucket, "website", "",
     Action::Delete, "DeleteBucketWebsite",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_bucket_website(std::move(b), auth);
     }},
    // ?lifecycle subresource (roadmap §2.4, root credential only, same model as ?website)
    {"GET", Scope::Bucket, "lifecycle", "",
     Action::Read, "GetBucketLifecycle",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.get_bucket_lifecycle(std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "lifecycle", "",
     Action::Write, "PutBucketLifecycle",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.put_bucket_lifecycle(req, std::move(b), auth);
     }},
    {"DELETE", Scope::Bucket, "lifecycle", "",
     Action::Delete, "DeleteBucketLifecycle",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_bucket_lifecycle(std::move(b), auth);
     }},
    // ?cors subresource (roadmap §2.1, root credential only, same model as ?website)
    {"GET", Scope::Bucket, "cors", "",
     Action::Read, "GetBucketCors",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.get_bucket_cors(std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "cors", "",
     Action::Write, "PutBucketCors",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.put_bucket_cors(req, std::move(b), auth);
     }},
    {"DELETE", Scope::Bucket, "cors", "",
     Action::Delete, "DeleteBucketCors",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_bucket_cors(std::move(b), auth);
     }},
    // ?quota subresource (roadmap §3.9 ②, docs/multi-tenancy.md §3): GET for anyone
    // admitted to the bucket, PUT/DELETE root only
    {"GET", Scope::Bucket, "quota", "",
     Action::Read, "GetBucketQuota",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.get_bucket_quota(std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "quota", "",
     Action::Write, "PutBucketQuota",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.put_bucket_quota(req, std::move(b), auth);
     }},
    {"DELETE", Scope::Bucket, "quota", "",
     Action::Delete, "DeleteBucketQuota",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_bucket_quota(std::move(b), auth);
     }},
    // All five parameters now take effect (docs/archive/gaps.md §5.1): previously pagination parameters were "allowed
    // but ignored" and prefix/delimiter simply not admitted (ignoring them would mix in uploads outside the filter)
    {"GET", Scope::Bucket, "uploads",
     "max-uploads key-marker upload-id-marker prefix delimiter encoding-type",
     Action::Read, "ListMultipartUploads",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.list_multipart_uploads(req, std::move(b), auth);
     }},
    // ListObjectsV2 and V1 compatibility share one entry. fetch-owner allowed but ignored: V2 omits Owner by
    // default, so ignoring equals =false and is not a "silent wrong answer"
    {"GET", Scope::Bucket, "",
     "list-type prefix delimiter marker continuation-token start-after max-keys "
     "encoding-type fetch-owner",
     Action::Read, "ListObjects",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.list_objects(req, std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "", "",
     Action::Write, "CreateBucket",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.create_bucket(req, std::move(b), auth);
     }},
    {"HEAD", Scope::Bucket, "", "",
     Action::Read, "HeadBucket",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth&) {
         return s.head_bucket(std::move(b));
     }},
    {"DELETE", Scope::Bucket, "", "",
     Action::Delete, "DeleteBucket",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_bucket(std::move(b), auth);
     }},
    {"POST", Scope::Bucket, "delete", "",
     Action::Delete, "DeleteObjects",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_objects(req, std::move(b), auth);
     }},

    // ?tagging subresource (roadmap §2.5)
    {"GET", Scope::Object, "tagging", "",
     Action::Read, "GetObjectTagging",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string k,
        const RequestAuth&) {
         return s.get_object_tagging(std::move(b), std::move(k));
     }},
    {"PUT", Scope::Object, "tagging", "",
     Action::Write, "PutObjectTagging",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.put_object_tagging(req, std::move(b), std::move(k));
     }},
    {"DELETE", Scope::Object, "tagging", "",
     Action::Delete, "DeleteObjectTagging",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string k,
        const RequestAuth&) {
         return s.delete_object_tagging(std::move(b), std::move(k));
     }},

    // Object level: multipart
    {"POST", Scope::Object, "uploads", "",
     Action::Write, "CreateMultipartUpload",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth& auth) {
         return s.create_multipart(req, std::move(b), std::move(k), auth);
     }},
    {"POST", Scope::Object, "uploadId", "",
     Action::Write, "CompleteMultipartUpload",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth& auth) {
         return s.complete_multipart(req, std::move(b), std::move(k), auth);
     }},
    {"PUT", Scope::Object, "partNumber", "uploadId",
     Action::Write, "UploadPart",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth& auth) {
         return s.upload_part(req, std::move(b), std::move(k), auth);
     }},
    {"GET", Scope::Object, "uploadId", "max-parts part-number-marker encoding-type",
     Action::Read, "ListParts",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.list_parts(req, std::move(b), std::move(k));
     }},
    {"DELETE", Scope::Object, "uploadId", "",
     Action::Delete, "AbortMultipartUpload",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.abort_multipart(req, std::move(b), std::move(k));
     }},

    // Object level: data plane
    {"PUT", Scope::Object, "", "",  // PutObject / CopyObject (steered by x-amz-copy-source)
     Action::Write, "PutObject",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth& auth) {
         if (req.headers.has("x-amz-copy-source"))
             return s.copy_object(req, std::move(b), std::move(k), auth);
         return s.put_object(req, std::move(b), std::move(k), auth);
     }},
    // response-* override parameters (docs/archive/gaps.md §5.3): the family most used in presigned download links
    // partNumber (roadmap §2.5): reads one part of a completed multipart object; ranges
    // resolve from the part_sizes layout recorded at complete
    {"GET", Scope::Object, "",
     "response-content-type response-content-language response-expires "
     "response-cache-control response-content-disposition response-content-encoding "
     "partNumber",
     Action::Read, "GetObject",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.get_object(req, std::move(b), std::move(k), false);
     }},
    {"HEAD", Scope::Object, "",
     "response-content-type response-content-language response-expires "
     "response-cache-control response-content-disposition response-content-encoding "
     "partNumber",
     Action::Read, "HeadObject",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.get_object(req, std::move(b), std::move(k), true);
     }},
    {"DELETE", Scope::Object, "", "",
     Action::Delete, "DeleteObject",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string k,
        const RequestAuth&) {
         return s.delete_object(std::move(b), std::move(k));
     }},
    };
    return kRoutes;
}

Task<http::HttpResponse> S3Service::route(http::HttpRequest& req, std::string bucket,
                                          std::string key, const RequestAuth& auth) {


    // The blocklist goes first only to give known subresources a clearer error message; the structural defenses
    // are the per-route query allowlist below (§3.5) and the request-header check (§3.4)
    reject_unsupported_subresource(req);
    reject_unsupported_headers(req);
    Scope scope = bucket.empty() ? Scope::Service
                  : key.empty() ? Scope::Bucket
                                : Scope::Object;
    if (const Route* r = match_route(req, scope)) {
        // Allowlist (§3.5): a query key outside this route's list -> 501. Under a blocklist model, any omission
        // silently degrades into "read/write the whole object" (?attributes returns the object body, ?partNumber
        // returns the whole object, response-* gets swallowed); 501 is at least honest
        enforce_query_whitelist(req, *r);
        co_return co_await r->fn(*this, req, std::move(bucket), std::move(key), auth);
    }
    // 405 must carry Allow (RFC 9110 §15.5.6, docs/archive/gaps.md §5.9): the answer is the other methods in the same
    // scope that would also match this request's query -- the list comes from the dispatch table itself, so it cannot drift from it
    std::string allow;
    for (auto& r : route_table()) {
        if (r.scope != scope || !flag_matches(req, r.flag)) continue;
        if (allow.find(r.method) != std::string::npos) continue;  // multiple routes per method listed once
        if (!allow.empty()) allow += ", ";
        allow += r.method;
    }
    // Driver/upstream semantics where HEAD is served by GET routes: listing GET lists HEAD along with it
    if (allow.find("GET") != std::string::npos && allow.find("HEAD") == std::string::npos)
        allow += ", HEAD";
    throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed.")
        .with_header("Allow", allow);
}

// ---------- readyz (docs/s3-protocol.md §7: per-backend liveness probes) ----------

Task<http::HttpResponse> S3Service::readyz() {
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "text/plain");

    // The endpoint is anonymously reachable while the probe issues real calls to every backend (cloudproxy is a
    // billed remote ListBuckets): short result cache + single-flight, so an anonymous loop cannot generate amplified traffic
    constexpr auto kTtl = std::chrono::seconds(5);
    {
        std::lock_guard lk(readyz_mu_);
        auto now = std::chrono::steady_clock::now();
        bool fresh = readyz_status_ != 0 && now - readyz_at_ < kTtl;
        if (fresh || readyz_inflight_) {
            resp.status = readyz_status_ != 0 ? readyz_status_ : 503;
            resp.small_body = readyz_status_ != 0 ? readyz_body_ : "probing\n";
            co_return resp;
        }
        readyz_inflight_ = true;
    }
    // The single-flight flag must also reset if the coroutine is destroyed early (disconnect), or readyz returns stale values forever
    struct InflightReset {
        S3Service* s;
        ~InflightReset() {
            std::lock_guard lk(s->readyz_mu_);
            s->readyz_inflight_ = false;
        }
    } inflight_reset{this};

    std::string report;
    bool ok = true;
    for (auto& [name, backend] : router_.backends()) {
        try {
            co_await backend->list_buckets();
            report += name + " ok\n";
        } catch (const std::exception& e) {
            ok = false;
            // Exception text may contain topology such as upstream endpoints: log only, never returned to anonymous callers
            LOG_WARN("readyz: backend {} probe failed: {}", name, e.what());
            report += name + " FAIL\n";
        }
    }
    // The credential-table wipe guard has fired (fail-open guard, README §1.2): report unhealthy to prompt ops intervention
    if (cred_store_ && cred_store_->degraded()) {
        ok = false;
        report += "credential-store DEGRADED\n";
    }

    {
        std::lock_guard lk(readyz_mu_);
        readyz_inflight_ = false;
        readyz_at_ = std::chrono::steady_clock::now();
        readyz_status_ = ok ? 200 : 503;
        readyz_body_ = report;
    }
    resp.status = ok ? 200 : 503;
    resp.small_body = std::move(report);
    co_return resp;
}

}  // namespace lights3::s3
