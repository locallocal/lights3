#include "s3/service.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <random>

#include "core/log.h"
#include "core/util/hex.h"
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
        return S3Error(e.code, "Please reduce your request rate.");
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

// Byte-counting decorators (docs/gaps.md §7): inbound wraps outside the checksum/de-framing decorators
// (counting the payload bytes the handler actually consumes); outbound wraps outside stream_body (counting the
// bytes the driver actually pulls -- streaming responses are written after dispatch returns, and only a decorator can see them)
class CountingBodyReader final : public http::BodyReader {
public:
    CountingBodyReader(std::unique_ptr<http::BodyReader> inner, Metrics* m, std::string bucket,
                       bool inbound)
        : inner_(std::move(inner)), m_(m), bucket_(std::move(bucket)), inbound_(inbound) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_->read(buf);
        if (inbound_) m_->add_bytes_in(bucket_, n);
        else m_->add_bytes_out(bucket_, n);
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    Metrics* m_;
    std::string bucket_;
    bool inbound_;
};

// Explicitly unsupported subresources (docs/s3-protocol.md §1): explicit 501, avoiding wrong answers from falling into the List/Get fallback
constexpr std::string_view kUnsupportedSubresources[] = {
    "acl",         "policy",       "versioning",     "versions",
    "lifecycle",   "tagging",      "cors",           "encryption",     "object-lock",
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

// Explicitly unsupported **request headers** (docs/gaps.md §3.4): SSE/SSE-C, tagging, object-lock, and
// ACL-grant classes used to be silently swallowed -- 200 with the semantics unfulfilled; in compliance scenarios
// clients would conclude the object is encrypted/locked. A hit is 501; x-amz-acl alone admits private (this implementation's actual semantics)
void reject_unsupported_headers(const http::HttpRequest& req) {
    constexpr std::string_view kPrefixes[] = {
        "x-amz-server-side-encryption",  // the whole SSE and SSE-C family (including -customer-*, -aws-kms-*)
        "x-amz-copy-source-server-side-encryption",  // the three SSE-C headers on the copy source side
        "x-amz-object-lock-",                        // mode / retain-until-date / legal-hold
        "x-amz-grant-",                              // the five ACL grant headers, same class as x-amz-acl
    };
    // x-amz-website-redirect-location left this list with docs/static-website.md phase ③:
    // it is now a first-class metadata field (kStdMetaFields)
    constexpr std::string_view kExact[] = {
        "x-amz-tagging",
    };
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
            // Likewise (docs/gaps.md §5.2): STANDARD is the only storage class; accepting GLACIER and echoing
            // it back would lie on behalf of the storage layer -- the object never entered any archive tier
            if (!http::HeaderMap::ieq(v, "STANDARD")) refuse();
            continue;
        }
        for (auto p : kPrefixes)
            if (lk.rfind(p, 0) == 0) refuse();
        for (auto e : kExact)
            if (lk == e) refuse();
    }
}

// Query allowlist (docs/gaps.md §3.5): keys permitted on all routes -- the presigned signature parameter
// family + SDK tracing parameters. Keys are case-sensitive (consistent with the SigV4 canonical query)
constexpr std::string_view kCommonQueryKeys[] = {
    "X-Amz-Algorithm",     "X-Amz-Credential", "X-Amz-Date",
    "X-Amz-Expires",       "X-Amz-Signature",  "X-Amz-SignedHeaders",
    "X-Amz-Security-Token", "X-Amz-Content-Sha256",
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
    RequestContext ctx{make_request_id(), make_host_id(), req.cancel};
    bool head = req.method == "HEAD";
    auto start = std::chrono::steady_clock::now();
    metrics_.request_start();
    MetricsEndGuard mguard{metrics_, req.method, start};

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
    try {
        // Resolve addressing before steering to internal endpoints (docs/gaps.md §3.8): under vhost, req.path is
        // the key, and "/-/metrics" may be a legitimate object in mybucket -- exact path comparison would turn a
        // GET into anonymous metrics and a PUT into "200 but the object was never written" silent data loss.
        // Only the /-/ prefix under path addressing (non-vhost) enters the internal branch
        auto addr = resolve_address(req);
        bool internal = !addr.vhost && req.path.rfind("/-/", 0) == 0;
        // Read endpoints accept only GET/HEAD (probes commonly use HEAD); previously PUT /-/metrics also returned 200
        auto internal_get = [&](std::string_view ep) {
            if (req.path != ep) return false;
            if (req.method != "GET" && req.method != "HEAD")
                throw S3Error(S3ErrorCode::MethodNotAllowed,
                              "The specified method is not allowed against this resource.");
            return true;
        };
        if (internal && internal_get("/-/healthz")) {
            resp.small_body = "ok\n";
            resp.headers.set("Content-Type", "text/plain");
        } else if (internal && internal_get("/-/metrics")) {
            resp.small_body = metrics_.render(pool_stats_, admission_stats_, timer_stats_);
            // The backend-level registry is appended after the L2 request metrics
            if (backend_metrics_) resp.small_body += backend_metrics_->render();
            resp.headers.set("Content-Type", "text/plain; version=0.0.4");
        } else if (internal && internal_get("/-/readyz")) {
            resp = co_await readyz();
        } else if (internal && (req.path == "/-/admin/credentials" ||
                                req.path.rfind("/-/admin/credentials/", 0) == 0)) {
            // The boundary must land on '/': bare prefix matching would let /-/admin/credentialsXYZ into the admin plane too
            resp = co_await admin_credentials(req, access_key);
        } else {
            // Static website hosting phase 1 (docs/static-website.md): requests with no
            // signature material may read objects from explicitly listed website buckets
            // anonymously. Decided before verify -- verify treats a missing Authorization
            // header as AccessDenied; when auth is globally disabled verify() admits
            // everything anyway and the anonymous branch changes nothing (the synthesized
            // read-only policy would only be stricter than "unrestricted")
            if (website_store_) web_snap = website_store_->snapshot();
            bool anon = auth_.enabled() && anonymous_website_read(req, addr, web_snap);
            // Authorization uses the verify-time policy snapshot (docs/gaps.md §3.7): with a second store lookup
            // after verification, the policy would vanish entirely in the race window where sync/remove deletes
            // the credential -- a readonly credential becomes unrestricted within the window. The snapshot makes
            // in-flight requests complete strictly with verify-time semantics
            auto ident = anon ? VerifiedIdentity{} : auth_.verify(req);
            access_key = ident.access_key;
            // Content-MD5 / x-amz-checksum-* (docs/gaps.md §5.6): installed after verify, hence wrapping outside
            // the sha256/aws-chunked decorators -- digests are computed over the de-framed plaintext, the same
            // bytes the client computed over. Independent of the signature; also effective with auth disabled
            install_checksum_guard(req);
            bucket = std::move(addr.bucket);
            key = std::move(addr.key);
            // Inbound byte counting (docs/gaps.md §7): bucket already resolved, decorated at the outermost layer
            if (req.body)
                req.body = std::make_unique<CountingBodyReader>(std::move(req.body), &metrics_,
                                                                bucket, /*inbound=*/true);
            // User-requested bucket names pass full validation here, the **single** authoritative gate
            // (docs/gaps.md §1.1). Previously only the first character was checked for '.', while under vhost
            // addressing the bucket comes entirely from the Host header and may contain '/' or even start with
            // '/', which combined with localfs's root_/bucket/key concatenation (fs::path replaces the whole path
            // on an absolute component) means arbitrary file reads; on the path-style side, %00 could turn the
            // first character into NUL to bypass the reserved-name check. validate_bucket_name's character-set
            // rules close both entrances at once, and reserved names (.sys) are only available to callers with
            // allow_reserved=true -- user requests never get that parameter
            if (!bucket.empty()) storage::validate_bucket_name(bucket);
            if (anon) {
                anon_site = WebsiteStore::find(web_snap, bucket);
                // Index document (docs/static-website.md phase ②): an empty key (bucket
                // root, with or without trailing slash) or a directory-style key
                // ("docs/") maps to the index object. Rewriting before the route gate
                // also turns what would be a bucket-scope listing into a plain object
                // read -- anonymous listing stays impossible by construction
                if (key.empty() || key.back() == '/') key += anon_site->index_suffix;
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
            // per-credential policy (docs/credential-management.md §10.4): the action comes from the matched
            // route, not the HTTP method (docs/gaps.md §5.10) -- DeleteObjects is a POST yet a delete,
            // CreateMultipartUpload is also a POST yet a write; the method dimension cannot separate the two.
            // The decision input is the snapshot verify returned, never a store lookup (§3.7)
            RequestAuth auth{access_key, ident.policy ? &*ident.policy : nullptr};
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
            // Per-request timeout + cancellation wiring (docs/gaps.md §3.1/§3.3): req_src is dedicated to this
            // request; external tokens (process shutdown, plus client disconnect once the driver is wired) attach
            // to the same source -- any trigger converges the whole L2/L3 chain with OperationCancelled from the
            // nearest cancellable suspension point (pool.schedule / semaphore.acquire). The token propagates down
            // the Task promise automatically, no per-handler/backend signature changes needed
            CancelSource req_src;
            CancelRegistration link;
            if (ctx.cancel.valid()) {
                link = ctx.cancel.on_cancel([&req_src] { req_src.request_cancel(); });
                if (ctx.cancel.cancelled()) req_src.request_cancel();
            }
            if (request_timeout_.count() > 0)
                resp = co_await with_timeout(route(req, bucket, key, auth), request_timeout_, req_src);
            else
                resp = co_await std::move(route(req, bucket, key, auth).with_cancel(req_src.token()));
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
                }
            }
        }
    } catch (const OperationCancelled&) {
        // Timeout/disconnect/shutdown: 503 lets SDKs retry. Blocking syscalls already running on pool threads are
        // not preempted; this response only means "the gateway stops waiting for it" (the cooperative semantics of docs/concurrency.md §5)
        LOG_WARN("req {} {} {} cancelled (timeout or shutdown)", ctx.request_id, req.method,
                 req.path);
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
        LOG_ERROR("req {} {} {} internal error: {}", ctx.request_id, req.method, req.path,
                  e.what());
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
    if (website_err) resp = co_await website_error_page(*website_err, *anon_site, head);
    // per-bucket request distribution and outbound bytes (docs/gaps.md §7). Streaming response bytes are pulled
    // by the driver after dispatch returns and counted via the decorator; small responses have a known length by now
    metrics_.record_bucket_request(bucket);
    if (resp.stream_body)
        resp.stream_body = std::make_unique<CountingBodyReader>(std::move(resp.stream_body),
                                                                &metrics_, bucket,
                                                                /*inbound=*/false);
    else
        metrics_.add_bytes_out(bucket, resp.small_body.size());
    resp.headers.set("x-amz-request-id", ctx.request_id);
    resp.headers.set("x-amz-id-2", ctx.host_id);
    resp.headers.set("Server", "lights3");

    // Access log (docs/s3-protocol.md §7): one structured line, field order aligned with a slimmed-down S3 access log
    double secs = mguard.finish(resp.status);
    uint64_t bytes = resp.content_length.value_or(resp.small_body.size());
    LOG_INFO("access {} {} {} {} {} {} {}ms", ctx.request_id,
             access_key.empty() ? "-" : access_key, req.method, req.path, resp.status, bytes,
             static_cast<uint64_t>(secs * 1000));
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
     Action::Read,
     [](S3Service& s, http::HttpRequest&, std::string, std::string,
        const RequestAuth& auth) {
         return s.list_buckets(auth);
     }},

    // Bucket level
    {"GET", Scope::Bucket, "location", "",
     Action::Read,
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth&) {
         return s.get_bucket_location(std::move(b));
     }},
    // ?website subresource (docs/static-website.md phase ③): flagged routes must precede
    // the flagless fallbacks of the same method, or PUT /bucket?website would create a bucket
    {"GET", Scope::Bucket, "website", "",
     Action::Read,
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.get_bucket_website(std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "website", "",
     Action::Write,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.put_bucket_website(req, std::move(b), auth);
     }},
    {"DELETE", Scope::Bucket, "website", "",
     Action::Delete,
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_bucket_website(std::move(b), auth);
     }},
    // All five parameters now take effect (docs/gaps.md §5.1): previously pagination parameters were "allowed
    // but ignored" and prefix/delimiter simply not admitted (ignoring them would mix in uploads outside the filter)
    {"GET", Scope::Bucket, "uploads",
     "max-uploads key-marker upload-id-marker prefix delimiter encoding-type",
     Action::Read,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.list_multipart_uploads(req, std::move(b), auth);
     }},
    // ListObjectsV2 and V1 compatibility share one entry. fetch-owner allowed but ignored: V2 omits Owner by
    // default, so ignoring equals =false and is not a "silent wrong answer"
    {"GET", Scope::Bucket, "",
     "list-type prefix delimiter marker continuation-token start-after max-keys "
     "encoding-type fetch-owner",
     Action::Read,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.list_objects(req, std::move(b), auth);
     }},
    {"PUT", Scope::Bucket, "", "",
     Action::Write,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth&) {
         return s.create_bucket(req, std::move(b));
     }},
    {"HEAD", Scope::Bucket, "", "",
     Action::Read,
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth&) {
         return s.head_bucket(std::move(b));
     }},
    {"DELETE", Scope::Bucket, "", "",
     Action::Delete,
     [](S3Service& s, http::HttpRequest&, std::string b, std::string,
        const RequestAuth&) {
         return s.delete_bucket(std::move(b));
     }},
    {"POST", Scope::Bucket, "delete", "",
     Action::Delete,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string,
        const RequestAuth& auth) {
         return s.delete_objects(req, std::move(b), auth);
     }},

    // Object level: multipart
    {"POST", Scope::Object, "uploads", "",
     Action::Write,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.create_multipart(req, std::move(b), std::move(k));
     }},
    {"POST", Scope::Object, "uploadId", "",
     Action::Write,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.complete_multipart(req, std::move(b), std::move(k));
     }},
    {"PUT", Scope::Object, "partNumber", "uploadId",
     Action::Write,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.upload_part(req, std::move(b), std::move(k));
     }},
    {"GET", Scope::Object, "uploadId", "max-parts part-number-marker",
     Action::Read,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.list_parts(req, std::move(b), std::move(k));
     }},
    {"DELETE", Scope::Object, "uploadId", "",
     Action::Delete,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.abort_multipart(req, std::move(b), std::move(k));
     }},

    // Object level: data plane
    {"PUT", Scope::Object, "", "",  // PutObject / CopyObject (steered by x-amz-copy-source)
     Action::Write,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         if (req.headers.has("x-amz-copy-source"))
             return s.copy_object(req, std::move(b), std::move(k));
         return s.put_object(req, std::move(b), std::move(k));
     }},
    // response-* override parameters (docs/gaps.md §5.3): the family most used in presigned download links
    {"GET", Scope::Object, "",
     "response-content-type response-content-language response-expires "
     "response-cache-control response-content-disposition response-content-encoding",
     Action::Read,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.get_object(req, std::move(b), std::move(k), false);
     }},
    {"HEAD", Scope::Object, "",
     "response-content-type response-content-language response-expires "
     "response-cache-control response-content-disposition response-content-encoding",
     Action::Read,
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k,
        const RequestAuth&) {
         return s.get_object(req, std::move(b), std::move(k), true);
     }},
    {"DELETE", Scope::Object, "", "",
     Action::Delete,
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
    // 405 must carry Allow (RFC 9110 §15.5.6, docs/gaps.md §5.9): the answer is the other methods in the same
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
