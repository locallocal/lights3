// L2: CORS (roadmap §2.1) — ?cors subresource (Get/Put/DeleteBucketCors, root
// credential only, same two-tier model as ?website), OPTIONS preflight, and
// response-header injection for cross-origin actual requests. Preflight runs before
// signature verification by design: browsers never attach signature material to
// OPTIONS, and the preflighted request (e.g. a presigned PUT) is verified on its own.
#include <algorithm>
#include <charconv>

#include "core/log.h"
#include "s3/auth/credential_store.h"
#include "s3/cors_store.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

namespace {

constexpr std::string_view kCorsMethods[] = {"GET", "PUT", "POST", "DELETE", "HEAD"};

std::string cors_xml(const std::vector<CorsRule>& rules) {
    XmlWriter x;
    x.open("CORSConfiguration", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    for (auto& r : rules) {
        x.open("CORSRule");
        if (!r.id.empty()) x.element("ID", r.id);
        for (auto& o : r.allowed_origins) x.element("AllowedOrigin", o);
        for (auto& m : r.allowed_methods) x.element("AllowedMethod", m);
        for (auto& h : r.allowed_headers) x.element("AllowedHeader", h);
        for (auto& e : r.expose_headers) x.element("ExposeHeader", e);
        if (r.max_age_seconds >= 0)
            x.element("MaxAgeSeconds", static_cast<uint64_t>(r.max_age_seconds));
        x.close();
    }
    x.close();
    return x.str();
}

// At most one '*' per AllowedOrigin/AllowedHeader (AWS rule): more would make the
// match semantics ambiguous, and silently accepting them would mismatch at request time
void check_single_wildcard(const char* what, const std::string& v) {
    if (std::count(v.begin(), v.end(), '*') > 1)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      std::string(what) + " \"" + v + "\" can not have more than one wildcard.");
}

std::vector<CorsRule> parse_cors_xml(const std::string& body) {
    auto root = xml_parse(body);
    if (root.name != "CORSConfiguration")
        throw S3Error(S3ErrorCode::MalformedXML,
                      "The XML you provided was not well-formed or did not validate "
                      "against our published schema.");
    std::vector<CorsRule> rules;
    for (auto& child : root.children) {
        if (child.name != "CORSRule") continue;
        if (rules.size() >= 100)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "The CORS configuration may contain at most 100 rules.");
        CorsRule r;
        r.id = child.get("ID");
        for (auto& e : child.children) {
            if (e.name == "AllowedOrigin") {
                check_single_wildcard("AllowedOrigin", e.text);
                r.allowed_origins.push_back(e.text);
            } else if (e.name == "AllowedMethod") {
                bool known = false;
                for (auto m : kCorsMethods)
                    if (e.text == m) known = true;
                if (!known)
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "Found unsupported HTTP method in CORS config. "
                                  "Unsupported method is " + e.text);
                r.allowed_methods.push_back(e.text);
            } else if (e.name == "AllowedHeader") {
                check_single_wildcard("AllowedHeader", e.text);
                std::string h;
                for (char c : e.text) h.push_back(http::HeaderMap::lower(c));
                r.allowed_headers.push_back(std::move(h));
            } else if (e.name == "ExposeHeader") {
                if (e.text.find('*') != std::string::npos)
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "ExposeHeader \"" + e.text + "\" contains wildcard. We "
                                  "currently do not support wildcard for ExposeHeader.");
                r.expose_headers.push_back(e.text);
            }
        }
        std::string age = child.get("MaxAgeSeconds");
        if (!age.empty()) {
            int v = 0;
            auto [p, ec] = std::from_chars(age.data(), age.data() + age.size(), v);
            if (ec != std::errc() || p != age.data() + age.size() || v < 0)
                throw S3Error(S3ErrorCode::MalformedXML, "Invalid MaxAgeSeconds value.");
            r.max_age_seconds = v;
        }
        if (r.allowed_origins.empty() || r.allowed_methods.empty())
            throw S3Error(S3ErrorCode::MalformedXML,
                          "Each CORSRule must have at least one AllowedOrigin and one "
                          "AllowedMethod.");
        rules.push_back(std::move(r));
    }
    if (rules.empty())
        throw S3Error(S3ErrorCode::MalformedXML,
                      "The CORS configuration must contain at least one CORSRule.");
    return rules;
}

// Access-Control-Request-Headers: comma-separated, OWS-trimmed, lowercased
std::vector<std::string> parse_request_headers(const http::HttpRequest& req) {
    std::vector<std::string> out;
    auto v = req.headers.get("Access-Control-Request-Headers");
    if (!v) return out;
    std::string cur;
    auto flush = [&] {
        while (!cur.empty() && (cur.back() == ' ' || cur.back() == '\t')) cur.pop_back();
        if (!cur.empty()) out.push_back(std::move(cur));
        cur.clear();
    };
    for (char c : *v) {
        if (c == ',') flush();
        else if ((c == ' ' || c == '\t') && cur.empty()) continue;
        else cur.push_back(http::HeaderMap::lower(c));
    }
    flush();
    return out;
}

std::string join(const std::vector<std::string>& v, const char* sep = ", ") {
    std::string out;
    for (auto& s : v) {
        if (!out.empty()) out += sep;
        out += s;
    }
    return out;
}

bool has_star_origin(const CorsRule& r) {
    for (auto& o : r.allowed_origins)
        if (o == "*") return true;
    return false;
}

// Same root gate as ?website (bucket_website.cc): CORS opens the bucket to browser
// cross-origin access, an operator decision, not a tenant one
void require_root(const std::shared_ptr<CredentialStore>& store, std::string_view access_key) {
    if (!store || !store->is_root(access_key))
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The CORS configuration API requires a root (statically configured) "
                      "credential.");
}

}  // namespace

Task<http::HttpResponse> S3Service::get_bucket_cors(std::string bucket,
                                                    const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    auto snap = cors_store_ ? cors_store_->snapshot() : CorsStore::Snapshot{};
    const auto* rules = CorsStore::find(snap, bucket);
    if (!rules)
        throw S3Error(S3ErrorCode::NoSuchCORSConfiguration,
                      "The CORS configuration does not exist", bucket);
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = cors_xml(*rules);
    co_return resp;
}

Task<http::HttpResponse> S3Service::put_bucket_cors(http::HttpRequest& req, std::string bucket,
                                                    const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    if (!cors_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic CORS configuration is not available on this deployment.");
    // AWS parity: configuring CORS on a bucket that does not exist is NoSuchBucket
    if (!co_await router_.resolve(bucket).bucket_exists(bucket))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    auto body = co_await handlers::read_body(req);
    auto rules = parse_cors_xml(body);
    co_await cors_store_->put(bucket, std::move(rules));
    LOG_INFO("cors: configuration for bucket {} set by {}", bucket,
             std::string(auth.access_key));
    co_return http::HttpResponse{};
}

Task<http::HttpResponse> S3Service::delete_bucket_cors(std::string bucket,
                                                       const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    if (!cors_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic CORS configuration is not available on this deployment.");
    co_await cors_store_->remove(bucket);
    LOG_INFO("cors: configuration for bucket {} deleted by {}", bucket,
             std::string(auth.access_key));
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

// OPTIONS preflight (roadmap §2.1): unauthenticated by design — browsers attach no
// signature material to preflights, and the preflighted request is verified on its own.
// The only information disclosed is whether a CORS rule admits the (origin, method)
// pair; object existence is never consulted
Task<http::HttpResponse> S3Service::cors_preflight(http::HttpRequest& req,
                                                   std::string bucket) {
    auto origin = req.headers.get("Origin");
    auto acrm = req.headers.get("Access-Control-Request-Method");
    if (!origin || !acrm)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "Insufficient information. Origin and Access-Control-Request-Method "
                      "request headers are needed.");
    if (bucket.empty())
        throw S3Error(S3ErrorCode::AccessDenied,
                      "CORS preflight requires a bucket-scoped request.");

    auto deny = [&] {
        return S3Error(S3ErrorCode::AccessDenied,
                       "CORSResponse: This CORS request is not allowed. This is usually "
                       "because the evaluation of Origin, request method / "
                       "Access-Control-Request-Method or Access-Control-Request-Headers are "
                       "not whitelisted by the resource's CORS spec.");
    };
    auto snap = cors_store_ ? cors_store_->snapshot() : CorsStore::Snapshot{};
    const auto* rules = CorsStore::find(snap, bucket);
    if (!rules) throw deny();
    auto req_headers = parse_request_headers(req);
    const CorsRule* r = match_cors_rule(*rules, *origin, *acrm, req_headers);
    if (!r) throw deny();

    http::HttpResponse resp;
    if (has_star_origin(*r)) {
        resp.headers.set("Access-Control-Allow-Origin", "*");
    } else {
        resp.headers.set("Access-Control-Allow-Origin", *origin);
        resp.headers.set("Access-Control-Allow-Credentials", "true");
    }
    resp.headers.set("Access-Control-Allow-Methods", join(r->allowed_methods));
    if (!req_headers.empty())
        resp.headers.set("Access-Control-Allow-Headers", join(req_headers));
    if (!r->expose_headers.empty())
        resp.headers.set("Access-Control-Expose-Headers", join(r->expose_headers));
    if (r->max_age_seconds >= 0)
        resp.headers.set("Access-Control-Max-Age", std::to_string(r->max_age_seconds));
    resp.headers.set("Vary",
                     "Origin, Access-Control-Request-Headers, Access-Control-Request-Method");
    co_return resp;
}

// Cross-origin actual requests (success AND error responses — the browser needs the
// allow header to surface either to the page). Never applied to OPTIONS (preflight
// builds its own full set)
void S3Service::apply_cors_headers(const http::HttpRequest& req, const std::string& bucket,
                                   http::HttpResponse& resp) {
    if (!cors_store_ || bucket.empty()) return;
    auto origin = req.headers.get("Origin");
    if (!origin) return;
    auto snap = cors_store_->snapshot();
    const auto* rules = CorsStore::find(snap, bucket);
    if (!rules) return;
    const CorsRule* r = match_cors_rule(*rules, *origin, req.method, {});
    if (!r) return;
    if (has_star_origin(*r)) {
        resp.headers.set("Access-Control-Allow-Origin", "*");
    } else {
        resp.headers.set("Access-Control-Allow-Origin", *origin);
        resp.headers.set("Access-Control-Allow-Credentials", "true");
    }
    if (!r->expose_headers.empty())
        resp.headers.set("Access-Control-Expose-Headers", join(r->expose_headers));
    resp.headers.set("Vary", "Origin");
}

}  // namespace lights3::s3
