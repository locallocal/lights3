#include "s3/service.h"

#include <cctype>
#include <chrono>
#include <cstring>
#include <random>

#include "core/log.h"
#include "core/util/hex.h"
#include "s3/errors.h"
#include "s3/router.h"

namespace lights3::s3 {

namespace {

std::string make_request_id() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    uint64_t v = rng();
    uint8_t bytes[8];
    memcpy(bytes, &v, 8);
    std::string hex = util::to_hex(std::span(bytes, 8));
    for (char& c : hex) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    return hex;
}

http::HttpResponse error_response(const S3Error& e, const std::string& request_id,
                                  bool head_only) {
    http::HttpResponse resp;
    resp.status = http_status(e.code);
    resp.headers.set("Content-Type", "application/xml");
    if (!head_only) resp.small_body = error_xml(e, request_id);
    return resp;
}

// 明确不支持的子资源（docs/05 §1）：显式 501，避免落进 List/Get 兜底造成误答
constexpr std::string_view kUnsupportedSubresources[] = {
    "acl",         "policy",       "versioning",     "versions",       "website",
    "lifecycle",   "tagging",      "cors",           "encryption",     "object-lock",
    "legal-hold",  "retention",    "torrent",        "replication",    "logging",
    "notification", "requestPayment", "accelerate",  "analytics",      "inventory",
    "intelligent-tiering", "metrics", "ownershipControls", "publicAccessBlock",
    "restore",     "select",       "policyStatus",
};

void reject_unsupported_subresource(const http::HttpRequest& req) {
    for (auto& sub : kUnsupportedSubresources)
        if (req.query_has(sub))
            throw S3Error(S3ErrorCode::NotImplemented,
                          "The requested sub-resource '" + std::string(sub) +
                              "' is not implemented.");
}

}  // namespace

// ---------- virtual-host style（docs/05 §2）----------

std::pair<std::string, std::string> S3Service::resolve_address(
    const http::HttpRequest& req) const {
    if (!base_domain_.empty()) {
        if (auto host = req.headers.get("Host")) {
            std::string h = *host;
            if (auto colon = h.rfind(':'); colon != std::string::npos) h.resize(colon);
            std::string suffix = "." + base_domain_;
            if (h.size() > suffix.size() && h.ends_with(suffix)) {
                std::string bucket = h.substr(0, h.size() - suffix.size());
                std::string key = req.path;
                if (!key.empty() && key.front() == '/') key.erase(0, 1);
                return {std::move(bucket), std::move(key)};
            }
        }
    }
    return parse_bucket_key(req.path);
}

// ---------- 顶层入口 ----------

Task<http::HttpResponse> S3Service::dispatch(http::HttpRequest req) {
    RequestContext ctx{make_request_id(), {}};
    bool head = req.method == "HEAD";
    auto start = std::chrono::steady_clock::now();
    metrics_.request_start();

    std::string access_key;
    std::string bucket, key;
    http::HttpResponse resp;
    try {
        if (req.path == "/-/healthz") {
            resp.small_body = "ok\n";
            resp.headers.set("Content-Type", "text/plain");
        } else if (req.path == "/-/metrics") {
            resp.small_body = metrics_.render(pool_stats_);
            resp.headers.set("Content-Type", "text/plain; version=0.0.4");
        } else if (req.path == "/-/readyz") {
            resp = co_await readyz();
        } else if (req.path.rfind("/-/admin/credentials", 0) == 0) {
            resp = co_await admin_credentials(req, access_key);
        } else {
            access_key = auth_.verify(req);
            std::tie(bucket, key) = resolve_address(req);
            // '.' 开头为内部保留名（docs/06 §4.1）：用户请求在此统一拒绝，
            // 后端的 validate 对保留名放行，仅 CredentialStore 可达
            if (!bucket.empty() && bucket.front() == '.')
                throw S3Error(S3ErrorCode::InvalidBucketName,
                              "The specified bucket is not valid.", bucket);
            resp = co_await route(req, bucket, key);
        }
    } catch (const S3Error& e) {
        metrics_.s3_error(wire_code(e.code));
        resp = error_response(e, ctx.request_id, head);
    } catch (const std::exception& e) {
        LOG_ERROR("req {} {} {} internal error: {}", ctx.request_id, req.method, req.path,
                  e.what());
        metrics_.s3_error("InternalError");
        resp = error_response(
            S3Error(S3ErrorCode::InternalError, "We encountered an internal error."),
            ctx.request_id, head);
    }
    resp.headers.set("x-amz-request-id", ctx.request_id);
    resp.headers.set("Server", "lights3");

    // 访问日志（docs/05 §7）：一行结构化，字段序对齐 S3 access log 精简版
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    metrics_.request_end(req.method, resp.status, secs);
    uint64_t bytes = resp.content_length.value_or(resp.small_body.size());
    LOG_INFO("access {} {} {} {} {} {} {}ms", ctx.request_id,
             access_key.empty() ? "-" : access_key, req.method, req.path, resp.status, bytes,
             static_cast<uint64_t>(secs * 1000));
    co_return resp;
}

// ---------- 显式分派表（docs/05 §2）----------

namespace {

bool flag_matches(const http::HttpRequest& req, std::string_view flag) {
    if (flag.empty()) return true;
    auto eq = flag.find('=');
    if (eq == std::string_view::npos) return req.query_has(flag);
    auto v = req.query_get(flag.substr(0, eq));
    return v && *v == flag.substr(eq + 1);
}

}  // namespace

Task<http::HttpResponse> S3Service::route(http::HttpRequest& req, std::string bucket,
                                          std::string key) {
    using Scope = S3Service::Scope;
    // 表项按声明序匹配：带 query-flag 的在前，"" 兜底在后。
    // 表定义在成员函数体内：lambda 由此获得私有 handler 的访问权
    static constexpr Route kRoutes[] = {
    // service 级
    {"GET", Scope::Service, "", [](S3Service& s, http::HttpRequest&, std::string, std::string) {
         return s.list_buckets();
     }},

    // bucket 级
    {"GET", Scope::Bucket, "location",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.get_bucket_location(std::move(b));
     }},
    {"GET", Scope::Bucket, "uploads",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.list_multipart_uploads(req, std::move(b));
     }},
    {"GET", Scope::Bucket, "",  // ListObjectsV2 与 V1 兼容同入口
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.list_objects(req, std::move(b));
     }},
    {"PUT", Scope::Bucket, "", [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.create_bucket(std::move(b));
     }},
    {"HEAD", Scope::Bucket, "", [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.head_bucket(std::move(b));
     }},
    {"DELETE", Scope::Bucket, "",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.delete_bucket(std::move(b));
     }},
    {"POST", Scope::Bucket, "delete",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.delete_objects(req, std::move(b));
     }},

    // object 级：multipart
    {"POST", Scope::Object, "uploads",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.create_multipart(req, std::move(b), std::move(k));
     }},
    {"POST", Scope::Object, "uploadId",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.complete_multipart(req, std::move(b), std::move(k));
     }},
    {"PUT", Scope::Object, "partNumber",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.upload_part(req, std::move(b), std::move(k));
     }},
    {"GET", Scope::Object, "uploadId",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.list_parts(req, std::move(b), std::move(k));
     }},
    {"DELETE", Scope::Object, "uploadId",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.abort_multipart(req, std::move(b), std::move(k));
     }},

    // object 级：数据面
    {"PUT", Scope::Object, "",  // PutObject / CopyObject（按 x-amz-copy-source 分流）
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         if (req.headers.has("x-amz-copy-source"))
             return s.copy_object(req, std::move(b), std::move(k));
         return s.put_object(req, std::move(b), std::move(k));
     }},
    {"GET", Scope::Object, "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.get_object(req, std::move(b), std::move(k), false);
     }},
    {"HEAD", Scope::Object, "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.get_object(req, std::move(b), std::move(k), true);
     }},
    {"DELETE", Scope::Object, "",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string k) {
         return s.delete_object(std::move(b), std::move(k));
     }},
    };

    reject_unsupported_subresource(req);
    Scope scope = bucket.empty() ? Scope::Service
                  : key.empty() ? Scope::Bucket
                                : Scope::Object;
    for (auto& r : kRoutes) {
        if (r.method != req.method || r.scope != scope) continue;
        if (!flag_matches(req, r.flag)) continue;
        co_return co_await r.fn(*this, req, std::move(bucket), std::move(key));
    }
    throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed.");
}

// ---------- readyz（docs/05 §7：各后端探活）----------

Task<http::HttpResponse> S3Service::readyz() {
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "text/plain");
    std::string report;
    bool ok = true;
    for (auto& [name, backend] : router_.backends()) {
        try {
            co_await backend->list_buckets();
            report += name + " ok\n";
        } catch (const std::exception& e) {
            ok = false;
            report += name + " FAIL: " + e.what() + "\n";
        }
    }
    resp.status = ok ? 200 : 503;
    resp.small_body = std::move(report);
    co_return resp;
}

}  // namespace lights3::s3
