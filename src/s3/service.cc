#include "s3/service.h"

#include <cctype>
#include <chrono>
#include <cstring>
#include <random>

#include "core/log.h"
#include "core/util/hex.h"
#include "s3/auth/credential_store.h"
#include "s3/errors.h"
#include "s3/handlers/common.h"
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

// InternalError/SlowDown 的原始文案可能含上游 endpoint 等内部拓扑（cloudproxy
// 传输错误直接拼 endpoint）：原文只进日志（经 request_id 关联），响应体固定文案
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

// request_start/request_end 的 RAII 配对：请求协程被驱动提前销毁（客户端断连、
// 关停）时也执行 request_end，inflight 计数不泄漏；该路径以 499 记入状态分布
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

// 明确不支持的子资源（docs/s3-protocol.md §1）：显式 501，避免落进 List/Get 兜底造成误答
constexpr std::string_view kUnsupportedSubresources[] = {
    "acl",         "policy",       "versioning",     "versions",       "website",
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

}  // namespace

// ---------- virtual-host style（docs/s3-protocol.md §2）----------

std::pair<std::string, std::string> S3Service::resolve_address(
    const http::HttpRequest& req) const {
    if (!base_domain_.empty()) {
        if (auto host = req.headers.get("Host")) {
            std::string h = *host;
            // 去端口：Host 可为 "name:port" 或 "[v6]:port"，IPv6 字面量内的 ':'
            // 不是端口分隔符（rfind 会把 "[::1]" 截成 "[:"）
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
    MetricsEndGuard mguard{metrics_, req.method, start};

    std::string access_key;
    std::string bucket, key;
    http::HttpResponse resp;
    try {
        if (req.path == "/-/healthz") {
            resp.small_body = "ok\n";
            resp.headers.set("Content-Type", "text/plain");
        } else if (req.path == "/-/metrics") {
            resp.small_body = metrics_.render(pool_stats_);
            // 后端级注册表（docs/todo.md §3.1）追加在 L2 请求指标之后
            if (backend_metrics_) resp.small_body += backend_metrics_->render();
            resp.headers.set("Content-Type", "text/plain; version=0.0.4");
        } else if (req.path == "/-/readyz") {
            resp = co_await readyz();
        } else if (req.path == "/-/admin/credentials" ||
                   req.path.rfind("/-/admin/credentials/", 0) == 0) {
            // 边界必须落在 '/'：裸前缀匹配会把 /-/admin/credentialsXYZ 也放进管理面
            resp = co_await admin_credentials(req, access_key);
        } else {
            access_key = auth_.verify(req);
            std::tie(bucket, key) = resolve_address(req);
            // 用户请求的 bucket 名在此统一过完整校验，这是**唯一**的权威闸门
            // （docs/gaps.md §1.1）。此前只查首字符是否为 '.'，而 vhost 寻址下
            // bucket 完全取自 Host 头、可含 '/' 甚至以 '/' 开头，配合 localfs 的
            // root_/bucket/key 拼接（fs::path 遇绝对路径会替换整条路径）即任意
            // 文件读；path-style 侧还能用 %00 让首字符变成 NUL 绕过保留名检查。
            // validate_bucket_name 的字符集规则一次堵死两条入口，保留名
            // （.sys）也只对 allow_reserved=true 的调用方开放——用户请求永远
            // 拿不到那个参数
            if (!bucket.empty()) storage::validate_bucket_name(bucket);
            // per-credential policy（docs/credential-management.md §10.4）：
            // GET/HEAD 为读，其余（PUT/POST/DELETE）算写
            if (cred_store_) {
                cred_store_->authorize(access_key, bucket,
                                       req.method != "GET" && req.method != "HEAD");
                // CopyObject / UploadPartCopy 的源在 header 里，不经上面的 bucket
                // 检查：对源桶单独做一次"读"授权，防 policy 凭证借 copy 读白名单外数据
                if (req.method == "PUT")
                    if (auto src = req.headers.get("x-amz-copy-source"))
                        cred_store_->authorize(access_key,
                                               handlers::parse_copy_source(*src).first,
                                               /*is_write=*/false);
            }
            resp = co_await route(req, bucket, key);
        }
    } catch (const S3Error& e) {
        metrics_.s3_error(wire_code(e.code));
        resp = error_response(public_error(e, ctx.request_id, req), ctx.request_id, head);
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

    // 访问日志（docs/s3-protocol.md §7）：一行结构化，字段序对齐 S3 access log 精简版
    double secs = mguard.finish(resp.status);
    uint64_t bytes = resp.content_length.value_or(resp.small_body.size());
    LOG_INFO("access {} {} {} {} {} {} {}ms", ctx.request_id,
             access_key.empty() ? "-" : access_key, req.method, req.path, resp.status, bytes,
             static_cast<uint64_t>(secs * 1000));
    co_return resp;
}

// ---------- 显式分派表（docs/s3-protocol.md §2）----------

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

// ---------- readyz（docs/s3-protocol.md §7：各后端探活）----------

Task<http::HttpResponse> S3Service::readyz() {
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "text/plain");

    // 端点匿名可达而探测对每个后端发真实调用（cloudproxy 是计费的远端
    // ListBuckets）：结果短缓存 + 单飞，匿名循环打不出放大流量
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
    // 协程被提前销毁（断连）也要复位单飞标记，否则 readyz 永久返回旧值
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
            // 异常文本可能含上游 endpoint 等拓扑：只进日志，不回给匿名调用方
            LOG_WARN("readyz: backend {} probe failed: {}", name, e.what());
            report += name + " FAIL\n";
        }
    }
    // 凭证表清空防护触发过（fail-open 防护，README §1.2）：报不健康引导运维介入
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
