#include "s3/service.h"

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

// x-amz-id-2 比 request id 长（AWS 是一串 base64）：这里同样用 hex，24 字节
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

// 明确不支持的**请求头**（docs/gaps.md §3.4）：SSE/SSE-C、tagging、object-lock、
// ACL 授权类此前被静默吞掉——200 但语义未兑现，合规场景下客户端会据此认为对象
// 已加密/已锁定。命中即 501；x-amz-acl 单独放行 private（即本实现的实际语义）
void reject_unsupported_headers(const http::HttpRequest& req) {
    constexpr std::string_view kPrefixes[] = {
        "x-amz-server-side-encryption",  // SSE 与 SSE-C 全家（含 -customer-*、-aws-kms-*）
        "x-amz-copy-source-server-side-encryption",  // 拷贝源侧的 SSE-C 三头
        "x-amz-object-lock-",                        // mode / retain-until-date / legal-hold
        "x-amz-grant-",                              // ACL grant 五头，与 x-amz-acl 同类
    };
    constexpr std::string_view kExact[] = {
        "x-amz-tagging",
        "x-amz-website-redirect-location",
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
            // private = 本实现的唯一语义，接受；其余（public-read 等）静默接受
            // 等于谎报"已公开授权"
            if (!http::HeaderMap::ieq(v, "private")) refuse();
            continue;
        }
        if (lk == "x-amz-storage-class") {
            // 同理（docs/gaps.md §5.2）：只有 STANDARD 一种存储类，收下 GLACIER
            // 再原样回显等于替存储层撒谎——对象根本没进任何归档层
            if (!http::HeaderMap::ieq(v, "STANDARD")) refuse();
            continue;
        }
        for (auto p : kPrefixes)
            if (lk.rfind(p, 0) == 0) refuse();
        for (auto e : kExact)
            if (lk == e) refuse();
    }
}

// query 白名单（docs/gaps.md §3.5）：所有路由共同允许的 key——presigned 签名
// 参数族 + SDK 溯源参数。key 区分大小写（与 SigV4 canonical query 一致）
constexpr std::string_view kCommonQueryKeys[] = {
    "X-Amz-Algorithm",     "X-Amz-Credential", "X-Amz-Date",
    "X-Amz-Expires",       "X-Amz-Signature",  "X-Amz-SignedHeaders",
    "X-Amz-Security-Token", "X-Amz-Content-Sha256",
    "x-id",  // aws-sdk-js v3 给每个操作附带的溯源参数，无语义
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
            // 域名大小写不敏感（RFC 4343）：不归一化则 Host: B.GW.EXAMPLE.COM
            // 静默降级成 path-style，同一 URL 两种大小写指向不同资源，且 policy
            // 判定的 bucket 输入被客户端控制
            for (char& c : h)
                if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
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
                return {std::move(bucket), std::move(key), /*vhost=*/true};
            }
        }
    }
    auto [bucket, key] = parse_bucket_key(req.path);
    return {std::move(bucket), std::move(key), /*vhost=*/false};
}

// ---------- 顶层入口 ----------

Task<http::HttpResponse> S3Service::dispatch(http::HttpRequest req) {
    RequestContext ctx{make_request_id(), make_host_id(), req.cancel};
    bool head = req.method == "HEAD";
    auto start = std::chrono::steady_clock::now();
    metrics_.request_start();
    MetricsEndGuard mguard{metrics_, req.method, start};

    std::string access_key;
    std::string bucket, key;
    http::HttpResponse resp;
    try {
        // 先解析寻址再分流内部端点（docs/gaps.md §3.8）：vhost 下 req.path 是 key，
        // "/-/metrics" 可能是 mybucket 里的合法对象——按 path 精确比较会把
        // GET 变成匿名 metrics、PUT 变成"200 但对象没写"的静默丢数据。
        // 只有 path 寻址（非 vhost）下的 /-/ 前缀才进入内部分支
        auto addr = resolve_address(req);
        bool internal = !addr.vhost && req.path.rfind("/-/", 0) == 0;
        // 读端点只认 GET/HEAD（探活器常用 HEAD）；此前 PUT /-/metrics 也回 200
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
            resp.small_body = metrics_.render(pool_stats_);
            // 后端级注册表（docs/todo.md §3.1）追加在 L2 请求指标之后
            if (backend_metrics_) resp.small_body += backend_metrics_->render();
            resp.headers.set("Content-Type", "text/plain; version=0.0.4");
        } else if (internal && internal_get("/-/readyz")) {
            resp = co_await readyz();
        } else if (internal && (req.path == "/-/admin/credentials" ||
                                req.path.rfind("/-/admin/credentials/", 0) == 0)) {
            // 边界必须落在 '/'：裸前缀匹配会把 /-/admin/credentialsXYZ 也放进管理面
            resp = co_await admin_credentials(req, access_key);
        } else {
            // 授权用验签时刻的 policy 快照（docs/gaps.md §3.7）：验签后回 store
            // 二次查表的话，凭证被 sync/remove 删掉的竞态窗口里 policy 会整体
            // 消失——readonly 凭证在窗口内变成不受限凭证。快照让在途请求严格
            // 按验签时的语义完成
            auto ident = auth_.verify(req);
            access_key = ident.access_key;
            // Content-MD5 / x-amz-checksum-*（docs/gaps.md §5.6）：装在 verify 之后，
            // 于是包在 sha256/aws-chunked 装饰器之外——摘要按解帧后的明文计算，
            // 与客户端算的是同一份字节。与签名无关，认证关闭时同样生效
            install_checksum_guard(req);
            bucket = std::move(addr.bucket);
            key = std::move(addr.key);
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
            // GET/HEAD 为读，其余（PUT/POST/DELETE）算写。判定输入是 verify
            // 带出的快照，不再回 store 查表（§3.7）
            if (ident.policy) {
                auto deny = [] {
                    throw S3Error(S3ErrorCode::AccessDenied,
                                  "Access denied by credential policy.");
                };
                if (!ident.policy->allows(bucket, req.method != "GET" && req.method != "HEAD"))
                    deny();
                // CopyObject / UploadPartCopy 的源在 header 里，不经上面的 bucket
                // 检查：对源桶单独做一次"读"授权，防 policy 凭证借 copy 读白名单外数据
                if (req.method == "PUT")
                    if (auto src = req.headers.get("x-amz-copy-source"))
                        if (!ident.policy->allows(handlers::parse_copy_source(*src).first,
                                                  /*is_write=*/false))
                            deny();
            }
            // 请求级超时 + 取消接线（docs/gaps.md §3.1/§3.3）：req_src 为本请求专用，
            // 外部 token（进程关停，驱动接线后还有客户端断连）接到同一个源上——任一
            // 触发都让整条 L2/L3 链从最近的可取消挂起点（pool.schedule /
            // semaphore.acquire）以 OperationCancelled 收敛。token 沿 Task promise
            // 自动下传，无需逐个 handler/后端改签名
            CancelSource req_src;
            CancelRegistration link;
            if (ctx.cancel.valid()) {
                link = ctx.cancel.on_cancel([&req_src] { req_src.request_cancel(); });
                if (ctx.cancel.cancelled()) req_src.request_cancel();
            }
            if (request_timeout_.count() > 0)
                resp = co_await with_timeout(route(req, bucket, key), request_timeout_, req_src);
            else
                resp = co_await std::move(route(req, bucket, key).with_cancel(req_src.token()));
        }
    } catch (const OperationCancelled&) {
        // 超时/断连/关停：503 让 SDK 重试。已在池线程上执行的阻塞系统调用不被抢占，
        // 本响应只代表"网关不再等它"（docs/concurrency.md §5 的协作式语义）
        LOG_WARN("req {} {} {} cancelled (timeout or shutdown)", ctx.request_id, req.method,
                 req.path);
        metrics_.s3_error(S3ErrorCode::SlowDown);
        resp = error_response(
            S3Error(S3ErrorCode::SlowDown, "Request cancelled: timed out or server shutting down."),
            ctx, head);
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        resp = error_response(public_error(e, ctx.request_id, req), ctx, head);
    } catch (const std::exception& e) {
        LOG_ERROR("req {} {} {} internal error: {}", ctx.request_id, req.method, req.path,
                  e.what());
        metrics_.s3_error(S3ErrorCode::InternalError);
        resp = error_response(
            S3Error(S3ErrorCode::InternalError, "We encountered an internal error."), ctx, head);
    }
    resp.headers.set("x-amz-request-id", ctx.request_id);
    resp.headers.set("x-amz-id-2", ctx.host_id);
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
    {"GET", Scope::Service, "", "",
     [](S3Service& s, http::HttpRequest&, std::string, std::string) {
         return s.list_buckets();
     }},

    // bucket 级
    {"GET", Scope::Bucket, "location", "",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.get_bucket_location(std::move(b));
     }},
    // 五个参数现已全部生效（docs/gaps.md §5.1）：此前分页参数是"允许但忽略"、
    // prefix/delimiter 干脆不放行（忽略它们会把过滤范围外的 upload 混进来）
    {"GET", Scope::Bucket, "uploads",
     "max-uploads key-marker upload-id-marker prefix delimiter encoding-type",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.list_multipart_uploads(req, std::move(b));
     }},
    // ListObjectsV2 与 V1 兼容同入口。fetch-owner 允许但忽略：V2 缺省本就不回
    // Owner，忽略等价于 =false，不属于"静默误答"
    {"GET", Scope::Bucket, "",
     "list-type prefix delimiter marker continuation-token start-after max-keys "
     "encoding-type fetch-owner",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.list_objects(req, std::move(b));
     }},
    {"PUT", Scope::Bucket, "", "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.create_bucket(req, std::move(b));
     }},
    {"HEAD", Scope::Bucket, "", "",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.head_bucket(std::move(b));
     }},
    {"DELETE", Scope::Bucket, "", "",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string) {
         return s.delete_bucket(std::move(b));
     }},
    {"POST", Scope::Bucket, "delete", "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string) {
         return s.delete_objects(req, std::move(b));
     }},

    // object 级：multipart
    {"POST", Scope::Object, "uploads", "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.create_multipart(req, std::move(b), std::move(k));
     }},
    {"POST", Scope::Object, "uploadId", "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.complete_multipart(req, std::move(b), std::move(k));
     }},
    {"PUT", Scope::Object, "partNumber", "uploadId",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.upload_part(req, std::move(b), std::move(k));
     }},
    {"GET", Scope::Object, "uploadId", "max-parts part-number-marker",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.list_parts(req, std::move(b), std::move(k));
     }},
    {"DELETE", Scope::Object, "uploadId", "",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.abort_multipart(req, std::move(b), std::move(k));
     }},

    // object 级：数据面
    {"PUT", Scope::Object, "", "",  // PutObject / CopyObject（按 x-amz-copy-source 分流）
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         if (req.headers.has("x-amz-copy-source"))
             return s.copy_object(req, std::move(b), std::move(k));
         return s.put_object(req, std::move(b), std::move(k));
     }},
    // response-* 覆盖参数（docs/gaps.md §5.3）：presigned 下载链接最常用的一族
    {"GET", Scope::Object, "",
     "response-content-type response-content-language response-expires "
     "response-cache-control response-content-disposition response-content-encoding",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.get_object(req, std::move(b), std::move(k), false);
     }},
    {"HEAD", Scope::Object, "",
     "response-content-type response-content-language response-expires "
     "response-cache-control response-content-disposition response-content-encoding",
     [](S3Service& s, http::HttpRequest& req, std::string b, std::string k) {
         return s.get_object(req, std::move(b), std::move(k), true);
     }},
    {"DELETE", Scope::Object, "", "",
     [](S3Service& s, http::HttpRequest&, std::string b, std::string k) {
         return s.delete_object(std::move(b), std::move(k));
     }},
    };

    // 黑名单先行只为给已知子资源更明确的报错文案；结构性防线是下面按路由的
    // query 白名单（§3.5）与请求头检查（§3.4）
    reject_unsupported_subresource(req);
    reject_unsupported_headers(req);
    Scope scope = bucket.empty() ? Scope::Service
                  : key.empty() ? Scope::Bucket
                                : Scope::Object;
    for (auto& r : kRoutes) {
        if (r.method != req.method || r.scope != scope) continue;
        if (!flag_matches(req, r.flag)) continue;
        // 白名单（§3.5）：本路由名单外的 query key → 501。黑名单模型下任何遗漏
        // 都静默降级成"读/写整对象"（?attributes 回对象体、?partNumber 回整个
        // 对象、response-* 被吞），501 至少是诚实的
        enforce_query_whitelist(req, r);
        co_return co_await r.fn(*this, req, std::move(bucket), std::move(key));
    }
    // 405 必须带 Allow（RFC 9110 §15.5.6，docs/gaps.md §5.9）：同 scope 下同样能
    // 匹配本请求 query 的其余方法即为答案——名单由分派表本身给出，不会与之漂移
    std::string allow;
    for (auto& r : kRoutes) {
        if (r.scope != scope || !flag_matches(req, r.flag)) continue;
        if (allow.find(r.method) != std::string::npos) continue;  // 同方法多路由只列一次
        if (!allow.empty()) allow += ", ";
        allow += r.method;
    }
    // HEAD 由 GET 路由承接的驱动/上游语义：列了 GET 就一并列 HEAD
    if (allow.find("GET") != std::string::npos && allow.find("HEAD") == std::string::npos)
        allow += ", HEAD";
    throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed.")
        .with_header("Allow", allow);
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
