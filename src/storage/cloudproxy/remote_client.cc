#include "storage/cloudproxy/remote_client.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <stdexcept>
#include <thread>

#include "core/config.h"
#include "core/log.h"
#include "core/util/uri.h"
#include "s3/xml.h"

namespace lights3::storage {

// ---------- 配置 ----------

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
        return parse_bool(*v);  // 共享 token 集（core/config.h），与 duostore 一致
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
    // 数值范围加载期钉死，杜绝运行期算术异常（如 backoff 溢出）
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
    require_range("queue_cap", static_cast<int64_t>(c.queue_cap_bytes), 4096, 1 << 30);
    // virtual-hosted style（force_path_style=false）：连接与 SNI 恒指 endpoint，仅
    // Host/签名与路径按 bucket 变化（docs/cloudproxy-backend.md §7）；含 '.' 的
    // bucket 名在 TLS 泛域名证书下会失配，属部署侧约束，不在此拦截
    // 拼接名整体按 S3 规则校验（docs/cloudproxy-backend.md §4.3）："aaa" 代表最短合法本地名，
    // 覆盖前缀引入的字符集/首字符/".."/长度问题，注定非法的前缀在加载期即报错
    if (!c.bucket_prefix.empty()) {
        try {
            validate_bucket_name(c.bucket_prefix + "aaa");
        } catch (const s3::S3Error& e) {
            throw std::runtime_error("cloudproxy backend '" + name + "': invalid bucket_prefix '" +
                                     c.bucket_prefix + "': " + e.message);
        }
    }
    cloudproxy::Endpoint::parse(c.endpoint);  // 提前校验，错误在加载期暴露
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
    // httplib 的 Host 头：默认端口只发 host，否则 host:port（docs/cloudproxy-backend.md §2.2 一致性陷阱）
    bool default_port = ep.port == (ep.https ? 443 : 80);
    ep.signed_host = default_port ? ep.host : ep.host + ":" + std::to_string(ep.port);
    ep.base_url = std::string(ep.https ? "https://" : "http://") + ep.host + ":" +
                  std::to_string(ep.port);
    return ep;
}

// ---------- 指标（docs/cloudproxy-backend.md §8.2）----------

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
    : cfg_(cfg), ep_(ep), wait_hist_(std::move(wait_hist)) {}

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

ClientPool::Lease ClientPool::acquire() {
    auto start = std::chrono::steady_clock::now();
    auto deadline = start + std::chrono::milliseconds(cfg_.request_timeout_ms);
    // 等待时长观测（§8.2）：Lease 发出或 SlowDown 抛出都记账——排队恶化是
    // max_connections 调优的直接信号
    auto observe = [&] {
        if (wait_hist_)
            wait_hist_->observe(std::chrono::duration<double>(
                                    std::chrono::steady_clock::now() - start)
                                    .count());
    };
    std::unique_lock lk(m_);
    for (;;) {
        if (!idle_.empty()) {
            auto c = std::move(idle_.back());
            idle_.pop_back();
            observe();
            return Lease(this, std::move(c));
        }
        if (total_ < cfg_.max_connections) {
            ++total_;
            lk.unlock();
            observe();
            try {
                return Lease(this, make_client());
            } catch (...) {
                // 回滚槽位并唤醒等待者，异常（如 bad_alloc）不得永久缩池
                std::lock_guard g(m_);
                --total_;
                cv_.notify_one();
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

void ClientPool::release(std::unique_ptr<httplib::Client> c) {
    std::lock_guard lk(m_);
    idle_.push_back(std::move(c));
    cv_.notify_one();
}

// ---------- 寻址与签名管线 ----------

Target RemoteContext::target(const std::string& remote_bucket) const {
    if (cfg.force_path_style)
        return {"/" + util::aws_uri_encode(remote_bucket, /*encode_slash=*/false),
                ep.signed_host};
    // virtual-hosted（§7）：Host = <rb>.<endpoint-host>[:port]，路径不含 bucket。
    // httplib 只在 Host 缺席时自设，本管线恒显式携带签名后的 Host，无需改连接
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
    auth.sign(req, cred, payload_hash);
    httplib::Headers out;
    for (auto& [k, v] : req.headers.items()) out.emplace(k, v);
    return out;
}

// ---------- 错误映射（docs/cloudproxy-backend.md §5.1）----------

std::optional<S3ErrorCode> map_remote_code(std::string_view wire) {
    if (auto c = s3::code_from_wire(wire)) return c;
    // 本地词表没有的近义码
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
            // 体不可解析：按状态码兜底
        }
    }
    auto res = std::string(resource);
    // 错误映射计数（§8.2）：优先远端 wire code，不可解析按状态码归桶
    metrics.count_error(remote_code.empty() ? "http_" + std::to_string(status) : remote_code);

    // 429/503/SlowDown → 本地 503，客户端可退避重试
    if (status == 429 || status == 503 || remote_code == "SlowDown")
        throw S3Error(S3ErrorCode::SlowDown,
                      remote_msg.empty() ? "remote replied slow down" : remote_msg, res);

    // 403 是网关云凭证/权限故障，不透传 AccessDenied 误导客户端排查自身凭证
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
        // 条件写被上游拒绝（If-None-Match/If-Match 透传，backend.h PutCondition）：
        // 体不可解析时也要按语义映射，不能落进 InternalError
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
        // 未知 4xx 不折成 500（docs/gaps.md §3.9）：SDK 对 500 自动重试，会把
        // InvalidObjectState 这类确定性拒绝变成无限重试循环。映射为本地 400
        //（InvalidRequest 不经 public_error 抹文案），远端码与原文随消息带出
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "remote rejected request: " + std::to_string(status) +
                          (remote_code.empty() ? "" : " " + remote_code) +
                          (remote_msg.empty() ? "" : " — " + remote_msg),
                      res);
    }

    // 5xx / 其他：本地 500（不引入 502，S3 错误词表本无 BadGateway）
    throw S3Error(S3ErrorCode::InternalError,
                  "remote returned " + std::to_string(status) +
                      (remote_code.empty() ? "" : " (" + remote_code + ")"),
                  res);
}

void RemoteContext::throw_transport_error(httplib::Error err) const {
    metrics.count_error("transport");  // §8.2：连接/DNS/超时类归一桶，细节进 message
    throw S3Error(S3ErrorCode::InternalError,
                  "cloudproxy: request to " + cfg.endpoint +
                      " failed: " + httplib::to_string(err));
}

void RemoteContext::backoff(int attempt) const {
    thread_local std::mt19937 rng{std::random_device{}()};
    // 64 位算术 + 上限钳制：配置极值不得溢出为负喂给 uniform_int_distribution（UB）
    int64_t base = static_cast<int64_t>(cfg.retry_base_ms) << std::min(attempt, 10);
    base = std::clamp<int64_t>(base, 0, 60'000);
    std::uniform_int_distribution<int64_t> jitter(0, base);
    std::this_thread::sleep_for(std::chrono::milliseconds(base + jitter(rng)));
}

// ---------- 分页辅助 ----------

std::string group_skip_token(std::string_view prefix) {
    // 1024 = S3 / validate_object_key 共同的 key 字节上限：任何组内合法 key
    // 都 <= prefix+0xff…（1024 字节），而首个分歧字节 > prefix 对应位的组外
    // key 一定 > 该值 —— 不重不漏
    constexpr size_t kMaxKeyBytes = 1024;
    std::string t(prefix);
    if (t.size() < kMaxKeyBytes) t.append(kMaxKeyBytes - t.size(), '\xff');
    return t;
}

}  // namespace lights3::storage::cloudproxy
