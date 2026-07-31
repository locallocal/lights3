// cloudproxy 内部头：ClientPool（httplib::Client 连接池）+ 通用签名管线 +
// 错误映射与重试（docs/cloudproxy-backend.md §2.2/§5/§8.1）。包含 httplib，只允许被
// src/storage/cloudproxy/*.cc 与 lights3_core 内部 TU include。
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

// SigV4 的 UNSIGNED-PAYLOAD 字面值（docs/cloudproxy-backend.md §3.2）
inline constexpr const char* kUnsignedPayload = "UNSIGNED-PAYLOAD";

struct Endpoint {
    bool https = false;
    std::string host;
    int port = 0;             // 显式或按 scheme 默认
    std::string signed_host;  // 与 httplib 实际发出的 Host 头逐字节一致（docs/cloudproxy-backend.md §2.2）
    std::string base_url;     // scheme://host:port，httplib universal Client 入参

    static Endpoint parse(const std::string& url);  // 非法抛 std::runtime_error
};

// 寻址目标（docs/cloudproxy-backend.md §7）：path-style = "/bucket/…" + endpoint Host；
// virtual-hosted = "/…" + "<bucket>.<endpoint-host>" Host。两种风格下 TCP 连接与
// SNI 恒指 endpoint（ClientPool 不分化），仅 Host/签名与路径变化
struct Target {
    std::string prefix;  // path-style 为 "/<rb>"（已编码）；vhost 为空
    std::string host;    // 进 Host 头与 SigV4 的值
    std::string bucket_path() const { return prefix.empty() ? "/" : prefix; }
    std::string object_path(std::string_view encoded_key_path) const {
        return prefix + std::string(encoded_key_path);
    }
};

// 远端观测指标（docs/cloudproxy-backend.md §8.2）：空 scope 时全部为孤立实例，
// 调用无害。error 计数按远端码动态注册（get-or-create），码集有界：wire code
// 词表 + "http_<status>" + "transport"
struct RemoteMetrics {
    explicit RemoteMetrics(const MetricsScope& scope);
    std::shared_ptr<MetricHistogram> op_seconds(const char* op) const;  // 按 op 缓存注册
    void count_retry(const char* op) const;
    void count_error(const std::string& code) const;
    std::shared_ptr<MetricCounter> etag_mismatch;
    std::shared_ptr<MetricHistogram> pool_wait;

private:
    MetricsScope scope_;
    mutable std::mutex m_;  // op/code → 实例缓存（免每次进注册表全局锁）
    mutable std::map<std::string, std::shared_ptr<MetricHistogram>> ops_;
    mutable std::map<std::string, std::shared_ptr<MetricCounter>> retries_, errors_;
};

// 互斥保护的空闲栈式连接池；httplib::Client 非线程安全，独占租借（docs/cloudproxy-backend.md §8.1）
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

    Lease acquire();  // 到上限阻塞等待；等待超 request_timeout 抛 SlowDown

private:
    friend class Lease;
    std::unique_ptr<httplib::Client> make_client() const;
    void release(std::unique_ptr<httplib::Client> c);

    const CloudProxyConfig cfg_;
    const Endpoint ep_;
    std::shared_ptr<MetricHistogram> wait_hist_;  // acquire 等待时长（§8.2；可空）
    std::mutex m_;
    std::condition_variable cv_;
    std::vector<std::unique_ptr<httplib::Client>> idle_;
    int total_ = 0;
};

// 错误映射的 404 上下文（docs/cloudproxy-backend.md §5.1：404 且体不可解析时按操作补语义）
enum class ErrCtx { None, Key, Bucket, Upload };

// 远端 wire code → 本地 S3ErrorCode，含近义别名（BucketAlreadyExists、
// TooManyRequests 等）；throw_remote_error 与 complete 的 200-错误体路径共用
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

    // 寻址（docs/cloudproxy-backend.md §7）：按 force_path_style 给出路径前缀与 Host
    Target target(const std::string& remote_bucket) const;

    // 构造最小 HttpRequest 只为签名，再搬运为 httplib::Headers（docs/cloudproxy-backend.md §2.2）。
    // extra 中的 x-amz-* 自动进 SignedHeaders；Content-Type 走 httplib 参数，勿放这里。
    // host 空 = endpoint Host；vhost 请求传 Target::host
    httplib::Headers signed_headers(
        const std::string& method, const std::string& raw_path, const std::string& raw_query,
        const std::vector<std::pair<std::string, std::string>>& extra,
        const std::string& payload_hash, const std::string& host = "") const;

    // 远端错误 → 本地 S3Error（docs/cloudproxy-backend.md §5.1 映射矩阵单点实现）
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
    // 连接建立阶段的错误（PUT 类可安全重试的子集，docs/cloudproxy-backend.md §5.2）
    static bool connection_stage_error(httplib::Error e) {
        return e == httplib::Error::Connection || e == httplib::Error::ConnectionTimeout ||
               e == httplib::Error::SSLConnection;
    }
    void backoff(int attempt) const;  // base × 2^n + 抖动

    // 幂等请求的统一重试执行：fn 拿租借的 client 发一次请求；
    // 传输层错误 / 5xx / 429 按策略重试，耗尽后返回最后一次 Result。
    // op 进 §8.2 指标：每次远端往返各记一次时延，重试另计
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

// 跳过整个 common prefix 组的 start-after 值（docs/cloudproxy-backend.md §4.2）：prefix 用 0xff
// 填充到 key 长度上限。排他语义下组内 key 全部 <= 该值被跳过，组外后继 key
// 全部 > 该值不遗漏。（"末字符 +1"的旧方案会把与边界串同名的字面 key 一并跳掉）
std::string group_skip_token(std::string_view prefix);

}  // namespace lights3::storage::cloudproxy
