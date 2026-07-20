// cloudproxy 内部头：ClientPool（httplib::Client 连接池）+ 通用签名管线 +
// 错误映射与重试（docs/cloudproxy-backend.md §2.2/§5/§8.1）。包含 httplib，只允许被
// src/storage/cloudproxy/*.cc 与 lights3_core 内部 TU include。
#pragma once

#include <httplib/httplib.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

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

// 互斥保护的空闲栈式连接池；httplib::Client 非线程安全，独占租借（docs/cloudproxy-backend.md §8.1）
class ClientPool {
public:
    ClientPool(const CloudProxyConfig& cfg, const Endpoint& ep);

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
    RemoteContext(CloudProxyConfig cfg_in, Endpoint ep_in)
        : cfg(std::move(cfg_in)),
          ep(ep_in),
          pool(cfg, ep),
          auth(s3::SigV4Authenticator::build(
              AuthConfig{{}, cfg.region, "s3"})),
          cred{cfg.access_key, cfg.secret_key} {}

    // 构造最小 HttpRequest 只为签名，再搬运为 httplib::Headers（docs/cloudproxy-backend.md §2.2）。
    // extra 中的 x-amz-* 自动进 SignedHeaders；Content-Type 走 httplib 参数，勿放这里
    httplib::Headers signed_headers(
        const std::string& method, const std::string& raw_path, const std::string& raw_query,
        const std::vector<std::pair<std::string, std::string>>& extra,
        const std::string& payload_hash) const;

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
    // 传输层错误 / 5xx / 429 按策略重试，耗尽后返回最后一次 Result
    template <class F>
    httplib::Result with_retry(F&& fn) {
        for (int attempt = 0;; ++attempt) {
            httplib::Result r = [&] {
                auto lease = pool.acquire();
                return fn(lease.client());
            }();
            bool retry = !r ? retryable_transport(r.error()) : retryable_status(r->status);
            if (!retry || attempt >= cfg.retry_max) return r;
            backoff(attempt);
        }
    }

    CloudProxyConfig cfg;
    Endpoint ep;
    ClientPool pool;
    s3::SigV4Authenticator auth;
    Credential cred;
};

// 跳过整个 common prefix 组的 start-after 值（docs/cloudproxy-backend.md §4.2）：prefix 用 0xff
// 填充到 key 长度上限。排他语义下组内 key 全部 <= 该值被跳过，组外后继 key
// 全部 > 该值不遗漏。（"末字符 +1"的旧方案会把与边界串同名的字面 key 一并跳掉）
std::string group_skip_token(std::string_view prefix);

}  // namespace lights3::storage::cloudproxy
