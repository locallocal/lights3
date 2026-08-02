// L2: AWS Signature V4 认证（见 docs/s3-protocol.md §3）
// 自实现验签 + 签名（签名端供单测与后续 cloudproxy 转发复用）。
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "core/config.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3 {

// 凭证查表接口（docs/s3-protocol.md §3.5、docs/credential-management.md §5.2）：验签热路径同步调用，实现须线程安全。
// build() 默认包一个静态表实现；CredentialStore 实现本接口后经 set_provider 注入
struct ICredentialProvider {
    virtual ~ICredentialProvider() = default;
    virtual std::optional<std::string> secret_for(std::string_view access_key) const = 0;
    virtual bool has_credentials() const = 0;
};

class SigV4Authenticator {
public:
    static SigV4Authenticator build(const AuthConfig& cfg);

    // require_auth_ 为 atomic（不可隐式拷贝/移动）：装配期按值搬运须显式定义
    SigV4Authenticator() = default;
    SigV4Authenticator(SigV4Authenticator&& o) noexcept
        : clock(std::move(o.clock)),
          provider_(std::move(o.provider_)),
          require_auth_(o.require_auth_.load()),
          region_(std::move(o.region_)),
          service_(std::move(o.service_)) {}
    SigV4Authenticator(const SigV4Authenticator& o)
        : clock(o.clock),
          provider_(o.provider_),
          require_auth_(o.require_auth_.load()),
          region_(o.region_),
          service_(o.service_) {}

    void set_provider(std::shared_ptr<const ICredentialProvider> p) {
        provider_ = std::move(p);
        if (provider_ && provider_->has_credentials()) require_auth_.store(true);
    }

    // 认证开关只升不降（docs/code-review/README.md §1.2 fail-open）：一旦观察到
    // 凭证表非空即固化为"必须认证"，运行期表被清空不再放行匿名（此后未知 AK 走
    // InvalidAccessKeyId，fail-closed）；表由空变非空仍即时开启（首个动态凭证生成即生效）
    bool enabled() const {
        if (require_auth_.load(std::memory_order_relaxed)) return true;
        bool has = provider_ && provider_->has_credentials();
        if (has) require_auth_.store(true, std::memory_order_relaxed);
        return has;
    }
    const std::string& region() const { return region_; }

    // 验签失败抛 S3Error；返回请求方 access key（认证关闭时为空，供访问日志）。
    // 通过后如需 payload 校验，把 req.body 包装为流式校验 reader：
    //  - hex 摘要 → SHA256 校验（EOF 不匹配抛 XAmzContentSHA256Mismatch）
    //  - STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER] → aws-chunked 剥壳 +
    //    逐 chunk 签名链验证（docs/s3-protocol.md §3.2）
    //  - STREAMING-UNSIGNED-PAYLOAD-TRAILER → 仅剥壳
    std::string verify(http::HttpRequest& req) const;

    // presigned URL 的 X-Amz-Expires 上限（7 天，与 S3 一致）
    static constexpr long kMaxPresignExpires = 7 * 24 * 3600;

    // 为请求补充 x-amz-date / x-amz-content-sha256 / Authorization
    // （payload_hash 传空则按空 body 计算）
    void sign(http::HttpRequest& req, const Credential& cred,
              std::string payload_hash = "") const;

    // 时钟可注入（单测固定时间）
    std::function<util::SysTime()> clock = [] { return std::chrono::system_clock::now(); };

    static constexpr int kMaxClockSkewSec = 15 * 60;

private:
    // presigned=true 时把 X-Amz-Signature 排除出 canonical query（仅 presigned 请求）
    std::string signature_for(const http::HttpRequest& req, const std::string& secret_key,
                              const std::string& amz_date, const std::string& scope,
                              const std::string& signed_headers,
                              const std::string& payload_hash, bool presigned = false) const;

    std::shared_ptr<const ICredentialProvider> provider_;
    mutable std::atomic<bool> require_auth_{false};
    std::string region_ = "us-east-1";
    std::string service_ = "s3";
};

}  // namespace lights3::s3
