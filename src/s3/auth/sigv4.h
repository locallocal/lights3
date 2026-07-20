// L2: AWS Signature V4 认证（见 docs/s3-protocol.md §3）
// 自实现验签 + 签名（签名端供单测与后续 cloudproxy 转发复用）。
#pragma once

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

    void set_provider(std::shared_ptr<const ICredentialProvider> p) { provider_ = std::move(p); }

    // 注意：接入 CredentialStore 后该状态是动态的（首个动态凭证生成即开启）
    bool enabled() const { return provider_ && provider_->has_credentials(); }
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
    std::string signature_for(const http::HttpRequest& req, const std::string& secret_key,
                              const std::string& amz_date, const std::string& scope,
                              const std::string& signed_headers,
                              const std::string& payload_hash) const;

    std::shared_ptr<const ICredentialProvider> provider_;
    std::string region_ = "us-east-1";
    std::string service_ = "s3";
};

}  // namespace lights3::s3
