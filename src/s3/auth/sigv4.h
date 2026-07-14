// L2: AWS Signature V4 认证（见 docs/05-s3-protocol.md §3）
// 自实现验签 + 签名（签名端供单测与后续 cloudproxy 转发复用）。
#pragma once

#include <functional>
#include <map>
#include <string>

#include "core/config.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3 {

class SigV4Authenticator {
public:
    static SigV4Authenticator build(const AuthConfig& cfg);

    bool enabled() const { return !creds_.empty(); }
    const std::string& region() const { return region_; }

    // 验签失败抛 S3Error；返回请求方 access key（认证关闭时为空，供访问日志）。
    // 通过后如需 payload 校验，把 req.body 包装为流式校验 reader：
    //  - hex 摘要 → SHA256 校验（EOF 不匹配抛 XAmzContentSHA256Mismatch）
    //  - STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER] → aws-chunked 剥壳 +
    //    逐 chunk 签名链验证（docs/05 §3.2）
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

    std::map<std::string, std::string> creds_;  // AK → SK
    std::string region_ = "us-east-1";
    std::string service_ = "s3";
};

}  // namespace lights3::s3
