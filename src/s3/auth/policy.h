// per-credential policy（docs/credential-management.md §10.4）。
// 独立成头：验签层（sigv4.h）要在 verify 时刻带出 policy 快照（docs/gaps.md §3.7），
// 而 credential_store.h 依赖 sigv4.h——放回去就成环了。
#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace lights3::s3 {

// 缺省（nullopt）= 无限制
struct CredentialPolicy {
    std::vector<std::string> buckets;  // bucket glob 白名单；空 = 全部
    bool readonly = false;             // true: 仅允许 GET/HEAD
    bool allows(std::string_view bucket, bool is_write) const;
};

// policy 的 JSON 字段约定（{"buckets": [...], "readonly": bool}），admin handler、
// 落盘对象与 credentials_file 共用一套；实现在 credential_store.cc（nlohmann 不进头文件）。
// 字段非法/未知抛 S3Error(InvalidRequest)
CredentialPolicy parse_policy_json(const std::string& text);
std::string policy_to_json(const CredentialPolicy& p);

}  // namespace lights3::s3
