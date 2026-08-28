// cloudproxy AWS credential chain (roadmap §3.3): resolves signing credentials
// when the config carries no static access_key/secret_key — the near-mandatory
// shape for EC2/EKS deployments. Order: environment (AWS_ACCESS_KEY_ID /
// AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]) → container endpoint
// (AWS_CONTAINER_CREDENTIALS_RELATIVE_URI / _FULL_URI, ECS & EKS pod identity)
// → EC2 IMDSv2 (token PUT, then the instance role's security-credentials).
// Session credentials refresh themselves ahead of expiry; a failed refresh
// keeps the previous credentials (still valid until their expiry) and
// negative results are cached briefly so a non-EC2 host does not probe the
// metadata address on every request.
#pragma once

#include <chrono>
#include <mutex>
#include <string>

namespace lights3::storage::cloudproxy {

struct AwsCreds {
    std::string access_key;
    std::string secret_key;
    std::string session_token;                       // empty for long-lived keys
    std::chrono::system_clock::time_point expiry{};  // zero = never expires
    bool valid() const { return !access_key.empty() && !secret_key.empty(); }
};

class CredentialProvider {
public:
    // name labels log lines; imds_endpoint is the metadata base URL
    // (CloudProxyConfig::imds_endpoint — overridable for tests/proxies)
    CredentialProvider(std::string name, std::string imds_endpoint);

    // Cached credentials, refreshed synchronously when missing or within the
    // expiry margin. Blocking (short-timeout HTTP to the metadata service) —
    // call on a pool or pump thread, never on an event loop.
    AwsCreds get();

private:
    AwsCreds resolve();
    static AwsCreds from_env();
    AwsCreds from_container();
    AwsCreds from_imds();

    const std::string name_;
    const std::string imds_endpoint_;
    std::mutex m_;
    AwsCreds cached_;
    std::string source_;  // last successful source, for change logging
    std::chrono::system_clock::time_point next_attempt_{};  // negative-result cache
};

}  // namespace lights3::storage::cloudproxy
