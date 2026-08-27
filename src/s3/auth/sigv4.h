// L2: AWS Signature V4 authentication (see docs/s3-protocol.md §3)
// Self-implemented verification + signing (the signing side is reused by unit tests and later by cloudproxy forwarding).
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "core/config.h"
#include "core/util/crypto.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/auth/policy.h"
#include "s3/errors.h"

namespace lights3::s3 {

// Credential lookup interface (docs/s3-protocol.md §3.5, docs/credential-management.md §5.2): called synchronously on the verification hot path; implementations must be thread-safe.
// build() wraps a static-table implementation by default; CredentialStore implements this interface and is injected via set_provider.
// A single lookup returns both the SK and a policy snapshot (docs/archive/gaps.md §3.7): querying the store again for the
// policy after verify risks the credential having been deleted by sync/remove -- a miss then is not "unrestricted" but a race window
struct CredentialLookup {
    util::SecretString secret_key;  // wiped on destruction (docs/archive/gaps.md §4)
    std::optional<CredentialPolicy> policy;  // snapshot at lookup time; nullopt = unrestricted
    // STS session credentials (roadmap §2.6): set for session AKs. verify() then
    // requires a matching X-Amz-Security-Token (mismatch -> InvalidToken) and refuses
    // past-expiry requests (ExpiredToken). Returned by the same single lookup so the
    // §3.7 snapshot invariant holds for sessions too
    std::optional<std::string> session_token;
    std::optional<std::chrono::system_clock::time_point> session_expires;
};

struct ICredentialProvider {
    virtual ~ICredentialProvider() = default;
    virtual std::optional<CredentialLookup> lookup(std::string_view access_key) const = 0;
    virtual bool has_credentials() const = 0;

    // Convenience SK accessor (tests/signing side); hot path should call lookup directly to avoid a second lookup
    std::optional<util::SecretString> secret_for(std::string_view access_key) const {
        auto l = lookup(access_key);
        if (!l) return std::nullopt;
        return std::move(l->secret_key);
    }
};

// Result of verify: the requester's identity + the policy snapshot from the moment of verification. Authorization
// decisions must use this snapshot rather than a second store lookup -- under an in-flight revocation race, a
// missed second lookup would make the policy vanish entirely
// (a readonly credential becomes unrestricted within the window, docs/archive/gaps.md §3.7)
struct VerifiedIdentity {
    std::string access_key;                  // empty when auth is disabled (for access logs)
    std::optional<CredentialPolicy> policy;  // nullopt = unrestricted
};

class SigV4Authenticator {
public:
    static SigV4Authenticator build(const AuthConfig& cfg);

    // require_auth_ is atomic (not implicitly copyable/movable): moving by value during assembly needs explicit definitions
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

    // The auth switch only ratchets up, never down (prevents fail-open): once the credential table is observed
    // non-empty, "auth required" is latched, and a table emptied at runtime no longer admits anonymous requests
    // (unknown AKs then get InvalidAccessKeyId, fail-closed); a table going from empty to non-empty still enables
    // auth immediately (the first generated dynamic credential takes effect at once)
    bool enabled() const {
        if (require_auth_.load(std::memory_order_relaxed)) return true;
        bool has = provider_ && provider_->has_credentials();
        if (has) require_auth_.store(true, std::memory_order_relaxed);
        return has;
    }
    const std::string& region() const { return region_; }

    // Verification failure throws S3Error; returns the requester identity and the policy snapshot at verify time.
    // If payload verification is needed after passing, wrap req.body in a streaming verifying reader:
    //  - hex digest -> SHA256 verification (mismatch at EOF throws XAmzContentSHA256Mismatch)
    //  - STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER] -> aws-chunked de-framing +
    //    per-chunk signature chain verification (docs/s3-protocol.md §3.2)
    //  - STREAMING-UNSIGNED-PAYLOAD-TRAILER -> de-framing only
    VerifiedIdentity verify(http::HttpRequest& req) const {
        return verify_impl(req, service_, nullptr);
    }
    // STS AssumeRole endpoint (roadmap §2.6): scope service "sts", payload hash computed
    // by the caller from the already-read form body (generic SigV4 carries the hash only
    // inside the canonical request, not as an x-amz-content-sha256 header)
    VerifiedIdentity verify_sts(http::HttpRequest& req, const std::string& payload_hash) const {
        return verify_impl(req, "sts", &payload_hash);
    }

    // X-Amz-Expires cap for presigned URLs (7 days, matching S3)
    static constexpr long kMaxPresignExpires = 7 * 24 * 3600;

    // Adds x-amz-date / x-amz-content-sha256 / Authorization to the request
    // (empty payload_hash computes as an empty body)
    void sign(http::HttpRequest& req, const Credential& cred,
              std::string payload_hash = "") const;

    // Injectable clock (fixed time in unit tests)
    std::function<util::SysTime()> clock = [] { return std::chrono::system_clock::now(); };

    static constexpr int kMaxClockSkewSec = 15 * 60;

private:
    // service = expected credential scope service; explicit_payload_hash != nullptr
    // supplies the hash directly (STS form POST) and skips body decorators — the caller
    // already consumed and hashed the body
    VerifiedIdentity verify_impl(http::HttpRequest& req, std::string_view service,
                                 const std::string* explicit_payload_hash) const;
    // With presigned=true, X-Amz-Signature is excluded from the canonical query (presigned requests only)
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
