// mTLS identity bindings (backlog-sequence ⑥, docs/usage/tls.md §2.1, docs/architecture/multi-tenancy.md
// §4.2): client-certificate subject -> credential, persisted as
// .sys/tls-identities/<encoded subject> JSON objects through SysConfigStore
// (write-through + tombstone sync, shared by every gateway on the backend).
// The subject is the field auth.tls_identity selects (subject CN or the first
// URI SAN); a bound certificate stands in for a SigV4 signature on unsigned
// requests and must agree with the signing credential's tenant on signed ones
// (S3Service::verify_identity). Managed by root through /-/admin/tls-identities
// and `lights3-ctl cred bind-cert`.
#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

#include "s3/sys_config_store.h"

namespace lights3::s3 {

struct TlsBinding {
    // the credential the certificate acts as
    std::string access_key;
    std::string comment;
    std::chrono::system_clock::time_point created;
    // root AK that bound it (audit trail)
    std::string created_by;

    bool operator==(const TlsBinding&) const = default;
};

struct TlsIdentityTraits {
    using Entry = TlsBinding;
    static constexpr std::string_view kPrefix = "tls-identities/";
    static constexpr const char* kName = "tls-identity";
    static std::string serialize(const Entry& b);
    static std::optional<Entry> deserialize(const std::string& subject, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return a != b; }
    // Subjects carry '/', '=', ':', spaces ... : percent-encode them so the object
    // key is one opaque path segment on every backend (localfs maps keys to paths)
    static std::string encode_key(const std::string& subject);
    static std::string decode_key(const std::string& suffix);
};

using TlsIdentityStore = SysConfigStore<TlsIdentityTraits>;

// Subject validity for a binding: 1..256 bytes, no control characters, no
// leading/trailing whitespace. Certificates with subjects outside this shape can
// never be bound (and never match), which keeps the object keys sane
bool valid_tls_subject(std::string_view s);
inline constexpr size_t kMaxTlsSubjectLen = 256;

}  // namespace lights3::s3
