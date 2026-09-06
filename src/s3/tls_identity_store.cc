#include "s3/tls_identity_store.h"

#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/util/time.h"
#include "core/util/uri.h"

namespace lights3::s3 {

using nlohmann::json;

std::string TlsIdentityTraits::serialize(const Entry& b) {
    json j;
    j["version"] = 1;
    j["access_key"] = b.access_key;
    if (!b.comment.empty()) j["comment"] = b.comment;
    j["created"] = util::iso8601(b.created);
    j["created_unix"] = std::chrono::duration_cast<std::chrono::seconds>(
                            b.created.time_since_epoch())
                            .count();
    if (!b.created_by.empty()) j["created_by"] = b.created_by;
    return j.dump();
}

std::optional<TlsBinding> TlsIdentityTraits::deserialize(const std::string& subject,
                                                         const std::string& body) {
    try {
        json j = json::parse(body);
        if (!j.is_object() || !j.contains("access_key") || !j["access_key"].is_string())
            return std::nullopt;
        if (!valid_tls_subject(subject)) return std::nullopt;
        TlsBinding b;
        b.access_key = j["access_key"].get<std::string>();
        if (b.access_key.empty()) return std::nullopt;
        b.comment = j.value("comment", "");
        b.created = std::chrono::system_clock::time_point(
            std::chrono::seconds(j.value("created_unix", int64_t{0})));
        b.created_by = j.value("created_by", "");
        return b;
    } catch (const json::exception& e) {
        LOG_WARN("tls-identity: malformed binding for '{}': {}", subject, e.what());
        return std::nullopt;
    }
}

std::string TlsIdentityTraits::encode_key(const std::string& subject) {
    return util::aws_uri_encode(subject, /*encode_slash=*/true);
}

std::string TlsIdentityTraits::decode_key(const std::string& suffix) {
    return util::percent_decode(suffix);
}

bool valid_tls_subject(std::string_view s) {
    if (s.empty() || s.size() > kMaxTlsSubjectLen) return false;
    if (s.front() == ' ' || s.back() == ' ') return false;
    for (unsigned char c : s)
        if (c < 0x20 || c == 0x7f) return false;
    return true;
}

}  // namespace lights3::s3
