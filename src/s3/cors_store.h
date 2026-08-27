// CORS configuration store + rule matching (roadmap §2.1, docs/s3-protocol.md).
// Entries are managed through PUT/GET/DELETE /bucket?cors (root credential only) and
// persisted as .sys/cors/<bucket> JSON objects via SysConfigStore (write-through +
// tombstone sync, the WebsiteStore pattern).
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "s3/sys_config_store.h"

namespace lights3::s3 {

// One <CORSRule>. allowed_origins/methods are required (validated at parse time);
// allowed_headers are stored lowercase (header matching is case-insensitive per
// RFC 9110, origin matching is exact/case-sensitive like AWS)
struct CorsRule {
    std::string id;
    std::vector<std::string> allowed_origins;  // "*" or scheme://host[:port], at most one '*' each
    std::vector<std::string> allowed_methods;  // subset of GET PUT POST DELETE HEAD
    std::vector<std::string> allowed_headers;  // lowercase, at most one '*' each
    std::vector<std::string> expose_headers;
    int max_age_seconds = -1;  // -1 = unset

    bool operator==(const CorsRule&) const = default;
};

struct CorsTraits {
    using Entry = std::vector<CorsRule>;
    static constexpr std::string_view kPrefix = "cors/";
    static constexpr const char* kName = "cors";
    static std::string serialize(const Entry& rules);
    static std::optional<Entry> deserialize(const std::string& bucket, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return a != b; }
};

using CorsStore = SysConfigStore<CorsTraits>;

// Single-wildcard match (AWS semantics for AllowedOrigin/AllowedHeader): the pattern
// may contain at most one '*', matching any (possibly empty) substring
bool cors_pattern_matches(std::string_view pattern, std::string_view value);

// First rule matching (origin, method, requested headers); nullptr = no match.
// req_headers must already be lowercase
const CorsRule* match_cors_rule(const std::vector<CorsRule>& rules, const std::string& origin,
                                const std::string& method,
                                const std::vector<std::string>& req_headers);

}  // namespace lights3::s3
