// Identifier rules and path helpers for the table catalog (docs/architecture/s3-tables-design.md §4.5).
// Segments are ^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$ so a namespace level or table name
// is always a safe object-key segment and never collides with the reserved segments
// ("_ns.json", "tbl") of the catalog layout
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::tables {

using Levels = std::vector<std::string>;

constexpr size_t kMaxSegment = 64;
constexpr size_t kMaxNamespace = 512;

bool valid_segment(std::string_view s);
// Throws RestError(400) with the offending value
void require_segment(std::string_view what, std::string_view s);
void require_namespace(const Levels& levels);

// URL path segment of a namespace: percent-decoded then split on U+001F; a value
// without the separator but with '.' is split on '.' (legacy client behaviour).
// Validates every level
Levels parse_namespace_path(std::string_view raw_segment);
// JSON "namespace": ["a","b"] (also accepts a single string "a.b")
Levels parse_namespace_json(const nlohmann::json& v);

// "a/b" -- the storage / key path form
std::string ns_path(const Levels& levels);
// "a.b" -- logs and audit
std::string ns_display(const Levels& levels);

// Table location checks (design §4.4 / §7.2): must be s3://<bucket>/<key>, inside the
// bucket, no "..", not under the reserved prefix. Returns the bucket-relative key
// (no trailing '/'). Throws RestError(400)
std::string location_to_key(std::string_view bucket, std::string_view reserved_prefix, std::string_view location);
// Any s3:// / s3a:// path inside the bucket -> key (used for metadata / manifest paths).
// Throws RestError(409 CommitFailedException) when the path points elsewhere
std::string path_to_key(std::string_view bucket, std::string_view path);
inline std::string key_to_location(std::string_view bucket, std::string_view key) {
    return "s3://" + std::string(bucket) + "/" + std::string(key);
}

// base64url without padding (page tokens, version tokens)
std::string base64url(std::string_view raw);
std::string base64url_decode(std::string_view text);
// 16 bytes of CSPRNG entropy as lowercase hex / uuid v4 text
std::string random_hex16();
std::string new_uuid();
bool looks_like_uuid(std::string_view s);

int64_t now_ms();
int64_t now_unix();

}  // namespace lights3::tables
