// Iceberg view metadata (docs/s3-tables-design.md §6.3; Iceberg view spec, format
// version 1): pure functions over nlohmann::json, no IO -- the initial metadata of a
// CreateView, structural validation, and the replace (commit) protocol's requirements /
// updates. Views carry no snapshot graph: the only commit-time check is the version
// token CAS the catalog does
#pragma once

#include <cstdint>
#include <map>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>

namespace lights3::tables::iceberg {

using Json = nlohmann::json;

struct CreateViewInput {
    std::string name;
    Json schema;
    // the "view-version" object of the request: representations, default-namespace, summary
    Json view_version;
    std::map<std::string, std::string> properties;
    std::string location;
    std::string view_uuid;
    int64_t now_ms = 0;
};

// {"view-uuid","format-version":1,"location","schemas":[…],"current-version-id":1,
//  "versions":[…],"version-log":[…],"properties":{…}}
Json initial_view_metadata(const CreateViewInput& in);
// Structural validation: format-version 1, uuid, schemas / versions with unique ids,
// current-version-id present, every version's schema-id and representations. Throws
// RestError(400 / 406 / 409)
void validate_view_metadata(const Json& md);
Json parse_and_validate_view(std::string_view text, size_t max_size);

// Requirements: only assert-view-uuid (409 on mismatch, 400 on anything else)
void check_view_requirements(const Json& current, const Json& requirements);
// Updates: assign-uuid, upgrade-format-version (1), add-schema, add-view-version,
// set-current-view-version (-1 = the last added), set-location, set-properties,
// remove-properties. Returns the next metadata; version-log appended when the current
// version changes
Json apply_view_updates(const Json& current, const Json& updates, int64_t now_ms);

int64_t current_view_version_id(const Json& md);

}  // namespace lights3::tables::iceberg
