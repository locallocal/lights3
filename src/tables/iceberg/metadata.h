// Iceberg table metadata model (docs/architecture/s3-tables-design.md §7): pure functions over
// nlohmann::json, no IO. Every rejection is a RestError. nlohmann's object is key-ordered,
// so dump() is already the canonical serialization used for replay comparison
#pragma once

#include <cstdint>
#include <map>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::tables::iceberg {

using Json = nlohmann::json;

struct MetadataLimits {
    int metadata_log_keep = 100;
};

// Parse + structural validation (format-version 1|2, required fields, unique and
// referenced ids). v1 files without "schemas"/"partition-specs" get them synthesized
Json parse_and_validate(std::string_view text, size_t max_size);
void validate_metadata(const Json& md);

struct CreateTableInput {
    std::string name;
    Json schema;
    std::optional<Json> partition_spec;
    std::optional<Json> write_order;
    std::map<std::string, std::string> properties;
    int format_version = 2;
    std::string location;
    std::string table_uuid;
    int64_t now_ms = 0;
};
// Fresh ids for schema / spec / sort order and the initial metadata (design §6.5)
Json initial_metadata(const CreateTableInput& in);

// v1: mirror "schema" / "partition-spec"; v2: drop the mirrors
void synchronize_version_fields(Json& md);
// After apply_updates: prune snapshot-log, append metadata-log (bounded), stamp last-updated-ms
void finish_transition(Json& md, std::string_view prev_metadata_location, int64_t now_ms, const MetadataLimits& lim);

std::string canonical(const Json& j);
int format_version(const Json& md);
int64_t current_snapshot_id(const Json& md);
// nullptr when absent
const Json* find_snapshot(const Json& md, int64_t id);
const Json* find_schema(const Json& md, int64_t id);
// every field id of a schema, nested types included
std::vector<int64_t> schema_field_ids(const Json& schema);
// {field id -> {name, type json, required}} flattened (nested included)
struct FieldInfo {
    std::string name;
    Json type;
    bool required = false;
};
std::map<int64_t, FieldInfo> schema_fields(const Json& schema);
// Iceberg type promotion (int→long, float→double, decimal precision widening); nested
// types must keep their kind (their inner fields are compared by their own ids)
bool type_promotion_ok(const Json& from, const Json& to);
// Every field id of `next_schema` that exists in `current_schema` must keep a compatible
// type and must not turn required. Throws RestError(status) with a message
void check_schema_evolution(const Json& current_schema, const Json& next_schema, int status);

}  // namespace lights3::tables::iceberg
