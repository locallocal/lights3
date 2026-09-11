#include "tables/iceberg/metadata.h"

#include <algorithm>
#include <cstdio>
#include <set>

#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

const std::set<std::string> kPrimitiveTypes = {"boolean", "int",       "long",  "float", "double",        "date",
                                               "time",    "timestamp", "timestamptz", "string", "uuid", "binary",
                                               "timestamp_ns", "timestamptz_ns", "unknown"};

bool is_primitive(const Json& t) {
    if (!t.is_string()) return false;
    std::string s = t.get<std::string>();
    if (kPrimitiveTypes.count(s)) return true;
    if (s.rfind("decimal(", 0) == 0 && s.back() == ')') return true;
    if (s.rfind("fixed[", 0) == 0 && s.back() == ']') return true;
    return false;
}

void require(bool cond, const std::string& msg) {
    if (!cond) throw bad_request(msg);
}

// Validates a type tree; collects field ids
void walk_type(const Json& t, std::vector<int64_t>& ids, std::map<int64_t, FieldInfo>* fields, int depth) {
    require(depth < 64, "schema nesting is too deep");
    if (is_primitive(t)) return;
    require(t.is_object() && t.contains("type") && t["type"].is_string(), "schema type must be a primitive name or an object");
    std::string kind = t["type"].get<std::string>();
    if (kind == "struct") {
        require(t.contains("fields") && t["fields"].is_array(), "struct type requires 'fields'");
        for (auto& f : t["fields"]) {
            require(f.is_object() && f.contains("id") && f["id"].is_number_integer(), "struct field requires an integer 'id'");
            require(f.contains("name") && f["name"].is_string() && !f["name"].get<std::string>().empty(),
                    "struct field requires a 'name'");
            require(f.contains("type"), "struct field requires a 'type'");
            int64_t id = f["id"].get<int64_t>();
            ids.push_back(id);
            if (fields) (*fields)[id] = FieldInfo{f["name"].get<std::string>(), f["type"], f.value("required", false)};
            walk_type(f["type"], ids, fields, depth + 1);
        }
    } else if (kind == "list") {
        require(t.contains("element-id") && t["element-id"].is_number_integer(), "list type requires 'element-id'");
        require(t.contains("element"), "list type requires 'element'");
        int64_t id = t["element-id"].get<int64_t>();
        ids.push_back(id);
        if (fields) (*fields)[id] = FieldInfo{"element", t["element"], t.value("element-required", false)};
        walk_type(t["element"], ids, fields, depth + 1);
    } else if (kind == "map") {
        require(t.contains("key-id") && t.contains("value-id") && t.contains("key") && t.contains("value"),
                "map type requires key-id/value-id/key/value");
        int64_t kid = t["key-id"].get<int64_t>(), vid = t["value-id"].get<int64_t>();
        ids.push_back(kid);
        ids.push_back(vid);
        if (fields) {
            (*fields)[kid] = FieldInfo{"key", t["key"], true};
            (*fields)[vid] = FieldInfo{"value", t["value"], t.value("value-required", false)};
        }
        walk_type(t["key"], ids, fields, depth + 1);
        walk_type(t["value"], ids, fields, depth + 1);
    } else {
        throw bad_request("unknown schema type '" + kind + "'");
    }
}

void validate_schema(const Json& s) {
    require(s.is_object() && s.value("type", "") == "struct", "schema must be a struct");
    std::vector<int64_t> ids;
    walk_type(s, ids, nullptr, 0);
    std::set<int64_t> seen;
    for (auto id : ids) require(seen.insert(id).second, "schema field id " + std::to_string(id) + " is not unique");
}

const std::set<std::string> kTransforms = {"identity", "year", "month", "day", "hour", "void"};

bool valid_transform(const std::string& t) {
    if (kTransforms.count(t)) return true;
    for (const char* p : {"bucket[", "truncate["}) {
        std::string pre = p;
        if (t.rfind(pre, 0) == 0 && t.back() == ']') {
            std::string n = t.substr(pre.size(), t.size() - pre.size() - 1);
            if (n.empty()) return false;
            for (char c : n)
                if (c < '0' || c > '9') return false;
            return true;
        }
    }
    return false;
}

void validate_spec(const Json& spec, const std::set<int64_t>& field_ids) {
    require(spec.is_object() && spec.contains("spec-id") && spec["spec-id"].is_number_integer(),
            "partition spec requires an integer 'spec-id'");
    require(spec.contains("fields") && spec["fields"].is_array(), "partition spec requires 'fields'");
    std::set<int64_t> seen;
    for (auto& f : spec["fields"]) {
        require(f.is_object() && f.contains("source-id") && f.contains("field-id") && f.contains("name") &&
                    f.contains("transform"),
                "partition field requires source-id/field-id/name/transform");
        require(field_ids.count(f["source-id"].get<int64_t>()) > 0,
                "partition field source-id " + std::to_string(f["source-id"].get<int64_t>()) + " is not in the schema");
        require(valid_transform(f["transform"].get<std::string>()),
                "unknown partition transform '" + f["transform"].get<std::string>() + "'");
        require(seen.insert(f["field-id"].get<int64_t>()).second, "partition field-id is not unique");
    }
}

void validate_sort_order(const Json& so, const std::set<int64_t>& field_ids) {
    require(so.is_object() && so.contains("order-id") && so["order-id"].is_number_integer(),
            "sort order requires an integer 'order-id'");
    require(so.contains("fields") && so["fields"].is_array(), "sort order requires 'fields'");
    for (auto& f : so["fields"]) {
        require(f.is_object() && f.contains("source-id") && f.contains("transform") && f.contains("direction") &&
                    f.contains("null-order"),
                "sort field requires source-id/transform/direction/null-order");
        require(field_ids.count(f["source-id"].get<int64_t>()) > 0, "sort field source-id is not in the schema");
        require(valid_transform(f["transform"].get<std::string>()), "unknown sort transform");
        std::string d = f["direction"].get<std::string>(), n = f["null-order"].get<std::string>();
        require(d == "asc" || d == "desc", "sort direction must be asc|desc");
        require(n == "nulls-first" || n == "nulls-last", "null-order must be nulls-first|nulls-last");
    }
}

std::set<int64_t> all_field_ids(const Json& md) {
    std::set<int64_t> ids;
    for (auto& s : md["schemas"])
        for (auto id : schema_field_ids(s)) ids.insert(id);
    return ids;
}

}  // namespace

std::vector<int64_t> schema_field_ids(const Json& schema) {
    std::vector<int64_t> ids;
    walk_type(schema, ids, nullptr, 0);
    return ids;
}

std::map<int64_t, FieldInfo> schema_fields(const Json& schema) {
    std::vector<int64_t> ids;
    std::map<int64_t, FieldInfo> out;
    walk_type(schema, ids, &out, 0);
    return out;
}

std::string canonical(const Json& j) { return j.dump(); }

bool type_promotion_ok(const Json& from, const Json& to) {
    if (from == to) return true;
    if (from.is_object() || to.is_object()) {
        // struct / list / map: same kind; nested fields are judged by their own ids
        return from.is_object() && to.is_object() && from.value("type", "") == to.value("type", "");
    }
    if (!from.is_string() || !to.is_string()) return false;
    std::string a = from.get<std::string>(), b = to.get<std::string>();
    if (a == "int" && b == "long") return true;
    if (a == "float" && b == "double") return true;
    if (a.rfind("decimal(", 0) == 0 && b.rfind("decimal(", 0) == 0) {
        int p1 = 0, s1 = 0, p2 = 0, s2 = 0;
        if (std::sscanf(a.c_str(), "decimal(%d,%d)", &p1, &s1) == 2 &&
            std::sscanf(b.c_str(), "decimal(%d,%d)", &p2, &s2) == 2)
            return s1 == s2 && p2 >= p1;
    }
    return false;
}

void check_schema_evolution(const Json& current_schema, const Json& next_schema, int status) {
    auto cur = schema_fields(current_schema);
    for (auto& [id, f] : schema_fields(next_schema)) {
        auto it = cur.find(id);
        if (it == cur.end()) continue;
        if (!type_promotion_ok(it->second.type, f.type))
            throw RestError(status, status == 409 ? "CommitFailedException" : "BadRequestException",
                            "field " + std::to_string(id) + " cannot change type from " + it->second.type.dump() +
                                " to " + f.type.dump());
        if (!it->second.required && f.required)
            throw RestError(status, status == 409 ? "CommitFailedException" : "BadRequestException",
                            "field " + std::to_string(id) + " cannot become required");
    }
}

int format_version(const Json& md) { return md.value("format-version", 0); }

int64_t current_snapshot_id(const Json& md) {
    auto it = md.find("current-snapshot-id");
    if (it == md.end() || it->is_null()) return -1;
    return it->get<int64_t>();
}

const Json* find_snapshot(const Json& md, int64_t id) {
    auto it = md.find("snapshots");
    if (it == md.end()) return nullptr;
    for (auto& s : *it)
        if (s.value("snapshot-id", int64_t(-1)) == id) return &s;
    return nullptr;
}

const Json* find_schema(const Json& md, int64_t id) {
    auto it = md.find("schemas");
    if (it == md.end()) return nullptr;
    for (auto& s : *it)
        if (s.value("schema-id", int64_t(-1)) == id) return &s;
    return nullptr;
}

void validate_metadata(const Json& mdc) {
    Json& md = const_cast<Json&>(mdc);
    require(md.is_object(), "table metadata must be a JSON object");
    int fv = format_version(md);
    if (fv != 1 && fv != 2) throw unsupported("Iceberg format-version " + std::to_string(fv) + " is not supported");
    require(md.contains("table-uuid") && md["table-uuid"].is_string(), "metadata requires 'table-uuid'");
    require(md.contains("location") && md["location"].is_string(), "metadata requires 'location'");
    require(md.contains("last-column-id") && md["last-column-id"].is_number_integer(),
            "metadata requires 'last-column-id'");
    // v1 mirrors -> lists
    if (fv == 1 && !md.contains("schemas") && md.contains("schema")) {
        Json s = md["schema"];
        if (!s.contains("schema-id")) s["schema-id"] = 0;
        md["schemas"] = Json::array({s});
        md["current-schema-id"] = s["schema-id"];
    }
    if (fv == 1 && !md.contains("partition-specs") && md.contains("partition-spec")) {
        Json spec;
        spec["spec-id"] = 0;
        spec["fields"] = md["partition-spec"];
        md["partition-specs"] = Json::array({spec});
        md["default-spec-id"] = 0;
    }
    if (!md.contains("sort-orders")) {
        Json so;
        so["order-id"] = 0;
        so["fields"] = Json::array();
        md["sort-orders"] = Json::array({so});
        md["default-sort-order-id"] = 0;
    }
    require(md.contains("schemas") && md["schemas"].is_array() && !md["schemas"].empty(),
            "metadata requires a non-empty 'schemas' list");
    std::set<int64_t> schema_ids;
    for (auto& s : md["schemas"]) {
        require(s.contains("schema-id") && s["schema-id"].is_number_integer(), "schema requires 'schema-id'");
        validate_schema(s);
        require(schema_ids.insert(s["schema-id"].get<int64_t>()).second, "schema-id is not unique");
    }
    require(md.contains("current-schema-id") && schema_ids.count(md["current-schema-id"].get<int64_t>()),
            "current-schema-id does not reference a schema");
    auto fids = all_field_ids(md);
    require(md.contains("partition-specs") && md["partition-specs"].is_array() && !md["partition-specs"].empty(),
            "metadata requires a non-empty 'partition-specs' list");
    std::set<int64_t> spec_ids;
    for (auto& s : md["partition-specs"]) {
        validate_spec(s, fids);
        require(spec_ids.insert(s["spec-id"].get<int64_t>()).second, "spec-id is not unique");
    }
    require(md.contains("default-spec-id") && spec_ids.count(md["default-spec-id"].get<int64_t>()),
            "default-spec-id does not reference a partition spec");
    if (!md.contains("last-partition-id")) {
        int64_t maxid = 999;
        for (auto& s : md["partition-specs"])
            for (auto& f : s["fields"]) maxid = std::max(maxid, f["field-id"].get<int64_t>());
        md["last-partition-id"] = maxid;
    }
    std::set<int64_t> order_ids;
    for (auto& s : md["sort-orders"]) {
        validate_sort_order(s, fids);
        require(order_ids.insert(s["order-id"].get<int64_t>()).second, "order-id is not unique");
    }
    require(md.contains("default-sort-order-id") && order_ids.count(md["default-sort-order-id"].get<int64_t>()),
            "default-sort-order-id does not reference a sort order");
    if (!md.contains("snapshots")) md["snapshots"] = Json::array();
    require(md["snapshots"].is_array(), "'snapshots' must be a list");
    std::set<int64_t> snap_ids;
    for (auto& s : md["snapshots"]) {
        require(s.is_object() && s.contains("snapshot-id") && s["snapshot-id"].is_number_integer(),
                "snapshot requires 'snapshot-id'");
        require(snap_ids.insert(s["snapshot-id"].get<int64_t>()).second, "snapshot-id is not unique");
        require(s.contains("timestamp-ms") && s["timestamp-ms"].is_number_integer(), "snapshot requires 'timestamp-ms'");
        if (fv == 2) {
            require(s.contains("manifest-list") && s["manifest-list"].is_string(),
                    "v2 snapshots require 'manifest-list'");
            require(s.contains("sequence-number") && s["sequence-number"].is_number_integer(),
                    "v2 snapshots require 'sequence-number'");
        } else {
            require((s.contains("manifest-list") && s["manifest-list"].is_string()) ||
                        (s.contains("manifests") && s["manifests"].is_array()),
                    "v1 snapshots require 'manifest-list' or 'manifests'");
        }
    }
    int64_t cur = current_snapshot_id(md);
    require(cur == -1 || snap_ids.count(cur), "current-snapshot-id does not reference a snapshot");
    if (!md.contains("refs")) md["refs"] = Json::object();
    require(md["refs"].is_object(), "'refs' must be an object");
    for (auto& [name, ref] : md["refs"].items()) {
        require(ref.is_object() && ref.contains("snapshot-id") && ref.contains("type"), "ref requires snapshot-id/type");
        require(snap_ids.count(ref["snapshot-id"].get<int64_t>()), "ref '" + name + "' points to an unknown snapshot");
        std::string t = ref["type"].get<std::string>();
        require(t == "branch" || t == "tag", "ref type must be branch|tag");
    }
    if (fv == 2) {
        if (!md.contains("last-sequence-number")) md["last-sequence-number"] = 0;
        require(md["last-sequence-number"].is_number_integer(), "'last-sequence-number' must be an integer");
    }
    if (!md.contains("properties")) md["properties"] = Json::object();
    require(md["properties"].is_object(), "'properties' must be an object");
    for (auto& [k, v] : md["properties"].items()) require(v.is_string(), "property '" + k + "' must be a string");
    if (!md.contains("snapshot-log")) md["snapshot-log"] = Json::array();
    if (!md.contains("metadata-log")) md["metadata-log"] = Json::array();
    if (!md.contains("last-updated-ms")) md["last-updated-ms"] = 0;
}

Json parse_and_validate(std::string_view text, size_t max_size) {
    if (text.size() > max_size) throw bad_request("table metadata exceeds the size limit");
    Json md;
    try {
        md = Json::parse(text);
    } catch (const Json::exception&) {
        throw bad_request("table metadata is not valid JSON");
    }
    validate_metadata(md);
    return md;
}

// ---------- fresh ids (design §6.5) ----------

namespace {

struct IdAssigner {
    int64_t next = 1;
    Json assign(const Json& t) {
        if (is_primitive(t)) return t;
        require(t.is_object() && t.contains("type"), "schema type must be a primitive name or an object");
        std::string kind = t["type"].get<std::string>();
        Json out = t;
        if (kind == "struct") {
            require(t.contains("fields") && t["fields"].is_array(), "struct type requires 'fields'");
            Json fields = Json::array();
            for (auto& f : t["fields"]) {
                require(f.is_object() && f.contains("name") && f.contains("type"), "struct field requires name/type");
                Json nf = f;
                nf["id"] = next++;
                nf["type"] = assign(f["type"]);
                if (!nf.contains("required")) nf["required"] = false;
                fields.push_back(std::move(nf));
            }
            out["fields"] = std::move(fields);
        } else if (kind == "list") {
            require(t.contains("element"), "list type requires 'element'");
            out["element-id"] = next++;
            out["element"] = assign(t["element"]);
            if (!out.contains("element-required")) out["element-required"] = false;
        } else if (kind == "map") {
            require(t.contains("key") && t.contains("value"), "map type requires key/value");
            out["key-id"] = next++;
            out["value-id"] = next++;
            out["key"] = assign(t["key"]);
            out["value"] = assign(t["value"]);
            if (!out.contains("value-required")) out["value-required"] = false;
        } else {
            throw bad_request("unknown schema type '" + kind + "'");
        }
        return out;
    }
};

// Maps the client's (possibly arbitrary) field ids onto the freshly assigned ones so
// partition / sort sources keep pointing at the same columns
std::map<int64_t, int64_t> id_mapping(const Json& client, const Json& fresh) {
    std::vector<int64_t> a = schema_field_ids(client), b = schema_field_ids(fresh);
    std::map<int64_t, int64_t> m;
    for (size_t i = 0; i < a.size() && i < b.size(); ++i) m[a[i]] = b[i];
    return m;
}

}  // namespace

Json initial_metadata(const CreateTableInput& in) {
    if (in.format_version != 1 && in.format_version != 2)
        throw unsupported("Iceberg format-version " + std::to_string(in.format_version) + " is not supported");
    require(in.schema.is_object() && in.schema.value("type", "") == "struct", "schema must be a struct");
    // client ids are only needed to remap spec / order sources; assign ids if absent
    Json client_schema = in.schema;
    bool client_has_ids = true;
    if (client_schema.contains("fields"))
        for (auto& f : client_schema["fields"])
            if (!f.contains("id")) client_has_ids = false;
    std::map<int64_t, int64_t> remap;
    IdAssigner assigner;
    Json schema = assigner.assign(in.schema);
    if (client_has_ids) {
        std::vector<int64_t> tmp;
        walk_type(client_schema, tmp, nullptr, 0);
        remap = id_mapping(client_schema, schema);
    }
    schema["schema-id"] = 0;
    if (schema.contains("identifier-field-ids")) {
        Json ids = Json::array();
        for (auto& id : schema["identifier-field-ids"]) {
            auto it = remap.find(id.get<int64_t>());
            ids.push_back(it == remap.end() ? id.get<int64_t>() : it->second);
        }
        schema["identifier-field-ids"] = ids;
    }
    int64_t last_column_id = assigner.next - 1;
    auto fids = schema_field_ids(schema);
    std::set<int64_t> fid_set(fids.begin(), fids.end());
    auto remap_source = [&](int64_t src) {
        auto it = remap.find(src);
        int64_t out = it == remap.end() ? src : it->second;
        require(fid_set.count(out) > 0, "source-id " + std::to_string(src) + " is not in the schema");
        return out;
    };

    Json spec;
    spec["spec-id"] = 0;
    spec["fields"] = Json::array();
    int64_t last_partition_id = 999;
    if (in.partition_spec && in.partition_spec->is_object() && in.partition_spec->contains("fields")) {
        for (auto& f : (*in.partition_spec)["fields"]) {
            require(f.is_object() && f.contains("source-id") && f.contains("name") && f.contains("transform"),
                    "partition field requires source-id/name/transform");
            Json nf;
            nf["source-id"] = remap_source(f["source-id"].get<int64_t>());
            nf["field-id"] = ++last_partition_id;
            nf["name"] = f["name"];
            nf["transform"] = f["transform"];
            require(valid_transform(f["transform"].get<std::string>()),
                    "unknown partition transform '" + f["transform"].get<std::string>() + "'");
            spec["fields"].push_back(std::move(nf));
        }
    }

    Json order;
    order["fields"] = Json::array();
    if (in.write_order && in.write_order->is_object() && in.write_order->contains("fields")) {
        for (auto& f : (*in.write_order)["fields"]) {
            require(f.is_object() && f.contains("source-id") && f.contains("transform"),
                    "sort field requires source-id/transform");
            Json nf = f;
            nf["source-id"] = remap_source(f["source-id"].get<int64_t>());
            if (!nf.contains("direction")) nf["direction"] = "asc";
            if (!nf.contains("null-order")) nf["null-order"] = "nulls-first";
            order["fields"].push_back(std::move(nf));
        }
    }
    order["order-id"] = order["fields"].empty() ? 0 : 1;

    Json md;
    md["format-version"] = in.format_version;
    md["table-uuid"] = in.table_uuid;
    md["location"] = in.location;
    md["last-updated-ms"] = in.now_ms;
    md["last-column-id"] = last_column_id;
    md["schemas"] = Json::array({schema});
    md["current-schema-id"] = 0;
    md["partition-specs"] = Json::array({spec});
    md["default-spec-id"] = 0;
    md["last-partition-id"] = last_partition_id;
    md["sort-orders"] = Json::array({order});
    md["default-sort-order-id"] = order["order-id"];
    md["properties"] = in.properties;
    md["current-snapshot-id"] = -1;
    md["snapshots"] = Json::array();
    md["snapshot-log"] = Json::array();
    md["metadata-log"] = Json::array();
    md["refs"] = Json::object();
    if (in.format_version == 2) md["last-sequence-number"] = 0;
    synchronize_version_fields(md);
    validate_metadata(md);
    return md;
}

void synchronize_version_fields(Json& md) {
    if (format_version(md) == 1) {
        const Json* s = find_schema(md, md["current-schema-id"].get<int64_t>());
        if (s) md["schema"] = *s;
        for (auto& sp : md["partition-specs"])
            if (sp["spec-id"] == md["default-spec-id"]) md["partition-spec"] = sp["fields"];
    } else {
        md.erase("schema");
        md.erase("partition-spec");
    }
}

void finish_transition(Json& md, std::string_view prev_metadata_location, int64_t now_ms,
                       const MetadataLimits& lim) {
    std::set<int64_t> snap_ids;
    for (auto& s : md["snapshots"]) snap_ids.insert(s["snapshot-id"].get<int64_t>());
    Json log = Json::array();
    for (auto& e : md["snapshot-log"])
        if (e.is_object() && snap_ids.count(e.value("snapshot-id", int64_t(-1)))) log.push_back(e);
    md["snapshot-log"] = std::move(log);
    Json entry;
    entry["metadata-file"] = std::string(prev_metadata_location);
    entry["timestamp-ms"] = md.value("last-updated-ms", int64_t(0));
    md["metadata-log"].push_back(std::move(entry));
    while (static_cast<int>(md["metadata-log"].size()) > lim.metadata_log_keep) md["metadata-log"].erase(0);
    md["last-updated-ms"] = now_ms;
    synchronize_version_fields(md);
}

}  // namespace lights3::tables::iceberg
