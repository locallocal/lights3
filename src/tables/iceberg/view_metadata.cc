#include "tables/iceberg/view_metadata.h"

#include <algorithm>
#include <set>

#include "tables/iceberg/metadata.h"
#include "tables/identifier.h"
#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

const Json& need(const Json& j, const char* key, const char* what) {
    auto it = j.find(key);
    if (it == j.end() || it->is_null()) throw bad_request(std::string(what) + " requires '" + key + "'");
    return *it;
}

// A view version as the REST request / metadata carries it: representations of type
// "sql" with "sql" and "dialect", a default-namespace (list), an optional summary
void validate_version(const Json& v, const char* what) {
    if (!v.is_object()) throw bad_request(std::string(what) + " must be an object");
    const Json& reps = need(v, "representations", what);
    if (!reps.is_array() || reps.empty()) throw bad_request(std::string(what) + " needs at least one representation");
    for (auto& r : reps) {
        if (!r.is_object() || r.value("type", "") != "sql" || !r.contains("sql") || !r["sql"].is_string() ||
            !r.contains("dialect") || !r["dialect"].is_string())
            throw bad_request(std::string(what) + ": every representation is {\"type\":\"sql\",\"sql\",\"dialect\"}");
    }
    if (v.contains("default-namespace") && !v["default-namespace"].is_null() && !v["default-namespace"].is_array())
        throw bad_request(std::string(what) + ".default-namespace must be a list");
    if (v.contains("summary") && !v["summary"].is_null() && !v["summary"].is_object())
        throw bad_request(std::string(what) + ".summary must be an object");
}

int64_t last_schema_id(const Json& md) {
    int64_t last = -1;
    for (auto& s : md["schemas"]) last = std::max(last, s.value("schema-id", int64_t(-1)));
    return last;
}

int64_t last_version_id(const Json& md) {
    int64_t last = 0;
    for (auto& v : md["versions"]) last = std::max(last, v.value("version-id", int64_t(0)));
    return last;
}

}  // namespace

int64_t current_view_version_id(const Json& md) { return md.value("current-version-id", int64_t(0)); }

Json initial_view_metadata(const CreateViewInput& in) {
    if (!in.schema.is_object() || in.schema.value("type", "") != "struct" || !in.schema.contains("fields"))
        throw bad_request("'schema' must be a struct schema");
    validate_version(in.view_version, "view-version");
    Json md;
    md["view-uuid"] = in.view_uuid;
    md["format-version"] = 1;
    md["location"] = in.location;
    Json schema = in.schema;
    schema["schema-id"] = 0;
    // ids come from the client (the schema has none on a fresh view: assign them)
    if (!in.schema.contains("fields") || !in.schema["fields"].is_array()) throw bad_request("schema needs fields");
    md["schemas"] = Json::array({schema});
    Json v = in.view_version;
    v["version-id"] = 1;
    v["schema-id"] = 0;
    v["timestamp-ms"] = in.now_ms;
    if (!v.contains("summary") || v["summary"].is_null()) v["summary"] = Json::object();
    if (!v["summary"].contains("operation")) v["summary"]["operation"] = "create";
    if (!v.contains("default-namespace") || v["default-namespace"].is_null()) v["default-namespace"] = Json::array();
    md["versions"] = Json::array({v});
    md["current-version-id"] = 1;
    md["version-log"] = Json::array({Json{{"timestamp-ms", in.now_ms}, {"version-id", 1}}});
    md["properties"] = in.properties;
    validate_view_metadata(md);
    return md;
}

void validate_view_metadata(const Json& md) {
    if (!md.is_object()) throw bad_request("view metadata must be an object");
    if (md.value("format-version", 0) != 1)
        throw unsupported("view format-version " + md.value("format-version", Json()).dump() + " is not supported");
    if (!md.contains("view-uuid") || !md["view-uuid"].is_string() ||
        !looks_like_uuid(md["view-uuid"].get<std::string>()))
        throw bad_request("view metadata needs a 'view-uuid'");
    if (!md.contains("location") || !md["location"].is_string()) throw bad_request("view metadata needs 'location'");
    if (!md.contains("schemas") || !md["schemas"].is_array() || md["schemas"].empty())
        throw bad_request("view metadata needs 'schemas'");
    std::set<int64_t> schema_ids;
    for (auto& s : md["schemas"]) {
        if (!s.is_object() || !s.contains("schema-id") || !s["schema-id"].is_number_integer())
            throw bad_request("every view schema needs an integer 'schema-id'");
        if (!schema_ids.insert(s["schema-id"].get<int64_t>()).second) throw bad_request("duplicate view schema-id");
        schema_field_ids(s);
    }
    if (!md.contains("versions") || !md["versions"].is_array() || md["versions"].empty())
        throw bad_request("view metadata needs 'versions'");
    std::set<int64_t> version_ids;
    for (auto& v : md["versions"]) {
        validate_version(v, "view version");
        if (!v.contains("version-id") || !v["version-id"].is_number_integer())
            throw bad_request("every view version needs an integer 'version-id'");
        if (!version_ids.insert(v["version-id"].get<int64_t>()).second) throw bad_request("duplicate view version-id");
        if (!v.contains("schema-id") || !schema_ids.count(v.value("schema-id", int64_t(-1))))
            throw bad_request("view version " + v["version-id"].dump() + " references an unknown schema-id");
    }
    if (!md.contains("current-version-id") || !version_ids.count(current_view_version_id(md)))
        throw bad_request("'current-version-id' must name one of the versions");
    if (md.contains("properties") && !md["properties"].is_object()) throw bad_request("'properties' must be an object");
    if (md.contains("version-log") && !md["version-log"].is_array()) throw bad_request("'version-log' must be a list");
}

Json parse_and_validate_view(std::string_view text, size_t max_size) {
    if (text.size() > max_size) throw bad_request("view metadata exceeds the size limit");
    Json md;
    try {
        md = Json::parse(text);
    } catch (const Json::exception& e) {
        throw bad_request(std::string("view metadata is not valid JSON: ") + e.what());
    }
    validate_view_metadata(md);
    return md;
}

void check_view_requirements(const Json& current, const Json& requirements) {
    if (!requirements.is_array()) throw bad_request("'requirements' must be a list");
    for (auto& r : requirements) {
        if (!r.is_object() || !r.contains("type") || !r["type"].is_string())
            throw bad_request("every requirement needs a 'type'");
        std::string type = r["type"].get<std::string>();
        if (type == "assert-view-uuid") {
            std::string want = r.value("uuid", "");
            if (want != current.value("view-uuid", ""))
                throw commit_failed("requirement failed: view uuid is " + current.value("view-uuid", "") + ", not " +
                                    want);
        } else {
            throw bad_request("unsupported view requirement '" + type + "'");
        }
    }
}

Json apply_view_updates(const Json& current, const Json& updates, int64_t now_ms) {
    if (!updates.is_array()) throw bad_request("'updates' must be a list");
    Json md = current;
    int64_t last_added_version = -1;
    const int64_t before = current_view_version_id(current);
    for (auto& u : updates) {
        if (!u.is_object() || !u.contains("action") || !u["action"].is_string())
            throw bad_request("every update needs an 'action'");
        std::string a = u["action"].get<std::string>();
        if (a == "assign-uuid") {
            std::string uuid = need(u, "uuid", "assign-uuid").get<std::string>();
            if (!looks_like_uuid(uuid)) throw bad_request("assign-uuid: not a uuid");
            if (md["view-uuid"].get<std::string>() != uuid) throw commit_failed("assign-uuid: the view uuid is fixed");
        } else if (a == "upgrade-format-version") {
            int fv = need(u, "format-version", "upgrade-format-version").get<int>();
            if (fv != 1) throw unsupported("view format-version " + std::to_string(fv) + " is not supported");
        } else if (a == "add-schema") {
            Json schema = need(u, "schema", "add-schema");
            if (!schema.is_object() || schema.value("type", "") != "struct")
                throw bad_request("add-schema: not a struct");
            schema_field_ids(schema);
            int64_t id = last_schema_id(md) + 1;
            if (schema.contains("schema-id") && schema["schema-id"].is_number_integer() &&
                schema["schema-id"].get<int64_t>() >= 0) {
                int64_t asked = schema["schema-id"].get<int64_t>();
                for (auto& s : md["schemas"])
                    if (s["schema-id"].get<int64_t>() == asked)
                        throw commit_failed("add-schema: schema-id " + std::to_string(asked) + " already exists");
                id = asked;
            }
            schema["schema-id"] = id;
            md["schemas"].push_back(schema);
        } else if (a == "add-view-version") {
            Json v = need(u, "view-version", "add-view-version");
            validate_version(v, "add-view-version.view-version");
            int64_t id = last_version_id(md) + 1;
            if (v.contains("version-id") && v["version-id"].is_number_integer() && v["version-id"].get<int64_t>() > 0) {
                int64_t asked = v["version-id"].get<int64_t>();
                for (auto& ex : md["versions"])
                    if (ex["version-id"].get<int64_t>() == asked)
                        throw commit_failed("add-view-version: version-id " + std::to_string(asked) +
                                            " already exists");
                id = asked;
            }
            v["version-id"] = id;
            int64_t schema_id = v.value("schema-id", int64_t(-1));
            // -1 = the last added schema (the Iceberg convention)
            if (schema_id < 0) schema_id = last_schema_id(md);
            bool known = false;
            for (auto& s : md["schemas"])
                if (s["schema-id"].get<int64_t>() == schema_id) known = true;
            if (!known) throw bad_request("add-view-version: unknown schema-id " + std::to_string(schema_id));
            v["schema-id"] = schema_id;
            if (!v.contains("timestamp-ms") || !v["timestamp-ms"].is_number_integer()) v["timestamp-ms"] = now_ms;
            if (!v.contains("summary") || v["summary"].is_null()) v["summary"] = Json::object();
            if (!v.contains("default-namespace") || v["default-namespace"].is_null())
                v["default-namespace"] = Json::array();
            md["versions"].push_back(v);
            last_added_version = id;
        } else if (a == "set-current-view-version") {
            int64_t id = need(u, "view-version-id", "set-current-view-version").get<int64_t>();
            if (id == -1) {
                if (last_added_version < 0) throw bad_request("set-current-view-version: no version was added");
                id = last_added_version;
            }
            bool known = false;
            for (auto& ex : md["versions"])
                if (ex["version-id"].get<int64_t>() == id) known = true;
            if (!known) throw bad_request("set-current-view-version: unknown version-id " + std::to_string(id));
            md["current-version-id"] = id;
        } else if (a == "set-location") {
            md["location"] = need(u, "location", "set-location").get<std::string>();
        } else if (a == "set-properties") {
            const Json& up = need(u, "updates", "set-properties");
            if (!up.is_object()) throw bad_request("set-properties.updates must be an object");
            for (auto& [k, v] : up.items()) {
                if (!v.is_string()) throw bad_request("set-properties: values must be strings");
                md["properties"][k] = v;
            }
        } else if (a == "remove-properties") {
            const Json& rm = need(u, "removals", "remove-properties");
            if (!rm.is_array()) throw bad_request("remove-properties.removals must be a list");
            for (auto& k : rm) {
                if (!k.is_string()) throw bad_request("remove-properties: names must be strings");
                md["properties"].erase(k.get<std::string>());
            }
        } else {
            throw unsupported("unsupported view update '" + a + "'");
        }
    }
    if (current_view_version_id(md) != before) {
        if (!md.contains("version-log") || !md["version-log"].is_array()) md["version-log"] = Json::array();
        md["version-log"].push_back(Json{{"timestamp-ms", now_ms}, {"version-id", current_view_version_id(md)}});
    }
    validate_view_metadata(md);
    return md;
}

}  // namespace lights3::tables::iceberg
