#include "tables/iceberg/updates.h"

#include <algorithm>
#include <set>

#include "tables/identifier.h"
#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

const Json& need(const Json& u, const char* field) {
    auto it = u.find(field);
    if (it == u.end()) throw bad_request("update '" + u.value("action", std::string()) + "' needs '" + field + "'");
    return *it;
}

int64_t need_int(const Json& u, const char* field) {
    const Json& v = need(u, field);
    if (!v.is_number_integer()) throw bad_request("update field '" + std::string(field) + "' must be an integer");
    return v.get<int64_t>();
}

// Same fields regardless of schema-id (Iceberg's "schema already exists" reuse)
bool same_fields(const Json& a, const Json& b) {
    Json x = a, y = b;
    x.erase("schema-id");
    y.erase("schema-id");
    return x == y;
}

struct Applier {
    Json md;
    const ApplyOptions& opt;
    int64_t last_added_schema = -1;
    int64_t last_added_spec = -1;
    int64_t last_added_order = -1;

    Applier(const Json& cur, const ApplyOptions& o) : md(cur), opt(o) {}

    std::set<int64_t> current_field_ids() {
        const Json* s = find_schema(md, md["current-schema-id"].get<int64_t>());
        std::set<int64_t> ids;
        if (s)
            for (auto id : schema_field_ids(*s)) ids.insert(id);
        return ids;
    }

    void assign_uuid(const Json& u) {
        const Json& id = need(u, "uuid");
        if (!id.is_string()) throw bad_request("assign-uuid needs a string 'uuid'");
        if (md["table-uuid"].get<std::string>() != id.get<std::string>())
            throw commit_failed("assign-uuid: table uuid " + md["table-uuid"].get<std::string>() + " cannot change");
    }

    void upgrade_format_version(const Json& u) {
        int64_t v = need_int(u, "format-version");
        int cur = format_version(md);
        if (v == cur) return;
        if (v < cur) throw bad_request("cannot downgrade format-version");
        if (v != 2) throw unsupported("Iceberg format-version " + std::to_string(v) + " is not supported");
        md["format-version"] = 2;
        if (!md.contains("last-sequence-number")) md["last-sequence-number"] = 0;
        for (auto& s : md["snapshots"])
            if (!s.contains("sequence-number")) s["sequence-number"] = 0;
    }

    void add_schema(const Json& u) {
        Json schema = need(u, "schema");
        if (!schema.is_object() || schema.value("type", "") != "struct") throw bad_request("add-schema needs a struct");
        // reuse an identical schema
        for (auto& s : md["schemas"])
            if (same_fields(s, schema)) {
                last_added_schema = s["schema-id"].get<int64_t>();
                return;
            }
        auto new_fields = schema_fields(schema);
        int64_t last_col = md["last-column-id"].get<int64_t>();
        const Json* cur = find_schema(md, md["current-schema-id"].get<int64_t>());
        auto cur_fields = cur ? schema_fields(*cur) : std::map<int64_t, FieldInfo>{};
        std::set<int64_t> known;
        for (auto& s : md["schemas"])
            for (auto id : schema_field_ids(s)) known.insert(id);
        int64_t max_id = last_col;
        if (cur) check_schema_evolution(*cur, schema, 400);
        for (auto& [id, f] : new_fields) {
            max_id = std::max(max_id, id);
            auto it = cur_fields.find(id);
            if (it != cur_fields.end()) {
                continue;
            } else if (known.count(id) || id <= last_col) {
                // an id below the watermark that is not in the current schema was used before
                if (!cur_fields.count(id))
                    throw bad_request("add-schema: field id " + std::to_string(id) +
                                      " was already assigned; new columns need ids above last-column-id");
            }
        }
        int64_t next_id = 0;
        for (auto& s : md["schemas"]) next_id = std::max(next_id, s["schema-id"].get<int64_t>() + 1);
        schema["schema-id"] = next_id;
        md["schemas"].push_back(schema);
        int64_t req_last = u.value("last-column-id", int64_t(-1));
        md["last-column-id"] = std::max({last_col, max_id, req_last});
        last_added_schema = next_id;
    }

    void set_current_schema(const Json& u) {
        int64_t id = need_int(u, "schema-id");
        if (id == -1) {
            if (last_added_schema < 0) throw bad_request("set-current-schema -1 without a preceding add-schema");
            id = last_added_schema;
        }
        if (!find_schema(md, id)) throw bad_request("set-current-schema: schema " + std::to_string(id) + " not found");
        md["current-schema-id"] = id;
    }

    void add_spec(const Json& u) {
        Json spec = need(u, "spec");
        if (!spec.is_object() || !spec.contains("fields") || !spec["fields"].is_array())
            throw bad_request("add-spec needs a spec with 'fields'");
        auto fids = current_field_ids();
        int64_t last_pid = md.value("last-partition-id", int64_t(999));
        Json fields = Json::array();
        std::set<int64_t> seen;
        for (auto& f : spec["fields"]) {
            if (!f.is_object() || !f.contains("source-id") || !f.contains("name") || !f.contains("transform"))
                throw bad_request("partition field requires source-id/name/transform");
            if (!fids.count(f["source-id"].get<int64_t>()))
                throw bad_request("add-spec: source-id " + std::to_string(f["source-id"].get<int64_t>()) +
                                  " is not in the current schema");
            Json nf = f;
            // client-assigned ids at or above 1000 are kept (a spec may reuse the id of an
            // identical partition field of an earlier spec); anything else is assigned here
            if (nf.contains("field-id") && nf["field-id"].is_number_integer() && nf["field-id"].get<int64_t>() >= 1000)
                last_pid = std::max(last_pid, nf["field-id"].get<int64_t>());
            else
                nf["field-id"] = ++last_pid;
            if (!seen.insert(nf["field-id"].get<int64_t>()).second) throw bad_request("add-spec: duplicate field-id");
            fields.push_back(std::move(nf));
        }
        spec["fields"] = fields;
        for (auto& s : md["partition-specs"]) {
            Json a = s, b = spec;
            a.erase("spec-id");
            b.erase("spec-id");
            if (a == b) {
                last_added_spec = s["spec-id"].get<int64_t>();
                return;
            }
        }
        int64_t next_id = 0;
        for (auto& s : md["partition-specs"]) next_id = std::max(next_id, s["spec-id"].get<int64_t>() + 1);
        spec["spec-id"] = next_id;
        md["partition-specs"].push_back(spec);
        md["last-partition-id"] = std::max(md.value("last-partition-id", int64_t(999)), last_pid);
        last_added_spec = next_id;
    }

    void set_default_spec(const Json& u) {
        int64_t id = need_int(u, "spec-id");
        if (id == -1) {
            if (last_added_spec < 0) throw bad_request("set-default-spec -1 without a preceding add-spec");
            id = last_added_spec;
        }
        bool found = false;
        for (auto& s : md["partition-specs"])
            if (s["spec-id"].get<int64_t>() == id) found = true;
        if (!found) throw bad_request("set-default-spec: spec " + std::to_string(id) + " not found");
        md["default-spec-id"] = id;
    }

    void add_sort_order(const Json& u) {
        Json order = need(u, "sort-order");
        if (!order.is_object() || !order.contains("fields") || !order["fields"].is_array())
            throw bad_request("add-sort-order needs a sort order with 'fields'");
        auto fids = current_field_ids();
        for (auto& f : order["fields"]) {
            if (!f.is_object() || !f.contains("source-id") || !f.contains("transform"))
                throw bad_request("sort field requires source-id/transform");
            if (!fids.count(f["source-id"].get<int64_t>()))
                throw bad_request("add-sort-order: source-id is not in the current schema");
            if (!f.contains("direction")) f["direction"] = "asc";
            if (!f.contains("null-order")) f["null-order"] = "nulls-first";
        }
        for (auto& s : md["sort-orders"]) {
            if (s["fields"] == order["fields"]) {
                last_added_order = s["order-id"].get<int64_t>();
                return;
            }
        }
        int64_t next_id = 1;
        for (auto& s : md["sort-orders"]) next_id = std::max(next_id, s["order-id"].get<int64_t>() + 1);
        order["order-id"] = order["fields"].empty() ? 0 : next_id;
        md["sort-orders"].push_back(order);
        last_added_order = order["order-id"].get<int64_t>();
    }

    void set_default_sort_order(const Json& u) {
        int64_t id = need_int(u, "sort-order-id");
        if (id == -1) {
            if (last_added_order < 0) throw bad_request("set-default-sort-order -1 without a preceding add-sort-order");
            id = last_added_order;
        }
        bool found = false;
        for (auto& s : md["sort-orders"])
            if (s["order-id"].get<int64_t>() == id) found = true;
        if (!found) throw bad_request("set-default-sort-order: order " + std::to_string(id) + " not found");
        md["default-sort-order-id"] = id;
    }

    void add_snapshot(const Json& u) {
        Json snap = need(u, "snapshot");
        if (!snap.is_object() || !snap.contains("snapshot-id") || !snap["snapshot-id"].is_number_integer())
            throw bad_request("add-snapshot needs an integer 'snapshot-id'");
        if (!snap.contains("timestamp-ms") || !snap["timestamp-ms"].is_number_integer())
            throw bad_request("add-snapshot needs an integer 'timestamp-ms'");
        int64_t id = snap["snapshot-id"].get<int64_t>();
        if (find_snapshot(md, id))
            throw commit_failed("add-snapshot: snapshot " + std::to_string(id) + " already exists");
        int fv = format_version(md);
        if (fv == 2) {
            if (snap.contains("manifests")) throw bad_request("v2 snapshots must use 'manifest-list', not 'manifests'");
            if (!snap.contains("manifest-list") || !snap["manifest-list"].is_string())
                throw bad_request("add-snapshot needs 'manifest-list'");
            if (!snap.contains("sequence-number") || !snap["sequence-number"].is_number_integer())
                throw bad_request("v2 snapshots need an integer 'sequence-number'");
            int64_t seq = snap["sequence-number"].get<int64_t>();
            int64_t last = md.value("last-sequence-number", int64_t(0));
            if (seq <= last && !md["snapshots"].empty())
                throw commit_failed("add-snapshot: sequence-number " + std::to_string(seq) +
                                    " must be greater than last-sequence-number " + std::to_string(last));
            md["last-sequence-number"] = std::max(last, seq);
        } else if (!snap.contains("manifest-list") && !snap.contains("manifests")) {
            throw bad_request("add-snapshot needs 'manifest-list' or 'manifests'");
        }
        if (snap.contains("parent-snapshot-id") && !snap["parent-snapshot-id"].is_null()) {
            int64_t parent = snap["parent-snapshot-id"].get<int64_t>();
            if (!find_snapshot(md, parent))
                throw commit_failed("add-snapshot: parent snapshot " + std::to_string(parent) + " does not exist");
        }
        if (snap.contains("summary")) {
            if (!snap["summary"].is_object()) throw bad_request("snapshot summary must be an object");
            std::string op = snap["summary"].value("operation", std::string());
            if (op != "append" && op != "overwrite" && op != "delete" && op != "replace")
                throw bad_request("snapshot summary operation must be append|overwrite|delete|replace");
        }
        if (snap.contains("manifest-list")) path_to_key(opt.bucket, snap["manifest-list"].get<std::string>());
        md["snapshots"].push_back(snap);
    }

    void set_snapshot_ref(const Json& u) {
        const Json& name_j = need(u, "ref-name");
        if (!name_j.is_string()) throw bad_request("set-snapshot-ref needs a string 'ref-name'");
        std::string name = name_j.get<std::string>();
        int64_t id = need_int(u, "snapshot-id");
        std::string type = u.value("type", "branch");
        if (type != "branch" && type != "tag") throw bad_request("set-snapshot-ref type must be branch|tag");
        const Json* snap = find_snapshot(md, id);
        if (!snap) throw commit_failed("set-snapshot-ref: snapshot " + std::to_string(id) + " does not exist");
        Json ref;
        ref["snapshot-id"] = id;
        ref["type"] = type;
        for (const char* k : {"min-snapshots-to-keep", "max-snapshot-age-ms", "max-ref-age-ms"})
            if (u.contains(k) && !u[k].is_null()) ref[k] = u[k];
        md["refs"][name] = ref;
        if (name == "main") {
            md["current-snapshot-id"] = id;
            Json e;
            e["snapshot-id"] = id;
            e["timestamp-ms"] = (*snap)["timestamp-ms"];
            md["snapshot-log"].push_back(e);
        }
    }

    void remove_snapshots(const Json& u) {
        const Json& ids = need(u, "snapshot-ids");
        if (!ids.is_array()) throw bad_request("remove-snapshots needs 'snapshot-ids'");
        std::set<int64_t> rm;
        for (auto& id : ids) {
            if (!id.is_number_integer()) throw bad_request("snapshot-ids must be integers");
            rm.insert(id.get<int64_t>());
        }
        Json kept = Json::array();
        for (auto& s : md["snapshots"])
            if (!rm.count(s["snapshot-id"].get<int64_t>())) kept.push_back(s);
        md["snapshots"] = kept;
        Json refs = Json::object();
        for (auto& [n, r] : md["refs"].items())
            if (!rm.count(r["snapshot-id"].get<int64_t>())) refs[n] = r;
        md["refs"] = refs;
        if (rm.count(current_snapshot_id(md))) md["current-snapshot-id"] = -1;
        for (const char* key : {"statistics", "partition-statistics"}) {
            if (!md.contains(key)) continue;
            Json keep = Json::array();
            for (auto& s : md[key])
                if (!rm.count(s.value("snapshot-id", int64_t(-1)))) keep.push_back(s);
            md[key] = keep;
        }
    }

    void remove_snapshot_ref(const Json& u) {
        const Json& name_j = need(u, "ref-name");
        if (!name_j.is_string()) throw bad_request("remove-snapshot-ref needs a string 'ref-name'");
        std::string name = name_j.get<std::string>();
        md["refs"].erase(name);
        if (name == "main") md["current-snapshot-id"] = -1;
    }

    void set_location(const Json& u) {
        const Json& loc = need(u, "location");
        if (!loc.is_string()) throw bad_request("set-location needs a string 'location'");
        std::string key = location_to_key(opt.bucket, opt.reserved_prefix, loc.get<std::string>());
        md["location"] = key_to_location(opt.bucket, key);
    }

    void set_properties(const Json& u) {
        const Json& up = need(u, "updates");
        if (!up.is_object()) throw bad_request("set-properties needs an object 'updates'");
        for (auto& [k, v] : up.items()) {
            if (!v.is_string()) throw bad_request("property '" + k + "' must be a string");
            if (k == "format-version") throw bad_request("use upgrade-format-version to change the format version");
            md["properties"][k] = v;
        }
    }

    void remove_properties(const Json& u) {
        const Json& rm = need(u, "removals");
        if (!rm.is_array()) throw bad_request("remove-properties needs a list 'removals'");
        for (auto& k : rm) {
            if (!k.is_string()) throw bad_request("removals must be strings");
            md["properties"].erase(k.get<std::string>());
        }
    }

    void set_stats(const Json& u, const char* key, const char* field) {
        const Json& st = need(u, field);
        if (!st.is_object() || !st.contains("snapshot-id"))
            throw bad_request(std::string(field) + " needs 'snapshot-id'");
        int64_t id = st["snapshot-id"].get<int64_t>();
        if (!find_snapshot(md, id)) throw commit_failed("statistics reference unknown snapshot " + std::to_string(id));
        if (!md.contains(key)) md[key] = Json::array();
        Json out = Json::array();
        for (auto& s : md[key])
            if (s.value("snapshot-id", int64_t(-1)) != id) out.push_back(s);
        out.push_back(st);
        md[key] = out;
    }

    void remove_stats(const Json& u, const char* key) {
        int64_t id = need_int(u, "snapshot-id");
        if (!md.contains(key)) return;
        Json out = Json::array();
        for (auto& s : md[key])
            if (s.value("snapshot-id", int64_t(-1)) != id) out.push_back(s);
        md[key] = out;
    }

    void remove_partition_specs(const Json& u) {
        const Json& ids = need(u, "spec-ids");
        if (!ids.is_array()) throw bad_request("remove-partition-specs needs 'spec-ids'");
        std::set<int64_t> rm;
        for (auto& id : ids) rm.insert(id.get<int64_t>());
        if (rm.count(md["default-spec-id"].get<int64_t>()))
            throw bad_request("cannot remove the default partition spec");
        Json out = Json::array();
        for (auto& s : md["partition-specs"])
            if (!rm.count(s["spec-id"].get<int64_t>())) out.push_back(s);
        md["partition-specs"] = out;
    }

    void remove_schemas(const Json& u) {
        const Json& ids = need(u, "schema-ids");
        if (!ids.is_array()) throw bad_request("remove-schemas needs 'schema-ids'");
        std::set<int64_t> rm;
        for (auto& id : ids) rm.insert(id.get<int64_t>());
        if (rm.count(md["current-schema-id"].get<int64_t>())) throw bad_request("cannot remove the current schema");
        Json out = Json::array();
        for (auto& s : md["schemas"])
            if (!rm.count(s["schema-id"].get<int64_t>())) out.push_back(s);
        md["schemas"] = out;
    }

    void apply(const Json& u) {
        if (!u.is_object() || !u.contains("action") || !u["action"].is_string())
            throw bad_request("each update needs a string 'action'");
        std::string a = u["action"].get<std::string>();
        if (a == "assign-uuid")
            assign_uuid(u);
        else if (a == "upgrade-format-version")
            upgrade_format_version(u);
        else if (a == "add-schema")
            add_schema(u);
        else if (a == "set-current-schema")
            set_current_schema(u);
        else if (a == "add-spec")
            add_spec(u);
        else if (a == "set-default-spec")
            set_default_spec(u);
        else if (a == "add-sort-order")
            add_sort_order(u);
        else if (a == "set-default-sort-order")
            set_default_sort_order(u);
        else if (a == "add-snapshot")
            add_snapshot(u);
        else if (a == "set-snapshot-ref")
            set_snapshot_ref(u);
        else if (a == "remove-snapshots")
            remove_snapshots(u);
        else if (a == "remove-snapshot-ref")
            remove_snapshot_ref(u);
        else if (a == "set-location")
            set_location(u);
        else if (a == "set-properties")
            set_properties(u);
        else if (a == "remove-properties")
            remove_properties(u);
        else if (a == "set-statistics")
            set_stats(u, "statistics", "statistics");
        else if (a == "remove-statistics")
            remove_stats(u, "statistics");
        else if (a == "set-partition-statistics")
            set_stats(u, "partition-statistics", "partition-statistics");
        else if (a == "remove-partition-statistics")
            remove_stats(u, "partition-statistics");
        else if (a == "remove-partition-specs")
            remove_partition_specs(u);
        else if (a == "remove-schemas")
            remove_schemas(u);
        else if (a == "add-encryption-key" || a == "remove-encryption-key")
            throw unsupported("update '" + a + "' requires Iceberg format-version 3");
        else
            throw bad_request("unknown update action '" + a + "'");
    }
};

}  // namespace

Json apply_updates(const Json& current, const Json& updates, const ApplyOptions& opt) {
    if (!updates.is_array()) throw bad_request("'updates' must be a list");
    Applier ap(current, opt);
    // uuid / format version first (Iceberg applies them ahead of everything else)
    for (auto& u : updates)
        if (u.is_object() &&
            (u.value("action", "") == "assign-uuid" || u.value("action", "") == "upgrade-format-version"))
            ap.apply(u);
    for (auto& u : updates) {
        if (u.is_object() &&
            (u.value("action", "") == "assign-uuid" || u.value("action", "") == "upgrade-format-version"))
            continue;
        ap.apply(u);
    }
    synchronize_version_fields(ap.md);
    validate_metadata(ap.md);
    return ap.md;
}

}  // namespace lights3::tables::iceberg
