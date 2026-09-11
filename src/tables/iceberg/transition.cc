#include "tables/iceberg/transition.h"

#include <map>
#include <set>

#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

// id -> canonical body of every element of a list keyed by `id_field`
std::map<int64_t, std::string> by_id(const Json& md, const char* list, const char* id_field) {
    std::map<int64_t, std::string> out;
    auto it = md.find(list);
    if (it == md.end() || !it->is_array()) return out;
    for (auto& e : *it)
        if (e.is_object() && e.contains(id_field)) out[e[id_field].get<int64_t>()] = canonical(e);
    return out;
}

void same_existing(const Json& cur, const Json& next, const char* list, const char* id_field, const char* what) {
    auto a = by_id(cur, list, id_field), b = by_id(next, list, id_field);
    for (auto& [id, body] : b) {
        auto it = a.find(id);
        if (it != a.end() && it->second != body)
            throw commit_failed(std::string("existing ") + what + " " + std::to_string(id) + " was modified");
    }
}

}  // namespace

void check_transition(const Json& cur, const Json& next) {
    if (cur.value("table-uuid", "") != next.value("table-uuid", "")) throw commit_failed("table uuid cannot change");
    if (format_version(next) < format_version(cur)) throw commit_failed("format-version cannot decrease");
    for (const char* f : {"last-column-id", "last-partition-id", "last-sequence-number"}) {
        int64_t a = cur.value(f, int64_t(0)), b = next.value(f, int64_t(0));
        if (b < a) throw commit_failed(std::string(f) + " cannot decrease");
    }
    same_existing(cur, next, "schemas", "schema-id", "schema");
    same_existing(cur, next, "partition-specs", "spec-id", "partition spec");
    same_existing(cur, next, "sort-orders", "order-id", "sort order");
    // snapshots: a v1→v2 upgrade adds sequence-number, so compare without it
    {
        auto strip = [](const Json& md) {
            std::map<int64_t, std::string> out;
            for (auto& s : md["snapshots"]) {
                Json c = s;
                c.erase("sequence-number");
                out[s["snapshot-id"].get<int64_t>()] = canonical(c);
            }
            return out;
        };
        auto a = strip(cur), b = strip(next);
        for (auto& [id, body] : b) {
            auto it = a.find(id);
            if (it != a.end() && it->second != body)
                throw commit_failed("existing snapshot " + std::to_string(id) + " was modified");
        }
    }
    // new schema field ids must sit above the old watermark unless they already existed
    {
        std::set<int64_t> old_ids;
        for (auto& s : cur["schemas"])
            for (auto id : schema_field_ids(s)) old_ids.insert(id);
        int64_t old_last = cur.value("last-column-id", int64_t(0));
        auto old_schemas = by_id(cur, "schemas", "schema-id");
        const Json* cur_schema = find_schema(cur, cur["current-schema-id"].get<int64_t>());
        for (auto& s : next["schemas"]) {
            if (old_schemas.count(s["schema-id"].get<int64_t>())) continue;
            for (auto id : schema_field_ids(s))
                if (!old_ids.count(id) && id <= old_last)
                    throw commit_failed("new schema reuses retired field id " + std::to_string(id));
            if (cur_schema) check_schema_evolution(*cur_schema, s, 409);
        }
    }
}

}  // namespace lights3::tables::iceberg
