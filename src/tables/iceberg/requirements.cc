#include "tables/iceberg/requirements.h"

#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

int64_t need_int(const Json& r, const char* field) {
    auto it = r.find(field);
    if (it == r.end() || !it->is_number_integer())
        throw bad_request(std::string("requirement '") + r.value("type", "") + "' needs integer '" + field + "'");
    return it->get<int64_t>();
}

}  // namespace

void check_requirements(const Json& current, const Json& requirements, bool table_exists) {
    if (!requirements.is_array()) throw bad_request("'requirements' must be a list");
    for (auto& r : requirements) {
        if (!r.is_object() || !r.contains("type") || !r["type"].is_string())
            throw bad_request("each requirement needs a string 'type'");
        std::string type = r["type"].get<std::string>();
        if (type == "assert-create") {
            if (table_exists) throw commit_failed("requirement failed: table already exists");
        } else if (type == "assert-table-uuid") {
            auto it = r.find("uuid");
            if (it == r.end() || !it->is_string()) throw bad_request("assert-table-uuid needs 'uuid'");
            if (current.value("table-uuid", "") != it->get<std::string>())
                throw commit_failed("requirement failed: table uuid does not match");
        } else if (type == "assert-ref-snapshot-id") {
            auto ref = r.find("ref");
            if (ref == r.end() || !ref->is_string()) throw bad_request("assert-ref-snapshot-id needs 'ref'");
            std::string name = ref->get<std::string>();
            auto want = r.find("snapshot-id");
            const Json& refs = current.contains("refs") ? current["refs"] : Json::object();
            bool exists = refs.contains(name);
            if (want == r.end() || want->is_null()) {
                if (exists) throw commit_failed("requirement failed: branch or tag " + name + " already exists");
            } else {
                if (!want->is_number_integer())
                    throw bad_request("assert-ref-snapshot-id needs an integer 'snapshot-id'");
                if (!exists) throw commit_failed("requirement failed: branch or tag " + name + " is missing");
                if (refs[name].value("snapshot-id", int64_t(-1)) != want->get<int64_t>())
                    throw commit_failed("requirement failed: branch or tag " + name + " has changed");
            }
        } else if (type == "assert-last-assigned-field-id") {
            if (current.value("last-column-id", int64_t(-1)) != need_int(r, "last-assigned-field-id"))
                throw commit_failed("requirement failed: last assigned field id changed");
        } else if (type == "assert-current-schema-id") {
            if (current.value("current-schema-id", int64_t(-1)) != need_int(r, "current-schema-id"))
                throw commit_failed("requirement failed: current schema changed");
        } else if (type == "assert-last-assigned-partition-id") {
            if (current.value("last-partition-id", int64_t(-1)) != need_int(r, "last-assigned-partition-id"))
                throw commit_failed("requirement failed: last assigned partition id changed");
        } else if (type == "assert-default-spec-id") {
            if (current.value("default-spec-id", int64_t(-1)) != need_int(r, "default-spec-id"))
                throw commit_failed("requirement failed: default partition spec changed");
        } else if (type == "assert-default-sort-order-id") {
            if (current.value("default-sort-order-id", int64_t(-1)) != need_int(r, "default-sort-order-id"))
                throw commit_failed("requirement failed: default sort order changed");
        } else {
            throw bad_request("unknown requirement type '" + type + "'");
        }
    }
}

}  // namespace lights3::tables::iceberg
