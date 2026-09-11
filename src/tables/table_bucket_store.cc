#include "tables/table_bucket_store.h"

#include <nlohmann/json.hpp>

namespace lights3::tables {

using nlohmann::json;

std::string TableBucketTraits::serialize(const Entry& e) {
    json j;
    j["version"] = e.version;
    j["enabled"] = e.enabled;
    j["reserved_prefix"] = e.reserved_prefix;
    j["properties"] = e.properties;
    j["created_unix"] = e.created_unix;
    return j.dump();
}

std::optional<TableBucketEntry> TableBucketTraits::deserialize(const std::string&, const std::string& body) {
    try {
        json j = json::parse(body);
        if (!j.is_object() || j.value("version", 0) != 1) return std::nullopt;
        TableBucketEntry e;
        e.enabled = j.value("enabled", true);
        e.reserved_prefix = j.at("reserved_prefix").get<std::string>();
        if (e.reserved_prefix.empty() || e.reserved_prefix.back() != '/') return std::nullopt;
        json props = j.value("properties", json::object());
        for (auto& [k, v] : props.items()) e.properties[k] = v.get<std::string>();
        e.created_unix = j.value("created_unix", int64_t(0));
        return e;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

}  // namespace lights3::tables
