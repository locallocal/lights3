// Table-bucket marker (docs/architecture/s3-tables-design.md §4.1): one .sys/tables/<bucket> object
// per enabled bucket, the same write-through + tombstone-sync SysConfigStore template
// as cors / lifecycle / quota. The snapshot is what the S3-plane guard consults
#pragma once

#include <map>
#include <optional>
#include <string>

#include "s3/sys_config_store.h"

namespace lights3::tables {

struct TableBucketEntry {
    int version = 1;
    bool enabled = true;
    // frozen at enable time (a later config change must not move the catalog)
    std::string reserved_prefix;
    std::map<std::string, std::string> properties;
    int64_t created_unix = 0;

    bool operator==(const TableBucketEntry&) const = default;
};

struct TableBucketTraits {
    using Entry = TableBucketEntry;
    static constexpr std::string_view kPrefix = "tables/";
    static constexpr const char* kName = "tables";
    static std::string serialize(const Entry& e);
    static std::optional<Entry> deserialize(const std::string& bucket, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return a != b; }
};

using TableBucketStore = s3::SysConfigStore<TableBucketTraits>;

}  // namespace lights3::tables
