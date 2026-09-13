// CommitTable updates (docs/architecture/s3-tables-design.md §7.2): the server applies the update
// list to the current metadata and returns the next one. Invalid → 400, unsupported
// (format v3, encryption keys) → 406, conflicts (uuid mismatch, duplicate snapshot,
// missing parent) → 409
#pragma once

#include <string>

#include "tables/iceberg/metadata.h"

namespace lights3::tables::iceberg {

struct ApplyOptions {
    std::string bucket;
    std::string reserved_prefix;
};

Json apply_updates(const Json& current, const Json& updates, const ApplyOptions& opt);

}  // namespace lights3::tables::iceberg
