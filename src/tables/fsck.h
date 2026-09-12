// Catalog reconciliation for `lights3 fsck` (docs/s3-tables-design.md §11,
// docs/s3-tables/step-3-validation-diagnostics.md §9): the .sys catalog state of the default
// backend against the table buckets it describes. Read-only; repairs go through
// `POST …/catalog/recovery`. Findings:
//   tables.orphan_state         catalog state / marker for a bucket that is not a table
//                               bucket or does not exist
//   tables.dangling_pointer     a table entry whose metadata_location object is missing
//   tables.stale_renaming       a table in RENAMING whose rename intent is gone
//   tables.inconsistent_rename  a rename intent whose source / destination entries do not
//                               match its stage
//   tables.malformed_entry      a catalog object that does not parse
#pragma once

#include <cstdint>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "core/task.h"
#include "storage/backend.h"
#include "storage/bucket_router.h"

namespace lights3::tables {

struct ReconcileFinding {
    std::string kind;
    std::string bucket;
    std::string detail;
};

struct ReconcileReport {
    uint64_t table_buckets = 0;
    uint64_t tables = 0;
    uint64_t intents = 0;
    std::vector<ReconcileFinding> findings;
    // {"table_buckets","tables","intents","findings":[{kind,bucket,detail}]}
    nlohmann::json to_json() const;
};

// sys_backend holds .sys (the default backend); table buckets resolve through the router
Task<ReconcileReport> reconcile_catalog(std::shared_ptr<storage::IStorageBackend> sys_backend,
                                        storage::BucketRouter router);

}  // namespace lights3::tables
