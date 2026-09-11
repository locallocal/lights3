// Snapshot-graph checks (docs/s3-tables-design.md §7.4). Step ① ships the shallow check:
// every snapshot added by a commit must point at a manifest list (or v1 manifests) that
// exists inside the bucket, outside the reserved prefix. The Avro walk lands with step ③
#pragma once

#include <string>

#include "core/task.h"
#include "storage/backend.h"
#include "tables/iceberg/metadata.h"

namespace lights3::tables::iceberg {

struct SnapshotCheckContext {
    storage::IStorageBackend& backend;
    std::string bucket;
    std::string reserved_prefix;
};

// Missing / foreign / reserved path → 409 CommitFailedException
Task<void> check_new_snapshots_shallow(const SnapshotCheckContext& ctx, const Json& current, const Json& next);

}  // namespace lights3::tables::iceberg
