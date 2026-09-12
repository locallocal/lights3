// Snapshot-graph checks (docs/s3-tables-design.md §7.4, docs/s3-tables/step-3-validation-diagnostics.md
// §5). The shallow check (step ①) only proves the manifest list exists inside the bucket;
// the deep check (step ③) walks manifest-list → manifests → data files through the Avro
// reader: every referenced object exists in the bucket outside the reserved prefix, the
// recorded lengths match, and the new snapshot does not re-add a live file, delete a
// non-live one, or smuggle deletes into an "append". Reads fan out through when_all in
// batches of `concurrency` (on cloudproxy every HEAD is a remote round trip)
#pragma once

#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "core/task.h"
#include "storage/backend.h"
#include "tables/iceberg/manifest.h"
#include "tables/iceberg/metadata.h"

namespace lights3::tables::iceberg {

struct SnapshotCheckContext {
    storage::IStorageBackend& backend;
    std::string bucket;
    std::string reserved_prefix;
};

// Missing / foreign / reserved path → 409 CommitFailedException
Task<void> check_new_snapshots_shallow(const SnapshotCheckContext& ctx, const Json& current, const Json& next);

struct DeepCheckOptions {
    int concurrency = 16;
    // one manifest list / manifest object
    size_t max_avro = 128u << 20;
    size_t max_manifests = 10000;
    size_t max_files = 1000000;
    // an Avro codec the reader cannot decode (snappy / zstd / bzip2, or deflate without
    // zlib): true = skip that snapshot and report it, false = 409
    bool allow_unsupported_codec = true;
};

struct DeepCheckReport {
    size_t manifests = 0;
    size_t files = 0;
    bool skipped_codec = false;
};

// Every snapshot of `next` that `current` does not have (all of them when `current`
// has none): manifest list, manifests, data / delete files, statistics files, and the
// conflict re-check against the parent snapshot's live file set. Throws RestError(409)
Task<DeepCheckReport> check_new_snapshots_deep(const SnapshotCheckContext& ctx, const Json& current, const Json& next,
                                               const DeepCheckOptions& opt);

// The live entries (status added / existing, any content) of one snapshot's manifests,
// for the compaction planner (step ⑥ §4); nullopt when a codec could not be read.
// Throws RestError(409) on malformed Avro
Task<std::optional<std::vector<DataFile>>> live_files_of_snapshot(const SnapshotCheckContext& ctx, const Json& snapshot,
                                                                  const DeepCheckOptions& opt);

// Every bucket-relative key the snapshots of `md` reach -- manifest lists, manifests and
// the files of every entry whatever its status (the maintenance planner's reachability
// set, step ④ §4). nullopt when a codec could not be read; manifests_seen accumulates
// across calls and exceeding opt.max_manifests is a 409 like any malformed Avro
Task<std::optional<std::set<std::string>>> reachable_files(const SnapshotCheckContext& ctx, const Json& md,
                                                           const DeepCheckOptions& opt, size_t& manifests_seen);

}  // namespace lights3::tables::iceberg
