// Commit-record diagnostics, recovery and the rename driver (docs/s3-tables-design.md §5.4,
// §5.6; docs/s3-tables/step-3-validation-diagnostics.md §6–§7). Everything here works on
// the store alone (plus the table bucket for the unreferenced-metadata listing) so the
// REST endpoints, the CLI and any writer entering the catalog can call it. Recovery only
// completes bookkeeping the pointer CAS already decided: it never moves a pointer
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "core/task.h"
#include "storage/backend.h"
#include "tables/catalog_store.h"

namespace lights3::tables {

// Classification of one commit record against the current pointer (design §5.4):
//   Committed               record is COMMITTED
//   StagedBeforeTableUpdate STAGED and the pointer still carries expected_token (row 2:
//                           replayable, otherwise dead)
//   FinalizationRequired    STAGED but the pointer already carries new_token, or the new
//                           metadata sits on the committed chain (row 3: externally
//                           committed, the record just was not finalized)
//   Superseded              STAGED and another commit moved the pointer past it
//   ManualReview            fields missing, or the metadata chain has a cycle
enum class CommitState { Committed, StagedBeforeTableUpdate, FinalizationRequired, Superseded, ManualReview };
const char* commit_state_name(CommitState s);

struct CommitDiagnosis {
    CommitRecord record;
    CommitState state = CommitState::Committed;
    std::string note;
};

struct TableDiagnostics {
    TableEntry entry;
    std::string etag;
    std::vector<CommitDiagnosis> commits;
    // keys under the table's reserved metadata/ directory that neither the pointer, the
    // metadata-log nor any commit record references (input for step ④'s planner)
    std::vector<std::string> unreferenced_metadata;
    nlohmann::json to_json(std::string_view bucket) const;
};

// metadata_dir = "<reserved>/<ns>/<table>/metadata/" inside bucket_backend. Throws
// not_found_table when the table has no entry (tombstones are diagnosable)
Task<TableDiagnostics> diagnose_table(ITableCatalogStore& store, storage::IStorageBackend& bucket_backend,
                                      std::string_view bucket, const Levels& levels, std::string_view name,
                                      std::string_view metadata_dir);

struct RecoveryReport {
    int finalized = 0;
    int pruned = 0;
    int manual = 0;
    nlohmann::json to_json() const;
};

// FinalizationRequired → the record is written COMMITTED; with prune, Superseded and
// StagedBeforeTableUpdate records are deleted; ManualReview is only counted
Task<RecoveryReport> recover_table(ITableCatalogStore& store, std::string_view bucket, const Levels& levels,
                                   std::string_view name, bool prune);

// ---- rename intents (design §5.6) ----

enum class RenameOutcome {
    // every step done and the intent deleted
    Completed,
    // the source moved from under the intent (or a Prepared intent aged past the ttl):
    // rolled back, intent deleted
    Abandoned,
    // the destination name is taken: rolled back, intent deleted
    DestinationTaken,
    // another driver advanced the intent concurrently; it will finish it
    Contended
};
const char* rename_outcome_name(RenameOutcome o);

// Continue an intent from its recorded stage. Every step is a CAS on the entity it
// changes and every stage advance a CAS on the intent, so any number of drivers may run
// this concurrently; whoever loses a CAS re-reads and either finds the step done or
// stops (Contended). prepared_ttl_sec ≤ 0 = Prepared intents never time out
Task<RenameOutcome> drive_rename(ITableCatalogStore& store, std::string_view bucket, Versioned<RenameIntent> intent,
                                 int64_t prepared_ttl_sec);
// Drive every intent of the bucket; a failing intent is logged and skipped. Returns the
// number of intents that were driven
Task<int> recover_renames(ITableCatalogStore& store, std::string_view bucket, int64_t prepared_ttl_sec);

}  // namespace lights3::tables
