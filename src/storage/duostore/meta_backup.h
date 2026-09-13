// L3: duostore meta backup chains and point-in-time restore planning
// (backlog-sequence ⑧, docs/architecture/storage/duostore-core.md §11.1). A backup directory
// holds one chain per engine: manifest.json lists the entries in order -- a full
// copy first, deltas after it -- and the engine-specific payloads next to it.
// Which payload an engine writes is the engine's business (IMetaStore::
// backup_physical for sqlite / rocksdb, the logical dump_meta stream plus a
// cluster-side restore marker for redis / tikv); this file only keeps the ledger
// and picks the chain prefix a restore replays:
//   --to-id N     every entry with id <= N (N must exist)
//   --to-ts T     every entry with ts <= T (at least the first full entry must fit)
// A prefix always starts at a full entry and is contiguous, so the manifest
// refuses gaps: entries are appended with consecutive ids
#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/meta_store.h"

namespace lights3::storage::duostore {

inline constexpr const char* kBackupManifest = "manifest.json";

struct BackupManifest {
    // "sqlite" | "rocksdb" | "redis" | "tikv"
    std::string engine;
    // configured backend name (informational)
    std::string backend;
    std::vector<MetaBackupEntry> entries;

    // Parse / render. load throws InternalError on a malformed file, returns an
    // empty manifest (no entries) when the file does not exist
    static BackupManifest load(const std::filesystem::path& dir);
    // atomic replace (write tmp + rename)
    void save(const std::filesystem::path& dir) const;

    uint64_t next_id() const { return entries.empty() ? 1 : entries.back().id + 1; }
    // The chain prefix to replay for a target: nullopt target = everything.
    // Throws InvalidRequest when the target precedes the first full entry, names
    // an unknown id, or the manifest is empty
    std::vector<MetaBackupEntry> plan(std::optional<uint64_t> to_id, std::optional<int64_t> to_ts_ms) const;
};

// "2026-09-06T12:34:56Z" (or with fractional seconds / offset) -> unix ms; also
// accepts a bare integer = unix ms. nullopt when unparsable
std::optional<int64_t> parse_restore_ts(const std::string& s);
int64_t backup_now_ms();

}  // namespace lights3::storage::duostore
