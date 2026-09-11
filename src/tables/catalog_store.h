// Catalog entities and the store boundary (docs/s3-tables-design.md §4.2). The interface
// is semantic (entities, not raw KV) so a duostore-meta implementation can map listing
// onto ordered iteration and CAS onto transactions (design §12). Every mutating read
// returns the entry with its ETag; writes take the storage PutCondition verbatim and
// let PreconditionFailed / NoSuchKey propagate -- the Catalog decides what they mean
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/task.h"
#include "storage/backend.h"
#include "tables/identifier.h"

namespace lights3::tables {

struct NamespaceEntry {
    int version = 1;
    Levels levels;
    std::map<std::string, std::string> properties;
    int64_t created_unix = 0;
    int64_t updated_unix = 0;
    // false = synthesized from evidence (children exist, no _ns.json)
    bool explicit_entry = true;
};

enum class TableState { Active, Renaming, Deleted };
const char* table_state_name(TableState s);
std::optional<TableState> table_state_from_name(std::string_view s);

struct TableEntry {
    int version = 1;
    Levels levels;
    std::string name;
    // storage identity (paths, commit log)
    std::string table_id;
    // Iceberg table-uuid (adopted on register)
    std::string table_uuid;
    // s3://<bucket>/<path>
    std::string location;
    // bucket-relative key, always under the reserved prefix (design §4.4)
    std::string metadata_location;
    std::string version_token;
    uint64_t generation = 1;
    int format_version = 2;
    TableState state = TableState::Active;
    std::string rename_id;
    int64_t created_unix = 0;
    int64_t updated_unix = 0;
};

struct CommitRecord {
    int version = 1;
    std::string commit_id;
    std::string table_id;
    std::string expected_token;
    std::string new_token;
    std::string prev_metadata_location;
    std::string new_metadata_location;
    // STAGED | COMMITTED
    std::string status;
    // sha256 hex of the canonical requirements+updates
    std::string request_digest;
    int64_t created_unix = 0;
};

struct RenameIntent {
    int version = 1;
    std::string rename_id;
    Levels src_levels, dst_levels;
    std::string src_name, dst_name;
    std::string src_etag;
    enum class Stage { Prepared, SourceFenced, DestinationWritten, SourceTombstoned };
    Stage stage = Stage::Prepared;
    int64_t created_unix = 0;
};
const char* rename_stage_name(RenameIntent::Stage s);
std::optional<RenameIntent::Stage> rename_stage_from_name(std::string_view s);

template <class T>
struct Versioned {
    T value;
    std::string etag;
};

template <class T>
struct ListPage {
    std::vector<T> items;
    // empty = no more pages
    std::string next_after;
};

struct PageCursor {
    std::string after;
    int limit = 1000;
};

// JSON (de)serialization; from_json rejects unknown fields and version != 1
nlohmann::json to_json(const NamespaceEntry&);
nlohmann::json to_json(const TableEntry&);
nlohmann::json to_json(const CommitRecord&);
nlohmann::json to_json(const RenameIntent&);
std::optional<NamespaceEntry> namespace_from_json(const nlohmann::json&);
std::optional<TableEntry> table_from_json(const nlohmann::json&);
std::optional<CommitRecord> commit_from_json(const nlohmann::json&);
std::optional<RenameIntent> rename_from_json(const nlohmann::json&);

struct ITableCatalogStore {
    virtual ~ITableCatalogStore() = default;

    virtual Task<std::optional<Versioned<NamespaceEntry>>> get_namespace(std::string_view bucket,
                                                                         const Levels& levels) = 0;
    virtual Task<ListPage<std::string>> list_child_namespaces(std::string_view bucket, const Levels& parent,
                                                              PageCursor cursor) = 0;
    // evidence: any child namespace or table (tombstones included) under the level
    virtual Task<bool> namespace_has_children(std::string_view bucket, const Levels& levels) = 0;
    virtual Task<void> put_namespace(std::string_view bucket, const NamespaceEntry& e, storage::PutCondition cond) = 0;
    virtual Task<void> delete_namespace(std::string_view bucket, const Levels& levels) = 0;

    virtual Task<std::optional<Versioned<TableEntry>>> get_table(std::string_view bucket, const Levels& levels,
                                                                 std::string_view name) = 0;
    virtual Task<ListPage<std::string>> list_tables(std::string_view bucket, const Levels& levels,
                                                    PageCursor cursor) = 0;
    // returns the new ETag
    virtual Task<std::string> put_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                        const TableEntry& e, storage::PutCondition cond) = 0;
    virtual Task<void> delete_table(std::string_view bucket, const Levels& levels, std::string_view name) = 0;

    virtual Task<std::optional<Versioned<CommitRecord>>> get_commit(std::string_view bucket,
                                                                    std::string_view table_id,
                                                                    std::string_view commit_id) = 0;
    virtual Task<void> put_commit(std::string_view bucket, std::string_view table_id, const CommitRecord& r,
                                  storage::PutCondition cond) = 0;
    virtual Task<std::vector<CommitRecord>> list_commits(std::string_view bucket, std::string_view table_id) = 0;

    virtual Task<std::optional<Versioned<RenameIntent>>> get_rename(std::string_view bucket, std::string_view id) = 0;
    virtual Task<std::vector<RenameIntent>> list_renames(std::string_view bucket) = 0;
    virtual Task<void> put_rename(std::string_view bucket, const RenameIntent& r, storage::PutCondition cond) = 0;
    virtual Task<void> delete_rename(std::string_view bucket, std::string_view id) = 0;

    // DeleteBucket: drop everything the catalog holds for the bucket
    virtual Task<void> delete_bucket_state(std::string_view bucket) = 0;
    // any catalog object at all for the bucket (namespaces, tables, tombstones)
    virtual Task<bool> bucket_state_empty(std::string_view bucket) = 0;
};

}  // namespace lights3::tables
