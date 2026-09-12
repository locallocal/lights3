// Catalog: the business layer of the table catalog (docs/s3-tables-design.md §5, §6.5,
// docs/s3-tables/step-1-catalog-core.md §9) -- namespace / table lifecycle and the
// single-table commit protocol on top of ITableCatalogStore + PutCondition CAS. Storage
// agnostic: the table bucket is reached through the router, catalog state through the
// store. Throws RestError; storage errors propagate as s3::S3Error for the REST layer
// to translate
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/config.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "s3/auth/policy.h"
#include "storage/bucket_router.h"
#include "tables/catalog_store.h"
#include "tables/table_bucket_store.h"

namespace lights3::tables {

struct NamespacePropsResult {
    std::vector<std::string> updated, removed, missing;
};

struct CreateTableRequest {
    std::string name;
    std::optional<std::string> location;
    nlohmann::json schema;
    std::optional<nlohmann::json> partition_spec;
    std::optional<nlohmann::json> write_order;
    std::map<std::string, std::string> properties;
};

struct CommitRequest {
    // empty = not replayable (a fresh id is recorded)
    std::string commit_id;
    nlohmann::json requirements = nlohmann::json::array();
    nlohmann::json updates = nlohmann::json::array();
};

// Per-request hooks the REST layer passes down (quota gate, usage accounting); either
// may be empty
struct CommitHooks {
    std::function<void(std::string_view bucket, int64_t add_bytes, int64_t add_objects)> quota_check;
    std::function<void(std::string_view bucket, int64_t d_objects, int64_t d_bytes)> note_usage;
};

class Catalog {
public:
    Catalog(std::shared_ptr<ITableCatalogStore> store, std::shared_ptr<TableBucketStore> buckets,
            storage::BucketRouter router, std::shared_ptr<ThreadPool> pool, TablesConfig cfg, MetricsScope metrics);

    const TablesConfig& config() const { return cfg_; }
    const std::shared_ptr<TableBucketStore>& bucket_store() const { return buckets_; }

    // ---- table buckets (root only; the caller checks) ----
    Task<TableBucketEntry> enable_bucket(std::string_view bucket);
    Task<void> disable_bucket(std::string_view bucket);
    // nullptr when the bucket is not table-enabled (pointer into the snapshot)
    const TableBucketEntry* table_bucket(const TableBucketStore::Snapshot& snap, std::string_view bucket) const;
    // throws not_found_ns when the bucket is not table-enabled
    Task<TableBucketEntry> require_table_bucket(std::string_view bucket);

    // ---- namespaces ----
    Task<NamespaceEntry> create_namespace(std::string_view bucket, Levels levels,
                                          std::map<std::string, std::string> props);
    // evidence based: an explicit entry, or a synthesized one when children exist
    Task<std::optional<NamespaceEntry>> load_namespace(std::string_view bucket, const Levels& levels);
    Task<bool> namespace_exists(std::string_view bucket, const Levels& levels);
    Task<ListPage<Levels>> list_namespaces(std::string_view bucket, const Levels& parent, PageCursor cursor);
    Task<NamespacePropsResult> update_namespace_properties(std::string_view bucket, const Levels& levels,
                                                           const std::vector<std::string>& removals,
                                                           const std::map<std::string, std::string>& updates);
    Task<void> drop_namespace(std::string_view bucket, const Levels& levels);

    // ---- tables ----
    struct LoadedTable {
        TableEntry entry;
        std::string etag;
        nlohmann::json metadata;
    };
    Task<LoadedTable> create_table(std::string_view bucket, const Levels& levels, const CreateTableRequest& req,
                                   const CommitHooks& hooks);
    Task<LoadedTable> register_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                     std::string_view metadata_location, const CommitHooks& hooks);
    Task<LoadedTable> load_table(std::string_view bucket, const Levels& levels, std::string_view name);
    // the pointer only (no metadata read); nullopt = no active table
    Task<std::optional<Versioned<TableEntry>>> table_pointer(std::string_view bucket, const Levels& levels,
                                                             std::string_view name);
    Task<bool> table_exists(std::string_view bucket, const Levels& levels, std::string_view name);
    Task<ListPage<std::string>> list_tables(std::string_view bucket, const Levels& levels, PageCursor cursor);
    Task<LoadedTable> commit_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                   const CommitRequest& req, const CommitHooks& hooks);
    // AWS UpdateTableMetadataLocation shape: pointer swap after the standard checks
    Task<TableEntry> update_metadata_location(std::string_view bucket, const Levels& levels, std::string_view name,
                                              std::string_view new_location, std::string_view expected_token);
    Task<void> rename_table(std::string_view bucket, const Levels& src_levels, std::string_view src_name,
                            const Levels& dst_levels, std::string_view dst_name);
    Task<void> drop_table(std::string_view bucket, const Levels& levels, std::string_view name);

    // ---- bucket lifecycle (DeleteBucket guard / cleanup) ----
    Task<bool> catalog_empty(std::string_view bucket);
    Task<void> forget_bucket(std::string_view bucket);

    // The narrowed policy a vended session gets (design §8.4): the table's data prefix and
    // its reserved metadata directory, nothing else in the bucket
    Task<s3::CredentialPolicy> vending_policy(std::string_view bucket, const TableEntry& entry, bool readonly);

    // client-facing form of a bucket-relative key
    static std::string to_client_location(std::string_view bucket, std::string_view key);
    // rewrites metadata-location style fields for the response
    nlohmann::json client_metadata(std::string_view bucket, const nlohmann::json& md) const;

private:
    struct TableLock {
        AsyncSemaphore sem{1};
    };
    std::shared_ptr<TableLock> table_lock(std::string_view bucket, std::string_view table_id);
    Task<void> schedule();

    std::string metadata_key(const TableBucketEntry& tb, const Levels& levels, std::string_view name, uint64_t gen,
                             std::string_view token) const;
    Task<nlohmann::json> read_metadata(storage::IStorageBackend& backend, std::string_view bucket,
                                       std::string_view key);
    Task<std::string> write_metadata(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key,
                                     std::string body, bool if_none_match);
    Task<bool> object_exists(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key);
    Task<void> best_effort_delete(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key);
    Task<Versioned<TableEntry>> require_active(std::string_view bucket, const Levels& levels, std::string_view name);
    Task<LoadedTable> finish_create(std::string_view bucket, const TableBucketEntry& tb, const Levels& levels,
                                    std::string_view name, TableEntry entry, nlohmann::json md, std::string body,
                                    storage::IStorageBackend& backend, const CommitHooks& hooks);
    static std::string new_token();
    static std::string request_digest(const CommitRequest& req);

    std::shared_ptr<ITableCatalogStore> store_;
    std::shared_ptr<TableBucketStore> buckets_;
    storage::BucketRouter router_;
    std::shared_ptr<ThreadPool> pool_;
    TablesConfig cfg_;
    MetricsScope metrics_;
    std::shared_ptr<MetricCounter> commits_ok_, commits_conflict_, commits_error_;
    std::shared_ptr<MetricHistogram> commit_seconds_;

    std::mutex locks_mu_;
    std::map<std::string, std::shared_ptr<TableLock>> locks_;
};

}  // namespace lights3::tables
