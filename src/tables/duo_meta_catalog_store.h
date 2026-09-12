// ITableCatalogStore on a duostore meta engine's KV facade (docs/s3-tables-design.md §12,
// docs/s3-tables/step-6-optional.md §5): the same key layout as ObjectCatalogStore (so
// `lights3 tables export|import` moves state between the two backings byte for byte)
// over IMetaStore::kv_*. CAS is the engine's own transaction, and a commit writes the
// COMMITTED record and the pointer in one batch (commit_atomic), which collapses the
// crash-window matrix of design §5.4 to a single row. Reachable only when the default
// backend is duostore (tables.catalog_backing: duostore)
#pragma once

#include <memory>
#include <string>

#include "storage/duostore/meta_store.h"
#include "tables/catalog_store.h"

namespace lights3::tables {

class DuoMetaCatalogStore final : public ITableCatalogStore {
public:
    // meta must outlive the store (the DuoStoreBackend owns it)
    explicit DuoMetaCatalogStore(storage::duostore::IMetaStore& meta) : meta_(meta) {}

    Task<std::optional<Versioned<NamespaceEntry>>> get_namespace(std::string_view bucket,
                                                                 const Levels& levels) override;
    Task<ListPage<std::string>> list_child_namespaces(std::string_view bucket, const Levels& parent,
                                                      PageCursor cursor) override;
    Task<bool> namespace_has_children(std::string_view bucket, const Levels& levels) override;
    Task<void> put_namespace(std::string_view bucket, const NamespaceEntry& e, storage::PutCondition cond) override;
    Task<void> delete_namespace(std::string_view bucket, const Levels& levels) override;

    Task<std::optional<Versioned<TableEntry>>> get_table(std::string_view bucket, const Levels& levels,
                                                         std::string_view name) override;
    Task<ListPage<std::string>> list_tables(std::string_view bucket, const Levels& levels, PageCursor cursor) override;
    Task<std::string> put_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                const TableEntry& e, storage::PutCondition cond) override;
    Task<void> delete_table(std::string_view bucket, const Levels& levels, std::string_view name) override;

    Task<std::optional<Versioned<CommitRecord>>> get_commit(std::string_view bucket, std::string_view table_id,
                                                            std::string_view commit_id) override;
    Task<void> put_commit(std::string_view bucket, std::string_view table_id, const CommitRecord& r,
                          storage::PutCondition cond) override;
    Task<std::vector<CommitRecord>> list_commits(std::string_view bucket, std::string_view table_id) override;
    Task<void> delete_commit(std::string_view bucket, std::string_view table_id, std::string_view commit_id) override;

    Task<std::optional<Versioned<RenameIntent>>> get_rename(std::string_view bucket, std::string_view id) override;
    Task<std::vector<RenameIntent>> list_renames(std::string_view bucket) override;
    Task<std::string> put_rename(std::string_view bucket, const RenameIntent& r, storage::PutCondition cond) override;
    Task<void> delete_rename(std::string_view bucket, std::string_view id) override;

    Task<std::optional<MaintenanceConfig>> get_maintenance_config(std::string_view bucket, const Levels& levels,
                                                                  std::string_view name) override;
    Task<void> put_maintenance_config(std::string_view bucket, const Levels& levels, std::string_view name,
                                      const MaintenanceConfig& c) override;
    Task<void> delete_maintenance_config(std::string_view bucket, const Levels& levels, std::string_view name) override;

    Task<std::optional<Versioned<ViewEntry>>> get_view(std::string_view bucket, const Levels& levels,
                                                       std::string_view name) override;
    Task<ListPage<std::string>> list_views(std::string_view bucket, const Levels& levels, PageCursor cursor) override;
    Task<std::string> put_view(std::string_view bucket, const Levels& levels, std::string_view name, const ViewEntry& e,
                               storage::PutCondition cond) override;
    Task<void> delete_view(std::string_view bucket, const Levels& levels, std::string_view name) override;

    bool supports_atomic_commit() const override { return true; }
    Task<std::string> commit_atomic(std::string_view bucket, const Levels& levels, std::string_view name,
                                    const TableEntry& next, storage::PutCondition table_cond,
                                    const CommitRecord& committed) override;

    Task<std::vector<RawEntry>> export_raw(std::string_view bucket) override;
    Task<void> import_raw(std::string_view bucket, const RawEntry& e) override;

    Task<void> delete_bucket_state(std::string_view bucket) override;
    Task<bool> bucket_state_empty(std::string_view bucket) override;

private:
    // every key under a prefix (paged through kv_scan)
    std::vector<storage::duostore::KvItem> scan_all(const std::string& prefix);
    // the "<dir><name>.json" children of a directory, paged
    ListPage<std::string> list_json_names(const std::string& dir, PageCursor cursor);

    storage::duostore::IMetaStore& meta_;
};

}  // namespace lights3::tables
