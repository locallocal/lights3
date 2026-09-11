// ITableCatalogStore on the .sys bucket of the default backend (docs/s3-tables-design.md
// §4.3): one JSON object per entity, listing through the backend's prefix/delimiter
// listing, CAS through PutCondition. No cache -- every read hits the backend, which is
// what makes the state shared across gateways
#pragma once

#include <memory>
#include <string>

#include "tables/catalog_store.h"

namespace lights3::tables {

class ObjectCatalogStore final : public ITableCatalogStore {
public:
    explicit ObjectCatalogStore(std::shared_ptr<storage::IStorageBackend> sys_backend)
        : backend_(std::move(sys_backend)) {}

    static constexpr std::string_view kRoot = "tables-catalog/";
    static std::string bucket_root(std::string_view bucket);
    static std::string ns_dir(std::string_view bucket, const Levels& levels);
    static std::string ns_key(std::string_view bucket, const Levels& levels);
    static std::string tbl_dir(std::string_view bucket, const Levels& levels);
    static std::string tbl_key(std::string_view bucket, const Levels& levels, std::string_view name);
    static std::string commit_dir(std::string_view bucket, std::string_view table_id);
    static std::string commit_key(std::string_view bucket, std::string_view table_id, std::string_view commit_id);
    static std::string rename_dir(std::string_view bucket);
    static std::string rename_key(std::string_view bucket, std::string_view id);

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

    Task<std::optional<Versioned<RenameIntent>>> get_rename(std::string_view bucket, std::string_view id) override;
    Task<std::vector<RenameIntent>> list_renames(std::string_view bucket) override;
    Task<void> put_rename(std::string_view bucket, const RenameIntent& r, storage::PutCondition cond) override;
    Task<void> delete_rename(std::string_view bucket, std::string_view id) override;

    Task<void> delete_bucket_state(std::string_view bucket) override;
    Task<bool> bucket_state_empty(std::string_view bucket) override;

private:
    struct Raw {
        std::string body;
        std::string etag;
    };
    // nullopt on NoSuchKey / NoSuchBucket
    Task<std::optional<Raw>> read(std::string key);
    // returns the ETag; PreconditionFailed propagates
    Task<std::string> write(std::string key, std::string body, storage::PutCondition cond);
    Task<void> remove(std::string key);
    Task<void> ensure_sys_bucket();
    Task<std::vector<std::string>> list_keys(std::string prefix);

    std::shared_ptr<storage::IStorageBackend> backend_;
};

}  // namespace lights3::tables
