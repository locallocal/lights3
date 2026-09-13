// S3-plane guard for table buckets (docs/architecture/s3-tables-design.md §8.1): the reserved catalog
// prefix is read-only through the S3 API, a non-empty table bucket cannot be deleted,
// and the catalog prefix's first segment is not a legal bucket name. Pure in-memory
// decisions on the TableBucketStore snapshot; the DeleteBucket check is the only one
// that touches storage
#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "core/task.h"
#include "storage/backend.h"
#include "tables/catalog.h"
#include "tables/table_bucket_store.h"

namespace lights3::tables {

class TableBucketGuard {
public:
    TableBucketGuard(std::shared_ptr<TableBucketStore> store, std::shared_ptr<Catalog> catalog, std::string path_prefix,
                     std::string compat_prefix);

    // Throws S3Error(InvalidRequest) for a mutating route on a reserved key
    void check(std::string_view bucket, std::string_view key, std::string_view route_name) const;
    // DeleteObjects per-key decision
    bool reserved_key(std::string_view bucket, std::string_view key) const;
    // Throws S3Error(BucketNotEmpty) while the catalog or the reserved prefix is non-empty
    Task<void> check_delete_bucket(std::string_view bucket, storage::IStorageBackend& backend) const;
    // After a successful DeleteBucket: drop the catalog state (best effort, logged)
    Task<void> forget_bucket(std::string_view bucket) const;
    // Throws S3Error(InvalidBucketName) for the catalog prefix's first segment
    void check_create_bucket(std::string_view bucket) const;
    bool is_table_bucket(std::string_view bucket) const;

private:
    // "" when not a table bucket
    std::string reserved_prefix_of(std::string_view bucket) const;

    std::shared_ptr<TableBucketStore> store_;
    std::shared_ptr<Catalog> catalog_;
    std::string reserved_bucket_;
    std::string reserved_bucket_compat_;
};

}  // namespace lights3::tables
