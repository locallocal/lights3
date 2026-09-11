#include "tables/bucket_guard.h"

#include "core/log.h"
#include "s3/errors.h"

namespace lights3::tables {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

std::string first_segment(const std::string& prefix) {
    std::string p = prefix;
    while (!p.empty() && p.front() == '/') p.erase(0, 1);
    auto slash = p.find('/');
    return slash == std::string::npos ? p : p.substr(0, slash);
}

constexpr std::string_view kMutatingRoutes[] = {
    "PutObject",
    "CopyObject",
    "DeleteObject",
    "CreateMultipartUpload",
    "UploadPart",
    "UploadPartCopy",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "PutObjectTagging",
    "DeleteObjectTagging",
};

}  // namespace

TableBucketGuard::TableBucketGuard(std::shared_ptr<TableBucketStore> store, std::shared_ptr<Catalog> catalog,
                                   std::string path_prefix, std::string compat_prefix)
    : store_(std::move(store)),
      catalog_(std::move(catalog)),
      reserved_bucket_(first_segment(path_prefix)),
      reserved_bucket_compat_(first_segment(compat_prefix)) {}

std::string TableBucketGuard::reserved_prefix_of(std::string_view bucket) const {
    if (!store_) return {};
    auto snap = store_->snapshot();
    const TableBucketEntry* e = TableBucketStore::find(snap, std::string(bucket));
    if (!e || !e->enabled) return {};
    return e->reserved_prefix;
}

bool TableBucketGuard::is_table_bucket(std::string_view bucket) const { return !reserved_prefix_of(bucket).empty(); }

bool TableBucketGuard::reserved_key(std::string_view bucket, std::string_view key) const {
    std::string prefix = reserved_prefix_of(bucket);
    if (prefix.empty() || key.empty()) return false;
    std::string_view bare(prefix.data(), prefix.size() - 1);
    return key == bare || key.rfind(prefix, 0) == 0;
}

void TableBucketGuard::check(std::string_view bucket, std::string_view key, std::string_view route_name) const {
    if (key.empty()) return;
    bool mutating = false;
    for (auto r : kMutatingRoutes)
        if (r == route_name) mutating = true;
    if (!mutating) return;
    if (reserved_key(bucket, key))
        throw S3Error(S3ErrorCode::InvalidRequest, "Object key is reserved for the table catalog.", std::string(key));
}

Task<void> TableBucketGuard::check_delete_bucket(std::string_view bucket, storage::IStorageBackend& backend) const {
    std::string prefix = reserved_prefix_of(bucket);
    if (prefix.empty()) co_return;
    if (catalog_ && !co_await catalog_->catalog_empty(bucket))
        throw S3Error(S3ErrorCode::BucketNotEmpty, "The table bucket still holds namespaces or tables.",
                      std::string(bucket));
    storage::ListOptions opt;
    opt.prefix = prefix;
    opt.max_keys = 1;
    auto res = co_await backend.list_objects(bucket, opt);
    if (!res.objects.empty() || !res.common_prefixes.empty())
        throw S3Error(S3ErrorCode::BucketNotEmpty, "The table bucket still holds catalog objects under " + prefix,
                      std::string(bucket));
}

Task<void> TableBucketGuard::forget_bucket(std::string_view bucket) const {
    if (!catalog_ || reserved_prefix_of(bucket).empty()) co_return;
    try {
        co_await catalog_->forget_bucket(bucket);
    } catch (const std::exception& e) {
        LOG_WARN("bucket {}: could not drop table catalog state: {}", bucket, e.what());
    }
}

void TableBucketGuard::check_create_bucket(std::string_view bucket) const {
    if ((!reserved_bucket_.empty() && bucket == reserved_bucket_) ||
        (!reserved_bucket_compat_.empty() && bucket == reserved_bucket_compat_))
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "The bucket name is reserved for the Iceberg REST catalog path prefix.", std::string(bucket));
}

}  // namespace lights3::tables
