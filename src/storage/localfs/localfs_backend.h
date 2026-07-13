// L3: 本地文件系统后端（见 docs/04-storage-backend.md §3）
// 布局：<root>/<bucket>/<key路径>，sidecar 元数据 <data>.lights3-meta，
// PUT 经 <staging>/put/<uuid> 写入后 rename 原子落地。
#pragma once

#include <filesystem>
#include <memory>

#include "core/thread_pool.h"
#include "storage/backend.h"

namespace lights3::storage {

class LocalFsBackend final : public IStorageBackend {
public:
    LocalFsBackend(std::filesystem::path root, std::filesystem::path staging,
                   std::shared_ptr<ThreadPool> pool);

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    static constexpr const char* kSidecarSuffix = ".lights3-meta";
    static constexpr const char* kBucketMarker = ".lights3-bucket";

private:
    std::filesystem::path bucket_dir(std::string_view bucket) const;
    std::filesystem::path object_path(std::string_view bucket, std::string_view key) const;
    void require_bucket(std::string_view bucket) const;      // 不存在抛 NoSuchBucket
    ObjectMeta load_meta(const std::filesystem::path& data_path, std::string key) const;

    std::filesystem::path root_;
    std::filesystem::path staging_;
    std::shared_ptr<ThreadPool> pool_;
};

}  // namespace lights3::storage
