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

    // multipart：分片落 <staging>/mpu/<upload_id>/part.NNNNN，complete 拼接后
    // 走与 PUT 相同的 rename 原子提交（docs/04 §3.2）
    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                               std::string_view upload_id) override;
    Task<std::vector<PartMeta>> list_parts(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id) override;
    Task<std::vector<UploadInfo>> list_multipart_uploads(std::string_view bucket) override;

    static constexpr const char* kSidecarSuffix = ".lights3-meta";
    static constexpr const char* kBucketMarker = ".lights3-bucket";
    static constexpr std::chrono::hours kMpuTtl{24 * 7};  // 孤儿上传清理阈值

private:
    std::filesystem::path bucket_dir(std::string_view bucket) const;
    std::filesystem::path object_path(std::string_view bucket, std::string_view key) const;
    void require_bucket(std::string_view bucket) const;      // 不存在抛 NoSuchBucket
    ObjectMeta load_meta(const std::filesystem::path& data_path, std::string key) const;
    void cleanup_stale_uploads();  // 启动时清理超期（kMpuTtl）的 mpu 目录

    std::filesystem::path root_;
    std::filesystem::path staging_;
    std::shared_ptr<ThreadPool> pool_;
};

}  // namespace lights3::storage
