// L3: 内存后端 —— 单测与 demo 用，语义与 LocalFs 对齐
#pragma once

#include <map>
#include <mutex>

#include "storage/backend.h"

namespace lights3::storage {

class MemoryBackend final : public IStorageBackend {
public:
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

private:
    struct Object {
        ObjectMeta meta;
        std::string data;
    };
    struct Bucket {
        BucketInfo info;
        std::map<std::string, Object> objects;  // key 有序
    };
    struct Part {
        std::string data;
        std::string etag;  // 分片内容 MD5 hex
    };
    struct Upload {
        std::string bucket;
        std::string key;
        ObjectMeta meta;
        std::map<int, Part> parts;  // part_no 有序
    };

    Bucket& bucket_or_throw(const std::string& name);
    Upload& upload_or_throw(std::string_view bucket, std::string_view key,
                            std::string_view upload_id);

    std::mutex m_;
    std::map<std::string, Bucket> buckets_;
    std::map<std::string, Upload> uploads_;  // upload_id → 状态
};

}  // namespace lights3::storage
