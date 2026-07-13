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

private:
    struct Object {
        ObjectMeta meta;
        std::string data;
    };
    struct Bucket {
        BucketInfo info;
        std::map<std::string, Object> objects;  // key 有序
    };

    Bucket& bucket_or_throw(const std::string& name);

    std::mutex m_;
    std::map<std::string, Bucket> buckets_;
};

}  // namespace lights3::storage
