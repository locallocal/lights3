#include "storage/memory/memory_backend.h"

#include "core/util/crypto.h"
#include "storage/listing.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

MemoryBackend::Bucket& MemoryBackend::bucket_or_throw(const std::string& name) {
    auto it = buckets_.find(name);
    if (it == buckets_.end())
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", name);
    return it->second;
}

Task<void> MemoryBackend::create_bucket(std::string_view bucket) {
    validate_bucket_name(bucket);
    std::lock_guard lk(m_);
    std::string name(bucket);
    if (buckets_.count(name))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists", name);
    buckets_[name].info = {name, std::chrono::system_clock::now()};
    co_return;
}

Task<void> MemoryBackend::delete_bucket(std::string_view bucket) {
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    if (!b.objects.empty())
        throw S3Error(S3ErrorCode::BucketNotEmpty, "The bucket you tried to delete is not empty",
                      std::string(bucket));
    buckets_.erase(std::string(bucket));
    co_return;
}

Task<bool> MemoryBackend::bucket_exists(std::string_view bucket) {
    std::lock_guard lk(m_);
    co_return buckets_.count(std::string(bucket)) > 0;
}

Task<std::vector<BucketInfo>> MemoryBackend::list_buckets() {
    std::lock_guard lk(m_);
    std::vector<BucketInfo> out;
    for (auto& [_, b] : buckets_) out.push_back(b.info);
    co_return out;
}

Task<PutResult> MemoryBackend::put_object(std::string_view bucket, std::string_view key,
                                          ObjectMeta meta, http::BodyReader& body) {
    validate_bucket_name(bucket);
    validate_object_key(key);
    // 先流式读完 body（不持锁），再提交
    std::string data;
    std::byte buf[64 * 1024];
    util::HashStream md5(util::HashStream::Algo::Md5);
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        data.append(reinterpret_cast<const char*>(buf), n);
    }
    meta.key = std::string(key);
    meta.size = data.size();
    meta.etag = md5.final_hex();
    meta.last_modified = std::chrono::system_clock::now();

    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    PutResult r{meta.etag};
    b.objects[std::string(key)] = Object{std::move(meta), std::move(data)};
    co_return r;
}

Task<ObjectStream> MemoryBackend::get_object(std::string_view bucket, std::string_view key,
                                             std::optional<ByteRange> range) {
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    auto it = b.objects.find(std::string(key));
    if (it == b.objects.end())
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    ObjectStream out;
    out.meta = it->second.meta;
    std::string data = it->second.data;
    if (range) {
        auto [f, l] = resolve_range(*range, data.size());
        data = data.substr(f, l - f + 1);
        out.range = ByteRange{f, l};
    }
    out.body = std::make_unique<http::StringBodyReader>(std::move(data));
    co_return out;
}

Task<ObjectMeta> MemoryBackend::head_object(std::string_view bucket, std::string_view key) {
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    auto it = b.objects.find(std::string(key));
    if (it == b.objects.end())
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    co_return it->second.meta;
}

Task<void> MemoryBackend::delete_object(std::string_view bucket, std::string_view key) {
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    b.objects.erase(std::string(key));  // 幂等
    co_return;
}

Task<ListResult> MemoryBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    std::vector<std::string> keys;
    keys.reserve(b.objects.size());
    for (auto& [k, _] : b.objects) keys.push_back(k);
    co_return apply_listing(keys, opt, [&](const std::string& k) { return b.objects[k].meta; });
}

}  // namespace lights3::storage
