#include "storage/memory/memory_backend.h"

#include "core/util/crypto.h"
#include "storage/listing.h"
#include "storage/multipart.h"

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

// ---------- multipart ----------

MemoryBackend::Upload& MemoryBackend::upload_or_throw(std::string_view bucket,
                                                      std::string_view key,
                                                      std::string_view upload_id) {
    auto it = uploads_.find(std::string(upload_id));
    if (it == uploads_.end() || it->second.bucket != bucket || it->second.key != key)
        throw S3Error(S3ErrorCode::NoSuchUpload,
                      "The specified multipart upload does not exist.", std::string(upload_id));
    return it->second;
}

Task<std::string> MemoryBackend::create_multipart(std::string_view bucket, std::string_view key,
                                                  ObjectMeta meta) {
    validate_bucket_name(bucket);
    validate_object_key(key);
    std::lock_guard lk(m_);
    bucket_or_throw(std::string(bucket));
    std::string id = new_upload_id();
    uploads_[id] = Upload{std::string(bucket), std::string(key), std::move(meta), {}};
    co_return id;
}

Task<PutResult> MemoryBackend::upload_part(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id, int part_no,
                                           http::BodyReader& body) {
    validate_part_number(part_no);
    {
        std::lock_guard lk(m_);
        upload_or_throw(bucket, key, upload_id);  // 早失败；不持锁读 body
    }
    std::string data;
    std::byte buf[64 * 1024];
    util::HashStream md5(util::HashStream::Algo::Md5);
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        data.append(reinterpret_cast<const char*>(buf), n);
    }
    std::string etag = md5.final_hex();

    std::lock_guard lk(m_);
    auto& up = upload_or_throw(bucket, key, upload_id);  // 读 body 期间可能已被 abort
    up.parts[part_no] = Part{std::move(data), etag};     // 同号重传 last-write-wins
    co_return PutResult{etag};
}

Task<PutResult> MemoryBackend::complete_multipart(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  std::span<const PartInfo> parts) {
    validate_part_order(parts);
    std::lock_guard lk(m_);
    auto& up = upload_or_throw(bucket, key, upload_id);
    auto& b = bucket_or_throw(std::string(bucket));

    std::string data;
    std::vector<std::string> md5s;
    for (auto& p : parts) {
        auto it = up.parts.find(p.part_no);
        if (it == up.parts.end() || it->second.etag != strip_etag_quotes(p.etag))
            throw S3Error(S3ErrorCode::InvalidPart,
                          "One or more of the specified parts could not be found or the "
                          "ETag did not match.",
                          std::string(key));
        data += it->second.data;
        md5s.push_back(it->second.etag);
    }

    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = data.size();
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    PutResult r{meta.etag};
    b.objects[std::string(key)] = Object{std::move(meta), std::move(data)};
    uploads_.erase(std::string(upload_id));
    co_return r;
}

Task<void> MemoryBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                          std::string_view upload_id) {
    std::lock_guard lk(m_);
    upload_or_throw(bucket, key, upload_id);
    uploads_.erase(std::string(upload_id));
    co_return;
}

}  // namespace lights3::storage
