#include "storage/memory/memory_backend.h"

#include <algorithm>
#include <cstring>
#include <tuple>

#include "core/util/crypto.h"
#include "storage/listing.h"
#include "storage/multipart.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// 共享底层数据的只读 reader：锁外流式吐给客户端，put 覆盖也不影响在途 GET
class SharedBlobReader final : public http::BodyReader {
public:
    SharedBlobReader(std::shared_ptr<const std::string> blob, size_t off, size_t len)
        : blob_(std::move(blob)), off_(off), len_(len) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = std::min(buf.size(), len_ - pos_);
        if (n > 0) {
            std::memcpy(buf.data(), blob_->data() + off_ + pos_, n);
            pos_ += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    std::shared_ptr<const std::string> blob_;
    size_t off_, len_, pos_ = 0;
};

}  // namespace

uint64_t MemoryBackend::used_bytes() const {
    std::lock_guard lk(m_);
    return used_bytes_;
}

// 容量闸门（docs/gaps.md §6.3）：此前本后端无任何上限——配错成生产后端就是把整个
// 网关的数据放进堆里直到 OOM killer 介入。超限返回 503 SlowDown（可重试，且运维
// 在指标上看得见），而不是让分配器决定谁死
void MemoryBackend::reserve_locked(int64_t delta) {
    if (delta > 0 && opt_.max_bytes) {
        uint64_t want = used_bytes_ + uint64_t(delta);
        if (want > opt_.max_bytes)
            throw S3Error(S3ErrorCode::SlowDown,
                          "memory backend is at its configured max_bytes capacity");
    }
    used_bytes_ = uint64_t(int64_t(used_bytes_) + delta);
}

// mpu 过期清理（docs/gaps.md §6.3）：本后端不接定时器（无后台线程是它作为单测
// 夹具的一部分），改在 multipart 类操作入口顺带扫一遍——从不 complete/abort 的
// 上传此前会永久占着内存
void MemoryBackend::expire_uploads_locked() {
    if (opt_.mpu_ttl_sec <= 0) return;
    auto cutoff = std::chrono::system_clock::now() - std::chrono::seconds(opt_.mpu_ttl_sec);
    for (auto it = uploads_.begin(); it != uploads_.end();) {
        if (it->second.initiated >= cutoff) {
            ++it;
            continue;
        }
        int64_t freed = 0;
        for (auto& [no, part] : it->second.parts) freed += int64_t(part.data.size());
        reserve_locked(-freed);
        it = uploads_.erase(it);
    }
}

MemoryBackend::Bucket& MemoryBackend::bucket_or_throw(const std::string& name) {
    auto it = buckets_.find(name);
    if (it == buckets_.end())
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", name);
    return it->second;
}

Task<void> MemoryBackend::create_bucket(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    std::string name(bucket);
    if (buckets_.count(name))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists", name);
    buckets_[name].info = {name, std::chrono::system_clock::now()};
    co_return;
}

Task<void> MemoryBackend::delete_bucket(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    if (!b.objects.empty())
        throw S3Error(S3ErrorCode::BucketNotEmpty, "The bucket you tried to delete is not empty",
                      std::string(bucket));
    buckets_.erase(std::string(bucket));
    co_return;
}

Task<bool> MemoryBackend::bucket_exists(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
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
                                          ObjectMeta meta, http::BodyReader& body,
                                          PutCondition cond) {
    validate_bucket_name(bucket, kAllowReserved);
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
    auto blob = std::make_shared<const std::string>(std::move(data));

    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    // 条件检查与提交同锁（PutCondition 契约）
    if (cond.active()) {
        auto it = b.objects.find(std::string(key));
        if (cond.if_none_match && it != b.objects.end())
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold",
                          std::string(key));
        if (cond.if_match_etag) {
            if (it == b.objects.end())
                throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                              std::string(key));
            if (*cond.if_match_etag != it->second.meta.etag)
                throw S3Error(S3ErrorCode::PreconditionFailed,
                              "At least one of the pre-conditions you specified did not hold",
                              std::string(key));
        }
    }
    PutResult r{meta.etag};
    auto& slot = b.objects[std::string(key)];
    int64_t old = slot.data ? int64_t(slot.data->size()) : 0;
    reserve_locked(int64_t(blob->size()) - old);  // 超限抛出：slot 是刚插入的空对象
    slot = Object{std::move(meta), std::move(blob)};
    co_return r;
}

Task<ObjectStream> MemoryBackend::get_object(std::string_view bucket, std::string_view key,
                                             std::optional<ByteRange> range) {
    validate_bucket_name(bucket, kAllowReserved);
    ObjectStream out;
    std::shared_ptr<const std::string> blob;
    {
        // 锁内只取 meta 与数据块的 shared_ptr（docs/gaps.md §3.9）：此前在全局
        // 锁内整体拷贝对象（1GB 对象 = 2GB 驻留 + 全程锁住所有 bucket）
        std::lock_guard lk(m_);
        auto& b = bucket_or_throw(std::string(bucket));
        auto it = b.objects.find(std::string(key));
        if (it == b.objects.end())
            throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                          std::string(key));
        out.meta = it->second.meta;
        blob = it->second.data;
    }
    size_t off = 0, len = blob->size();
    if (range) {
        auto [f, l] = resolve_range(*range, blob->size());
        off = f;
        len = l - f + 1;
        out.range = ByteRange{f, l};
    }
    out.body = std::make_unique<SharedBlobReader>(std::move(blob), off, len);
    co_return out;
}

Task<ObjectMeta> MemoryBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    auto it = b.objects.find(std::string(key));
    if (it == b.objects.end())
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    co_return it->second.meta;
}

Task<void> MemoryBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    if (auto it = b.objects.find(std::string(key)); it != b.objects.end()) {
        reserve_locked(-int64_t(it->second.data ? it->second.data->size() : 0));
        b.objects.erase(it);
    }  // 幂等
    co_return;
}

Task<ListResult> MemoryBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    std::vector<std::string> keys;
    keys.reserve(b.objects.size());
    for (auto& [k, _] : b.objects) keys.push_back(k);
    // at() 而非 operator[]：后者对不存在的 key 会静默插入空对象（docs/gaps.md §3.9）
    co_return apply_listing(keys, opt, [&](const std::string& k) { return b.objects.at(k).meta; });
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
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    std::lock_guard lk(m_);
    expire_uploads_locked();
    bucket_or_throw(std::string(bucket));
    std::string id = new_upload_id();
    uploads_[id] = Upload{std::string(bucket), std::string(key), std::move(meta), {},
                          std::chrono::system_clock::now()};
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
    // 同号重传 last-write-wins
    auto& slot = up.parts[part_no];
    reserve_locked(int64_t(data.size()) - int64_t(slot.data.size()));
    slot = Part{std::move(data), etag, std::chrono::system_clock::now()};
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
    auto& slot = b.objects[std::string(key)];
    int64_t old = slot.data ? int64_t(slot.data->size()) : 0;
    // 拼接后的对象与分片同时驻留一瞬：先记新对象（可能超限而抛，此时上传仍在，
    // 客户端可重试或 abort），成功后再释放分片
    reserve_locked(int64_t(data.size()) - old);
    slot = Object{std::move(meta), std::make_shared<const std::string>(std::move(data))};
    int64_t freed = 0;
    for (auto& [no, part] : up.parts) freed += int64_t(part.data.size());
    reserve_locked(-freed);
    uploads_.erase(std::string(upload_id));
    co_return r;
}

Task<void> MemoryBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                          std::string_view upload_id) {
    std::lock_guard lk(m_);
    auto& up = upload_or_throw(bucket, key, upload_id);
    int64_t freed = 0;
    for (auto& [no, part] : up.parts) freed += int64_t(part.data.size());
    reserve_locked(-freed);
    uploads_.erase(std::string(upload_id));
    co_return;
}

Task<ListPartsResult> MemoryBackend::list_parts(std::string_view bucket, std::string_view key,
                                                std::string_view upload_id,
                                                const ListPartsOptions& opt) {
    std::lock_guard lk(m_);
    auto& up = upload_or_throw(bucket, key, upload_id);
    // parts 是 std::map<int,...>，遍历即升序；marker 之前的直接跳过
    std::vector<PartMeta> all;
    for (auto it = up.parts.upper_bound(opt.part_number_marker); it != up.parts.end(); ++it)
        all.push_back({it->first, it->second.data.size(), it->second.etag, it->second.uploaded});
    co_return apply_parts_page(std::move(all), opt);
}

Task<ListUploadsResult> MemoryBackend::list_multipart_uploads(std::string_view bucket,
                                                              const ListUploadsOptions& opt) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    expire_uploads_locked();
    bucket_or_throw(std::string(bucket));
    // 索引按 upload_id 建，桶内枚举本就是全扫 + 排序，marker 只能在排序后应用
    std::vector<UploadInfo> all;
    for (auto& [id, up] : uploads_)
        if (up.bucket == bucket) all.push_back({up.key, id, up.initiated});
    std::sort(all.begin(), all.end(), [](const UploadInfo& a, const UploadInfo& b) {
        return std::tie(a.key, a.upload_id) < std::tie(b.key, b.upload_id);
    });
    co_return apply_uploads_page(std::move(all), opt);
}

Task<void> MemoryBackend::close() {
    std::lock_guard lk(m_);
    buckets_.clear();
    uploads_.clear();
    used_bytes_ = 0;
    co_return;
}

}  // namespace lights3::storage
