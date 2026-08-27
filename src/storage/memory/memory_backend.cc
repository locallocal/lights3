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

// Read-only reader sharing the underlying data: streams to the client outside the lock,
// and a put overwrite does not affect in-flight GETs
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

// Capacity gate (docs/archive/gaps.md §6.3): previously this backend had no limit at all --
// misconfigured as a production backend it would put the entire gateway's data into the
// heap until the OOM killer stepped in. Over the limit returns 503 SlowDown (retryable,
// and visible to operators on the metrics) rather than letting the allocator decide who dies
void MemoryBackend::reserve_locked(int64_t delta) {
    if (delta > 0 && opt_.max_bytes) {
        uint64_t want = used_bytes_ + uint64_t(delta);
        if (want > opt_.max_bytes)
            throw S3Error(S3ErrorCode::SlowDown,
                          "memory backend is at its configured max_bytes capacity");
    }
    used_bytes_ = uint64_t(int64_t(used_bytes_) + delta);
}

// Early gate while reading the body: if capacity were checked only after EOF, an oversized
// body's bytes would already all be resident in the heap, and OOM protection would fail
// exactly when it is needed most. In-flight buffers do not count toward used_bytes_
// (reserve happens at commit); here we conservatively pre-check "used + this request's
// buffered bytes" and return 503 over the limit (same semantics as reserve_locked)
void MemoryBackend::check_inflight(size_t buffered) const {
    if (opt_.max_bytes && used_bytes() + buffered > opt_.max_bytes)
        throw S3Error(S3ErrorCode::SlowDown,
                      "memory backend is at its configured max_bytes capacity");
}

// mpu expiry cleanup (docs/archive/gaps.md §6.3): this backend takes no timer (having no
// background threads is part of its role as a unit-test fixture), so it sweeps as a side
// task at multipart operation entry points instead -- uploads never completed/aborted used
// to occupy memory forever
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
    // Read the body to the end first (without the lock), then commit
    std::string data;
    std::byte buf[64 * 1024];
    util::HashStream md5(util::HashStream::Algo::Md5);
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        data.append(reinterpret_cast<const char*>(buf), n);
        check_inflight(data.size());
    }
    meta.key = std::string(key);
    meta.size = data.size();
    meta.etag = md5.final_hex();
    meta.last_modified = std::chrono::system_clock::now();
    finalize_checksum(meta);  // trailer-form value exists now that the body is drained (§2.2)
    auto blob = std::make_shared<const std::string>(std::move(data));

    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    // Condition check and commit under the same lock (PutCondition contract)
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
    // Only touch the map after the capacity check passes: if operator[] inserted first,
    // a throw from reserve would leave a ghost entry with a null data pointer in the map,
    // and a later GET would dereference a null pointer
    auto it = b.objects.find(std::string(key));
    int64_t old = it != b.objects.end() && it->second.data ? int64_t(it->second.data->size()) : 0;
    reserve_locked(int64_t(blob->size()) - old);
    b.objects.insert_or_assign(std::string(key), Object{std::move(meta), std::move(blob)});
    co_return r;
}

Task<ObjectStream> MemoryBackend::get_object(std::string_view bucket, std::string_view key,
                                             std::optional<ByteRange> range) {
    validate_bucket_name(bucket, kAllowReserved);
    ObjectStream out;
    std::shared_ptr<const std::string> blob;
    {
        // Inside the lock, grab only the meta and the data block's shared_ptr
        // (docs/archive/gaps.md §3.9): previously the object was copied wholesale under the global
        // lock (1GB object = 2GB resident + all buckets locked for the duration)
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
    }  // idempotent
    co_return;
}

Task<ListResult> MemoryBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    auto& b = bucket_or_throw(std::string(bucket));
    std::vector<std::string> keys;
    keys.reserve(b.objects.size());
    for (auto& [k, _] : b.objects) keys.push_back(k);
    // at() rather than operator[]: the latter silently inserts an empty object for a
    // missing key (docs/archive/gaps.md §3.9)
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
                                           http::BodyReader& body,
                                           const std::optional<PartChecksum>& checksum) {
    validate_part_number(part_no);
    {
        std::lock_guard lk(m_);
        upload_or_throw(bucket, key, upload_id);  // fail early; read the body without the lock
    }
    std::string data;
    std::byte buf[64 * 1024];
    util::HashStream md5(util::HashStream::Algo::Md5);
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        data.append(reinterpret_cast<const char*>(buf), n);
        check_inflight(data.size());
    }
    std::string etag = md5.final_hex();

    std::lock_guard lk(m_);
    auto& up = upload_or_throw(bucket, key, upload_id);  // may have been aborted while reading the body
    // Same-number re-upload is last-write-wins; only touch the map after the capacity
    // check passes (same as put_object, otherwise a throw from reserve leaves a ghost part
    // with an empty etag)
    auto it = up.parts.find(part_no);
    reserve_locked(int64_t(data.size()) -
                   (it != up.parts.end() ? int64_t(it->second.data.size()) : 0));
    Part part{std::move(data), etag, std::chrono::system_clock::now(), "", ""};
    if (checksum) {  // resolved() only after the body was drained above (trailer form)
        part.checksum_algorithm = checksum->algorithm;
        part.checksum_value = checksum->resolved();
    }
    up.parts.insert_or_assign(part_no, std::move(part));
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
    std::vector<PartDigest> digests;
    std::vector<uint64_t> sizes;
    for (auto& p : parts) {
        auto it = up.parts.find(p.part_no);
        if (it == up.parts.end() || it->second.etag != strip_etag_quotes(p.etag))
            throw S3Error(S3ErrorCode::InvalidPart,
                          "One or more of the specified parts could not be found or the "
                          "ETag did not match.",
                          std::string(key));
        data += it->second.data;
        md5s.push_back(it->second.etag);
        sizes.push_back(it->second.data.size());
        digests.push_back({it->second.checksum_algorithm, it->second.checksum_value});
    }

    // Copy rather than move: the reserve below may throw on over-limit, and the promise
    // that "the upload survives, the client may retry or abort" requires up.meta to stay
    // intact on the failure path
    ObjectMeta meta = up.meta;
    meta.key = std::string(key);
    meta.size = data.size();
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    meta.part_sizes = std::move(sizes);  // GET ?partNumber layout (roadmap §2.5)
    PutResult r{meta.etag};
    // Composite checksum from the stored, verified per-part values (roadmap §2.2)
    apply_composite_checksum(digests, meta, r);
    // The concatenated object and the parts are resident simultaneously for a moment:
    // account the new object first (may throw on over-limit, in which case the upload
    // survives and the client may retry or abort), release the parts only after success.
    // Only touch the map after the capacity check passes (same as put_object, otherwise a
    // failure leaves a null-pointer ghost object)
    auto it = b.objects.find(std::string(key));
    int64_t old = it != b.objects.end() && it->second.data ? int64_t(it->second.data->size()) : 0;
    reserve_locked(int64_t(data.size()) - old);
    b.objects.insert_or_assign(
        std::string(key),
        Object{std::move(meta), std::make_shared<const std::string>(std::move(data))});
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
    // parts is a std::map<int,...>, so iteration is already ascending; entries before the
    // marker are skipped directly
    std::vector<PartMeta> all;
    for (auto it = up.parts.upper_bound(opt.part_number_marker); it != up.parts.end(); ++it)
        all.push_back({it->first, it->second.data.size(), it->second.etag, it->second.uploaded,
                       it->second.checksum_algorithm, it->second.checksum_value});
    co_return apply_parts_page(std::move(all), opt);
}

Task<ListUploadsResult> MemoryBackend::list_multipart_uploads(std::string_view bucket,
                                                              const ListUploadsOptions& opt) {
    validate_bucket_name(bucket, kAllowReserved);
    std::lock_guard lk(m_);
    expire_uploads_locked();
    bucket_or_throw(std::string(bucket));
    // The index is keyed by upload_id, so per-bucket enumeration is inherently a full scan
    // + sort; the marker can only be applied after sorting
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
