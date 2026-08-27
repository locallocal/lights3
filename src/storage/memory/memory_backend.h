// L3: in-memory backend -- for unit tests and demos, semantics aligned with LocalFs.
// It is also registered as a first-class backend (storage/registry.cc); misconfiguring it
// means putting the entire gateway's data into an unbounded heap, hence the capacity gate
// and mpu expiry cleanup (docs/archive/gaps.md §6.3)
#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "storage/backend.h"

namespace lights3::storage {

struct MemoryOptions {
    // Capacity cap (byte sum of objects + in-flight parts). 0 = unlimited -- the default
    // for unit tests and demos, preserving existing behavior; production configs should
    // set an explicit value; writes over the limit return 503 SlowDown
    uint64_t max_bytes = 0;
    // Expiry for multipart uploads never completed/aborted. 0 = never expire. Cleanup
    // piggybacks on multipart operation entry points (this backend takes no timer: having
    // no background threads is part of its role as a unit-test fixture)
    int mpu_ttl_sec = 24 * 3600;
};

class MemoryBackend final : public IStorageBackend {
public:
    MemoryBackend() = default;
    explicit MemoryBackend(MemoryOptions opt) : opt_(opt) {}

    // Usage (for tests and the registry's metrics callback)
    uint64_t used_bytes() const;
    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    using IStorageBackend::upload_part;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no, http::BodyReader& body,
                                const std::optional<PartChecksum>& checksum) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                               std::string_view upload_id) override;
    Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key,
                                     std::string_view upload_id,
                                     const ListPartsOptions& opt) override;
    Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket,
                                                   const ListUploadsOptions& opt) override;
    // Release everything resident (shutdown returns the memory; previously there was no
    // close and it stayed occupied until process teardown)
    Task<void> close() override;

private:
    // data is an immutable shared block (docs/archive/gaps.md §3.9): get_object only grabs the
    // shared_ptr once inside the lock; large objects are no longer copied wholesale under
    // the global lock. When put overwrites the same key, the old block is held by GETs
    // still streaming it and is released naturally when they finish -- snapshot isolation
    // for free.
    // The remaining critical sections are O(map operation), so this backend takes no
    // thread pool: in a demo/unit-test setting, keeping the microsecond-scale lock on the
    // event-loop thread is a deliberate trade-off
    struct Object {
        ObjectMeta meta;
        std::shared_ptr<const std::string> data;
    };
    struct Bucket {
        BucketInfo info;
        std::map<std::string, Object> objects;  // keys ordered
    };
    struct Part {
        std::string data;
        std::string etag;  // part content MD5 hex
        std::chrono::system_clock::time_point uploaded;
        // Verified part checksum (roadmap §2.2); empty = none declared
        std::string checksum_algorithm;
        std::string checksum_value;
    };
    struct Upload {
        std::string bucket;
        std::string key;
        ObjectMeta meta;
        std::map<int, Part> parts;  // part_no ordered
        std::chrono::system_clock::time_point initiated;
    };

    Bucket& bucket_or_throw(const std::string& name);
    Upload& upload_or_throw(std::string_view bucket, std::string_view key,
                            std::string_view upload_id);
    // Capacity gate (called holding m_): delta is this call's net byte increase; over the
    // limit throws SlowDown without touching the books
    void reserve_locked(int64_t delta);
    // Early gate while reading the body (called without m_): throws SlowDown once
    // used + this request's buffered bytes exceed the limit
    void check_inflight(size_t buffered) const;
    void expire_uploads_locked();  // mpu_ttl expiry cleanup (holding m_)

    MemoryOptions opt_;
    mutable std::mutex m_;
    uint64_t used_bytes_ = 0;  // byte sum of objects + in-flight parts (guarded by m_)
    std::map<std::string, Bucket> buckets_;
    std::map<std::string, Upload> uploads_;  // upload_id → state
};

}  // namespace lights3::storage
