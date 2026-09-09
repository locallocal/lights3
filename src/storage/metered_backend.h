// L3 decorator: per-backend operation timing and error counting (roadmap §5.1,
// docs/s3-protocol.md §7). Wraps a backend behind the bucket router so every
// data-plane call is measured once, uniformly for all six implementations:
//   - lights3_backend_op_seconds{backend,op} histogram (+ _count/_sum),
//     lights3_backend_errors_total{backend,op} — an error is anything that is not
//     an S3 4xx (NoSuchKey on a HEAD is the client's problem, a 5xx or a
//     transport exception is the backend's);
//   - the request-scoped RequestBackendStats attached to the cancellation token
//     (CancelSource::set_data at dispatch) accumulates the same durations, so the
//     access log can print the backend share of each request's latency.
// The time measured is the call's wall time: for put_object/upload_part that
// includes streaming the body in (which is what the backend spends its time on),
// for get_object it is the open only — bytes stream afterwards through the driver.
// The decorator also keeps the per-backend in-flight count that backend hot
// removal drains on (backlog-sequence ⑦): every call holds a lease for its
// duration, and a get_object lease travels with the returned body until the
// stream is released, so "in flight" covers streaming reads as well.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "core/metrics.h"
#include "storage/backend.h"
#include "storage/request_stats.h"

namespace lights3::storage {

class MeteredBackend final : public IStorageBackend {
public:
    MeteredBackend(std::string name, std::shared_ptr<IStorageBackend> inner, std::shared_ptr<MetricsRegistry> registry);

    const std::string& name() const { return name_; }
    const std::shared_ptr<IStorageBackend>& inner() const { return inner_; }

    // In-flight operations (calls in progress + open get_object streams)
    long inflight() const;
    // Block until nothing is in flight or the deadline passes; true = drained.
    // The lease release signals the condition, no polling (the
    // AsyncSemaphore::wait_drained shape, roadmap §4.5)
    bool wait_idle(std::chrono::milliseconds timeout);

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;
    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta, http::BodyReader& body,
                               PutCondition cond) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<std::optional<PutResult>> copy_object_fast(std::string_view src_bucket, std::string_view src_key,
                                                    std::string_view dst_bucket, std::string_view dst_key,
                                                    ObjectMeta meta) override;
    Task<std::optional<ObjectPartExtent>> resolve_object_part(std::string_view bucket, std::string_view key,
                                                              int part_no) override;
    Task<void> set_object_tagging(std::string_view bucket, std::string_view key, std::string tagging) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;
    Task<std::string> create_multipart(std::string_view bucket, std::string_view key, ObjectMeta meta) override;
    using IStorageBackend::upload_part;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key, std::string_view upload_id, int part_no,
                                http::BodyReader& body, const std::optional<PartChecksum>& checksum) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key, std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key, std::string_view upload_id) override;
    Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key, std::string_view upload_id,
                                     const ListPartsOptions& opt) override;
    Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket, const ListUploadsOptions& opt) override;
    Task<std::optional<ObjectLayout>> inspect_object(std::string_view bucket, std::string_view key) override;
    // Not forwarded: the application closes the raw backends it built (the router
    // holds the decorators, main.cc's admin tasks the raw instances)
    Task<void> close() override { co_return; }

private:
    // One op's instruments, created lazily per op name (bounded: the op set is the interface)
    struct OpMetrics {
        std::shared_ptr<MetricHistogram> latency;
        std::shared_ptr<MetricCounter> errors;
    };
    OpMetrics& op(const char* name);
    void record(OpMetrics& m, std::chrono::steady_clock::duration dt, bool error, const CancelToken& tok);

    template <class T>
    Task<T> timed(const char* name, Task<T> inner);

public:
    // Shared by the decorator and the leases handed to open streams: a stream may
    // outlive the decorator (a request holds the body after a reload dropped the
    // backend), so the counter lives in its own block
    struct Inflight {
        std::mutex m;
        std::condition_variable cv;
        long n = 0;
    };
    class Lease {
    public:
        Lease() = default;
        explicit Lease(std::shared_ptr<Inflight> in);
        Lease(Lease&& o) noexcept : in_(std::move(o.in_)) {}
        Lease& operator=(Lease&& o) noexcept {
            release();
            in_ = std::move(o.in_);
            return *this;
        }
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease() { release(); }
        void release();

    private:
        std::shared_ptr<Inflight> in_;
    };

private:
    std::string name_;
    std::shared_ptr<IStorageBackend> inner_;
    MetricsScope scope_;
    std::mutex mu_;
    std::map<std::string, OpMetrics> ops_;
    std::shared_ptr<Inflight> inflight_ = std::make_shared<Inflight>();
};

// Wrap every backend of the map (used for the bucket router; the raw map stays
// with the application for close() and the offline admin tasks)
std::map<std::string, std::shared_ptr<IStorageBackend>> meter_backends(
    const std::map<std::string, std::shared_ptr<IStorageBackend>>& backends, std::shared_ptr<MetricsRegistry> registry);

}  // namespace lights3::storage
