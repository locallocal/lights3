#include "storage/metered_backend.h"

#include <chrono>
#include <exception>

namespace lights3::storage {

namespace {
// Backend-side latency bands: sub-ms cache hits up to minute-long transfers
const std::vector<double> kOpBounds{0.001, 0.005, 0.02, 0.1, 0.5, 2.0, 10.0, 60.0};

// 4xx S3 errors are the client's outcome, not a backend failure
bool is_backend_error(std::exception_ptr ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const s3::S3Error& e) {
        return s3::http_status(e.code) >= 500;
    } catch (...) {
        return true;
    }
}
}  // namespace

MeteredBackend::MeteredBackend(std::string name, std::shared_ptr<IStorageBackend> inner,
                               std::shared_ptr<MetricsRegistry> registry)
    : name_(std::move(name)), inner_(std::move(inner)),
      scope_(registry ? MetricsScope(std::move(registry), {{"backend", name_}}) : MetricsScope()) {}

MeteredBackend::OpMetrics& MeteredBackend::op(const char* name) {
    std::lock_guard lk(mu_);
    auto it = ops_.find(name);
    if (it != ops_.end()) return it->second;
    OpMetrics m;
    m.latency = scope_.histogram("lights3_backend_op_seconds",
                                 "Storage backend operation wall time by backend and op",
                                 kOpBounds, {{"op", name}});
    m.errors = scope_.counter("lights3_backend_errors_total",
                              "Storage backend operations that failed (5xx or transport error)",
                              {{"op", name}});
    return ops_.emplace(name, std::move(m)).first->second;
}

void MeteredBackend::record(OpMetrics& m, std::chrono::steady_clock::duration dt, bool error,
                            const CancelToken& tok) {
    double secs = std::chrono::duration<double>(dt).count();
    m.latency->observe(secs);
    if (error) m.errors->inc();
    if (auto st = tok.data<RequestBackendStats>()) {
        st->nanos.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count(),
                            std::memory_order_relaxed);
        st->calls.fetch_add(1, std::memory_order_relaxed);
        if (error) st->errors.fetch_add(1, std::memory_order_relaxed);
    }
}

template <class T>
Task<T> MeteredBackend::timed(const char* name, Task<T> inner) {
    // The token is inherited from the awaiting handler chain: it carries the
    // request's RequestBackendStats when dispatch attached one
    CancelToken tok = co_await current_cancel();
    OpMetrics& m = op(name);
    auto t0 = std::chrono::steady_clock::now();
    std::exception_ptr ep;
    if constexpr (std::is_void_v<T>) {
        try {
            co_await std::move(inner);
        } catch (...) {
            ep = std::current_exception();
        }
        record(m, std::chrono::steady_clock::now() - t0, ep && is_backend_error(ep), tok);
        if (ep) std::rethrow_exception(ep);
        co_return;
    } else {
        std::optional<T> result;
        try {
            result.emplace(co_await std::move(inner));
        } catch (...) {
            ep = std::current_exception();
        }
        record(m, std::chrono::steady_clock::now() - t0, ep && is_backend_error(ep), tok);
        if (ep) std::rethrow_exception(ep);
        co_return std::move(*result);
    }
}

Task<void> MeteredBackend::create_bucket(std::string_view bucket) {
    co_return co_await timed("create_bucket", inner_->create_bucket(bucket));
}
Task<void> MeteredBackend::delete_bucket(std::string_view bucket) {
    co_return co_await timed("delete_bucket", inner_->delete_bucket(bucket));
}
Task<bool> MeteredBackend::bucket_exists(std::string_view bucket) {
    co_return co_await timed("bucket_exists", inner_->bucket_exists(bucket));
}
Task<std::vector<BucketInfo>> MeteredBackend::list_buckets() {
    co_return co_await timed("list_buckets", inner_->list_buckets());
}
Task<ObjectStream> MeteredBackend::get_object(std::string_view bucket, std::string_view key,
                                              std::optional<ByteRange> range) {
    co_return co_await timed("get_object", inner_->get_object(bucket, key, range));
}
Task<PutResult> MeteredBackend::put_object(std::string_view bucket, std::string_view key,
                                           ObjectMeta meta, http::BodyReader& body,
                                           PutCondition cond) {
    co_return co_await timed("put_object", inner_->put_object(bucket, key, std::move(meta), body, cond));
}
Task<ObjectMeta> MeteredBackend::head_object(std::string_view bucket, std::string_view key) {
    co_return co_await timed("head_object", inner_->head_object(bucket, key));
}
Task<std::optional<PutResult>> MeteredBackend::copy_object_fast(std::string_view src_bucket,
                                                                std::string_view src_key,
                                                                std::string_view dst_bucket,
                                                                std::string_view dst_key,
                                                                ObjectMeta meta) {
    co_return co_await timed("copy_object",
                             inner_->copy_object_fast(src_bucket, src_key, dst_bucket, dst_key,
                                                      std::move(meta)));
}
Task<std::optional<IStorageBackend::ObjectPartExtent>> MeteredBackend::resolve_object_part(
    std::string_view bucket, std::string_view key, int part_no) {
    co_return co_await timed("resolve_object_part",
                             inner_->resolve_object_part(bucket, key, part_no));
}
Task<void> MeteredBackend::set_object_tagging(std::string_view bucket, std::string_view key,
                                              std::string tagging) {
    co_return co_await timed("set_object_tagging",
                             inner_->set_object_tagging(bucket, key, std::move(tagging)));
}
Task<void> MeteredBackend::delete_object(std::string_view bucket, std::string_view key) {
    co_return co_await timed("delete_object", inner_->delete_object(bucket, key));
}
Task<ListResult> MeteredBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    co_return co_await timed("list_objects", inner_->list_objects(bucket, opt));
}
Task<std::string> MeteredBackend::create_multipart(std::string_view bucket, std::string_view key,
                                                   ObjectMeta meta) {
    co_return co_await timed("create_multipart", inner_->create_multipart(bucket, key, std::move(meta)));
}
Task<PutResult> MeteredBackend::upload_part(std::string_view bucket, std::string_view key,
                                            std::string_view upload_id, int part_no,
                                            http::BodyReader& body,
                                            const std::optional<PartChecksum>& checksum) {
    co_return co_await timed("upload_part",
                             inner_->upload_part(bucket, key, upload_id, part_no, body, checksum));
}
Task<PutResult> MeteredBackend::complete_multipart(std::string_view bucket, std::string_view key,
                                                   std::string_view upload_id,
                                                   std::span<const PartInfo> parts) {
    co_return co_await timed("complete_multipart",
                             inner_->complete_multipart(bucket, key, upload_id, parts));
}
Task<void> MeteredBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id) {
    co_return co_await timed("abort_multipart", inner_->abort_multipart(bucket, key, upload_id));
}
Task<ListPartsResult> MeteredBackend::list_parts(std::string_view bucket, std::string_view key,
                                                 std::string_view upload_id,
                                                 const ListPartsOptions& opt) {
    co_return co_await timed("list_parts", inner_->list_parts(bucket, key, upload_id, opt));
}
Task<ListUploadsResult> MeteredBackend::list_multipart_uploads(std::string_view bucket,
                                                               const ListUploadsOptions& opt) {
    co_return co_await timed("list_multipart_uploads", inner_->list_multipart_uploads(bucket, opt));
}

std::map<std::string, std::shared_ptr<IStorageBackend>> meter_backends(
    const std::map<std::string, std::shared_ptr<IStorageBackend>>& backends,
    std::shared_ptr<MetricsRegistry> registry) {
    std::map<std::string, std::shared_ptr<IStorageBackend>> out;
    for (auto& [name, b] : backends)
        out[name] = std::make_shared<MeteredBackend>(name, b, registry);
    return out;
}

}  // namespace lights3::storage
