// L3: CloudProxyBackend -- proxy backend mapping onto a public cloud (docs/cloudproxy-backend.md).
// Self-signed SigV4 + vendored httplib connecting directly to a remote S3-compatible
// endpoint; this header exposes no httplib types (all httplib details are contained inside
// remote_client.h/.cc).
#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/metrics.h"
#include "core/thread_pool.h"
#include "storage/backend.h"

// Forward declaration only — this header stays free of httplib includes; the
// retry_io member template is declared with the incomplete type and defined in
// the .cc (its only users live there)
namespace httplib {
class Result;
}

namespace lights3::storage {

struct CloudProxyConfig {
    std::string endpoint;  // scheme://host[:port]
    std::string region = "us-east-1";
    std::string access_key;
    std::string secret_key;
    std::string bucket_prefix;  // remote bucket = prefix + local name
    // false = virtual-hosted style (docs/cloudproxy-backend.md §7): connection and SNI always
    // point at the endpoint; only Host/signature and path vary per bucket -- requires the
    // remote to accept vhost Hosts under the endpoint certificate (the common shape for AWS
    // regional endpoints / S3-compatible gateways)
    bool force_path_style = true;
    // Short control-plane requests use a private thread instead of the shared pool
    // (docs/cloudproxy-backend.md §2.3): a high-RTT remote does not hold a pool thread; the
    // cost = one thread creation per control request. Benchmarks say default false (see §2.3)
    bool control_in_pump = false;
    bool tls_verify = true;
    std::string ca_cert;
    int connect_timeout_ms = 5000;
    int request_timeout_ms = 60000;
    int retry_max = 3;
    int retry_base_ms = 100;
    int max_connections = 16;
    // Connection-pool hygiene (roadmap §3.3): an idle connection older than
    // pool_idle_timeout is never reused (a NAT/remote that silently dropped it would
    // surface as periodic first-request retry spikes) and is closed by a light reaper;
    // pool_max_lifetime additionally retires connections by age at release (0 = off)
    int pool_idle_timeout_ms = 60'000;  // 0 = never expire idles
    int pool_max_lifetime_ms = 0;
    // Circuit breaker (roadmap §3.3): after `breaker_threshold` consecutive definitive
    // failures (transport error or 5xx; 429 is neutral) requests fail fast with SlowDown
    // for breaker_cooldown, then a single half-open probe decides. 0 = disabled
    int breaker_threshold = 10;
    int breaker_cooldown_ms = 10'000;
    // Total per-operation budget across the whole retry loop (roadmap §3.3): a retry is
    // skipped when its backoff would land past the deadline. Caps the retry loop only —
    // an in-flight transfer is never cut mid-stream. 0 = no cap (legacy worst case
    // (retry_max+1) x request_timeout)
    int op_deadline_ms = 0;
    // EC2 metadata service base URL for the credential chain (overridable for tests /
    // IMDS proxies); used only when access_key/secret_key are not configured
    std::string imds_endpoint = "http://169.254.169.254";
    bool verify_etag = true;             // docs/cloudproxy-backend.md §6: single-part PUT compares MD5 against the remote ETag
    size_t queue_cap_bytes = 1 << 20;    // data-plane BlockQueue capacity (backpressure watermark)
    // Spool for length-less uploads (docs/archive/gaps.md §6.2): 0 = disabled (back to
    // NotImplemented). The cap guards against abuse -- the spool lands on the gateway's
    // local disk, and AWS's 5GiB single-PUT limit is the natural default
    uint64_t spool_max_bytes = 5ull << 30;
    std::string spool_dir;               // empty = std::filesystem::temp_directory_path()

    // BackendConfig::params -> config; invalid values throw std::runtime_error at config-load time
    static CloudProxyConfig from_params(const std::string& name,
                                        const std::map<std::string, std::string>& params);
};

namespace cloudproxy {
struct RemoteContext;  // remote_client.h: ClientPool + signing pipeline + error mapping
}

class CloudProxyBackend final : public IStorageBackend {
public:
    // metrics defaults to an empty scope (docs/cloudproxy-backend.md §8.2): tests construct
    // directly without wiring, counts land on orphan instances
    CloudProxyBackend(CloudProxyConfig cfg, std::shared_ptr<ThreadPool> pool,
                      MetricsScope metrics = {});
    ~CloudProxyBackend() override;

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
    // GET ?partNumber (roadmap §2.5): the remote owns the part layout — resolved with a
    // HEAD ?partNumber=N upstream (Content-Range + x-amz-mp-parts-count)
    Task<std::optional<ObjectPartExtent>> resolve_object_part(std::string_view bucket,
                                                              std::string_view key,
                                                              int part_no) override;
    // ?tagging forwarded to the remote (roadmap §2.5)
    Task<void> set_object_tagging(std::string_view bucket, std::string_view key,
                                  std::string tagging) override;
    // Same-backend copy fast path (docs/archive/gaps.md §6.2): remote server-side COPY
    // (x-amz-copy-source) -- previously an intra-cloud copy would "download to the gateway
    // and upload back", doubling cross-network traffic and cost
    Task<std::optional<PutResult>> copy_object_fast(std::string_view src_bucket,
                                                    std::string_view src_key,
                                                    std::string_view dst_bucket,
                                                    std::string_view dst_key,
                                                    ObjectMeta meta) override;
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

private:
    // Local-name validation + prefix mapping; throws InvalidBucketName if the mapped name
    // exceeds 63 bytes
    std::string remote_bucket(std::string_view bucket) const;
    // Execution environment for control-plane blocking sections (docs/cloudproxy-backend.md
    // §2.3): pool thread by default; control_in_pump=true uses a one-shot private thread.
    // Defined in the .cc (used only in that TU)
    template <class Fn>
    Task<std::invoke_result_t<Fn>> control_io(Fn fn);
    // Coroutine-level retry driver for idempotent control-plane requests (roadmap §3.3):
    // per attempt — breaker gate, async pool lease, one blocking send via control_io —
    // then backoff via the TimerQueue (never sleeping on a pool thread), honoring the
    // remote's Retry-After and the per-op deadline. Returns the last Result after policy
    // exhaustion. Defined in the .cc
    template <class Fn>
    Task<httplib::Result> retry_io(const char* op, Fn fn);
    // TimerQueue sleep + hop back to a pool thread (the timer callback thread must not
    // run business logic)
    Task<void> async_backoff(int64_t delay_ms);
    // Outbound hop of the request's trace (roadmap §5.4): when the awaiting chain
    // carries the request payload (storage/request_stats.h), appends `traceparent`
    // (the gateway's own span, so the remote logs it as parent) and `tracestate`.
    // Background work (tiered demotion, GC) carries no request: nothing is added
    using Extra = std::vector<std::pair<std::string, std::string>>;
    Task<Extra> trace_extra(Extra extra = {});
    // Streaming upload shared by PUT / upload_part (docs/cloudproxy-backend.md §3.2).
    // resource is the client-view "/bucket/key" (goes into the error XML; does not leak the
    // prefixed remote path); multipart_ctx decides the semantic fallback for a body-less 404
    // (Upload / Bucket)
    Task<PutResult> stream_upload(std::string raw_path, std::string raw_query,
                                  std::string host, std::string content_type,
                                  std::vector<std::pair<std::string, std::string>> extra,
                                  http::BodyReader& body, std::string resource,
                                  bool multipart_ctx);
    // Length-less upload (docs/archive/gaps.md §6.2): AWS rejects bare chunked, so spool to a local
    // temp file first to obtain the length, then go through stream_upload -- previously this
    // was a flat NotImplemented, making chunked PUTs without x-amz-decoded-content-length
    // entirely unusable on this backend
    Task<PutResult> spool_and_upload(std::string raw_path, std::string raw_query,
                                     std::string host, std::string content_type,
                                     std::vector<std::pair<std::string, std::string>> extra,
                                     http::BodyReader& body, std::string resource,
                                     bool multipart_ctx);

    std::shared_ptr<cloudproxy::RemoteContext> ctx_;
    std::shared_ptr<ThreadPool> pool_;
    ThreadPoolExecutor exec_{*pool_};  // continuation posting after a control_in_pump private thread finishes (§2.3)
};

}  // namespace lights3::storage
