// CloudProxyBackend implementation (docs/cloudproxy-backend.md).
// Generic pipeline: build a minimal HttpRequest for signing -> carry over headers ->
// send via ClientPool -> map errors. The data plane flips the push/pull model between a
// private pump thread and the handler coroutine through http/pushpull.h's BlockQueue;
// short control-plane requests call synchronously on shared pool threads
// (docs/cloudproxy-backend.md §2.3).
#include "storage/cloudproxy/cloudproxy_backend.h"
#include "storage/request_stats.h"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <optional>
#include <semaphore>
#include <thread>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/pushpull.h"
#include "s3/xml.h"
#include "storage/cloudproxy/remote_client.h"
#include "storage/localfs/fs_util.h"
#include "storage/multipart.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using namespace cloudproxy;

namespace {

// ETag quote handling and md5-shape detection reuse shared helpers (storage/multipart.h, util::from_hex)

bool is_md5_hex(const std::string& s) { return util::from_hex(s).size() == 16; }

// The key's path segment ("/key" after encoding); the bucket segment is carried by
// Target::prefix ("/<rb>" for path-style, empty for vhost, docs/cloudproxy-backend.md §7)
std::string key_path(std::string_view key) {
    return "/" + util::aws_uri_encode(key, /*encode_slash=*/false);
}

std::string qv(std::string_view v) { return util::aws_uri_encode(v, /*encode_slash=*/true); }

std::string format_range(const ByteRange& r) {
    std::string out = "bytes=";
    if (r.first) out += std::to_string(*r.first);
    out += "-";
    if (r.last) out += std::to_string(*r.last);
    return out;
}

// x-amz-meta-* and first-class metadata headers (signing sweeps them into SignedHeaders
// automatically; no signing-side changes needed)
std::vector<std::pair<std::string, std::string>> meta_headers(const ObjectMeta& meta) {
    std::vector<std::pair<std::string, std::string>> out;
    for (auto& f : kStdMetaFields)
        if (!(meta.*f.field).empty()) out.emplace_back(f.header, meta.*f.field);
    // Header-form checksum forwarded so the remote stores/verifies it too (roadmap §2.2).
    // Trailer-form values (checksum_pending) cannot be sent — headers leave before the
    // body is read — so they stop at this gateway's verification (documented limitation)
    if (!meta.checksum_algorithm.empty() && !meta.checksum_value.empty()) {
        std::string h = "x-amz-checksum-";
        for (char c : meta.checksum_algorithm) h.push_back(http::HeaderMap::lower(c));
        out.emplace_back(std::move(h), meta.checksum_value);
    }
    for (auto& [k, v] : meta.user_meta) out.emplace_back("x-amz-meta-" + k, v);
    return out;
}

uint64_t parse_u64(const std::string& s) {
    try {
        return std::stoull(s);
    } catch (...) {
        return 0;
    }
}

// The full object length must be known (backend.h contract: meta.size is the full object
// length); when the remote gives no Content-Length (e.g. chunked responses), prefer erroring
// out over silently truncating to length 0
uint64_t require_content_length(const httplib::Response& res) {
    if (!res.has_header("Content-Length"))
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote response lacks Content-Length");
    try {
        return std::stoull(res.get_header_value("Content-Length"));
    } catch (...) {
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote sent invalid Content-Length");
    }
}

ObjectMeta meta_from_response(std::string_view key, const httplib::Response& res) {
    ObjectMeta m;
    m.key = std::string(key);
    m.etag = strip_etag_quotes(res.get_header_value("ETag"));
    m.size = require_content_length(res);
    if (res.has_header("Content-Type")) m.content_type = res.get_header_value("Content-Type");
    if (auto t = util::parse_http_date(res.get_header_value("Last-Modified")))
        m.last_modified = *t;
    for (auto& f : kStdMetaFields)
        if (res.has_header(f.header)) m.*f.field = res.get_header_value(f.header);
    // Remote checksum echo (roadmap §2.2): present when the request carried
    // x-amz-checksum-mode: ENABLED (GET/HEAD below always send it)
    for (auto& [k, v] : res.headers) {
        constexpr std::string_view kCk = "x-amz-checksum-";
        if (k.size() > kCk.size() &&
            http::HeaderMap::ieq(std::string_view(k).substr(0, kCk.size()), kCk)) {
            std::string algo = k.substr(kCk.size());
            for (char& c : algo) c = http::HeaderMap::lower(c);
            if (algo == "type") {
                m.checksum_type = v;
            } else if (algo == "crc32" || algo == "crc32c" || algo == "crc64nvme" ||
                       algo == "sha1" || algo == "sha256") {
                for (char& c : algo) c = char(toupper(static_cast<unsigned char>(c)));
                m.checksum_algorithm = std::move(algo);
                m.checksum_value = v;
            }
        }
    }
    for (auto& [k, v] : res.headers) {
        constexpr std::string_view kMetaPrefix = "x-amz-meta-";
        if (k.size() > kMetaPrefix.size() &&
            http::HeaderMap::ieq(std::string_view(k).substr(0, kMetaPrefix.size()),
                                 kMetaPrefix)) {
            std::string mk = k.substr(kMetaPrefix.size());
            for (char& c : mk) c = http::HeaderMap::lower(c);
            m.user_meta[mk] = v;
        }
    }
    return m;
}

// Payload delivered to the waiting coroutine once the GET headers arrive
// (docs/cloudproxy-backend.md §3.1 step 1)
struct GetHead {
    ObjectMeta meta;
    std::optional<ByteRange> range;
    std::optional<uint64_t> body_len;
};

GetHead head_from_response(std::string_view key, const httplib::Response& res) {
    GetHead h;
    h.meta = meta_from_response(key, res);
    h.body_len = h.meta.size;
    if (res.status == 206) {
        // Content-Range: bytes a-b/total -- meta.size is the full object length (interface
        // contract). An unparsable value (including the RFC-permitted "bytes a-b/*"
        // unknown-total form) must be an error: silently falling back to full-object
        // semantics would return partial content to the client as a complete object
        std::string cr = res.get_header_value("Content-Range");
        unsigned long long a = 0, b = 0, total = 0;
        if (sscanf(cr.c_str(), "bytes %llu-%llu/%llu", &a, &b, &total) != 3)
            throw S3Error(S3ErrorCode::InternalError,
                          "cloudproxy: remote 206 has missing or unparsable Content-Range: " +
                              cr);
        h.meta.size = total;
        h.range = ByteRange{a, b};
        h.body_len = b - a + 1;
    }
    return h;
}

// Abort an in-flight httplib transfer from another thread. Cancelling the queue only
// unblocks a pump stuck in push; if the pump is blocked in a socket read/write (remote
// stalled), client.stop() must interrupt it, or the aborter is held hostage until the
// read/write timeout (default 60s). A stopped connection, once returned to the pool, is
// reconnected automatically by httplib on the next request.
struct TransferAbort {
    std::mutex m;
    httplib::Client* active = nullptr;
    bool aborted = false;

    void arm(httplib::Client& c) {
        std::lock_guard lk(m);
        active = &c;
        if (aborted) c.stop();
    }
    void disarm() {
        std::lock_guard lk(m);
        active = nullptr;
    }
    void abort() {
        std::lock_guard lk(m);
        aborted = true;
        if (active) active->stop();
    }
    bool is_aborted() {
        std::lock_guard lk(m);
        return aborted;
    }
};

// Destruction = cancel + interrupt the in-flight transfer + join the pump: aborts the
// remote transfer on client disconnect / handler exception (docs/cloudproxy-backend.md §3.1)
class PumpBodyReader final : public http::BodyReader {
public:
    PumpBodyReader(std::shared_ptr<http::BlockQueue> q, std::optional<uint64_t> len,
                   std::shared_ptr<TransferAbort> abort, std::thread pump,
                   std::shared_ptr<ThreadPool> pool)
        : q_(q), inner_(std::move(q), len), abort_(std::move(abort)), pump_(std::move(pump)),
          pool_(std::move(pool)) {}
    ~PumpBodyReader() override {
        q_->cancel();
        abort_->abort();
        if (pump_.joinable()) pump_.join();
    }
    // QueueBodyReader::pop blocks on a cv: when the remote is slower than the client (the
    // cloudproxy norm) almost every read waits for the pump to push data. The caller may be
    // beast's io thread / a seastar reactor shard (where the reading coroutine resumes), and
    // blocking there stalls the whole event loop -- switch to a pool thread before blocking,
    // consistent with the other streaming readers in the project (the outbound
    // stream_upload already does this)
    Task<size_t> read(std::span<std::byte> buf) override {
        co_await pool_->schedule();
        co_return co_await inner_.read(buf);
    }
    std::optional<uint64_t> length() const override { return inner_.length(); }

private:
    std::shared_ptr<http::BlockQueue> q_;
    http::QueueBodyReader inner_;
    std::shared_ptr<TransferAbort> abort_;
    std::thread pump_;
    std::shared_ptr<ThreadPool> pool_;
};

std::string resource_of(std::string_view bucket, std::string_view key = "") {
    std::string r = "/" + std::string(bucket);
    if (!key.empty()) r += "/" + std::string(key);
    return r;
}

// The total-ETag rule reuses combined_etag (used by docs/cloudproxy-backend.md §5.2
// complete ambiguity resolution)
std::string expected_total_etag(std::span<const PartInfo> parts) {
    std::vector<std::string> md5s;
    md5s.reserve(parts.size());
    for (auto& p : parts) {
        std::string hex(strip_etag_quotes(p.etag));
        if (!is_md5_hex(hex)) return "";  // non-md5-shaped part etag: unpredictable
        md5s.push_back(std::move(hex));
    }
    return combined_etag(md5s);
}

}  // namespace

// ---------- Construction ----------

CloudProxyBackend::CloudProxyBackend(CloudProxyConfig cfg, std::shared_ptr<ThreadPool> pool,
                                     MetricsScope metrics)
    : pool_(std::move(pool)) {
    auto ep = Endpoint::parse(cfg.endpoint);
    ctx_ = std::make_shared<RemoteContext>(std::move(cfg), ep, metrics);
    // Async pool waiters resume business logic on pool threads, never on the
    // releasing/timer thread (roadmap §3.3)
    ctx_->pool.set_resume_executor(&exec_);
    LOG_INFO("cloudproxy backend: endpoint={} region={} prefix='{}' style={} control={} "
             "credentials={}",
             ctx_->cfg.endpoint, ctx_->cfg.region, ctx_->cfg.bucket_prefix,
             ctx_->cfg.force_path_style ? "path" : "vhost",
             ctx_->cfg.control_in_pump ? "pump" : "pool",
             ctx_->cred_chain ? "chain(env/container/imds)" : "static");
}

CloudProxyBackend::~CloudProxyBackend() = default;

// Execution environment for control-plane blocking sections (docs/cloudproxy-backend.md
// §2.3): by default switch to a shared pool thread (each occupancy ~ one remote round trip
// + retry backoff); control_in_pump=true spawns a one-shot private thread (same family as
// the data-plane pump), and on completion the continuation resumes via the pool executor --
// a high-RTT remote does not hold a pool thread, at the cost of one thread creation per
// control request (~tens of microseconds, benchmarks in §2.3). fn is a purely blocking
// function and must not contain co_await; exceptions pass through verbatim via exception_ptr
template <class Fn>
Task<std::invoke_result_t<Fn>> CloudProxyBackend::control_io(Fn fn) {
    using R = std::invoke_result_t<Fn>;
    if (!ctx_->cfg.control_in_pump) {
        co_await pool_->schedule();
        co_return fn();
    }
    struct Awaiter {
        Fn* fn;
        IExecutor* ex;
        std::optional<R> result;
        std::exception_ptr err;
        std::thread th;
        std::binary_semaphore gate{0};  // gate the thread body: it must not run before the move-assignment to th completes
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            // Without the gate there is a race: a very fast fn could post before the
            // `th = ...` assignment finishes; the pool thread resumes the coroutine and
            // destroys this awaiter (reading th), trampling the move-assignment on this thread
            th = std::thread([this, h] {
                gate.acquire();
                try {
                    result.emplace((*fn)());
                } catch (...) {
                    err = std::current_exception();
                }
                ex->post(h);  // the private thread only posts; business logic continues on a pool thread
            });
            gate.release();  // must not touch any member after this (the coroutine may have already resumed on a pool thread)
        }
        R await_resume() {
            th.join();  // the thread wraps up right after post; the join is only microseconds
            if (err) std::rethrow_exception(err);
            return std::move(*result);
        }
    };
    co_return co_await Awaiter{&fn, &exec_};
}

Task<CloudProxyBackend::Extra> CloudProxyBackend::trace_extra(Extra extra) {
    CancelToken tok = co_await current_cancel();
    if (auto st = tok.data<RequestBackendStats>(); st && st->trace.valid()) {
        extra.emplace_back("traceparent", st->trace.traceparent());
        if (!st->trace.tracestate.empty()) extra.emplace_back("tracestate", st->trace.tracestate);
    }
    co_return extra;
}

Task<void> CloudProxyBackend::async_backoff(int64_t delay_ms) {
    co_await async_sleep(std::chrono::milliseconds(delay_ms));
    co_await pool_->schedule();  // off the timer callback thread before any business logic
}

// Coroutine retry driver for idempotent control-plane requests (roadmap §3.3): replaces
// the old blocking with_retry, whose backoff slept on a pool thread — worst case 700ms
// per request, and a jittery remote would eat the pool wholesale with concurrent
// backoffs. Per attempt: breaker gate (fail fast when the remote is decidedly down) →
// async lease (a pool thread never parks in the pool's cv) → one blocking send inside
// control_io → outcome into the breaker. Between attempts: Retry-After-aware backoff on
// the TimerQueue, bounded by the per-op deadline. The lease is scoped to the attempt so
// no pooled connection is held through a backoff.
template <class Fn>
Task<httplib::Result> CloudProxyBackend::retry_io(const char* op, Fn fn) {
    auto hist = ctx_->metrics.op_seconds(op);
    const auto deadline = ctx_->op_deadline();
    for (int attempt = 0;; ++attempt) {
        ctx_->breaker_gate();
        httplib::Result r = co_await [&]() -> Task<httplib::Result> {
            auto lease = co_await ctx_->pool.acquire_async();
            co_return co_await control_io(
                [&] { return ctx_->attempt(hist, lease.client(), fn); });
        }();
        ctx_->breaker_observe(r);
        bool retry = !r ? RemoteContext::retryable_transport(r.error())
                        : ctx_->retryable_status(r->status);
        if (!retry || attempt >= ctx_->cfg.retry_max) co_return r;
        auto delay = ctx_->backoff_delay_ms(attempt, RemoteContext::retry_after_hint(r));
        if (!RemoteContext::deadline_allows(deadline, delay)) co_return r;
        ctx_->metrics.count_retry(op);
        co_await async_backoff(delay);
    }
}

// Remote transliterated name for the reserved bucket .sys: a leading '.' is illegal under
// S3 naming rules, and prefix concatenation would also produce adjacent "-." ("e2e-" +
// ".sys"), which both real AWS and a lights3 remote reject -- with naive concatenation, the
// "cloudproxy as default backend + dynamic credentials" combination is guaranteed to blow
// up at startup
inline constexpr std::string_view kRemoteSysBucket = "lights3-sys";

std::string CloudProxyBackend::remote_bucket(std::string_view bucket) const {
    validate_bucket_name(bucket, kAllowReserved);
    std::string_view local = bucket;
    if (bucket == kSysBucketName) {
        local = kRemoteSysBucket;
    } else if (bucket == kRemoteSysBucket) {
        // Name-collision guard: a user bucket that happens to bear the transliterated name
        // would merge with the remote credential bucket, and being able to read .sys
        // objects means credential leakage -- the name is reserved for this backend
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "bucket name is reserved by the cloudproxy backend",
                      std::string(bucket));
    }
    std::string rb = ctx_->cfg.bucket_prefix + std::string(local);
    if (rb.size() > 63)
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "bucket name with cloudproxy bucket_prefix exceeds 63 bytes",
                      std::string(bucket));
    return rb;
}

// ---------- Bucket operations (docs/cloudproxy-backend.md §4.3) ----------

Task<void> CloudProxyBackend::create_bucket(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto resource = resource_of(bucket);
    std::string body;
    if (ctx_->cfg.region != "us-east-1") {
        s3::XmlWriter w;
        w.open("CreateBucketConfiguration");
        w.element("LocationConstraint", ctx_->cfg.region);
        w.close();
        body = w.str();
    }
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("create_bucket", [&](httplib::Client& c) {
        auto headers = ctx_->signed_headers("PUT", path, "", tp,
                                            body.empty() ? "" : util::sha256_hex(body),
                                            t.host);
        return c.Put(path, headers, body, body.empty() ? "" : "application/xml");
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::None, resource);
}

Task<void> CloudProxyBackend::delete_bucket(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("delete_bucket", [&](httplib::Client& c) {
        return c.Delete(path, ctx_->signed_headers("DELETE", path, "", tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));
}

Task<bool> CloudProxyBackend::bucket_exists(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("head_bucket", [&](httplib::Client& c) {
        return c.Head(path, ctx_->signed_headers("HEAD", path, "", tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return true;
    if (res->status == 404) co_return false;
    if (res->status == 403) {
        // AWS HeadBucket semantics: exists-but-unauthorized is also 403; treat as existing
        // (docs/cloudproxy-backend.md §4.3)
        LOG_WARN("cloudproxy: HEAD bucket {} returned 403, treating as exists", rb);
        co_return true;
    }
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));
}

Task<std::vector<BucketInfo>> CloudProxyBackend::list_buckets() {
    // Service-level operations always go to the endpoint itself, regardless of addressing style
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("list_buckets", [&](httplib::Client& c) {
        return c.Get("/", ctx_->signed_headers("GET", "/", "", tp, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::None, "/");
    std::vector<BucketInfo> out;
    auto root = s3::xml_parse(res->body);
    const auto& prefix = ctx_->cfg.bucket_prefix;
    if (auto* buckets = root.find("Buckets")) {
        for (auto& b : buckets->children) {
            if (b.name != "Bucket") continue;
            std::string name = b.get("Name");
            // Keep only prefixed names and return them with the prefix stripped; the rest
            // are unrelated buckets under the remote account
            if (name.size() <= prefix.size() || name.compare(0, prefix.size(), prefix) != 0)
                continue;
            BucketInfo info;
            info.name = name.substr(prefix.size());
            // Reverse the transliteration (dual of remote_bucket): the upper layer still
            // sees .sys, handled by L2's reserved-bucket filtering, rather than the name
            // showing up in the listing masquerading as a user bucket
            if (info.name == kRemoteSysBucket) info.name = kSysBucketName;
            if (auto t = util::parse_iso8601(b.get("CreationDate"))) info.created = *t;
            out.push_back(std::move(info));
        }
    }
    co_return out;
}

// ---------- Object data plane (docs/cloudproxy-backend.md §3) ----------

Task<ObjectStream> CloudProxyBackend::get_object(std::string_view bucket, std::string_view key,
                                                 std::optional<ByteRange> range) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto resource = resource_of(bucket, key);
    std::vector<std::pair<std::string, std::string>> extra;
    if (range) extra.emplace_back("Range", format_range(*range));
    extra.emplace_back("x-amz-checksum-mode", "ENABLED");  // capture the remote checksum (§2.2)
    extra = co_await trace_extra(std::move(extra));

    co_await pool_->schedule();
    auto ctx = ctx_;
    auto queue = std::make_shared<http::BlockQueue>(ctx->cfg.queue_cap_bytes);
    auto abortst = std::make_shared<TransferAbort>();
    auto prom = std::make_shared<std::promise<GetHead>>();
    auto fut = prom->get_future();
    std::string keycopy(key);

    // pump: the ResponseHandler delivers meta on arrival; the ContentReceiver converts push
    // to pull through the queue (§3.1)
    std::thread pump([ctx, queue, abortst, prom, path, extra, resource, keycopy,
                      host = t.host] {
        auto op_hist = ctx->metrics.op_seconds("get");  // §8.2: the whole transfer is one observation
        const auto deadline = ctx->op_deadline();  // §3.3: caps the retry loop, not a transfer
        bool delivered = false;
        try {
            for (int attempt = 0;; ++attempt) {
                ctx->breaker_gate();  // fail fast while the remote is decidedly down (§3.3)
                std::string err_body;
                int64_t delay_ms = 0;
                {
                    auto lease = ctx->pool.acquire();
                    abortst->arm(lease.client());
                    auto headers = ctx->signed_headers("GET", path, "", extra, "", host);
                    auto t0 = std::chrono::steady_clock::now();
                    auto res = lease.client().Get(
                        path, headers,
                        [&](const httplib::Response& r) {
                            if (r.status != 200 && r.status != 206) return true;
                            try {
                                auto h = head_from_response(keycopy, r);
                                delivered = true;
                                prom->set_value(std::move(h));
                                return true;
                            } catch (...) {
                                // Headers violate the contract (e.g. 206 missing
                                // Content-Range): deliver the exception and abort the
                                // transfer; never silently proceed with full-object semantics
                                delivered = true;
                                prom->set_exception(std::current_exception());
                                return false;
                            }
                        },
                        [&](const char* data, size_t n) {
                            if (delivered) return queue->push(data, n);
                            if (err_body.size() < 64 * 1024) err_body.append(data, n);
                            return true;
                        });
                    abortst->disarm();
                    op_hist->observe(std::chrono::duration<double>(
                                         std::chrono::steady_clock::now() - t0)
                                         .count());
                    // A deliberately aborted transfer (client gone) says nothing about
                    // the remote's health
                    if (!abortst->is_aborted()) ctx->breaker_observe(res);
                    if (delivered) {
                        queue->close(static_cast<bool>(res));  // empty res = transfer failed midway
                        return;
                    }
                    // Headers not delivered: retry or deliver the mapped exception.
                    // The remote's Retry-After (429/503) overrides the formula; a retry
                    // whose backoff would land past the per-op deadline is not taken
                    bool retry = (!res ? RemoteContext::retryable_transport(res.error())
                                       : ctx->retryable_status(res->status)) &&
                                 attempt < ctx->cfg.retry_max;
                    if (retry) {
                        delay_ms = ctx->backoff_delay_ms(
                            attempt, RemoteContext::retry_after_hint(res));
                        if (!RemoteContext::deadline_allows(deadline, delay_ms))
                            retry = false;
                    }
                    if (!retry) {
                        try {
                            if (!res) ctx->throw_transport_error(res.error());
                            ctx->throw_remote_error(res->status, err_body, ErrCtx::Key,
                                                    resource);
                        } catch (...) {
                            prom->set_exception(std::current_exception());
                        }
                        return;
                    }
                    ctx->metrics.count_retry("get");
                }  // return the connection before backing off
                // Private per-transfer thread: a blocking sleep costs no pool capacity
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            }
        } catch (...) {
            // Surprises such as acquire timeout: choose the propagation path by delivery stage
            if (!delivered)
                prom->set_exception(std::current_exception());
            else
                queue->close(false);
        }
    });

    GetHead head;
    try {
        // A pool thread blocks waiting for headers; the pump advances on a private thread,
        // no mutual waiting (§2.3). The wait must be bounded (docs §3.1: timeout =
        // request_timeout) -- otherwise a trickling remote can keep a single Get from ever
        // completing, and with concurrent GETs ~ pool size the shared pool fills up and
        // everything stalls. Leave headroom for the connection-establishment budget: the
        // retry chain is at worst (retry_max+1) rounds, and each round's actual IO is
        // backstopped by httplib's own timeouts
        auto budget = std::chrono::milliseconds(ctx_->cfg.request_timeout_ms) *
                      (ctx_->cfg.retry_max + 1);
        if (ctx_->cfg.op_deadline_ms > 0)  // §3.3: the per-op deadline caps the whole loop
            budget = std::min(budget, std::chrono::milliseconds(ctx_->cfg.op_deadline_ms));
        if (fut.wait_for(budget) != std::future_status::ready) {
            abortst->abort();   // interrupt in-flight socket IO; do not sit out httplib's timeout
            queue->cancel();
            pump.join();
            ctx_->metrics.count_error("transport");
            throw S3Error(S3ErrorCode::SlowDown,
                          "cloudproxy: timed out waiting for remote response headers");
        }
        head = fut.get();
    } catch (...) {
        if (pump.joinable()) pump.join();
        throw;
    }
    ObjectStream out;
    out.meta = std::move(head.meta);
    out.range = head.range;
    out.body = std::make_unique<PumpBodyReader>(queue, head.body_len, abortst,
                                                std::move(pump), pool_);
    co_return out;
}

Task<PutResult> CloudProxyBackend::stream_upload(
    std::string raw_path, std::string raw_query, std::string host, std::string content_type,
    std::vector<std::pair<std::string, std::string>> extra, http::BodyReader& body,
    std::string resource, bool multipart_ctx) {
    extra = co_await trace_extra(std::move(extra));
    auto len_opt = body.length();
    // AWS rejects bare chunked uploads (§3.2). Without a length (chunked and no
    // x-amz-decoded-content-length), spool to a local temp file first to obtain the length,
    // then upload (docs/archive/gaps.md §6.2 -- previously a flat NotImplemented, making such PUTs
    // entirely unusable on this backend)
    if (!len_opt) {
        if (ctx_->cfg.spool_max_bytes == 0)
            throw S3Error(S3ErrorCode::NotImplemented,
                          "cloudproxy: upload without content length is not supported "
                          "(spool disabled)");
        co_return co_await spool_and_upload(std::move(raw_path), std::move(raw_query),
                                            std::move(host), std::move(content_type),
                                            std::move(extra), body, std::move(resource),
                                            multipart_ctx);
    }
    const uint64_t len = *len_opt;
    std::string full = raw_query.empty() ? raw_path : raw_path + "?" + raw_query;

    co_await pool_->schedule();
    auto ctx = ctx_;
    auto queue = std::make_shared<http::BlockQueue>(ctx->cfg.queue_cap_bytes);
    auto abortst = std::make_shared<TransferAbort>();

    struct Outcome {
        bool has_response = false;
        int status = 0;
        std::string resp_body;
        std::string etag;
        httplib::Error err = httplib::Error::Unknown;
        std::exception_ptr exc;
    };
    auto out = std::make_shared<Outcome>();

    // pump: pull-to-pull, the Provider takes data from the queue and writes the DataSink (§3.2)
    const char* op = multipart_ctx ? "upload_part" : "put";
    std::thread pump([ctx, queue, abortst, out, raw_path, raw_query, host, full, content_type,
                      extra, len, op] {
        auto op_hist = ctx->metrics.op_seconds(op);  // §8.2: the whole transfer is one observation
        const auto deadline = ctx->op_deadline();  // §3.3: caps the retry loop, not a transfer
        try {
            for (int attempt = 0;; ++attempt) {
                ctx->breaker_gate();  // fail fast while the remote is decidedly down (§3.3)
                // Single-threaded reads/writes within the pump suffice, no atomics needed:
                // only used for the connection-stage retry decision
                bool provider_called = false;
                auto lease = ctx->pool.acquire();
                abortst->arm(lease.client());
                auto headers = ctx->signed_headers("PUT", raw_path, raw_query, extra,
                                                   kUnsignedPayload, host);
                auto t0 = std::chrono::steady_clock::now();
                auto res = lease.client().Put(
                    full, headers, static_cast<size_t>(len),
                    [&](size_t /*offset*/, size_t length, httplib::DataSink& sink) {
                        provider_called = true;
                        std::byte buf[64 * 1024];
                        size_t want = std::min(sizeof(buf), length);
                        size_t n = 0;
                        try {
                            n = queue->pop(std::span(buf, want));
                        } catch (...) {
                            return false;  // producer (client upstream) failed midway
                        }
                        if (n == 0) return false;  // EOF before Content-Length: abort
                        return sink.write(reinterpret_cast<const char*>(buf), n);
                    },
                    content_type);
                abortst->disarm();
                op_hist->observe(std::chrono::duration<double>(
                                     std::chrono::steady_clock::now() - t0)
                                     .count());
                // A deliberately aborted transfer says nothing about the remote's
                // health; nor does a mid-transfer failure with no response — it may be
                // our own producer breaking off (provider false ⇒ canceled), so only a
                // response or a pre-provider (connection-stage) failure is observed
                if (!abortst->is_aborted() && (res || !provider_called))
                    ctx->breaker_observe(res);
                // Retry only when the failure is in the connection-establishment stage and
                // the Provider was never called (queue unconsumed) (§5.2); a deliberately
                // aborted transfer is not retried, and a backoff past the per-op deadline
                // is not taken
                if (!res && !provider_called && !abortst->is_aborted() &&
                    RemoteContext::connection_stage_error(res.error()) &&
                    attempt < ctx->cfg.retry_max) {
                    auto delay = ctx->backoff_delay_ms(attempt);
                    if (RemoteContext::deadline_allows(deadline, delay)) {
                        ctx->metrics.count_retry(op);
                        // Private per-transfer thread: blocking sleep costs no pool capacity
                        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
                        continue;
                    }
                }
                if (res) {
                    out->has_response = true;
                    out->status = res->status;
                    out->resp_body = res->body;
                    out->etag = res->get_header_value("ETag");
                } else {
                    out->err = res.error();
                }
                break;
            }
        } catch (...) {
            out->exc = std::current_exception();
        }
        queue->cancel();  // release the producer from a possible push block
    });

    // Producer: the handler coroutine chain drives body.read, with incremental MD5
    // (§6 end-to-end verification)
    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t sent = 0;
    bool remote_gone = false;
    std::exception_ptr read_err;
    std::vector<std::byte> buf(64 * 1024);
    try {
        // Read until EOF (n==0) rather than stopping at sent==len: the storage-backend
        // contract requires draining the body -- the verification decorators (sha256/chunked
        // checks) hook at full-read/EOF, and stopping at full-read would skip them
        for (;;) {
            size_t n = co_await body.read(std::span(buf));
            // body.read may resume the coroutine on an L1 driver thread (beast returns to
            // the strand via symmetric transfer); push can block on backpressure, so it must
            // be done back on a pool thread, never holding the event loop (§2.3)
            co_await pool_->schedule();
            if (n == 0) break;
            md5.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
            if (!queue->push(reinterpret_cast<const char*>(buf.data()), n)) {
                remote_gone = true;
                break;
            }
            sent += n;
        }
    } catch (...) {
        read_err = std::current_exception();
    }
    queue->close(!read_err && !remote_gone && sent == len);
    // When the upstream breaks, the pump may be blocked in a socket write waiting for the
    // remote to accept data: interrupt proactively, do not sit out the timeout
    if (read_err) abortst->abort();
    co_await pool_->schedule();  // the join waits at most one remote response cycle, likewise off the driver thread
    pump.join();

    if (out->exc) std::rethrow_exception(out->exc);
    if (read_err) std::rethrow_exception(read_err);  // client upstream broke off
    if (!out->has_response) ctx->throw_transport_error(out->err);
    if (out->status / 100 == 2) {
        std::string etag(strip_etag_quotes(out->etag));
        if (ctx->cfg.verify_etag && is_md5_hex(etag)) {
            if (etag != md5.final_hex()) {
                ctx->metrics.etag_mismatch->inc();  // §8.2: in-transit corruption signal
                throw S3Error(S3ErrorCode::InternalError,
                              "cloudproxy: upload corrupted in transit (remote etag != "
                              "local md5)");
            }
        }
        co_return PutResult{etag};
    }
    ctx->throw_remote_error(out->status, out->resp_body,
                            multipart_ctx ? ErrCtx::Upload : ErrCtx::Bucket, resource);
}

// Spool for length-less uploads (docs/archive/gaps.md §6.2): the body lands fully in a temp file
// (O_TMPFILE anonymous inode, auto-reclaimed on process crash; filesystems without support
// fall back to unlink-after-open); once the length is known, go through the known-length
// stream_upload via FdStreamReader. The cost is one local disk write/read plus
// full-arrival latency -- a trade-off for "rare-path availability"
Task<PutResult> CloudProxyBackend::spool_and_upload(
    std::string raw_path, std::string raw_query, std::string host, std::string content_type,
    std::vector<std::pair<std::string, std::string>> extra, http::BodyReader& body,
    std::string resource, bool multipart_ctx) {
    co_await pool_->schedule();
    namespace fs = std::filesystem;
    fs::path dir = ctx_->cfg.spool_dir.empty() ? fs::temp_directory_path()
                                               : fs::path(ctx_->cfg.spool_dir);
    int fd = ::open(dir.c_str(), O_TMPFILE | O_RDWR, 0600);
    if (fd < 0) {
        // O_TMPFILE unsupported (old kernels/NFS): create named, then unlink immediately;
        // equally leaves no residue
        fs::path p = dir / ("lights3-spool-" + std::to_string(::getpid()) + "-" +
                            std::to_string(reinterpret_cast<uintptr_t>(&body)));
        fd = ::open(p.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd < 0)
            throw S3Error(S3ErrorCode::InternalError, "cloudproxy: cannot create spool file");
        ::unlink(p.c_str());
    }
    struct FdGuard {
        int fd;
        ~FdGuard() { ::close(fd); }
    } guard{fd};

    uint64_t total = 0;
    std::vector<std::byte> buf(256 * 1024);
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        co_await pool_->schedule();  // read may resume the coroutine on a driver thread; write to disk back on the pool
        if (n == 0) break;
        total += n;
        if (total > ctx_->cfg.spool_max_bytes)
            throw S3Error(S3ErrorCode::EntityTooLarge,
                          "cloudproxy: unsized upload exceeds spool_max_bytes");
        const char* p = reinterpret_cast<const char*>(buf.data());
        size_t left = n;
        while (left > 0) {
            ssize_t w = ::write(fd, p, left);
            if (w < 0) throw S3Error(S3ErrorCode::InternalError, "cloudproxy: spool write failed");
            p += w;
            left -= size_t(w);
        }
    }
    // FdStreamReader takes over fd ownership; the guard steps aside
    int owned = fd;
    guard.fd = -1;
    fsutil::FdStreamReader replay(owned, 0, total, pool_);
    co_return co_await stream_upload(std::move(raw_path), std::move(raw_query), std::move(host),
                                     std::move(content_type), std::move(extra), replay,
                                     std::move(resource), multipart_ctx);
}

// Server-side COPY (docs/archive/gaps.md §6.2): previously a copy within the same cloudproxy
// backend would "download to the gateway and upload back", doubling cross-network traffic
// and cost, when the remote could have done it with one x-amz-copy-source. Always send
// REPLACE + our metadata -- the handler has already folded COPY/REPLACE semantics into
// meta, and the remote just copies it verbatim
Task<std::optional<PutResult>> CloudProxyBackend::copy_object_fast(
    std::string_view src_bucket, std::string_view src_key, std::string_view dst_bucket,
    std::string_view dst_key, ObjectMeta meta) {
    validate_object_key(src_key);
    validate_object_key(dst_key);
    auto src_rb = remote_bucket(src_bucket);
    auto dst_rb = remote_bucket(dst_bucket);
    auto tgt = ctx_->target(dst_rb);
    auto path = tgt.object_path(key_path(dst_key));
    auto resource = resource_of(dst_bucket, dst_key);

    std::vector<std::pair<std::string, std::string>> extra = meta_headers(meta);
    extra.emplace_back("x-amz-copy-source",
                       "/" + util::aws_uri_encode(src_rb, /*encode_slash=*/false) +
                           std::string(key_path(src_key)));
    extra.emplace_back("x-amz-metadata-directive", "REPLACE");
    extra = co_await trace_extra(std::move(extra));

    // Server-side COPY is an idempotent PUT: transport/5xx retries are safe, and the
    // 200-with-error-body trap is resolved after the loop like complete's (§4.4)
    auto res = co_await retry_io("copy", [&](httplib::Client& c) {
        auto headers = ctx_->signed_headers("PUT", path, "", extra,
                                            util::sha256_hex(""), tgt.host);
        return c.Put(path, headers, "", meta.content_type.empty()
                                            ? "application/octet-stream"
                                            : meta.content_type.c_str());
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource);
    // COPY shares complete's trap: a slow copy returns 200 first with the error in the body (§4.4)
    s3::XmlNode root;
    try {
        root = s3::xml_parse(res->body);
    } catch (...) {
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote returned unparsable CopyObjectResult body");
    }
    if (root.name == "Error") {
        auto code = map_remote_code(root.get("Code"));
        throw S3Error(code.value_or(S3ErrorCode::InternalError), root.get("Message"), resource);
    }
    co_return PutResult{std::string(strip_etag_quotes(root.get("ETag")))};
}

Task<PutResult> CloudProxyBackend::put_object(std::string_view bucket, std::string_view key,
                                              ObjectMeta meta, http::BodyReader& body,
                                              PutCondition cond) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    // Conditional writes pass through to the upstream (S3 conditional writes): only the
    // party holding the object can guarantee check-and-commit atomicity; no head+put
    // combination on the proxy side can. An upstream without support rejects with 4xx/501,
    // mapped verbatim by throw_remote_error; 412 -> PreconditionFailed
    auto extra = meta_headers(meta);
    if (cond.if_none_match) extra.emplace_back("If-None-Match", "*");
    if (cond.if_match_etag) extra.emplace_back("If-Match", "\"" + *cond.if_match_etag + "\"");
    co_return co_await stream_upload(t.object_path(key_path(key)), "", t.host,
                                     meta.content_type, std::move(extra), body,
                                     resource_of(bucket, key), /*multipart_ctx=*/false);
}

Task<ObjectMeta> CloudProxyBackend::head_object(std::string_view bucket,
                                                std::string_view key) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto tp = co_await trace_extra({{"x-amz-checksum-mode", "ENABLED"}});  // §2.2
    auto res = co_await retry_io("head", [&](httplib::Client& c) {
        return c.Head(path, ctx_->signed_headers("HEAD", path, "", tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status == 200) co_return meta_from_response(key, *res);
    // HEAD has no error body: a 404 gets NoSuchKey filled in from context
    // (docs/cloudproxy-backend.md §4.1/§5.1)
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource_of(bucket, key));
}

Task<std::optional<IStorageBackend::ObjectPartExtent>> CloudProxyBackend::resolve_object_part(
    std::string_view bucket, std::string_view key, int part_no) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    std::string query = "partNumber=" + std::to_string(part_no);
    std::string full = path + "?" + query;
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("head_part", [&](httplib::Client& c) {
        return c.Head(full, ctx_->signed_headers("HEAD", path, query, tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status == 416)
        throw S3Error(S3ErrorCode::InvalidPartNumber,
                      "The requested partnumber is not satisfiable", std::string(key));
    if (res->status != 200 && res->status != 206)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key,
                                 resource_of(bucket, key));
    int count = 1;
    if (res->has_header("x-amz-mp-parts-count")) {
        try {
            count = std::stoi(res->get_header_value("x-amz-mp-parts-count"));
        } catch (...) {
        }
    }
    ObjectPartExtent pe;
    pe.parts_count = count;
    if (res->status == 206) {
        std::string cr = res->get_header_value("Content-Range");
        unsigned long long a = 0, b = 0, total = 0;
        if (sscanf(cr.c_str(), "bytes %llu-%llu/%llu", &a, &b, &total) != 3)
            throw S3Error(S3ErrorCode::InternalError,
                          "cloudproxy: remote part HEAD has unparsable Content-Range: " + cr);
        pe.offset = a;
        pe.size = b - a + 1;
    } else {
        pe.offset = 0;
        pe.size = require_content_length(*res);
    }
    co_return pe;
}

Task<void> CloudProxyBackend::set_object_tagging(std::string_view bucket,
                                                 std::string_view key, std::string tagging) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    std::string query = "tagging";
    std::string full = path + "?" + query;
    if (tagging.empty()) {  // DeleteObjectTagging upstream
        auto tp = co_await trace_extra();
        auto res = co_await retry_io("delete_tagging", [&](httplib::Client& c) {
            return c.Delete(full,
                            ctx_->signed_headers("DELETE", path, query, tp, "", t.host));
        });
        if (!res) ctx_->throw_transport_error(res.error());
        if (res->status / 100 == 2) co_return;
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key,
                                 resource_of(bucket, key));
    }
    // Rebuild the Tagging XML from the canonical encoded form
    s3::XmlWriter w;
    w.open("Tagging");
    w.open("TagSet");
    size_t pos = 0;
    while (pos < tagging.size()) {
        size_t amp = tagging.find('&', pos);
        if (amp == std::string::npos) amp = tagging.size();
        std::string item = tagging.substr(pos, amp - pos);
        auto eq = item.find('=');
        w.open("Tag");
        w.element("Key", util::percent_decode(eq == std::string::npos ? item
                                                                      : item.substr(0, eq)));
        w.element("Value",
                  eq == std::string::npos ? "" : util::percent_decode(item.substr(eq + 1)));
        w.close();
        pos = amp + 1;
    }
    w.close();
    w.close();
    const std::string body = w.str();
    const std::string body_hash = util::sha256_hex(body);
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("put_tagging", [&](httplib::Client& c) {
        return c.Put(full,
                     ctx_->signed_headers("PUT", path, query, tp, body_hash, t.host),
                     body, "application/xml");
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource_of(bucket, key));
}

Task<void> CloudProxyBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("delete", [&](httplib::Client& c) {
        return c.Delete(path, ctx_->signed_headers("DELETE", path, "", tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    // Both 204 and 404 count as success (S3 idempotent delete semantics)
    if (res->status / 100 == 2 || res->status == 404) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource_of(bucket, key));
}

// ---------- list (docs/cloudproxy-backend.md §4.2: always paginate with start-after) ----------

Task<ListResult> CloudProxyBackend::list_objects(std::string_view bucket,
                                                 const ListOptions& opt) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    std::string query = "list-type=2&max-keys=" + std::to_string(opt.max_keys);
    if (!opt.prefix.empty()) query += "&prefix=" + qv(opt.prefix);
    if (!opt.delimiter.empty()) query += "&delimiter=" + qv(opt.delimiter);
    if (!opt.start_after.empty()) query += "&start-after=" + qv(opt.start_after);
    std::string full = path + "?" + query;

    auto tp = co_await trace_extra();
    auto res = co_await retry_io("list", [&](httplib::Client& c) {
        return c.Get(full, ctx_->signed_headers("GET", path, query, tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));

    ListResult out;
    auto root = s3::xml_parse(res->body);
    std::string last_key, last_prefix;
    for (auto& child : root.children) {
        if (child.name == "Contents") {
            ObjectMeta m;
            m.key = child.get("Key");
            m.etag = std::string(strip_etag_quotes(child.get("ETag")));
            m.size = parse_u64(child.get("Size"));
            if (auto t = util::parse_iso8601(child.get("LastModified"))) m.last_modified = *t;
            last_key = m.key;
            out.objects.push_back(std::move(m));
        } else if (child.name == "CommonPrefixes") {
            last_prefix = child.get("Prefix");
            out.common_prefixes.push_back(last_prefix);
        }
    }
    out.is_truncated = root.get("IsTruncated") == "true";
    if (out.is_truncated) {
        // token = the last element of this page; for a group (common prefix), use its
        // lexicographic upper bound to skip the whole group
        if (last_prefix > last_key)
            out.next_token = group_skip_token(last_prefix);
        else
            out.next_token = last_key;
    }
    co_return out;
}

// ---------- multipart passthrough (docs/cloudproxy-backend.md §4.4) ----------

Task<std::string> CloudProxyBackend::create_multipart(std::string_view bucket,
                                                      std::string_view key, ObjectMeta meta) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    std::string query = "uploads";
    std::string full = path + "?" + query;
    auto extra = meta_headers(meta);
    if (!meta.checksum_algorithm.empty())
        extra.emplace_back("x-amz-checksum-algorithm", meta.checksum_algorithm);
    // Note: create retries may leave empty orphan uploads on the remote (a known §5.2
    // trade-off, standard industry practice) -- recommend configuring an
    // AbortIncompleteMultipartUpload lifecycle rule on the remote account
    extra = co_await trace_extra(std::move(extra));
    auto res = co_await retry_io("create_multipart", [&](httplib::Client& c) {
        return c.Post(full, ctx_->signed_headers("POST", path, query, extra, "", t.host),
                      "", meta.content_type);
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket,
                                 resource_of(bucket, key));
    auto root = s3::xml_parse(res->body);
    std::string id = root.get("UploadId");
    if (root.name != "InitiateMultipartUploadResult" || id.empty())
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote returned unexpected CreateMultipartUpload body");
    co_return id;
}

Task<PutResult> CloudProxyBackend::upload_part(std::string_view bucket, std::string_view key,
                                               std::string_view upload_id, int part_no,
                                               http::BodyReader& body,
                                               const std::optional<PartChecksum>& checksum) {
    validate_object_key(key);
    validate_part_number(part_no);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    std::string query =
        "partNumber=" + std::to_string(part_no) + "&uploadId=" + qv(upload_id);
    // Header-form part checksum forwarded so the remote verifies/stores it too (§2.2);
    // trailer-form values arrive after the headers left — this gateway verified them,
    // the remote just is not told (documented limitation)
    std::vector<std::pair<std::string, std::string>> extra;
    if (checksum && !checksum->value.empty()) {
        std::string h = "x-amz-checksum-";
        for (char c : checksum->algorithm) h.push_back(http::HeaderMap::lower(c));
        extra.emplace_back(std::move(h), checksum->value);
    }
    co_return co_await stream_upload(t.object_path(key_path(key)), query, t.host, "",
                                     std::move(extra), body, resource_of(bucket, key),
                                     /*multipart_ctx=*/true);
}

Task<PutResult> CloudProxyBackend::complete_multipart(std::string_view bucket,
                                                      std::string_view key,
                                                      std::string_view upload_id,
                                                      std::span<const PartInfo> parts) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto tgt = ctx_->target(rb);
    auto path = tgt.object_path(key_path(key));
    auto resource = resource_of(bucket, key);
    std::string query = "uploadId=" + qv(upload_id);
    std::string full = path + "?" + query;

    s3::XmlWriter w;
    w.open("CompleteMultipartUpload");
    for (auto& p : parts) {
        w.open("Part");
        w.element("PartNumber", static_cast<uint64_t>(p.part_no));
        w.element("ETag", "\"" + std::string(strip_etag_quotes(p.etag)) + "\"");
        // Client-declared part checksum forwarded verbatim; the remote re-validates (§2.2)
        if (!p.checksum_algorithm.empty() && !p.checksum_value.empty()) {
            std::string tag = "Checksum";
            for (char c : p.checksum_algorithm)
                tag.push_back(c);  // wire names are already uppercase
            w.element(tag, p.checksum_value);
        }
        w.close();
    }
    w.close();
    const std::string body = w.str();
    const std::string body_hash = util::sha256_hex(body);

    // The retry loop lives at coroutine level (roadmap §3.3): each POST is one blocking
    // attempt inside control_io, backoff goes through the TimerQueue instead of sleeping
    // on a pool/private thread, gated by the breaker and the per-op deadline. Ambiguity
    // resolution (co_await head_object) already lived on the coroutine side
    struct CompleteOutcome {
        std::string etag;
        std::string checksum_algorithm, checksum_value, checksum_type;
        std::exception_ptr ambiguous;
    };
    auto op_hist = ctx_->metrics.op_seconds("complete_multipart");
    const auto deadline = ctx_->op_deadline();
    CompleteOutcome outcome;
    auto tp = co_await trace_extra();
    for (int attempt = 0;; ++attempt) {
        ctx_->breaker_gate();
        httplib::Result res = co_await [&]() -> Task<httplib::Result> {
            auto lease = co_await ctx_->pool.acquire_async();
            co_return co_await control_io([&] {
                return ctx_->attempt(op_hist, lease.client(), [&](httplib::Client& c) {
                    return c.Post(full,
                                  ctx_->signed_headers("POST", path, query, tp, body_hash,
                                                       tgt.host),
                                  body, "application/xml");
                });
            });
        }();
        ctx_->breaker_observe(res);
        bool retry = !res ? RemoteContext::retryable_transport(res.error())
                          : ctx_->retryable_status(res->status);
        // S3 quirk: a long-running complete returns 200 first with the error in the
        // body (§4.4). InternalError/SlowDown in the body are the same thing as their
        // namesake HTTP statuses and equally worth retrying -- not retrying turns
        // straight into a 500 for the client, whose retry then goes through the
        // NoSuchUpload ambiguity resolution, wasting a round trip. A post-retry
        // NoSuchUpload is resolved by the retried branch below
        if (!retry && res && res->status == 200 &&
            res->body.find("<Error") != std::string::npos) {
            try {
                auto root = s3::xml_parse(res->body);
                if (root.name == "Error") {
                    auto code = map_remote_code(root.get("Code"));
                    retry = code == S3ErrorCode::InternalError ||
                            code == S3ErrorCode::SlowDown;
                }
            } catch (...) {  // unparsable: leave it to the unified handling below
            }
        }
        if (retry && attempt < ctx_->cfg.retry_max) {
            auto delay = ctx_->backoff_delay_ms(attempt, RemoteContext::retry_after_hint(res));
            if (RemoteContext::deadline_allows(deadline, delay)) {
                ctx_->metrics.count_retry("complete_multipart");
                co_await async_backoff(delay);
                continue;
            }
        }
        const bool retried = attempt > 0;
        if (!res) ctx_->throw_transport_error(res.error());
        try {
            if (res->status != 200)
                ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload, resource);
            // S3 quirk: a long-running complete returns 200 first with the error in the
            // body (docs/cloudproxy-backend.md §4.4)
            s3::XmlNode root;
            try {
                root = s3::xml_parse(res->body);
            } catch (...) {
                throw S3Error(S3ErrorCode::InternalError,
                              "cloudproxy: remote returned unparsable "
                              "CompleteMultipartUpload body");
            }
            if (root.name == "Error") {
                auto code = map_remote_code(root.get("Code"));
                throw S3Error(code.value_or(S3ErrorCode::InternalError),
                              root.get("Message"), resource);
            }
            outcome.etag = std::string(strip_etag_quotes(root.get("ETag")));
            for (std::string_view a : {"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"}) {
                std::string v = root.get("Checksum" + std::string(a));
                if (!v.empty()) {
                    outcome.checksum_algorithm = std::string(a);
                    outcome.checksum_value = std::move(v);
                    outcome.checksum_type = root.get("ChecksumType");
                    break;
                }
            }
        } catch (const S3Error& e) {
            // NoSuchUpload after a retry: the previous attempt may have actually
            // succeeded -> verify with HEAD (docs/cloudproxy-backend.md §5.2)
            if (e.code == S3ErrorCode::NoSuchUpload && retried) {
                outcome.ambiguous = std::current_exception();
                break;
            }
            throw;
        }
        break;
    }
    std::string etag_out = std::move(outcome.etag);
    if (outcome.ambiguous) {
        std::string expect = expected_total_etag(parts);
        bool completed_before = false;
        if (!expect.empty()) {
            try {
                auto m = co_await head_object(bucket, key);
                completed_before = m.etag == expect;
            } catch (...) {
                completed_before = false;
            }
        }
        if (!completed_before) std::rethrow_exception(outcome.ambiguous);
        etag_out = expect;
    }
    co_return PutResult{etag_out, outcome.checksum_algorithm, outcome.checksum_value,
                        outcome.checksum_type};
}

Task<void> CloudProxyBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                              std::string_view upload_id) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    std::string query = "uploadId=" + qv(upload_id);
    std::string full = path + "?" + query;
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("abort_multipart", [&](httplib::Client& c) {
        return c.Delete(full, ctx_->signed_headers("DELETE", path, query, tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload,
                             resource_of(bucket, key));
}

// Now that the contract carries pagination fields, this changed from "accumulate all pages
// then return" to forwarding a single page (docs/archive/gaps.md §5.1): the client's marker becomes
// the remote's marker directly, and the remote's IsTruncated is passed back verbatim.
// Previously the bare-vector contract forced pulling every remote page, so a client wanting
// just the first page still waited for everything
Task<ListPartsResult> CloudProxyBackend::list_parts(std::string_view bucket,
                                                    std::string_view key,
                                                    std::string_view upload_id,
                                                    const ListPartsOptions& opt) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto resource = resource_of(bucket, key);
    ListPartsResult out;
    if (opt.max_parts <= 0) co_return out;

    std::string query = "uploadId=" + qv(upload_id) +
                        "&max-parts=" + std::to_string(opt.max_parts);
    if (opt.part_number_marker > 0)
        query += "&part-number-marker=" + std::to_string(opt.part_number_marker);
    std::string full = path + "?" + query;
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("list_parts", [&](httplib::Client& c) {
        return c.Get(full, ctx_->signed_headers("GET", path, query, tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload, resource);
    auto root = s3::xml_parse(res->body);
    for (auto& child : root.children) {
        if (child.name != "Part") continue;
        PartMeta p;
        p.part_no = static_cast<int>(parse_u64(child.get("PartNumber")));
        p.size = parse_u64(child.get("Size"));
        p.etag = std::string(strip_etag_quotes(child.get("ETag")));
        if (auto ts = util::parse_iso8601(child.get("LastModified"))) p.last_modified = *ts;
        out.parts.push_back(std::move(p));
    }
    out.is_truncated = root.get("IsTruncated") == "true";
    if (out.is_truncated) {
        out.next_part_number_marker =
            static_cast<int>(parse_u64(root.get("NextPartNumberMarker")));
        // Remote truncated but gave no cursor: continuation has nothing to go on; better to
        // report honestly to the end than let the client loop forever
        if (out.next_part_number_marker == 0 && !out.parts.empty())
            out.next_part_number_marker = out.parts.back().part_no;
    }
    co_return out;
}

Task<ListUploadsResult> CloudProxyBackend::list_multipart_uploads(
    std::string_view bucket, const ListUploadsOptions& opt) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto resource = resource_of(bucket);
    ListUploadsResult out;
    if (opt.max_uploads <= 0) co_return out;

    std::string query = "uploads&max-uploads=" + std::to_string(opt.max_uploads);
    if (!opt.prefix.empty()) query += "&prefix=" + qv(opt.prefix);
    if (!opt.delimiter.empty()) query += "&delimiter=" + qv(opt.delimiter);
    if (!opt.key_marker.empty())
        query += "&key-marker=" + qv(opt.key_marker) +
                 "&upload-id-marker=" + qv(opt.upload_id_marker);
    std::string full = path + "?" + query;
    auto tp = co_await trace_extra();
    auto res = co_await retry_io("list_uploads", [&](httplib::Client& c) {
        return c.Get(full, ctx_->signed_headers("GET", path, query, tp, "", t.host));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource);
    auto root = s3::xml_parse(res->body);
    for (auto& child : root.children) {
        if (child.name == "Upload") {
            UploadInfo u;
            u.key = child.get("Key");
            u.upload_id = child.get("UploadId");
            if (auto ts = util::parse_iso8601(child.get("Initiated"))) u.initiated = *ts;
            out.uploads.push_back(std::move(u));
        } else if (child.name == "CommonPrefixes") {
            out.common_prefixes.push_back(child.get("Prefix"));
        }
    }
    out.is_truncated = root.get("IsTruncated") == "true";
    if (out.is_truncated) {
        out.next_key_marker = root.get("NextKeyMarker");
        out.next_upload_id_marker = root.get("NextUploadIdMarker");
        // As above: if the remote gives no cursor, do not pass truncated along, or the
        // client spins in place
        if (out.next_key_marker.empty() && !out.uploads.empty()) {
            out.next_key_marker = out.uploads.back().key;
            out.next_upload_id_marker = out.uploads.back().upload_id;
        } else if (out.next_key_marker.empty()) {
            out.is_truncated = false;
        }
    }
    co_return out;
}

}  // namespace lights3::storage
