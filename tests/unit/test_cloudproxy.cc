// cloudproxy unit tests (docs/cloudproxy-backend.md §10): in-process dual-stack bootstrap, no httplib mocking --
// the test starts lights3's own HTTP server + S3Service + MemoryBackend as the "remote",
// and points CloudProxyBackend at it to run the conformance suite; also covers interop between our own
// sign() and local verify(). Dedicated cases use a bare handler server to construct error-mapping/retry/cancel/validation paths.
#ifdef LIGHTS3_CLOUDPROXY

#include <atomic>
#include <thread>

#include "core/thread_pool.h"
#include "http/server.h"
#include "s3/service.h"
#include "core/util/time.h"
#include "storage/cloudproxy/cloudproxy_backend.h"
#include "storage/cloudproxy/remote_client.h"
#include "storage/memory/memory_backend.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::storage;
using backend_suite::put;
using backend_suite::read_all;
using backend_suite::run_backend_suite;

namespace {

constexpr const char* kAk = "CLOUDPROXYTESTAK";
constexpr const char* kSk = "cloudproxy-test-sk";

std::string pick_driver() {
    auto ds = http::HttpServerFactory::drivers();
    for (auto* want : {"builtin", "httplib", "beast"})
        for (auto& d : ds)
            if (d == want) return d;
    return ds.front();
}

// In-process HTTP server for an arbitrary handler
struct HandlerServer {
    std::unique_ptr<http::IHttpServer> srv;
    std::thread th;
    uint16_t port = 0;

    explicit HandlerServer(http::Handler h) {
        HttpConfig cfg;
        cfg.driver = pick_driver();
        cfg.io_threads = 4;
        cfg.idle_timeout_sec = 5;
        srv = http::HttpServerFactory::create(cfg.driver, cfg);
        srv->set_handler(std::move(h));
        srv->listen("127.0.0.1", 0);
        port = srv->bound_port();
        th = std::thread([this] { srv->run(); });
    }
    ~HandlerServer() {
        srv->shutdown();
        th.join();
    }
};

// The full "remote" stack: S3Service + MemoryBackend + static credentials; with a non-empty base_domain the
// remote accepts virtual-hosted addressing (Host: <bucket>.<base_domain>)
struct RemoteStack {
    std::shared_ptr<MemoryBackend> mem = std::make_shared<MemoryBackend>();
    s3::S3Service svc;
    HandlerServer server;

    explicit RemoteStack(std::string base_domain = "")
        : svc(make_router(mem), s3::SigV4Authenticator::build(auth_cfg()),
              std::move(base_domain)),
          server([this](http::HttpRequest req) { return svc.dispatch(std::move(req)); }) {
        // The backend conformance suite uses parts of a few bytes: turn off the remote's 5MiB minimum-part limit,
        // otherwise we would be testing "this implementation's L2 rules" instead of cloudproxy's forwarding behavior
        svc.set_min_part_size(0);
    }

    static AuthConfig auth_cfg() {
        AuthConfig a;
        a.credentials.push_back({kAk, kSk});
        return a;
    }
    static BucketRouter make_router(std::shared_ptr<MemoryBackend> mem) {
        std::map<std::string, std::shared_ptr<IStorageBackend>> backends;
        backends["mem"] = std::move(mem);
        BucketsConfig cfg;
        cfg.default_backend = "mem";
        return BucketRouter::build(cfg, std::move(backends));
    }

    CloudProxyConfig proxy_cfg(std::string prefix = "px-") const {
        CloudProxyConfig c;
        c.endpoint = "http://127.0.0.1:" + std::to_string(server.port);
        c.access_key = kAk;
        c.secret_key = kSk;
        c.bucket_prefix = std::move(prefix);
        c.retry_max = 1;
        c.retry_base_ms = 10;
        return c;
    }
};

// cloudproxy configuration for the bare handler servers
CloudProxyConfig cfg_for(uint16_t port, int retry_max = 0) {
    CloudProxyConfig c;
    c.endpoint = "http://127.0.0.1:" + std::to_string(port);
    c.access_key = kAk;
    c.secret_key = kSk;
    c.retry_max = retry_max;
    c.retry_base_ms = 10;
    return c;
}

http::HttpResponse xml_error(int status, const std::string& code) {
    http::HttpResponse r;
    r.status = status;
    r.headers.set("Content-Type", "application/xml");
    r.small_body = "<Error><Code>" + code + "</Code><Message>scripted</Message></Error>";
    return r;
}

}  // namespace

// Conformance suite + sign()/verify() interop (the remote verifies signatures throughout)
TEST(cloudproxy_backend_suite) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    // checksum round trip skipped: the httplib test double does not store checksums
    run_backend_suite(b, /*checksum_roundtrip=*/false);
}

// bucket_prefix mapping and list_buckets filtering (docs/cloudproxy-backend.md §4.2/§4.3)
TEST(cloudproxy_bucket_prefix_mapping) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg("px-"), pool);

    sync_wait(b.create_bucket("mapped"));
    // The remote's real bucket name carries the prefix
    CHECK(sync_wait(remote.mem->bucket_exists("px-mapped")));
    // Unrelated buckets under the remote account do not appear in the proxy view
    sync_wait(remote.mem->create_bucket("unrelated"));
    auto buckets = sync_wait(b.list_buckets());
    CHECK_EQ(buckets.size(), size_t(1));
    CHECK_EQ(buckets[0].name, "mapped");

    // Prefix + local name over 63 bytes: rejected at request time ("px-" + 61 = 64)
    CHECK_THROWS_S3(sync_wait(b.create_bucket(std::string(61, 'a'))),
                    s3::S3ErrorCode::InvalidBucketName);
    sync_wait(b.delete_bucket("mapped"));
}

// Error-mapping matrix (docs/cloudproxy-backend.md §5.1)
TEST(cloudproxy_error_mapping) {
    using s3::S3ErrorCode;
    std::atomic<int> mode{0};
    HandlerServer remote([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        // Drain the body to avoid driver-level disconnect noise
        if (req.body) {
            std::byte buf[4096];
            while (co_await req.body->read(std::span(buf)) > 0) {}
        }
        switch (mode.load()) {
            case 0: co_return xml_error(403, "AccessDenied");
            case 1: co_return xml_error(503, "SlowDown");
            case 2: {
                http::HttpResponse r;  // 404 with an unparseable body
                r.status = 404;
                co_return r;
            }
            case 3: co_return xml_error(500, "InternalError");
            default: co_return xml_error(400, "InvalidPart");
        }
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);

    // A remote 403 is a gateway credential fault -> InternalError, AccessDenied is not passed through
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::InternalError);
    mode = 1;
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::SlowDown);
    mode = 2;  // 404 without a body: semantics filled in from the operation context
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::NoSuchKey);
    CHECK(!sync_wait(b.bucket_exists("bkt")));
    mode = 3;
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::InternalError);
    mode = 4;  // parseable 4xx -> wire code passed through as-is
    CHECK_THROWS_S3(sync_wait(b.get_object("bkt", "k", std::nullopt)),
                    S3ErrorCode::InvalidPart);
}

// The HEAD 403 exception for bucket_exists: treated as existing (docs/cloudproxy-backend.md §4.3)
TEST(cloudproxy_head_bucket_403_means_exists) {
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        co_return xml_error(403, "AccessDenied");
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);
    CHECK(sync_wait(b.bucket_exists("bkt")));
}

// Exponential-backoff retry of idempotent requests on 5xx (docs/cloudproxy-backend.md §5.2)
TEST(cloudproxy_retry_on_5xx) {
    std::atomic<int> hits{0};
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        if (hits.fetch_add(1) < 2) co_return xml_error(503, "SlowDown");
        http::HttpResponse ok;
        ok.headers.set("Content-Length", "0");
        co_return ok;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port, /*retry_max=*/3), pool);
    CHECK(sync_wait(b.bucket_exists("bkt")));
    CHECK_EQ(hits.load(), 3);  // 2 times 503 + 1 success

    // Retries exhausted -> mapped to SlowDown
    HandlerServer always503([&](http::HttpRequest) -> Task<http::HttpResponse> {
        co_return xml_error(503, "SlowDown");
    });
    CloudProxyBackend b2(cfg_for(always503.port, /*retry_max=*/1), pool);
    CHECK_THROWS_S3(sync_wait(b2.head_object("bkt", "k")), s3::S3ErrorCode::SlowDown);
}

// Remote unreachable: InternalError after retries are exhausted, rather than hanging (docs/cloudproxy-backend.md §9.6)
TEST(cloudproxy_unreachable_endpoint) {
    auto pool = std::make_shared<ThreadPool>(2);
    // Port 1: almost certainly connection refused
    CloudProxyConfig c = cfg_for(1, /*retry_max=*/1);
    c.connect_timeout_ms = 300;
    CloudProxyBackend b(std::move(c), pool);
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), s3::S3ErrorCode::InternalError);
}

// GET cancelled midway: the reader is destroyed early -> the remote stream is aborted, the connection does not rot (docs/cloudproxy-backend.md §3.1)
TEST(cloudproxy_get_cancel_mid_stream) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = remote.proxy_cfg();
    cfg.queue_cap_bytes = 64 * 1024;  // small queue, guaranteeing the pump blocks on push
    CloudProxyBackend b(cfg, pool);

    sync_wait(b.create_bucket("big"));
    std::string data(4 * 1024 * 1024, 'x');
    backend_suite::put(b, "big", "blob", data);

    {
        auto got = sync_wait(b.get_object("big", "blob", std::nullopt));
        std::byte buf[8192];
        CHECK(sync_wait(got.body->read(std::span(buf))) > 0);
        // Read only a little then drop -- destruction should cancel the queue and join the pump, without hanging
    }
    // The backend is still usable
    auto meta = sync_wait(b.head_object("big", "blob"));
    CHECK_EQ(meta.size, uint64_t(data.size()));
    auto again = sync_wait(b.get_object("big", "blob", std::nullopt));
    CHECK_EQ(read_all(*again.body).size(), data.size());
    sync_wait(b.delete_object("big", "blob"));
    sync_wait(b.delete_bucket("big"));
}

// End-to-end ETag verification: the remote returns a wrong ETag -> InternalError (docs/cloudproxy-backend.md §6)
TEST(cloudproxy_etag_verify_failure) {
    HandlerServer remote([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        if (req.body) {
            std::byte buf[4096];
            while (co_await req.body->read(std::span(buf)) > 0) {}
        }
        http::HttpResponse r;
        r.headers.set("ETag", "\"00000000000000000000000000000000\"");
        co_return r;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);
    http::StringBodyReader body("payload");
    CHECK_THROWS_S3(sync_wait(b.put_object("bkt", "k", {}, body)),
                    s3::S3ErrorCode::InternalError);

    // Allowed through when verify_etag=false (remote SSE scenario)
    auto cfg = cfg_for(remote.port);
    cfg.verify_etag = false;
    CloudProxyBackend b2(std::move(cfg), pool);
    http::StringBodyReader body2("payload");
    auto r = sync_wait(b2.put_object("bkt", "k", {}, body2));
    CHECK_EQ(r.etag, "00000000000000000000000000000000");
}

// S3's peculiar "200 OK but the body is <Error>" (the famous complete pitfall, docs/cloudproxy-backend.md §4.4)
TEST(cloudproxy_complete_200_with_error_body) {
    HandlerServer remote([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        if (req.body) {
            std::byte buf[4096];
            while (co_await req.body->read(std::span(buf)) > 0) {}
        }
        co_return xml_error(200, "InvalidPart");
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);
    std::vector<PartInfo> parts{{1, "f814893777bcc2295fff05f00e508da6"}};
    CHECK_THROWS_S3(sync_wait(b.complete_multipart("bkt", "k", "uid", parts)),
                    s3::S3ErrorCode::InvalidPart);
}

// Pass-through of the three Range forms + degradation when the remote ignores Range and returns 200 (docs/cloudproxy-backend.md §3.3)
TEST(cloudproxy_range_forms) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    sync_wait(b.create_bucket("rng"));
    backend_suite::put(b, "rng", "k", "0123456789");

    auto r1 = sync_wait(b.get_object("rng", "k", ByteRange{2, 5}));
    CHECK_EQ(read_all(*r1.body), "2345");
    CHECK(r1.range.has_value());
    CHECK_EQ(r1.meta.size, uint64_t(10));  // size is always the full object length
    CHECK_EQ(*r1.range->first, uint64_t(2));
    CHECK_EQ(*r1.range->last, uint64_t(5));

    // A non-conforming remote ignores Range and returns 200 -> treated as full content, range cleared
    HandlerServer ignore_range([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        (void)req;
        http::HttpResponse r;
        r.small_body = "0123456789";
        r.headers.set("ETag", "\"781e5e245d69b566979b86e28d23f2c7\"");
        co_return r;
    });
    CloudProxyBackend b2(cfg_for(ignore_range.port), pool);
    auto r2 = sync_wait(b2.get_object("rng", "k", ByteRange{2, 5}));
    CHECK(!r2.range.has_value());
    CHECK_EQ(read_all(*r2.body), "0123456789");
}

// Pagination boundary: the group-tail token must not swallow a literal key equal to the "prefix upper bound" (docs/cloudproxy-backend.md §4.2)
TEST(cloudproxy_list_pagination_boundary_key) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    sync_wait(b.create_bucket("pgb"));
    backend_suite::put(b, "pgb", "a/1", "x");
    backend_suite::put(b, "pgb", "a/2", "y");
    backend_suite::put(b, "pgb", "a0", "z");  // "a0" == "a/" with the last character +1, was once skipped

    ListOptions opt;
    opt.delimiter = "/";
    opt.max_keys = 1;
    std::vector<std::string> keys, prefixes;
    for (int guard = 0; guard < 10; ++guard) {
        auto r = sync_wait(b.list_objects("pgb", opt));
        for (auto& o : r.objects) keys.push_back(o.key);
        for (auto& p : r.common_prefixes) prefixes.push_back(p);
        if (!r.is_truncated) break;
        opt.start_after = r.next_token;
    }
    CHECK_EQ(prefixes.size(), size_t(1));
    CHECK_EQ(prefixes[0], "a/");
    CHECK_EQ(keys.size(), size_t(1));
    CHECK_EQ(keys[0], "a0");
}

// Non-conforming remote responses must error, never silently truncate (docs/cloudproxy-backend.md §3.3 / backend.h size contract)
TEST(cloudproxy_rejects_nonconforming_remote_responses) {
    std::atomic<int> mode{0};
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        http::HttpResponse r;
        if (mode.load() == 0) {
            // A 206 that is RFC-legal but with unknown total length: must not be handled with full-content semantics
            r.status = 206;
            r.headers.set("Content-Range", "bytes 0-4/*");
            r.small_body = "01234";
        } else {
            // 200 chunked without Content-Length: the full object length is unknowable
            r.stream_body = std::make_unique<http::StringBodyReader>("payload");
        }
        co_return r;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);
    CHECK_THROWS_S3(sync_wait(b.get_object("bkt", "k", ByteRange{0, 4})),
                    s3::S3ErrorCode::InternalError);
    mode = 1;
    CHECK_THROWS_S3(sync_wait(b.get_object("bkt", "k", std::nullopt)),
                    s3::S3ErrorCode::InternalError);
}

// Config load-time validation: prefix placement rules / numeric ranges / queue_cap parsing (docs/cloudproxy-backend.md §4.3/§7)
TEST(cloudproxy_config_load_validation) {
    auto expect_reject = [](std::map<std::string, std::string> params) {
        params.emplace("endpoint", "http://127.0.0.1:1");
        bool threw = false;
        try {
            CloudProxyConfig::from_params("t", params);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        CHECK(threw);
    };
    expect_reject({{"bucket_prefix", "-stage-"}});    // first character invalid after concatenation
    expect_reject({{"bucket_prefix", "a..b-"}});      // contains ".." after concatenation
    expect_reject({{"retry_base_ms", "0"}});
    expect_reject({{"retry_base_ms", "-5"}});
    expect_reject({{"retry_max", "100"}});
    expect_reject({{"queue_cap", "1KiB"}});           // below the lower bound
    expect_reject({{"max_connections", "0"}});

    auto ok = CloudProxyConfig::from_params(
        "t", {{"endpoint", "http://127.0.0.1:1"}, {"queue_cap", "64KiB"},
              {"bucket_prefix", "px-"}});
    CHECK_EQ(ok.queue_cap_bytes, size_t(64 * 1024));
    CHECK(ok.force_path_style && !ok.control_in_pump);  // defaults

    // P4 remainder (docs/cloudproxy-backend.md §2.3/§7): both keys parse, vhost no longer errors
    auto ok2 = CloudProxyConfig::from_params(
        "t", {{"endpoint", "http://127.0.0.1:1"}, {"force_path_style", "false"},
              {"control_in_pump", "true"}});
    CHECK(!ok2.force_path_style);
    CHECK(ok2.control_in_pump);
    expect_reject({{"control_in_pump", "not-a-bool"}});
}

// virtual-hosted style (docs/cloudproxy-backend.md §7): connections always target the endpoint, only
// Host/signature and path vary by bucket; the remote accepts vhost via base_domain, and passing the full suite =
// addressing/signing/pagination/multipart are all self-consistent under vhost
TEST(cloudproxy_virtual_hosted_style) {
    RemoteStack remote("127.0.0.1");  // remote vhost domain = <bucket>.127.0.0.1
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = remote.proxy_cfg();
    cfg.force_path_style = false;
    CloudProxyBackend b(cfg, pool);
    // checksum round trip skipped: the httplib test double does not store checksums
    run_backend_suite(b, /*checksum_roundtrip=*/false);
}

// control_in_pump=true (docs/cloudproxy-backend.md §2.3): the control plane uses a one-shot private
// thread, semantically identical to the pool-thread path (the full suite passes); then the two modes are compared
// on HEAD latency, printing benchmark numbers (the data source for the default-value argument, no assertions -- local loopback is only an order-of-magnitude reference)
TEST(cloudproxy_control_in_pump_suite_and_bench) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = remote.proxy_cfg();
    cfg.control_in_pump = true;
    CloudProxyBackend b(cfg, pool);
    // checksum round trip skipped: the httplib test double does not store checksums
    run_backend_suite(b, /*checksum_roundtrip=*/false);

    auto bench = [&](bool in_pump) {
        auto c = remote.proxy_cfg("bench-");
        c.control_in_pump = in_pump;
        CloudProxyBackend bb(c, pool);
        sync_wait(bb.create_bucket("bench"));
        backend_suite::put(bb, "bench", "k", "x");
        constexpr int kOps = 300;
        auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < kOps; ++i) sync_wait(bb.head_object("bench", "k"));
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0)
                      .count();
        sync_wait(bb.delete_object("bench", "k"));
        sync_wait(bb.delete_bucket("bench"));
        return double(us) / kOps;
    };
    double pool_us = bench(false), pump_us = bench(true);
    printf("       [bench] control HEAD us/op: pool=%.0f pump=%.0f (loopback)\n", pool_us,
           pump_us);
}

// §8.2 metrics: zero values visible at construction; request latency/error mapping/pool waits recorded (a backend with an empty scope is unaffected)
TEST(cloudproxy_metrics_registered) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    auto reg = std::make_shared<MetricsRegistry>();
    CloudProxyBackend b(remote.proxy_cfg(), pool, MetricsScope(reg, {{"backend", "cp"}}));
    auto text0 = reg->render();
    CHECK(text0.find("lights3_cloudproxy_etag_mismatch_total{backend=\"cp\"} 0") !=
          std::string::npos);
    CHECK(text0.find("lights3_cloudproxy_pool_wait_seconds") != std::string::npos);

    sync_wait(b.create_bucket("bkt"));
    backend_suite::put(b, "bkt", "k", "hello metrics");
    auto got = sync_wait(b.get_object("bkt", "k", std::nullopt));
    CHECK_EQ(read_all(*got.body), "hello metrics");
    got.body.reset();
    sync_wait(b.head_object("bkt", "k"));
    bool threw = false;
    try {
        // GET of a missing key: the remote returns 404 + XML error body -> counted as NoSuchKey by wire code
        // (HEAD has no error body and would only fall into the http_404 bucket)
        sync_wait(b.get_object("bkt", "missing", std::nullopt));
    } catch (const s3::S3Error&) {
        threw = true;
    }
    CHECK(threw);

    auto text = reg->render();
    for (const char* op : {"create_bucket", "put", "get", "head"})
        CHECK(text.find("lights3_cloudproxy_remote_request_seconds_count"
                        "{backend=\"cp\",op=\"" +
                        std::string(op) + "\"}") != std::string::npos);
    CHECK(text.find("lights3_cloudproxy_remote_errors_total{backend=\"cp\",code=\"NoSuchKey\"}"
                    " 1") != std::string::npos);
    CHECK(text.find("lights3_cloudproxy_pool_wait_seconds_count{backend=\"cp\"}") !=
          std::string::npos);
}

// A length-less body (true chunked) uploads via a local spool (docs/archive/gaps.md §6.2): first written to a temp file
// to obtain the length, then goes down the known-length path; spool_max_bytes=0 keeps the old NotImplemented semantics
TEST(cloudproxy_chunked_upload_spools) {
    struct NoLenReader final : http::BodyReader {
        explicit NoLenReader(std::string d) : data_(std::move(d)) {}
        Task<size_t> read(std::span<std::byte> buf) override {
            size_t n = std::min(buf.size(), data_.size() - off_);
            std::memcpy(buf.data(), data_.data() + off_, n);
            off_ += n;
            co_return n;
        }
        std::optional<uint64_t> length() const override { return std::nullopt; }
        std::string data_;
        size_t off_ = 0;
    };
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    sync_wait(b.create_bucket("bkt"));

    std::string data(300 * 1024, 'q');  // spans multiple 64KiB chunks
    for (size_t i = 0; i < data.size(); i += 7) data[i] = char('a' + (i % 26));
    NoLenReader body(data);
    auto pr = sync_wait(b.put_object("bkt", "k", {}, body));
    auto got = sync_wait(b.get_object("bkt", "k", std::nullopt));
    CHECK_EQ(got.meta.size, uint64_t(data.size()));
    std::string back = read_all(*got.body);
    CHECK(back == data);
    (void)pr;

    // Spool cap: a length-less upload over the cap gets EntityTooLarge, nothing lands on the remote
    auto small = remote.proxy_cfg();
    small.spool_max_bytes = 1024;
    CloudProxyBackend b2(small, pool);
    NoLenReader big(std::string(4096, 'x'));
    CHECK_THROWS_S3(sync_wait(b2.put_object("bkt", "k2", {}, big)),
                    s3::S3ErrorCode::EntityTooLarge);

    // Spool disabled: back to NotImplemented
    auto off = remote.proxy_cfg();
    off.spool_max_bytes = 0;
    CloudProxyBackend b3(off, pool);
    NoLenReader nl(std::string("x"));
    CHECK_THROWS_S3(sync_wait(b3.put_object("bkt", "k3", {}, nl)),
                    s3::S3ErrorCode::NotImplemented);
}

// Same-backend server-side COPY (docs/archive/gaps.md §6.2): x-amz-copy-source completes in one remote call,
// the gateway moves no bytes; REPLACE semantics carry our metadata
TEST(cloudproxy_server_side_copy) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    sync_wait(b.create_bucket("bkt"));
    std::string data = "server side copy payload";
    put(b, "bkt", "src.txt", data);

    ObjectMeta meta;
    meta.content_type = "text/plain";
    meta.user_meta["note"] = "copied";
    auto r = sync_wait(b.copy_object_fast("bkt", "src.txt", "bkt", "dst.txt", meta));
    CHECK(r.has_value());
    auto got = sync_wait(b.get_object("bkt", "dst.txt", std::nullopt));
    CHECK_EQ(read_all(*got.body), data);
    CHECK_EQ(got.meta.etag, r->etag);
    CHECK_EQ(got.meta.user_meta.at("note"), std::string("copied"));

    // Source missing -> NoSuchKey mapped as-is
    CHECK_THROWS_S3(sync_wait(b.copy_object_fast("bkt", "absent", "bkt", "d2", {})),
                    s3::S3ErrorCode::NoSuchKey);
}

// ---------- roadmap §3.3: backoff/Retry-After, breaker, deadline, pool hygiene, creds ----------

// A large Retry-After combined with a small op deadline: the retry whose backoff would
// land past the deadline is not taken — exactly one remote hit, and the 503 maps out.
// This also proves the hint is honored (the formula would have retried after ~10ms)
TEST(cloudproxy_retry_after_respected_and_deadline_caps) {
    std::atomic<int> hits{0};
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        ++hits;
        auto r = xml_error(503, "SlowDown");
        r.headers.set("Retry-After", "5");
        co_return r;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    auto cfg = cfg_for(remote.port, /*retry_max=*/3);
    cfg.op_deadline_ms = 300;
    CloudProxyBackend b(cfg, pool);
    auto t0 = std::chrono::steady_clock::now();
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), s3::S3ErrorCode::SlowDown);
    CHECK_EQ(hits.load(), 1);  // no retry: 5s hint > 300ms deadline
    CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(2));
}

// A small Retry-After is actually waited out (async backoff path): one 503 with
// Retry-After: 1, then success — two hits and >= ~1s elapsed
TEST(cloudproxy_retry_after_waits_hint) {
    std::atomic<int> hits{0};
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        if (hits.fetch_add(1) == 0) {
            auto r = xml_error(503, "SlowDown");
            r.headers.set("Retry-After", "1");
            co_return r;
        }
        http::HttpResponse ok;
        ok.headers.set("Content-Length", "0");
        co_return ok;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port, /*retry_max=*/1), pool);
    auto t0 = std::chrono::steady_clock::now();
    CHECK(sync_wait(b.bucket_exists("bkt")));
    CHECK_EQ(hits.load(), 2);
    CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(900));
}

// Circuit breaker: consecutive 5xx opens it (fail fast without touching the remote),
// the cooldown ends with a half-open probe, and success closes it again
TEST(cloudproxy_breaker_opens_and_recovers) {
    std::atomic<int> hits{0};
    std::atomic<bool> healthy{false};
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        ++hits;
        if (!healthy) co_return xml_error(500, "InternalError");
        http::HttpResponse ok;
        ok.headers.set("Content-Length", "0");
        co_return ok;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    auto cfg = cfg_for(remote.port, /*retry_max=*/0);
    cfg.breaker_threshold = 3;
    cfg.breaker_cooldown_ms = 300;
    CloudProxyBackend b(cfg, pool);

    for (int i = 0; i < 3; ++i)
        CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), s3::S3ErrorCode::InternalError);
    CHECK_EQ(hits.load(), 3);
    // Open: shed without a remote round trip, even though the remote is healthy again
    healthy = true;
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), s3::S3ErrorCode::SlowDown);
    CHECK_EQ(hits.load(), 3);
    // Cooldown over: the half-open probe goes through and closes the breaker
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    CHECK(sync_wait(b.bucket_exists("bkt")));
    CHECK_EQ(hits.load(), 4);
    CHECK(sync_wait(b.bucket_exists("bkt")));  // closed: normal traffic
    CHECK_EQ(hits.load(), 5);
}

// Pool hygiene (drives ClientPool directly): idle entries beyond pool_idle_timeout are
// reaped (total_ shrinks — a NAT-dropped socket is never reused), and pool_max_lifetime
// retires aged connections at release
TEST(cloudproxy_pool_idle_reap_and_max_lifetime) {
    cloudproxy::Endpoint ep = cloudproxy::Endpoint::parse("http://127.0.0.1:1");
    {
        CloudProxyConfig cfg = cfg_for(1);
        cfg.pool_idle_timeout_ms = 100;  // reaper interval clamps to 1s
        cloudproxy::ClientPool pool(cfg, ep);
        { auto lease = pool.acquire(); }
        auto st = pool.stats();
        CHECK_EQ(st.total, 1);
        CHECK_EQ(st.idle, size_t(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(1300));
        st = pool.stats();  // background reaper dropped the stale idle
        CHECK_EQ(st.total, 0);
        CHECK_EQ(st.idle, size_t(0));
        { auto lease = pool.acquire(); }  // pool still serves fresh connections
        CHECK_EQ(pool.stats().total, 1);
    }
    {
        CloudProxyConfig cfg = cfg_for(1);
        cfg.pool_max_lifetime_ms = 50;
        cloudproxy::ClientPool pool(cfg, ep);
        { auto lease = pool.acquire(); }          // age ~0: pooled
        CHECK_EQ(pool.stats().idle, size_t(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        { auto lease = pool.acquire(); }          // reused, now past its lifetime
        auto st = pool.stats();                   // -> retired at release
        CHECK_EQ(st.total, 0);
        CHECK_EQ(st.idle, size_t(0));
    }
}

// Async acquire (drives ClientPool directly): at capacity the waiter queues without
// parking a thread in the pool's cv; a release hands the connection over, and the
// request_timeout produces the same SlowDown as the sync path
TEST(cloudproxy_pool_async_acquire_handoff_and_timeout) {
    cloudproxy::Endpoint ep = cloudproxy::Endpoint::parse("http://127.0.0.1:1");
    CloudProxyConfig cfg = cfg_for(1);
    cfg.max_connections = 1;
    cfg.request_timeout_ms = 150;
    cloudproxy::ClientPool pool(cfg, ep);

    {
        auto held = pool.acquire();
        std::thread waiter([&] {
            CHECK_THROWS_S3(sync_wait(pool.acquire_async()), s3::S3ErrorCode::SlowDown);
        });
        waiter.join();  // timed out while the lease was held
    }
    {
        auto held = std::make_optional(pool.acquire());
        std::atomic<bool> got{false};
        std::thread waiter([&] {
            auto lease = sync_wait(pool.acquire_async());
            got = true;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        CHECK(!got.load());
        held.reset();  // release: direct handoff to the waiter
        waiter.join();
        CHECK(got.load());
    }
    CHECK_EQ(pool.stats().total, 1);
}

// Credential chain via a fake IMDS (roadmap §3.3): empty static keys resolve through
// IMDSv2 (token PUT → role → credential doc); an expiry inside the refresh margin
// re-fetches on the next signing. The remote is the full SigV4-verifying stack, so a
// wrong chain would fail the signature, not just the assertion
TEST(cloudproxy_credential_chain_imds) {
    std::atomic<int> token_hits{0};
    HandlerServer imds([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        http::HttpResponse r;
        if (req.method == "PUT" && req.path == "/latest/api/token") {
            ++token_hits;
            r.small_body = "imds-tok";
        } else if (req.path == "/latest/meta-data/iam/security-credentials/") {
            if (req.headers.get("X-aws-ec2-metadata-token").value_or("") != "imds-tok") {
                r.status = 401;
                co_return r;
            }
            r.small_body = "test-role\n";
        } else if (req.path == "/latest/meta-data/iam/security-credentials/test-role") {
            // Expiration 4min out: inside the 5min refresh margin, so every signing
            // refreshes — observable as growing token_hits
            auto exp = std::chrono::system_clock::now() + std::chrono::minutes(4);
            r.small_body = std::string("{\"AccessKeyId\":\"") + kAk +
                           "\",\"SecretAccessKey\":\"" + kSk +
                           "\",\"Token\":\"\",\"Expiration\":\"" +
                           util::iso8601(exp) + "\"}";
        } else {
            r.status = 404;
        }
        co_return r;
    });

    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(2);
    auto cfg = remote.proxy_cfg();
    cfg.access_key.clear();
    cfg.secret_key.clear();
    cfg.imds_endpoint = "http://127.0.0.1:" + std::to_string(imds.port);
    CloudProxyBackend b(cfg, pool);

    sync_wait(b.create_bucket("chain"));
    CHECK(sync_wait(b.bucket_exists("chain")));
    CHECK(token_hits.load() >= 2);  // near-expiry doc forced a refresh per signing
    sync_wait(b.delete_bucket("chain"));
}

// The session token is set before signing and travels as x-amz-security-token
TEST(cloudproxy_credential_chain_session_token_header) {
    HandlerServer imds([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        http::HttpResponse r;
        if (req.method == "PUT" && req.path == "/latest/api/token") {
            r.small_body = "t";
        } else if (req.path == "/latest/meta-data/iam/security-credentials/") {
            r.small_body = "role";
        } else if (req.path == "/latest/meta-data/iam/security-credentials/role") {
            r.small_body = std::string("{\"AccessKeyId\":\"") + kAk +
                           "\",\"SecretAccessKey\":\"" + kSk +
                           "\",\"Token\":\"sess-token\"}";  // no Expiration: cached forever
        } else {
            r.status = 404;
        }
        co_return r;
    });
    std::atomic<int> with_token{0};
    HandlerServer remote([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        if (req.headers.get("x-amz-security-token").value_or("") == "sess-token") ++with_token;
        http::HttpResponse ok;
        ok.headers.set("Content-Length", "0");
        co_return ok;
    });
    auto pool = std::make_shared<ThreadPool>(2);
    auto cfg = cfg_for(remote.port);
    cfg.access_key.clear();
    cfg.secret_key.clear();
    cfg.imds_endpoint = "http://127.0.0.1:" + std::to_string(imds.port);
    CloudProxyBackend b(cfg, pool);
    CHECK(sync_wait(b.bucket_exists("bkt")));
    CHECK_EQ(with_token.load(), 1);
}

#endif  // LIGHTS3_CLOUDPROXY
