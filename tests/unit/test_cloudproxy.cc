// cloudproxy 单测（docs/09 §10）：in-process 双栈自举，不 mock httplib——
// 测试内起 lights3 自己的 HTTP server + S3Service + MemoryBackend 当"远端"，
// CloudProxyBackend 指向它跑一致性套件；同时覆盖自签 sign() 与本地 verify()
// 的互操作。专项用例用裸 handler server 构造错误映射/重试/取消/校验路径。
#ifdef LIGHTS3_CLOUDPROXY

#include <atomic>
#include <thread>

#include "core/thread_pool.h"
#include "http/server.h"
#include "s3/service.h"
#include "storage/cloudproxy/cloudproxy_backend.h"
#include "storage/memory/memory_backend.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::storage;
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

// 任意 handler 的 in-process HTTP server
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

// "远端"完整栈：S3Service + MemoryBackend + 静态凭证
struct RemoteStack {
    std::shared_ptr<MemoryBackend> mem = std::make_shared<MemoryBackend>();
    s3::S3Service svc;
    HandlerServer server;

    RemoteStack()
        : svc(make_router(mem), s3::SigV4Authenticator::build(auth_cfg())),
          server([this](http::HttpRequest req) { return svc.dispatch(std::move(req)); }) {}

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

// 裸 handler 用的 cloudproxy 配置
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

// 一致性套件 + sign()/verify() 互操作（远端全程验签）
TEST(cloudproxy_backend_suite) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    run_backend_suite(b);
}

// bucket_prefix 映射与 list_buckets 过滤（docs/09 §4.2/§4.3）
TEST(cloudproxy_bucket_prefix_mapping) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg("px-"), pool);

    sync_wait(b.create_bucket("mapped"));
    // 远端真实 bucket 名带前缀
    CHECK(sync_wait(remote.mem->bucket_exists("px-mapped")));
    // 远端账号下的无关 bucket 不出现在代理视图
    sync_wait(remote.mem->create_bucket("unrelated"));
    auto buckets = sync_wait(b.list_buckets());
    CHECK_EQ(buckets.size(), size_t(1));
    CHECK_EQ(buckets[0].name, "mapped");

    // 前缀 + 本地名超 63 字节：请求期拒绝（"px-" + 61 = 64）
    CHECK_THROWS_S3(sync_wait(b.create_bucket(std::string(61, 'a'))),
                    s3::S3ErrorCode::InvalidBucketName);
    sync_wait(b.delete_bucket("mapped"));
}

// 错误映射矩阵（docs/09 §5.1）
TEST(cloudproxy_error_mapping) {
    using s3::S3ErrorCode;
    std::atomic<int> mode{0};
    HandlerServer remote([&](http::HttpRequest req) -> Task<http::HttpResponse> {
        // 排空 body，避免驱动层断连噪音
        if (req.body) {
            std::byte buf[4096];
            while (co_await req.body->read(std::span(buf)) > 0) {}
        }
        switch (mode.load()) {
            case 0: co_return xml_error(403, "AccessDenied");
            case 1: co_return xml_error(503, "SlowDown");
            case 2: {
                http::HttpResponse r;  // 404 且体不可解析
                r.status = 404;
                co_return r;
            }
            case 3: co_return xml_error(500, "InternalError");
            default: co_return xml_error(400, "InvalidPart");
        }
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);

    // 远端 403 是网关凭证故障 → InternalError，不透传 AccessDenied
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::InternalError);
    mode = 1;
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::SlowDown);
    mode = 2;  // 404 无体：按操作上下文补语义
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::NoSuchKey);
    CHECK(!sync_wait(b.bucket_exists("bkt")));
    mode = 3;
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::InternalError);
    mode = 4;  // 4xx 可解析 → wire code 原样透传
    CHECK_THROWS_S3(sync_wait(b.get_object("bkt", "k", std::nullopt)),
                    S3ErrorCode::InvalidPart);
}

// bucket_exists 的 HEAD 403 例外：视为存在（docs/09 §4.3）
TEST(cloudproxy_head_bucket_403_means_exists) {
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        co_return xml_error(403, "AccessDenied");
    });
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(cfg_for(remote.port), pool);
    CHECK(sync_wait(b.bucket_exists("bkt")));
}

// 幂等请求对 5xx 的指数退避重试（docs/09 §5.2）
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
    CHECK_EQ(hits.load(), 3);  // 2 次 503 + 1 次成功

    // 重试耗尽 → 映射为 SlowDown
    HandlerServer always503([&](http::HttpRequest) -> Task<http::HttpResponse> {
        co_return xml_error(503, "SlowDown");
    });
    CloudProxyBackend b2(cfg_for(always503.port, /*retry_max=*/1), pool);
    CHECK_THROWS_S3(sync_wait(b2.head_object("bkt", "k")), s3::S3ErrorCode::SlowDown);
}

// 远端不可达：重试耗尽后 InternalError 而非挂死（docs/09 §9.6）
TEST(cloudproxy_unreachable_endpoint) {
    auto pool = std::make_shared<ThreadPool>(2);
    // 端口 1：几乎必然连接拒绝
    CloudProxyConfig c = cfg_for(1, /*retry_max=*/1);
    c.connect_timeout_ms = 300;
    CloudProxyBackend b(std::move(c), pool);
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), s3::S3ErrorCode::InternalError);
}

// GET 中途取消：reader 提前析构 → 远端流被中止，连接不腐化（docs/09 §3.1）
TEST(cloudproxy_get_cancel_mid_stream) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = remote.proxy_cfg();
    cfg.queue_cap_bytes = 64 * 1024;  // 小队列，保证 pump 阻塞在 push
    CloudProxyBackend b(cfg, pool);

    sync_wait(b.create_bucket("big"));
    std::string data(4 * 1024 * 1024, 'x');
    backend_suite::put(b, "big", "blob", data);

    {
        auto got = sync_wait(b.get_object("big", "blob", std::nullopt));
        std::byte buf[8192];
        CHECK(sync_wait(got.body->read(std::span(buf))) > 0);
        // 只读一点即丢弃 —— 析构应 cancel 队列并 join pump，不挂死
    }
    // 后端仍可用
    auto meta = sync_wait(b.head_object("big", "blob"));
    CHECK_EQ(meta.size, uint64_t(data.size()));
    auto again = sync_wait(b.get_object("big", "blob", std::nullopt));
    CHECK_EQ(read_all(*again.body).size(), data.size());
    sync_wait(b.delete_object("big", "blob"));
    sync_wait(b.delete_bucket("big"));
}

// ETag 端到端校验：远端回错误 ETag → InternalError（docs/09 §6）
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

    // verify_etag=false 时放行（远端 SSE 场景）
    auto cfg = cfg_for(remote.port);
    cfg.verify_etag = false;
    CloudProxyBackend b2(std::move(cfg), pool);
    http::StringBodyReader body2("payload");
    auto r = sync_wait(b2.put_object("bkt", "k", {}, body2));
    CHECK_EQ(r.etag, "00000000000000000000000000000000");
}

// S3 特有的"200 OK 但 body 是 <Error>"（docs/09 §4.4 complete 的著名坑）
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

// Range 三形态透传 + 远端忽略 Range 回 200 的降级（docs/09 §3.3）
TEST(cloudproxy_range_forms) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    sync_wait(b.create_bucket("rng"));
    backend_suite::put(b, "rng", "k", "0123456789");

    auto r1 = sync_wait(b.get_object("rng", "k", ByteRange{2, 5}));
    CHECK_EQ(read_all(*r1.body), "2345");
    CHECK(r1.range.has_value());
    CHECK_EQ(r1.meta.size, uint64_t(10));  // size 恒为对象全长
    CHECK_EQ(*r1.range->first, uint64_t(2));
    CHECK_EQ(*r1.range->last, uint64_t(5));

    // 非标远端忽略 Range 回 200 → 按全量处理，range 置空
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

// 分页边界：组尾 token 不得吞掉与"前缀上界"同名的字面 key（docs/09 §4.2）
TEST(cloudproxy_list_pagination_boundary_key) {
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(4);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    sync_wait(b.create_bucket("pgb"));
    backend_suite::put(b, "pgb", "a/1", "x");
    backend_suite::put(b, "pgb", "a/2", "y");
    backend_suite::put(b, "pgb", "a0", "z");  // "a0" == "a/" 末字符 +1，曾被跳过

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

// 不合契约的远端响应必须报错，不得静默截断（docs/09 §3.3 / backend.h size 契约）
TEST(cloudproxy_rejects_nonconforming_remote_responses) {
    std::atomic<int> mode{0};
    HandlerServer remote([&](http::HttpRequest) -> Task<http::HttpResponse> {
        http::HttpResponse r;
        if (mode.load() == 0) {
            // RFC 合法但总长未知的 206：不可按全量语义处理
            r.status = 206;
            r.headers.set("Content-Range", "bytes 0-4/*");
            r.small_body = "01234";
        } else {
            // 200 chunked 无 Content-Length：对象全长不可知
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

// 配置加载期校验：前缀位置规则 / 数值范围 / queue_cap 解析（docs/09 §4.3/§7）
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
    expect_reject({{"bucket_prefix", "-stage-"}});    // 拼接后首字符非法
    expect_reject({{"bucket_prefix", "a..b-"}});      // 拼接后含 ".."
    expect_reject({{"retry_base_ms", "0"}});
    expect_reject({{"retry_base_ms", "-5"}});
    expect_reject({{"retry_max", "100"}});
    expect_reject({{"queue_cap", "1KiB"}});           // 低于下限
    expect_reject({{"max_connections", "0"}});

    auto ok = CloudProxyConfig::from_params(
        "t", {{"endpoint", "http://127.0.0.1:1"}, {"queue_cap", "64KiB"},
              {"bucket_prefix", "px-"}});
    CHECK_EQ(ok.queue_cap_bytes, size_t(64 * 1024));
}

// 无长度 body（真 chunked）首期拒绝为 NotImplemented（docs/09 §3.2）
TEST(cloudproxy_chunked_upload_not_implemented) {
    struct NoLenReader final : http::BodyReader {
        Task<size_t> read(std::span<std::byte>) override { co_return 0; }
        std::optional<uint64_t> length() const override { return std::nullopt; }
    };
    RemoteStack remote;
    auto pool = std::make_shared<ThreadPool>(2);
    CloudProxyBackend b(remote.proxy_cfg(), pool);
    NoLenReader body;
    CHECK_THROWS_S3(sync_wait(b.put_object("bkt", "k", {}, body)),
                    s3::S3ErrorCode::NotImplemented);
}

#endif  // LIGHTS3_CLOUDPROXY
