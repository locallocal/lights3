// RedisMetaStore dedicated unit tests (docs/duostore-redis-meta.md §9): meta consistency suite,
// backend suite over the injected combination, prefix isolation, NOSCRIPT self-healing, swap_extents CAS, multiple gateways sharing meta,
// concurrent CAS convergence. Obtaining a real redis: probe for redis-server in PATH and start a private instance
// on a unix socket (--save '' --appendonly no); if none is found, SKIP explicitly (not a failure).
// LIGHTS3_TEST_REDIS_URI can override with an external instance (isolation relies on a random per-case key prefix).
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_REDIS_META)

#include <hiredis.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>

#include "core/metrics.h"

#include "core/fault.h"
#include "core/thread_pool.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/redis_meta_store.h"
#include "unit/backend_suite.h"
#include "unit/meta_store_suite.h"
#include "unit/mini_test.h"

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;

namespace {

// Process-level singleton for the private redis-server: lazily started by the first test case, reaped at process exit.
// A unix socket avoids port allocation conflicts; --appendonly no (unit tests do not test crash semantics, the same tradeoff
// as sync=false in the rocks unit tests).
class RedisTestServer {
public:
    static RedisTestServer& instance() {
        static RedisTestServer srv;
        return srv;
    }

    bool available = false;
    // Instance ownership: only the self-started private redis allows server-global operations such as SCRIPT FLUSH /
    // CLIENT KILL; an external instance (LIGHTS3_TEST_REDIS_URI) is by convention isolated only via key prefixes
    bool owned = false;
    std::string uri;

    // Send admin commands directly to the server (SCRIPT FLUSH etc., for test injection)
    bool raw_command(const char* cmd) {
        redisContext* ctx = connect_raw();
        if (!ctx) return false;
        auto* r = static_cast<redisReply*>(redisCommand(ctx, cmd));
        bool ok = r && r->type != REDIS_REPLY_ERROR;
        if (r) freeReplyObject(r);
        redisFree(ctx);
        return ok;
    }

private:
    RedisTestServer() {
        if (const char* env = std::getenv("LIGHTS3_TEST_REDIS_URI"); env && *env) {
            uri = env;
            available = true;
            return;
        }
        char tmpl[] = "/tmp/lights3-redis-test-XXXXXX";
        if (!mkdtemp(tmpl)) return;
        dir_ = tmpl;
        sock_ = dir_ + "/redis.sock";
        pid_ = fork();
        if (pid_ == 0) {
            // Child process: keep logs in the temp dir for troubleshooting; exit if execlp fails (parent side times out and skips)
            std::string logfile = dir_ + "/redis.log";
            if (FILE* f = fopen(logfile.c_str(), "w")) {
                dup2(fileno(f), 1);
                dup2(fileno(f), 2);
            }
            execlp("redis-server", "redis-server", "--port", "0", "--unixsocket",
                   sock_.c_str(), "--save", "", "--appendonly", "no", "--dir", dir_.c_str(),
                   (char*)nullptr);
            _exit(127);
        }
        if (pid_ < 0) return;
        uri = "unix://" + sock_;
        // Readiness polling: usable once PING succeeds (<=5s)
        for (int i = 0; i < 50; ++i) {
            if (redisContext* ctx = connect_raw()) {
                auto* r = static_cast<redisReply*>(redisCommand(ctx, "PING"));
                bool ok = r && r->type == REDIS_REPLY_STATUS;
                if (r) freeReplyObject(r);
                redisFree(ctx);
                if (ok) {
                    available = true;
                    owned = true;
                    return;
                }
            }
            int status = 0;
            if (waitpid(pid_, &status, WNOHANG) == pid_) {  // exec failed / died right at startup
                pid_ = -1;
                break;
            }
            usleep(100 * 1000);
        }
    }

    ~RedisTestServer() {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            waitpid(pid_, nullptr, 0);
        }
        if (!dir_.empty()) {
            std::error_code ec;
            fs::remove_all(dir_, ec);
        }
    }

    redisContext* connect_raw() {
        timeval tv{1, 0};
        redisContext* ctx = nullptr;
        if (uri.rfind("unix://", 0) == 0)
            ctx = redisConnectUnixWithTimeout(uri.c_str() + 7, tv);
        else if (uri.rfind("redis://", 0) == 0) {
            // The override instance only supports the simplest host:port form (for test injection)
            std::string rest = uri.substr(8);
            if (auto slash = rest.find('/'); slash != std::string::npos) rest.resize(slash);
            std::string host = rest;
            int port = 6379;
            if (auto colon = rest.rfind(':'); colon != std::string::npos) {
                host = rest.substr(0, colon);
                port = atoi(rest.c_str() + colon + 1);
            }
            ctx = redisConnectWithTimeout(host.c_str(), port, tv);
        }
        if (ctx && ctx->err) {
            redisFree(ctx);
            return nullptr;
        }
        return ctx;
    }

    std::string dir_, sock_;
    pid_t pid_ = -1;
};

// Independent prefix per test case: multiple suites (and repeated runs against an external override instance) do not pollute each other (§2.1)
std::string unique_prefix() {
    static std::atomic<int> counter{0};
    return "t" + std::to_string(getpid()) + "-" + std::to_string(counter++) + ":";
}

RedisMetaOptions redis_opts(const std::string& prefix) {
    RedisMetaOptions o;
    o.uri = RedisTestServer::instance().uri;
    o.prefix = prefix;
    o.timeout_ms = 3000;
    o.pool_size = 4;
    return o;
}

#define REDIS_OR_SKIP()                                                       \
    if (!RedisTestServer::instance().available) {                             \
        printf("       [SKIP] redis-server not available\n");                \
        return;                                                               \
    }

// Cases that need server-global injection such as SCRIPT FLUSH / CLIENT KILL: skip when pointing at an external shared
// instance -- this neither disrupts others nor lets other connections distort the exact reconnects assertion
#define OWNED_REDIS_OR_SKIP()                                                 \
    REDIS_OR_SKIP();                                                          \
    if (!RedisTestServer::instance().owned) {                                 \
        printf("       [SKIP] external redis: server-wide commands not allowed\n"); \
        return;                                                               \
    }

using backend_suite::put;
using backend_suite::read_all;
using backend_suite::TmpDir;
using meta_store_suite::chunk_extent;
using meta_store_suite::make_rec;

}  // namespace

// Same meta semantics baseline (suite shared with RocksMetaStore, docs/duostore-redis-meta.md §9.1)
TEST(duostore_redis_meta_store_suite) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    meta_store_suite::run_meta_store_suite(
        [&] { return std::make_unique<RedisMetaStore>(redis_opts(prefix)); });
}

// Run the backend consistency suite over the injected combination (RedisMetaStore + FsDataStore) (§9.3)
TEST(duostore_redis_backend_suite) {
    REDIS_OR_SKIP();
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RedisMetaStore>(redis_opts(unique_prefix()));
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "redis-suite";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<FsDataStore>(
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                      cfg.pack_max_size, cfg.pack_writers, {}},
        pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// Prefix isolation (§2.1): two stores share one server yet cannot see each other
TEST(duostore_redis_prefix_isolation) {
    REDIS_OR_SKIP();
    RedisMetaStore a(redis_opts(unique_prefix()));
    RedisMetaStore b(redis_opts(unique_prefix()));
    a.create_bucket("iso");
    CHECK(a.bucket_exists("iso"));
    CHECK(!b.bucket_exists("iso"));
    CHECK_EQ(b.list_buckets().size(), size_t(0));
    a.delete_bucket("iso");
    a.close();
    b.close();
}

// Multiple gateways sharing meta (§3.4): two instances with the same prefix share metadata; segment allocation never collides
TEST(duostore_redis_multi_gateway_shared_meta) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore g1(redis_opts(prefix));
    RedisMetaStore g2(redis_opts(prefix));
    g1.create_bucket("shared");
    CHECK(g2.bucket_exists("shared"));
    g2.put_object("shared", "k", make_rec("k", {}));
    CHECK(g1.get_object("shared", "k").has_value());
    CHECK_THROWS_S3(g2.create_bucket("shared"), s3::S3ErrorCode::BucketAlreadyOwnedByYou);

    // Each gateway takes a batch of file_ids: globally unique (INCRBY segments, §4)
    std::set<uint64_t> ids;
    for (int i = 0; i < 5000; ++i) {
        CHECK(ids.insert(g1.alloc_file_id(Extent::Kind::kChunk)).second);
        CHECK(ids.insert(g2.alloc_file_id(Extent::Kind::kChunk)).second);
    }
    CHECK(g1.delete_object("shared", "k"));
    g1.delete_bucket("shared");
    g1.close();
    g2.close();
}

// NOSCRIPT self-healing (§3.5): after SCRIPT FLUSH, commit and list work as usual (fall back to EVAL reload)
TEST(duostore_redis_noscript_selfheal) {
    OWNED_REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.create_bucket("heal");
    m.put_object("heal", "k1", make_rec("k1", {}));
    CHECK(RedisTestServer::instance().raw_command("SCRIPT FLUSH"));
    m.put_object("heal", "k2", make_rec("k2", {}));  // commit script self-heals
    auto r = m.list_objects("heal", {});             // list script self-heals
    CHECK_EQ(r.objects.size(), size_t(2));
    for (auto k : {"k1", "k2"}) m.delete_object("heal", k);
    m.delete_bucket("heal");
    m.close();
}

// swap_extents CAS abandon path (§3.3/§9.2): version or extents mismatch -> false, nothing is written
TEST(duostore_redis_swap_extents_cas) {
    REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.create_bucket("swap");
    uint64_t id1 = m.alloc_file_id(Extent::Kind::kChunk);
    uint64_t id2 = m.alloc_file_id(Extent::Kind::kChunk);
    DataRef from{{chunk_extent(id1, 8)}};
    DataRef to{{chunk_extent(id2, 8)}};
    m.put_object("swap", "k", make_rec("k", from.extents));  // version=1

    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));  // version mismatch
    CHECK(m.chunk_referenced(id1));
    CHECK(!m.chunk_referenced(id2));

    CHECK(m.swap_extents("swap", "k", /*expect_version=*/1, from, to));
    auto rec = m.get_object("swap", "k");
    CHECK_EQ(rec->version, uint64_t(2));
    CHECK(rec->data.extents == to.extents);
    CHECK(!m.chunk_referenced(id1));
    CHECK(m.chunk_referenced(id2));

    // After a swap the old from is stale -> swapping again must fail
    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));
    CHECK(m.delete_object("swap", "k"));
    m.delete_bucket("swap");
    m.close();
}

// Concurrent CAS convergence (§3.2): two "gateways" race to overwrite the same key -- version counts strictly,
// refs keeps only the final extent, gcq has exactly (total writes - 1) entries (each overwrite books the old ref once)
TEST(duostore_redis_concurrent_cas_converges) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore g1(redis_opts(prefix));
    RedisMetaStore g2(redis_opts(prefix));
    g1.create_bucket("race");
    constexpr int kPerWriter = 25;

    // Exceptions in threads are carried back via exception_ptr and rethrown on the main thread (otherwise terminate hides the assertion info)
    std::exception_ptr errs[2];
    auto writer = [&](RedisMetaStore& m, std::exception_ptr& err) {
        try {
            for (int i = 0; i < kPerWriter; ++i) {
                uint64_t id = m.alloc_file_id(Extent::Kind::kChunk);
                m.put_object("race", "hot", make_rec("hot", {chunk_extent(id, 1)}));
            }
        } catch (...) {
            err = std::current_exception();
        }
    };
    std::thread t1(writer, std::ref(g1), std::ref(errs[0]));
    std::thread t2(writer, std::ref(g2), std::ref(errs[1]));
    t1.join();
    t2.join();
    for (auto& e : errs)
        if (e) std::rethrow_exception(e);

    auto rec = g1.get_object("race", "hot");
    CHECK(rec.has_value());
    CHECK_EQ(rec->version, uint64_t(2 * kPerWriter));
    CHECK(g1.chunk_referenced(rec->data.extents.at(0).file_id));
    CHECK_EQ(g1.peek_reclaims(1000, 0).size(), size_t(2 * kPerWriter - 1));

    CHECK(g1.delete_object("race", "hot"));
    g1.delete_bucket("race");
    g1.close();
    g2.close();
}

// Calls after close fail cleanly (500) instead of crashing (§5.5)
TEST(duostore_redis_closed_store_throws) {
    REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.close();
    CHECK_THROWS_S3(m.bucket_exists("x"), s3::S3ErrorCode::InternalError);
}

// R4 config: redis_wait_replicas parsing + range validation (pure parsing, no real server needed)
TEST(duostore_redis_config_wait_replicas) {
    std::map<std::string, std::string> p{{"root", "/tmp/duo-redis-cfg"},
                                         {"meta", "redis"},
                                         {"redis_uri", "redis://127.0.0.1:6379"},
                                         {"redis_wait_replicas", "2"}};
    auto c = DuoStoreConfig::from_params("t", p);
    CHECK_EQ(c.redis_wait_replicas, 2);
    p["redis_wait_replicas"] = "-1";
    bool threw = false;
    try {
        DuoStoreConfig::from_params("t", p);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

// R4 metrics + reconnect edge cases (§5.3/§3.5): zero values registered at construction are visible; after CLIENT KILL kills a pooled
// connection -- pure reads retry on a fresh connection (reconnects increments), while an IO failure on a commit-class single command = outcome
// unknown -> InternalError (blind-retry ban), and the store is not disabled (the next call creates a new connection as usual)
TEST(duostore_redis_reconnect_metric_and_commit_boundary) {
    OWNED_REDIS_OR_SKIP();
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = redis_opts(unique_prefix());
    opts.metrics = MetricsScope(reg, {{"backend", "r4"}});
    RedisMetaStore m(opts);
    CHECK(reg->render().find(
              "lights3_duostore_redis_cas_retries_total{backend=\"r4\"} 0\n") !=
          std::string::npos);
    CHECK(reg->render().find(
              "lights3_duostore_redis_reconnects_total{backend=\"r4\"} 0\n") !=
          std::string::npos);

    m.create_bucket("kill");
    CHECK(RedisTestServer::instance().raw_command("CLIENT KILL TYPE normal"));
    CHECK(m.bucket_exists("kill"));  // pure read: bad connection dropped, reconnect and retry succeed
    CHECK(reg->render().find(
              "lights3_duostore_redis_reconnects_total{backend=\"r4\"} 1\n") !=
          std::string::npos);

    CHECK(RedisTestServer::instance().raw_command("CLIENT KILL TYPE normal"));
    CHECK_THROWS_S3(m.ack_reclaim(1), s3::S3ErrorCode::InternalError);
    m.delete_bucket("kill");  // bad connection already discarded, a fresh connection works as usual
    m.close();
}

// R4 wait_replicas (§6): on standalone (0 replicas), WAIT falling short of the replica count only WARNs instead of
// erroring -- the write already took effect on the primary, and an error would mislead clients into retrying. timeout shortened to bound case duration
TEST(duostore_redis_wait_replicas_no_replica_tolerated) {
    REDIS_OR_SKIP();
    auto opts = redis_opts(unique_prefix());
    opts.timeout_ms = 400;  // WAIT timeout is halved = 200ms per commit
    opts.wait_replicas = 1;
    RedisMetaStore m(opts);
    m.create_bucket("wr");
    m.put_object("wr", "k", make_rec("k", {}));
    auto rec = m.get_object("wr", "k");
    CHECK(rec.has_value());
    CHECK_EQ(rec->version, uint64_t(1));
    CHECK(m.delete_object("wr", "k"));
    m.delete_bucket("wr");
    m.close();
}

// R4 list_uploads HSCAN batching (§2.2): the uploads table still returns completely beyond the listpack threshold and a single-batch
// COUNT, in (key, upload_id) lexicographic order, with contents matching what was registered
TEST(duostore_redis_list_uploads_hscan_batches) {
    REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.create_bucket("many");
    constexpr int kUploads = 600;  // > hash-max-listpack-entries(128) converts to a real hashtable, > COUNT 512 spans batches
    std::set<std::pair<std::string, std::string>> expect;
    for (int i = 0; i < kUploads; ++i) {
        std::string k = "k" + std::to_string(i % 40);  // multiple uploads mixed on the same key
        expect.emplace(k, m.create_upload("many", k, {}));
    }
    auto got = m.list_uploads("many", {}, {}, 0);
    CHECK_EQ(got.size(), size_t(kUploads));
    for (size_t i = 1; i < got.size(); ++i)
        CHECK(std::pair(got[i - 1].key, got[i - 1].upload_id) <
              std::pair(got[i].key, got[i].upload_id));
    for (const auto& u : got) CHECK(expect.count({u.key, u.upload_id}) == 1);
    for (const auto& u : got) m.abort_upload("many", u.key, u.upload_id);
    m.delete_bucket("many");
    m.close();
}

// Multi-gateway GC lease (docs/archive/gaps.md §6.1): of two instances with the same prefix only one wins the lease;
// the same owner renewing refreshes the TTL; another owner's expired lease can be taken over
TEST(duostore_redis_gc_lease) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore a(redis_opts(prefix)), b(redis_opts(prefix));
    CHECK(a.try_gc_lease("owner-a", 60'000));
    CHECK(!b.try_gc_lease("owner-b", 60'000));  // held by another and not expired
    CHECK(a.try_gc_lease("owner-a", 60'000));   // same owner renews
    // Takeover after a short lease expires
    RedisMetaStore c(redis_opts(unique_prefix()));
    CHECK(c.try_gc_lease("x", 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK(c.try_gc_lease("y", 60'000));  // x's lease has expired
    a.close();
    b.close();
    c.close();
}

// Multi-gateway read lease (roadmap §3.7): the min across published leases wins;
// expiry (PX) retires a crashed publisher; no publishers = nullopt; no snapshot
// support on this engine (the online dump falls back to the writes-stopped path)
TEST(duostore_redis_read_lease) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore a(redis_opts(prefix)), b(redis_opts(prefix));
    CHECK(!a.min_read_lease().has_value());
    CHECK(a.publish_read_lease("gw-a", 1'000, 60'000));
    CHECK(b.publish_read_lease("gw-b", 500, 60'000));
    auto min = a.min_read_lease();
    CHECK(min.has_value());
    CHECK_EQ(*min, int64_t(500));
    CHECK(b.publish_read_lease("gw-b", 2'000, 60'000));  // b's oldest read finished
    CHECK_EQ(*a.min_read_lease(), int64_t(1'000));
    // A short-TTL lease expires and stops holding the floor down
    CHECK(a.publish_read_lease("gw-c", 1, 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK_EQ(*a.min_read_lease(), int64_t(1'000));
    CHECK(a.snapshot() == nullptr);  // documented engine limitation
    a.close();
    b.close();
}

// Object metadata cache over a shared engine (roadmap §3.8): a peer gateway's overwrite is
// invisible to a cached reader for at most meta_cache_ttl (the documented bounded-staleness
// contract), the published read lease is backdated by that TTL, and from_params enforces
// "off unless configured, and then only with 0 < ttl < gc_grace"
TEST(duostore_redis_meta_cache_bounded_staleness) {
    REDIS_OR_SKIP();
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    std::string prefix = unique_prefix();
    auto make = [&](const char* name) {
        auto meta = std::make_unique<RedisMetaStore>(redis_opts(prefix));
        IMetaStore* mp = meta.get();
        DuoStoreConfig cfg;
        cfg.name = name;
        // Shared data plane, as in a real multi-gateway deployment (rados or a shared
        // filesystem): chunk ids come from the shared meta, so two writers never collide
        cfg.root = tmp.path / "duo";
        cfg.meta_kind = DuoMetaKind::kRedis;
        cfg.pack_threshold = 0;
        cfg.gc_interval_sec = 0;
        cfg.read_lease_sec = 1;
        cfg.meta_cache_ttl_sec = 1;  // default budget stays (direct construction); ttl < gc_grace (300)
        // The TTL contract on its own: with the invalidation feed on (the default), a
        // peer's write would be visible at once -- that path has its own test below
        cfg.meta_cache_feed = false;
        fs::create_directories(cfg.root);
        auto data = std::make_unique<FsDataStore>(
            FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                          cfg.pack_max_size, cfg.pack_writers, {}},
            pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
            [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
        return std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    };
    auto a = make("gw-a");
    auto b = make("gw-b");
    CHECK(a->meta_cache_enabled());
    sync_wait(a->create_bucket("bkt"));
    auto p1 = put(*a, "bkt", "k", "v1");
    {
        auto g = sync_wait(a->get_object("bkt", "k", std::nullopt));  // a caches the full record
        CHECK_EQ(read_all(*g.body), std::string("v1"));
    }
    auto p2 = put(*b, "bkt", "k", "v2-two");  // peer overwrite: a's record is now stale
    CHECK_EQ(sync_wait(b->head_object("bkt", "k")).etag, p2.etag);
    // Within the TTL a still answers from its cache (bounded staleness by contract); the
    // old chunks are still on a's disk (no GC ran), so even the body is the old one
    CHECK_EQ(sync_wait(a->head_object("bkt", "k")).etag, p1.etag);
    CHECK_EQ(a->meta_cache_stats().hits, uint64_t(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(1300));
    // Expired: the next reads go to the shared meta and see the peer's write
    CHECK_EQ(sync_wait(a->head_object("bkt", "k")).etag, p2.etag);
    {
        auto g = sync_wait(a->get_object("bkt", "k", std::nullopt));
        CHECK_EQ(read_all(*g.body), std::string("v2-two"));
    }
    // Read lease backdating: with the cache on, every gateway publishes
    // "oldest in-flight read − ttl", so the floor a peer's GC sees is at least one TTL
    // in the past even while no read is in flight
    RedisMetaStore probe(redis_opts(prefix));
    auto floor = probe.min_read_lease();
    CHECK(floor.has_value());
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    CHECK(*floor + 1000 <= now_ms);
    probe.close();
    sync_wait(a->close());
    sync_wait(b->close());

    // from_params: redis defaults the cache off; enabling it needs a TTL below gc_grace
    std::map<std::string, std::string> base{{"root", (tmp.path / "p").string()},
                                            {"meta", "redis"},
                                            {"redis_uri", RedisTestServer::instance().uri}};
    CHECK_EQ(DuoStoreConfig::from_params("p", base).meta_cache_entries, size_t(0));
    auto on = base;
    on["meta_cache_entries"] = "1K";
    bool thrown = false;
    try {
        DuoStoreConfig::from_params("p", on);  // no ttl
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
    on["meta_cache_ttl"] = "300s";  // == gc_grace default
    thrown = false;
    try {
        DuoStoreConfig::from_params("p", on);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
    on["meta_cache_ttl"] = "2s";
    auto c = DuoStoreConfig::from_params("p", on);
    CHECK_EQ(c.meta_cache_entries, size_t(1024));
    CHECK_EQ(c.meta_cache_ttl_sec, 2);
    CHECK(c.meta_cache_feed);  // the feed is on by default
    on["meta_cache_feed"] = "false";
    CHECK(!DuoStoreConfig::from_params("p", on).meta_cache_feed);
}

// backlog-sequence ⑤ (docs/duostore-redis-meta.md §3.6): with the cache on, a peer's
// commit publishes on <prefix>inv and the local record drops within a message's
// latency instead of at the TTL; a lost feed clears the cache on reconnect
TEST(duostore_redis_cache_invalidation_feed) {
    REDIS_OR_SKIP();
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    std::string prefix = unique_prefix();
    auto metrics = std::make_shared<MetricsRegistry>();
    auto make = [&](const char* name) {
        auto ro = redis_opts(prefix);
        ro.metrics = MetricsScope(metrics, {{"backend", name}});
        auto meta = std::make_unique<RedisMetaStore>(std::move(ro));
        IMetaStore* mp = meta.get();
        DuoStoreConfig cfg;
        cfg.name = name;
        cfg.root = tmp.path / "duo";
        cfg.meta_kind = DuoMetaKind::kRedis;
        cfg.pack_threshold = 0;
        cfg.gc_interval_sec = 0;
        cfg.read_lease_sec = 1;
        cfg.meta_cache_ttl_sec = 100;  // long TTL: only the feed can make a peer's write visible in time
        fs::create_directories(cfg.root);
        auto data = std::make_unique<FsDataStore>(
            FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                          cfg.pack_max_size, cfg.pack_writers, {}},
            pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
            [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
        return std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data),
                                                 MetricsScope(metrics, {{"backend", name}}));
    };
    auto a = make("gw-a");
    auto b = make("gw-b");
    CHECK(a->meta_cache_enabled());
    // Both feeds subscribed (each subscription clears the -- still empty -- cache once)
    auto wait_for = [&](auto pred, int ms) {
        for (int i = 0; i < ms / 20; ++i) {
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return pred();
    };
    CHECK(wait_for([&] {
        return metrics->render().find("lights3_duostore_redis_invalidation_subscribes_total{backend=\"gw-a\"} 1") != std::string::npos;
    }, 3000));

    sync_wait(a->create_bucket("bkt"));
    auto p1 = put(*a, "bkt", "k", "v1");
    CHECK_EQ(sync_wait(a->head_object("bkt", "k")).etag, p1.etag);  // a caches the record
    CHECK(a->meta_cache_stats().entries >= 1);
    // Peer overwrite: the feed drops a's record and the next read sees v2 -- well
    // inside the 100 s TTL
    auto p2 = put(*b, "bkt", "k", "v2-two");
    CHECK(wait_for([&] { return sync_wait(a->head_object("bkt", "k")).etag == p2.etag; }, 3000));
    {
        auto g = sync_wait(a->get_object("bkt", "k", std::nullopt));
        CHECK_EQ(read_all(*g.body), std::string("v2-two"));
    }
    CHECK(a->meta_cache_stats().invalidations >= 1);
    // Peer delete
    sync_wait(a->head_object("bkt", "k"));  // re-cache
    sync_wait(b->delete_object("bkt", "k"));
    CHECK(wait_for([&] {
        try {
            sync_wait(a->head_object("bkt", "k"));
            return false;
        } catch (const s3::S3Error& e) {
            return e.code == s3::S3ErrorCode::NoSuchKey;
        }
    }, 3000));
    // The feed counter on a saw both messages (its own put is published too and
    // dropped harmlessly after the write path's own invalidation)
    CHECK(metrics->render().find("lights3_duostore_meta_cache_feed_invalidations_total{backend=\"gw-a\"}") != std::string::npos);

    // Feed loss: killing the pub/sub clients makes each store reconnect and reset
    // (clear) its cache -- only on a private server (CLIENT KILL is server-global)
    if (RedisTestServer::instance().owned) {
        put(*a, "bkt", "k2", "warm");
        sync_wait(a->head_object("bkt", "k2"));
        CHECK(a->meta_cache_stats().entries >= 1);
        CHECK(RedisTestServer::instance().raw_command("CLIENT KILL TYPE pubsub"));
        CHECK(wait_for([&] {
            return metrics->render().find("lights3_duostore_meta_cache_feed_resets_total{backend=\"gw-a\"} 2") != std::string::npos;
        }, 6000));
        CHECK_EQ(a->meta_cache_stats().entries, size_t(0));
        // ...and keeps working afterwards
        auto p3 = put(*b, "bkt", "k2", "after-reconnect");
        CHECK(wait_for([&] { return sync_wait(a->head_object("bkt", "k2")).etag == p3.etag; }, 3000));
    }
    sync_wait(a->close());
    sync_wait(b->close());
}

// roadmap §3.5: uz:<b> lex index. Pages walked with the composite cursor must concatenate to
// the full listing (which itself must match what was registered); a legacy table without an
// index (simulated by deleting uz:<b>) still lists completely and gets its index rebuilt;
// a stale index member (hash field gone) is skipped and healed
TEST(duostore_redis_list_uploads_lex_index) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore m(redis_opts(prefix));
    m.create_bucket("idx");
    constexpr int kUploads = 700;  // > one ZRANGEBYLEX page (512)
    std::set<std::pair<std::string, std::string>> expect;
    for (int i = 0; i < kUploads; ++i) {
        std::string k = "p" + std::to_string(i % 7) + "/k" + std::to_string(i % 50);
        expect.emplace(k, m.create_upload("idx", k, {}));
    }
    auto full = m.list_uploads("idx");
    CHECK_EQ(full.size(), size_t(kUploads));
    for (size_t i = 1; i < full.size(); ++i)
        CHECK(std::pair(full[i - 1].key, full[i - 1].upload_id) <
              std::pair(full[i].key, full[i].upload_id));
    for (const auto& u : full) CHECK(expect.count({u.key, u.upload_id}) == 1);

    // Cursor pushdown: walk in pages of 97 and compare with the full list
    std::vector<UploadInfo> walked;
    std::string km, im;
    for (int guard = 0; guard < 100; ++guard) {
        auto page = m.list_uploads("idx", km, im, 97);
        if (page.empty()) break;
        CHECK(page.size() <= size_t(97));
        for (auto& u : page) walked.push_back(u);
        km = page.back().key;
        im = page.back().upload_id;
    }
    CHECK_EQ(walked.size(), full.size());
    for (size_t i = 0; i < walked.size() && i < full.size(); ++i) {
        CHECK_EQ(walked[i].key, full[i].key);
        CHECK_EQ(walked[i].upload_id, full[i].upload_id);
    }
    // Prefix pushdown: only p3/ entries, in order, and the limit is honored
    auto p3 = m.list_uploads("idx", {}, {}, 10, "p3/");
    CHECK_EQ(p3.size(), size_t(10));
    for (auto& u : p3) CHECK(u.key.compare(0, 3, "p3/") == 0);
    CHECK_EQ(p3[0].key, full[std::lower_bound(full.begin(), full.end(), std::string("p3/"),
                                              [](const UploadInfo& u, const std::string& k) {
                                                  return u.key < k;
                                              }) -
                             full.begin()]
                            .key);

    // Legacy table: drop the index; the listing must still be complete and rebuild it
    CHECK(RedisTestServer::instance().raw_command(("DEL " + prefix + "uz:idx").c_str()));
    auto legacy = m.list_uploads("idx");
    CHECK_EQ(legacy.size(), size_t(kUploads));
    auto again = m.list_uploads("idx", {}, {}, 5);  // indexed again: limited page comes back
    CHECK_EQ(again.size(), size_t(5));
    CHECK_EQ(again[0].upload_id, full[0].upload_id);

    // Stale index member without a hash field (a write racing the rebuild): never
    // surfaced. The cardinality check routes this listing through the rebuild, which
    // drops the member; the next one is indexed again and honors the limit
    m.abort_upload("idx", full[0].key, full[0].upload_id);
    CHECK(RedisTestServer::instance().raw_command(
        ("ZADD " + prefix + "uz:idx 0 zz-stale-member").c_str()));
    auto rest = m.list_uploads("idx");
    CHECK_EQ(rest.size(), size_t(kUploads - 1));
    for (const auto& u : rest) CHECK(u.key.compare(0, 2, "zz") != 0);
    auto tail = m.list_uploads("idx", "zz", {}, 5);  // indexed: nothing at or after "zz"
    CHECK(tail.empty());
    auto lim = m.list_uploads("idx", {}, {}, 3);
    CHECK_EQ(lim.size(), size_t(3));
    CHECK_EQ(lim[0].upload_id, full[1].upload_id);
    for (const auto& u : rest) m.abort_upload("idx", u.key, u.upload_id);
    m.delete_bucket("idx");
    m.close();
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_REDIS_META

// roadmap §6.1: the redis.command fault point simulates a connection-level
// failure — a read retries once on a fresh connection (reconnect counted), a write
// surfaces InternalError; the store keeps working once the point clears
TEST(duostore_redis_fault_point) {
    REDIS_OR_SKIP();
    RedisMetaStore a(redis_opts(unique_prefix()));
    a.create_bucket("flt");
    fault::arm("redis.command:1:ECONNRESET");
    CHECK(a.bucket_exists("flt"));  // read: retried on a fresh connection
    fault::arm("redis.command:1:ECONNRESET");
    CHECK_THROWS_S3(a.create_bucket("flt2"), s3::S3ErrorCode::InternalError);
    fault::reset();
    a.create_bucket("flt2");
    CHECK(a.bucket_exists("flt2"));
    a.delete_bucket("flt");
    a.delete_bucket("flt2");
    a.close();
}
