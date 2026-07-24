// RadosDataStore 专项单测（docs/duostore-rados-data.md §11）：注入组合跑后端套件、
// 多 chunk roundtrip 与跨 extent Range、未知长度流式、remove 幂等、namespace 隔离、
// 信号量背压、refs 在而对象缺的告警路径、位腐检出、close 守卫。
// 真实集群获取：环境变量 LIGHTS3_TEST_RADOS_CONF + LIGHTS3_TEST_RADOS_POOL 同时
// 设置才跑（可选 LIGHTS3_TEST_RADOS_CLIENT，默认 client.admin），否则显式 SKIP
// （不算失败，机制同 test_duostore_redis.cc）。隔离：每用例唯一 rados_namespace，
// teardown 列举本 namespace 全部对象删除——多套测试可共用一个 pool。
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_RADOS_DATA)

#include <rados/librados.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/thread_pool.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/rados_data_store.h"
#include "storage/duostore/rocks_meta_store.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;
using backend_suite::put;
using backend_suite::read_all;
using backend_suite::TmpDir;

namespace {

struct RadosTestEnv {
    static RadosTestEnv& instance() {
        static RadosTestEnv env;
        return env;
    }
    bool available = false;
    std::string conf, pool, client = "client.admin";

private:
    RadosTestEnv() {
        const char* c = std::getenv("LIGHTS3_TEST_RADOS_CONF");
        const char* p = std::getenv("LIGHTS3_TEST_RADOS_POOL");
        if (!c || !*c || !p || !*p) return;
        conf = c;
        pool = p;
        if (const char* cl = std::getenv("LIGHTS3_TEST_RADOS_CLIENT"); cl && *cl) client = cl;
        available = true;
    }
};

#define RADOS_OR_SKIP()                                                                       \
    if (!RadosTestEnv::instance().available) {                                                \
        printf("       [SKIP] LIGHTS3_TEST_RADOS_CONF/LIGHTS3_TEST_RADOS_POOL not set\n");    \
        return;                                                                               \
    }

// 每用例唯一 namespace（对应 redis_prefix 手法，§11.2）
std::string unique_ns() {
    static std::atomic<int> counter{0};
    return "t" + std::to_string(getpid()) + "-" + std::to_string(counter++);
}

RadosDataOptions rados_opts(const std::string& ns, uint64_t chunk_size = 8ull << 20) {
    auto& env = RadosTestEnv::instance();
    RadosDataOptions o;
    o.conf_path = env.conf;
    o.client_name = env.client;
    o.pool = env.pool;
    o.ns = ns;
    o.chunk_size = chunk_size;
    return o;
}

// 直连集群的观察/注入/清扫通道（librados C API）：列举 namespace、越过 store 删改对象
struct RadosRaw {
    rados_t cluster = nullptr;
    rados_ioctx_t io = nullptr;

    explicit RadosRaw(const std::string& ns) {
        auto& env = RadosTestEnv::instance();
        if (rados_create2(&cluster, "ceph", env.client.c_str(), 0) < 0 ||
            rados_conf_read_file(cluster, env.conf.c_str()) < 0 ||
            rados_connect(cluster) < 0 ||
            rados_ioctx_create(cluster, env.pool.c_str(), &io) < 0)
            throw std::runtime_error("rados test harness: cluster connect failed");
        rados_ioctx_set_namespace(io, ns.c_str());
    }
    ~RadosRaw() {
        if (io) rados_ioctx_destroy(io);
        if (cluster) rados_shutdown(cluster);
    }

    std::vector<std::string> list() {
        rados_list_ctx_t ctx;
        if (rados_nobjects_list_open(io, &ctx) < 0)
            throw std::runtime_error("rados test harness: list_open failed");
        std::vector<std::string> out;
        const char* entry = nullptr;
        while (rados_nobjects_list_next(ctx, &entry, nullptr, nullptr) == 0)
            out.emplace_back(entry);
        rados_nobjects_list_close(ctx);
        return out;
    }
    void remove_all() {
        for (const auto& o : list()) rados_remove(io, o.c_str());
    }
};

// namespace 清扫的 RAII（用例正常/异常路径都清，pool 可复用）
struct NsCleaner {
    std::string ns;
    explicit NsCleaner(std::string n) : ns(std::move(n)) {}
    ~NsCleaner() {
        try {
            RadosRaw(ns).remove_all();
        } catch (...) {
        }
    }
};

// file_id 自给的分配器（直连 data store 的用例不需要 meta）
RadosDataStore::FileIdAlloc counter_alloc() {
    auto next = std::make_shared<std::atomic<uint64_t>>(1);
    return [next](Extent::Kind) { return (*next)++; };
}

// 未知长度流（chunked）：length() = nullopt，走"缓冲至 EOF"的同一切片路径（§3.3）
struct ChunkedBody final : http::BodyReader {
    std::string data;
    size_t pos = 0;
    explicit ChunkedBody(std::string d) : data(std::move(d)) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = std::min(buf.size(), data.size() - pos);
        if (n > 0) {
            std::memcpy(buf.data(), data.data() + pos, n);
            pos += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return std::nullopt; }
};

std::string patterned(size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) s[i] = char('a' + i % 26);
    return s;
}

}  // namespace

// 注入组合（RocksMetaStore + RadosDataStore）跑后端一致性套件（§11.3）
TEST(duostore_rados_backend_suite) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), /*sync=*/false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-suite";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns), pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// 多 chunk 大对象 roundtrip 与跨 extent Range（§11.4）；对象名/个数落位、0 字节对象
TEST(duostore_rados_multichunk_roundtrip_and_layout) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-multi";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    // 4KiB 切片强制多对象 manifest
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));

    std::string body = patterned(10000);
    auto pr = put(*b, "bkt", "big", body);
    auto got = sync_wait(b->get_object("bkt", "big", std::nullopt));
    CHECK_EQ(got.meta.size, uint64_t(10000));
    CHECK_EQ(got.meta.etag, pr.etag);
    CHECK_EQ(read_all(*got.body), body);

    // Range 跨对象边界（4096/8192 两个切点都覆盖）
    auto mid = sync_wait(b->get_object("bkt", "big", ByteRange{4000, 8500}));
    CHECK_EQ(read_all(*mid.body), body.substr(4000, 4501));

    // 布局：10000B / 4KiB = 3 个 rados 对象，名字 c.<016x>
    RadosRaw raw(ns);
    auto objs = raw.list();
    CHECK_EQ(objs.size(), size_t(3));
    for (const auto& o : objs) CHECK(o.rfind("c.", 0) == 0 && o.size() == 18);

    // 0 字节对象：空 DataRef，不产生 rados 对象
    put(*b, "bkt", "empty", "");
    auto empty = sync_wait(b->get_object("bkt", "empty", std::nullopt));
    CHECK_EQ(empty.meta.size, uint64_t(0));
    CHECK_EQ(read_all(*empty.body), "");
    CHECK_EQ(raw.list().size(), size_t(3));
    sync_wait(b->close());
}

// 未知长度流式（§11.4）：EOF 落在 chunk_size 两侧各一例 + 恰好整切点一例
TEST(duostore_rados_unknown_length_stream) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-chunked";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));

    size_t sizes[] = {100, 4096, 5000};  // < 切点 / 恰好切点 / > 切点
    for (size_t n : sizes) {
        std::string body = patterned(n);
        ChunkedBody reader(body);
        sync_wait(b->put_object("bkt", "k" + std::to_string(n), {}, reader));
        auto got = sync_wait(b->get_object("bkt", "k" + std::to_string(n), std::nullopt));
        CHECK_EQ(got.meta.size, uint64_t(n));
        CHECK_EQ(read_all(*got.body), body);
    }
    // 对象数：100→1、4096→1、5000→2
    CHECK_EQ(RadosRaw(ns).list().size(), size_t(4));
    sync_wait(b->close());
}

// remove 幂等（双删，§11.4）与 namespace 隔离（§3.2）
TEST(duostore_rados_remove_idempotent_and_ns_isolation) {
    RADOS_OR_SKIP();
    std::string ns_a = unique_ns(), ns_b = unique_ns();
    NsCleaner ca(ns_a), cb(ns_b);
    auto pool = std::make_shared<ThreadPool>(4);
    RadosDataStore a(rados_opts(ns_a, 4096), pool, counter_alloc());
    RadosDataStore other(rados_opts(ns_b, 4096), pool, counter_alloc());

    auto w = sync_wait(a.open_writer({std::nullopt}));
    std::string body = patterned(9000);
    sync_wait(w->write(std::span(reinterpret_cast<const std::byte*>(body.data()), body.size())));
    DataRef ref = sync_wait(w->finish());
    CHECK_EQ(ref.total(), uint64_t(9000));
    CHECK_EQ(ref.extents.size(), size_t(3));
    CHECK_EQ(RadosRaw(ns_a).list().size(), size_t(3));
    // 隔离：同 pool 另一 namespace 不可见
    CHECK_EQ(RadosRaw(ns_b).list().size(), size_t(0));

    sync_wait(a.remove(ref.extents));
    CHECK_EQ(RadosRaw(ns_a).list().size(), size_t(0));
    sync_wait(a.remove(ref.extents));  // 双删：-ENOENT 幂等忽略
    sync_wait(a.close());
    sync_wait(other.close());
}

// buffer 信号量背压（§4.2/§11.4）：并发 PUT 数 > 缓冲额度数，不死锁、全部完成
TEST(duostore_rados_buffer_backpressure) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-sem";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto opts = rados_opts(ns, 4096);
    opts.buffer_total = 2 * 4096;  // 2 份额度，6 路并发
    auto data = std::make_unique<RadosDataStore>(
        opts, pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));

    constexpr int kWriters = 6;
    std::exception_ptr errs[kWriters];
    std::vector<std::thread> threads;
    for (int i = 0; i < kWriters; ++i)
        threads.emplace_back([&, i] {
            try {
                put(*b, "bkt", "k" + std::to_string(i), patterned(10000 + size_t(i)));
            } catch (...) {
                errs[i] = std::current_exception();
            }
        });
    for (auto& t : threads) t.join();
    for (auto& e : errs)
        if (e) std::rethrow_exception(e);
    for (int i = 0; i < kWriters; ++i) {
        auto got = sync_wait(b->get_object("bkt", "k" + std::to_string(i), std::nullopt));
        CHECK_EQ(read_all(*got.body), patterned(10000 + size_t(i)));
    }
    sync_wait(b->close());
}

// refs 在而对象缺（§6.3/§11.4）：手工 rados 删对象注入 → GET 500，非静默空读
TEST(duostore_rados_missing_object_alarm) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-missing";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns), pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", std::string(1000, 'x'));

    RadosRaw(ns).remove_all();  // 越过 store 删数据面对象，meta refs 仍在
    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    CHECK_THROWS_S3(read_all(*got.body), s3::S3ErrorCode::InternalError);
    sync_wait(b->close());
}

// verify_chunk_crc=true：位腐（越过 store 改写对象内容）在 GET 时检出 500（§5/§11.4）
TEST(duostore_rados_get_detects_bitrot) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-crc";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto opts = rados_opts(ns);
    opts.verify_chunk_crc = true;
    auto data = std::make_unique<RadosDataStore>(
        opts, pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", std::string(1000, 'x'));

    // 注入位腐：同名同长改写一个字节
    RadosRaw raw(ns);
    auto objs = raw.list();
    CHECK_EQ(objs.size(), size_t(1));
    std::string corrupted(1000, 'x');
    corrupted[500] = 'y';
    CHECK(rados_write_full(raw.io, objs[0].c_str(), corrupted.data(), corrupted.size()) == 0);
    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    CHECK_THROWS_S3(read_all(*got.body), s3::S3ErrorCode::InternalError);
    sync_wait(b->close());
}

// close 后调用干净失败（500）而非崩溃（§6.5 守卫）
TEST(duostore_rados_closed_store_throws) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    auto pool = std::make_shared<ThreadPool>(2);
    RadosDataStore d(rados_opts(ns), pool, counter_alloc());
    sync_wait(d.close());
    CHECK_THROWS_S3(sync_wait(d.open_writer({std::nullopt})), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(sync_wait(d.remove({})), s3::S3ErrorCode::InternalError);
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_RADOS_DATA
