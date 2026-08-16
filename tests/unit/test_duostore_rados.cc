// Dedicated RadosDataStore unit tests (docs/duostore-rados-data.md §11): the injected combination runs the backend
// suite, multi-chunk roundtrip and cross-extent Range, unknown-length streaming, idempotent remove, namespace
// isolation, semaphore backpressure, the alarm path for refs-present-but-object-missing, bitrot detection, close
// guard; C3 double-buffered pipeline (including serial degradation), C4 orphan scan (forward/reverse/grace/foreign
// objects) and op metrics.
// Real-cluster acquisition: runs only when the environment variables LIGHTS3_TEST_RADOS_CONF +
// LIGHTS3_TEST_RADOS_POOL are both set (optional LIGHTS3_TEST_RADOS_CLIENT, default client.admin), otherwise an
// explicit SKIP (not a failure; same mechanism as test_duostore_redis.cc). Isolation: a unique rados_namespace per
// case, and teardown lists and deletes all objects in that namespace -- multiple test suites can share one pool.
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

// A unique namespace per case (the counterpart of the redis_prefix technique, §11.2)
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

// Direct-to-cluster channel for observation/injection/cleanup (librados C API): list the namespace, delete/modify objects bypassing the store
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

// RAII for namespace cleanup (cleans on both normal and exceptional case paths, the pool stays reusable)
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

// Self-contained file_id allocator (cases talking directly to the data store need no meta)
RadosDataStore::FileIdAlloc counter_alloc() {
    auto next = std::make_shared<std::atomic<uint64_t>>(1);
    return [next](Extent::Kind, uint32_t n) { return next->fetch_add(n); };
}

// Unknown-length stream (chunked): length() = nullopt, takes the same "buffer until EOF" slicing path (§3.3)
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

// The injected combination (RocksMetaStore + RadosDataStore) runs the backend conformance suite (§11.3)
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
        rados_opts(ns), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// Multi-chunk large-object roundtrip and cross-extent Range (§11.4); object names/count land correctly, 0-byte object
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
    // 4KiB slicing forces a multi-object manifest
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));

    std::string body = patterned(10000);
    auto pr = put(*b, "bkt", "big", body);
    auto got = sync_wait(b->get_object("bkt", "big", std::nullopt));
    CHECK_EQ(got.meta.size, uint64_t(10000));
    CHECK_EQ(got.meta.etag, pr.etag);
    CHECK_EQ(read_all(*got.body), body);

    // Range crossing object boundaries (covers both cut points, 4096 and 8192)
    auto mid = sync_wait(b->get_object("bkt", "big", ByteRange{4000, 8500}));
    CHECK_EQ(read_all(*mid.body), body.substr(4000, 4501));

    // Layout: 10000B / 4KiB = 3 rados objects, named c.<016x>
    RadosRaw raw(ns);
    auto objs = raw.list();
    CHECK_EQ(objs.size(), size_t(3));
    for (const auto& o : objs) CHECK(o.rfind("c.", 0) == 0 && o.size() == 18);

    // 0-byte object: empty DataRef, produces no rados object
    put(*b, "bkt", "empty", "");
    auto empty = sync_wait(b->get_object("bkt", "empty", std::nullopt));
    CHECK_EQ(empty.meta.size, uint64_t(0));
    CHECK_EQ(read_all(*empty.body), "");
    CHECK_EQ(raw.list().size(), size_t(3));
    sync_wait(b->close());
}

// Unknown-length streaming (§11.4): one case with EOF on each side of chunk_size + one exactly on the cut point
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
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));

    size_t sizes[] = {100, 4096, 5000};  // < cut point / exactly the cut point / > cut point
    for (size_t n : sizes) {
        std::string body = patterned(n);
        ChunkedBody reader(body);
        sync_wait(b->put_object("bkt", "k" + std::to_string(n), {}, reader));
        auto got = sync_wait(b->get_object("bkt", "k" + std::to_string(n), std::nullopt));
        CHECK_EQ(got.meta.size, uint64_t(n));
        CHECK_EQ(read_all(*got.body), body);
    }
    // Object counts: 100 -> 1, 4096 -> 1, 5000 -> 2
    CHECK_EQ(RadosRaw(ns).list().size(), size_t(4));
    sync_wait(b->close());
}

// Idempotent remove (double delete, §11.4) and namespace isolation (§3.2)
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
    // Isolation: not visible from another namespace in the same pool
    CHECK_EQ(RadosRaw(ns_b).list().size(), size_t(0));

    sync_wait(a.remove(ref.extents));
    CHECK_EQ(RadosRaw(ns_a).list().size(), size_t(0));
    sync_wait(a.remove(ref.extents));  // double delete: -ENOENT idempotently ignored
    sync_wait(a.close());
    sync_wait(other.close());
}

// Buffer semaphore backpressure (§4.2/§11.4): concurrent PUTs > buffer quota slots, no deadlock, all complete
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
    opts.buffer_total = 2 * 4096;  // 2 quota slots, 6-way concurrency
    auto data = std::make_unique<RadosDataStore>(
        opts, pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
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

// Refs present but object missing (§6.3/§11.4): injected by manually deleting the rados object -> GET 500, not a silent empty read
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
        rados_opts(ns), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", std::string(1000, 'x'));

    RadosRaw(ns).remove_all();  // delete data-plane objects bypassing the store, meta refs remain
    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    CHECK_THROWS_S3(read_all(*got.body), s3::S3ErrorCode::InternalError);
    sync_wait(b->close());
}

// verify_chunk_crc=true: bitrot (object content rewritten bypassing the store) is detected at GET as 500 (§5/§11.4)
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
        opts, pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", std::string(1000, 'x'));

    // Inject bitrot: rewrite one byte, same name and length
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

// run_gc_once converges after overwrite/delete (§11.4, backfilled with mainline P3): rados objects disappear, gcq drains
TEST(duostore_rados_gc_reclaims_after_delete) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-gc";
    cfg.root = tmp.path / "duo";
    cfg.gc_interval_sec = 0;  // background off, tests the manual hook specifically
    cfg.gc_grace_sec = 0;
    fs::create_directories(cfg.root);
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));

    put(*b, "bkt", "k", patterned(10000));  // 3 objects
    put(*b, "bkt", "k", patterned(5000));   // overwrite: old 3 enter gcq, new 2
    sync_wait(b->delete_object("bkt", "k"));
    CHECK_EQ(RadosRaw(ns).list().size(), size_t(5));

    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.reclaims_acked, uint64_t(2));
    CHECK_EQ(st.files_removed, uint64_t(5));
    CHECK_EQ(RadosRaw(ns).list().size(), size_t(0));

    // Convergence: another round performs zero actions
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(0));
    sync_wait(b->close());
}

// GC skips while a concurrent GET holds the pin (§8.1/§11.4: rados has no POSIX open-fd safety net, the pin is
// the read side's only defense, must be tested): delete during read + GC does not remove; content intact after reading; reclaimed once unpinned
TEST(duostore_rados_gc_pin_blocks_remove_during_get) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-gc-pin";
    cfg.root = tmp.path / "duo";
    cfg.gc_interval_sec = 0;
    cfg.gc_grace_sec = 0;
    fs::create_directories(cfg.root);
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));
    std::string body = patterned(10000);
    put(*b, "bkt", "k", body);

    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    std::byte buf[100];
    size_t n0 = sync_wait(got.body->read(std::span(buf)));
    CHECK(n0 > 0);

    sync_wait(b->delete_object("bkt", "k"));
    auto st1 = sync_wait(b->run_gc_once());
    CHECK_EQ(st1.skipped_pinned, uint64_t(1));
    CHECK_EQ(RadosRaw(ns).list().size(), size_t(3));

    std::string rest = read_all(*got.body);  // no -ENOENT
    CHECK_EQ(std::string(reinterpret_cast<char*>(buf), n0) + rest, body);

    got.body.reset();  // destruction releases the pin
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(RadosRaw(ns).list().size(), size_t(0));
    sync_wait(b->close());
}

// C3 double-buffered pipeline (§4.2): a multi-chunk stream through the "write N while collecting N+1" path
// roundtrips without distortion; with buffer_total = chunk_size (try_acquire always fails) it degrades to
// single-buffer serial with the same result. CRC on throughout (full read verifies segment by segment), extent accounting and object counts land correctly
TEST(duostore_rados_pipeline_multi_chunk_stream) {
    RADOS_OR_SKIP();
    auto pool = std::make_shared<ThreadPool>(4);
    std::string body = patterned(100000);  // 100000B / 4KiB = 25 objects
    for (uint64_t buffer_total : {8 * 4096ull, 4096ull}) {  // pipelined / serial degradation
        std::string ns = unique_ns();
        NsCleaner cleaner(ns);
        auto opts = rados_opts(ns, 4096);
        opts.buffer_total = buffer_total;
        opts.verify_chunk_crc = true;
        RadosDataStore d(opts, pool, counter_alloc());
        auto w = sync_wait(d.open_writer({std::nullopt}));
        // Writes in 7000B strides: write boundaries interleave with chunk boundaries, covering cross-slice copying
        std::span<const std::byte> rest(reinterpret_cast<const std::byte*>(body.data()),
                                        body.size());
        while (!rest.empty()) {
            size_t n = std::min<size_t>(7000, rest.size());
            sync_wait(w->write(rest.first(n)));
            rest = rest.subspan(n);
        }
        DataRef ref = sync_wait(w->finish());
        CHECK_EQ(ref.total(), uint64_t(body.size()));
        CHECK_EQ(ref.extents.size(), size_t(25));
        CHECK_EQ(RadosRaw(ns).list().size(), size_t(25));
        auto r = sync_wait(d.open_reader(ref, 0, ref.total() - 1));
        CHECK_EQ(read_all(*r), body);
        sync_wait(d.close());
    }
}

// C4 orphan scan (§8.2): objects written without committing meta -> forward reclaim; objects with foreign names
// do not belong to this store; reverse refs-present-but-object-missing only warns; orphans within grace are untouched
TEST(duostore_rados_orphan_scan_forward_reverse_and_grace) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp.path / "meta").string(), false, 8ull << 20});
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "rados-orphan";
    cfg.root = tmp.path / "duo";
    cfg.data_kind = DuoDataKind::kRados;  // determines the extent kind for orphan unlink
    cfg.gc_interval_sec = 0;
    cfg.gc_grace_sec = 0;
    fs::create_directories(cfg.root);
    auto data = std::make_unique<RadosDataStore>(
        rados_opts(ns, 4096), pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));  // 3 objects on the books

    // Orphan injection: write objects directly to the data store without committing meta (crash-residue shape); ids taken far away to avoid the id segment
    {
        auto far = std::make_shared<std::atomic<uint64_t>>(0xabc0);
        RadosDataStore orphan_src(rados_opts(ns, 4096), pool,
                                  [far](Extent::Kind, uint32_t n) { return far->fetch_add(n); });
        auto w = sync_wait(orphan_src.open_writer({std::nullopt}));
        std::string junk = patterned(5000);  // 2 objects
        sync_wait(w->write(std::span(reinterpret_cast<const std::byte*>(junk.data()),
                                     junk.size())));
        (void)sync_wait(w->finish());  // DataRef discarded as soon as it lands -- meta has no record
        sync_wait(orphan_src.close());
    }
    // Foreign object: not named c.<016x>, ignored by enumeration (§8.2)
    RadosRaw raw(ns);
    CHECK(rados_write_full(raw.io, "not-ours", "x", 1) == 0);

    auto st1 = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(st1.chunks_scanned, uint64_t(5));  // 3 on the books + 2 orphans; foreign objects not counted
    CHECK_EQ(st1.orphans_removed, uint64_t(2));
    CHECK_EQ(st1.refs_missing, uint64_t(0));
    auto after = raw.list();
    CHECK_EQ(after.size(), size_t(4));  // 3 on the books + not-ours
    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    CHECK_EQ(read_all(*got.body), patterned(10000));
    got.body.reset();

    // Reverse: delete one on-the-books object bypassing the store -> only a warning count, meta kept (a lead for manual intervention)
    for (const auto& o : raw.list())
        if (o.rfind("c.", 0) == 0) {
            CHECK(rados_remove(raw.io, o.c_str()) == 0);
            break;
        }
    auto st2 = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(st2.chunks_scanned, uint64_t(2));
    CHECK_EQ(st2.orphans_removed, uint64_t(0));
    CHECK_EQ(st2.refs_missing, uint64_t(1));
    CHECK(sync_wait(b->head_object("bkt", "k")).size == 10000);  // meta untouched
    sync_wait(b->close());

    // Grace shields fresh writes: unreferenced objects within the grace period are untouched (suspected in-flight writes)
    std::string ns2 = unique_ns();
    NsCleaner cleaner2(ns2);
    TmpDir tmp2;
    auto meta2 = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{(tmp2.path / "meta").string(), false, 8ull << 20});
    DuoStoreConfig cfg2 = cfg;
    cfg2.name = "rados-orphan-grace";
    cfg2.root = tmp2.path / "duo";
    cfg2.gc_grace_sec = 3600;
    fs::create_directories(cfg2.root);
    auto far2 = std::make_shared<std::atomic<uint64_t>>(1);
    auto data2 = std::make_unique<RadosDataStore>(
        rados_opts(ns2, 4096), pool, [far2](Extent::Kind, uint32_t n) { return far2->fetch_add(n); });
    auto b2 = std::make_shared<DuoStoreBackend>(cfg2, pool, std::move(meta2), std::move(data2));
    {
        RadosDataStore orphan_src(rados_opts(ns2, 4096), pool,
                                  [far2](Extent::Kind, uint32_t n) { return far2->fetch_add(n); });
        auto w = sync_wait(orphan_src.open_writer({std::nullopt}));
        sync_wait(w->write(std::span(reinterpret_cast<const std::byte*>("x"), 1)));
        (void)sync_wait(w->finish());
        sync_wait(orphan_src.close());
    }
    auto st3 = sync_wait(b2->run_orphan_scan_once());
    CHECK_EQ(st3.skipped_grace, uint64_t(1));
    CHECK_EQ(st3.orphans_removed, uint64_t(0));
    CHECK_EQ(RadosRaw(ns2).list().size(), size_t(1));
    sync_wait(b2->close());
}

// C4 op metrics (§10): zero values visible at construction; write_full/read/remove recorded, errors stay 0
TEST(duostore_rados_op_metrics_registered) {
    RADOS_OR_SKIP();
    std::string ns = unique_ns();
    NsCleaner cleaner(ns);
    auto pool = std::make_shared<ThreadPool>(4);
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = rados_opts(ns, 4096);
    opts.metrics = MetricsScope(reg, {{"backend", "rados-m"}});
    RadosDataStore d(opts, pool, counter_alloc());
    auto text0 = reg->render();
    CHECK(text0.find("lights3_duostore_rados_op_duration_seconds") != std::string::npos);
    CHECK(text0.find("lights3_duostore_rados_op_errors_total") != std::string::npos);

    auto w = sync_wait(d.open_writer({std::nullopt}));
    std::string body = patterned(9000);  // 3 objects
    sync_wait(w->write(std::span(reinterpret_cast<const std::byte*>(body.data()), body.size())));
    DataRef ref = sync_wait(w->finish());
    auto r = sync_wait(d.open_reader(ref, 0, ref.total() - 1));
    CHECK_EQ(read_all(*r), body);
    sync_wait(d.remove(ref.extents));
    auto text = reg->render();
    CHECK(text.find("lights3_duostore_rados_op_duration_seconds_count"
                    "{backend=\"rados-m\",op=\"write_full\"} 3") != std::string::npos);
    CHECK(text.find("op=\"read\"") != std::string::npos);
    CHECK(text.find("lights3_duostore_rados_op_duration_seconds_count"
                    "{backend=\"rados-m\",op=\"remove\"} 3") != std::string::npos);
    CHECK(text.find("lights3_duostore_rados_op_errors_total"
                    "{backend=\"rados-m\",op=\"write_full\"} 0") != std::string::npos);
    sync_wait(d.close());
}

// Calls after close fail cleanly (500) rather than crashing (§6.5 guard)
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
