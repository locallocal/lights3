// TikvMetaStore dedicated unit tests (docs/duostore-tikv-meta.md §10): meta consistency suite,
// backend suite over the injected combination, prefix isolation, multiple gateways sharing meta, write-skew guard materialization (Op::Lock semantics
// smoke test), swap_extents CAS, concurrent conflict convergence, close guard.
// Obtaining a real cluster: runs only if the env var LIGHTS3_TEST_PD_ADDR is set (comma-separated PD addresses pointing at a tiup
// playground / existing test cluster), otherwise SKIP explicitly (not a failure, same mechanism as
// test_duostore_rados.cc). Isolation: a unique tikv_prefix per test case -- the cluster is reusable and multiple
// test suites do not pollute each other (version garbage is handled by the cluster GC safepoint, §7.3).
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_TIKV_META)

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "core/metrics.h"
#include "core/thread_pool.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/tikv_meta_store.h"
#include "unit/backend_suite.h"
#include "unit/meta_store_suite.h"
#include "unit/mini_test.h"

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;

namespace {

struct TikvTestEnv {
    static TikvTestEnv& instance() {
        static TikvTestEnv env;
        return env;
    }
    bool available = false;
    std::vector<std::string> pd_endpoints;

private:
    TikvTestEnv() {
        const char* v = std::getenv("LIGHTS3_TEST_PD_ADDR");
        if (!v || !*v) return;
        std::string_view rest = v;
        while (!rest.empty()) {
            auto comma = rest.find(',');
            auto ep = rest.substr(0, comma);
            // Trim whitespace: same rule as the pd_endpoints parsing in duostore_backend.cc
            // (the "a:2379, b:2379" writing style behaves identically on both paths)
            while (!ep.empty() && (ep.front() == ' ' || ep.front() == '\t')) ep.remove_prefix(1);
            while (!ep.empty() && (ep.back() == ' ' || ep.back() == '\t')) ep.remove_suffix(1);
            if (!ep.empty()) pd_endpoints.emplace_back(ep);
            if (comma == std::string_view::npos) break;
            rest.remove_prefix(comma + 1);
        }
        available = !pd_endpoints.empty();
    }
};

#define TIKV_OR_SKIP()                                                        \
    if (!TikvTestEnv::instance().available) {                                 \
        printf("       [SKIP] LIGHTS3_TEST_PD_ADDR not set\n");               \
        return;                                                               \
    }

// Unique prefix per test case: the cluster is reusable, multiple suites/runs do not pollute each other (§3.1)
std::string unique_prefix() {
    static std::atomic<int> counter{0};
    return "t" + std::to_string(getpid()) + "-" + std::to_string(counter++) + ":";
}

TikvMetaOptions tikv_opts(const std::string& prefix) {
    TikvMetaOptions o;
    o.pd_endpoints = TikvTestEnv::instance().pd_endpoints;
    o.prefix = prefix;
    return o;
}

using backend_suite::TmpDir;
using meta_store_suite::chunk_extent;
using meta_store_suite::make_rec;

}  // namespace

// Same meta semantics baseline (suite shared with RocksMetaStore, docs/duostore-tikv-meta.md §10)
TEST(duostore_tikv_meta_store_suite) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    meta_store_suite::run_meta_store_suite(
        [&] { return std::make_unique<TikvMetaStore>(tikv_opts(prefix)); });
}

// Run the backend consistency suite over the injected combination (TikvMetaStore + FsDataStore) (§10)
TEST(duostore_tikv_backend_suite) {
    TIKV_OR_SKIP();
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<TikvMetaStore>(tikv_opts(unique_prefix()));
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "tikv-suite";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<FsDataStore>(
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                      cfg.pack_max_size, cfg.pack_writers},
        pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// Prefix isolation (§3.1): two stores share one cluster yet cannot see each other
TEST(duostore_tikv_prefix_isolation) {
    TIKV_OR_SKIP();
    TikvMetaStore a(tikv_opts(unique_prefix()));
    TikvMetaStore b(tikv_opts(unique_prefix()));
    a.create_bucket("iso");
    CHECK(a.bucket_exists("iso"));
    CHECK(!b.bucket_exists("iso"));
    CHECK_EQ(b.list_buckets().size(), size_t(0));
    a.delete_bucket("iso");
    a.close();
    b.close();
}

// Multiple gateways sharing meta (§4.5): two instances with the same prefix share metadata; segment allocation is globally unique
TEST(duostore_tikv_multi_gateway_shared_meta) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore g1(tikv_opts(prefix));
    TikvMetaStore g2(tikv_opts(prefix));
    g1.create_bucket("shared");
    CHECK(g2.bucket_exists("shared"));
    g2.put_object("shared", "k", make_rec("k", {}));
    CHECK(g1.get_object("shared", "k").has_value());
    CHECK_THROWS_S3(g2.create_bucket("shared"), s3::S3ErrorCode::BucketAlreadyOwnedByYou);

    // Each gateway takes a batch of file_ids: globally unique (counter RMW segments, §5)
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

// Write-skew guard materialization (§4.3, T1 verification item: Op::Lock records participate in subsequent prewrite conflict detection):
// put_object and delete_bucket race concurrently; under any interleaving the ghost state "bucket deleted yet object
// remains" must never appear -- with the guard broken this invariant is violated within a few rounds
TEST(duostore_tikv_write_skew_guard) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore a(tikv_opts(prefix));
    TikvMetaStore b(tikv_opts(prefix));
    for (int round = 0; round < 10; ++round) {
        std::string bkt = "skew" + std::to_string(round);
        a.create_bucket(bkt);
        std::exception_ptr errs[2];
        std::thread t1([&] {
            try {
                a.put_object(bkt, "k", make_rec("k", {}));
            } catch (const s3::S3Error& e) {
                if (e.code != s3::S3ErrorCode::NoSuchBucket)  // legitimate failure when bucket deletion wins
                    errs[0] = std::current_exception();
            }
        });
        std::thread t2([&] {
            try {
                b.delete_bucket(bkt);
            } catch (const s3::S3Error& e) {
                if (e.code != s3::S3ErrorCode::BucketNotEmpty)  // legitimate failure when the put wins
                    errs[1] = std::current_exception();
            }
        });
        t1.join();
        t2.join();
        for (auto& e : errs)
            if (e) std::rethrow_exception(e);

        if (!a.bucket_exists(bkt)) {
            // Bucket deletion succeeded => the put must not have left a ghost object (get_object does not check the bucket, so it can see residue)
            CHECK(!a.get_object(bkt, "k").has_value());
        } else {
            // put wins => object exists, bucket exists; the bucket is deletable after cleanup
            CHECK(a.get_object(bkt, "k").has_value());
            CHECK(a.delete_object(bkt, "k"));
            a.delete_bucket(bkt);
        }
    }
    // Clean up the GC ledger (this case's reclaims have no data-plane files, so write them off directly)
    for (auto& [seq, r] : a.peek_reclaims(1000, 0)) a.ack_reclaim(seq);
    a.close();
    b.close();
}

// put_part racing abort_upload (§4.3 upload guard): after abort wins there must be no orphan part left
TEST(duostore_tikv_part_abort_guard) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore a(tikv_opts(prefix));
    TikvMetaStore b(tikv_opts(prefix));
    a.create_bucket("pg");
    for (int round = 0; round < 10; ++round) {
        ObjectMeta meta;
        auto id = a.create_upload("pg", "k", meta);
        std::exception_ptr errs[2];
        std::thread t1([&] {
            try {
                PartRec p;
                p.part_no = 1 + round;
                p.etag = "d41d8cd98f00b204e9800998ecf8427e";
                a.put_part("pg", "k", id, p);
            } catch (const s3::S3Error& e) {
                if (e.code != s3::S3ErrorCode::NoSuchUpload)  // legitimate failure when abort wins
                    errs[0] = std::current_exception();
            }
        });
        std::thread t2([&] {
            try {
                b.abort_upload("pg", "k", id);
            } catch (const s3::S3Error& e) {
                errs[1] = std::current_exception();
            }
        });
        t1.join();
        t2.join();
        for (auto& e : errs)
            if (e) std::rethrow_exception(e);
        // abort committed (no concurrent complete, so it must succeed) => the upload is gone; orphan part detection:
        // with the guard broken, put_part can land after the abort, leaving a residual part row and leaked refs -- observed
        // via chunk_referenced (this case's parts have no extents, so it degrades to a list semantics check)
        CHECK_THROWS_S3(a.list_parts("pg", "k", id), s3::S3ErrorCode::NoSuchUpload);
    }
    a.delete_bucket("pg");
    for (auto& [seq, r] : a.peek_reclaims(1000, 0)) a.ack_reclaim(seq);
    a.close();
    b.close();
}

// Single-value size protection (gaps §2.12): a manifest exceeding what a raft entry can carry fail-fasts with
// EntityTooLarge (400) instead of a 500 from a permanently failing prewrite -- unable to write yet unable to delete hurts most
TEST(duostore_tikv_object_manifest_size_guard) {
    TIKV_OR_SKIP();
    TikvMetaStore m(tikv_opts(unique_prefix()));
    m.create_bucket("etl");
    std::vector<Extent> huge;
    huge.reserve(200'001);
    // Interleaved ids break the run encoding (pathological shape); the count exceeds kMaxObjectExtents
    for (size_t i = 0; i < 200'001; ++i)
        huge.push_back(chunk_extent(i * 2 + 1, 1));
    CHECK_THROWS_S3(m.put_object("etl", "k", make_rec("k", std::move(huge))),
                    s3::S3ErrorCode::EntityTooLarge);
    CHECK(!m.get_object("etl", "k").has_value());  // nothing was written
    m.delete_bucket("etl");
    m.close();
}

// swap_extents CAS abandon path (§4.4/main doc §9.2): version/extents mismatch -> false, nothing is written
TEST(duostore_tikv_swap_extents_cas) {
    TIKV_OR_SKIP();
    TikvMetaStore m(tikv_opts(unique_prefix()));
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

// Concurrent conflict convergence (§4.1): two "gateways" race to overwrite the same key -- WriteConflict retries
// guarantee serializability: version counts strictly, refs keeps only the final extent, gcq has exactly (total writes - 1) entries
TEST(duostore_tikv_concurrent_conflict_converges) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore g1(tikv_opts(prefix));
    TikvMetaStore g2(tikv_opts(prefix));
    g1.create_bucket("race");
    constexpr int kPerWriter = 25;

    // Exceptions in threads are carried back via exception_ptr and rethrown on the main thread (otherwise terminate hides the assertion info)
    std::exception_ptr errs[2];
    auto writer = [&](TikvMetaStore& m, std::exception_ptr& err) {
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
    for (auto& [seq, r] : g1.peek_reclaims(1000, 0)) g1.ack_reclaim(seq);
    g1.close();
    g2.close();
}

// Calls after close fail cleanly (500) instead of crashing (defense-in-depth convention)
TEST(duostore_tikv_closed_store_throws) {
    TIKV_OR_SKIP();
    TikvMetaStore m(tikv_opts(unique_prefix()));
    m.close();
    CHECK_THROWS_S3(m.bucket_exists("x"), s3::S3ErrorCode::InternalError);
}

// ---------- T5 specials (§11) ----------

// Extract a given series' value from the Prometheus rendered text (returns -1 if not found)
namespace {
long long metric_value(const std::string& render, const std::string& series) {
    auto pos = render.find(series + " ");
    if (pos == std::string::npos) return -1;
    return std::stoll(render.substr(pos + series.size() + 1));
}
}  // namespace

// T5 metrics: conflict retry counter -- two gateways race overwriting a hot key; WriteConflict retry rounds land in
// lights3_duostore_tikv_txn_conflict_retries_total; the zero value registered at construction is visible.
// Also smoke-tests backoff budget parameterization (backoff_budget_ms shrunk to 5s, all functional paths pass)
TEST(duostore_tikv_conflict_metric_counts) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = tikv_opts(prefix);
    opts.backoff_budget_ms = 5000;
    opts.metrics = MetricsScope(reg, {{"backend", "t5m"}});
    TikvMetaStore g1(opts);
    CHECK_EQ(metric_value(
                 reg->render(),
                 "lights3_duostore_tikv_txn_conflict_retries_total{backend=\"t5m\"}"),
             0);
    CHECK_EQ(metric_value(reg->render(),
                          "lights3_duostore_tikv_safepoint_update_failures_total{backend=\"t5m\"}"),
             0);

    TikvMetaStore g2(tikv_opts(prefix));
    g1.create_bucket("cm");
    // Conflicts are a product of concurrent interleaving; a single batch may happen to miss -- run bounded rounds until observed
    // (2x15 hot-key overwrites per round; normally shows up within a round or two)
    long long retries = 0;
    for (int round = 0; round < 20 && retries <= 0; ++round) {
        std::exception_ptr errs[2];
        auto writer = [&](TikvMetaStore& m, std::exception_ptr& err) {
            try {
                for (int i = 0; i < 15; ++i) m.put_object("cm", "hot", make_rec("hot", {}));
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
        retries = metric_value(
            reg->render(), "lights3_duostore_tikv_txn_conflict_retries_total{backend=\"t5m\"}");
    }
    CHECK(retries > 0);  // the counter only ever increases

    CHECK(g1.delete_object("cm", "hot"));
    g1.delete_bucket("cm");
    for (auto& [seq, r] : g1.peek_reclaims(1000, 0)) g1.ack_reclaim(seq);
    g1.close();
    g2.close();
}

// T5 GC safepoint (§7.3): a single-round advance = service safepoint declaration + cluster safepoint
// advance, returns >0 and is monotonic across rounds; worker mode (interval>0) advances automatically in the background, close stops cleanly
TEST(duostore_tikv_gc_safepoint_advances) {
    TIKV_OR_SKIP();
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = tikv_opts(unique_prefix());
    opts.gc_retention_s = 60;  // shared cluster: keep a 60s window, do not disturb other cases' in-flight snapshots
    opts.metrics = MetricsScope(reg, {{"backend", "t5sp"}});
    TikvMetaStore m(opts);
    CHECK_EQ(metric_value(reg->render(),
                          "lights3_duostore_tikv_gc_safepoint_ms{backend=\"t5sp\"}"),
             0);
    uint64_t sp1 = m.update_gc_safepoint_once();
    CHECK(sp1 > 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    uint64_t sp2 = m.update_gc_safepoint_once();
    CHECK(sp2 >= sp1);  // monotonic, forward-only on the PD side
    // gauge = physical ms of the most recently advanced cluster safepoint
    CHECK_EQ(metric_value(reg->render(),
                          "lights3_duostore_tikv_gc_safepoint_ms{backend=\"t5sp\"}"),
             (long long)(sp2 >> 18));
    m.close();

    // Worker mode: the first tick advances immediately -- poll until the gauge is nonzero (10s cap), close stops it
    auto wopts = tikv_opts(unique_prefix());
    wopts.gc_safepoint_interval_s = 1;
    wopts.gc_retention_s = 60;
    wopts.metrics = MetricsScope(reg, {{"backend", "t5spw"}});
    TikvMetaStore w(wopts);
    long long pushed = 0;
    for (int i = 0; i < 200 && pushed <= 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        pushed = metric_value(reg->render(),
                              "lights3_duostore_tikv_gc_safepoint_ms{backend=\"t5spw\"}");
    }
    CHECK(pushed > 0);
    w.close();
}

// T5 ten-thousand-part complete special (§6.3): a complete_upload of 10000 parts is the largest transaction in the whole
// implementation (object + upload deletion + full guard + ten-thousand-scale part deletion/refs transfer ~= 20k mutations).
// Verifies: prewrite/commit converge within the scaled lock_ttl (txn_lock_ttl, sqrt(MiB) amplification); concurrent
// readers drive the LockResolver's TTL judgment on the in-flight primary the whole time -- with insufficient TTL they would
// judge the prewriting transaction dead and roll it back, and complete could not succeed. The rationale for not wiring up
// the TTLManager heartbeat is also here: transaction size is bounded (S3 caps at 10k parts), the scaled TTL is enough to cover it
TEST(duostore_tikv_bulk_complete_10k_parts) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore m(tikv_opts(prefix));
    TikvMetaStore reader_store(tikv_opts(prefix));
    m.create_bucket("big");
    ObjectMeta meta;
    auto id = m.create_upload("big", "k", meta);

    constexpr int kParts = 10000;
    const char* kEtag = "d41d8cd98f00b204e9800998ecf8427e";
    std::vector<uint64_t> ids(kParts + 1, 0);
    // 8 threads split work by part_no residue class: the guard shards by part_no % 16, residue classes are disjoint -> zero false collisions
    std::exception_ptr errs[8];
    std::vector<std::thread> ths;
    for (int t = 0; t < 8; ++t) {
        ths.emplace_back([&, t] {
            try {
                for (int no = 1 + t; no <= kParts; no += 8) {
                    uint64_t fid = m.alloc_file_id(Extent::Kind::kChunk);
                    ids[size_t(no)] = fid;
                    PartRec p;
                    p.part_no = no;
                    p.size = 8;
                    p.etag = kEtag;
                    p.data.extents = {chunk_extent(fid, 8)};
                    m.put_part("big", "k", id, p);
                }
            } catch (...) {
                errs[t] = std::current_exception();
            }
        });
    }
    for (auto& th : ths) th.join();
    for (auto& e : errs)
        if (e) std::rethrow_exception(e);

    // Concurrent readers: keep reading the same key during complete, driving the lock-resolution path to judge the primary's TTL
    std::atomic<bool> stop{false};
    std::thread reader([&] {
        while (!stop.load(std::memory_order_relaxed)) reader_store.get_object("big", "k");
    });

    std::vector<PartInfo> parts;
    parts.reserve(kParts);
    for (int no = 1; no <= kParts; ++no) parts.push_back({no, kEtag});
    auto t0 = std::chrono::steady_clock::now();
    std::string etag = m.complete_upload("big", "k", id, parts);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    stop.store(true);
    reader.join();
    printf("       [info] 10k-part complete_upload took %lld ms\n", (long long)ms);
    CHECK(!etag.empty());

    auto rec = m.get_object("big", "k");
    CHECK(rec.has_value());
    CHECK_EQ(rec->data.extents.size(), size_t(kParts));
    CHECK_EQ(rec->meta.size, uint64_t(kParts) * 8);
    CHECK(m.chunk_referenced(ids[1]));
    CHECK(m.chunk_referenced(ids[kParts]));
    CHECK_THROWS_S3(m.list_parts("big", "k", id), s3::S3ErrorCode::NoSuchUpload);

    // Cleanup: delete the object (a second ten-thousand-scale mutation transaction) -> refs emptied, GC ledger written off
    CHECK(m.delete_object("big", "k"));
    CHECK(!m.chunk_referenced(ids[1]));
    m.delete_bucket("big");
    std::vector<uint64_t> seqs;
    for (auto& [seq, r] : m.peek_reclaims(100000, 0)) seqs.push_back(seq);
    m.ack_reclaims(seqs);
    m.close();
    reader_store.close();
}

// Multi-gateway GC lease (docs/gaps.md §6.1, same semantics as the redis version): of two instances with the same prefix only one
// wins; the same owner renews; another owner's expired lease can be taken over
TEST(duostore_tikv_gc_lease) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore a(tikv_opts(prefix)), b(tikv_opts(prefix));
    CHECK(a.try_gc_lease("owner-a", 60'000));
    CHECK(!b.try_gc_lease("owner-b", 60'000));  // held by another and not expired
    CHECK(a.try_gc_lease("owner-a", 60'000));   // same owner renews
    TikvMetaStore c(tikv_opts(unique_prefix()));
    CHECK(c.try_gc_lease("x", 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK(c.try_gc_lease("y", 60'000));  // x's lease has expired (wall-clock judgment)
    a.close();
    b.close();
    c.close();
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_TIKV_META
