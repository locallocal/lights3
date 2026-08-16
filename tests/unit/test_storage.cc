// Backend consistency suite: the same set of cases runs parameterized over memory / localfs / xlocalfs (docs/storage-backend.md §6);
// the suite body lives in unit/backend_suite.h (also used by the cloudproxy tests, docs/cloudproxy-backend.md §10)
#include <fcntl.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <thread>

#include "core/thread_pool.h"
#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"
#include "storage/tiered/tiered_backend.h"
#include "storage/xlocalfs/xlocalfs_backend.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif
#include "unit/backend_suite.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::storage;
using s3::S3ErrorCode;
using backend_suite::put;
using backend_suite::read_all;
using backend_suite::run_backend_suite;
namespace fs = std::filesystem;

using backend_suite::TmpDir;

TEST(memory_backend_suite) {
    MemoryBackend b;
    run_backend_suite(b);
}

// An over-capacity write must not leave a "ghost entry": previously put/complete inserted the slot via operator[] before
// checking capacity, so after the over-limit throw the map kept an object whose data was a null pointer -- later GETs crashed on a
// null dereference (remotely triggerable), HEAD returned fake metadata, and list showed ghost keys. Also verifies the early gate:
// an oversized body is judged while being read, without waiting for full buffering
TEST(memory_backend_capacity_no_ghost) {
    MemoryBackend b(MemoryOptions{/*max_bytes=*/64, /*mpu_ttl_sec=*/0});
    sync_wait(b.create_bucket("bkt"));

    CHECK_THROWS_S3(put(b, "bkt", "big", std::string(1024, 'x')), S3ErrorCode::SlowDown);
    CHECK_THROWS_S3(sync_wait(b.get_object("bkt", "big", std::nullopt)),
                    S3ErrorCode::NoSuchKey);
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "big")), S3ErrorCode::NoSuchKey);
    CHECK(sync_wait(b.list_objects("bkt", {})).objects.empty());
    CHECK_EQ(b.used_bytes(), uint64_t{0});

    put(b, "bkt", "small", "1234");  // a normal in-capacity write is unaffected
    CHECK_EQ(read_all(*sync_wait(b.get_object("bkt", "small", std::nullopt)).body), "1234");

    // Same-shaped multipart defect: an over-limit upload_part leaves no ghost part
    auto uid = sync_wait(b.create_multipart("bkt", "obj", {}));
    {
        http::StringBodyReader oversized(std::string(100, 'y'));
        CHECK_THROWS_S3(sync_wait(b.upload_part("bkt", "obj", uid, 1, oversized)),
                        S3ErrorCode::SlowDown);
    }
    CHECK(sync_wait(b.list_parts("bkt", "obj", uid, {})).parts.empty());
    std::string p1(20, 'a'), p2(20, 'b');
    http::StringBodyReader r1(p1), r2(p2);
    auto e1 = sync_wait(b.upload_part("bkt", "obj", uid, 1, r1));
    auto e2 = sync_wait(b.upload_part("bkt", "obj", uid, 2, r2));
    // Assembled object and parts momentarily coexist (44 + 40 > 64): complete throws on over-limit, but must leave no ghost object
    CHECK_THROWS_S3(
        sync_wait(b.complete_multipart("bkt", "obj", uid,
                                       std::vector<PartInfo>{{1, e1.etag}, {2, e2.etag}})),
        S3ErrorCode::SlowDown);
    CHECK_THROWS_S3(sync_wait(b.get_object("bkt", "obj", std::nullopt)),
                    S3ErrorCode::NoSuchKey);
    // The upload is still intact after the failure: abort reclaims the part accounting normally
    sync_wait(b.abort_multipart("bkt", "obj", uid));
    CHECK_EQ(b.used_bytes(), uint64_t{4});
}

TEST(localfs_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    run_backend_suite(b);
}

TEST(xlocalfs_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    run_backend_suite(b);
    sync_wait(b.close());
}

// tiered is still an ordinary backend toward L2 (docs/tiered-storage.md §2): run the same consistency cases in the all-local state
TEST(tiered_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;  // no background tasks in unit tests
    auto b = std::make_shared<TieredBackend>(local, std::make_shared<MemoryBackend>(), pool, cfg);
    run_backend_suite(*b);
    sync_wait(b->close());
}

#ifdef LIGHTS3_DUOSTORE
// duostore (RocksDB meta + chunk/pack data plane, docs/duostore-backend.md §14):
// three layout variants all green on the same suite -- default parameters (mixed: small objects go to pack), small chunk (forcing
// multi-chunk manifests), forced all-pack (larger threshold + small pack_max_size for high-frequency rotation and sealing)
TEST(duostore_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "suite";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    run_backend_suite(*b);
    sync_wait(b->close());
}

TEST(duostore_backend_suite_small_chunk) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "suite-4k";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 0;  // pack off: this variant specifically tests the multi-chunk manifest path
    cfg.meta_sync = false;   // the variant also covers the meta_sync-off path (§6.3)
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    run_backend_suite(*b);
    sync_wait(b->close());
}

TEST(duostore_backend_suite_all_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "suite-pack";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.pack_threshold = 8 << 10;  // all suite objects are <= 8KiB -> force all-pack (§14 variant)
    cfg.pack_max_size = 8 << 10;   // threshold == cap: small packs rotate frequently, exercising the sealing path fully
    cfg.pack_writers = 2;
    cfg.meta_sync = false;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    run_backend_suite(*b);
    sync_wait(b->close());
}
#endif

// Read/write paths spanning multiple 64KiB data blocks: io_uring streaming writes and offset reads
TEST(xlocalfs_large_object_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    std::string data(1 << 20, '\0');  // 1 MiB of pseudo-random content
    uint32_t x = 0x12345678;
    for (auto& c : data) {
        x = x * 1664525 + 1013904223;
        c = static_cast<char>(x >> 24);
    }
    auto pr = put(b, "bkt", "big/blob.bin", data);

    auto whole = sync_wait(b.get_object("bkt", "big/blob.bin", std::nullopt));
    CHECK_EQ(whole.meta.size, uint64_t(data.size()));
    CHECK_EQ(whole.meta.etag, pr.etag);
    CHECK(read_all(*whole.body) == data);

    // Range crossing a block boundary
    auto mid = sync_wait(b.get_object("bkt", "big/blob.bin",
                                      ByteRange{uint64_t(65530), uint64_t(65545)}));
    CHECK(read_all(*mid.body) == data.substr(65530, 16));

    // multipart: two block-crossing parts assembled via io_uring
    auto uid = sync_wait(b.create_multipart("bkt", "big/joined.bin", {}));
    std::string p1 = data.substr(0, 300 * 1024), p2 = data.substr(300 * 1024);
    http::StringBodyReader b1(p1), b2(p2);
    auto r1 = sync_wait(b.upload_part("bkt", "big/joined.bin", uid, 1, b1));
    auto r2 = sync_wait(b.upload_part("bkt", "big/joined.bin", uid, 2, b2));
    sync_wait(b.complete_multipart("bkt", "big/joined.bin", uid,
                                   std::vector<PartInfo>{{1, r1.etag}, {2, r2.etag}}));
    auto joined = sync_wait(b.get_object("bkt", "big/joined.bin", std::nullopt));
    CHECK(read_all(*joined.body) == data);
    sync_wait(b.close());
}

// Kernel capability probing (docs/gaps.md §6.3): previously IORING_OP_READ/WRITE (5.6+) was used unconditionally, so on
// 5.1-5.5 every IO got -EINVAL. With probing in effect, old kernels take the READV/WRITEV fallback --
// here we positively verify the probe conclusion is self-consistent and exercise the fallback path itself (forcing READ/WRITE
// off cannot be injected, so uring_forced_readv_roundtrip covers it via direct engine calls instead)
TEST(xlocalfs_feature_probe_is_self_consistent) {
    auto pool = std::make_shared<ThreadPool>(2);
    UringEngine eng(pool, UringOptions{});
    const auto& f = eng.features();
    CHECK(!f.describe().empty());
    // When probing is unavailable it must fall to the conservative 5.1 baseline (READV/WRITEV), never optimistically assume READ/WRITE
    if (!f.probed) CHECK(!f.op_read_write);
    eng.shutdown();
}

// READV/WRITEV fallback path (old-kernel shape): submit READV/WRITEV opcodes directly for a read/write pass,
// ensuring iovec framing and offset semantics match READ/WRITE
TEST(xlocalfs_uring_readv_writev_fallback_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto eng = std::make_shared<UringEngine>(pool, UringOptions{});
    auto path = tmp.path / "rw.bin";
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);

    std::string payload = "readv/writev fallback payload";
    auto io = [&]() -> Task<std::string> {
        UringEngine::Awaitable w{*eng,   IORING_OP_WRITEV,
                                 fd,    payload.data(),
                                 unsigned(payload.size()), 0,
                                 {},    {}};
        int n = co_await w;
        CHECK_EQ(n, int(payload.size()));
        std::string out(payload.size(), '\0');
        UringEngine::Awaitable r{*eng,  IORING_OP_READV,
                                 fd,    out.data(),
                                 unsigned(out.size()), 0,
                                 {},    {}};
        int m = co_await r;
        CHECK_EQ(m, int(out.size()));
        co_return out;
    };
    CHECK_EQ(sync_wait(io()), payload);
    ::close(fd);
    eng->shutdown();
}

// Batched submission (docs/gaps.md §6.3): previously one io_uring_enter per SQE. After switching to "the on-duty
// flusher submits on behalf of others", concurrent submissions piggyback on each other -- the correctness criterion is that every
// co_await gets its own result, with no lost or mismatched completions. SQ depth is set below the concurrency, also covering "SQ full -> wait for the flusher to make progress"
TEST(xlocalfs_uring_batched_submit_under_concurrency) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(8);
    auto eng = std::make_shared<UringEngine>(pool, UringOptions{/*entries=*/8});
    constexpr int kN = 16;  // minimal thread count needed: SQ depth 8 already covers "SQ full -> wait for flusher"
    std::vector<int> fds(kN, -1);
    for (int i = 0; i < kN; ++i) {
        auto p = tmp.path / ("c" + std::to_string(i) + ".bin");
        fds[i] = ::open(p.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        CHECK(fds[i] >= 0);
    }
    // Each fd writes unique content and reads it back: a mismatch (A's CQE fed to B) shows up immediately
    auto one = [&](int i) -> Task<bool> {
        std::string want(4096, char('a' + (i % 26)));
        want.replace(0, 8, std::to_string(1000000 + i));
        int n = co_await eng->write(
            fds[i], std::span<const std::byte>(
                        reinterpret_cast<const std::byte*>(want.data()), want.size()),
            0);
        if (n != int(want.size())) co_return false;
        std::string got(want.size(), '\0');
        int m = co_await eng->read(
            fds[i], std::span<std::byte>(reinterpret_cast<std::byte*>(got.data()), got.size()),
            0);
        co_return m == int(want.size()) && got == want;
    };
    std::vector<std::thread> ts;
    std::atomic<int> ok{0};
    for (int i = 0; i < kN; ++i)
        ts.emplace_back([&, i] {
            if (sync_wait(one(i))) ok.fetch_add(1);
        });
    for (auto& t : ts) t.join();
    CHECK_EQ(ok.load(), kN);
    for (int fd : fds) ::close(fd);
    eng->shutdown();
}

// Same-backend copy fast path (docs/gaps.md §6.3): copy_file_range in-kernel transfer, etag identical to the source;
// new user_meta with REPLACE semantics takes effect
TEST(localfs_copy_object_fast) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    std::string data(700 * 1024, 'z');
    for (size_t i = 0; i < data.size(); i += 11) data[i] = char('A' + (i % 26));
    auto pr = put(b, "bkt", "src.bin", data);

    ObjectMeta meta;
    meta.content_type = "application/x-copied";
    meta.user_meta["origin"] = "fast";
    auto r = sync_wait(b.copy_object_fast("bkt", "src.bin", "bkt", "dst/copy.bin", meta));
    CHECK(r.has_value());
    CHECK_EQ(r->etag, pr.etag);  // bytes unchanged, etag identical to the source
    auto got = sync_wait(b.get_object("bkt", "dst/copy.bin", std::nullopt));
    CHECK_EQ(read_all(*got.body), data);
    CHECK_EQ(got.meta.content_type, std::string("application/x-copied"));
    CHECK_EQ(got.meta.user_meta.at("origin"), std::string("fast"));

    // Source missing -> NoSuchKey (not nullopt -- nullopt would make the handler go through a pointless streaming failure)
    CHECK_THROWS_S3(sync_wait(b.copy_object_fast("bkt", "absent", "bkt", "d", {})),
                    lights3::s3::S3ErrorCode::NoSuchKey);
}

TEST(localfs_atomic_layout) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "x/y.bin", "payload");

    // On-disk layout matches docs/storage-backend.md §3.1: data file + sidecar, no staging residue
    CHECK(fs::exists(tmp.path / "data/bkt/x/y.bin"));
    CHECK(fs::exists(tmp.path / "data/bkt/x/y.bin.lights3-meta"));
    size_t staging_leftover = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "staging"))
        if (e.is_regular_file()) ++staging_leftover;
    CHECK_EQ(staging_leftover, size_t(0));

    // Internal reserved names cannot be used as keys
    CHECK_THROWS_S3(put(b, "bkt", "x/y.bin.lights3-meta", "z"),
                    lights3::s3::S3ErrorCode::InvalidArgument);
}

TEST(localfs_multipart_layout_and_cleanup) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    // Parts land in <staging>/mpu/<id>/; after complete the directory is cleaned and the object lands atomically (docs/storage-backend.md §3.2)
    auto uid = sync_wait(b.create_multipart("bkt", "big.bin", {}));
    http::StringBodyReader part("data");
    auto pr = sync_wait(b.upload_part("bkt", "big.bin", uid, 1, part));
    fs::path mpu = tmp.path / "staging/mpu" / uid;
    CHECK(fs::exists(mpu / "manifest"));
    CHECK(fs::exists(mpu / "part.00001"));
    sync_wait(b.complete_multipart("bkt", "big.bin", uid,
                                   std::vector<PartInfo>{{1, pr.etag}}));
    CHECK(!fs::exists(mpu));
    CHECK(fs::exists(tmp.path / "data/bkt/big.bin"));
    CHECK(fs::exists(tmp.path / "data/bkt/big.bin.lights3-meta"));

    // Expired (>7 days) orphan uploads are cleaned when a new instance starts
    auto stale = sync_wait(b.create_multipart("bkt", "stale.bin", {}));
    fs::path stale_dir = tmp.path / "staging/mpu" / stale;
    fs::last_write_time(stale_dir / "manifest",
                        fs::file_time_type::clock::now() - std::chrono::hours(24 * 8));
    LocalFsBackend b2(tmp.path / "data", tmp.path / "staging", pool);
    CHECK(!fs::exists(stale_dir));
}

// ---------- Regression cases found in review (tearing / metadata same-origin / orphan sidecar) ----------

// Concurrent PUTs on the same key do not tear (top critical item in storage.md): the per-key lock in the commit section guarantees
// the body and ETag a GET sees always come from the same write
TEST(localfs_concurrent_put_same_key_not_torn) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(8);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    // Each writer's body content is distinct, so its md5 fingerprints that write. The race window is narrow;
    // repeated rounds push the detection probability for a lock-free implementation close to 1 (with the lock it always passes)
    for (int round = 0; round < 40; ++round) {
        std::vector<std::string> bodies;
        for (int i = 0; i < 16; ++i)
            bodies.push_back(std::string(4096, char('a' + i)) + std::to_string(round));
        std::map<std::string, std::string> etag_of;  // etag → body
        std::mutex m;
        std::vector<std::thread> writers;
        for (auto& body : bodies) {
            writers.emplace_back([&, body] {
                auto r = put(b, "bkt", "hot.bin", body);
                std::lock_guard lk(m);
                etag_of[r.etag] = body;
            });
        }
        for (auto& t : writers) t.join();

        // The body read must be exactly the content of the write its ETag corresponds to (they disagree on tearing)
        auto s = sync_wait(b.get_object("bkt", "hot.bin", std::nullopt));
        std::string got = read_all(*s.body);
        auto it = etag_of.find(s.meta.etag);
        CHECK(it != etag_of.end());
        CHECK(got == it->second);
        CHECK_EQ(s.meta.size, uint64_t(got.size()));

        // The sidecar must also describe the write that finally landed: data and sidecar are two renames, and
        // without the per-key lock they can interleave into "data from A, sidecar from B". xattr is bound to the inode
        // and unaffected by interleaving, so the sidecar is the direct observation point for this lock (and what external tools see)
        std::string sidecar_etag;
        {
            std::ifstream f(tmp.path / "data/bkt/hot.bin.lights3-meta", std::ios::binary);
            std::string line;
            while (std::getline(f, line))
                if (line.rfind("etag\t", 0) == 0) sidecar_etag = line.substr(5);
        }
        CHECK_EQ(sidecar_etag, s.meta.etag);
    }
}

// Metadata is committed together with the data file: xattr is bound to the inode, so even with the sidecar missing (the crash
// window of "data renamed, sidecar not yet written") GET still returns an etag consistent with the body
TEST(localfs_meta_committed_with_data) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    auto pr = put(b, "bkt", "k.bin", "hello xattr");

    std::error_code ec;
    fs::remove(tmp.path / "data/bkt/k.bin.lights3-meta", ec);

    auto s = sync_wait(b.get_object("bkt", "k.bin", std::nullopt));
    CHECK_EQ(read_all(*s.body), std::string("hello xattr"));
    // On filesystems with xattr the etag is still correct; without support it degrades to empty (sidecar-only semantics)
    if (!s.meta.etag.empty()) CHECK_EQ(s.meta.etag, pr.etag);
}

// GET uses fstat on the already-open fd: after a concurrent overwrite, body and meta must not come from different inodes
TEST(localfs_get_meta_matches_open_inode) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    std::string v1(8192, 'x');
    auto pr1 = put(b, "bkt", "k.bin", v1);

    auto s = sync_wait(b.get_object("bkt", "k.bin", std::nullopt));  // holds an fd on the old inode
    put(b, "bkt", "k.bin", std::string(64, 'y'));                    // overwrite with a shorter new object
    std::string got = read_all(*s.body);
    // A second stat on the path would swap size to the new object's 64 and etag to the new etag, while the body is still
    // the old inode's content -- all three must be consistent
    CHECK_EQ(s.meta.size, uint64_t(v1.size()));
    CHECK_EQ(s.meta.etag, pr1.etag);
    CHECK(got == v1);
}

// Orphan sidecar self-healing: a sidecar left by a crash between the two delete steps is cleaned up in passing by list
TEST(localfs_orphan_sidecar_reaped_by_list) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "gone.bin", "x");
    put(b, "bkt", "stay.bin", "y");

    fs::remove(tmp.path / "data/bkt/gone.bin");  // simulate "data deleted, sidecar not deleted"
    fs::path orphan = tmp.path / "data/bkt/gone.bin.lights3-meta";
    CHECK(fs::exists(orphan));

    auto res = sync_wait(b.list_objects("bkt", ListOptions{}));
    CHECK_EQ(res.objects.size(), size_t(1));
    CHECK_EQ(res.objects[0].key, std::string("stay.bin"));
    CHECK(!fs::exists(orphan));
    CHECK(fs::exists(tmp.path / "data/bkt/stay.bin.lights3-meta"));
}

// ---------- P0 §1.3 / §1.4 regressions ----------

// commit_cached must flush data to disk before the rename: the sidecar written afterwards is fsynced, so losing power
// before the data is flushed would yield an object where "the sidecar says cached/size=N but the file is N bytes of zero blocks",
// and the StubRace check compares st_size (the inode size the rename already committed) so it cannot catch this.
// What is asserted here is that correct content is readable immediately after commit (fsync correctness cannot simulate power
// loss in a unit test, but a wrong ordering would be exposed under LIGHTS3_FSYNC=1 via fsync_path's errno path)
TEST(localfs_commit_cached_persists_data_before_sidecar) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;
    cfg.cold_after_sec = 0;  // considered cold immediately
    auto cloud = std::make_shared<MemoryBackend>();
    auto b = std::make_shared<TieredBackend>(local, cloud, pool, cfg);
    sync_wait(b->create_bucket("bkt"));
    std::string data(64 * 1024, 'c');
    put(*b, "bkt", "k.bin", data);

    // Sink to the cloud (local becomes a stub), then GET triggers Tee backfill -> commit_cached
    sync_wait(b->scan_once());
    auto s1 = sync_wait(b->get_object("bkt", "k.bin", std::nullopt));
    CHECK_EQ(read_all(*s1.body), data);   // backfill commits at EOF

    // Content read via the cache-hit path must match the original byte for byte (a wrong commit order reads truncated/zero blocks)
    auto s2 = sync_wait(b->get_object("bkt", "k.bin", std::nullopt));
    CHECK_EQ(read_all(*s2.body), data);
    CHECK_EQ(s2.meta.size, uint64_t(data.size()));
    sync_wait(b->close());
}

// ---------- Diff between localfs pruned LIST and the full-scan reference implementation (gaps §2.7) ----------
// The memory backend goes through apply_listing (full collection + sort) as the semantic reference; localfs's
// pruned directory-tree walk must produce identical results for any prefix/delimiter/pagination combination
TEST(localfs_list_pruning_matches_reference) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    LocalFsBackend lf(tmp.path / "data", tmp.path / "staging", pool);
    MemoryBackend mem;
    sync_wait(lf.create_bucket("bkt"));
    sync_wait(mem.create_bucket("bkt"));
    const std::vector<std::string> keys = {
        "a.txt",      "a/b.txt",     "a/b/c.txt",  "a/b/d.txt", "a/e.txt",
        "a0after",    "dir-x/1",     "dir-x/2",    "dir-y/1",   "photos/2026/a.jpg",
        "photos/2026/b.jpg", "photos/2027/c.jpg", "readme.md", "z-last",
    };
    for (auto& k : keys) {
        put(lf, "bkt", k, "v");
        put(mem, "bkt", k, "v");
    }

    auto keys_of = [](const ListResult& r) {
        std::vector<std::string> out;
        for (auto& o : r.objects) out.push_back(o.key);
        return out;
    };
    auto check_same = [&](ListOptions opt) {
        auto a = sync_wait(lf.list_objects("bkt", opt));
        auto b = sync_wait(mem.list_objects("bkt", opt));
        CHECK(keys_of(a) == keys_of(b));
        CHECK(a.common_prefixes == b.common_prefixes);
        CHECK_EQ(a.is_truncated, b.is_truncated);
        // Pagination walk: token semantics only need to be self-consistent; here each does another full-alignment pass
        return std::pair(a, b);
    };

    for (const std::string& prefix :
         {std::string(""), std::string("a"), std::string("a/"), std::string("a/b"),
          std::string("photos/202"), std::string("photos/2026/"), std::string("nope/"),
          std::string("dir-")}) {
        for (const std::string& delim : {std::string(""), std::string("/"), std::string("-")}) {
            ListOptions opt;
            opt.prefix = prefix;
            opt.delimiter = delim;
            check_same(opt);
            // Page-by-page walk (max_keys=1/2/3): concatenated it must equal the one-shot full listing with no duplicates
            for (int mk : {1, 2, 3}) {
                for (auto* backend : std::initializer_list<IStorageBackend*>{&lf, &mem}) {
                    ListOptions page;
                    page.prefix = prefix;
                    page.delimiter = delim;
                    page.max_keys = mk;
                    std::vector<std::string> walked;
                    for (int guard = 0; guard < 50; ++guard) {
                        auto r = sync_wait(backend->list_objects("bkt", page));
                        for (auto& o : r.objects) walked.push_back(o.key);
                        for (auto& g : r.common_prefixes) walked.push_back(g);
                        if (!r.is_truncated) break;
                        CHECK(!r.next_token.empty());
                        page.start_after = r.next_token;
                    }
                    ListOptions full;
                    full.prefix = prefix;
                    full.delimiter = delim;
                    auto fr = sync_wait(backend->list_objects("bkt", full));
                    std::vector<std::string> expect;
                    for (auto& o : fr.objects) expect.push_back(o.key);
                    for (auto& g : fr.common_prefixes) expect.push_back(g);
                    std::sort(walked.begin(), walked.end());
                    std::sort(expect.begin(), expect.end());
                    CHECK(walked == expect);
                }
            }
        }
    }

    // start_after falls inside a group: the group must still be emitted (consistent with the reference implementation)
    ListOptions mid;
    mid.delimiter = "/";
    mid.start_after = "a/b/c.txt";
    check_same(mid);
}
