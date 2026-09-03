// Backend consistency suite: the same set of cases runs parameterized over memory / localfs / xlocalfs (docs/storage-backend.md §6);
// the suite body lives in unit/backend_suite.h (also used by the cloudproxy tests, docs/cloudproxy-backend.md §10)
#include <fcntl.h>
#include <sys/xattr.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <thread>

#include "core/metrics.h"
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

// io_uring fs data plane (roadmap §3.4 ⑤): the same suite over the uring-backed
// FsDataStore -- default parameters (mixed pack/chunk) and small-chunk (multi-chunk
// manifests exercising the read-ahead stream across extents). Where io_uring is
// unavailable the config constructor falls back to the sync path, so the suite stays
// meaningful either way
TEST(duostore_backend_suite_uring) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "suite-uring";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.fs_uring = true;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    run_backend_suite(*b);
    sync_wait(b->close());
}

TEST(duostore_backend_suite_uring_small_chunk) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "suite-uring-4k";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 0;
    cfg.meta_sync = false;
    cfg.fs_uring = true;
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

// Kernel capability probing (docs/archive/gaps.md §6.3): previously IORING_OP_READ/WRITE (5.6+) was used unconditionally, so on
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
        UringEngine::Sqe w;
        w.opcode = IORING_OP_WRITEV;
        w.fd = fd;
        w.addr = reinterpret_cast<uint64_t>(payload.data());
        w.len = unsigned(payload.size());
        int n = co_await eng->raw(0, w);
        CHECK_EQ(n, int(payload.size()));
        std::string out(payload.size(), '\0');
        UringEngine::Sqe r;
        r.opcode = IORING_OP_READV;
        r.fd = fd;
        r.addr = reinterpret_cast<uint64_t>(out.data());
        r.len = unsigned(out.size());
        int m = co_await eng->raw(0, r);
        CHECK_EQ(m, int(out.size()));
        co_return out;
    };
    CHECK_EQ(sync_wait(io()), payload);
    ::close(fd);
    eng->shutdown();
}

// Batched submission (docs/archive/gaps.md §6.3): previously one io_uring_enter per SQE. After switching to "the on-duty
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

// Same-backend copy fast path (docs/archive/gaps.md §6.3): copy_file_range in-kernel transfer, etag identical to the source;
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

// ---------- scrub (roadmap §3.1) ----------

// Full verify over the real layout: single-part, directory marker, and a
// multipart composite recomputed from part_sizes all pass clean; a silently
// flipped byte surfaces as an ETag mismatch; an orphan sidecar is reported but
// (unlike listing) never healed — the scrub is read-only
TEST(localfs_scrub_verifies_and_detects) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "ok.bin", "hello world");
    put(b, "bkt", "dir/", "");
    put(b, "bkt", "bad.bin", "corrupt me please");
    auto id = sync_wait(b.create_multipart("bkt", "mp.bin", {}));
    std::vector<PartInfo> parts;
    {
        http::StringBodyReader r(std::string(1000, 'a'));
        parts.push_back({1, sync_wait(b.upload_part("bkt", "mp.bin", id, 1, r)).etag});
    }
    {
        http::StringBodyReader r(std::string(500, 'b'));
        parts.push_back({2, sync_wait(b.upload_part("bkt", "mp.bin", id, 2, r)).etag});
    }
    sync_wait(b.complete_multipart("bkt", "mp.bin", id, parts));

    auto st0 = sync_wait(b.run_scrub_once());
    CHECK_EQ(st0.objects_scanned, uint64_t(4));
    CHECK_EQ(st0.etag_mismatches, uint64_t(0));
    CHECK_EQ(st0.unverifiable, uint64_t(0));
    CHECK_EQ(st0.read_errors, uint64_t(0));
    CHECK_EQ(st0.orphan_sidecars, uint64_t(0));

    {
        std::fstream f(tmp.path / "data/bkt/bad.bin",
                       std::ios::binary | std::ios::in | std::ios::out);
        f.seekp(0);
        f.put('X');
    }
    auto st1 = sync_wait(b.run_scrub_once());
    CHECK_EQ(st1.etag_mismatches, uint64_t(1));

    fs::remove(tmp.path / "data/bkt/ok.bin");
    auto st2 = sync_wait(b.run_scrub_once());
    CHECK_EQ(st2.objects_scanned, uint64_t(3));
    CHECK_EQ(st2.orphan_sidecars, uint64_t(1));
    CHECK(fs::exists(tmp.path / "data/bkt/ok.bin.lights3-meta"));  // reported, not healed
}

// A multipart object whose metadata predates part_sizes has unrecoverable part
// boundaries: the honest verdict is unverifiable, never a mismatch
TEST(localfs_scrub_legacy_multipart_unverifiable) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    auto id = sync_wait(b.create_multipart("bkt", "mp.bin", {}));
    std::vector<PartInfo> parts;
    {
        http::StringBodyReader r(std::string(800, 'x'));
        parts.push_back({1, sync_wait(b.upload_part("bkt", "mp.bin", id, 1, r)).etag});
    }
    sync_wait(b.complete_multipart("bkt", "mp.bin", id, parts));

    // Strip part_sizes from both metadata carriers to simulate a legacy object
    fs::path data = tmp.path / "data/bkt/mp.bin";
    fs::path side = tmp.path / "data/bkt/mp.bin.lights3-meta";
    std::string filtered;
    {
        std::ifstream in(side);
        std::string line;
        while (std::getline(in, line))
            if (line.rfind("part_sizes\t", 0) != 0) filtered += line + "\n";
    }
    {
        std::ofstream out(side, std::ios::trunc);
        out << filtered;
    }
    (void)::setxattr(data.c_str(), "user.lights3.meta", filtered.data(), filtered.size(), 0);

    auto st = sync_wait(b.run_scrub_once());
    CHECK_EQ(st.objects_scanned, uint64_t(1));
    CHECK_EQ(st.unverifiable, uint64_t(1));
    CHECK_EQ(st.etag_mismatches, uint64_t(0));
}

// xlocalfs shares the on-disk format and inherits the scrub unchanged
TEST(xlocalfs_scrub_inherited) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "k.bin", "xlocalfs payload");
    auto st0 = sync_wait(b.run_scrub_once());
    CHECK_EQ(st0.objects_scanned, uint64_t(1));
    CHECK_EQ(st0.etag_mismatches, uint64_t(0));
    {
        std::fstream f(tmp.path / "data/bkt/k.bin",
                       std::ios::binary | std::ios::in | std::ios::out);
        f.seekp(0);
        f.put('Y');
    }
    auto st1 = sync_wait(b.run_scrub_once());
    CHECK_EQ(st1.etag_mismatches, uint64_t(1));
    sync_wait(b.close());
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

// ---------- roadmap §3.5: listing fan-out + directory snapshot cache ----------

namespace {

// Directory mtimes older than the cache's racy window (2s), so freshly written fixtures are cacheable
void backdate_dirs(const fs::path& root) {
    auto old = fs::file_time_type::clock::now() - std::chrono::seconds(30);
    std::error_code ec;
    for (auto& e : fs::recursive_directory_iterator(root, ec))
        if (e.is_directory(ec)) fs::last_write_time(e.path(), old, ec);
    fs::last_write_time(root, old, ec);
}

std::vector<std::string> listed_keys(const ListResult& r) {
    std::vector<std::string> out;
    for (auto& o : r.objects) out.push_back(o.key);
    return out;
}

// The two backends must agree on the one-shot listing and on the concatenation of paged walks
std::vector<std::string> walk_pages(IStorageBackend& b, ListOptions page) {
    std::vector<std::string> out;
    for (int guard = 0; guard < 200; ++guard) {
        auto r = sync_wait(b.list_objects("bkt", page));
        for (auto& o : r.objects) out.push_back(o.key);
        for (auto& g : r.common_prefixes) out.push_back(g);
        if (!r.is_truncated) break;
        page.start_after = r.next_token;
    }
    return out;
}

void check_listing_matches(IStorageBackend& a, IStorageBackend& b, ListOptions opt) {
    auto ra = sync_wait(a.list_objects("bkt", opt));
    auto rb = sync_wait(b.list_objects("bkt", opt));
    CHECK(listed_keys(ra) == listed_keys(rb));
    CHECK(ra.common_prefixes == rb.common_prefixes);
    CHECK_EQ(ra.is_truncated, rb.is_truncated);
    for (size_t i = 0; i < ra.objects.size() && i < rb.objects.size(); ++i)
        CHECK_EQ(ra.objects[i].etag, rb.objects[i].etag);
    for (int mk : {7, 100}) {
        ListOptions page = opt;
        page.max_keys = mk;
        auto wa = walk_pages(a, page), wb = walk_pages(b, page);
        CHECK(wa == wb);
        CHECK_EQ(wa.size(), ra.objects.size() + ra.common_prefixes.size());
    }
}

}  // namespace

// Metadata of a page is loaded by strided pool workers and directory snapshots come from
// the cache on later pages: results must stay byte-identical to the full-scan reference
// (memory backend), and the second pass must actually hit the cache
TEST(localfs_list_parallel_meta_and_dir_cache_match_reference) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    LocalFsOptions o;
    o.list_meta_concurrency = 4;
    o.list_cache_min_dir_entries = 1;
    o.sidecar_scan_interval_sec = 0;
    LocalFsBackend lf(tmp.path / "data", tmp.path / "staging", pool, o);
    MemoryBackend mem;
    sync_wait(lf.create_bucket("bkt"));
    sync_wait(mem.create_bucket("bkt"));
    for (int i = 0; i < 300; ++i) {
        char num[8];
        snprintf(num, sizeof(num), "%03d", i);
        std::string k = (i % 3 == 0 ? "flat-" : i % 3 == 1 ? "d1/" : "d2/sub/") + std::string(num);
        put(lf, "bkt", k, "v" + std::string(num));
        put(mem, "bkt", k, "v" + std::string(num));
    }
    put(lf, "bkt", "d1/", "");  // directory-marker object sorts first inside d1/
    put(mem, "bkt", "d1/", "");
    backdate_dirs(tmp.path / "data" / "bkt");

    for (const std::string& prefix : {std::string(""), std::string("d1/"), std::string("d2/su"),
                                      std::string("flat-1")}) {
        for (const std::string& delim : {std::string(""), std::string("/"), std::string("-")}) {
            ListOptions opt;
            opt.prefix = prefix;
            opt.delimiter = delim;
            check_listing_matches(lf, mem, opt);
        }
    }
    // start_after deep inside a large directory (binary-searched page start)
    ListOptions mid;
    mid.start_after = "d2/sub/250";
    check_listing_matches(lf, mem, mid);
    auto st = lf.list_cache_stats();
    CHECK(st.misses > 0);
    CHECK(st.hits > st.misses);  // the paged walks re-read the same directories from the snapshot
    CHECK(st.entries > 0);
}

// A directory write (create/delete) changes the directory's mtime, which invalidates its
// snapshot; a directory modified within the racy window is never cached in the first place
TEST(localfs_list_dir_cache_invalidates_on_change) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsOptions o;
    o.list_cache_min_dir_entries = 1;
    o.sidecar_scan_interval_sec = 0;
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, o);
    sync_wait(b.create_bucket("bkt"));
    for (const char* k : {"a", "b", "c"}) put(b, "bkt", k, "x");
    // Freshly modified: two listings, no cache hit
    CHECK_EQ(sync_wait(b.list_objects("bkt", {})).objects.size(), size_t(3));
    CHECK_EQ(sync_wait(b.list_objects("bkt", {})).objects.size(), size_t(3));
    CHECK_EQ(b.list_cache_stats().hits, uint64_t(0));
    backdate_dirs(tmp.path / "data" / "bkt");
    CHECK_EQ(sync_wait(b.list_objects("bkt", {})).objects.size(), size_t(3));  // fills
    CHECK_EQ(sync_wait(b.list_objects("bkt", {})).objects.size(), size_t(3));  // hits
    CHECK_EQ(b.list_cache_stats().hits, uint64_t(1));
    put(b, "bkt", "d", "x");  // mtime bumps → stale snapshot dropped
    auto r = sync_wait(b.list_objects("bkt", {}));
    CHECK_EQ(r.objects.size(), size_t(4));
    CHECK_EQ(r.objects[3].key, std::string("d"));
    sync_wait(b.delete_object("bkt", "a"));
    r = sync_wait(b.list_objects("bkt", {}));
    CHECK_EQ(r.objects.size(), size_t(3));
    CHECK_EQ(r.objects[0].key, std::string("b"));
    CHECK_EQ(b.list_cache_stats().hits, uint64_t(1));  // nothing served stale
}

// A key deleted between the directory read and its stat drops out of the page instead of
// failing the whole LIST (the old inline load_meta turned it into NoSuchKey)
TEST(localfs_list_tolerates_vanished_key) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "a", "x");
    put(b, "bkt", "b", "x");
    // Data gone, sidecar gone, but the directory still lists nothing else -- simulate the
    // window by racing a listing against a delete many times; at minimum it must never throw
    for (int i = 0; i < 20; ++i) {
        put(b, "bkt", "r", "x");
        std::thread t([&] { sync_wait(b.delete_object("bkt", "r")); });
        auto r = sync_wait(b.list_objects("bkt", {}));
        t.join();
        CHECK(r.objects.size() == 2 || r.objects.size() == 3);
    }
}

// ---------- roadmap §3.5: sidecar modes ----------

TEST(localfs_sidecar_modes) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    if (fsutil::probe_meta_xattr(tmp.path) != 0) {
        printf("       [SKIP] temp filesystem has no xattr support\n");
        return;
    }
    // lazy: no sidecar while the xattr succeeds; reads and listings are xattr-backed
    {
        LocalFsOptions o;
        o.sidecar = fsutil::SidecarMode::kLazy;
        o.sidecar_scan_interval_sec = 0;
        LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, o);
        sync_wait(b.create_bucket("bkt"));
        auto pr = put(b, "bkt", "k.bin", "lazy body");
        CHECK(!fs::exists(tmp.path / "data/bkt/k.bin.lights3-meta"));
        CHECK_EQ(sync_wait(b.head_object("bkt", "k.bin")).etag, pr.etag);
        CHECK_EQ(read_all(*sync_wait(b.get_object("bkt", "k.bin", std::nullopt)).body),
                 std::string("lazy body"));
        auto l = sync_wait(b.list_objects("bkt", {}));
        CHECK_EQ(l.objects.size(), size_t(1));
        CHECK_EQ(l.objects[0].etag, pr.etag);
        // A stale sidecar from an earlier sync-mode write is unlinked by the overwrite
        std::ofstream(tmp.path / "data/bkt/k.bin.lights3-meta") << "etag\tstale\n";
        put(b, "bkt", "k.bin", "lazy body v2");
        CHECK(!fs::exists(tmp.path / "data/bkt/k.bin.lights3-meta"));
        // Tagging rewrite stays sidecar-less too
        sync_wait(b.set_object_tagging("bkt", "k.bin", "a=b"));
        CHECK(!fs::exists(tmp.path / "data/bkt/k.bin.lights3-meta"));
        CHECK_EQ(sync_wait(b.head_object("bkt", "k.bin")).tagging, std::string("a=b"));
        // Multipart complete takes the same path
        auto uid = sync_wait(b.create_multipart("bkt", "mp.bin", {}));
        http::StringBodyReader p1("part-1");
        auto e1 = sync_wait(b.upload_part("bkt", "mp.bin", uid, 1, p1));
        sync_wait(b.complete_multipart("bkt", "mp.bin", uid, std::vector<PartInfo>{{1, e1.etag}}));
        CHECK(!fs::exists(tmp.path / "data/bkt/mp.bin.lights3-meta"));
        CHECK_EQ(sync_wait(b.head_object("bkt", "mp.bin")).size, uint64_t(6));
        sync_wait(b.close());
    }
    // async: the sidecar lands after the response, before close() returns
    {
        LocalFsOptions o;
        o.sidecar = fsutil::SidecarMode::kAsync;
        o.sidecar_scan_interval_sec = 0;
        LocalFsBackend b(tmp.path / "data2", tmp.path / "staging2", pool, o);
        sync_wait(b.create_bucket("bkt"));
        auto pr = put(b, "bkt", "k.bin", "async body");
        CHECK_EQ(sync_wait(b.head_object("bkt", "k.bin")).etag, pr.etag);
        sync_wait(b.close());
        fs::path sc = tmp.path / "data2/bkt/k.bin.lights3-meta";
        CHECK(fs::exists(sc));
        bool has_etag = false;
        for (auto& [k, v] : fsutil::read_tsv(sc))
            if (k == "etag" && v == pr.etag) has_etag = true;
        CHECK(has_etag);
    }
    // Both modes keep the sidecar synchronous when the xattr write fails (the sidecar is
    // then the only source): a value above XATTR_SIZE_MAX (64KiB) fails on every filesystem
    for (auto mode : {fsutil::SidecarMode::kLazy, fsutil::SidecarMode::kAsync}) {
        LocalFsOptions o;
        o.sidecar = mode;
        o.sidecar_scan_interval_sec = 0;
        fs::path root = tmp.path / ("data3-" + std::string(fsutil::sidecar_mode_name(mode)));
        LocalFsBackend b(root, root / ".staging", pool, o);
        sync_wait(b.create_bucket("bkt"));
        ObjectMeta m;
        m.user_meta["big"] = std::string(70000, 'x');
        put(b, "bkt", "big.bin", "payload", m);
        CHECK(fs::exists(root / "bkt/big.bin.lights3-meta"));  // written inline, before the response
        CHECK_EQ(sync_wait(b.head_object("bkt", "big.bin")).user_meta["big"].size(), size_t(70000));
        CHECK_EQ(b.xattr_policy().failure_count.load(), uint64_t(1));
        sync_wait(b.close());
    }
}

// ---------- roadmap §3.5: xattr degradation visibility + fail-fast ----------

TEST(localfs_xattr_fallback_gauge_and_require_xattr) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    if (fsutil::probe_meta_xattr(tmp.path) != 0) {
        printf("       [SKIP] temp filesystem has no xattr support\n");
        return;
    }
    auto reg = std::make_shared<MetricsRegistry>();
    {
        LocalFsOptions o;
        o.sidecar_scan_interval_sec = 0;
        LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, o,
                         MetricsScope(reg, {{"backend", "lf"}}));
        sync_wait(b.create_bucket("bkt"));
        CHECK_EQ(b.xattr_policy().failure_count.load(), uint64_t(0));
        std::string out = reg->render();
        CHECK(out.find("lights3_localfs_xattr_fallback{backend=\"lf\"} 0") != std::string::npos);
        ObjectMeta m;
        m.user_meta["big"] = std::string(70000, 'x');  // > XATTR_SIZE_MAX → E2BIG, degrades to sidecar
        put(b, "bkt", "big.bin", "payload", m);
        CHECK_EQ(b.xattr_policy().failure_count.load(), uint64_t(1));
        out = reg->render();
        CHECK(out.find("lights3_localfs_xattr_fallback{backend=\"lf\"} 1") != std::string::npos);
        CHECK(out.find("lights3_localfs_xattr_write_failures_total{backend=\"lf\"} 1") !=
              std::string::npos);
        CHECK_EQ(sync_wait(b.head_object("bkt", "big.bin")).user_meta["big"].size(), size_t(70000));
        sync_wait(b.close());
    }
    {
        LocalFsOptions o;
        o.require_xattr = true;
        o.sidecar_scan_interval_sec = 0;
        LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, o);  // probe passes here
        ObjectMeta m;
        m.user_meta["big"] = std::string(70000, 'x');
        CHECK_THROWS_S3(put(b, "bkt", "strict.bin", "payload", m), S3ErrorCode::InternalError);
        CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "strict.bin")), S3ErrorCode::NoSuchKey);
        CHECK(fs::is_empty(tmp.path / "staging/put"));  // tmp discarded, nothing half-committed
        put(b, "bkt", "ok.bin", "payload");             // normal writes unaffected
        CHECK_EQ(sync_wait(b.head_object("bkt", "ok.bin")).size, uint64_t(7));
        sync_wait(b.close());
    }
}

// ---------- roadmap §3.5: orphan sidecar sweep ----------

TEST(localfs_orphan_sidecar_sweep) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsOptions o;
    o.sidecar_scan_interval_sec = 0;  // drive it by hand
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, o);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "a/b.bin", "x");
    put(b, "bkt", "keep.bin", "y");
    put(b, "bkt", "dir/", "");  // marker object: sidecar is dir/.lights3-dir.lights3-meta
    fs::remove(tmp.path / "data/bkt/a/b.bin");
    fs::remove(tmp.path / "data/bkt/dir/.lights3-dir");
    CHECK(fs::exists(tmp.path / "data/bkt/a/b.bin.lights3-meta"));
    CHECK(fs::exists(tmp.path / "data/bkt/dir/.lights3-dir.lights3-meta"));
    CHECK_EQ(sync_wait(b.run_sidecar_sweep_once()), uint64_t(2));
    CHECK(!fs::exists(tmp.path / "data/bkt/a/b.bin.lights3-meta"));
    CHECK(!fs::exists(tmp.path / "data/bkt/dir/.lights3-dir.lights3-meta"));
    CHECK(fs::exists(tmp.path / "data/bkt/keep.bin.lights3-meta"));
    CHECK_EQ(sync_wait(b.run_sidecar_sweep_once()), uint64_t(0));
    CHECK_EQ(sync_wait(b.head_object("bkt", "keep.bin")).size, uint64_t(1));
    sync_wait(b.close());
}

// ---------- roadmap §3.4: multi-in-flight streams, fixed resources, meta opcodes ----------

namespace {

std::string patterned_bytes(size_t n, uint32_t seed) {
    std::string s(n, '\0');
    uint32_t x = seed;
    for (auto& c : s) {
        x = x * 1664525 + 1013904223;
        c = char(x >> 24);
    }
    return s;
}

void write_file(const fs::path& p, const std::string& data) {
    std::ofstream f(p, std::ios::binary);
    f.write(data.data(), std::streamsize(data.size()));
}

}  // namespace

// Read-ahead stream (roadmap §3.4 ①): multi-block file consumed through caller buffers
// that are deliberately not divisors of the block size (partial slot consumption), plus a
// Range window crossing a block boundary
TEST(uring_read_stream_readahead_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    UringOptions uo;
    uo.read_depth = 3;
    uo.fixed_buffers = 4;
    auto eng = std::make_shared<UringEngine>(pool, uo);
    auto path = tmp.path / "ra.bin";
    std::string data = patterned_bytes((1 << 20) + 12345, 0x5eed);
    write_file(path, data);

    int fd = ::open(path.c_str(), O_RDONLY);
    CHECK(fd >= 0);
    auto io = [&]() -> Task<std::string> {
        UringReadStream rs(eng, fd, 0, data.size());  // fd ownership moves to the stream
        std::string got;
        std::vector<std::byte> buf(40000);
        for (;;) {
            size_t n = co_await rs.read(std::span(buf.data(), buf.size()));
            if (n == 0) break;
            got.append(reinterpret_cast<const char*>(buf.data()), n);
        }
        co_return got;
    };
    CHECK(sync_wait(io()) == data);

    int fd2 = ::open(path.c_str(), O_RDONLY);
    CHECK(fd2 >= 0);
    auto ranged = [&]() -> Task<std::string> {
        UringReadStream rs(eng, fd2, 65530, 16);  // crosses the 64KiB block boundary
        std::string got(16, '\0');
        size_t off = 0;
        while (off < got.size()) {
            size_t n = co_await rs.read(
                std::span(reinterpret_cast<std::byte*>(got.data()) + off, got.size() - off));
            if (n == 0) break;
            off += n;
        }
        got.resize(off);
        co_return got;
    };
    CHECK(sync_wait(ranged()) == data.substr(65530, 16));
    eng->shutdown();
}

// Destroying a stream with read-ahead still in flight must not free buffers under the
// kernel: the refcounted state holds buffers/fd until the last CQE lands, and engine
// shutdown's drain then completes cleanly (ASan/TSan would flag a violation)
TEST(uring_read_stream_abandon_inflight_safe) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    UringOptions uo;
    uo.read_depth = 4;
    auto eng = std::make_shared<UringEngine>(pool, uo);
    auto path = tmp.path / "ab.bin";
    write_file(path, std::string(2 << 20, 'q'));
    for (int i = 0; i < 8; ++i) {
        int fd = ::open(path.c_str(), O_RDONLY);
        CHECK(fd >= 0);
        auto io = [&]() -> Task<void> {
            UringReadStream rs(eng, fd, 0, uint64_t(2 << 20));
            std::byte buf[1000];
            size_t n = co_await rs.read(std::span(buf));  // fills the read-ahead window
            CHECK(n > 0);
            co_return;  // rs destroyed with up to 3 reads still in flight
        };
        sync_wait(io());
    }
    eng->shutdown();
}

// Write pipeline (roadmap §3.4 ①③): odd-sized commits exercise block reuse and the
// hold-back; finish(true) sends the final write + FSYNC as one linked chain. Also the
// empty-stream edge (finish with nothing written)
TEST(uring_write_stream_linked_fsync_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    UringOptions uo;
    uo.write_depth = 2;
    uo.fixed_buffers = 2;
    auto eng = std::make_shared<UringEngine>(pool, uo);
    auto path = tmp.path / "ws.bin";
    std::string data = patterned_bytes(300 * 1024 + 777, 0x17e5);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);
    auto io = [&]() -> Task<void> {
        UringWriteStream ws(eng, fd, 0, uint64_t(data.size()));
        size_t off = 0, step = 1;
        while (off < data.size()) {
            std::span<std::byte> buf = co_await ws.acquire();
            size_t n = std::min({buf.size(), data.size() - off, step});
            std::memcpy(buf.data(), data.data() + off, n);
            ws.commit(n);
            off += n;
            step = step * 3 + 1;
        }
        co_await ws.finish(true);
        CHECK_EQ(ws.written(), uint64_t(data.size()));
    };
    sync_wait(io());
    ::close(fd);
    std::ifstream in(path, std::ios::binary);
    std::string got{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    CHECK(got == data);

    auto path2 = tmp.path / "ws-empty.bin";
    int fd2 = ::open(path2.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd2 >= 0);
    auto empty = [&]() -> Task<void> {
        UringWriteStream ws(eng, fd2, 0, uint64_t(0));
        co_await ws.finish(true);
    };
    sync_wait(empty());
    ::close(fd2);
    CHECK_EQ(uint64_t(fs::file_size(path2)), uint64_t(0));
    eng->shutdown();
}

// Fixed buffer pool exhaustion (roadmap §3.4 ②): more concurrent streams than registered
// blocks -- the overflow silently falls back to heap blocks with identical results, and
// every registered block returns to the pool once the streams are gone
TEST(uring_fixed_buffers_exhaust_and_return) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    UringOptions uo;
    uo.fixed_buffers = 2;
    uo.read_depth = 2;
    auto eng = std::make_shared<UringEngine>(pool, uo);
    if (!eng->features().fixed_buffers) {  // registration refused (old kernel/quota): nothing to assert
        eng->shutdown();
        return;
    }
    CHECK_EQ(eng->fixed_free(0), 2u);
    auto path = tmp.path / "fx.bin";
    std::string data = patterned_bytes(512 * 1024 + 99, 0xf1);
    write_file(path, data);
    auto io = [&]() -> Task<void> {
        std::vector<std::unique_ptr<UringReadStream>> streams;
        for (int i = 0; i < 3; ++i) {  // 3 streams x depth 2 > 2 registered blocks
            int fd = ::open(path.c_str(), O_RDONLY);
            CHECK(fd >= 0);
            streams.push_back(
                std::make_unique<UringReadStream>(eng, fd, 0, uint64_t(data.size())));
        }
        std::vector<std::byte> buf(64 * 1024);
        for (auto& s : streams) {
            std::string got;
            for (;;) {
                size_t n = co_await s->read(std::span(buf.data(), buf.size()));
                if (n == 0) break;
                got.append(reinterpret_cast<const char*>(buf.data()), n);
            }
            CHECK(got == data);
        }
        co_return;
    };
    sync_wait(io());
    CHECK_EQ(eng->fixed_free(0), 2u);  // all registered blocks returned
    eng->shutdown();
}

// Fixed file table (roadmap §3.4 ②): register -> IO through the slot with
// IOSQE_FIXED_FILE -> unregister returns the slot to the pool
TEST(uring_fixed_files_register_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto eng = std::make_shared<UringEngine>(pool, UringOptions{});
    if (!eng->features().fixed_files || !eng->features().op_read_write) {
        eng->shutdown();
        return;
    }
    auto path = tmp.path / "ff.bin";
    std::string data = "fixed-file payload";
    write_file(path, data);
    int fd = ::open(path.c_str(), O_RDONLY);
    CHECK(fd >= 0);
    int slot = eng->register_file(0, fd);
    CHECK(slot >= 0);
    auto io = [&]() -> Task<std::string> {
        std::string out(data.size(), '\0');
        UringEngine::Sqe q;
        q.opcode = IORING_OP_READ;
        q.flags = IOSQE_FIXED_FILE;
        q.fd = slot;
        q.addr = reinterpret_cast<uint64_t>(out.data());
        q.len = unsigned(out.size());
        int n = co_await eng->raw(0, q);
        CHECK_EQ(n, int(out.size()));
        co_return out;
    };
    CHECK_EQ(sync_wait(io()), data);
    eng->unregister_file(0, slot);
    int slot2 = eng->register_file(0, fd);  // the slot came back to the free pool
    CHECK(slot2 >= 0);
    eng->unregister_file(0, slot2);
    ::close(fd);
    eng->shutdown();
}

// Metadata opcodes (roadmap §3.4 ③): openat -> statx -> renameat -> unlinkat through the
// ring, each gated on its probe bit (older kernels skip the missing ones)
TEST(uring_meta_opcodes_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto eng = std::make_shared<UringEngine>(pool, UringOptions{});
    const auto& f = eng->features();
    auto io = [&]() -> Task<void> {
        fs::path p = tmp.path / "meta-a.txt";
        fs::path q = tmp.path / "meta-b.txt";
        if (f.op_openat) {
            int fd = co_await eng->openat(AT_FDCWD, p.c_str(), O_WRONLY | O_CREAT | O_EXCL,
                                          0644);
            CHECK(fd >= 0);
            const char payload[2] = {'h', 'i'};
            int w = co_await eng->write(
                fd, std::span(reinterpret_cast<const std::byte*>(payload), 2), 0);
            CHECK_EQ(w, 2);
            ::close(fd);
        } else {
            write_file(p, "hi");
        }
        if (f.op_statx) {
            struct ::statx stx {};
            CHECK_EQ(co_await eng->statx(AT_FDCWD, p.c_str(), 0, STATX_SIZE, &stx), 0);
            CHECK_EQ(uint64_t(stx.stx_size), uint64_t(2));
        }
        if (f.op_renameat) {
            CHECK_EQ(co_await eng->renameat(AT_FDCWD, p.c_str(), AT_FDCWD, q.c_str()), 0);
        } else {
            fs::rename(p, q);
        }
        CHECK(!fs::exists(p));
        CHECK(fs::exists(q));
        if (f.op_unlinkat) {
            CHECK_EQ(co_await eng->unlinkat(AT_FDCWD, q.c_str(), 0), 0);
            CHECK_EQ(co_await eng->unlinkat(AT_FDCWD, q.c_str(), 0), -ENOENT);  // idempotence signal
            CHECK(!fs::exists(q));
        }
        co_return;
    };
    sync_wait(io());
    eng->shutdown();
}

// Ring sharding (roadmap §3.4 ④): the batched-submission correctness criterion (every
// co_await gets its own result) holds across two independent rings with tiny SQs
TEST(uring_multi_ring_concurrent_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(8);
    UringOptions uo;
    uo.entries = 8;
    uo.rings = 2;
    auto eng = std::make_shared<UringEngine>(pool, uo);
    CHECK_EQ(eng->features().rings, 2u);
    constexpr int kN = 16;
    std::vector<int> fds(kN, -1);
    for (int i = 0; i < kN; ++i) {
        auto p = tmp.path / ("m" + std::to_string(i) + ".bin");
        fds[i] = ::open(p.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        CHECK(fds[i] >= 0);
    }
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

// The full backend consistency suite under stress-shaped stream options: tiny SQs over
// two rings, a 2-block fixed pool, shallow pipelines -- covers SQ-full waits, linked
// chains, fixed-buffer fallback and multi-ring interleaving on the real data paths
TEST(xlocalfs_backend_suite_stream_options) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    UringOptions uo;
    uo.entries = 8;
    uo.rings = 2;
    uo.fixed_buffers = 2;
    uo.read_depth = 2;
    uo.write_depth = 2;
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, uo);
    run_backend_suite(b);
    sync_wait(b.close());
}

// meta_ops=false forces the blocking open/statx/rename/unlink fallbacks while keeping the
// pipelined data plane -- the suite must be indistinguishable
TEST(xlocalfs_backend_suite_no_meta_ops) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    UringOptions uo;
    uo.meta_ops = false;
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, uo);
    run_backend_suite(b);
    sync_wait(b.close());
}

// ---------- object metadata cache (roadmap §3.8) ----------

// MetaCache core: LRU budget, invalidation, the fill token, TTL, clear
TEST(meta_cache_lru_token_ttl) {
    MetaCache<int> c(MetaCacheOptions{4, std::chrono::milliseconds(0), /*shards=*/1});
    CHECK(c.enabled());
    MetaCache<int>::Token tok;
    CHECK(c.lookup("b", "k1", &tok) == nullptr);
    c.insert(tok, "b", "k1", std::make_shared<int>(1));
    CHECK_EQ(*c.lookup("b", "k1"), 1);
    CHECK_EQ(c.stats().hits, uint64_t(1));
    CHECK_EQ(c.stats().misses, uint64_t(1));

    // A fill whose token predates an invalidation of the shard is refused
    MetaCache<int>::Token stale;
    CHECK(c.lookup("b", "k2", &stale) == nullptr);
    c.invalidate("b", "k1");
    c.insert(stale, "b", "k2", std::make_shared<int>(2));
    CHECK(c.lookup("b", "k2") == nullptr);
    CHECK_EQ(c.stats().fills_dropped, uint64_t(1));
    CHECK(c.lookup("b", "k1") == nullptr);  // invalidated
    CHECK_EQ(c.stats().invalidations, uint64_t(1));

    // LRU eviction under the budget: k3..k6 fill it, touching k3 keeps it, k4 goes
    for (int i = 3; i <= 6; ++i) {
        MetaCache<int>::Token t;
        c.lookup("b", "k" + std::to_string(i), &t);
        c.insert(t, "b", "k" + std::to_string(i), std::make_shared<int>(i));
    }
    CHECK_EQ(c.stats().entries, size_t(4));
    CHECK_EQ(*c.lookup("b", "k3"), 3);
    {
        MetaCache<int>::Token t;
        c.lookup("b", "k7", &t);
        c.insert(t, "b", "k7", std::make_shared<int>(7));
    }
    CHECK_EQ(c.stats().entries, size_t(4));
    CHECK(c.lookup("b", "k4") == nullptr);
    CHECK_EQ(*c.lookup("b", "k3"), 3);

    // Same key in another bucket is a different entry
    CHECK(c.lookup("other", "k3") == nullptr);

    // Validation predicate: a rejected record counts as stale, is dropped, and refuses a
    // fill whose token predates the rejection
    {
        MetaCache<int>::Token before;
        c.lookup("b", "k-none", &before);  // token from an unrelated miss on the same shard
        MetaCache<int>::Token t;
        CHECK(c.lookup("b", "k3", &t, [](const int& v) { return v != 3; }) == nullptr);
        CHECK_EQ(c.stats().stale, uint64_t(1));
        CHECK(c.lookup("b", "k3") == nullptr);
        c.insert(before, "b", "k3", std::make_shared<int>(33));  // stale token: dropped
        CHECK(c.lookup("b", "k3") == nullptr);
        c.insert(t, "b", "k3", std::make_shared<int>(3));
        CHECK_EQ(*c.lookup("b", "k3", nullptr, [](const int& v) { return v == 3; }), 3);
    }

    c.clear();
    CHECK_EQ(c.stats().entries, size_t(0));
    CHECK(c.lookup("b", "k3") == nullptr);

    // TTL: an entry expires and counts as a miss afterwards
    MetaCache<int> t(MetaCacheOptions{8, std::chrono::milliseconds(30)});
    MetaCache<int>::Token tt;
    t.lookup("b", "k", &tt);
    t.insert(tt, "b", "k", std::make_shared<int>(9));
    CHECK_EQ(*t.lookup("b", "k"), 9);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    CHECK(t.lookup("b", "k") == nullptr);
    CHECK_EQ(t.stats().entries, size_t(0));

    // Disabled cache: every call is a no-op
    MetaCache<int> off(MetaCacheOptions{0});
    CHECK(!off.enabled());
    off.insert({}, "b", "k", std::make_shared<int>(1));
    CHECK(off.lookup("b", "k") == nullptr);
    CHECK_EQ(off.stats().misses, uint64_t(0));
}

// localfs: HEAD/GET fill and hit, every write path invalidates, tagging rewrite included
TEST(localfs_meta_cache_hits_and_write_invalidation) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsOptions o;
    o.sidecar_scan_interval_sec = 0;
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, o);
    sync_wait(b.create_bucket("bkt"));
    auto p1 = put(b, "bkt", "k", "v1");
    CHECK_EQ(b.meta_cache_stats().entries, size_t(0));  // PUT does not populate

    auto h1 = sync_wait(b.head_object("bkt", "k"));  // miss + fill
    CHECK_EQ(h1.etag, p1.etag);
    CHECK_EQ(b.meta_cache_stats().misses, uint64_t(1));
    CHECK_EQ(b.meta_cache_stats().entries, size_t(1));
    auto h2 = sync_wait(b.head_object("bkt", "k"));  // validated hit
    CHECK_EQ(h2.etag, p1.etag);
    CHECK_EQ(b.meta_cache_stats().hits, uint64_t(1));
    {
        auto s = sync_wait(b.get_object("bkt", "k", std::nullopt));  // hit via the fstat stamp
        CHECK_EQ(s.meta.etag, p1.etag);
        CHECK_EQ(read_all(*s.body), std::string("v1"));
    }
    CHECK_EQ(b.meta_cache_stats().hits, uint64_t(2));

    // Overwrite: the record is dropped after the commit, the next HEAD refetches
    auto p2 = put(b, "bkt", "k", "value-two");
    CHECK_EQ(b.meta_cache_stats().entries, size_t(0));
    auto h3 = sync_wait(b.head_object("bkt", "k"));
    CHECK_EQ(h3.etag, p2.etag);
    CHECK_EQ(h3.size, uint64_t(9));
    CHECK_EQ(b.meta_cache_stats().misses, uint64_t(2));

    // In-place tag rewrite changes the record without a new inode
    sync_wait(b.set_object_tagging("bkt", "k", "a=1"));
    CHECK_EQ(sync_wait(b.head_object("bkt", "k")).tagging, std::string("a=1"));

    // Copy fast path invalidates the destination
    put(b, "bkt", "dst", "old");
    CHECK_EQ(sync_wait(b.head_object("bkt", "dst")).size, uint64_t(3));
    ObjectMeta cm;
    auto cr = sync_wait(b.copy_object_fast("bkt", "k", "bkt", "dst", cm));
    if (cr) CHECK_EQ(sync_wait(b.head_object("bkt", "dst")).etag, p2.etag);

    // Multipart complete invalidates
    auto up = sync_wait(b.create_multipart("bkt", "k", {}));
    {
        http::StringBodyReader part("part-one");
        sync_wait(b.upload_part("bkt", "k", up, 1, part));
    }
    std::vector<PartInfo> parts{{1, sync_wait(b.list_parts("bkt", "k", up, {})).parts[0].etag}};
    auto cpl = sync_wait(b.complete_multipart("bkt", "k", up, parts));
    CHECK_EQ(sync_wait(b.head_object("bkt", "k")).etag, cpl.etag);

    // Delete drops the record; HEAD after delete is a real miss
    sync_wait(b.delete_object("bkt", "k"));
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::NoSuchKey);
    // Nothing above was ever served stale: every hit carried the etag of the time
    CHECK_EQ(b.meta_cache_stats().fills_dropped, uint64_t(0));
}

// localfs: a write made by another process on the same root (a second backend instance
// with its own cache) is caught by the stat stamp when validation is on; with validation
// off the first instance serves the old record until invalidated (the documented trade)
TEST(localfs_meta_cache_stamp_catches_external_write) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsOptions o;
    o.sidecar_scan_interval_sec = 0;
    LocalFsBackend a(tmp.path / "data", tmp.path / "staging", pool, o);
    LocalFsBackend w(tmp.path / "data", tmp.path / "staging2", pool, o);  // "another process"
    sync_wait(a.create_bucket("bkt"));
    auto p1 = put(a, "bkt", "k", "v1");
    CHECK_EQ(sync_wait(a.head_object("bkt", "k")).etag, p1.etag);  // fill
    auto p2 = put(w, "bkt", "k", "v2-longer");
    auto h = sync_wait(a.head_object("bkt", "k"));  // stamp mismatch → stale, refetch
    CHECK_EQ(h.etag, p2.etag);
    CHECK_EQ(h.size, uint64_t(9));
    CHECK_EQ(a.meta_cache_stats().hits, uint64_t(0));
    CHECK_EQ(a.meta_cache_stats().stale, uint64_t(1));
    {
        auto s = sync_wait(a.get_object("bkt", "k", std::nullopt));
        CHECK_EQ(s.meta.etag, p2.etag);
        CHECK_EQ(read_all(*s.body), std::string("v2-longer"));
    }
    CHECK_EQ(a.meta_cache_stats().hits, uint64_t(1));
    // Deleted behind our back: a validated HEAD refetches and reports NoSuchKey
    sync_wait(w.delete_object("bkt", "k"));
    CHECK_THROWS_S3(sync_wait(a.head_object("bkt", "k")), S3ErrorCode::NoSuchKey);

    LocalFsOptions nv = o;
    nv.meta_cache_validate = false;
    LocalFsBackend t(tmp.path / "data", tmp.path / "staging3", pool, nv);
    auto p3 = put(t, "bkt", "k", "v3");
    CHECK_EQ(sync_wait(t.head_object("bkt", "k")).etag, p3.etag);
    auto p4 = put(w, "bkt", "k", "v4");
    CHECK_EQ(sync_wait(t.head_object("bkt", "k")).etag, p3.etag);  // unvalidated hit: stale by design
    CHECK_EQ(t.meta_cache_stats().hits, uint64_t(1));
    // GET always validates against the fd it holds, so it refetches and repairs the record
    {
        auto s = sync_wait(t.get_object("bkt", "k", std::nullopt));
        CHECK_EQ(s.meta.etag, p4.etag);
    }
    CHECK_EQ(sync_wait(t.head_object("bkt", "k")).etag, p4.etag);
    // A cache-off backend behaves exactly as before
    LocalFsOptions off = o;
    off.meta_cache_entries = 0;
    LocalFsBackend z(tmp.path / "data", tmp.path / "staging4", pool, off);
    CHECK_EQ(sync_wait(z.head_object("bkt", "k")).etag, p4.etag);
    CHECK_EQ(z.meta_cache_stats().misses, uint64_t(0));
    CHECK_EQ(z.meta_cache_stats().entries, size_t(0));
}

// xlocalfs shares the cache through the inherited plumbing: its own put/complete/delete
// overrides invalidate and its ring-based GET validates against the fd's fstat
TEST(xlocalfs_meta_cache_paths) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsOptions o;
    o.sidecar_scan_interval_sec = 0;
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool, {}, o);
    sync_wait(b.create_bucket("bkt"));
    auto p1 = put(b, "bkt", "k", "one");
    {
        auto s = sync_wait(b.get_object("bkt", "k", std::nullopt));  // miss + fill
        CHECK_EQ(read_all(*s.body), std::string("one"));
    }
    CHECK_EQ(sync_wait(b.head_object("bkt", "k")).etag, p1.etag);  // hit
    CHECK_EQ(b.meta_cache_stats().hits, uint64_t(1));
    auto p2 = put(b, "bkt", "k", "two-2");
    {
        auto s = sync_wait(b.get_object("bkt", "k", std::nullopt));
        CHECK_EQ(s.meta.etag, p2.etag);
        CHECK_EQ(read_all(*s.body), std::string("two-2"));
    }
    sync_wait(b.delete_object("bkt", "k"));
    CHECK_THROWS_S3(sync_wait(b.head_object("bkt", "k")), S3ErrorCode::NoSuchKey);
    sync_wait(b.close());
}
