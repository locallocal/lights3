// Dedicated DuoStore unit tests (docs/duostore-backend.md §14): codec roundtrip and run boundaries,
// cross-chunk read/write, bitrot detection, GC phase one (P3), pack aggregation (P2: routing / chunked
// buffering / rotation sealing / record format / crc / abandonment on restart / whole-empty-pack deletion).
// Meta-semantics cases (GC accounting, monotonic id segments, pack liveness accounting, etc.) have been
// interfaced out as meta_store_suite (docs/duostore-redis-meta.md §9); RocksMetaStore always runs here,
// redis/sqlite/tikv run conditionally in their own test files.
// Compaction / crash-injection specials added with P4.
#ifdef LIGHTS3_DUOSTORE

#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <semaphore>
#include <string>
#include <thread>
#include <vector>

#include "core/thread_pool.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/meta_util.h"
#include "storage/duostore/rocks_meta_store.h"
#include "unit/backend_suite.h"
#include "unit/meta_store_suite.h"
#include "unit/mini_test.h"

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;
using backend_suite::put;
using backend_suite::read_all;

namespace {

using backend_suite::TmpDir;

RocksMetaOptions meta_opts(const TmpDir& tmp) {
    // Unit tests do not need WAL fsync (crash semantics have their own specials, P4)
    return {(tmp.path / "meta").string(), /*sync=*/false, /*block_cache=*/8ull << 20};
}

Extent chunk_extent(uint64_t id, uint64_t len, uint32_t crc = 0) {
    return {Extent::Kind::kChunk, id, 0, len, crc};
}

ObjectRec make_rec(std::string key, std::vector<Extent> extents) {
    ObjectRec rec;
    rec.meta.key = std::move(key);
    rec.meta.etag = "deadbeef";
    rec.meta.last_modified = std::chrono::system_clock::now();
    rec.data.extents = std::move(extents);
    rec.meta.size = rec.data.total();
    return rec;
}

}  // namespace

TEST(duostore_crc32c_vectors) {
    // RFC 3720 Appendix B vector: crc32c("123456789") = 0xE3069283
    CHECK_EQ(codec::crc32c_of(std::string_view("123456789")), 0xE3069283u);
    // Chained incremental == one-shot
    uint32_t chained = codec::crc32c_of(std::string_view("12345"));
    std::string_view rest = "6789";
    chained = codec::crc32c_update(
        chained, std::span(reinterpret_cast<const std::byte*>(rest.data()), rest.size()));
    CHECK_EQ(chained, 0xE3069283u);
    CHECK_EQ(codec::crc32c_of(std::string_view("")), 0u);
}

TEST(duostore_extent_run_roundtrip) {
    // Fixed-length chunks with consecutive file_ids (last block short) compress into a single run: 4B n_runs + 37B run header + 5×4B crc
    std::vector<Extent> ex;
    for (uint64_t i = 0; i < 5; ++i)
        ex.push_back(chunk_extent(100 + i, i == 4 ? 1000 : 8192, uint32_t(i)));
    std::string enc = codec::encode_extents(ex);
    CHECK_EQ(enc.size(), size_t(4 + 37 + 5 * 4));
    CHECK(codec::decode_extents(enc) == ex);

    // Non-consecutive file_id -> splits into a new run
    ex.push_back(chunk_extent(200, 8192, 9));
    CHECK(codec::decode_extents(codec::encode_extents(ex)) == ex);

    // A short block in the middle blocks merging (within a run all but the last segment must be full-length)
    std::vector<Extent> mixed = {chunk_extent(1, 8192, 1), chunk_extent(2, 100, 2),
                                 chunk_extent(3, 8192, 3)};
    CHECK(codec::decode_extents(codec::encode_extents(mixed)) == mixed);

    // Pack extents do not merge and keep their offset
    std::vector<Extent> packs = {{Extent::Kind::kPack, 7, 4096, 100, 42},
                                 {Extent::Kind::kPack, 7, 8192, 50, 43}};
    CHECK(codec::decode_extents(codec::encode_extents(packs)) == packs);

    // kRados merges isomorphically to kChunk (docs/duostore-rados-data.md §3.1): consecutive ids compress into a
    // single run; adjacent extents of different kinds do not merge
    std::vector<Extent> rados;
    for (uint64_t i = 0; i < 4; ++i)
        rados.push_back({Extent::Kind::kRados, 500 + i, 0, i == 3 ? 100 : 8192, uint32_t(i)});
    CHECK_EQ(codec::encode_extents(rados).size(), size_t(4 + 37 + 4 * 4));
    CHECK(codec::decode_extents(codec::encode_extents(rados)) == rados);
    std::vector<Extent> cross = {chunk_extent(600, 8192, 1),
                                 {Extent::Kind::kRados, 601, 0, 8192, 2}};
    CHECK(codec::decode_extents(codec::encode_extents(cross)) == cross);

    // Empty = 0-byte object
    CHECK(codec::decode_extents(codec::encode_extents({})).empty());
}

TEST(duostore_value_codec_roundtrip) {
    ObjectRec rec = make_rec("dir/a.txt", {chunk_extent(3, 8192, 7), chunk_extent(4, 55, 8)});
    rec.meta.content_type = "text/plain";
    rec.meta.user_meta = {{"color", "red"}, {"origin", "test"}};
    rec.version = 7;
    auto back = codec::decode_object("dir/a.txt", codec::encode_object(rec));
    CHECK_EQ(back.meta.key, rec.meta.key);
    CHECK_EQ(back.meta.size, rec.meta.size);
    CHECK_EQ(back.meta.etag, rec.meta.etag);
    CHECK_EQ(back.meta.content_type, rec.meta.content_type);
    CHECK(back.meta.user_meta == rec.meta.user_meta);
    CHECK_EQ(codec::to_unix_ms(back.meta.last_modified),
             codec::to_unix_ms(rec.meta.last_modified));
    CHECK_EQ(back.version, rec.version);
    CHECK(back.data.extents == rec.data.extents);

    UploadRec up;
    up.upload_id = "0123456789abcdef0123456789abcdef";
    up.meta.key = "mp/k";
    up.meta.content_type = "application/x-mpu";
    up.meta.user_meta = {{"a", "b"}};
    up.initiated_ms = 1234567890123;
    auto up2 = codec::decode_upload("mp/k", up.upload_id, codec::encode_upload(up));
    CHECK_EQ(up2.meta.content_type, up.meta.content_type);
    CHECK_EQ(up2.initiated_ms, up.initiated_ms);
    CHECK(up2.meta.user_meta == up.meta.user_meta);

    PartRec p;
    p.part_no = 3;
    p.size = 555;
    p.etag = "cafebabe";
    p.modified_ms = 42;
    p.data.extents = {chunk_extent(9, 555, 1)};
    auto p2 = codec::decode_part(3, codec::encode_part(p));
    CHECK_EQ(p2.size, p.size);
    CHECK_EQ(p2.etag, p.etag);
    CHECK_EQ(p2.modified_ms, p.modified_ms);
    CHECK(p2.data.extents == p.data.extents);

    int64_t enq = 0;
    Reclaim r{{chunk_extent(11, 100, 5)}};
    auto r2 = codec::decode_reclaim(codec::encode_reclaim(r, 777), &enq);
    CHECK(r2.extents == r.extents);
    CHECK_EQ(enq, int64_t(777));

    // part key trailing be16 is ascending
    CHECK(codec::part_key("b", "k", "id", 2) < codec::part_key("b", "k", "id", 300));
    CHECK_EQ(codec::part_no_of_key(codec::part_key("b", "k", "id", 300)), 300);
}

// Meta-semantics baseline (GC accounting, monotonic id segments, MPU blocking bucket delete, max-keys=0, delimiter
// pagination): the interfaced suite, RocksMetaStore always runs; the same suite for RedisMetaStore is in test_duostore_redis.cc
TEST(duostore_meta_store_suite_rocksdb) {
    TmpDir tmp;
    meta_store_suite::run_meta_store_suite(
        [&] { return std::make_unique<RocksMetaStore>(meta_opts(tmp)); });
}

// RocksDB implementation detail: the pack counter starts at 0 (the Redis version burns its first segment; the absolute value is an implementation freedom)
TEST(duostore_alloc_pack_counter_starts_at_zero) {
    TmpDir tmp;
    RocksMetaStore m(meta_opts(tmp));
    CHECK_EQ(m.alloc_file_id(Extent::Kind::kPack), uint64_t(0));
    m.close();
}

// decode_object_meta (list's materialization-free path) matches decode_object().meta field by field
TEST(duostore_decode_object_meta_parity) {
    ObjectRec rec = make_rec("k", {chunk_extent(1, 8192, 1), chunk_extent(2, 100, 2)});
    rec.meta.content_type = "text/x-parity";
    rec.meta.user_meta = {{"a", "1"}, {"b", "2"}};
    rec.version = 3;
    auto v = codec::encode_object(rec);
    auto full = codec::decode_object("k", v).meta;
    auto lite = codec::decode_object_meta("k", v);
    CHECK_EQ(lite.key, full.key);
    CHECK_EQ(lite.size, full.size);
    CHECK_EQ(lite.etag, full.etag);
    CHECK_EQ(lite.content_type, full.content_type);
    CHECK(lite.user_meta == full.user_meta);
    CHECK_EQ(codec::to_unix_ms(lite.last_modified), codec::to_unix_ms(full.last_modified));
}

// Canonical parsing of the pack record owner (docs/archive/gaps.md §6.1): three historical forms converge on a single entry point
TEST(duostore_parse_pack_owner_forms) {
    using codec::PackOwner;
    // parse_pack_owner returns string_views into the argument's bytes: the input string must outlive the assertions
    const std::string obj_in("bkt\0some/key", 12);
    auto obj = codec::parse_pack_owner(obj_in);
    CHECK(obj.kind == PackOwner::Kind::kObject);
    CHECK_EQ(std::string(obj.bucket), "bkt");
    CHECK_EQ(std::string(obj.key), "some/key");

    const std::string part_in("mpu\0b\0k\0uid42\0" "7", 15);
    auto part = codec::parse_pack_owner(part_in);
    CHECK(part.kind == PackOwner::Kind::kPart);
    CHECK_EQ(std::string(part.bucket), "b");
    CHECK_EQ(std::string(part.key), "k");
    CHECK_EQ(std::string(part.upload_id), "uid42");
    CHECK_EQ(part.part_no, 7);

    const std::string legacy_in("mpu\0uid42\0" "300", 13);
    auto legacy = codec::parse_pack_owner(legacy_in);
    CHECK(legacy.kind == PackOwner::Kind::kLegacyPart);
    CHECK_EQ(std::string(legacy.upload_id), "uid42");
    CHECK_EQ(legacy.part_no, 300);

    // Malformed inputs are all kUnknown (compaction conservatively does not migrate them): empty string, wrong segment count, non-numeric part_no
    CHECK(codec::parse_pack_owner("").kind == PackOwner::Kind::kUnknown);
    CHECK(codec::parse_pack_owner("solo").kind == PackOwner::Kind::kUnknown);
    CHECK(codec::parse_pack_owner(std::string("mpu\0b\0k\0uid\0x7", 14)).kind ==
          PackOwner::Kind::kUnknown);
    CHECK(codec::parse_pack_owner(std::string("\0k", 2)).kind == PackOwner::Kind::kUnknown);
}

// Schema marker validation (the pure precondition of the migration hook, docs/archive/gaps.md §6.1): equal to current
// passes straight through, newer than this build is rejected (prevents a downgrade silently corrupting writes), garbage is rejected
TEST(duostore_rocks_schema_marker_validation) {
    CHECK_EQ(RocksMetaStore::validate_schema_marker(
                 std::to_string(RocksMetaStore::kSchemaCurrent)),
             RocksMetaStore::kSchemaCurrent);
    CHECK_THROWS_S3(RocksMetaStore::validate_schema_marker(
                        std::to_string(RocksMetaStore::kSchemaCurrent + 1)),
                    s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(RocksMetaStore::validate_schema_marker("banana"),
                    s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(RocksMetaStore::validate_schema_marker("1x"),
                    s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(RocksMetaStore::validate_schema_marker(""),
                    s3::S3ErrorCode::InternalError);
}

// Lineage-prefix semantics of the shared parse_schema_marker check (redis "r" and tikv "t" use the same entry point)
TEST(duostore_schema_marker_lineage_prefixes) {
    CHECK_EQ(parse_schema_marker("r1", "r", 1, "t"), 1);
    CHECK_EQ(parse_schema_marker("t1", "t", 1, "t"), 1);
    CHECK_EQ(parse_schema_marker("r1", "r", 3, "t"), 1);  // an old version passes, handed to the migration chain
    CHECK_THROWS_S3(parse_schema_marker("r2", "r", 1, "t"),
                    s3::S3ErrorCode::InternalError);  // newer than this build
    CHECK_THROWS_S3(parse_schema_marker("t1", "r", 1, "t"),
                    s3::S3ErrorCode::InternalError);  // lineage mismatch
    CHECK_THROWS_S3(parse_schema_marker("r", "r", 1, "t"), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(parse_schema_marker("r-1", "r", 1, "t"), s3::S3ErrorCode::InternalError);
}

// Defense in depth for the '\0'-delimited encoding: a segment containing NUL entering a key constructor must fail loudly (§4.1)
TEST(duostore_codec_rejects_nul_key) {
    std::string nul_key("k\0x", 3);
    CHECK_THROWS_S3(codec::object_key("b", nul_key), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(codec::upload_key("b", "k", nul_key), s3::S3ErrorCode::InternalError);
}

// Cross-chunk write/read and Range (4KiB chunks force a multi-chunk manifest); chunk file layout lands correctly
TEST(duostore_multichunk_roundtrip_and_layout) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "t";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 0;  // this case tests chunk layout specifically (pack specials are below)
    cfg.meta_sync = false;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    sync_wait(b->create_bucket("bkt"));

    std::string data(10000, '\0');
    for (size_t i = 0; i < data.size(); ++i) data[i] = char('a' + i % 26);
    auto pr = put(*b, "bkt", "big", data);

    auto got = sync_wait(b->get_object("bkt", "big", std::nullopt));
    CHECK_EQ(got.meta.size, uint64_t(10000));
    CHECK_EQ(got.meta.etag, pr.etag);
    CHECK_EQ(read_all(*got.body), data);

    // Range across chunk boundaries (covers both cut points, 4096 and 8192)
    auto mid = sync_wait(b->get_object("bkt", "big", ByteRange{4000, 8500}));
    CHECK_EQ(read_all(*mid.body), data.substr(4000, 4501));

    // Layout: 10000B / 4KiB = 3 chunk files land under chunks/<ss>/
    size_t chunk_files = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "duo" / "chunks"))
        if (e.is_regular_file() && e.path().extension() == ".chk") ++chunk_files;
    CHECK_EQ(chunk_files, size_t(3));

    // 0-byte object: empty DataRef
    put(*b, "bkt", "empty", "");
    auto empty = sync_wait(b->get_object("bkt", "empty", std::nullopt));
    CHECK_EQ(empty.meta.size, uint64_t(0));
    CHECK_EQ(read_all(*empty.body), "");

    sync_wait(b->delete_object("bkt", "big"));
    sync_wait(b->delete_object("bkt", "empty"));
    sync_wait(b->delete_bucket("bkt"));
    sync_wait(b->close());
}

// verify_chunk_crc=true: a bitrotted chunk is detected at GET (500) instead of silently serving bad data (§7)
TEST(duostore_get_detects_chunk_bitrot) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "crc";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.pack_threshold = 0;  // take the chunk path: packs always verify crc and have their own case
    cfg.meta_sync = false;
    cfg.verify_chunk_crc = true;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", std::string(1000, 'x'));

    // Inject bitrot: flip one byte in the middle of the only chunk file
    fs::path chunk;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "duo" / "chunks"))
        if (e.is_regular_file() && e.path().extension() == ".chk") chunk = e.path();
    CHECK(!chunk.empty());
    {
        std::fstream f(chunk, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(500);
        f.put('y');
    }
    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    CHECK_THROWS_S3(read_all(*got.body), s3::S3ErrorCode::InternalError);
    sync_wait(b->close());
}

// ---------- GC phase-one specials (§9/§15 P3 acceptance: convergence after overwrite/delete/abort + GET vs GC) ----------

namespace {

// Uniform config for the GC specials: 4KiB chunks force multi-chunk, pack disabled (chunk unlink semantics tested
// specifically; pack-side GC has its own cases), grace=0 for immediate reclaimability, background worker off (tests the manual hook specifically)
DuoStoreConfig gc_cfg(const TmpDir& tmp, const char* name) {
    DuoStoreConfig cfg;
    cfg.name = name;
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 0;
    cfg.meta_sync = false;
    cfg.gc_interval_sec = 0;
    cfg.gc_grace_sec = 0;
    return cfg;
}

size_t chunk_files_on_disk(const fs::path& root) {
    size_t n = 0;
    std::error_code ec;
    fs::recursive_directory_iterator it(root / "chunks", ec), end;
    for (; !ec && it != end; it.increment(ec))
        if (it->is_regular_file() && it->path().extension() == ".chk") ++n;
    return n;
}

std::string patterned(size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) s[i] = char('a' + i % 26);
    return s;
}

}  // namespace

// run_gc_once converges after overwrite + delete: chunks physically disappear, gcq drains, another round performs zero actions
TEST(duostore_gc_reclaims_after_overwrite_and_delete) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    put(*b, "bkt", "k", patterned(10000));  // 3 chunks
    put(*b, "bkt", "k", patterned(5000));   // overwrite: old 3 chunks enter gcq, new 2 chunks
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(5));

    auto st1 = sync_wait(b->run_gc_once());
    CHECK_EQ(st1.reclaims_acked, uint64_t(1));
    CHECK_EQ(st1.files_removed, uint64_t(3));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(2));
    {
        // The live version is unaffected (read and destroyed within the scope -- the reader holds a pin while alive)
        auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
        CHECK_EQ(read_all(*got.body), patterned(5000));
    }
    sync_wait(b->delete_object("bkt", "k"));
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(st2.files_removed, uint64_t(2));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));

    // Convergence: another round on the empty gcq performs zero actions
    auto st3 = sync_wait(b->run_gc_once());
    CHECK_EQ(st3.reclaims_acked, uint64_t(0));
    CHECK_EQ(st3.files_removed, uint64_t(0));
    sync_wait(b->close());
}

// Backend-level metrics: GC counts land in the registry via MetricsScope,
// the backend label comes from the assembly side; the direct-construction (default empty scope) path is covered by the other GC cases
TEST(duostore_gc_metrics_registered) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gcm");
    auto reg = std::make_shared<MetricsRegistry>();
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool,
                                               MetricsScope(reg, {{"backend", "gcm"}}));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));  // 3 chunks
    sync_wait(b->delete_object("bkt", "k"));
    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.reclaims_acked, uint64_t(1));

    auto out = reg->render();
    CHECK(out.find("lights3_duostore_gc_runs_total{backend=\"gcm\"} 1\n") != std::string::npos);
    CHECK(out.find("lights3_duostore_gc_reclaims_total{backend=\"gcm\"} 1\n") !=
          std::string::npos);
    CHECK(out.find("lights3_duostore_gc_files_removed_total{backend=\"gcm\"} 3\n") !=
          std::string::npos);
    sync_wait(b->close());
}

// gc_grace defense in depth (§7/§9.1): items not yet past the grace period are skipped -- no ack, files kept
TEST(duostore_gc_grace_defers_reclaim) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-grace");
    cfg.gc_grace_sec = 3600;
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(100));
    sync_wait(b->delete_object("bkt", "k"));

    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.skipped_grace, uint64_t(1));
    CHECK_EQ(st.reclaims_acked, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(1));
    sync_wait(b->close());
}

// Concurrent GET vs GC (§7 pin counting; §15 P3 acceptance "no ENOENT"): the object is deleted mid-read, the pin
// keeps GC from unlinking; the full content is verified after reading; the next round reclaims once the reader's destruction releases the pin
TEST(duostore_gc_pin_blocks_unlink_during_get) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-pin");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    std::string body = patterned(10000);
    put(*b, "bkt", "k", body);

    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    // Read a small piece (the first chunk is open), later chunks rely on lazy opening -- exactly the window the pin protects
    std::byte buf[100];
    size_t n0 = sync_wait(got.body->read(std::span(buf)));
    CHECK(n0 > 0);

    sync_wait(b->delete_object("bkt", "k"));
    auto st1 = sync_wait(b->run_gc_once());
    CHECK_EQ(st1.skipped_pinned, uint64_t(1));
    CHECK_EQ(st1.reclaims_acked, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(3));

    // The remainder reads out intact, no ENOENT / 500
    std::string rest = read_all(*got.body);
    CHECK_EQ(std::string(reinterpret_cast<char*>(buf), n0) + rest, body);

    got.body.reset();  // destruction releases the pin
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    sync_wait(b->close());
}

// GC converges after abort + mpu_ttl expiry cleanup (end of §8): an expired upload is aborted internally, its parts
// are reclaimed in the same round; the aborted upload_id is cleanly invalidated
TEST(duostore_gc_mpu_ttl_expiry) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-mpu");
    cfg.mpu_ttl_sec = 1;  // smallest positive ttl (0 = cleanup disabled, not "expire immediately")
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    auto id = sync_wait(b->create_multipart("bkt", "mpu", {}));
    {
        http::StringBodyReader part(patterned(6000));  // 2 chunks
        sync_wait(b->upload_part("bkt", "mpu", id, 1, part));
    }
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(2));

    usleep(1100 * 1000);  // past the 1s ttl
    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.uploads_expired, uint64_t(1));
    // Parts entered into the gcq by the abort are consumed in the same round (mpu cleanup precedes gcq consumption)
    CHECK_EQ(st.reclaims_acked, uint64_t(1));
    CHECK_EQ(st.files_removed, uint64_t(2));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    CHECK_EQ(sync_wait(b->list_multipart_uploads("bkt", {})).uploads.size(), size_t(0));
    {
        http::StringBodyReader part("x");
        CHECK_THROWS_S3(sync_wait(b->upload_part("bkt", "mpu", id, 2, part)),
                        s3::S3ErrorCode::NoSuchUpload);
    }
    sync_wait(b->close());
}

// mpu_ttl=0 = cleanup disabled (aligned with gc_interval's 0 semantics); a non-expired upload is unaffected either way
TEST(duostore_gc_mpu_fresh_upload_survives) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-mpu-fresh");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    sync_wait(b->create_multipart("bkt", "mpu", {}));

    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.uploads_expired, uint64_t(0));
    CHECK_EQ(sync_wait(b->list_multipart_uploads("bkt", {})).uploads.size(), size_t(1));
    sync_wait(b->close());

    // ttl=0: cleanup entirely off, even an "already expired" upload is untouched
    TmpDir tmp2;
    auto cfg2 = gc_cfg(tmp2, "gc-mpu-off");
    cfg2.mpu_ttl_sec = 0;
    auto b2 = std::make_shared<DuoStoreBackend>(cfg2, pool);
    sync_wait(b2->create_bucket("bkt"));
    sync_wait(b2->create_multipart("bkt", "mpu", {}));
    auto st2 = sync_wait(b2->run_gc_once());
    CHECK_EQ(st2.uploads_expired, uint64_t(0));
    CHECK_EQ(sync_wait(b2->list_multipart_uploads("bkt", {})).uploads.size(), size_t(1));
    sync_wait(b2->close());
}

// gcq resumable scan (§9.1): a fully pinned batch at the queue head does not stall the whole round -- the backlog
// behind it still gets reclaimed, and skipped items are counted only once
TEST(duostore_gc_skipped_head_does_not_stall_round) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-stall");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    // 257 single-chunk objects: readers hold pins on the first 256 (filling a full peek batch), the 257th is reclaimable
    constexpr int kN = 257;
    for (int i = 0; i < kN; ++i)
        put(*b, "bkt", "k" + std::to_string(i), patterned(64));
    std::vector<ObjectStream> readers;
    for (int i = 0; i < kN - 1; ++i)
        readers.push_back(sync_wait(b->get_object("bkt", "k" + std::to_string(i), std::nullopt)));
    for (int i = 0; i < kN; ++i) sync_wait(b->delete_object("bkt", "k" + std::to_string(i)));

    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.skipped_pinned, uint64_t(kN - 1));  // each item counted only once
    CHECK_EQ(st.reclaims_acked, uint64_t(1));       // a fully pinned head does not block reclaiming the tail
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(kN - 1));

    readers.clear();  // full convergence once unpinned
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(kN - 1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    sync_wait(b->close());
}

// Background worker (§9): reclaims automatically on a gc_interval=1s cadence; close cancels the timer and waits for in-flight GC
TEST(duostore_gc_background_worker_runs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-worker");
    cfg.gc_interval_sec = 1;
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));
    sync_wait(b->delete_object("bkt", "k"));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(3));

    // Wait at most 15s to converge (normally 1-2 cycles)
    bool converged = false;
    for (int i = 0; i < 150 && !converged; ++i) {
        if (chunk_files_on_disk(cfg.root) == 0) converged = true;
        else usleep(100 * 1000);
    }
    CHECK(converged);
    sync_wait(b->close());
}

// Lifecycle: a far-future timer is cleanly cancelled by close; destructing without close takes the dtor fallback --
// neither path may hang or use-after-free (covered by the asan/tsan matrix)
TEST(duostore_gc_close_and_dtor_cancel_worker) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    {
        auto cfg = gc_cfg(tmp, "gc-close");
        cfg.gc_interval_sec = 300;
        auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
        sync_wait(b->create_bucket("bkt"));
        sync_wait(b->close());
    }
    {
        auto cfg = gc_cfg(tmp, "gc-dtor");
        cfg.root = tmp.path / "duo2";
        cfg.meta_path = cfg.root / "meta";
        cfg.gc_interval_sec = 300;
        DuoStoreBackend b(cfg, pool);  // dtor fallback path
    }
}

// close concurrent with the manual run_gc_once: the manual hook registers in-flight via the wait group, and close
// waits for it before tearing down meta/data; after close the manual hook refuses entry and returns zero stats (verified UAF-free under the asan/tsan matrix)
TEST(duostore_gc_manual_hook_vs_close) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-race");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));
    sync_wait(b->delete_object("bkt", "k"));

    std::thread gc([&] {
        for (int i = 0; i < 50; ++i) sync_wait(b->run_gc_once());
    });
    std::thread closer([&] { sync_wait(b->close()); });
    gc.join();
    closer.join();
    auto st = sync_wait(b->run_gc_once());  // already closed: refuses entry
    CHECK_EQ(st.reclaims_acked, uint64_t(0));
}

// FsDataStore::remove_pack: whole-pack-file deletion is idempotent (§9.1; file crafted by hand, not via the write path)
TEST(duostore_fs_remove_pack_idempotent) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    uint64_t next_id = 1;
    FsDataStore d(FsDataOptions{tmp.path / "duo", 4096, false, 0, 128ull << 20, 4, {}}, pool,
                  [&](Extent::Kind, uint32_t n) { uint64_t f = next_id; next_id += n; return f; });
    auto p = d.pack_path(7);
    fs::create_directories(p.parent_path());
    std::ofstream(p) << "stub";
    CHECK(fs::exists(p));
    sync_wait(d.remove_pack(7));
    CHECK(!fs::exists(p));
    sync_wait(d.remove_pack(7));  // double delete is idempotent (ENOENT ignored)
    sync_wait(d.close());
}

// ---------- P2 pack aggregation specials (§5.2/§5.3/§14) ----------

namespace {

// Uniform config for the pack specials: 1KiB threshold + single writer (deterministic layout assertions); 4KiB chunks;
// manual GC hook, grace=0
DuoStoreConfig pack_cfg(const TmpDir& tmp, const char* name) {
    DuoStoreConfig cfg;
    cfg.name = name;
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 1024;
    cfg.pack_max_size = 64 << 10;
    cfg.pack_writers = 1;
    cfg.pack_max_age_sec = 0;  // age rotation off by default: layout assertions are only deterministic with capacity-based rotation
    cfg.meta_sync = false;
    cfg.gc_interval_sec = 0;
    cfg.gc_grace_sec = 0;
    return cfg;
}

size_t pack_files_on_disk(const fs::path& root) {
    size_t n = 0;
    std::error_code ec;
    fs::recursive_directory_iterator it(root / "packs", ec), end;
    for (; !ec && it != end; it.increment(ec))
        if (it->is_regular_file() && it->path().extension() == ".pak") ++n;
    return n;
}

std::string read_file(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

fs::path sole_pack_file(const fs::path& root) {
    fs::path found;
    for (auto& e : fs::recursive_directory_iterator(root / "packs"))
        if (e.is_regular_file() && e.path().extension() == ".pak") found = e.path();
    return found;
}

// Unknown-length stream (simulated chunked PUT): length() is always nullopt, forcing the buffering/routing path (§5.3)
class UnknownLenReader final : public http::BodyReader {
public:
    explicit UnknownLenReader(std::string data) : data_(std::move(data)) {}
    std::optional<uint64_t> length() const override { return std::nullopt; }
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = std::min(buf.size(), data_.size() - off_);
        std::memcpy(buf.data(), data_.data() + off_, n);
        off_ += n;
        co_return n;
    }

private:
    std::string data_;
    size_t off_ = 0;
};

// Injected assembly: keeps a raw meta pointer to assert pack liveness accounting (the backend owns it)
struct PackHarness {
    std::shared_ptr<DuoStoreBackend> b;
    RocksMetaStore* meta = nullptr;  // lifetime follows b
};

PackHarness make_pack_backend(const DuoStoreConfig& cfg, std::shared_ptr<ThreadPool> pool) {
    fs::create_directories(cfg.root);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{cfg.meta_path.string(), /*sync=*/false, 8ull << 20});
    auto* mp = meta.get();
    // Migration callback matches the cfg-constructed assembly (the standard migrate_pack_record implementation); the
    // write-side pin hook is not wired -- the compaction cases' migration payloads are <= the threshold and always take the pack path, so passing empty pins suffices
    auto data = std::make_unique<FsDataStore>(
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                      cfg.pack_max_size, cfg.pack_writers, cfg.pack_max_age_sec, {}},
        pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); },
        [mp](IDataStore& ds, std::vector<PackScanRecord>&& batch) {
            return migrate_pack_records(*mp, ds, nullptr, std::move(batch));
        });
    PackHarness h;
    h.meta = mp;
    h.b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    return h;
}

std::optional<PackStat> find_pack_stat(IMetaStore& m, uint64_t pack_id) {
    for (const auto& ps : m.pack_stats())
        if (ps.pack_id == pack_id) return ps;
    return std::nullopt;
}

}  // namespace

// Routing + layout + record format (§5.2): <= threshold appends into the same active pack (zero chunks),
// magic/owner persisted, > threshold takes the chunk path, GET full and Range, liveness accounting accumulates with writes
TEST(duostore_pack_layout_roundtrip_and_stats) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    std::string d1 = patterned(600), d2 = patterned(500);
    put(*h.b, "bkt", "k1", d1);
    put(*h.b, "bkt", "k2", d2);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));  // appended into the same active pack
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));

    // Record format on disk: the file starts with the "LP3R" magic, owner = "bucket\0key" embedded
    std::string raw = read_file(sole_pack_file(cfg.root));
    CHECK_EQ(raw.substr(0, 4), "LP3R");
    CHECK(raw.find(std::string("bkt\0k1", 6)) != std::string::npos);
    CHECK(raw.find(std::string("bkt\0k2", 6)) != std::string::npos);

    // Read back: full + a Range unrelated to record boundaries (slicing within the payload)
    auto g1 = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));
    CHECK_EQ(read_all(*g1.body), d1);
    auto g2 = sync_wait(h.b->get_object("bkt", "k2", ByteRange{100, 299}));
    CHECK_EQ(read_all(*g2.body), d2.substr(100, 200));

    // Liveness accounting (meta side): 2 records, payload 1100B + a 28B record header each (22 fixed +
    // "bkt\0kN" owner, §2.3a same accounting basis as file_size), not sealed
    auto rec = h.meta->get_object("bkt", "k1");
    CHECK(rec.has_value());
    CHECK_EQ(size_t(rec->data.extents.size()), size_t(1));
    uint64_t pid = rec->data.extents[0].file_id;
    CHECK(rec->data.extents[0].kind == Extent::Kind::kPack);
    auto ps = find_pack_stat(*h.meta, pid);
    CHECK(ps.has_value());
    CHECK_EQ(ps->live_bytes, int64_t(1156));
    CHECK_EQ(ps->live_recs, int64_t(2));
    CHECK(!ps->sealed);

    // Larger than the threshold -> chunk path (nothing new in the pack)
    std::string big = patterned(2000);
    put(*h.b, "bkt", "big", big);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(1));
    auto g3 = sync_wait(h.b->get_object("bkt", "big", std::nullopt));
    CHECK_EQ(read_all(*g3.body), big);

    // Small multipart parts also go into the pack, owner = "mpu\0<b>\0<k>\0<id>\0<no>" (P4 §9.2:
    // only with b/k can the owning object be looked up after complete)
    auto id = sync_wait(h.b->create_multipart("bkt", "mp", {}));
    {
        http::StringBodyReader body(patterned(300));
        sync_wait(h.b->upload_part("bkt", "mp", id, 1, body));
    }
    raw = read_file(sole_pack_file(cfg.root));
    CHECK(raw.find(std::string("mpu\0bkt\0mp\0", 11) + id) != std::string::npos);
    sync_wait(h.b->abort_multipart("bkt", "mp", id));
    sync_wait(h.b->close());
}

// Buffered routing of a chunked (unknown-length) PUT (§5.3): exactly == the threshold goes into the pack whole;
// over the threshold the buffer spills to disk and switches to the streaming chunk path, content intact
TEST(duostore_pack_chunked_put_buffer_and_spill) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-chunked");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    std::string exact = patterned(1024);  // == pack_threshold: goes into the pack whole at EOF
    {
        UnknownLenReader body(exact);
        sync_wait(h.b->put_object("bkt", "fit", {}, body));
    }
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    auto g1 = sync_wait(h.b->get_object("bkt", "fit", std::nullopt));
    CHECK_EQ(read_all(*g1.body), exact);

    std::string spill = patterned(10000);  // over the threshold: buffer spills to disk + switches to chunks (3×4KiB)
    {
        UnknownLenReader body(spill);
        sync_wait(h.b->put_object("bkt", "spill", {}, body));
    }
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(3));
    auto g2 = sync_wait(h.b->get_object("bkt", "spill", std::nullopt));
    CHECK_EQ(read_all(*g2.body), spill);
    auto g3 = sync_wait(h.b->get_object("bkt", "spill", ByteRange{4000, 8500}));
    CHECK_EQ(read_all(*g3.body), spill.substr(4000, 4501));
    sync_wait(h.b->close());
}

// Rotation sealing (§5.2): sealed on reaching pack_max_size (file_size reported), switches to a new pack_id;
// close() seals the remaining active pack
TEST(duostore_pack_rotation_seals_and_close_seals_rest) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-rot");
    cfg.pack_max_size = 2048;  // record ≈ 600+29 -> each pack fills at 3 records
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    for (int i = 0; i < 4; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));  // 3 + 1 distribution

    auto stats = h.meta->pack_stats();
    CHECK_EQ(stats.size(), size_t(2));
    size_t sealed = 0, active = 0;
    for (const auto& ps : stats) {
        if (ps.sealed) {
            ++sealed;
            CHECK(ps.file_size > 0);  // rotation sealing reports the final file size
            CHECK_EQ(ps.live_recs, int64_t(3));
        } else {
            ++active;
            CHECK_EQ(ps.live_recs, int64_t(1));
        }
    }
    CHECK_EQ(sealed, size_t(1));
    CHECK_EQ(active, size_t(1));
    for (int i = 0; i < 4; ++i) {  // all readable across packs
        auto g = sync_wait(h.b->get_object("bkt", "k" + std::to_string(i), std::nullopt));
        CHECK_EQ(read_all(*g.body), patterned(600));
    }

    IMetaStore* mp = h.meta;
    sync_wait(h.b->close());  // seals the remaining active pack (§9 lifecycle: within close, data before meta)
    (void)mp;                 // meta is closed after close; re-checking the accounting is left to the restart cases
}

// Pack records always verify crc (§7): payload bitrot is detected at GET (500), independent of the
// verify_chunk_crc switch
TEST(duostore_pack_get_detects_bitrot) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-crc");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k", patterned(600));

    auto p = sole_pack_file(cfg.root);
    CHECK(!p.empty());
    {
        std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(-10, std::ios::end);  // payload tail (the header is at the start of the file)
        f.put('!');
    }
    auto got = sync_wait(h.b->get_object("bkt", "k", std::nullopt));
    CHECK_THROWS_S3(read_all(*got.body), s3::S3ErrorCode::InternalError);
    sync_wait(h.b->close());
}

// P5 corruption metric: crc mismatches on the GET read path land in the registry via the on_corruption callback --
// the chunk and pack integration points each count once (cfg-constructed assembly; the injected assembly does not wire the hook, so the other bitrot cases do not count)
TEST(duostore_read_corruption_metric) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "corrupt");
    cfg.verify_chunk_crc = true;
    auto reg = std::make_shared<MetricsRegistry>();
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool,
                                               MetricsScope(reg, {{"backend", "corrupt"}}));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "small", patterned(600));  // <= threshold: pack record
    put(*b, "bkt", "big", patterned(5000));   // > threshold: 2 chunks (chunk_size 4KiB)
    CHECK(reg->render().find(
              "lights3_duostore_read_corruption_total{backend=\"corrupt\"} 0\n") !=
          std::string::npos);  // registered at construction, zero value visible

    fs::path chunk;
    for (auto& e : fs::recursive_directory_iterator(cfg.root / "chunks"))
        if (e.is_regular_file() && e.path().extension() == ".chk") chunk = e.path();
    CHECK(!chunk.empty());
    {
        std::fstream f(chunk, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(100);
        f.put('!');
    }
    auto got1 = sync_wait(b->get_object("bkt", "big", std::nullopt));
    CHECK_THROWS_S3(read_all(*got1.body), s3::S3ErrorCode::InternalError);

    auto p = sole_pack_file(cfg.root);
    CHECK(!p.empty());
    {
        std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(-10, std::ios::end);  // payload tail (the header is at the start of the record)
        f.put('!');
    }
    auto got2 = sync_wait(b->get_object("bkt", "small", std::nullopt));
    CHECK_THROWS_S3(read_all(*got2.body), s3::S3ErrorCode::InternalError);

    CHECK(reg->render().find(
              "lights3_duostore_read_corruption_total{backend=\"corrupt\"} 2\n") !=
          std::string::npos);
    sync_wait(b->close());
}

// P5 exposed RocksDB tuning: size/integer parsing + range validation (>=1)
TEST(duostore_config_rocksdb_tuning_params) {
    std::map<std::string, std::string> p{{"root", "/tmp/duo-cfg"},
                                         {"rocksdb_write_buffer", "8MiB"},
                                         {"rocksdb_max_write_buffers", "3"},
                                         {"rocksdb_max_background_jobs", "4"}};
    auto c = DuoStoreConfig::from_params("t", p);
    CHECK_EQ(c.rocksdb_write_buffer, size_t(8) << 20);
    CHECK_EQ(c.rocksdb_max_write_buffers, 3);
    CHECK_EQ(c.rocksdb_max_background_jobs, 4);

    p["rocksdb_max_write_buffers"] = "0";
    bool threw = false;
    try {
        DuoStoreConfig::from_params("t", p);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

// Single-instance execution gating for multiple gateways (C4, docs/duostore-rados-data.md §8.3): gc_enabled=false
// only stops the scheduling of the background worker/orphan scan; the manual hooks (test/ops channel) are not gated
TEST(duostore_config_gc_enabled_gates_background_only) {
    std::map<std::string, std::string> p{{"root", "/tmp/duo-cfg"}, {"gc_enabled", "false"}};
    auto c = DuoStoreConfig::from_params("t", p);
    CHECK(!c.gc_enabled);
    CHECK(DuoStoreConfig::from_params("t", {{"root", "/tmp/duo-cfg"}}).gc_enabled);  // on by default
    p["gc_enabled"] = "not-a-bool";
    bool threw = false;
    try {
        DuoStoreConfig::from_params("t", p);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    // Behavior: gc_enabled=false + gc_interval>0 still schedules no background worker; the manual hook reclaims as usual
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-gated");
    cfg.gc_enabled = false;
    cfg.gc_interval_sec = 3600;
    cfg.orphan_scan_interval_sec = 3600;
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(5000));
    sync_wait(b->delete_object("bkt", "k"));
    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.reclaims_acked, uint64_t(1));
    CHECK_EQ(st.files_removed, uint64_t(2));  // 5000B / 4KiB = 2 chunks
    auto ost = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(ost.orphans_removed, uint64_t(0));
    sync_wait(b->close());
}

// Whole-empty-pack deletion (§9.1): sealed and live_recs==0 -> unlink + clear the packstat; the pin blocks whole
// deletion. Reclaiming pack records from the gcq does not count files_removed (dead space is reclaimed by compaction)
TEST(duostore_pack_gc_empty_pack_removal_respects_pin) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-gc");
    cfg.pack_max_size = 1024;  // one record fills it: writing the second object seals the first pack
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));  // pack P1
    put(*h.b, "bkt", "k2", patterned(600));  // P1 sealed, P2 active
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    uint64_t p1 = h.meta->get_object("bkt", "k1")->data.extents[0].file_id;
    auto got = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));  // holds a pin
    sync_wait(h.b->delete_object("bkt", "k1"));  // live_recs(P1) -> 0

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.skipped_pinned, uint64_t(1));  // the gcq item is blocked by the pin
    CHECK_EQ(st1.packs_removed, uint64_t(0));   // whole-empty-pack deletion is blocked by the pin as well
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));
    CHECK_EQ(read_all(*got.body), patterned(600));  // the reader is unaffected

    got.body.reset();  // release the pin
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(st2.files_removed, uint64_t(0));  // pack records do not count as physical deletions
    CHECK_EQ(st2.packs_removed, uint64_t(1));  // whole-file unlink
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK(!find_pack_stat(*h.meta, p1).has_value());  // packstat cleared
    sync_wait(h.b->close());
}

// Restart abandons the active pack (§5.2): destructing without close (crash-equivalent) -> after reopening the same
// root, the previous generation's active pack is back-sealed (sealed/size unknown=0), old objects stay readable, new
// writes go to a new pack, and once the old objects are all deleted the whole pack is reclaimed
TEST(duostore_pack_restart_abandons_active) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-restart");
    {
        auto h = make_pack_backend(cfg, pool);
        sync_wait(h.b->create_bucket("bkt"));
        put(*h.b, "bkt", "old", patterned(600));
        CHECK(!h.meta->pack_stats()[0].sealed);
        // No close: dtor fallback (the active pack is not sealed, equivalent to crash leftovers)
    }
    {
        // Torn tail injection (§5.2/§6.2): a crash may leave half a record mid-append --
        // unreferenced means dead space, and it must not affect reading committed objects
        std::ofstream f(sole_pack_file(cfg.root), std::ios::binary | std::ios::app);
        f << "LP3R" << std::string(7, '\x5a');  // magic + truncated header
    }
    auto h = make_pack_backend(cfg, pool);
    auto stats = h.meta->pack_stats();
    CHECK_EQ(stats.size(), size_t(1));
    CHECK(stats[0].sealed);  // back-sealed at construction (abandon_stale_packs)
    CHECK_EQ(stats[0].file_size, uint64_t(0));  // size unknown, 0 placeholder
    uint64_t p1 = stats[0].pack_id;

    auto g = sync_wait(h.b->get_object("bkt", "old", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));  // the old pack is merely abandoned, reads are unaffected
    g.body.reset();

    put(*h.b, "bkt", "fresh", patterned(600));  // a new write opens a new pack (does not reuse the old active)
    uint64_t p2 = h.meta->get_object("bkt", "fresh")->data.extents[0].file_id;
    CHECK(p2 != p1);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    sync_wait(h.b->delete_object("bkt", "old"));
    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_removed, uint64_t(1));  // only after back-sealing can the old pack become a whole-deletion candidate
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    auto g2 = sync_wait(h.b->get_object("bkt", "fresh", std::nullopt));
    CHECK_EQ(read_all(*g2.body), patterned(600));
    sync_wait(h.b->close());
}

// ---------- P4 compaction specials (§9.2/§15 P4 acceptance: low-liveness compaction all green) ----------

namespace {

// Pack file path (matches the FsDataStore layout convention; computed directly in the test to avoid holding a store pointer)
fs::path pack_file_path(const fs::path& root, uint64_t pack_id) {
    char ss[3], name[32];
    std::snprintf(ss, sizeof ss, "%02x", unsigned((pack_id >> 8) & 0xff));
    std::snprintf(name, sizeof name, "%016llx.pak", (unsigned long long)pack_id);
    return root / "packs" / ss / name;
}

void corrupt_file_at(const fs::path& p, uint64_t off) {
    std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(std::streamoff(off));
    char c = 0;
    f.get(c);
    f.seekp(std::streamoff(off));
    f.put(char(c ^ 0x5a));
}

}  // namespace

// Low-liveness compaction converges: high liveness does not trigger it; once below pack_gc_ratio, a sequential scan
// migrates live records to the active pack, swap replaces the ref, and the empty pack is whole-deleted in the same round (grace=0); object reads are seamless
TEST(duostore_compact_low_liveness_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact");
    cfg.pack_max_size = 2048;  // 600B record ≈ 629B, fills and rotates at 3 records
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    for (int i = 0; i < 4; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));  // P1 sealed (k0-k2), P2 active (k3)

    uint64_t p1 = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;

    // Liveness 3/3 and 2/3 (0.64 >= 0.5) both do not trigger compaction
    auto st0 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st0.packs_compacted, uint64_t(0));
    sync_wait(h.b->delete_object("bkt", "k0"));
    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(0));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    // Liveness 1/3 (0.32 < 0.5) -> compaction: k2 migrates, P1 empties, whole-deleted in the same round
    sync_wait(h.b->delete_object("bkt", "k1"));
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.packs_compacted, uint64_t(1));
    CHECK_EQ(st2.records_migrated, uint64_t(1));
    CHECK_EQ(st2.records_corrupt, uint64_t(0));
    CHECK_EQ(st2.packs_removed, uint64_t(1));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK(!find_pack_stat(*h.meta, p1).has_value());  // packstat cleared

    // After the ref swap, k2 points at the new pack and is byte-for-byte correct; k3 is undisturbed
    auto rec = h.meta->get_object("bkt", "k2");
    CHECK(rec.has_value());
    CHECK(rec->data.extents[0].file_id != p1);
    auto g = sync_wait(h.b->get_object("bkt", "k2", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    auto g3 = sync_wait(h.b->get_object("bkt", "k3", std::nullopt));
    CHECK_EQ(read_all(*g3.body), patterned(600));

    // Convergence: another round performs zero actions
    auto st3 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st3.packs_compacted, uint64_t(0));
    CHECK_EQ(st3.packs_removed, uint64_t(0));
    sync_wait(h.b->close());
}

// Age rotation (docs/archive/gaps.md §6.1): under low write volume an active pack sealed only by capacity never rotates,
// and its dead space can never enter the compaction candidate set. seal_aged_packs seals over-age active packs,
// reporting file_size truthfully (not the crash back-seal's 0), after which the dead space can be reclaimed by compaction
TEST(duostore_pack_age_rotation_seals_idle_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "age");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    for (int i = 0; i < 3; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));

    uint64_t pid = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;
    auto before = find_pack_stat(*h.meta, pid);
    CHECK(before.has_value());
    CHECK(!before->sealed);  // far from pack_max_size: the capacity criterion would never seal it

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK_EQ(sync_wait(h.b->data_for_test().seal_aged_packs(10)), uint64_t(1));
    auto after = find_pack_stat(*h.meta, pid);
    CHECK(after.has_value());
    CHECK(after->sealed);
    CHECK(after->file_size > 0);  // reported truthfully, not the restart back-seal's "unknown 0"
    CHECK_EQ(after->live_recs, int64_t(3));

    // Idempotent: returns 0 when there is no active pack to seal
    CHECK_EQ(sync_wait(h.b->data_for_test().seal_aged_packs(10)), uint64_t(0));

    // After sealing the dead space is reclaimable: deleting down to 1/3 liveness drops below pack_gc_ratio, compaction converges as usual
    sync_wait(h.b->delete_object("bkt", "k0"));
    sync_wait(h.b->delete_object("bkt", "k1"));
    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_compacted, uint64_t(1));
    CHECK_EQ(st.records_migrated, uint64_t(1));
    auto g = sync_wait(h.b->get_object("bkt", "k2", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    sync_wait(h.b->close());
}

// Wiring inside the GC round: once pack_max_age is due, run_gc_once seals by itself (no write needed to trigger it)
TEST(duostore_pack_age_rotation_runs_in_gc) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "agegc");
    cfg.pack_max_age_sec = 1;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k0", patterned(600));
    uint64_t pid = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;

    CHECK_EQ(sync_wait(h.b->run_gc_once()).packs_sealed_aged, uint64_t(0));  // not due yet
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    CHECK_EQ(sync_wait(h.b->run_gc_once()).packs_sealed_aged, uint64_t(1));
    CHECK(find_pack_stat(*h.meta, pid)->sealed);
    sync_wait(h.b->close());
}

// Compaction budget and priority (docs/archive/gaps.md §6.1): capped at N per round, taken in descending order of
// reclaimable bytes -- previously it was "rewrite every eligible pack in one round", and after a bulk delete a single round could hold the lock for hours
TEST(duostore_compact_budget_prioritises_by_reclaimable) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "budget");
    cfg.pack_max_size = 2048;          // 600B record ≈ 629B, fills and rotates at 3 records
    cfg.gc_compact_max_packs = 1;      // only one per round
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    // Three full packs (k0-k2 / k3-k5 / k6-k8) + one active (k9)
    for (int i = 0; i < 10; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));
    uint64_t p0 = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;
    uint64_t p1 = h.meta->get_object("bkt", "k3")->data.extents[0].file_id;
    uint64_t p2 = h.meta->get_object("bkt", "k6")->data.extents[0].file_id;
    CHECK(p0 != p1 && p1 != p2);

    // p0 keeps 2 live records, p1 keeps 1, p2 keeps 1 -- all three qualify, but reclaimable bytes p1/p2 > p0
    sync_wait(h.b->delete_object("bkt", "k0"));
    sync_wait(h.b->delete_object("bkt", "k1"));
    sync_wait(h.b->delete_object("bkt", "k3"));
    sync_wait(h.b->delete_object("bkt", "k4"));
    sync_wait(h.b->delete_object("bkt", "k6"));
    sync_wait(h.b->delete_object("bkt", "k7"));

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(1));       // budget cap
    CHECK_EQ(st1.packs_compact_deferred, uint64_t(2));  // the rest are deferred

    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.packs_compacted, uint64_t(1));
    CHECK_EQ(st2.packs_compact_deferred, uint64_t(1));

    auto st3 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st3.packs_compacted, uint64_t(1));
    CHECK_EQ(st3.packs_compact_deferred, uint64_t(0));

    // Data intact after convergence
    auto st4 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st4.packs_compacted, uint64_t(0));
    for (int i : {2, 5, 8, 9}) {
        auto g = sync_wait(h.b->get_object("bkt", "k" + std::to_string(i), std::nullopt));
        CHECK_EQ(read_all(*g.body), patterned(600));
    }
    sync_wait(h.b->close());
}

// A corrupt dead record does not block compaction (§10): the liveness accounting proves the corrupt one is dead --
// the survivors migrate as usual, and once live reaches zero the empty pack (corrupt dead space included) is whole-deleted without losing any data
TEST(duostore_compact_corrupt_dead_record) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-cdead");
    cfg.pack_max_size = 1400;  // fills at 2 records (the 3rd triggers rotation sealing)
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));
    put(*h.b, "bkt", "k2", patterned(600));
    auto old2 = h.meta->get_object("bkt", "k2")->data.extents[0];  // P1 location before the overwrite
    put(*h.b, "bkt", "k2", patterned(500));  // 3rd record -> P1 sealed, the new value goes to P2; old k2 becomes dead space
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    corrupt_file_at(pack_file_path(cfg.root, old2.file_id), old2.offset + 10);  // corrupt the dead space
    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_compacted, uint64_t(1));
    CHECK_EQ(st.records_corrupt, uint64_t(1));   // corrupt dead space: warn and skip
    CHECK_EQ(st.records_migrated, uint64_t(1));  // live k1 migrates as usual
    CHECK_EQ(st.packs_removed, uint64_t(1));     // live reaches zero -> whole-delete the empty pack (dead space included)
    auto g1 = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));
    CHECK_EQ(read_all(*g1.body), patterned(600));
    auto g2 = sync_wait(h.b->get_object("bkt", "k2", std::nullopt));
    CHECK_EQ(read_all(*g2.body), patterned(500));
    sync_wait(h.b->close());
}

// A corrupt live record -> cannot migrate, live never reaches zero -> the original pack is kept, not deleted (manual
// intervention, §10); no accounting progress + no re-scan within the cooldown window (compact_blocked memory)
TEST(duostore_compact_corrupt_live_record_keeps_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-clive");
    cfg.pack_max_size = 1400;
    cfg.gc_grace_sec = 3600;  // cooldown window in effect (also used to verify the re-scan is skipped)
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));
    put(*h.b, "bkt", "k2", patterned(600));
    put(*h.b, "bkt", "filler", patterned(600));  // P1 sealed
    sync_wait(h.b->delete_object("bkt", "k1"));  // P1 liveness 1/2 -> candidate

    auto live2 = h.meta->get_object("bkt", "k2")->data.extents[0];
    corrupt_file_at(pack_file_path(cfg.root, live2.file_id), live2.offset + 10);  // corrupt the survivor

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(1));
    CHECK_EQ(st1.records_corrupt, uint64_t(1));
    CHECK_EQ(st1.records_migrated, uint64_t(0));
    CHECK_EQ(st1.packs_removed, uint64_t(0));  // live>0: the pack is kept (do not lose data destined for manual rescue)
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    // No accounting progress + within the cooldown window: the next round skips the re-scan
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.packs_compacted, uint64_t(0));
    sync_wait(h.b->close());
}

// Compaction of mpu parts (§9.2 owner hint): pack parts of an in-progress upload cannot migrate (conservatively
// blocked, pack kept); after complete, the b/k embedded in the owner looks up the owning object -> migration unlocks, pack reclaimed
TEST(duostore_compact_mpu_part_blocks_then_migrates_after_complete) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-mpu");
    cfg.pack_max_size = 1400;
    // With headers counted in live (§2.3a), a part record's owner header is longer: after deleting f1 the live
    // ratio is ≈ 0.52, so the threshold is raised to 0.6 to keep the "this pack is a compaction candidate" test context
    cfg.pack_gc_ratio = 0.6;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    auto id = sync_wait(h.b->create_multipart("bkt", "mp", {}));
    std::string etag;
    {
        http::StringBodyReader body(patterned(600));
        etag = sync_wait(h.b->upload_part("bkt", "mp", id, 1, body)).etag;
    }
    put(*h.b, "bkt", "f1", patterned(600));
    put(*h.b, "bkt", "f2", patterned(600));  // P1 (part+f1) sealed, f2 goes to P2
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));
    sync_wait(h.b->delete_object("bkt", "f1"));  // P1 liveness = 1 in-progress part -> candidate

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(1));
    CHECK_EQ(st1.records_migrated, uint64_t(0));  // in-progress mpu: the object does not exist -> conservatively not migrated
    CHECK_EQ(st1.packs_removed, uint64_t(0));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    std::vector<PartInfo> parts = {{1, etag}};
    sync_wait(h.b->complete_multipart("bkt", "mp", id, parts));

    // After complete (grace=0, no cooldown): the owner's b/k hint successfully looks up the object -> migrate + whole-delete
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.records_migrated, uint64_t(1));
    CHECK_EQ(st2.packs_removed, uint64_t(1));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    auto g = sync_wait(h.b->get_object("bkt", "mp", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    sync_wait(h.b->close());
}

// After compacting a mixed object (chunk + pack extents), the chunk refs must survive (storage.md top-risk item
// number one): swap_extents' to/from share the unmigrated chunk; add-all-then-delete-all under the refs
// last-wins semantics nets out to a delete -> the orphan scan would then unlink live data
TEST(duostore_compact_mixed_object_keeps_chunk_refs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-mixed");
    cfg.pack_max_size = 1400;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    // Mixed MPU: part1 large (chunk path), part2 small (pack path)
    std::string big = patterned(6000), small = patterned(600);
    auto id = sync_wait(h.b->create_multipart("bkt", "mixed", {}));
    std::string e1, e2;
    {
        http::StringBodyReader b1(big);
        e1 = sync_wait(h.b->upload_part("bkt", "mixed", id, 1, b1)).etag;
        http::StringBodyReader b2(small);
        e2 = sync_wait(h.b->upload_part("bkt", "mixed", id, 2, b2)).etag;
    }
    std::vector<PartInfo> parts = {{1, e1}, {2, e2}};
    sync_wait(h.b->complete_multipart("bkt", "mixed", id, parts));

    auto rec = h.meta->get_object("bkt", "mixed");
    CHECK(rec.has_value());
    std::vector<uint64_t> chunk_ids;
    bool has_pack = false;
    for (const auto& e : rec->data.extents) {
        if (e.kind == Extent::Kind::kPack) has_pack = true;
        else chunk_ids.push_back(e.file_id);
    }
    CHECK(has_pack && !chunk_ids.empty());  // it really is a mixed object
    for (uint64_t cid : chunk_ids) CHECK(h.meta->chunk_referenced(cid));

    // Drop that pack's liveness below the threshold -> compaction migrates that one pack extent
    put(*h.b, "bkt", "f1", patterned(600));
    put(*h.b, "bkt", "f2", patterned(600));  // triggers rotation sealing
    sync_wait(h.b->delete_object("bkt", "f1"));
    auto st = sync_wait(h.b->run_gc_once());
    CHECK(st.records_migrated >= uint64_t(1));

    // The unmigrated chunks are still referenced by the object: the refs entries must not be wiped by the swap
    auto after = h.meta->get_object("bkt", "mixed");
    CHECK(after.has_value());
    for (uint64_t cid : chunk_ids) CHECK(h.meta->chunk_referenced(cid));

    // The orphan scan must not delete them as unreferenced files; the object content is still byte-for-byte correct
    auto os = sync_wait(h.b->run_orphan_scan_once());
    CHECK_EQ(os.orphans_removed, uint64_t(0));
    auto g = sync_wait(h.b->get_object("bkt", "mixed", std::nullopt));
    CHECK_EQ(read_all(*g.body), big + small);
    sync_wait(h.b->close());
}

// P0 §1.4: an active pack another instance is writing must not be back-sealed (back-seal -> full compaction
// rewrite -> whole-deletion once the accounting zeroes out, while the other side still appends via its fd = silent
// data loss). A second FsDataStore holding an active pack on the same root simulates "another gateway", verifying pack_write_locked can detect it
TEST(duostore_active_pack_of_other_writer_not_sealed) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-owner");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));  // creates an active (unsealed) pack

    uint64_t pack_id = h.meta->get_object("bkt", "k1")->data.extents[0].file_id;
    auto stats = h.meta->pack_stats();
    bool unsealed = false;
    for (const auto& ps : stats)
        if (ps.pack_id == pack_id && !ps.sealed) unsealed = true;
    CHECK(unsealed);  // premise: the pack really is in the active state

    // This instance holds the write lock -> the probe should report "being written"
    CHECK(h.b->data_for_test().pack_write_locked(pack_id));

    // After closing this instance (fd closed -> lock released), the same pack is no longer judged as being written
    sync_wait(h.b->close());
    FsDataOptions probe_opt{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc,
                            cfg.pack_threshold, cfg.pack_max_size, cfg.pack_writers, {}};
    FsDataStore probe(probe_opt, pool, [](Extent::Kind, uint32_t) -> uint64_t { return 0; });
    CHECK(!probe.pack_write_locked(pack_id));
    sync_wait(probe.close());
}

// Denominator backfill for crash-leftover packs (gaps §2.3b): after the seal(0) back-seal, the first GC round uses
// one stat_pack stat to backfill file_size -- packs with healthy liveness no longer unconditionally enter the full sequential-scan rewrite
TEST(duostore_gc_stat_backfills_crash_leftover_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-statfill");
    {
        auto h = make_pack_backend(cfg, pool);
        sync_wait(h.b->create_bucket("bkt"));
        put(*h.b, "bkt", "k1", patterned(600));
        put(*h.b, "bkt", "k2", patterned(600));
        // No close: dtor fallback (equivalent to crash leftovers, back-sealed seal(0) on reopen)
    }
    auto h = make_pack_backend(cfg, pool);
    uint64_t pid = h.meta->get_object("bkt", "k1")->data.extents[0].file_id;
    auto ps0 = find_pack_stat(*h.meta, pid);
    CHECK(ps0->sealed);
    CHECK_EQ(ps0->file_size, uint64_t(0));  // size unknown at back-seal time

    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_compacted, uint64_t(0));  // 100% live: after the backfill the liveness ratio skips the rewrite
    auto ps1 = find_pack_stat(*h.meta, pid);
    CHECK_EQ(ps1->file_size, uint64_t(fs::file_size(pack_file_path(cfg.root, pid))));

    auto g = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    sync_wait(h.b->close());
}

// rewrite_pack sequential-scan semantics (store level, no migration callback): stats, file_size reporting, torn tail
// stops the scan silently (the expected shape of restart abandonment), corrupted magic stops the scan loudly
TEST(duostore_rewrite_pack_scan_stats_and_torn_tail) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    RocksMetaStore meta({(tmp.path / "meta").string(), false, 8ull << 20});
    FsDataOptions opt{tmp.path / "duo", 4096, false, /*pack_threshold=*/1024, 64 << 10, 1, {}};
    FsDataStore d(opt, pool, [&](Extent::Kind k, uint32_t n) { return meta.alloc_file_run(k, n); });

    auto append = [&](std::string owner, size_t n) {
        auto w = sync_wait(d.open_writer({uint64_t(n), std::move(owner)}));
        std::string data = patterned(n);
        sync_wait(w->write(std::span(reinterpret_cast<const std::byte*>(data.data()), n)));
        return sync_wait(w->finish()).extents.at(0);
    };
    auto e1 = append(std::string("bkt\0a", 5), 600);
    auto e2 = append(std::string("bkt\0b", 5), 600);
    CHECK_EQ(e1.file_id, e2.file_id);
    uint64_t real_size = fs::file_size(pack_file_path(tmp.path / "duo", e1.file_id));
    {
        // Torn tail injection: a truncated record header (crash residue)
        std::ofstream f(pack_file_path(tmp.path / "duo", e1.file_id),
                        std::ios::binary | std::ios::app);
        f << "LP3R" << std::string(7, '\x5a');
    }
    auto rw = sync_wait(d.rewrite_pack(e1.file_id));
    CHECK_EQ(rw.scanned, uint64_t(2));
    CHECK_EQ(rw.migrated, uint64_t(0));  // no migration callback: scan only, no migration
    CHECK_EQ(rw.corrupt, uint64_t(0));   // torn tail does not count as corruption
    CHECK_EQ(rw.file_size, real_size + 11);

    corrupt_file_at(pack_file_path(tmp.path / "duo", e1.file_id), 0);  // corrupt the magic
    auto rw2 = sync_wait(d.rewrite_pack(e1.file_id));
    CHECK_EQ(rw2.scanned, uint64_t(0));  // cannot resynchronize: stops the scan loudly
    CHECK_EQ(rw2.corrupt, uint64_t(1));
    sync_wait(d.close());
    meta.close();
}

// Pre-P4 legacy mpu owner format ("mpu\0<id>\0<no>", no b/k): cannot be looked up -> conservatively not migrated,
// pack kept, object stays readable; the new format (with b/k) migrates normally via object lookup -- two on-disk generations coexist safely
TEST(duostore_compact_legacy_mpu_owner_blocks) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    RocksMetaStore meta({(tmp.path / "meta").string(), false, 8ull << 20});
    meta.create_bucket("bkt");
    FsDataOptions opt{tmp.path / "duo", 4096, false, /*pack_threshold=*/1024, 64 << 10, 1, {}};
    uint64_t pack_id = 0;
    {
        FsDataStore d1(opt, pool, [&](Extent::Kind k, uint32_t n) { return meta.alloc_file_run(k, n); },
                       [&](uint64_t id, uint64_t sz) { meta.seal_pack(id, sz); });
        auto write_rec = [&](std::string owner, const std::string& data) {
            auto w = sync_wait(d1.open_writer({uint64_t(data.size()), std::move(owner)}));
            sync_wait(w->write(
                std::span(reinterpret_cast<const std::byte*>(data.data()), data.size())));
            return sync_wait(w->finish()).extents.at(0);
        };
        // One record each in the legacy format (simulating a pre-P4 leftover disk) and the new format, both belonging to completed objects
        auto legacy = write_rec(std::string("mpu\0oldid\0001", 11), patterned(300));
        auto modern =
            write_rec(std::string("mpu\0bkt\0knew\0newid\0001", 20), patterned(400));
        pack_id = legacy.file_id;
        ObjectRec r1;
        r1.meta.key = "kold";
        r1.meta.etag = "e1";
        r1.meta.size = 300;
        r1.meta.last_modified = std::chrono::system_clock::now();
        r1.data.extents = {legacy};
        meta.put_object("bkt", "kold", std::move(r1));
        ObjectRec r2;
        r2.meta.key = "knew";
        r2.meta.etag = "e2";
        r2.meta.size = 400;
        r2.meta.last_modified = std::chrono::system_clock::now();
        r2.data.extents = {modern};
        meta.put_object("bkt", "knew", std::move(r2));
        sync_wait(d1.close());  // seals the pack (the migration target must be a different active pack)
    }
    FsDataStore d2(opt, pool, [&](Extent::Kind k, uint32_t n) { return meta.alloc_file_run(k, n); },
                   [&](uint64_t id, uint64_t sz) { meta.seal_pack(id, sz); },
                   [&](IDataStore& ds, std::vector<PackScanRecord>&& batch) {
                       return migrate_pack_records(meta, ds, nullptr, std::move(batch));
                   });
    auto rw = sync_wait(d2.rewrite_pack(pack_id));
    CHECK_EQ(rw.scanned, uint64_t(2));
    CHECK_EQ(rw.migrated, uint64_t(1));  // new format migrates; legacy format conservatively shelved
    CHECK(meta.get_object("bkt", "kold")->data.extents[0].file_id == pack_id);  // ref untouched
    CHECK(meta.get_object("bkt", "knew")->data.extents[0].file_id != pack_id);  // ref swapped
    // Both objects are readable with correct content
    auto read_obj = [&](const char* k, size_t n) {
        auto rec = meta.get_object("bkt", k);
        auto r = sync_wait(d2.open_reader(rec->data, 0, n - 1));
        return read_all(*r);
    };
    CHECK_EQ(read_obj("kold", 300), patterned(300));
    CHECK_EQ(read_obj("knew", 400), patterned(400));
    sync_wait(d2.close());
    meta.close();
}

// ---------- P4 orphan scan suite (§9.3) ----------

// Forward: file on disk with no ref and past grace → unlink (recorded objects
// untouched). Reverse: ref exists but file missing → alert counter only, never
// delete meta. Grace shields fresh writes.
TEST(duostore_orphan_scan_forward_and_reverse) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "orphan");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));  // 3 chunks on record

    // Inject an orphan shaped like crash residue (file on disk, no meta record);
    // pick a far-away id to stay clear of the allocated range
    fs::create_directories(cfg.root / "chunks" / "ab");
    std::ofstream(cfg.root / "chunks" / "ab" / "000000000000abcd.chk") << patterned(100);

    auto st1 = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(st1.chunks_scanned, uint64_t(4));
    CHECK_EQ(st1.orphans_removed, uint64_t(1));
    CHECK_EQ(st1.refs_missing, uint64_t(0));
    CHECK_EQ(st1.skipped_pinned, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(3));
    auto g = sync_wait(b->get_object("bkt", "k", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(10000));
    g.body.reset();

    // Reverse: manually delete a recorded chunk file → alert counter only, meta
    // kept (as a lead for manual intervention)
    uint64_t lost = 0;
    {
        auto rec = sync_wait(b->head_object("bkt", "k"));
        (void)rec;
    }
    {
        // Grab a recorded id via the orphan-scan enumeration (the intersection
        // with refs is necessarily non-empty)
        fs::path victim;
        for (auto& e : fs::recursive_directory_iterator(cfg.root / "chunks"))
            if (e.is_regular_file() && e.path().extension() == ".chk") {
                victim = e.path();
                break;
            }
        CHECK(!victim.empty());
        lost = std::stoull(victim.filename().string().substr(0, 16), nullptr, 16);
        fs::remove(victim);
    }
    auto st2 = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(st2.chunks_scanned, uint64_t(2));
    CHECK_EQ(st2.orphans_removed, uint64_t(0));
    CHECK_EQ(st2.refs_missing, uint64_t(1));
    (void)lost;
    CHECK(sync_wait(b->head_object("bkt", "k")).size == 10000);  // meta untouched
    sync_wait(b->close());

    // Grace shields fresh writes: unreferenced files within the grace window stay
    TmpDir tmp2;
    auto cfg2 = gc_cfg(tmp2, "orphan-grace");
    cfg2.gc_grace_sec = 3600;
    auto b2 = std::make_shared<DuoStoreBackend>(cfg2, pool);
    sync_wait(b2->create_bucket("bkt"));
    fs::create_directories(cfg2.root / "chunks" / "ab");
    std::ofstream(cfg2.root / "chunks" / "ab" / "000000000000abcd.chk") << "x";
    auto st3 = sync_wait(b2->run_orphan_scan_once());
    CHECK_EQ(st3.skipped_grace, uint64_t(1));
    CHECK_EQ(st3.orphans_removed, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(cfg2.root), size_t(1));
    sync_wait(b2->close());
}

// packs/ two-way reconciliation (docs/archive/gaps.md §6.1): the pack file exists as soon
// as it is created, but the packstat row only lands when the first record
// commits — a hard crash inside that window leaks the file forever with no
// record anywhere. Forward: unrecorded pack file past grace is deleted.
// Reverse: packstat exists but file missing → alert only, never drop the record.
TEST(duostore_orphan_scan_reconciles_packs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "orphanpack");
    cfg.orphan_scan_interval_sec = 0;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k", patterned(600));
    uint64_t live_pack = h.meta->get_object("bkt", "k")->data.extents[0].file_id;

    // Unrecorded pack file (residue of "crashed after file creation, before the
    // first record committed")
    fs::create_directories(cfg.root / "packs" / "ab");
    std::ofstream(cfg.root / "packs" / "ab" / "000000000000abcd.pak") << "leaked";

    auto st1 = sync_wait(h.b->run_orphan_scan_once());
    CHECK_EQ(st1.packs_scanned, uint64_t(2));
    CHECK_EQ(st1.orphan_packs_removed, uint64_t(1));
    CHECK_EQ(st1.pack_stats_missing, uint64_t(0));
    CHECK(st1.pack_bytes > 0);
    CHECK(!fs::exists(cfg.root / "packs" / "ab" / "000000000000abcd.pak"));
    // Recorded pack untouched — it is still the active pack this process is writing
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(read_all(*sync_wait(h.b->get_object("bkt", "k", std::nullopt)).body),
             patterned(600));

    // Reverse: manually delete the recorded pack file → count + alert, packstat
    // kept for manual intervention
    sync_wait(h.b->close());  // shut down the writer first, or we'd delete the
                              // active pack this process holds locked
    auto h2 = make_pack_backend(cfg, pool);
    fs::remove(pack_file_path(cfg.root, live_pack));
    auto st2 = sync_wait(h2.b->run_orphan_scan_once());
    CHECK_EQ(st2.packs_scanned, uint64_t(0));
    CHECK_EQ(st2.pack_stats_missing, uint64_t(1));
    CHECK(find_pack_stat(*h2.meta, live_pack).has_value());  // record untouched
    sync_wait(h2.b->close());
}

namespace {

// Gateable body: emits `first`, then blocks until release() before emitting
// `rest` — creates a long "write in flight, meta not yet committed" window
// (exactly what the write-side pin protects, §9.3)
class GatedReader final : public http::BodyReader {
public:
    GatedReader(std::string first, std::string rest)
        : first_(std::move(first)), rest_(std::move(rest)),
          total_(first_.size() + rest_.size()) {}

    std::optional<uint64_t> length() const override { return total_; }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (stage_ == 0) {
            size_t n = std::min(buf.size(), first_.size() - off_);
            std::memcpy(buf.data(), first_.data() + off_, n);
            off_ += n;
            if (off_ == first_.size()) {
                stage_ = 1;
                off_ = 0;
            }
            co_return n;
        }
        if (stage_ == 1) {
            gate_.acquire();  // blocks a pool thread until release() (pool has >1
                              // thread, so the scan is not blocked)
            stage_ = 2;
        }
        size_t n = std::min(buf.size(), rest_.size() - off_);
        std::memcpy(buf.data(), rest_.data() + off_, n);
        off_ += n;
        co_return n;
    }

    void release() { gate_.release(); }

private:
    std::string first_, rest_;
    uint64_t total_;
    size_t off_ = 0;
    int stage_ = 0;
    std::binary_semaphore gate_{0};
};

}  // namespace

// Write-side pin (§9.3): chunks of a slow streaming PUT that are on disk but not
// yet committed to meta must not be mistakenly deleted by the orphan scan — the
// mtime grace is insufficient for very long writes; the pin is the hard defense
// (and the only one when grace=0)
TEST(duostore_orphan_scan_write_pin_protects_inflight_put) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "orphan-pin");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    std::string body_data = patterned(9000);
    GatedReader body(body_data.substr(0, 5000), body_data.substr(5000));
    std::thread writer([&] { sync_wait(b->put_object("bkt", "slow", {}, body)); });

    // Wait for the first chunk to hit disk (4096 slicing: 5000B → chunk0 sealed +
    // chunk1 being written)
    for (int i = 0; i < 200 && chunk_files_on_disk(cfg.root) < 1; ++i) usleep(20 * 1000);
    size_t on_disk = chunk_files_on_disk(cfg.root);
    CHECK(on_disk >= 1);

    auto st1 = sync_wait(b->run_orphan_scan_once());
    CHECK(st1.skipped_pinned >= 1);  // grace=0: the write-side pin is the only defense
    CHECK_EQ(st1.orphans_removed, uint64_t(0));
    CHECK(chunk_files_on_disk(cfg.root) >= on_disk);

    body.release();
    writer.join();

    // After commit: refs on record, pin released; rescanning takes zero actions
    // and the content is intact
    auto st2 = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(st2.orphans_removed, uint64_t(0));
    CHECK_EQ(st2.skipped_pinned, uint64_t(0));
    CHECK_EQ(st2.refs_missing, uint64_t(0));
    auto g = sync_wait(b->get_object("bkt", "slow", std::nullopt));
    CHECK_EQ(read_all(*g.body), body_data);
    sync_wait(b->close());
}

// ---------- P4 crash injection (§15 P4 acceptance: kill -9, restart, converge) ----------
// A child process (execv of self into duostore-crash-child mode) _exits at a
// chosen point / gets SIGKILLed — equivalent for process state (no destructors,
// no flush). The parent reopens the same root and verifies: committed objects
// survive byte-for-byte (the meta_sync=true commit contract), and GC + orphan
// scan converge the residue to zero.

namespace {

DuoStoreConfig crash_cfg(const fs::path& root) {
    DuoStoreConfig cfg;
    cfg.name = "crash";
    cfg.root = root;
    cfg.meta_path = root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 1024;
    cfg.pack_max_size = 64 << 10;
    cfg.pack_writers = 1;
    cfg.meta_sync = true;  // the crash-semantics linchpin: commit point = WAL fsync (§6)
    cfg.gc_interval_sec = 0;
    cfg.orphan_scan_interval_sec = 0;
    cfg.gc_grace_sec = 0;
    return cfg;
}

// _exits inside read() after emitting `limit` bytes — a crash mid-way through a
// PUT pump (data partially on disk, no meta record)
class ExitMidwayReader final : public http::BodyReader {
public:
    ExitMidwayReader(std::string data, size_t limit) : data_(std::move(data)), limit_(limit) {}
    std::optional<uint64_t> length() const override { return data_.size(); }
    Task<size_t> read(std::span<std::byte> buf) override {
        if (off_ >= limit_) ::_exit(0);
        size_t n = std::min({buf.size(), data_.size() - off_, limit_ - off_, size_t(3000)});
        std::memcpy(buf.data(), data_.data() + off_, n);
        off_ += n;
        co_return n;
    }

private:
    std::string data_;
    size_t limit_;
    size_t off_ = 0;
};

int duostore_crash_child(int argc, char** argv) {
    if (argc < 4) return 2;
    std::string mode = argv[2];
    fs::path root = argv[3];
    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    sync_wait(b->create_bucket("bkt"));
    if (mode == "commit") {
        // Small pack object + multi-chunk large object + multipart complete:
        // exercise every commit path once
        put(*b, "bkt", "small", patterned(600));
        put(*b, "bkt", "big", patterned(10000));
        auto id = sync_wait(b->create_multipart("bkt", "mp", {}));
        std::vector<PartInfo> parts;
        for (int no = 1; no <= 2; ++no) {
            http::StringBodyReader body(patterned(6000));
            parts.push_back({no, sync_wait(b->upload_part("bkt", "mp", id, no, body)).etag});
        }
        sync_wait(b->complete_multipart("bkt", "mp", id, parts));
    } else if (mode == "midput") {
        put(*b, "bkt", "before", patterned(600));
        ExitMidwayReader body(patterned(20000), 9000);
        sync_wait(b->put_object("bkt", "victim", {}, body));
        return 3;  // unreachable: the body _exits at byte 9000
    } else if (mode == "afterdelete") {
        put(*b, "bkt", "gone", patterned(10000));
        sync_wait(b->delete_object("bkt", "gone"));  // gcq entry recorded, files not yet reclaimed
    } else if (mode == "spin") {
        // Commit in a loop and report each one; the parent SIGKILLs at a random
        // moment. The line is written only after put returns (WAL fsync) — any
        // full line the parent reads means the object must exist after restart
        for (int i = 0;; ++i) {
            size_t n = (i % 2) ? 600 : 10000;
            put(*b, "bkt", "k" + std::to_string(i), patterned(n));
            std::string line = "ok " + std::to_string(i) + "\n";
            if (::write(1, line.data(), line.size()) < 0) return 4;
        }
    } else {
        return 2;
    }
    ::_exit(0);  // equivalent to kill -9: no close, no destructors (WAL replay and
                 // active-pack abandonment are left to the restart)
}

mini_test::ChildRegistrar crash_child_reg("duostore-crash-child", duostore_crash_child);

pid_t spawn_crash_child(const char* mode, const fs::path& root, int* out_fd = nullptr) {
    int pfd[2] = {-1, -1};
    if (out_fd) CHECK(::pipe(pfd) == 0);
    pid_t pid = ::fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        if (out_fd) {
            ::dup2(pfd[1], 1);
            ::close(pfd[0]);
            ::close(pfd[1]);
        }
        char exe[4096];
        ssize_t n = ::readlink("/proc/self/exe", exe, sizeof exe - 1);
        if (n <= 0) ::_exit(127);
        exe[n] = 0;
        ::execl(exe, exe, "duostore-crash-child", mode, root.c_str(), (char*)nullptr);
        ::_exit(127);
    }
    if (out_fd) {
        ::close(pfd[1]);
        *out_fd = pfd[0];
    }
    return pid;
}

int wait_child(pid_t pid) {
    int stat = 0;
    CHECK(::waitpid(pid, &stat, 0) == pid);
    return stat;
}

std::string expect_body(int i) { return patterned((i % 2) ? 600 : 10000); }

// Restart-convergence check: run GC + orphan scan to a fixed point, then one
// more round of each must take zero actions
void verify_converged(DuoStoreBackend& b) {
    sync_wait(b.run_gc_once());
    sync_wait(b.run_orphan_scan_once());
    sync_wait(b.run_gc_once());  // one more round to realize second-order effects
                                 // (e.g. empty packs unlocked by compaction)
    auto gc = sync_wait(b.run_gc_once());
    CHECK_EQ(gc.reclaims_acked, uint64_t(0));
    CHECK_EQ(gc.files_removed, uint64_t(0));
    CHECK_EQ(gc.packs_removed, uint64_t(0));
    CHECK_EQ(gc.uploads_expired, uint64_t(0));
    auto os = sync_wait(b.run_orphan_scan_once());
    CHECK_EQ(os.orphans_removed, uint64_t(0));
    CHECK_EQ(os.refs_missing, uint64_t(0));
    CHECK_EQ(os.skipped_pinned, uint64_t(0));
}

void check_body(DuoStoreBackend& b, const std::string& key, const std::string& want) {
    auto g = sync_wait(b.get_object("bkt", key, std::nullopt));
    CHECK_EQ(read_all(*g.body), want);
}

}  // namespace

// Crash after commit: objects from all three commit paths (pack / multi-chunk /
// multipart) survive byte-for-byte across restart; crash residue (unsealed
// active pack, orphans) converges to zero; content stays correct after
// convergence (including compaction migration)
TEST(duostore_crash_after_commit_recovers_all) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    CHECK_EQ(wait_child(spawn_crash_child("commit", root)), 0);

    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    check_body(*b, "small", patterned(600));
    check_body(*b, "big", patterned(10000));
    check_body(*b, "mp", patterned(6000) + patterned(6000));
    verify_converged(*b);
    check_body(*b, "small", patterned(600));  // recheck: convergence (incl. compaction
                                              // migration) leaves content untouched
    check_body(*b, "big", patterned(10000));
    check_body(*b, "mp", patterned(6000) + patterned(6000));
    sync_wait(b->close());
}

// Crash mid-way through a PUT pump: victim has no record (NoSuchKey), the
// half-landed chunks become orphans and are swept clean; previously committed
// objects are untouched
TEST(duostore_crash_mid_put_leaves_no_garbage) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    CHECK_EQ(wait_child(spawn_crash_child("midput", root)), 0);

    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "victim")), s3::S3ErrorCode::NoSuchKey);
    check_body(*b, "before", patterned(600));
    // 9000B / 4096 slicing → 2 sealed chunks + 1 in-flight chunk, all unrecorded
    auto os = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(os.orphans_removed, uint64_t(3));
    CHECK_EQ(os.refs_missing, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(root), size_t(0));  // "before" is a pack object, so
                                                     // chunks/ should be clean
    verify_converged(*b);
    check_body(*b, "before", patterned(600));
    sync_wait(b->close());
}

// Crash after the delete is recorded: the leftover gcq entry is realized by GC
// after restart (physical delete before record removal is the crash-safe side)
TEST(duostore_crash_after_delete_converges) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    CHECK_EQ(wait_child(spawn_crash_child("afterdelete", root)), 0);

    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "gone")), s3::S3ErrorCode::NoSuchKey);
    auto gc = sync_wait(b->run_gc_once());
    CHECK_EQ(gc.reclaims_acked, uint64_t(1));
    CHECK_EQ(gc.files_removed, uint64_t(3));  // 10000B / 4096 → 3 chunks
    CHECK_EQ(chunk_files_on_disk(root), size_t(0));
    verify_converged(*b);
    sync_wait(b->close());
}

// SIGKILL at a random moment: every commit the child reported (line written
// after WAL fsync) must exist byte-for-byte after restart — the durability
// contract under a real kill -9; residue converges as usual
TEST(duostore_crash_random_sigkill_keeps_reported_commits) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    int fd = -1;
    pid_t pid = spawn_crash_child("spin", root, &fd);

    // Wait for the first reported commit before killing at a random point
    // (guarantees the test is not a no-op)
    std::string buf;
    char c;
    while (buf.find('\n') == std::string::npos && ::read(fd, &c, 1) == 1) buf.push_back(c);
    CHECK(buf.find('\n') != std::string::npos);
    usleep((200 + unsigned(::getpid()) % 500) * 1000);  // 200-700ms random window
    CHECK_EQ(::kill(pid, SIGKILL), 0);
    int stat = wait_child(pid);
    CHECK(WIFSIGNALED(stat) && WTERMSIG(stat) == SIGKILL);
    // Drain the pipe; only complete lines count
    for (;;) {
        char rb[4096];
        ssize_t n = ::read(fd, rb, sizeof rb);
        if (n <= 0) break;
        buf.append(rb, size_t(n));
    }
    ::close(fd);
    std::vector<int> committed;
    for (size_t pos = 0; pos < buf.size();) {
        size_t nl = buf.find('\n', pos);
        if (nl == std::string::npos) break;  // trailing partial line: doesn't count
        std::string line = buf.substr(pos, nl - pos);
        pos = nl + 1;
        if (line.rfind("ok ", 0) == 0) committed.push_back(std::stoi(line.substr(3)));
    }
    CHECK(!committed.empty());

    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    for (int i : committed) check_body(*b, "k" + std::to_string(i), expect_body(i));
    verify_converged(*b);
    for (int i : committed) check_body(*b, "k" + std::to_string(i), expect_body(i));
    sync_wait(b->close());
}

// gaps §3.9: chunk ids are batch-allocated in geometric runs — even when
// concurrent writers interleave and burn ids in the allocator (runs are not
// contiguous with each other), an object's chunk ids stay contiguous within
// each run, so the manifest's run encoding remains valid
TEST(duostore_chunk_ids_batched_in_runs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    uint64_t next_id = 1;
    FsDataStore d(FsDataOptions{tmp.path / "duo", 64, false, 0, 128ull << 20, 4, {}}, pool,
                  [&](Extent::Kind, uint32_t n) {
                      uint64_t f = next_id;
                      next_id += n + 7;  // simulate concurrent writers consuming ids
                                         // between two batch grabs
                      return f;
                  });
    auto w = sync_wait(d.open_writer({std::nullopt, "t/runs"}));
    std::string body(64 * 7, 'x');  // 7 chunks: run sequence 1, 2, 4
    sync_wait(
        w->write(std::span(reinterpret_cast<const std::byte*>(body.data()), body.size())));
    auto ref = sync_wait(w->finish());
    CHECK_EQ(ref.extents.size(), size_t(7));
    CHECK_EQ(ref.extents[2].file_id, ref.extents[1].file_id + 1);  // contiguous within run(2)
    for (size_t i = 4; i <= 6; ++i)                                // contiguous within run(4)
        CHECK_EQ(ref.extents[i].file_id, ref.extents[3].file_id + (i - 3));
    CHECK(ref.extents[1].file_id != ref.extents[0].file_id + 1);  // interleaving really happened
    sync_wait(d.close());
}

#endif  // LIGHTS3_DUOSTORE
