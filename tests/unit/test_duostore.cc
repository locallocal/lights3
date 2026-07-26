// DuoStore 专项单测（docs/duostore-backend.md §14）：编解码 roundtrip 与 run 边界、
// 跨 chunk 读写、位腐检出、GC 一期（P3）、pack 聚合（P2：分流/chunked 缓冲/轮转
// 封存/record 格式/crc/重启弃用/空 pack 整删）。meta 语义用例（GC 记账、号段单调、
// pack 存活账等）已接口化为 meta_store_suite（docs/duostore-redis-meta.md §9），
// RocksMetaStore 在此恒跑，redis/sqlite/tikv 在各自测试文件条件跑。
// 压实/崩溃注入专项随 P4 增补。
#ifdef LIGHTS3_DUOSTORE

#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/thread_pool.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
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
    // 单测不需要 WAL fsync（崩溃语义有专项，P4）
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
    // RFC 3720 附录 B 向量：crc32c("123456789") = 0xE3069283
    CHECK_EQ(codec::crc32c_of(std::string_view("123456789")), 0xE3069283u);
    // 链式增量 == 一次性
    uint32_t chained = codec::crc32c_of(std::string_view("12345"));
    std::string_view rest = "6789";
    chained = codec::crc32c_update(
        chained, std::span(reinterpret_cast<const std::byte*>(rest.data()), rest.size()));
    CHECK_EQ(chained, 0xE3069283u);
    CHECK_EQ(codec::crc32c_of(std::string_view("")), 0u);
}

TEST(duostore_extent_run_roundtrip) {
    // 连续 file_id 的定长 chunk（末块短）压成单 run：4B n_runs + 37B run 头 + 5×4B crc
    std::vector<Extent> ex;
    for (uint64_t i = 0; i < 5; ++i)
        ex.push_back(chunk_extent(100 + i, i == 4 ? 1000 : 8192, uint32_t(i)));
    std::string enc = codec::encode_extents(ex);
    CHECK_EQ(enc.size(), size_t(4 + 37 + 5 * 4));
    CHECK(codec::decode_extents(enc) == ex);

    // file_id 不连续 → 分裂为新 run
    ex.push_back(chunk_extent(200, 8192, 9));
    CHECK(codec::decode_extents(codec::encode_extents(ex)) == ex);

    // 中段短块阻断合并（run 内除末段外必须满长）
    std::vector<Extent> mixed = {chunk_extent(1, 8192, 1), chunk_extent(2, 100, 2),
                                 chunk_extent(3, 8192, 3)};
    CHECK(codec::decode_extents(codec::encode_extents(mixed)) == mixed);

    // pack extent 不合并且保留 offset
    std::vector<Extent> packs = {{Extent::Kind::kPack, 7, 4096, 100, 42},
                                 {Extent::Kind::kPack, 7, 8192, 50, 43}};
    CHECK(codec::decode_extents(codec::encode_extents(packs)) == packs);

    // kRados 与 kChunk 同构合并（docs/duostore-rados-data.md §3.1）：连续 id 压单
    // run；异 kind 相邻不合并
    std::vector<Extent> rados;
    for (uint64_t i = 0; i < 4; ++i)
        rados.push_back({Extent::Kind::kRados, 500 + i, 0, i == 3 ? 100 : 8192, uint32_t(i)});
    CHECK_EQ(codec::encode_extents(rados).size(), size_t(4 + 37 + 4 * 4));
    CHECK(codec::decode_extents(codec::encode_extents(rados)) == rados);
    std::vector<Extent> cross = {chunk_extent(600, 8192, 1),
                                 {Extent::Kind::kRados, 601, 0, 8192, 2}};
    CHECK(codec::decode_extents(codec::encode_extents(cross)) == cross);

    // 空 = 0 字节对象
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

    // part key 尾部 be16 升序
    CHECK(codec::part_key("b", "k", "id", 2) < codec::part_key("b", "k", "id", 300));
    CHECK_EQ(codec::part_no_of_key(codec::part_key("b", "k", "id", 300)), 300);
}

// meta 语义基线（GC 记账、号段单调、MPU 挡桶删、max-keys=0、delimiter 分页）：
// 接口化套件，RocksMetaStore 恒跑；RedisMetaStore 同一套件见 test_duostore_redis.cc
TEST(duostore_meta_store_suite_rocksdb) {
    TmpDir tmp;
    meta_store_suite::run_meta_store_suite(
        [&] { return std::make_unique<RocksMetaStore>(meta_opts(tmp)); });
}

// RocksDB 实现细节：pack 计数器从 0 起（Redis 版首段空烧，绝对值是实现自由度）
TEST(duostore_alloc_pack_counter_starts_at_zero) {
    TmpDir tmp;
    RocksMetaStore m(meta_opts(tmp));
    CHECK_EQ(m.alloc_file_id(Extent::Kind::kPack), uint64_t(0));
    m.close();
}

// decode_object_meta（list 的免物化路径）与 decode_object().meta 逐字段一致
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

// '\0' 分隔编码的防御纵深：含 NUL 的段进入 key 构造器必须响亮失败（§4.1）
TEST(duostore_codec_rejects_nul_key) {
    std::string nul_key("k\0x", 3);
    CHECK_THROWS_S3(codec::object_key("b", nul_key), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(codec::upload_key("b", "k", nul_key), s3::S3ErrorCode::InternalError);
}

// 跨 chunk 的写读与 Range（4KiB chunk 强制多 chunk manifest）；chunk 文件布局落位
TEST(duostore_multichunk_roundtrip_and_layout) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "t";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 0;  // 本用例专测 chunk 布局（pack 专项在下方）
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

    // Range 跨 chunk 边界（4096/8192 两个切点都覆盖）
    auto mid = sync_wait(b->get_object("bkt", "big", ByteRange{4000, 8500}));
    CHECK_EQ(read_all(*mid.body), data.substr(4000, 4501));

    // 布局：10000B / 4KiB = 3 个 chunk 文件落在 chunks/<ss>/ 下
    size_t chunk_files = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "duo" / "chunks"))
        if (e.is_regular_file() && e.path().extension() == ".chk") ++chunk_files;
    CHECK_EQ(chunk_files, size_t(3));

    // 0 字节对象：空 DataRef
    put(*b, "bkt", "empty", "");
    auto empty = sync_wait(b->get_object("bkt", "empty", std::nullopt));
    CHECK_EQ(empty.meta.size, uint64_t(0));
    CHECK_EQ(read_all(*empty.body), "");

    sync_wait(b->delete_object("bkt", "big"));
    sync_wait(b->delete_object("bkt", "empty"));
    sync_wait(b->delete_bucket("bkt"));
    sync_wait(b->close());
}

// verify_chunk_crc=true：位腐的 chunk 在 GET 时被检出（500），而非静默吐坏数据（§7）
TEST(duostore_get_detects_chunk_bitrot) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "crc";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.pack_threshold = 0;  // 走 chunk：pack 恒校验 crc，有独立用例
    cfg.meta_sync = false;
    cfg.verify_chunk_crc = true;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", std::string(1000, 'x'));

    // 注入位腐：翻转唯一 chunk 文件中段的一个字节
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

// ---------- GC 一期专项（§9/§15 P3 验收：覆盖/删除/abort 后收敛 + GET vs GC）----------

namespace {

// GC 专项统一配置：4KiB chunk 强制多 chunk、pack 关闭（chunk unlink 语义专测；
// pack 侧 GC 有独立用例）、grace=0 立即可回收、后台 worker 关闭（专测手动钩子）
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

// 覆盖 + 删除后 run_gc_once 收敛：chunk 物理消失、gcq 清空、再跑一轮零动作
TEST(duostore_gc_reclaims_after_overwrite_and_delete) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    put(*b, "bkt", "k", patterned(10000));  // 3 chunk
    put(*b, "bkt", "k", patterned(5000));   // 覆盖：旧 3 chunk 入 gcq，新 2 chunk
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(5));

    auto st1 = sync_wait(b->run_gc_once());
    CHECK_EQ(st1.reclaims_acked, uint64_t(1));
    CHECK_EQ(st1.files_removed, uint64_t(3));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(2));
    {
        // 存活版本不受影响（作用域内读完即析构——reader 存活期间持 pin）
        auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
        CHECK_EQ(read_all(*got.body), patterned(5000));
    }
    sync_wait(b->delete_object("bkt", "k"));
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(st2.files_removed, uint64_t(2));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));

    // 收敛：空 gcq 再跑一轮零动作
    auto st3 = sync_wait(b->run_gc_once());
    CHECK_EQ(st3.reclaims_acked, uint64_t(0));
    CHECK_EQ(st3.files_removed, uint64_t(0));
    sync_wait(b->close());
}

// gc_grace 防御纵深（§7/§9.1）：未逾宽限期的项跳过——不销账、文件保留
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

// 并发 GET vs GC（§7 pin 计数；§15 P3 验收"无 ENOENT"）：读中对象被删，pin 挡
// GC 不 unlink；读完整校验内容；reader 析构解 pin 后下一轮回收
TEST(duostore_gc_pin_blocks_unlink_during_get) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-pin");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    std::string body = patterned(10000);
    put(*b, "bkt", "k", body);

    auto got = sync_wait(b->get_object("bkt", "k", std::nullopt));
    // 读一小段（首 chunk 已打开），后续 chunk 依赖懒打开——正是 pin 防护的窗口
    std::byte buf[100];
    size_t n0 = sync_wait(got.body->read(std::span(buf)));
    CHECK(n0 > 0);

    sync_wait(b->delete_object("bkt", "k"));
    auto st1 = sync_wait(b->run_gc_once());
    CHECK_EQ(st1.skipped_pinned, uint64_t(1));
    CHECK_EQ(st1.reclaims_acked, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(3));

    // 剩余部分完整读出，无 ENOENT / 500
    std::string rest = read_all(*got.body);
    CHECK_EQ(std::string(reinterpret_cast<char*>(buf), n0) + rest, body);

    got.body.reset();  // 析构解 pin
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    sync_wait(b->close());
}

// abort 后 GC 收敛 + mpu_ttl 过期清理（§8 末）：过期 upload 内部 abort，分片
// 同轮变现；abort 后的 upload_id 干净失效
TEST(duostore_gc_mpu_ttl_expiry) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-mpu");
    cfg.mpu_ttl_sec = 1;  // 最小正 ttl（0 = 关闭清理，非"立即过期"）
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    auto id = sync_wait(b->create_multipart("bkt", "mpu", {}));
    {
        http::StringBodyReader part(patterned(6000));  // 2 chunk
        sync_wait(b->upload_part("bkt", "mpu", id, 1, part));
    }
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(2));

    usleep(1100 * 1000);  // 越过 1s ttl
    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.uploads_expired, uint64_t(1));
    // abort 入 gcq 的分片在同一轮消费（mpu 清理先于 gcq 消费）
    CHECK_EQ(st.reclaims_acked, uint64_t(1));
    CHECK_EQ(st.files_removed, uint64_t(2));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    CHECK_EQ(sync_wait(b->list_multipart_uploads("bkt")).size(), size_t(0));
    {
        http::StringBodyReader part("x");
        CHECK_THROWS_S3(sync_wait(b->upload_part("bkt", "mpu", id, 2, part)),
                        s3::S3ErrorCode::NoSuchUpload);
    }
    sync_wait(b->close());
}

// mpu_ttl=0 = 关闭清理（与 gc_interval 的 0 语义对齐）；未过期 upload 也不受影响
TEST(duostore_gc_mpu_fresh_upload_survives) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-mpu-fresh");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    sync_wait(b->create_multipart("bkt", "mpu", {}));

    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.uploads_expired, uint64_t(0));
    CHECK_EQ(sync_wait(b->list_multipart_uploads("bkt")).size(), size_t(1));
    sync_wait(b->close());

    // ttl=0：清理整体关闭，任何"已过期"的 upload 都不动
    TmpDir tmp2;
    auto cfg2 = gc_cfg(tmp2, "gc-mpu-off");
    cfg2.mpu_ttl_sec = 0;
    auto b2 = std::make_shared<DuoStoreBackend>(cfg2, pool);
    sync_wait(b2->create_bucket("bkt"));
    sync_wait(b2->create_multipart("bkt", "mpu", {}));
    auto st2 = sync_wait(b2->run_gc_once());
    CHECK_EQ(st2.uploads_expired, uint64_t(0));
    CHECK_EQ(sync_wait(b2->list_multipart_uploads("bkt")).size(), size_t(1));
    sync_wait(b2->close());
}

// gcq 断点续扫（§9.1）：整批被 pin 的队头不卡整轮——后续积压仍被回收，且跳过
// 项只计一次
TEST(duostore_gc_skipped_head_does_not_stall_round) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gc-stall");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    // 257 个单 chunk 对象：前 256 个读者持 pin（占满一个 peek 批），第 257 个可回收
    constexpr int kN = 257;
    for (int i = 0; i < kN; ++i)
        put(*b, "bkt", "k" + std::to_string(i), patterned(64));
    std::vector<ObjectStream> readers;
    for (int i = 0; i < kN - 1; ++i)
        readers.push_back(sync_wait(b->get_object("bkt", "k" + std::to_string(i), std::nullopt)));
    for (int i = 0; i < kN; ++i) sync_wait(b->delete_object("bkt", "k" + std::to_string(i)));

    auto st = sync_wait(b->run_gc_once());
    CHECK_EQ(st.skipped_pinned, uint64_t(kN - 1));  // 每项只计一次
    CHECK_EQ(st.reclaims_acked, uint64_t(1));       // 队头全 pin 不挡队尾变现
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(kN - 1));

    readers.clear();  // 解 pin 后全量收敛
    auto st2 = sync_wait(b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(kN - 1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    sync_wait(b->close());
}

// 后台 worker（§9）：gc_interval=1s 周期自动变现；close 撤定时器、等在途 GC
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

    // 至多等 15s 收敛（正常 1-2 个周期）
    bool converged = false;
    for (int i = 0; i < 150 && !converged; ++i) {
        if (chunk_files_on_disk(cfg.root) == 0) converged = true;
        else usleep(100 * 1000);
    }
    CHECK(converged);
    sync_wait(b->close());
}

// 生命周期：远期定时器被 close 干净撤销；不 close 直接析构走 dtor 兜底——两条
// 路径都不得悬挂/用后释放（asan/tsan 矩阵覆盖）
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
        DuoStoreBackend b(cfg, pool);  // 析构兜底路径
    }
}

// close 与手动 run_gc_once 并发：手动钩子经等待组登记在途，close 等它结束后才拆
// meta/data；关闭后的手动钩子拒绝进入、返回零统计（asan/tsan 矩阵下验证无 UAF）
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
    auto st = sync_wait(b->run_gc_once());  // 已关闭：拒绝进入
    CHECK_EQ(st.reclaims_acked, uint64_t(0));
}

// FsDataStore::remove_pack：整 pack 文件删除幂等（§9.1；手工造文件，不走写路径）
TEST(duostore_fs_remove_pack_idempotent) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    uint64_t next_id = 1;
    FsDataStore d(FsDataOptions{tmp.path / "duo", 4096, false}, pool,
                  [&](Extent::Kind) { return next_id++; });
    auto p = d.pack_path(7);
    fs::create_directories(p.parent_path());
    std::ofstream(p) << "stub";
    CHECK(fs::exists(p));
    sync_wait(d.remove_pack(7));
    CHECK(!fs::exists(p));
    sync_wait(d.remove_pack(7));  // 双删幂等（ENOENT 忽略）
    sync_wait(d.close());
}

// ---------- P2 pack 聚合专项（§5.2/§5.3/§14）----------

namespace {

// pack 专项统一配置：1KiB 阈值 + 单 writer（布局断言确定性）；chunk 4KiB；
// GC 手动钩子、grace=0
DuoStoreConfig pack_cfg(const TmpDir& tmp, const char* name) {
    DuoStoreConfig cfg;
    cfg.name = name;
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 1024;
    cfg.pack_max_size = 64 << 10;
    cfg.pack_writers = 1;
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

// 长度未知的流（chunked PUT 模拟）：length() 恒 nullopt，逼出缓冲/分流路径（§5.3）
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

// 注入组装：拿得到 meta 裸指针以断言 pack 存活账（backend 持有所有权）
struct PackHarness {
    std::shared_ptr<DuoStoreBackend> b;
    RocksMetaStore* meta = nullptr;  // 生命周期随 b
};

PackHarness make_pack_backend(const DuoStoreConfig& cfg, std::shared_ptr<ThreadPool> pool) {
    fs::create_directories(cfg.root);
    auto meta = std::make_unique<RocksMetaStore>(
        RocksMetaOptions{cfg.meta_path.string(), /*sync=*/false, 8ull << 20});
    auto* mp = meta.get();
    auto data = std::make_unique<FsDataStore>(
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                      cfg.pack_max_size, cfg.pack_writers},
        pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
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

// 分流 + 布局 + record 格式（§5.2）：≤ 阈值进同一 active pack 追加（零 chunk）、
// magic/owner 落盘、> 阈值走 chunk、GET 全量与 Range、存活账随写累计
TEST(duostore_pack_layout_roundtrip_and_stats) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    std::string d1 = patterned(600), d2 = patterned(500);
    put(*h.b, "bkt", "k1", d1);
    put(*h.b, "bkt", "k2", d2);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));  // 同一 active pack 追加
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));

    // record 格式落盘：文件以 "LP3R" magic 起头，owner = "bucket\0key" 内嵌
    std::string raw = read_file(sole_pack_file(cfg.root));
    CHECK_EQ(raw.substr(0, 4), "LP3R");
    CHECK(raw.find(std::string("bkt\0k1", 6)) != std::string::npos);
    CHECK(raw.find(std::string("bkt\0k2", 6)) != std::string::npos);

    // 读回：全量 + 跨 record 无关的 Range（payload 内切片）
    auto g1 = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));
    CHECK_EQ(read_all(*g1.body), d1);
    auto g2 = sync_wait(h.b->get_object("bkt", "k2", ByteRange{100, 299}));
    CHECK_EQ(read_all(*g2.body), d2.substr(100, 200));

    // 存活账（meta 侧）：2 record / 1100B，未封存
    auto rec = h.meta->get_object("bkt", "k1");
    CHECK(rec.has_value());
    CHECK_EQ(size_t(rec->data.extents.size()), size_t(1));
    uint64_t pid = rec->data.extents[0].file_id;
    CHECK(rec->data.extents[0].kind == Extent::Kind::kPack);
    auto ps = find_pack_stat(*h.meta, pid);
    CHECK(ps.has_value());
    CHECK_EQ(ps->live_bytes, int64_t(1100));
    CHECK_EQ(ps->live_recs, int64_t(2));
    CHECK(!ps->sealed);

    // 大于阈值 → chunk 路径（pack 无新增）
    std::string big = patterned(2000);
    put(*h.b, "bkt", "big", big);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(1));
    auto g3 = sync_wait(h.b->get_object("bkt", "big", std::nullopt));
    CHECK_EQ(read_all(*g3.body), big);

    // multipart 小分片同样进 pack，owner = "mpu\0<id>\0<no>"
    auto id = sync_wait(h.b->create_multipart("bkt", "mp", {}));
    {
        http::StringBodyReader body(patterned(300));
        sync_wait(h.b->upload_part("bkt", "mp", id, 1, body));
    }
    raw = read_file(sole_pack_file(cfg.root));
    CHECK(raw.find(std::string("mpu\0", 4) + id) != std::string::npos);
    sync_wait(h.b->abort_multipart("bkt", "mp", id));
    sync_wait(h.b->close());
}

// chunked（长度未知）PUT 的缓冲分流（§5.3）：恰 == 阈值整体进 pack；超阈值缓冲
// 落盘转 chunk 流式路径，内容完整
TEST(duostore_pack_chunked_put_buffer_and_spill) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-chunked");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    std::string exact = patterned(1024);  // == pack_threshold：EOF 时整体进 pack
    {
        UnknownLenReader body(exact);
        sync_wait(h.b->put_object("bkt", "fit", {}, body));
    }
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(0));
    auto g1 = sync_wait(h.b->get_object("bkt", "fit", std::nullopt));
    CHECK_EQ(read_all(*g1.body), exact);

    std::string spill = patterned(10000);  // 超阈值：缓冲落盘 + 转 chunk（3×4KiB）
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

// 轮转封存（§5.2）：达 pack_max_size 即 sealed（file_size 回报）、换新 pack_id；
// close() 封存余下 active pack
TEST(duostore_pack_rotation_seals_and_close_seals_rest) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-rot");
    cfg.pack_max_size = 2048;  // record ≈ 600+29 → 每 pack 3 条即满
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    for (int i = 0; i < 4; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));  // 3 + 1 分布

    auto stats = h.meta->pack_stats();
    CHECK_EQ(stats.size(), size_t(2));
    size_t sealed = 0, active = 0;
    for (const auto& ps : stats) {
        if (ps.sealed) {
            ++sealed;
            CHECK(ps.file_size > 0);  // 轮转封存回报最终文件大小
            CHECK_EQ(ps.live_recs, int64_t(3));
        } else {
            ++active;
            CHECK_EQ(ps.live_recs, int64_t(1));
        }
    }
    CHECK_EQ(sealed, size_t(1));
    CHECK_EQ(active, size_t(1));
    for (int i = 0; i < 4; ++i) {  // 跨 pack 全部可读
        auto g = sync_wait(h.b->get_object("bkt", "k" + std::to_string(i), std::nullopt));
        CHECK_EQ(read_all(*g.body), patterned(600));
    }

    IMetaStore* mp = h.meta;
    sync_wait(h.b->close());  // 封存余下 active pack（§9 生命周期：close 内 data 先于 meta）
    (void)mp;                 // close 后 meta 已关，账的复核放重启用例
}

// pack record 恒校验 crc（§7）：payload 位腐在 GET 时被检出（500），与
// verify_chunk_crc 开关无关
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
        f.seekp(-10, std::ios::end);  // payload 尾部（头在文件首）
        f.put('!');
    }
    auto got = sync_wait(h.b->get_object("bkt", "k", std::nullopt));
    CHECK_THROWS_S3(read_all(*got.body), s3::S3ErrorCode::InternalError);
    sync_wait(h.b->close());
}

// 空 pack 整删（§9.1）：sealed 且 live_recs==0 → unlink + 销 packstat；pin 挡整删。
// pack record 的 gcq 变现不计 files_removed（死区随压实回收）
TEST(duostore_pack_gc_empty_pack_removal_respects_pin) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-gc");
    cfg.pack_max_size = 1024;  // 单 record 即满：写第二个对象时封存第一个 pack
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));  // pack P1
    put(*h.b, "bkt", "k2", patterned(600));  // P1 封存，P2 active
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    uint64_t p1 = h.meta->get_object("bkt", "k1")->data.extents[0].file_id;
    auto got = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));  // 持 pin
    sync_wait(h.b->delete_object("bkt", "k1"));  // live_recs(P1) → 0

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.skipped_pinned, uint64_t(1));  // gcq 项被 pin 挡
    CHECK_EQ(st1.packs_removed, uint64_t(0));   // 空 pack 整删同样被 pin 挡
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));
    CHECK_EQ(read_all(*got.body), patterned(600));  // 读者不受影响

    got.body.reset();  // 解 pin
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.reclaims_acked, uint64_t(1));
    CHECK_EQ(st2.files_removed, uint64_t(0));  // pack record 不计物理删除
    CHECK_EQ(st2.packs_removed, uint64_t(1));  // 整文件 unlink
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK(!find_pack_stat(*h.meta, p1).has_value());  // packstat 已销
    sync_wait(h.b->close());
}

// 重启弃用 active pack（§5.2）：不 close 直接析构（崩溃等价）→ 重开同 root 后
// 上代 active pack 被补封（sealed/size 未知=0）、旧对象仍可读、新写入走新 pack、
// 旧对象删净后整 pack 回收
TEST(duostore_pack_restart_abandons_active) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-restart");
    {
        auto h = make_pack_backend(cfg, pool);
        sync_wait(h.b->create_bucket("bkt"));
        put(*h.b, "bkt", "old", patterned(600));
        CHECK(!h.meta->pack_stats()[0].sealed);
        // 不 close：析构兜底（active pack 不封存，等价崩溃遗留）
    }
    {
        // torn tail 注入（§5.2/§6.2）：崩溃可能在 append 中途留下半截 record——
        // 无引用即死区，不得影响已提交对象的读取
        std::ofstream f(sole_pack_file(cfg.root), std::ios::binary | std::ios::app);
        f << "LP3R" << std::string(7, '\x5a');  // magic + 截断的头
    }
    auto h = make_pack_backend(cfg, pool);
    auto stats = h.meta->pack_stats();
    CHECK_EQ(stats.size(), size_t(1));
    CHECK(stats[0].sealed);  // 构造时补封（abandon_stale_packs）
    CHECK_EQ(stats[0].file_size, uint64_t(0));  // 大小未知，0 占位
    uint64_t p1 = stats[0].pack_id;

    auto g = sync_wait(h.b->get_object("bkt", "old", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));  // 旧 pack 只是弃用，不影响读
    g.body.reset();

    put(*h.b, "bkt", "fresh", patterned(600));  // 新写入开新 pack（不复用旧 active）
    uint64_t p2 = h.meta->get_object("bkt", "fresh")->data.extents[0].file_id;
    CHECK(p2 != p1);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    sync_wait(h.b->delete_object("bkt", "old"));
    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_removed, uint64_t(1));  // 补封后旧 pack 才进得了整删候选
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    auto g2 = sync_wait(h.b->get_object("bkt", "fresh", std::nullopt));
    CHECK_EQ(read_all(*g2.body), patterned(600));
    sync_wait(h.b->close());
}

#endif  // LIGHTS3_DUOSTORE
