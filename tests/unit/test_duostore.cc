// DuoStore 专项单测（docs/duostore-backend.md §14）：编解码 roundtrip 与 run 边界、
// 跨 chunk 读写、位腐检出、GC 一期（P3）、pack 聚合（P2：分流/chunked 缓冲/轮转
// 封存/record 格式/crc/重启弃用/空 pack 整删）。meta 语义用例（GC 记账、号段单调、
// pack 存活账等）已接口化为 meta_store_suite（docs/duostore-redis-meta.md §9），
// RocksMetaStore 在此恒跑，redis/sqlite/tikv 在各自测试文件条件跑。
// 压实/崩溃注入专项随 P4 增补。
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

// pack record owner 的规范解析（docs/gaps.md §6.1）：三种历史形态收敛到唯一入口
TEST(duostore_parse_pack_owner_forms) {
    using codec::PackOwner;
    // parse_pack_owner 返回指向入参字节的 string_view：输入串必须活到断言结束
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

    // 不成形态一律 kUnknown（压实对其保守不迁）：空串、段数不符、part_no 非数字
    CHECK(codec::parse_pack_owner("").kind == PackOwner::Kind::kUnknown);
    CHECK(codec::parse_pack_owner("solo").kind == PackOwner::Kind::kUnknown);
    CHECK(codec::parse_pack_owner(std::string("mpu\0b\0k\0uid\0x7", 14)).kind ==
          PackOwner::Kind::kUnknown);
    CHECK(codec::parse_pack_owner(std::string("\0k", 2)).kind == PackOwner::Kind::kUnknown);
}

// schema 标记判定（docs/gaps.md §6.1 迁移钩子的纯前置）：等于当前直过、
// 比本构建新拒绝（防降级静默写坏）、乱码拒绝
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

// 共享判定 parse_schema_marker 的谱系前缀语义（redis "r"、tikv "t" 走同一入口）
TEST(duostore_schema_marker_lineage_prefixes) {
    CHECK_EQ(parse_schema_marker("r1", "r", 1, "t"), 1);
    CHECK_EQ(parse_schema_marker("t1", "t", 1, "t"), 1);
    CHECK_EQ(parse_schema_marker("r1", "r", 3, "t"), 1);  // 旧版本放行，交迁移链
    CHECK_THROWS_S3(parse_schema_marker("r2", "r", 1, "t"),
                    s3::S3ErrorCode::InternalError);  // 比本构建新
    CHECK_THROWS_S3(parse_schema_marker("t1", "r", 1, "t"),
                    s3::S3ErrorCode::InternalError);  // 谱系不符
    CHECK_THROWS_S3(parse_schema_marker("r", "r", 1, "t"), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(parse_schema_marker("r-1", "r", 1, "t"), s3::S3ErrorCode::InternalError);
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

// 后端级 metrics：GC 计数经 MetricsScope 落注册表，
// backend 标签来自装配侧；直构（默认空 scope）路径由其余 GC 用例覆盖
TEST(duostore_gc_metrics_registered) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "gcm");
    auto reg = std::make_shared<MetricsRegistry>();
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool,
                                               MetricsScope(reg, {{"backend", "gcm"}}));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));  // 3 chunk
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
    CHECK_EQ(sync_wait(b->list_multipart_uploads("bkt", {})).uploads.size(), size_t(0));
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
    CHECK_EQ(sync_wait(b->list_multipart_uploads("bkt", {})).uploads.size(), size_t(1));
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
    CHECK_EQ(sync_wait(b2->list_multipart_uploads("bkt", {})).uploads.size(), size_t(1));
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
    FsDataStore d(FsDataOptions{tmp.path / "duo", 4096, false, 0, 128ull << 20, 4, {}}, pool,
                  [&](Extent::Kind, uint32_t n) { uint64_t f = next_id; next_id += n; return f; });
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
    cfg.pack_max_age_sec = 0;  // 老化轮转默认关：布局断言按容量轮转才确定
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
    // 迁移回调同 cfg 构造的装配（migrate_pack_record 标准实现）；写侧 pin 钩子不接
    // ——压实用例的迁移 payload ≤ 阈值恒走 pack 路径，pins 传空即可
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

    // 存活账（meta 侧）：2 record，payload 1100B + 每条 28B record 头（22 固定 +
    // "bkt\0kN" owner，§2.3a 与 file_size 同口径），未封存
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

    // 大于阈值 → chunk 路径（pack 无新增）
    std::string big = patterned(2000);
    put(*h.b, "bkt", "big", big);
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(chunk_files_on_disk(cfg.root), size_t(1));
    auto g3 = sync_wait(h.b->get_object("bkt", "big", std::nullopt));
    CHECK_EQ(read_all(*g3.body), big);

    // multipart 小分片同样进 pack，owner = "mpu\0<b>\0<k>\0<id>\0<no>"（P4 §9.2：
    // 带 b/k 才能在 complete 后反查归属对象）
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

// P5 corruption 指标：GET 读路径 crc 失配经 on_corruption 回调落注册表——chunk 与
// pack 两处接入各计一次（cfg 构造装配；注入组装不接钩子，其余位腐用例不计数）
TEST(duostore_read_corruption_metric) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "corrupt");
    cfg.verify_chunk_crc = true;
    auto reg = std::make_shared<MetricsRegistry>();
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool,
                                               MetricsScope(reg, {{"backend", "corrupt"}}));
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "small", patterned(600));  // ≤ 阈值：pack record
    put(*b, "bkt", "big", patterned(5000));   // > 阈值：2 chunk（chunk_size 4KiB）
    CHECK(reg->render().find(
              "lights3_duostore_read_corruption_total{backend=\"corrupt\"} 0\n") !=
          std::string::npos);  // 构造期即注册，0 值可见

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
        f.seekp(-10, std::ios::end);  // payload 尾部（头在 record 首）
        f.put('!');
    }
    auto got2 = sync_wait(b->get_object("bkt", "small", std::nullopt));
    CHECK_THROWS_S3(read_all(*got2.body), s3::S3ErrorCode::InternalError);

    CHECK(reg->render().find(
              "lights3_duostore_read_corruption_total{backend=\"corrupt\"} 2\n") !=
          std::string::npos);
    sync_wait(b->close());
}

// P5 RocksDB 调参外露：尺寸/整数解析 + 范围校验（≥1）
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

// 多网关单实例执行门控（C4，docs/duostore-rados-data.md §8.3）：gc_enabled=false
// 只停后台 worker/孤儿扫描的排程，手动钩子（测试/运维通道）不受门控
TEST(duostore_config_gc_enabled_gates_background_only) {
    std::map<std::string, std::string> p{{"root", "/tmp/duo-cfg"}, {"gc_enabled", "false"}};
    auto c = DuoStoreConfig::from_params("t", p);
    CHECK(!c.gc_enabled);
    CHECK(DuoStoreConfig::from_params("t", {{"root", "/tmp/duo-cfg"}}).gc_enabled);  // 默认开
    p["gc_enabled"] = "not-a-bool";
    bool threw = false;
    try {
        DuoStoreConfig::from_params("t", p);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    // 行为：gc_enabled=false + gc_interval>0 也不排后台 worker；手动钩子照常回收
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
    CHECK_EQ(st.files_removed, uint64_t(2));  // 5000B / 4KiB = 2 chunk
    auto ost = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(ost.orphans_removed, uint64_t(0));
    sync_wait(b->close());
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

// ---------- P4 压实专项（§9.2/§15 P4 验收：低存活压实全绿）----------

namespace {

// pack 文件路径（与 FsDataStore 布局约定一致；测试直算免持 store 指针）
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

// 低存活压实收敛：存活率高不触发；跌破 pack_gc_ratio 后顺扫迁移存活 record 到
// active pack、swap 换 ref、空 pack 同轮整删（grace=0）；对象读取无缝
TEST(duostore_compact_low_liveness_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact");
    cfg.pack_max_size = 2048;  // 600B record ≈ 629B，3 条即满轮转
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    for (int i = 0; i < 4; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));  // P1 封存（k0-k2），P2 active（k3）

    uint64_t p1 = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;

    // 存活 3/3 与 2/3（0.64 ≥ 0.5）都不触发压实
    auto st0 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st0.packs_compacted, uint64_t(0));
    sync_wait(h.b->delete_object("bkt", "k0"));
    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(0));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    // 存活 1/3（0.32 < 0.5）→ 压实：k2 迁移、P1 空、同轮整删
    sync_wait(h.b->delete_object("bkt", "k1"));
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.packs_compacted, uint64_t(1));
    CHECK_EQ(st2.records_migrated, uint64_t(1));
    CHECK_EQ(st2.records_corrupt, uint64_t(0));
    CHECK_EQ(st2.packs_removed, uint64_t(1));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK(!find_pack_stat(*h.meta, p1).has_value());  // packstat 已销

    // 换 ref 后 k2 指向新 pack 且逐字节正确；k3 不受扰
    auto rec = h.meta->get_object("bkt", "k2");
    CHECK(rec.has_value());
    CHECK(rec->data.extents[0].file_id != p1);
    auto g = sync_wait(h.b->get_object("bkt", "k2", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    auto g3 = sync_wait(h.b->get_object("bkt", "k3", std::nullopt));
    CHECK_EQ(read_all(*g3.body), patterned(600));

    // 收敛：再跑一轮零动作
    auto st3 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st3.packs_compacted, uint64_t(0));
    CHECK_EQ(st3.packs_removed, uint64_t(0));
    sync_wait(h.b->close());
}

// 老化轮转（docs/gaps.md §6.1）：低写入量下 active pack 只按容量封存永不轮转，
// 其中的死区进不了压实候选集。seal_aged_packs 把逾龄 active pack 封存，file_size
// 如实回报（不是崩溃补封的 0），随后死区即可被压实回收
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
    CHECK(!before->sealed);  // 远未达 pack_max_size：容量判据不会封存它

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK_EQ(sync_wait(h.b->data_for_test().seal_aged_packs(10)), uint64_t(1));
    auto after = find_pack_stat(*h.meta, pid);
    CHECK(after.has_value());
    CHECK(after->sealed);
    CHECK(after->file_size > 0);  // 如实回报，不是重启补封的"未知 0"
    CHECK_EQ(after->live_recs, int64_t(3));

    // 幂等：无 active pack 可封时返回 0
    CHECK_EQ(sync_wait(h.b->data_for_test().seal_aged_packs(10)), uint64_t(0));

    // 封存后死区可回收：删到 1/3 存活即跌破 pack_gc_ratio，压实照常收敛
    sync_wait(h.b->delete_object("bkt", "k0"));
    sync_wait(h.b->delete_object("bkt", "k1"));
    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_compacted, uint64_t(1));
    CHECK_EQ(st.records_migrated, uint64_t(1));
    auto g = sync_wait(h.b->get_object("bkt", "k2", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    sync_wait(h.b->close());
}

// GC 轮内接线：pack_max_age 到点后 run_gc_once 自己会封存（不需要写入触发）
TEST(duostore_pack_age_rotation_runs_in_gc) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "agegc");
    cfg.pack_max_age_sec = 1;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k0", patterned(600));
    uint64_t pid = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;

    CHECK_EQ(sync_wait(h.b->run_gc_once()).packs_sealed_aged, uint64_t(0));  // 未到点
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    CHECK_EQ(sync_wait(h.b->run_gc_once()).packs_sealed_aged, uint64_t(1));
    CHECK(find_pack_stat(*h.meta, pid)->sealed);
    sync_wait(h.b->close());
}

// 压实预算与优先级（docs/gaps.md §6.1）：单轮封顶 N 个，且按可回收字节降序取——
// 此前是"一轮把全部符合条件的 pack 重写完"，批量删除后单轮可持锁数小时
TEST(duostore_compact_budget_prioritises_by_reclaimable) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "budget");
    cfg.pack_max_size = 2048;          // 600B record ≈ 629B，3 条即满轮转
    cfg.gc_compact_max_packs = 1;      // 每轮只做一个
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    // 三个满 pack（k0-k2 / k3-k5 / k6-k8）+ 一个 active（k9）
    for (int i = 0; i < 10; ++i) put(*h.b, "bkt", "k" + std::to_string(i), patterned(600));
    uint64_t p0 = h.meta->get_object("bkt", "k0")->data.extents[0].file_id;
    uint64_t p1 = h.meta->get_object("bkt", "k3")->data.extents[0].file_id;
    uint64_t p2 = h.meta->get_object("bkt", "k6")->data.extents[0].file_id;
    CHECK(p0 != p1 && p1 != p2);

    // p0 剩 2 条存活、p1 剩 1 条、p2 剩 1 条 —— 三者都够格，但可回收字节 p1/p2 > p0
    sync_wait(h.b->delete_object("bkt", "k0"));
    sync_wait(h.b->delete_object("bkt", "k1"));
    sync_wait(h.b->delete_object("bkt", "k3"));
    sync_wait(h.b->delete_object("bkt", "k4"));
    sync_wait(h.b->delete_object("bkt", "k6"));
    sync_wait(h.b->delete_object("bkt", "k7"));

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(1));       // 预算封顶
    CHECK_EQ(st1.packs_compact_deferred, uint64_t(2));  // 其余顺延

    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.packs_compacted, uint64_t(1));
    CHECK_EQ(st2.packs_compact_deferred, uint64_t(1));

    auto st3 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st3.packs_compacted, uint64_t(1));
    CHECK_EQ(st3.packs_compact_deferred, uint64_t(0));

    // 收敛后数据完好
    auto st4 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st4.packs_compacted, uint64_t(0));
    for (int i : {2, 5, 8, 9}) {
        auto g = sync_wait(h.b->get_object("bkt", "k" + std::to_string(i), std::nullopt));
        CHECK_EQ(read_all(*g.body), patterned(600));
    }
    sync_wait(h.b->close());
}

// 死 record 损坏不阻压实（§10）：存活账证明损坏者已死——存活者照常迁移、
// live 归零后空 pack 整删（含损坏死区）不丢任何数据
TEST(duostore_compact_corrupt_dead_record) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-cdead");
    cfg.pack_max_size = 1400;  // 2 条即满（第 3 条触发轮转封存）
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));
    put(*h.b, "bkt", "k2", patterned(600));
    auto old2 = h.meta->get_object("bkt", "k2")->data.extents[0];  // 覆盖前的 P1 定位
    put(*h.b, "bkt", "k2", patterned(500));  // 第 3 条 → P1 封存，新值进 P2；旧 k2 成死区
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    corrupt_file_at(pack_file_path(cfg.root, old2.file_id), old2.offset + 10);  // 损坏死区
    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_compacted, uint64_t(1));
    CHECK_EQ(st.records_corrupt, uint64_t(1));   // 死区损坏：告警跳过
    CHECK_EQ(st.records_migrated, uint64_t(1));  // 存活 k1 照常迁移
    CHECK_EQ(st.packs_removed, uint64_t(1));     // live 归零 → 空 pack（含死区）整删
    auto g1 = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));
    CHECK_EQ(read_all(*g1.body), patterned(600));
    auto g2 = sync_wait(h.b->get_object("bkt", "k2", std::nullopt));
    CHECK_EQ(read_all(*g2.body), patterned(500));
    sync_wait(h.b->close());
}

// 存活 record 损坏 → 迁移不了、live 不归零 → 保留原 pack 不删（人工介入，§10）；
// 账无推进 + 冷却窗内不重扫（compact_blocked 记忆）
TEST(duostore_compact_corrupt_live_record_keeps_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-clive");
    cfg.pack_max_size = 1400;
    cfg.gc_grace_sec = 3600;  // 冷却窗生效（也用于验证跳过重扫）
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));
    put(*h.b, "bkt", "k2", patterned(600));
    put(*h.b, "bkt", "filler", patterned(600));  // P1 封存
    sync_wait(h.b->delete_object("bkt", "k1"));  // P1 存活 1/2 → 候选

    auto live2 = h.meta->get_object("bkt", "k2")->data.extents[0];
    corrupt_file_at(pack_file_path(cfg.root, live2.file_id), live2.offset + 10);  // 损坏存活者

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(1));
    CHECK_EQ(st1.records_corrupt, uint64_t(1));
    CHECK_EQ(st1.records_migrated, uint64_t(0));
    CHECK_EQ(st1.packs_removed, uint64_t(0));  // live>0：pack 保留（不丢注定要人工救的数据）
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    // 账无推进 + 冷却窗内：下一轮跳过重扫
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.packs_compacted, uint64_t(0));
    sync_wait(h.b->close());
}

// mpu 分片的压实（§9.2 owner 提示）：进行中 upload 的 pack 分片不可迁（保守阻塞、
// pack 保留）；complete 后凭 owner 内嵌的 b/k 反查归属对象 → 迁移解锁、pack 回收
TEST(duostore_compact_mpu_part_blocks_then_migrates_after_complete) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-mpu");
    cfg.pack_max_size = 1400;
    // live 计头后（§2.3a）分片 record 的 owner 头较长：删 f1 后存活占比 ≈ 0.52，
    // 阈值抬到 0.6 保持"该 pack 是压实候选"的测试语境
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
    put(*h.b, "bkt", "f2", patterned(600));  // P1（part+f1）封存，f2 进 P2
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));
    sync_wait(h.b->delete_object("bkt", "f1"));  // P1 存活 = 进行中分片 1 条 → 候选

    auto st1 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st1.packs_compacted, uint64_t(1));
    CHECK_EQ(st1.records_migrated, uint64_t(0));  // 进行中 mpu：对象不存在 → 保守不迁
    CHECK_EQ(st1.packs_removed, uint64_t(0));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(2));

    std::vector<PartInfo> parts = {{1, etag}};
    sync_wait(h.b->complete_multipart("bkt", "mp", id, parts));

    // complete 后（grace=0 无冷却）：owner 的 b/k 提示反查对象成功 → 迁移 + 整删
    auto st2 = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st2.records_migrated, uint64_t(1));
    CHECK_EQ(st2.packs_removed, uint64_t(1));
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    auto g = sync_wait(h.b->get_object("bkt", "mp", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    sync_wait(h.b->close());
}

// 混合对象（chunk + pack extent）压实后 chunk 的 refs 必须留存（storage.md 高危
// 第一条）：swap_extents 的 to/from 共享未迁移的 chunk，整加再整删在 refs 的
// last-wins 语义下净效果是删除 → 孤儿扫描随后 unlink 活数据
TEST(duostore_compact_mixed_object_keeps_chunk_refs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "compact-mixed");
    cfg.pack_max_size = 1400;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));

    // 混合 MPU：part1 大（走 chunk）、part2 小（走 pack）
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
    CHECK(has_pack && !chunk_ids.empty());  // 确实是混合对象
    for (uint64_t cid : chunk_ids) CHECK(h.meta->chunk_referenced(cid));

    // 让该 pack 存活率跌破阈值 → 压实迁移那一个 pack extent
    put(*h.b, "bkt", "f1", patterned(600));
    put(*h.b, "bkt", "f2", patterned(600));  // 触发轮转封存
    sync_wait(h.b->delete_object("bkt", "f1"));
    auto st = sync_wait(h.b->run_gc_once());
    CHECK(st.records_migrated >= uint64_t(1));

    // 未迁移的 chunk 仍被对象引用：refs 表项不得被 swap 抹掉
    auto after = h.meta->get_object("bkt", "mixed");
    CHECK(after.has_value());
    for (uint64_t cid : chunk_ids) CHECK(h.meta->chunk_referenced(cid));

    // 孤儿扫描不得把它们当无引用文件删掉，对象内容仍逐字节正确
    auto os = sync_wait(h.b->run_orphan_scan_once());
    CHECK_EQ(os.orphans_removed, uint64_t(0));
    auto g = sync_wait(h.b->get_object("bkt", "mixed", std::nullopt));
    CHECK_EQ(read_all(*g.body), big + small);
    sync_wait(h.b->close());
}

// P0 §1.4：另一实例正在写的 active pack 不得被补封（补封 → 压实全量重写 →
// 账归零后整删，而对方仍持 fd 追加 = 静默数据丢失）。用第二个 FsDataStore 持有
// 同一 root 上的 active pack 模拟"另一个网关"，验证 pack_write_locked 能识别
TEST(duostore_active_pack_of_other_writer_not_sealed) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-owner");
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k1", patterned(600));  // 建一个 active（未封存）pack

    uint64_t pack_id = h.meta->get_object("bkt", "k1")->data.extents[0].file_id;
    auto stats = h.meta->pack_stats();
    bool unsealed = false;
    for (const auto& ps : stats)
        if (ps.pack_id == pack_id && !ps.sealed) unsealed = true;
    CHECK(unsealed);  // 前提：该 pack 确实处于 active 态

    // 本实例持有写锁 → 探测应报 "正被写"
    CHECK(h.b->data_for_test().pack_write_locked(pack_id));

    // 关掉本实例（fd 关闭 → 锁释放）后，同一 pack 不再被判为在写
    sync_wait(h.b->close());
    FsDataOptions probe_opt{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc,
                            cfg.pack_threshold, cfg.pack_max_size, cfg.pack_writers, {}};
    FsDataStore probe(probe_opt, pool, [](Extent::Kind, uint32_t) -> uint64_t { return 0; });
    CHECK(!probe.pack_write_locked(pack_id));
    sync_wait(probe.close());
}

// 崩溃遗留 pack 的分母回填（gaps §2.3b）：补封 seal(0) 后首轮 GC 用 stat_pack 一次
// stat 回填 file_size——健康存活率的 pack 不再无条件进全量顺扫重写
TEST(duostore_gc_stat_backfills_crash_leftover_pack) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "pack-statfill");
    {
        auto h = make_pack_backend(cfg, pool);
        sync_wait(h.b->create_bucket("bkt"));
        put(*h.b, "bkt", "k1", patterned(600));
        put(*h.b, "bkt", "k2", patterned(600));
        // 不 close：析构兜底（等价崩溃遗留，重开后补封 seal(0)）
    }
    auto h = make_pack_backend(cfg, pool);
    uint64_t pid = h.meta->get_object("bkt", "k1")->data.extents[0].file_id;
    auto ps0 = find_pack_stat(*h.meta, pid);
    CHECK(ps0->sealed);
    CHECK_EQ(ps0->file_size, uint64_t(0));  // 补封时大小未知

    auto st = sync_wait(h.b->run_gc_once());
    CHECK_EQ(st.packs_compacted, uint64_t(0));  // 100% 存活：回填后按存活率跳过重写
    auto ps1 = find_pack_stat(*h.meta, pid);
    CHECK_EQ(ps1->file_size, uint64_t(fs::file_size(pack_file_path(cfg.root, pid))));

    auto g = sync_wait(h.b->get_object("bkt", "k1", std::nullopt));
    CHECK_EQ(read_all(*g.body), patterned(600));
    sync_wait(h.b->close());
}

// rewrite_pack 顺扫语义（store 级，无迁移回调）：统计、file_size 回报、torn tail
// 静默止扫（重启弃用的预期形态）、magic 损坏响亮止扫
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
        // torn tail 注入：截断的 record 头（崩溃残迹）
        std::ofstream f(pack_file_path(tmp.path / "duo", e1.file_id),
                        std::ios::binary | std::ios::app);
        f << "LP3R" << std::string(7, '\x5a');
    }
    auto rw = sync_wait(d.rewrite_pack(e1.file_id));
    CHECK_EQ(rw.scanned, uint64_t(2));
    CHECK_EQ(rw.migrated, uint64_t(0));  // 无迁移回调：只扫不迁
    CHECK_EQ(rw.corrupt, uint64_t(0));   // torn tail 不计损坏
    CHECK_EQ(rw.file_size, real_size + 11);

    corrupt_file_at(pack_file_path(tmp.path / "duo", e1.file_id), 0);  // magic 损坏
    auto rw2 = sync_wait(d.rewrite_pack(e1.file_id));
    CHECK_EQ(rw2.scanned, uint64_t(0));  // 无法重同步：响亮止扫
    CHECK_EQ(rw2.corrupt, uint64_t(1));
    sync_wait(d.close());
    meta.close();
}

// P4 前旧格式 mpu owner（"mpu\0<id>\0<no>"，无 b/k）：不可反查 → 保守不迁，
// pack 保留、对象照常可读；新格式（带 b/k）经对象反查正常迁移——两代盘面共存安全
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
        // 旧格式（模拟 P4 前遗留盘面）与新格式各一条，都归属 complete 后的对象
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
        sync_wait(d1.close());  // 封存 pack（迁移目标须是另一个 active pack）
    }
    FsDataStore d2(opt, pool, [&](Extent::Kind k, uint32_t n) { return meta.alloc_file_run(k, n); },
                   [&](uint64_t id, uint64_t sz) { meta.seal_pack(id, sz); },
                   [&](IDataStore& ds, std::vector<PackScanRecord>&& batch) {
                       return migrate_pack_records(meta, ds, nullptr, std::move(batch));
                   });
    auto rw = sync_wait(d2.rewrite_pack(pack_id));
    CHECK_EQ(rw.scanned, uint64_t(2));
    CHECK_EQ(rw.migrated, uint64_t(1));  // 新格式迁移；旧格式保守搁置
    CHECK(meta.get_object("bkt", "kold")->data.extents[0].file_id == pack_id);  // ref 未动
    CHECK(meta.get_object("bkt", "knew")->data.extents[0].file_id != pack_id);  // 已换 ref
    // 两个对象都可读且内容正确
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

// ---------- P4 孤儿扫描专项（§9.3）----------

// 正向：盘上无引用且逾宽限 → unlink（在册对象不受扰）；反向：refs 在而文件缺 →
// 告警计数、绝不删 meta；grace 挡新写
TEST(duostore_orphan_scan_forward_and_reverse) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "orphan");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "k", patterned(10000));  // 3 chunk 在册

    // 孤儿注入：崩溃残留形态（盘上有文件、meta 无账）；id 取远端避开号段
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

    // 反向：手工删一个在册 chunk 文件 → 只告警计数，meta 保持（人工介入线索）
    uint64_t lost = 0;
    {
        auto rec = sync_wait(b->head_object("bkt", "k"));
        (void)rec;
    }
    {
        // 经孤儿扫描枚举拿到一个在册 id（与 refs 交集必非空）
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
    CHECK(sync_wait(b->head_object("bkt", "k")).size == 10000);  // meta 未被动
    sync_wait(b->close());

    // grace 挡新写：宽限内的无引用文件不动
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

// packs/ 双向对账（docs/gaps.md §6.1）：pack 文件在"建文件"时就存在、packstat 行
// 要到首条 record 提交才落账——恰在这个窗口硬崩，文件永久泄漏且不在任何账里。
// 正向：账外 pack 文件逾宽限即删；反向：packstat 在而文件缺 → 只告警不销账
TEST(duostore_orphan_scan_reconciles_packs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = pack_cfg(tmp, "orphanpack");
    cfg.orphan_scan_interval_sec = 0;
    auto h = make_pack_backend(cfg, pool);
    sync_wait(h.b->create_bucket("bkt"));
    put(*h.b, "bkt", "k", patterned(600));
    uint64_t live_pack = h.meta->get_object("bkt", "k")->data.extents[0].file_id;

    // 账外 pack 文件（"建文件后、首条 record 提交前崩溃"的残迹）
    fs::create_directories(cfg.root / "packs" / "ab");
    std::ofstream(cfg.root / "packs" / "ab" / "000000000000abcd.pak") << "leaked";

    auto st1 = sync_wait(h.b->run_orphan_scan_once());
    CHECK_EQ(st1.packs_scanned, uint64_t(2));
    CHECK_EQ(st1.orphan_packs_removed, uint64_t(1));
    CHECK_EQ(st1.pack_stats_missing, uint64_t(0));
    CHECK(st1.pack_bytes > 0);
    CHECK(!fs::exists(cfg.root / "packs" / "ab" / "000000000000abcd.pak"));
    // 在账 pack 不受扰——它还是本进程正在写的 active pack
    CHECK_EQ(pack_files_on_disk(cfg.root), size_t(1));
    CHECK_EQ(read_all(*sync_wait(h.b->get_object("bkt", "k", std::nullopt)).body),
             patterned(600));

    // 反向：手工删掉在账 pack 文件 → 计数 + 告警，packstat 保留供人工介入
    sync_wait(h.b->close());  // 先关掉写者，否则删的是本进程持锁的 active pack
    auto h2 = make_pack_backend(cfg, pool);
    fs::remove(pack_file_path(cfg.root, live_pack));
    auto st2 = sync_wait(h2.b->run_orphan_scan_once());
    CHECK_EQ(st2.packs_scanned, uint64_t(0));
    CHECK_EQ(st2.pack_stats_missing, uint64_t(1));
    CHECK(find_pack_stat(*h2.meta, live_pack).has_value());  // 账未被动
    sync_wait(h2.b->close());
}

namespace {

// 可拦停的 body：吐出 first 后阻塞等 release，再吐 rest——制造"写入中、meta 未
// 提交"的长窗口（写侧 pin 的防护对象，§9.3）
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
            gate_.acquire();  // 阻塞池线程直至 release()（池 >1 线程，扫描不受阻）
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

// 写侧 pin（§9.3）：慢流式 PUT 已落盘、未提交 meta 的 chunk 不被孤儿扫描误删
// ——mtime 宽限对超长写不充分，pin 是硬防线（grace=0 下唯一防线）
TEST(duostore_orphan_scan_write_pin_protects_inflight_put) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cfg = gc_cfg(tmp, "orphan-pin");
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool);
    sync_wait(b->create_bucket("bkt"));

    std::string body_data = patterned(9000);
    GatedReader body(body_data.substr(0, 5000), body_data.substr(5000));
    std::thread writer([&] { sync_wait(b->put_object("bkt", "slow", {}, body)); });

    // 等首个 chunk 落盘（4096 切片：5000B → chunk0 封存 + chunk1 在写）
    for (int i = 0; i < 200 && chunk_files_on_disk(cfg.root) < 1; ++i) usleep(20 * 1000);
    size_t on_disk = chunk_files_on_disk(cfg.root);
    CHECK(on_disk >= 1);

    auto st1 = sync_wait(b->run_orphan_scan_once());
    CHECK(st1.skipped_pinned >= 1);  // grace=0：写侧 pin 是唯一防线
    CHECK_EQ(st1.orphans_removed, uint64_t(0));
    CHECK(chunk_files_on_disk(cfg.root) >= on_disk);

    body.release();
    writer.join();

    // 提交后：refs 在账、pin 已解，再扫零动作；内容完整
    auto st2 = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(st2.orphans_removed, uint64_t(0));
    CHECK_EQ(st2.skipped_pinned, uint64_t(0));
    CHECK_EQ(st2.refs_missing, uint64_t(0));
    auto g = sync_wait(b->get_object("bkt", "slow", std::nullopt));
    CHECK_EQ(read_all(*g.body), body_data);
    sync_wait(b->close());
}

// ---------- P4 崩溃注入（§15 P4 验收：kill -9 重启收敛）----------
// 子进程（execv 自身进 duostore-crash-child 模式）在指定点 _exit / 被 SIGKILL
// ——两者对进程状态等价（无析构无 flush）；父进程重开同 root 验证：已提交对象
// 逐字节存活（meta_sync=true 的提交契约）、GC + 孤儿扫描把残迹收敛到零

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
    cfg.meta_sync = true;  // 崩溃语义主角：提交点 = WAL fsync（§6）
    cfg.gc_interval_sec = 0;
    cfg.orphan_scan_interval_sec = 0;
    cfg.gc_grace_sec = 0;
    return cfg;
}

// 吐满 limit 字节后在 read() 内 _exit——PUT 泵送中途崩溃（数据半落盘、meta 无账）
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
        // pack 小对象 + 多 chunk 大对象 + multipart complete，各提交路径全走一遍
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
        return 3;  // 不可达：body 在 9000 字节处 _exit
    } else if (mode == "afterdelete") {
        put(*b, "bkt", "gone", patterned(10000));
        sync_wait(b->delete_object("bkt", "gone"));  // gcq 已入账、文件未回收
    } else if (mode == "spin") {
        // 循环提交并回报；父进程随机时刻 SIGKILL。行在 put 返回（WAL fsync）后才
        // 写出——凡父进程读到整行，重启后该对象必须存在
        for (int i = 0;; ++i) {
            size_t n = (i % 2) ? 600 : 10000;
            put(*b, "bkt", "k" + std::to_string(i), patterned(n));
            std::string line = "ok " + std::to_string(i) + "\n";
            if (::write(1, line.data(), line.size()) < 0) return 4;
        }
    } else {
        return 2;
    }
    ::_exit(0);  // 等价 kill -9：不 close、不析构（WAL 重放与 active pack 弃用留给重启）
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

// 重启收敛验证：GC + 孤儿扫描跑到不动点后，再各跑一轮零动作
void verify_converged(DuoStoreBackend& b) {
    sync_wait(b.run_gc_once());
    sync_wait(b.run_orphan_scan_once());
    sync_wait(b.run_gc_once());  // 压实解锁的空 pack 等二阶效应再变现一轮
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

// 提交后崩溃：pack/多 chunk/multipart 三条提交路径的对象重启后逐字节存活；
// 崩溃残迹（未封存 active pack、孤儿）收敛到零；收敛后（含压实迁移）内容仍正确
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
    check_body(*b, "small", patterned(600));  // 复核：收敛（含压实迁移）不动内容
    check_body(*b, "big", patterned(10000));
    check_body(*b, "mp", patterned(6000) + patterned(6000));
    sync_wait(b->close());
}

// PUT 泵送中途崩溃：victim 无账（NoSuchKey），半落盘 chunk 成孤儿被扫净；
// 先行提交的对象不受扰
TEST(duostore_crash_mid_put_leaves_no_garbage) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    CHECK_EQ(wait_child(spawn_crash_child("midput", root)), 0);

    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "victim")), s3::S3ErrorCode::NoSuchKey);
    check_body(*b, "before", patterned(600));
    // 9000B / 4096 切片 → 2 个封存 chunk + 1 个在写 chunk 全部无账
    auto os = sync_wait(b->run_orphan_scan_once());
    CHECK_EQ(os.orphans_removed, uint64_t(3));
    CHECK_EQ(os.refs_missing, uint64_t(0));
    CHECK_EQ(chunk_files_on_disk(root), size_t(0));  // before 是 pack 对象，chunks/ 应净
    verify_converged(*b);
    check_body(*b, "before", patterned(600));
    sync_wait(b->close());
}

// 删除入账后崩溃：gcq 残账重启后由 GC 变现（先物理删后销账的崩溃安全侧）
TEST(duostore_crash_after_delete_converges) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    CHECK_EQ(wait_child(spawn_crash_child("afterdelete", root)), 0);

    auto pool = std::make_shared<ThreadPool>(4);
    auto b = std::make_shared<DuoStoreBackend>(crash_cfg(root), pool);
    CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "gone")), s3::S3ErrorCode::NoSuchKey);
    auto gc = sync_wait(b->run_gc_once());
    CHECK_EQ(gc.reclaims_acked, uint64_t(1));
    CHECK_EQ(gc.files_removed, uint64_t(3));  // 10000B / 4096 → 3 chunk
    CHECK_EQ(chunk_files_on_disk(root), size_t(0));
    verify_converged(*b);
    sync_wait(b->close());
}

// 随机时刻 SIGKILL：凡子进程回报过的提交（行在 WAL fsync 后写出）重启后必须
// 逐字节存在——真 kill -9 下的持久性契约；残迹照常收敛
TEST(duostore_crash_random_sigkill_keeps_reported_commits) {
    TmpDir tmp;
    fs::path root = tmp.path / "duo";
    int fd = -1;
    pid_t pid = spawn_crash_child("spin", root, &fd);

    // 等到首个提交回报后再随机点开杀（保证测试非空转）
    std::string buf;
    char c;
    while (buf.find('\n') == std::string::npos && ::read(fd, &c, 1) == 1) buf.push_back(c);
    CHECK(buf.find('\n') != std::string::npos);
    usleep((200 + unsigned(::getpid()) % 500) * 1000);  // 200-700ms 随机窗口
    CHECK_EQ(::kill(pid, SIGKILL), 0);
    int stat = wait_child(pid);
    CHECK(WIFSIGNALED(stat) && WTERMSIG(stat) == SIGKILL);
    // 排空管道；只认完整行
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
        if (nl == std::string::npos) break;  // 尾部半行：不作数
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

// gaps §3.9：chunk id 按几何 run 批取——即使分配器被并发写者穿插烧号（run 之间
// 不连续），同对象的 chunk id 在每个 run 内仍连续，manifest 的 run 编码不失效
TEST(duostore_chunk_ids_batched_in_runs) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    uint64_t next_id = 1;
    FsDataStore d(FsDataOptions{tmp.path / "duo", 64, false, 0, 128ull << 20, 4, {}}, pool,
                  [&](Extent::Kind, uint32_t n) {
                      uint64_t f = next_id;
                      next_id += n + 7;  // 模拟并发写者在两次批取之间消耗 id
                      return f;
                  });
    auto w = sync_wait(d.open_writer({std::nullopt, "t/runs"}));
    std::string body(64 * 7, 'x');  // 7 个 chunk：run 序列 1, 2, 4
    sync_wait(
        w->write(std::span(reinterpret_cast<const std::byte*>(body.data()), body.size())));
    auto ref = sync_wait(w->finish());
    CHECK_EQ(ref.extents.size(), size_t(7));
    CHECK_EQ(ref.extents[2].file_id, ref.extents[1].file_id + 1);  // run(2) 内连续
    for (size_t i = 4; i <= 6; ++i)                                // run(4) 内连续
        CHECK_EQ(ref.extents[i].file_id, ref.extents[3].file_id + (i - 3));
    CHECK(ref.extents[1].file_id != ref.extents[0].file_id + 1);  // 穿插确实发生了
    sync_wait(d.close());
}

#endif  // LIGHTS3_DUOSTORE
