// DuoStore 专项单测（docs/duostore-backend.md §14）：编解码 roundtrip 与 run 边界、
// list 的 delimiter Seek 跳跃与分页 token、meta 事务的 GC 记账、号段重启单调、
// 跨 chunk 读写。pack/GC 变现/压实/崩溃注入专项随 P2-P4 增补。
#ifdef LIGHTS3_DUOSTORE

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "core/thread_pool.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/rocks_meta_store.h"
#include "unit/backend_suite.h"
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

// 覆盖写/删除的 GC 记账（§4.5 同批不变量）：gcq 入账、refs 增删、version 递增
TEST(duostore_meta_gc_accounting) {
    TmpDir tmp;
    RocksMetaStore m(meta_opts(tmp));
    m.create_bucket("b");

    uint64_t id1 = m.alloc_file_id(Extent::Kind::kChunk);
    m.put_object("b", "k", make_rec("k", {chunk_extent(id1, 3)}));
    CHECK(m.chunk_referenced(id1));
    CHECK_EQ(m.peek_reclaims(10).size(), size_t(0));
    CHECK_EQ(m.get_object("b", "k")->version, uint64_t(1));

    // 覆盖写：旧 extent 入 gcq、旧 refs 删除、新 refs 建立
    uint64_t id2 = m.alloc_file_id(Extent::Kind::kChunk);
    m.put_object("b", "k", make_rec("k", {chunk_extent(id2, 5)}));
    auto rs = m.peek_reclaims(10);
    CHECK_EQ(rs.size(), size_t(1));
    CHECK_EQ(rs[0].second.extents.at(0).file_id, id1);
    CHECK(!m.chunk_referenced(id1));
    CHECK(m.chunk_referenced(id2));
    CHECK_EQ(m.get_object("b", "k")->version, uint64_t(2));

    // 删除：入 gcq；幂等二次删返回 false
    CHECK(m.delete_object("b", "k"));
    CHECK(!m.delete_object("b", "k"));
    CHECK(!m.chunk_referenced(id2));
    auto rs2 = m.peek_reclaims(10);
    CHECK_EQ(rs2.size(), size_t(2));

    // 销账后 gcq 清空
    for (auto& [seq, rec] : rs2) m.ack_reclaim(seq);
    CHECK_EQ(m.peek_reclaims(10).size(), size_t(0));
    m.close();
}

// file_id 号段：重启后不回退（§4.5——只需唯一单调，不需连续）
TEST(duostore_alloc_monotonic_across_reopen) {
    TmpDir tmp;
    uint64_t last = 0;
    {
        RocksMetaStore m(meta_opts(tmp));
        for (int i = 0; i < 10; ++i) last = m.alloc_file_id(Extent::Kind::kChunk);
        m.close();
    }
    {
        RocksMetaStore m(meta_opts(tmp));
        CHECK(m.alloc_file_id(Extent::Kind::kChunk) > last);
        // pack 与 chunk 计数器相互独立
        CHECK_EQ(m.alloc_file_id(Extent::Kind::kPack), uint64_t(0));
        m.close();
    }
}

// delete_bucket：有进行中 multipart 时拒绝（对齐 AWS；防 refs 永久泄漏与幽灵上传复活）
TEST(duostore_meta_delete_bucket_blocks_on_mpu) {
    TmpDir tmp;
    RocksMetaStore m(meta_opts(tmp));
    m.create_bucket("b");
    ObjectMeta meta;
    auto id = m.create_upload("b", "k", meta);
    CHECK_THROWS_S3(m.delete_bucket("b"), s3::S3ErrorCode::BucketNotEmpty);
    m.abort_upload("b", "k", id);
    m.delete_bucket("b");  // abort 后可删
    CHECK(!m.bucket_exists("b"));
    m.close();
}

// max-keys=0：S3 语义为空结果 + IsTruncated=false（否则空 token 使客户端死循环）
TEST(duostore_meta_list_max_keys_zero) {
    TmpDir tmp;
    RocksMetaStore m(meta_opts(tmp));
    m.create_bucket("b");
    m.put_object("b", "k", make_rec("k", {}));
    ListOptions opt;
    opt.max_keys = 0;
    auto r = m.list_objects("b", opt);
    CHECK_EQ(r.objects.size(), size_t(0));
    CHECK(!r.is_truncated);
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

// list：delimiter 组的 Seek 跳跃 + 分页 token 落在组尾（§4.4）
TEST(duostore_meta_list_delimiter_paging) {
    TmpDir tmp;
    RocksMetaStore m(meta_opts(tmp));
    m.create_bucket("b");
    for (auto k : {"a", "b/1", "b/2", "b/3", "c"}) m.put_object("b", k, make_rec(k, {}));

    ListOptions opt;
    opt.delimiter = "/";
    opt.max_keys = 2;
    auto p1 = m.list_objects("b", opt);
    CHECK_EQ(p1.objects.size(), size_t(1));
    CHECK_EQ(p1.objects[0].key, "a");
    CHECK_EQ(p1.common_prefixes.size(), size_t(1));
    CHECK_EQ(p1.common_prefixes[0], "b/");
    CHECK(p1.is_truncated);
    CHECK_EQ(p1.next_token, "b/3");  // token 语义（start after）须落在组尾

    opt.start_after = p1.next_token;
    auto p2 = m.list_objects("b", opt);
    CHECK_EQ(p2.objects.size(), size_t(1));
    CHECK_EQ(p2.objects[0].key, "c");
    CHECK(p2.common_prefixes.empty());
    CHECK(!p2.is_truncated);

    // prefix + delimiter：子层级分组
    ListOptions sub;
    sub.prefix = "b/";
    sub.delimiter = "/";
    auto ps = m.list_objects("b", sub);
    CHECK_EQ(ps.objects.size(), size_t(3));
    CHECK(ps.common_prefixes.empty());
    m.close();
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

#endif  // LIGHTS3_DUOSTORE
