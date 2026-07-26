// meta store 一致性套件（docs/duostore-redis-meta.md §9）：同一组用例参数化跑
// 所有 IMetaStore 实现（RocksMetaStore 恒跑、RedisMetaStore 条件跑），两实现
// 共享同一语义基线。自 test_duostore.cc 的 meta 用例提取。
// factory 约定：每次调用在同一底层存储上打开一个新实例（"重启"语义）；套件内
// 各场景用互不相同的 bucket 名隔离，实例串行 open/close（RocksDB 单进程锁）。
#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/meta_store.h"
#include "unit/mini_test.h"

namespace meta_store_suite {

using namespace lights3::storage;
using namespace lights3::storage::duostore;

using MetaFactory = std::function<std::unique_ptr<IMetaStore>()>;

inline Extent chunk_extent(uint64_t id, uint64_t len, uint32_t crc = 0) {
    return {Extent::Kind::kChunk, id, 0, len, crc};
}

inline Extent pack_extent(uint64_t pack_id, uint64_t off, uint64_t len, uint32_t crc = 0) {
    return {Extent::Kind::kPack, pack_id, off, len, crc};
}

inline std::optional<PackStat> find_pack(IMetaStore& m, uint64_t pack_id) {
    for (const auto& ps : m.pack_stats())
        if (ps.pack_id == pack_id) return ps;
    return std::nullopt;
}

// 用例末尾清空 gcq（后续用例对队列状态的断言不受本例遗留影响）
inline void drain_gcq(IMetaStore& m) {
    for (;;) {
        auto rs = m.peek_reclaims(256);
        if (rs.empty()) return;
        std::vector<uint64_t> seqs;
        for (auto& [seq, rc] : rs) seqs.push_back(seq);
        m.ack_reclaims(seqs);
    }
}

inline ObjectRec make_rec(std::string key, std::vector<Extent> extents) {
    ObjectRec rec;
    rec.meta.key = std::move(key);
    rec.meta.etag = "deadbeef";
    rec.meta.last_modified = std::chrono::system_clock::now();
    rec.data.extents = std::move(extents);
    rec.meta.size = rec.data.total();
    return rec;
}

// 覆盖写/删除的 GC 记账（主文档 §4.5 同批不变量）：gcq 入账、refs 增删、version 递增
inline void case_gc_accounting(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-gc");

    uint64_t id1 = m->alloc_file_id(Extent::Kind::kChunk);
    m->put_object("ms-gc", "k", make_rec("k", {chunk_extent(id1, 3)}));
    CHECK(m->chunk_referenced(id1));
    CHECK_EQ(m->peek_reclaims(10).size(), size_t(0));
    CHECK_EQ(m->get_object("ms-gc", "k")->version, uint64_t(1));

    // 覆盖写：旧 extent 入 gcq、旧 refs 删除、新 refs 建立
    uint64_t id2 = m->alloc_file_id(Extent::Kind::kChunk);
    m->put_object("ms-gc", "k", make_rec("k", {chunk_extent(id2, 5)}));
    auto rs = m->peek_reclaims(10);
    CHECK_EQ(rs.size(), size_t(1));
    CHECK_EQ(rs[0].second.extents.at(0).file_id, id1);
    CHECK(rs[0].second.enqueue_ms > 0);  // 入队时刻回传（GC 消费端判 gc_grace，§9.1）
    CHECK(!m->chunk_referenced(id1));
    CHECK(m->chunk_referenced(id2));
    CHECK_EQ(m->get_object("ms-gc", "k")->version, uint64_t(2));

    // 删除：入 gcq；幂等二次删返回 false
    CHECK(m->delete_object("ms-gc", "k"));
    CHECK(!m->delete_object("ms-gc", "k"));
    CHECK(!m->chunk_referenced(id2));
    auto rs2 = m->peek_reclaims(10);
    CHECK_EQ(rs2.size(), size_t(2));
    // min_seq 断点续扫（§9.1）：从第二项 seq 起 peek 只见后续；越过队尾即空
    auto tail = m->peek_reclaims(10, rs2[1].first);
    CHECK_EQ(tail.size(), size_t(1));
    CHECK_EQ(tail[0].first, rs2[1].first);
    CHECK_EQ(m->peek_reclaims(10, rs2[1].first + 1).size(), size_t(0));

    // 销账后 gcq 清空：逐条与批量接口各走一遍（批量为 GC 消费端的首选形态）
    m->ack_reclaim(rs2[0].first);
    std::vector<uint64_t> rest;
    for (size_t i = 1; i < rs2.size(); ++i) rest.push_back(rs2[i].first);
    m->ack_reclaims(rest);
    CHECK_EQ(m->peek_reclaims(10).size(), size_t(0));
    m->delete_bucket("ms-gc");
    m->close();
}

// file_id 号段：重启后不回退（只需唯一单调，不需连续——绝对值是实现细节：
// RocksDB 从 0 起，Redis 首段空烧从 kIdSegment 起，docs/duostore-redis-meta.md §4）
inline void case_alloc_monotonic_across_reopen(const MetaFactory& make) {
    uint64_t last = 0;
    {
        auto m = make();
        for (int i = 0; i < 10; ++i) last = m->alloc_file_id(Extent::Kind::kChunk);
        m->close();
    }
    {
        auto m = make();
        CHECK(m->alloc_file_id(Extent::Kind::kChunk) > last);
        // pack 与 chunk 计数器相互独立且各自单调
        uint64_t p1 = m->alloc_file_id(Extent::Kind::kPack);
        CHECK_EQ(m->alloc_file_id(Extent::Kind::kPack), p1 + 1);
        m->close();
    }
}

// delete_bucket：有进行中 multipart 时拒绝（对齐 AWS；防 refs 永久泄漏与幽灵上传复活）
inline void case_delete_bucket_blocks_on_mpu(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-mpu");
    lights3::storage::ObjectMeta meta;
    auto id = m->create_upload("ms-mpu", "k", meta);
    CHECK_THROWS_S3(m->delete_bucket("ms-mpu"), lights3::s3::S3ErrorCode::BucketNotEmpty);
    m->abort_upload("ms-mpu", "k", id);
    m->delete_bucket("ms-mpu");  // abort 后可删
    CHECK(!m->bucket_exists("ms-mpu"));
    m->close();
}

// max-keys=0：S3 语义为空结果 + IsTruncated=false（否则空 token 使客户端死循环）
inline void case_list_max_keys_zero(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-mk0");
    m->put_object("ms-mk0", "k", make_rec("k", {}));
    ListOptions opt;
    opt.max_keys = 0;
    auto r = m->list_objects("ms-mk0", opt);
    CHECK_EQ(r.objects.size(), size_t(0));
    CHECK(!r.is_truncated);
    m->delete_object("ms-mk0", "k");
    m->delete_bucket("ms-mk0");
    m->close();
}

// list：delimiter 组的跳跃迭代 + 分页 token 落在组尾（主文档 §4.4 / redis 版 §2.3）
inline void case_list_delimiter_paging(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-page");
    for (auto k : {"a", "b/1", "b/2", "b/3", "c"})
        m->put_object("ms-page", k, make_rec(k, {}));

    ListOptions opt;
    opt.delimiter = "/";
    opt.max_keys = 2;
    auto p1 = m->list_objects("ms-page", opt);
    CHECK_EQ(p1.objects.size(), size_t(1));
    CHECK_EQ(p1.objects[0].key, "a");
    CHECK_EQ(p1.common_prefixes.size(), size_t(1));
    CHECK_EQ(p1.common_prefixes[0], "b/");
    CHECK(p1.is_truncated);
    CHECK_EQ(p1.next_token, "b/3");  // token 语义（start after）须落在组尾

    opt.start_after = p1.next_token;
    auto p2 = m->list_objects("ms-page", opt);
    CHECK_EQ(p2.objects.size(), size_t(1));
    CHECK_EQ(p2.objects[0].key, "c");
    CHECK(p2.common_prefixes.empty());
    CHECK(!p2.is_truncated);

    // prefix + delimiter：子层级分组
    ListOptions sub;
    sub.prefix = "b/";
    sub.delimiter = "/";
    auto ps = m->list_objects("ms-page", sub);
    CHECK_EQ(ps.objects.size(), size_t(3));
    CHECK(ps.common_prefixes.empty());
    for (auto k : {"a", "b/1", "b/2", "b/3", "c"}) m->delete_object("ms-page", k);
    m->delete_bucket("ms-page");
    m->close();
}

// pack 存活账（主文档 §9.1/P2）：提交类事务同批增减、seal 幂等且 0 不覆盖已知
// size、swap 账随 extent 迁移、live=0 项仍可见、drop 销账
inline void case_pack_stats_accounting(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-pk");
    uint64_t pid = m->alloc_file_id(Extent::Kind::kPack);
    CHECK(!find_pack(*m, pid).has_value());  // 未写入即无账

    m->put_object("ms-pk", "a", make_rec("a", {pack_extent(pid, 30, 100)}));
    m->put_object("ms-pk", "b", make_rec("b", {pack_extent(pid, 160, 50)}));
    auto ps = find_pack(*m, pid);
    CHECK(ps.has_value());
    CHECK_EQ(ps->live_bytes, int64_t(150));
    CHECK_EQ(ps->live_recs, int64_t(2));
    CHECK(!ps->sealed);

    // 压实换 ref：账随 extent 迁移（−pack +chunk）
    uint64_t cid = m->alloc_file_id(Extent::Kind::kChunk);
    CHECK(m->swap_extents("ms-pk", "a", /*expect_version=*/1,
                          DataRef{{pack_extent(pid, 30, 100)}}, DataRef{{chunk_extent(cid, 100)}}));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(50));
    CHECK_EQ(ps->live_recs, int64_t(1));

    // 封存：幂等，file_size=0 不覆盖已知值
    m->seal_pack(pid, 4096);
    m->seal_pack(pid, 0);
    ps = find_pack(*m, pid);
    CHECK(ps->sealed);
    CHECK_EQ(ps->file_size, uint64_t(4096));

    // 覆盖写（pack→pack）：旧减新增
    uint64_t pid2 = m->alloc_file_id(Extent::Kind::kPack);
    m->put_object("ms-pk", "b", make_rec("b", {pack_extent(pid2, 0, 70)}));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(0));
    CHECK_EQ(ps->live_recs, int64_t(0));
    CHECK(ps->sealed);  // live=0 的封存 pack 仍可见——空 pack 整删的候选（§9.1）
    auto ps2 = find_pack(*m, pid2);
    CHECK_EQ(ps2->live_bytes, int64_t(70));

    // 销账（空 pack 整删后）
    m->drop_pack_stat(pid);
    CHECK(!find_pack(*m, pid).has_value());

    // 删除对象：pid2 归零
    CHECK(m->delete_object("ms-pk", "b"));
    ps2 = find_pack(*m, pid2);
    CHECK_EQ(ps2->live_bytes, int64_t(0));
    CHECK_EQ(ps2->live_recs, int64_t(0));
    m->drop_pack_stat(pid2);

    CHECK(m->delete_object("ms-pk", "a"));
    drain_gcq(*m);
    m->delete_bucket("ms-pk");
    m->close();
}

// multipart 的 pack 账：put_part 计入、同号重传换账、complete 选中分片不双计
// （refs 转移 ≠ 存活变化）、未选中分片扣减、abort 全扣
inline void case_pack_stats_multipart(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-pmpu");
    uint64_t pid = m->alloc_file_id(Extent::Kind::kPack);
    lights3::storage::ObjectMeta meta;
    auto id = m->create_upload("ms-pmpu", "k", meta);

    auto part = [&](int no, uint64_t off, uint64_t len) {
        PartRec p;
        p.part_no = no;
        p.size = len;
        p.etag = "deadbeef";
        p.data.extents = {pack_extent(pid, off, len)};
        return p;
    };
    m->put_part("ms-pmpu", "k", id, part(1, 0, 100));
    m->put_part("ms-pmpu", "k", id, part(2, 130, 60));
    m->put_part("ms-pmpu", "k", id, part(3, 220, 40));
    auto ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(200));
    CHECK_EQ(ps->live_recs, int64_t(3));

    // 同号重传（last-write-wins）：旧 60 扣、新 80 计
    m->put_part("ms-pmpu", "k", id, part(2, 300, 80));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(220));
    CHECK_EQ(ps->live_recs, int64_t(3));

    // complete 选 1/2：选中分片账不动（仅 refs 转移），未选中 part3 扣 40
    std::vector<PartInfo> sel = {{1, "deadbeef"}, {2, "deadbeef"}};
    m->complete_upload("ms-pmpu", "k", id, sel);
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(180));
    CHECK_EQ(ps->live_recs, int64_t(2));

    // abort 路径：另一 upload 的 pack 分片全扣
    auto id2 = m->create_upload("ms-pmpu", "k2", meta);
    m->put_part("ms-pmpu", "k2", id2, part(1, 400, 30));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(210));
    m->abort_upload("ms-pmpu", "k2", id2);
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(180));
    CHECK_EQ(ps->live_recs, int64_t(2));

    // 删除 complete 出的对象 → 归零
    CHECK(m->delete_object("ms-pmpu", "k"));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(0));
    CHECK_EQ(ps->live_recs, int64_t(0));
    m->drop_pack_stat(pid);
    drain_gcq(*m);
    m->delete_bucket("ms-pmpu");
    m->close();
}

inline void run_meta_store_suite(const MetaFactory& make) {
    case_gc_accounting(make);
    case_alloc_monotonic_across_reopen(make);
    case_delete_bucket_blocks_on_mpu(make);
    case_list_max_keys_zero(make);
    case_list_delimiter_paging(make);
    case_pack_stats_accounting(make);
    case_pack_stats_multipart(make);
}

}  // namespace meta_store_suite
