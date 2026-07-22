// meta store 一致性套件（docs/duostore-redis-meta.md §9）：同一组用例参数化跑
// 所有 IMetaStore 实现（RocksMetaStore 恒跑、RedisMetaStore 条件跑），两实现
// 共享同一语义基线。自 test_duostore.cc 的 meta 用例提取。
// factory 约定：每次调用在同一底层存储上打开一个新实例（"重启"语义）；套件内
// 各场景用互不相同的 bucket 名隔离，实例串行 open/close（RocksDB 单进程锁）。
#pragma once

#include <functional>
#include <memory>
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
    CHECK(!m->chunk_referenced(id1));
    CHECK(m->chunk_referenced(id2));
    CHECK_EQ(m->get_object("ms-gc", "k")->version, uint64_t(2));

    // 删除：入 gcq；幂等二次删返回 false
    CHECK(m->delete_object("ms-gc", "k"));
    CHECK(!m->delete_object("ms-gc", "k"));
    CHECK(!m->chunk_referenced(id2));
    auto rs2 = m->peek_reclaims(10);
    CHECK_EQ(rs2.size(), size_t(2));

    // 销账后 gcq 清空
    for (auto& [seq, rec] : rs2) m->ack_reclaim(seq);
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

inline void run_meta_store_suite(const MetaFactory& make) {
    case_gc_accounting(make);
    case_alloc_monotonic_across_reopen(make);
    case_delete_bucket_blocks_on_mpu(make);
    case_list_max_keys_zero(make);
    case_list_delimiter_paging(make);
}

}  // namespace meta_store_suite
