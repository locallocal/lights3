// TikvMetaStore 专项单测（docs/duostore-tikv-meta.md §10）：meta 一致性套件、
// 注入组合跑后端套件、前缀隔离、多网关共 meta、写偏斜守卫物化（Op::Lock 语义
// 冒烟）、swap_extents CAS、并发冲突收敛、close 守卫。
// 真实集群获取：环境变量 LIGHTS3_TEST_PD_ADDR（逗号分隔 PD 地址，指向 tiup
// playground / 既有测试集群）设置才跑，否则显式 SKIP（不算失败，机制同
// test_duostore_rados.cc）。隔离：每用例唯一 tikv_prefix——集群可复用、多套
// 测试互不污染（版本垃圾由集群 GC safepoint 治理，§7.3）。
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_TIKV_META)

#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

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
            // 修剪空白：与 duostore_backend.cc 的 pd_endpoints 解析同规则
            //（"a:2379, b:2379" 书写风格两条路径行为一致）
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

// 每用例唯一前缀：集群可复用，多套用例/多次运行互不污染（§3.1）
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

// 同一 meta 语义基线（与 RocksMetaStore 共享套件，docs/duostore-tikv-meta.md §10）
TEST(duostore_tikv_meta_store_suite) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    meta_store_suite::run_meta_store_suite(
        [&] { return std::make_unique<TikvMetaStore>(tikv_opts(prefix)); });
}

// 注入组合（TikvMetaStore + FsDataStore）跑后端一致性套件（§10）
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
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc}, pool,
        [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// 前缀隔离（§3.1）：两个 store 共用一个集群，互不可见
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

// 多网关共 meta（§4.5）：同前缀两实例即共享元数据；号段派发全局唯一
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

    // 两网关各取一批 file_id：全局唯一（计数器 RMW 号段，§5）
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

// 写偏斜守卫物化（§4.3，T1 验证项：Op::Lock 记录参与后续 prewrite 冲突检测）：
// put_object 与 delete_bucket 并发竞争，任何交错下不得出现"桶已删而对象残留"
// 的幽灵状态——守卫失效时此不变量在若干轮内必被打破
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
                if (e.code != s3::S3ErrorCode::NoSuchBucket)  // 删桶胜出的合法失败
                    errs[0] = std::current_exception();
            }
        });
        std::thread t2([&] {
            try {
                b.delete_bucket(bkt);
            } catch (const s3::S3Error& e) {
                if (e.code != s3::S3ErrorCode::BucketNotEmpty)  // put 胜出的合法失败
                    errs[1] = std::current_exception();
            }
        });
        t1.join();
        t2.join();
        for (auto& e : errs)
            if (e) std::rethrow_exception(e);

        if (!a.bucket_exists(bkt)) {
            // 桶删成功 ⇒ put 必须没有落下幽灵对象（get_object 不查桶，能看见残留）
            CHECK(!a.get_object(bkt, "k").has_value());
        } else {
            // put 胜出 ⇒ 对象在、桶在；清理后桶可删
            CHECK(a.get_object(bkt, "k").has_value());
            CHECK(a.delete_object(bkt, "k"));
            a.delete_bucket(bkt);
        }
    }
    // 清理 GC 账（本用例的 reclaim 无数据面文件，直接销账）
    for (auto& [seq, r] : a.peek_reclaims(1000)) a.ack_reclaim(seq);
    a.close();
    b.close();
}

// put_part 与 abort_upload 并发（§4.3 上传守卫）：abort 胜出后不得残留孤儿 part
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
                if (e.code != s3::S3ErrorCode::NoSuchUpload)  // abort 胜出的合法失败
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
        // abort 已提交（无并发 complete，必成功）⇒ upload 消失；孤儿 part 检测：
        // 守卫失效时 put_part 可落在 abort 之后，part 行残留、refs 泄漏——经
        // chunk_referenced 观察（本用例 part 无 extent，退化为 list 语义检查）
        CHECK_THROWS_S3(a.list_parts("pg", "k", id), s3::S3ErrorCode::NoSuchUpload);
    }
    a.delete_bucket("pg");
    for (auto& [seq, r] : a.peek_reclaims(1000)) a.ack_reclaim(seq);
    a.close();
    b.close();
}

// swap_extents 的 CAS 放弃路径（§4.4/主文档 §9.2）：version/extents 不符 → false 不落写
TEST(duostore_tikv_swap_extents_cas) {
    TIKV_OR_SKIP();
    TikvMetaStore m(tikv_opts(unique_prefix()));
    m.create_bucket("swap");
    uint64_t id1 = m.alloc_file_id(Extent::Kind::kChunk);
    uint64_t id2 = m.alloc_file_id(Extent::Kind::kChunk);
    DataRef from{{chunk_extent(id1, 8)}};
    DataRef to{{chunk_extent(id2, 8)}};
    m.put_object("swap", "k", make_rec("k", from.extents));  // version=1

    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));  // version 不符
    CHECK(m.chunk_referenced(id1));
    CHECK(!m.chunk_referenced(id2));

    CHECK(m.swap_extents("swap", "k", /*expect_version=*/1, from, to));
    auto rec = m.get_object("swap", "k");
    CHECK_EQ(rec->version, uint64_t(2));
    CHECK(rec->data.extents == to.extents);
    CHECK(!m.chunk_referenced(id1));
    CHECK(m.chunk_referenced(id2));

    // 换过之后旧 from 过期 → 再换必失败
    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));
    CHECK(m.delete_object("swap", "k"));
    m.delete_bucket("swap");
    m.close();
}

// 并发冲突收敛（§4.1）：两个"网关"对同一 key 竞争覆盖写——WriteConflict 重试
// 保证可串行化：version 严格计数、refs 只剩最终 extent、gcq 恰 (总写数-1) 条
TEST(duostore_tikv_concurrent_conflict_converges) {
    TIKV_OR_SKIP();
    std::string prefix = unique_prefix();
    TikvMetaStore g1(tikv_opts(prefix));
    TikvMetaStore g2(tikv_opts(prefix));
    g1.create_bucket("race");
    constexpr int kPerWriter = 25;

    // 线程内异常经 exception_ptr 传回主线程重抛（否则 terminate 掩盖断言信息）
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
    CHECK_EQ(g1.peek_reclaims(1000).size(), size_t(2 * kPerWriter - 1));

    CHECK(g1.delete_object("race", "hot"));
    g1.delete_bucket("race");
    for (auto& [seq, r] : g1.peek_reclaims(1000)) g1.ack_reclaim(seq);
    g1.close();
    g2.close();
}

// close 后调用干净失败（500），而非崩溃（防御纵深惯例）
TEST(duostore_tikv_closed_store_throws) {
    TIKV_OR_SKIP();
    TikvMetaStore m(tikv_opts(unique_prefix()));
    m.close();
    CHECK_THROWS_S3(m.bucket_exists("x"), s3::S3ErrorCode::InternalError);
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_TIKV_META
