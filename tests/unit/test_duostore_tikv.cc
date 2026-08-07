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
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                      cfg.pack_max_size, cfg.pack_writers},
        pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
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
    for (auto& [seq, r] : a.peek_reclaims(1000, 0)) a.ack_reclaim(seq);
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
    for (auto& [seq, r] : a.peek_reclaims(1000, 0)) a.ack_reclaim(seq);
    a.close();
    b.close();
}

// 单值体积保护（gaps §2.12）：manifest 超过 raft entry 承载即 fail-fast 抛
// EntityTooLarge（400），而非 prewrite 永久失败的 500——写不进也删不掉最伤
TEST(duostore_tikv_object_manifest_size_guard) {
    TIKV_OR_SKIP();
    TikvMetaStore m(tikv_opts(unique_prefix()));
    m.create_bucket("etl");
    std::vector<Extent> huge;
    huge.reserve(200'001);
    // 交错 id 破坏 run 编码（病态形态），条数越过 kMaxObjectExtents
    for (size_t i = 0; i < 200'001; ++i)
        huge.push_back(chunk_extent(i * 2 + 1, 1));
    CHECK_THROWS_S3(m.put_object("etl", "k", make_rec("k", std::move(huge))),
                    s3::S3ErrorCode::EntityTooLarge);
    CHECK(!m.get_object("etl", "k").has_value());  // 未落写
    m.delete_bucket("etl");
    m.close();
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
    CHECK_EQ(g1.peek_reclaims(1000, 0).size(), size_t(2 * kPerWriter - 1));

    CHECK(g1.delete_object("race", "hot"));
    g1.delete_bucket("race");
    for (auto& [seq, r] : g1.peek_reclaims(1000, 0)) g1.ack_reclaim(seq);
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

// ---------- T5 专项（§11）----------

// 从 Prometheus 渲染文本中取指定序列的值（找不到返回 -1）
namespace {
long long metric_value(const std::string& render, const std::string& series) {
    auto pos = render.find(series + " ");
    if (pos == std::string::npos) return -1;
    return std::stoll(render.substr(pos + series.size() + 1));
}
}  // namespace

// T5 指标：冲突重试计数——两网关热点 key 竞争覆盖写，WriteConflict 重试轮次
// 落 lights3_duostore_tikv_txn_conflict_retries_total；构造期注册 0 值可见。
// 顺带冒烟退避预算参数化（backoff_budget_ms 缩到 5s，功能路径全通）
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
    // 冲突是并发交错的产物，单批可能恰好错开——有界轮次跑到观察为止（每轮
    // 2×15 次热点覆盖写，正常一两轮内必现）
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
    CHECK(retries > 0);  // 计数只增不减

    CHECK(g1.delete_object("cm", "hot"));
    g1.delete_bucket("cm");
    for (auto& [seq, r] : g1.peek_reclaims(1000, 0)) g1.ack_reclaim(seq);
    g1.close();
    g2.close();
}

// T5 GC safepoint（§7.3）：单轮推进 = service safepoint 声明 + 集群 safepoint
// 推进，返回值 >0 且跨轮单调；worker 模式（interval>0）后台自动推进，close 干净停
TEST(duostore_tikv_gc_safepoint_advances) {
    TIKV_OR_SKIP();
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = tikv_opts(unique_prefix());
    opts.gc_retention_s = 60;  // 集群共享：保留 60s 窗口，不动别的用例的在途快照
    opts.metrics = MetricsScope(reg, {{"backend", "t5sp"}});
    TikvMetaStore m(opts);
    CHECK_EQ(metric_value(reg->render(),
                          "lights3_duostore_tikv_gc_safepoint_ms{backend=\"t5sp\"}"),
             0);
    uint64_t sp1 = m.update_gc_safepoint_once();
    CHECK(sp1 > 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    uint64_t sp2 = m.update_gc_safepoint_once();
    CHECK(sp2 >= sp1);  // PD 端单调只进
    // gauge = 最近推进的集群 safepoint 物理 ms
    CHECK_EQ(metric_value(reg->render(),
                          "lights3_duostore_tikv_gc_safepoint_ms{backend=\"t5sp\"}"),
             (long long)(sp2 >> 18));
    m.close();

    // worker 模式：首 tick 立即推进——轮询等 gauge 非零（上限 10s），close 即停
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

// T5 万分片 complete 专项（§6.3）：10000 分片的 complete_upload 是全实现最大
// 事务（对象 + upload 删 + 全量守卫 + 万级 part 删/refs 转移 ≈ 2 万 mutation）。
// 验证：prewrite/commit 在伸缩 lock_ttl（txn_lock_ttl，√MiB 放大）内收敛；并发
// 读者全程驱动 LockResolver 对在途 primary 做 TTL 判定——TTL 不足时会把
// prewrite 中的事务判死回滚，complete 无法成功。TTLManager 心跳不接线的评估
// 依据也在此：事务规模有上界（S3 万分片顶格），伸缩 TTL 足够覆盖
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
    // 8 线程按 part_no 残差类分工：守卫按 part_no % 16 分片，残差类互斥 → 零误撞
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

    // 并发读者：complete 期间持续读同 key，驱动锁解析路径对 primary 判 TTL
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

    // 清理：删除对象（第二个万级 mutation 事务）→ refs 清空、GC 账销掉
    CHECK(m.delete_object("big", "k"));
    CHECK(!m.chunk_referenced(ids[1]));
    m.delete_bucket("big");
    std::vector<uint64_t> seqs;
    for (auto& [seq, r] : m.peek_reclaims(100000, 0)) seqs.push_back(seq);
    m.ack_reclaims(seqs);
    m.close();
    reader_store.close();
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_TIKV_META
