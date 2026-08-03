// 后端一致性套件：同一组用例参数化跑 memory / localfs / xlocalfs（docs/storage-backend.md §6）；
// 套件本体在 unit/backend_suite.h（cloudproxy 测试同用，docs/cloudproxy-backend.md §10）
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
using backend_suite::put;
using backend_suite::read_all;
using backend_suite::run_backend_suite;
namespace fs = std::filesystem;

using backend_suite::TmpDir;

TEST(memory_backend_suite) {
    MemoryBackend b;
    run_backend_suite(b);
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

// tiered 对 L2 仍是普通后端（docs/tiered-storage.md §2）：全 local 态跑同一套一致性用例
TEST(tiered_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;  // 单测不开后台任务
    auto b = std::make_shared<TieredBackend>(local, std::make_shared<MemoryBackend>(), pool, cfg);
    run_backend_suite(*b);
    sync_wait(b->close());
}

#ifdef LIGHTS3_DUOSTORE
// duostore（RocksDB meta + chunk/pack 数据面，docs/duostore-backend.md §14）：
// 三种布局变体同套件全绿——默认参数（混合：小对象进 pack）、小 chunk（强制多
// chunk manifest）、强制全 pack（阈值调大 + 小 pack_max_size 高频轮转封存）
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
    cfg.pack_threshold = 0;  // 关 pack：本变体专测多 chunk manifest 路径
    cfg.meta_sync = false;   // 变体顺带覆盖 meta_sync 关闭路径（§6.3）
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
    cfg.pack_threshold = 8 << 10;  // 套件对象全部 ≤ 8KiB → 强制全 pack（§14 变体）
    cfg.pack_max_size = 8 << 10;   // 阈值 == 上限：小 pack 高频轮转，封存路径吃满
    cfg.pack_writers = 2;
    cfg.meta_sync = false;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    run_backend_suite(*b);
    sync_wait(b->close());
}
#endif

// 跨多个 64KiB 数据块的读写路径：io_uring 流式写入与带偏移读取
TEST(xlocalfs_large_object_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    std::string data(1 << 20, '\0');  // 1 MiB 伪随机内容
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

    // 跨块边界的 Range
    auto mid = sync_wait(b.get_object("bkt", "big/blob.bin",
                                      ByteRange{uint64_t(65530), uint64_t(65545)}));
    CHECK(read_all(*mid.body) == data.substr(65530, 16));

    // multipart：两个跨块分片经 io_uring 拼接
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

TEST(localfs_atomic_layout) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "x/y.bin", "payload");

    // 磁盘布局符合 docs/storage-backend.md §3.1：数据文件 + sidecar，staging 无残留
    CHECK(fs::exists(tmp.path / "data/bkt/x/y.bin"));
    CHECK(fs::exists(tmp.path / "data/bkt/x/y.bin.lights3-meta"));
    size_t staging_leftover = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "staging"))
        if (e.is_regular_file()) ++staging_leftover;
    CHECK_EQ(staging_leftover, size_t(0));

    // 内部保留名不可作为 key
    CHECK_THROWS_S3(put(b, "bkt", "x/y.bin.lights3-meta", "z"),
                    lights3::s3::S3ErrorCode::InvalidArgument);
}

TEST(localfs_multipart_layout_and_cleanup) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    // 分片落 <staging>/mpu/<id>/，complete 后目录清理、对象原子落地（docs/storage-backend.md §3.2）
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

    // 超期（>7 天）孤儿上传在新实例启动时被清理
    auto stale = sync_wait(b.create_multipart("bkt", "stale.bin", {}));
    fs::path stale_dir = tmp.path / "staging/mpu" / stale;
    fs::last_write_time(stale_dir / "manifest",
                        fs::file_time_type::clock::now() - std::chrono::hours(24 * 8));
    LocalFsBackend b2(tmp.path / "data", tmp.path / "staging", pool);
    CHECK(!fs::exists(stale_dir));
}

// ---------- 评审发现的回归用例（撕裂/元数据同源/孤儿 sidecar）----------

// 并发 PUT 同 key 不撕裂（storage.md 高危第一条）：提交段的 per-key 锁保证
// GET 拿到的 body 与 ETag 恒来自同一次写入
TEST(localfs_concurrent_put_same_key_not_torn) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(8);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    // 每个写者的 body 内容互不相同，其 md5 即该次写入的指纹。竞态窗口很窄，
    // 多轮重复把无锁实现的检出概率推到接近 1（有锁时恒定通过）
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

        // 读到的 body 必须正是其 ETag 所对应的那次写入的内容（撕裂时两者不符）
        auto s = sync_wait(b.get_object("bkt", "hot.bin", std::nullopt));
        std::string got = read_all(*s.body);
        auto it = etag_of.find(s.meta.etag);
        CHECK(it != etag_of.end());
        CHECK(got == it->second);
        CHECK_EQ(s.meta.size, uint64_t(got.size()));

        // sidecar 也须描述最终落地的那次写入：数据与 sidecar 是两次 rename，
        // 没有 per-key 锁时会交错成"数据来自 A、sidecar 来自 B"。xattr 与 inode
        // 绑定不受交错影响，故 sidecar 才是这把锁的直接观测点（也是外部工具看到的）
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

// 元数据随数据文件一同提交：xattr 与 inode 绑定，即便 sidecar 缺失（"数据已
// rename、sidecar 未写"的崩溃窗口）GET 仍拿到与 body 一致的 etag
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
    // xattr 可用的文件系统上 etag 仍然正确；不支持时退化为空（sidecar-only 语义）
    if (!s.meta.etag.empty()) CHECK_EQ(s.meta.etag, pr.etag);
}

// GET 用已打开 fd 的 fstat：并发覆盖写后 body 与 meta 不得来自不同 inode
TEST(localfs_get_meta_matches_open_inode) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    std::string v1(8192, 'x');
    auto pr1 = put(b, "bkt", "k.bin", v1);

    auto s = sync_wait(b.get_object("bkt", "k.bin", std::nullopt));  // 持旧 inode 的 fd
    put(b, "bkt", "k.bin", std::string(64, 'y'));                    // 覆盖为更短的新对象
    std::string got = read_all(*s.body);
    // 对路径二次 stat 会把 size 换成新对象的 64、etag 换成新 etag，而 body 仍是
    // 旧 inode 的内容 —— 三者必须一致
    CHECK_EQ(s.meta.size, uint64_t(v1.size()));
    CHECK_EQ(s.meta.etag, pr1.etag);
    CHECK(got == v1);
}

// 孤儿 sidecar 自愈：delete 两步之间崩溃遗留的 sidecar 由 list 顺手清掉
TEST(localfs_orphan_sidecar_reaped_by_list) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "gone.bin", "x");
    put(b, "bkt", "stay.bin", "y");

    fs::remove(tmp.path / "data/bkt/gone.bin");  // 模拟"数据已删、sidecar 未删"
    fs::path orphan = tmp.path / "data/bkt/gone.bin.lights3-meta";
    CHECK(fs::exists(orphan));

    auto res = sync_wait(b.list_objects("bkt", ListOptions{}));
    CHECK_EQ(res.objects.size(), size_t(1));
    CHECK_EQ(res.objects[0].key, std::string("stay.bin"));
    CHECK(!fs::exists(orphan));
    CHECK(fs::exists(tmp.path / "data/bkt/stay.bin.lights3-meta"));
}

// ---------- P0 §1.3 / §1.4 回归 ----------

// commit_cached 必须在 rename 之前把数据落盘：随后写的 sidecar 是 fsync 过的，
// 数据没落盘就掉电会得到"sidecar 说 cached/size=N、文件是 N 字节零块"的对象，
// 而 StubRace 检查比的是 st_size（rename 已提交 inode size）故检查不出来。
// 这里断言的是提交后立即可读到正确内容（fsync 的正确性无法在单测里模拟掉电，
// 但顺序错误会在 LIGHTS3_FSYNC=1 下被 fsync_path 的 errno 路径暴露）
TEST(localfs_commit_cached_persists_data_before_sidecar) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;
    cfg.cold_after_sec = 0;  // 立即判冷
    auto cloud = std::make_shared<MemoryBackend>();
    auto b = std::make_shared<TieredBackend>(local, cloud, pool, cfg);
    sync_wait(b->create_bucket("bkt"));
    std::string data(64 * 1024, 'c');
    put(*b, "bkt", "k.bin", data);

    // 下沉到云端（本地成 stub），再 GET 触发 Tee 回填 → commit_cached
    sync_wait(b->scan_once());
    auto s1 = sync_wait(b->get_object("bkt", "k.bin", std::nullopt));
    CHECK_EQ(read_all(*s1.body), data);   // 回填在 EOF 处提交

    // 缓存命中路径读出的内容必须与原文逐字节一致（提交顺序错会读到截断/零块）
    auto s2 = sync_wait(b->get_object("bkt", "k.bin", std::nullopt));
    CHECK_EQ(read_all(*s2.body), data);
    CHECK_EQ(s2.meta.size, uint64_t(data.size()));
    sync_wait(b->close());
}

// ---------- localfs 剪枝式 LIST 与全量参考实现的差分（gaps §2.7）----------
// memory 后端走 apply_listing（全量收集 + 排序），作为语义参考；localfs 的
// 目录树剪枝遍历必须在任意 prefix/delimiter/分页组合下产出一致结果
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
        // 分页游走：token 语义各自自洽即可，这里再各走一轮全量对齐
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
            // 逐页游走（max_keys=1/2/3），拼起来必须与一次性全量一致且无重复
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

    // start_after 落在组内：组仍应被发出（与参考实现一致）
    ListOptions mid;
    mid.delimiter = "/";
    mid.start_after = "a/b/c.txt";
    check_same(mid);
}
