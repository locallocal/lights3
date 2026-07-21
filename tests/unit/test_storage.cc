// 后端一致性套件：同一组用例参数化跑 memory / localfs / xlocalfs（docs/storage-backend.md §6）；
// 套件本体在 unit/backend_suite.h（cloudproxy 测试同用，docs/cloudproxy-backend.md §10）
#include <filesystem>

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
// duostore（RocksDB meta + chunk 数据面，docs/duostore-backend.md §14）：
// 默认参数与小 chunk（强制多 chunk manifest）两个布局变体同套件；全 pack 变体随 P2 引入
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
    cfg.meta_sync = false;  // 变体顺带覆盖 meta_sync 关闭路径（§6.3）
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
