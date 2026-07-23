// SqliteMetaStore 专项单测（docs/duostore-sqlite-meta.md §9）：meta 一致性套件、
// 注入组合跑后端套件、BLOB key 排序（非 UTF-8 字节）、重开持久性、冷备单文件、
// swap_extents CAS、文件谱系校验。零外部依赖——无 Redis 版的探测/SKIP 路径。
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_SQLITE_META)

#include <sqlite3.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "core/thread_pool.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/sqlite_meta_store.h"
#include "unit/backend_suite.h"
#include "unit/meta_store_suite.h"
#include "unit/mini_test.h"

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;

namespace {

using backend_suite::TmpDir;
using meta_store_suite::chunk_extent;
using meta_store_suite::make_rec;

SqliteMetaOptions sqlite_opts(const fs::path& file) {
    // 单测不需要 fsync（崩溃语义另测；号段连接内部恒 FULL 不受此影响）
    return {file.string(), /*sync=*/false, /*cache_bytes=*/8ull << 20, /*pool_size=*/4};
}

}  // namespace

// 同一 meta 语义基线（与 RocksMetaStore / RedisMetaStore 共享套件，§9.1）；
// factory 反复 open/close 同一 DB 文件，天然覆盖重启语义（号段不回退、schema 校验）
TEST(duostore_sqlite_meta_store_suite) {
    TmpDir tmp;
    meta_store_suite::run_meta_store_suite([&] {
        return std::make_unique<SqliteMetaStore>(sqlite_opts(tmp.path / "meta.sqlite3"));
    });
}

// 注入组合（SqliteMetaStore + FsDataStore）跑后端一致性套件（§9.2）
TEST(duostore_sqlite_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<SqliteMetaStore>(sqlite_opts(tmp.path / "meta.sqlite3"));
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "sqlite-suite";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<FsDataStore>(
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc}, pool,
        [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// BLOB key = memcmp 序（§2.1）：高位字节 / 非 UTF-8 序列的 key 排序与分页 token
TEST(duostore_sqlite_binary_key_ordering) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("bin");
    // memcmp 升序（字面量拆开写避免 \x 贪婪吞掉后续 hex 字符）
    std::vector<std::string> keys = {
        std::string("a\x01") + "b",        // 0x01 控制字节
        "a\x7f",                           // DEL
        "a\xc3\x28",                       // 非法 UTF-8 序列
        "a\xff",                           // 0xff（TEXT 存储下常见的坑位）
        "b",
    };
    for (auto it = keys.rbegin(); it != keys.rend(); ++it)  // 乱序写入
        m.put_object("bin", *it, make_rec(*it, {}));

    ListOptions opt;
    opt.max_keys = 2;
    auto p1 = m.list_objects("bin", opt);
    CHECK_EQ(p1.objects.size(), size_t(2));
    CHECK_EQ(p1.objects[0].key, keys[0]);
    CHECK_EQ(p1.objects[1].key, keys[1]);
    CHECK(p1.is_truncated);
    CHECK_EQ(p1.next_token, keys[1]);

    opt.start_after = p1.next_token;
    auto p2 = m.list_objects("bin", opt);
    CHECK_EQ(p2.objects.size(), size_t(2));
    CHECK_EQ(p2.objects[0].key, keys[2]);
    CHECK_EQ(p2.objects[1].key, keys[3]);
    CHECK(p2.is_truncated);

    opt.start_after = p2.next_token;
    auto p3 = m.list_objects("bin", opt);
    CHECK_EQ(p3.objects.size(), size_t(1));
    CHECK_EQ(p3.objects[0].key, "b");
    CHECK(!p3.is_truncated);

    ListOptions pre;
    pre.prefix = "a";
    CHECK_EQ(m.list_objects("bin", pre).objects.size(), size_t(4));

    for (const auto& k : keys) CHECK(m.delete_object("bin", k));
    m.delete_bucket("bin");
    m.close();
}

// 重开持久性：对象/桶/version 原样保留（WAL 回放 + 单文件即全部状态）
TEST(duostore_sqlite_persistence_across_reopen) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    uint64_t id1 = 0, id2 = 0;
    {
        SqliteMetaStore m(sqlite_opts(db));
        m.create_bucket("keep");
        id1 = m.alloc_file_id(Extent::Kind::kChunk);
        id2 = m.alloc_file_id(Extent::Kind::kChunk);
        m.put_object("keep", "k", make_rec("k", {chunk_extent(id1, 7)}));
        m.put_object("keep", "k", make_rec("k", {chunk_extent(id2, 7)}));  // version=2
        m.close();
    }
    {
        SqliteMetaStore m(sqlite_opts(db));
        CHECK(m.bucket_exists("keep"));
        auto rec = m.get_object("keep", "k");
        CHECK(rec.has_value());
        CHECK_EQ(rec->version, uint64_t(2));
        CHECK_EQ(rec->data.extents.at(0).file_id, id2);
        CHECK(m.chunk_referenced(id2));
        CHECK(!m.chunk_referenced(id1));
        CHECK_EQ(m.peek_reclaims(10).size(), size_t(1));  // 覆盖写的旧账仍在
        m.close();
    }
}

// 冷备 = 干净 close 后拷单文件（§6）：无 -wal/-shm 残留，副本开出即完整数据
TEST(duostore_sqlite_cold_backup_single_file) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    {
        SqliteMetaStore m(sqlite_opts(db));
        m.create_bucket("bak");
        m.put_object("bak", "k", make_rec("k", {}));
        m.close();
    }
    CHECK(!fs::exists(db.string() + "-wal"));
    CHECK(!fs::exists(db.string() + "-shm"));
    fs::path copy = tmp.path / "backup.sqlite3";
    fs::copy_file(db, copy);
    SqliteMetaStore m(sqlite_opts(copy));
    CHECK(m.bucket_exists("bak"));
    CHECK(m.get_object("bak", "k").has_value());
    m.close();
}

// 桶重复创建 → BucketAlreadyOwnedByYou（§3.3）
TEST(duostore_sqlite_create_bucket_duplicate) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("dup");
    CHECK_THROWS_S3(m.create_bucket("dup"), s3::S3ErrorCode::BucketAlreadyOwnedByYou);
    m.delete_bucket("dup");
    m.close();
}

// swap_extents 的乐观放弃路径（§3.3 / 主文档 §9.2）：不符 → false 且事务回滚不落写
TEST(duostore_sqlite_swap_extents_cas) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
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

// 文件谱系（§2.2）：拿错文件（非 SQLite / application_id 不符）→ 响亮拒绝
TEST(duostore_sqlite_rejects_foreign_file) {
    TmpDir tmp;
    fs::path garbage = tmp.path / "not-a-db.sqlite3";
    {
        std::ofstream f(garbage);
        f << "this is not a sqlite database";
    }
    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(sqlite_opts(garbage)),
                    s3::S3ErrorCode::InternalError);
}

// 文件谱系（§2.2）：app_id=0/ver=0 但已有表 = 别人的 SQLite 库（野生库常态）——
// 拒绝且不留痕：不建表、不盖章、不做 WAL 转换
TEST(duostore_sqlite_rejects_foreign_populated_db) {
    TmpDir tmp;
    fs::path foreign = tmp.path / "foreign.sqlite3";
    sqlite3* db = nullptr;
    CHECK_EQ(sqlite3_open(foreign.string().c_str(), &db), SQLITE_OK);
    CHECK_EQ(sqlite3_exec(db, "CREATE TABLE their_data(x INTEGER)", nullptr, nullptr,
                          nullptr),
             SQLITE_OK);
    sqlite3_close(db);

    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(sqlite_opts(foreign)),
                    s3::S3ErrorCode::InternalError);

    // 未被污染：无盖章（app_id 仍 0）、无 duostore 表、journal 未转 WAL
    CHECK_EQ(sqlite3_open(foreign.string().c_str(), &db), SQLITE_OK);
    auto query_i64 = [&](const char* sql) {
        sqlite3_stmt* st = nullptr;
        CHECK_EQ(sqlite3_prepare_v2(db, sql, -1, &st, nullptr), SQLITE_OK);
        CHECK_EQ(sqlite3_step(st), SQLITE_ROW);
        int64_t v = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
        return v;
    };
    CHECK_EQ(query_i64("PRAGMA application_id"), int64_t(0));
    CHECK_EQ(query_i64("SELECT count(*) FROM sqlite_master"), int64_t(1));
    sqlite3_close(db);
}

// 单进程独占 fail-fast（§1）：第二个实例（同进程模拟第二进程的 open）被 flock
// 拒绝；close 释放锁后可重开
TEST(duostore_sqlite_single_process_lock) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    SqliteMetaStore a(sqlite_opts(db));
    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(sqlite_opts(db)),
                    s3::S3ErrorCode::InternalError);
    a.close();
    SqliteMetaStore b(sqlite_opts(db));  // 锁已释放
    CHECK(!b.bucket_exists("x"));
    b.close();
}

// close 后调用干净失败（500），而非崩溃（§5.3）
TEST(duostore_sqlite_closed_store_throws) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.close();
    CHECK_THROWS_S3(m.bucket_exists("x"), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(m.create_bucket("x"), s3::S3ErrorCode::InternalError);
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_SQLITE_META
