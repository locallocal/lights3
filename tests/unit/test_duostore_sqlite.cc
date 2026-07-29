// SqliteMetaStore 专项单测（docs/duostore-sqlite-meta.md §9）：meta 一致性套件、
// 注入组合跑后端套件、BLOB key 排序（非 UTF-8 字节）、重开持久性、冷备单文件、
// swap_extents CAS、文件谱系校验。零外部依赖——无 Redis 版的探测/SKIP 路径。
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_SQLITE_META)

#include <signal.h>
#include <sqlite3.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "core/metrics.h"
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
    SqliteMetaOptions o;
    o.path = file.string();
    o.sync = false;
    o.cache_bytes = 8ull << 20;
    o.pool_size = 4;
    return o;
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
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                      cfg.pack_max_size, cfg.pack_writers, {}},
        pool, [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
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
        CHECK_EQ(m.peek_reclaims(10, 0).size(), size_t(1));  // 覆盖写的旧账仍在
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

// ---------- S4 崩溃模拟（§9/§10 S4：kill 后 WAL 回放对账）----------
// 子进程（execv 自身进 duostore-sqlite-crash-child 模式）以 sync=true 循环提交
// 并逐行回报（行在 COMMIT 的 WAL fsync 之后才写出）；父进程随机时刻 SIGKILL。
// 重启后：凡回报过的提交必在（持久性契约）、refs↔objects 双向对账收敛、gcq
// 无幻账、号段不回退、integrity_check 干净——WAL 回放的完整验收

namespace {

int sqlite_crash_child(int argc, char** argv) {
    if (argc < 3) return 2;
    SqliteMetaOptions o;
    o.path = argv[2];
    o.sync = true;  // 崩溃语义主角：提交点 = WAL fsync（§6）
    SqliteMetaStore m(o);
    m.create_bucket("bkt");
    for (int i = 0;; ++i) {
        uint64_t id = m.alloc_file_id(Extent::Kind::kChunk);
        std::string k = "k" + std::to_string(i);
        m.put_object("bkt", k, make_rec(k, {chunk_extent(id, 8)}));
        std::string line = "ok " + std::to_string(i) + " " + std::to_string(id) + "\n";
        if (::write(1, line.data(), line.size()) < 0) return 4;
    }
}

mini_test::ChildRegistrar sqlite_crash_reg("duostore-sqlite-crash-child",
                                           sqlite_crash_child);

pid_t spawn_sqlite_crash_child(const fs::path& db, int* out_fd) {
    int pfd[2] = {-1, -1};
    CHECK(::pipe(pfd) == 0);
    pid_t pid = ::fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        ::dup2(pfd[1], 1);
        ::close(pfd[0]);
        ::close(pfd[1]);
        char exe[4096];
        ssize_t n = ::readlink("/proc/self/exe", exe, sizeof exe - 1);
        if (n <= 0) ::_exit(127);
        exe[n] = 0;
        ::execl(exe, exe, "duostore-sqlite-crash-child", db.c_str(), (char*)nullptr);
        ::_exit(127);
    }
    ::close(pfd[1]);
    *out_fd = pfd[0];
    return pid;
}

}  // namespace

TEST(duostore_sqlite_crash_wal_replay_reconciles) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    int fd = -1;
    pid_t pid = spawn_sqlite_crash_child(db, &fd);

    // 等到首个提交回报后再随机点开杀（保证测试非空转）
    std::string buf;
    char ch;
    while (buf.find('\n') == std::string::npos && ::read(fd, &ch, 1) == 1) buf.push_back(ch);
    CHECK(buf.find('\n') != std::string::npos);
    usleep((100 + unsigned(::getpid()) % 300) * 1000);  // 100-400ms 随机窗口
    CHECK_EQ(::kill(pid, SIGKILL), 0);
    int stat = 0;
    CHECK(::waitpid(pid, &stat, 0) == pid);
    CHECK(WIFSIGNALED(stat) && WTERMSIG(stat) == SIGKILL);
    for (;;) {  // 排空管道；只认完整行
        char rb[4096];
        ssize_t n = ::read(fd, rb, sizeof rb);
        if (n <= 0) break;
        buf.append(rb, size_t(n));
    }
    ::close(fd);
    std::vector<std::pair<int, uint64_t>> reported;  // (i, file_id)
    for (size_t pos = 0; pos < buf.size();) {
        size_t nl = buf.find('\n', pos);
        if (nl == std::string::npos) break;  // 尾部半行：不作数
        std::string line = buf.substr(pos, nl - pos);
        pos = nl + 1;
        if (line.rfind("ok ", 0) != 0) continue;
        size_t sp = line.find(' ', 3);
        CHECK(sp != std::string::npos);
        reported.emplace_back(std::stoi(line.substr(3, sp - 3)),
                              std::stoull(line.substr(sp + 1)));
    }
    CHECK(!reported.empty());
    CHECK(fs::exists(db.string() + "-wal"));  // 未 close：WAL 待回放

    uint64_t max_id = 0;
    {
        SqliteMetaStore m(sqlite_opts(db));
        // 回报过的提交必在，且账目正确
        for (const auto& [i, id] : reported) {
            auto rec = m.get_object("bkt", "k" + std::to_string(i));
            CHECK(rec.has_value());
            CHECK_EQ(rec->version, uint64_t(1));
            CHECK_EQ(rec->data.extents.at(0).file_id, id);
            CHECK(m.chunk_referenced(id));
            max_id = std::max(max_id, id);
        }
        // 对账：refs 表 = 全部存活对象 extents 的并集（可能含已提交未回报的尾部
        // 对象——同样要成立；无孤儿 ref、无漏 ref）
        std::set<uint64_t> live;
        ListOptions lo;
        for (;;) {
            auto r = m.list_objects("bkt", lo);
            for (const auto& o : r.objects) {
                auto rec = m.get_object("bkt", o.key);
                CHECK(rec.has_value());
                for (const auto& e : rec->data.extents) live.insert(e.file_id);
            }
            if (!r.is_truncated) break;
            lo.start_after = r.next_token;
        }
        std::set<uint64_t> refs;
        m.scan_refs([&](uint64_t id) { refs.insert(id); });
        CHECK(live == refs);
        // 唯一 key 无覆盖写 → gcq 必空（无幻账）
        CHECK_EQ(m.peek_reclaims(10, 0).size(), size_t(0));
        // 号段不回退：崩溃后新派发的 id 严格大于一切已用 id（counters 连接恒 FULL）
        CHECK(m.alloc_file_id(Extent::Kind::kChunk) > max_id);
        m.close();
    }
    // 干净 close 后底层库物理完好
    sqlite3* raw = nullptr;
    CHECK_EQ(sqlite3_open(db.string().c_str(), &raw), SQLITE_OK);
    sqlite3_stmt* st = nullptr;
    CHECK_EQ(sqlite3_prepare_v2(raw, "PRAGMA integrity_check", -1, &st, nullptr), SQLITE_OK);
    CHECK_EQ(sqlite3_step(st), SQLITE_ROW);
    CHECK_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(st, 0))),
             std::string("ok"));
    sqlite3_finalize(st);
    sqlite3_close(raw);
}

// S4 一致视图注入（§2.3/§9）：list 迭代中途从写连接并发提交——本次 list 的 WAL
// snapshot 必须岿然不动（插入不可见、删除仍可见、覆盖不串台）；下一次 list 见新态
TEST(duostore_sqlite_list_consistent_view_under_concurrent_write) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("iso");
    for (const char* k : {"a", "b", "c", "d"}) m.put_object("iso", k, make_rec(k, {}));

    int fired = 0;
    m.set_list_pause_for_test([&] {
        ++fired;
        m.put_object("iso", "bb", make_rec("bb", {}));  // 未访问区间插入
        CHECK(m.delete_object("iso", "d"));             // 未访问 key 删除
        m.put_object("iso", "a", make_rec("a", {}));    // 已访问 key 覆盖
    });
    auto r1 = m.list_objects("iso", {});
    CHECK_EQ(fired, 1);
    CHECK_EQ(r1.objects.size(), size_t(4));  // snapshot：恰是 a b c d
    const char* want1[] = {"a", "b", "c", "d"};
    for (size_t i = 0; i < 4; ++i) CHECK_EQ(r1.objects[i].key, std::string(want1[i]));

    m.set_list_pause_for_test(nullptr);
    auto r2 = m.list_objects("iso", {});  // 新视图：bb 可见、d 没了
    CHECK_EQ(r2.objects.size(), size_t(4));
    const char* want2[] = {"a", "b", "bb", "c"};
    for (size_t i = 0; i < 4; ++i) CHECK_EQ(r2.objects[i].key, std::string(want2[i]));
    // 覆盖写生效于快照之外：a 的 version 已 bump
    CHECK_EQ(m.get_object("iso", "a")->version, uint64_t(2));

    for (const char* k : {"a", "b", "bb", "c"}) CHECK(m.delete_object("iso", k));
    m.delete_bucket("iso");
    m.close();
}

// S4 指标：BUSY 计数——外部裸连接持写锁（flock 只拦本店实例，正好扮演"进程外
// 来客"，§5.4 表的 BUSY 行）：单语句写 busy_timeout 耗尽 → 500 计 1；号段预留
// 有界重试 4 轮全饥饿 → 500 计 4；锁释放后恢复。busy_timeout_ms 调短控制时长
TEST(duostore_sqlite_busy_metric_counts_starvation) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = sqlite_opts(db);
    opts.busy_timeout_ms = 100;
    opts.metrics = MetricsScope(reg, {{"backend", "s4"}});
    SqliteMetaStore m(opts);
    // 构造期注册：0 值可见
    CHECK(reg->render().find(
              "lights3_duostore_sqlite_busy_total{backend=\"s4\"} 0\n") != std::string::npos);
    CHECK(reg->render().find(
              "lights3_duostore_sqlite_corruption_total{backend=\"s4\"} 0\n") !=
          std::string::npos);

    sqlite3* ext = nullptr;
    CHECK_EQ(sqlite3_open(db.string().c_str(), &ext), SQLITE_OK);
    CHECK_EQ(sqlite3_exec(ext, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr), SQLITE_OK);
    CHECK_THROWS_S3(m.seal_pack(1, 0), s3::S3ErrorCode::InternalError);          // +1
    CHECK_THROWS_S3(m.alloc_file_id(Extent::Kind::kChunk),
                    s3::S3ErrorCode::InternalError);                             // +4（4 轮饥饿）
    CHECK_EQ(sqlite3_exec(ext, "ROLLBACK", nullptr, nullptr, nullptr), SQLITE_OK);
    sqlite3_close(ext);

    m.seal_pack(1, 0);  // 锁释放后恢复
    CHECK(reg->render().find(
              "lights3_duostore_sqlite_busy_total{backend=\"s4\"} 5\n") != std::string::npos);
    m.close();
}

// S4 指标：corruption 计数——文件头写花后重开，打开路径的 NOTADB 计入并响亮拒绝
TEST(duostore_sqlite_corruption_metric_counts_notadb) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    {
        SqliteMetaStore m(sqlite_opts(db));
        m.create_bucket("x");
        m.close();
    }
    {
        std::fstream f(db, std::ios::in | std::ios::out | std::ios::binary);
        f.write("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", 32);  // 覆写 SQLite 文件头魔数
    }
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = sqlite_opts(db);
    opts.metrics = MetricsScope(reg, {{"backend", "s4c"}});
    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(opts), s3::S3ErrorCode::InternalError);
    CHECK(reg->render().find(
              "lights3_duostore_sqlite_corruption_total{backend=\"s4c\"} 1\n") !=
          std::string::npos);
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_SQLITE_META
