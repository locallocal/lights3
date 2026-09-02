// SqliteMetaStore dedicated unit tests (docs/duostore-sqlite-meta.md §9): meta consistency suite,
// backend suite over the injected combination, BLOB key ordering (non-UTF-8 bytes), reopen durability, single-file cold backup,
// swap_extents CAS, file lineage validation. Zero external dependencies -- no probe/SKIP path like the Redis version.
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_SQLITE_META)

#include <signal.h>
#include <sqlite3.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "core/thread_pool.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/meta_dump.h"
#include "storage/duostore/rocks_meta_store.h"
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
    // Unit tests need no fsync (crash semantics are tested separately; the segment connection is internally always FULL, unaffected by this)
    SqliteMetaOptions o;
    o.path = file.string();
    o.sync = false;
    o.cache_bytes = 8ull << 20;
    o.pool_size = 4;
    return o;
}

}  // namespace

// Same meta semantics baseline (suite shared with RocksMetaStore / RedisMetaStore, §9.1);
// the factory repeatedly opens/closes the same DB file, naturally covering restart semantics (segments never roll back, schema validation)
TEST(duostore_sqlite_meta_store_suite) {
    TmpDir tmp;
    meta_store_suite::run_meta_store_suite([&] {
        return std::make_unique<SqliteMetaStore>(sqlite_opts(tmp.path / "meta.sqlite3"));
    });
}

// Run the backend consistency suite over the injected combination (SqliteMetaStore + FsDataStore) (§9.2)
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
        pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// BLOB key = memcmp order (§2.1): ordering and pagination tokens for keys with high-bit bytes / non-UTF-8 sequences
TEST(duostore_sqlite_binary_key_ordering) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("bin");
    // memcmp ascending (literals split up so \x does not greedily swallow following hex characters)
    std::vector<std::string> keys = {
        std::string("a\x01") + "b",        // 0x01 control byte
        "a\x7f",                           // DEL
        "a\xc3\x28",                       // invalid UTF-8 sequence
        "a\xff",                           // 0xff (a classic pitfall under TEXT storage)
        "b",
    };
    for (auto it = keys.rbegin(); it != keys.rend(); ++it)  // write out of order
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

// Reopen durability: objects/buckets/version preserved intact (WAL replay + the single file is the entire state)
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
        CHECK_EQ(m.peek_reclaims(10, 0).size(), size_t(1));  // the overwrite's old ledger entry is still there
        m.close();
    }
}

// Cold backup = copy the single file after a clean close (§6): no -wal/-shm residue, the copy opens with complete data
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

// Duplicate bucket creation -> BucketAlreadyOwnedByYou (§3.3)
TEST(duostore_sqlite_create_bucket_duplicate) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("dup");
    CHECK_THROWS_S3(m.create_bucket("dup"), s3::S3ErrorCode::BucketAlreadyOwnedByYou);
    m.delete_bucket("dup");
    m.close();
}

// swap_extents optimistic abandon path (§3.3 / main doc §9.2): mismatch -> false, and the transaction rolls back writing nothing
TEST(duostore_sqlite_swap_extents_cas) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("swap");
    uint64_t id1 = m.alloc_file_id(Extent::Kind::kChunk);
    uint64_t id2 = m.alloc_file_id(Extent::Kind::kChunk);
    DataRef from{{chunk_extent(id1, 8)}};
    DataRef to{{chunk_extent(id2, 8)}};
    m.put_object("swap", "k", make_rec("k", from.extents));  // version=1

    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));  // version mismatch
    CHECK(m.chunk_referenced(id1));
    CHECK(!m.chunk_referenced(id2));

    CHECK(m.swap_extents("swap", "k", /*expect_version=*/1, from, to));
    auto rec = m.get_object("swap", "k");
    CHECK_EQ(rec->version, uint64_t(2));
    CHECK(rec->data.extents == to.extents);
    CHECK(!m.chunk_referenced(id1));
    CHECK(m.chunk_referenced(id2));

    // After a swap the old from is stale -> swapping again must fail
    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));
    CHECK(m.delete_object("swap", "k"));
    m.delete_bucket("swap");
    m.close();
}

// File lineage (§2.2): wrong file (not SQLite / application_id mismatch) -> loud rejection
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

// File lineage (§2.2): app_id=0/ver=0 but tables already exist = somebody else's SQLite database (typical in the wild) --
// reject without leaving a trace: no table creation, no stamping, no WAL conversion
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

    // Unpolluted: no stamp (app_id still 0), no duostore tables, journal not converted to WAL
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

// Single-process exclusivity fail-fast (§1): a second instance (same process simulating a second process's open) is rejected
// by flock; reopening works after close releases the lock
TEST(duostore_sqlite_single_process_lock) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    SqliteMetaStore a(sqlite_opts(db));
    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(sqlite_opts(db)),
                    s3::S3ErrorCode::InternalError);
    a.close();
    SqliteMetaStore b(sqlite_opts(db));  // lock has been released
    CHECK(!b.bucket_exists("x"));
    b.close();
}

// Calls after close fail cleanly (500) instead of crashing (§5.3)
TEST(duostore_sqlite_closed_store_throws) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.close();
    CHECK_THROWS_S3(m.bucket_exists("x"), s3::S3ErrorCode::InternalError);
    CHECK_THROWS_S3(m.create_bucket("x"), s3::S3ErrorCode::InternalError);
}

// ---------- S4 crash simulation (§9/§10 S4: WAL replay reconciliation after kill) ----------
// The child process (execv of ourselves into duostore-sqlite-crash-child mode) commits in a loop with sync=true
// and reports line by line (each line is written only after the COMMIT's WAL fsync); the parent SIGKILLs at a random moment.
// After restart: every reported commit must exist (durability contract), refs<->objects reconcile both ways, gcq has
// no phantom entries, segments never roll back, integrity_check is clean -- the full acceptance test for WAL replay

namespace {

int sqlite_crash_child(int argc, char** argv) {
    if (argc < 3) return 2;
    SqliteMetaOptions o;
    o.path = argv[2];
    o.sync = true;  // the star of crash semantics: commit point = WAL fsync (§6)
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

    // Wait for the first reported commit before killing at a random point (guarantees the test is not a no-op)
    std::string buf;
    char ch;
    while (buf.find('\n') == std::string::npos && ::read(fd, &ch, 1) == 1) buf.push_back(ch);
    CHECK(buf.find('\n') != std::string::npos);
    usleep((100 + unsigned(::getpid()) % 300) * 1000);  // 100-400ms random window
    CHECK_EQ(::kill(pid, SIGKILL), 0);
    int stat = 0;
    CHECK(::waitpid(pid, &stat, 0) == pid);
    CHECK(WIFSIGNALED(stat) && WTERMSIG(stat) == SIGKILL);
    for (;;) {  // drain the pipe; only complete lines count
        char rb[4096];
        ssize_t n = ::read(fd, rb, sizeof rb);
        if (n <= 0) break;
        buf.append(rb, size_t(n));
    }
    ::close(fd);
    std::vector<std::pair<int, uint64_t>> reported;  // (i, file_id)
    for (size_t pos = 0; pos < buf.size();) {
        size_t nl = buf.find('\n', pos);
        if (nl == std::string::npos) break;  // trailing partial line: does not count
        std::string line = buf.substr(pos, nl - pos);
        pos = nl + 1;
        if (line.rfind("ok ", 0) != 0) continue;
        size_t sp = line.find(' ', 3);
        CHECK(sp != std::string::npos);
        reported.emplace_back(std::stoi(line.substr(3, sp - 3)),
                              std::stoull(line.substr(sp + 1)));
    }
    CHECK(!reported.empty());
    CHECK(fs::exists(db.string() + "-wal"));  // not closed: WAL awaiting replay

    uint64_t max_id = 0;
    {
        SqliteMetaStore m(sqlite_opts(db));
        // Every reported commit must exist, with correct bookkeeping
        for (const auto& [i, id] : reported) {
            auto rec = m.get_object("bkt", "k" + std::to_string(i));
            CHECK(rec.has_value());
            CHECK_EQ(rec->version, uint64_t(1));
            CHECK_EQ(rec->data.extents.at(0).file_id, id);
            CHECK(m.chunk_referenced(id));
            max_id = std::max(max_id, id);
        }
        // Reconciliation: the refs table = the union of all surviving objects' extents (may include committed-but-unreported
        // tail objects -- which must hold too; no orphan refs, no missing refs)
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
        // Unique keys with no overwrites -> gcq must be empty (no phantom entries)
        CHECK_EQ(m.peek_reclaims(10, 0).size(), size_t(0));
        // Segments never roll back: ids allocated after the crash are strictly greater than every used id (the counters connection is always FULL)
        CHECK(m.alloc_file_id(Extent::Kind::kChunk) > max_id);
        m.close();
    }
    // After a clean close the underlying database is physically intact
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

// S4 consistent-view injection (§2.3/§9): commit concurrently from the write connection mid-iteration of a list -- this list's WAL
// snapshot must stand rock solid (inserts invisible, deletions still visible, overwrites do not bleed through); the next list sees the new state
TEST(duostore_sqlite_list_consistent_view_under_concurrent_write) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("iso");
    for (const char* k : {"a", "b", "c", "d"}) m.put_object("iso", k, make_rec(k, {}));

    int fired = 0;
    m.set_list_pause_for_test([&] {
        ++fired;
        m.put_object("iso", "bb", make_rec("bb", {}));  // insert in an unvisited range
        CHECK(m.delete_object("iso", "d"));             // delete an unvisited key
        m.put_object("iso", "a", make_rec("a", {}));    // overwrite an already-visited key
    });
    auto r1 = m.list_objects("iso", {});
    CHECK_EQ(fired, 1);
    CHECK_EQ(r1.objects.size(), size_t(4));  // snapshot: exactly a b c d
    const char* want1[] = {"a", "b", "c", "d"};
    for (size_t i = 0; i < 4; ++i) CHECK_EQ(r1.objects[i].key, std::string(want1[i]));

    m.set_list_pause_for_test(nullptr);
    auto r2 = m.list_objects("iso", {});  // new view: bb visible, d gone
    CHECK_EQ(r2.objects.size(), size_t(4));
    const char* want2[] = {"a", "b", "bb", "c"};
    for (size_t i = 0; i < 4; ++i) CHECK_EQ(r2.objects[i].key, std::string(want2[i]));
    // The overwrite took effect outside the snapshot: a's version has been bumped
    CHECK_EQ(m.get_object("iso", "a")->version, uint64_t(2));

    for (const char* k : {"a", "b", "bb", "c"}) CHECK(m.delete_object("iso", k));
    m.delete_bucket("iso");
    m.close();
}

// S4 metrics: BUSY counter -- an external raw connection holds the write lock (flock only blocks our own instances, so it
// plays the "out-of-process visitor" perfectly, the BUSY row of the §5.4 table): a single-statement write exhausting busy_timeout -> 500 counts 1;
// segment reservation starving through all 4 bounded retry rounds -> 500 counts 4; recovers after the lock is released. busy_timeout_ms shortened to bound the duration
TEST(duostore_sqlite_busy_metric_counts_starvation) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = sqlite_opts(db);
    opts.busy_timeout_ms = 100;
    opts.metrics = MetricsScope(reg, {{"backend", "s4"}});
    SqliteMetaStore m(opts);
    // Registered at construction: zero value visible
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
                    s3::S3ErrorCode::InternalError);                             // +4 (4 starved rounds)
    CHECK_EQ(sqlite3_exec(ext, "ROLLBACK", nullptr, nullptr, nullptr), SQLITE_OK);
    sqlite3_close(ext);

    m.seal_pack(1, 0);  // recovers after the lock is released
    CHECK(reg->render().find(
              "lights3_duostore_sqlite_busy_total{backend=\"s4\"} 5\n") != std::string::npos);
    m.close();
}

// S4 metrics: corruption counter -- reopen after scribbling the file header; the open path's NOTADB is counted and loudly rejected
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
        f.write("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", 32);  // overwrite the SQLite file header magic
    }
    auto reg = std::make_shared<MetricsRegistry>();
    auto opts = sqlite_opts(db);
    opts.metrics = MetricsScope(reg, {{"backend", "s4c"}});
    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(opts), s3::S3ErrorCode::InternalError);
    CHECK(reg->render().find(
              "lights3_duostore_sqlite_corruption_total{backend=\"s4c\"} 1\n") !=
          std::string::npos);
}

// meta backup/restore doubling as cross-engine migration (docs/archive/gaps.md §6.1, meta_dump.h): rocks source dump ->
// sqlite target load, data directory shared in place (the unit-test incarnation of the restore procedure "place data first,
// then load meta"). Asserts: objects restored byte for byte (both pack and multi-chunk extents covered), deleted objects
// do not resurrect, new writes after restore do not collide with existing file numbers (counter is raised)
TEST(duostore_meta_dump_migrates_rocks_to_sqlite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.root = tmp.path / "duo";
    cfg.chunk_size = 4096;      // force multi-chunk manifests
    cfg.pack_threshold = 1024;  // small objects go to pack
    cfg.gc_interval_sec = 0;    // manual hook; no background contention
    fs::create_directories(cfg.root);
    auto mk_data = [&](IMetaStore* mp) {
        return std::make_unique<FsDataStore>(
            FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc, cfg.pack_threshold,
                          cfg.pack_max_size, cfg.pack_writers, {}},
            pool, [mp](Extent::Kind kind, uint32_t n) { return mp->alloc_file_run(kind, n); },
            [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
    };
    const std::string small(200, 's');    // pack record
    const std::string big(10000, 'b');    // a 3-chunk file
    std::stringstream archive;
    {
        cfg.name = "migrate-src";
        auto meta = std::make_unique<RocksMetaStore>(
            RocksMetaOptions{(tmp.path / "meta-rocks").string(), false, 8ull << 20});
        IMetaStore* mp = meta.get();
        auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), mk_data(mp));
        sync_wait(b->create_bucket("bkt"));
        backend_suite::put(*b, "bkt", "small", small);
        backend_suite::put(*b, "bkt", "big", big);
        backend_suite::put(*b, "bkt", "doomed", "gone");
        sync_wait(b->delete_object("bkt", "doomed"));  // create a gcq entry (deliberately not archived)
        auto st = sync_wait(b->run_meta_dump(archive));
        CHECK_EQ(st.buckets, uint64_t(1));
        CHECK_EQ(st.objects, uint64_t(2));
        sync_wait(b->close());
    }
    {
        cfg.name = "migrate-dst";
        auto meta = std::make_unique<SqliteMetaStore>(sqlite_opts(tmp.path / "meta.sqlite3"));
        IMetaStore* mp = meta.get();
        auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), mk_data(mp));
        auto st = sync_wait(b->run_meta_load(archive));  // built-in forced orphan scan
        CHECK_EQ(st.objects, uint64_t(2));
        auto g1 = sync_wait(b->get_object("bkt", "small", std::nullopt));
        CHECK_EQ(backend_suite::read_all(*g1.body), small);
        auto g2 = sync_wait(b->get_object("bkt", "big", std::nullopt));
        CHECK_EQ(backend_suite::read_all(*g2.body), big);
        CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "doomed")),
                        s3::S3ErrorCode::NoSuchKey);
        // Counter raised: file numbers allocated by new writes must not collide with existing ones (a collision = silently clobbering existing data)
        const std::string fresh(9000, 'n');
        backend_suite::put(*b, "bkt", "fresh", fresh);
        auto g3 = sync_wait(b->get_object("bkt", "fresh", std::nullopt));
        CHECK_EQ(backend_suite::read_all(*g3.body), fresh);
        auto g4 = sync_wait(b->get_object("bkt", "big", std::nullopt));
        CHECK_EQ(backend_suite::read_all(*g4.body), big);  // existing data not clobbered
        sync_wait(b->close());
    }
}

// Schema evolution policy (docs/archive/gaps.md §6.1): user_version newer than this build -> refuse to run downgraded;
// older than current with no migration in the chain -> loud failure ("changing layout without leaving a migration" is a programming error).
// Neither rejection may pollute the database -- after restoring the real version it must reopen normally
TEST(duostore_sqlite_schema_version_policy) {
    TmpDir tmp;
    fs::path db = tmp.path / "meta.sqlite3";
    {
        SqliteMetaStore m(sqlite_opts(db));
        m.create_bucket("x");
        m.close();
    }
    auto set_user_version = [&](int64_t v) {
        sqlite3* raw = nullptr;
        CHECK_EQ(sqlite3_open(db.string().c_str(), &raw), SQLITE_OK);
        std::string sql = "PRAGMA user_version=" + std::to_string(v);
        CHECK_EQ(sqlite3_exec(raw, sql.c_str(), nullptr, nullptr, nullptr), SQLITE_OK);
        sqlite3_close(raw);
    };
    set_user_version(999);  // future version: a database written by a newer program
    CHECK_THROWS_S3(std::make_unique<SqliteMetaStore>(sqlite_opts(db)),
                    s3::S3ErrorCode::InternalError);
    set_user_version(1);  // restore the real version; the database was not polluted by the rejection paths
    {
        SqliteMetaStore m(sqlite_opts(db));
        CHECK(m.bucket_exists("x"));
        m.close();
    }
}

// Online meta dump (roadmap §3.7): the snapshot view is a held-open WAL read
// transaction — writes committed after snapshot() stay invisible to every read
// through the view, while the write connection keeps committing
TEST(duostore_sqlite_snapshot_dump_is_consistent) {
    TmpDir tmp;
    SqliteMetaStore m(sqlite_opts(tmp.path / "meta.sqlite3"));
    m.create_bucket("b");
    m.put_object("b", "k1", make_rec("k1", {chunk_extent(1, 10)}));
    m.put_object("b", "k2", make_rec("k2", {chunk_extent(2, 10)}));

    auto view = m.snapshot();
    CHECK(view != nullptr);
    m.put_object("b", "k3", make_rec("k3", {chunk_extent(3, 10)}));  // after the snapshot
    m.delete_object("b", "k1");
    m.create_bucket("b2");

    CHECK_EQ(view->list_buckets().size(), size_t(1));
    CHECK(view->get_object("b", "k1").has_value());
    CHECK(!view->get_object("b", "k3").has_value());
    std::ostringstream out;
    auto st = dump_meta(*view, out);
    CHECK_EQ(st.buckets, uint64_t(1));
    CHECK_EQ(st.objects, uint64_t(2));
    view.reset();

    // The live store sees everything, and the writes committed during the
    // snapshot's lifetime landed
    CHECK(m.get_object("b", "k3").has_value());
    CHECK(!m.get_object("b", "k1").has_value());
    CHECK_EQ(m.list_buckets().size(), size_t(2));
    m.close();
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_SQLITE_META
