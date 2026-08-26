#include "storage/duostore/sqlite_meta_store.h"

#include <fcntl.h>
#include <sqlite3.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <map>
#include <set>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/meta_util.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// File lineage (§2.2): application_id marks "this is a duostore sqlite meta file",
// user_version is the schema version — SQLite built-in mechanisms, zero tables needed
constexpr int64_t kAppId = 0x4C335351;  // "L3SQ"
constexpr int64_t kSchemaVersion = 1;

// The counters table stores only file_id segments ('chunk' / 'pack'); gcq seq uses AUTOINCREMENT (§2.2)
constexpr const char* kCtrChunk = "chunk";
constexpr const char* kCtrPack = "pack";

// Table-creation DDL (§2.2): key columns are all BLOB (memcmp order = S3
// lexicographic order), STRICT enforces column types, primary key doubles as the
// clustered index (WITHOUT ROWID). counters seeds are not in the DDL — they are
// written via bound statements from the kCtrChunk/kCtrPack constants (init_schema):
// single source of truth, no hand-written hex
constexpr const char* kSchemaDdl = R"(
CREATE TABLE IF NOT EXISTS buckets(
  name BLOB PRIMARY KEY,
  val  BLOB NOT NULL
) WITHOUT ROWID, STRICT;
CREATE TABLE IF NOT EXISTS objects(
  bucket BLOB NOT NULL, key BLOB NOT NULL, val BLOB NOT NULL,
  PRIMARY KEY(bucket, key)
) WITHOUT ROWID, STRICT;
CREATE TABLE IF NOT EXISTS uploads(
  bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL, val BLOB NOT NULL,
  PRIMARY KEY(bucket, key, id)
) WITHOUT ROWID, STRICT;
CREATE TABLE IF NOT EXISTS parts(
  bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
  part_no INTEGER NOT NULL, val BLOB NOT NULL,
  PRIMARY KEY(bucket, key, id, part_no)
) WITHOUT ROWID, STRICT;
CREATE TABLE IF NOT EXISTS refs(
  file_id INTEGER PRIMARY KEY, owner BLOB NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS gcq(
  seq INTEGER PRIMARY KEY AUTOINCREMENT, val BLOB NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS counters(
  name BLOB PRIMARY KEY, val INTEGER NOT NULL
) WITHOUT ROWID, STRICT;
CREATE TABLE IF NOT EXISTS pack_stats(
  pack_id INTEGER PRIMARY KEY, file_size INTEGER NOT NULL DEFAULT 0,
  live_bytes INTEGER NOT NULL DEFAULT 0, live_recs INTEGER NOT NULL DEFAULT 0,
  sealed INTEGER NOT NULL DEFAULT 0
) STRICT;
)";

// SQL constants (§5.3): each connection keeps a resident prepared-statement cache
// keyed by the literal's address; all parameters are bound via ?N — string
// concatenation is forbidden (BLOB truncation source + injection surface)
constexpr const char* kBegin = "BEGIN";
constexpr const char* kBeginImmediate = "BEGIN IMMEDIATE";
constexpr const char* kCommit = "COMMIT";
constexpr const char* kRollback = "ROLLBACK";
constexpr const char* kBucketGet = "SELECT val FROM buckets WHERE name=?1";
constexpr const char* kBucketPut = "INSERT INTO buckets(name,val) VALUES(?1,?2)";
constexpr const char* kBucketDel = "DELETE FROM buckets WHERE name=?1";
constexpr const char* kBucketList = "SELECT name,val FROM buckets ORDER BY name";
constexpr const char* kObjGet = "SELECT val FROM objects WHERE bucket=?1 AND key=?2";
constexpr const char* kObjPut =
    "INSERT OR REPLACE INTO objects(bucket,key,val) VALUES(?1,?2,?3)";
constexpr const char* kObjDel = "DELETE FROM objects WHERE bucket=?1 AND key=?2";
constexpr const char* kObjAny = "SELECT 1 FROM objects WHERE bucket=?1 LIMIT 1";
constexpr const char* kObjScanGe =
    "SELECT key,val FROM objects WHERE bucket=?1 AND key>=?2 ORDER BY key";
constexpr const char* kObjPrev =
    "SELECT key FROM objects WHERE bucket=?1 AND key<?2 ORDER BY key DESC LIMIT 1";
constexpr const char* kUpGet = "SELECT val FROM uploads WHERE bucket=?1 AND key=?2 AND id=?3";
constexpr const char* kUpPut = "INSERT INTO uploads(bucket,key,id,val) VALUES(?1,?2,?3,?4)";
constexpr const char* kUpDel = "DELETE FROM uploads WHERE bucket=?1 AND key=?2 AND id=?3";
constexpr const char* kUpAny = "SELECT 1 FROM uploads WHERE bucket=?1 LIMIT 1";
// Composite-cursor pushdown (docs/archive/gaps.md §5.1): (key,id) > (?2,?3) uses row-value
// comparison, which follows primary-key order exactly; ?4<=0 means unlimited rows
// (a negative LIMIT in SQLite means no limit)
constexpr const char* kUpList =
    "SELECT key,id,val FROM uploads WHERE bucket=?1 AND (key,id) > (?2,?3) "
    "ORDER BY key,id LIMIT ?4";
constexpr const char* kPartGet =
    "SELECT val FROM parts WHERE bucket=?1 AND key=?2 AND id=?3 AND part_no=?4";
constexpr const char* kPartPut =
    "INSERT OR REPLACE INTO parts(bucket,key,id,part_no,val) VALUES(?1,?2,?3,?4,?5)";
constexpr const char* kPartScan =
    "SELECT part_no,val FROM parts WHERE bucket=?1 AND key=?2 AND id=?3 ORDER BY part_no";
constexpr const char* kPartDelAll = "DELETE FROM parts WHERE bucket=?1 AND key=?2 AND id=?3";
constexpr const char* kRefPut = "INSERT OR REPLACE INTO refs(file_id,owner) VALUES(?1,?2)";
constexpr const char* kRefDel = "DELETE FROM refs WHERE file_id=?1";
constexpr const char* kRefGet = "SELECT 1 FROM refs WHERE file_id=?1";
constexpr const char* kRefScan = "SELECT file_id FROM refs";
constexpr const char* kGcqPut = "INSERT INTO gcq(val) VALUES(?1)";
constexpr const char* kGcqPeek = "SELECT seq,val FROM gcq WHERE seq>=?2 ORDER BY seq LIMIT ?1";
constexpr const char* kGcqDel = "DELETE FROM gcq WHERE seq=?1";
constexpr const char* kCtrReserve =
    "UPDATE counters SET val=val+?1 WHERE name=?2 RETURNING val";
constexpr const char* kCtrSeed = "INSERT OR IGNORE INTO counters(name,val) VALUES(?1,0)";
constexpr const char* kAnyTable = "SELECT 1 FROM sqlite_master LIMIT 1";
// Pack liveness accounting (§2.2: native numeric columns, arithmetic UPDATE gives
// incremental accounting, batched with the business write inside the transaction)
constexpr const char* kPackDelta =
    "INSERT INTO pack_stats(pack_id,live_bytes,live_recs) VALUES(?1,?2,?3) "
    "ON CONFLICT(pack_id) DO UPDATE SET live_bytes=live_bytes+excluded.live_bytes,"
    "live_recs=live_recs+excluded.live_recs";
// Seal (idempotent): file_size=0 means unknown and must not overwrite a recorded non-zero value (IMetaStore contract)
constexpr const char* kPackSeal =
    "INSERT INTO pack_stats(pack_id,file_size,sealed) VALUES(?1,?2,1) "
    "ON CONFLICT(pack_id) DO UPDATE SET sealed=1,file_size=CASE WHEN "
    "excluded.file_size>0 THEN excluded.file_size ELSE file_size END";
constexpr const char* kPackList =
    "SELECT pack_id,file_size,live_bytes,live_recs,sealed FROM pack_stats ORDER BY pack_id";
constexpr const char* kPackDrop = "DELETE FROM pack_stats WHERE pack_id=?1";

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

using codec::bump_last_byte;  // successor for delimiter group skipping (codec.h, shared by meta store impls)

}  // namespace

// ---------- Connection / statement / transaction primitives ----------

struct SqliteMetaStore::Conn {
    sqlite3* db = nullptr;
    std::map<const char*, sqlite3_stmt*> stmts;  // key = SQL literal address (§5.3)
    // S4 metrics: error-classification counters (§5.4). Assigned by open_raw;
    // isolated instances under an empty scope, always non-null
    std::shared_ptr<MetricCounter> busy;
    std::shared_ptr<MetricCounter> corrupt;

    ~Conn() {
        for (auto& [sql, st] : stmts) sqlite3_finalize(st);
        if (db && sqlite3_close(db) != SQLITE_OK)
            LOG_ERROR("duostore meta(sqlite): close connection: {}", sqlite3_errmsg(db));
    }

    // Error classification (metric slices for the §5.4 table): BUSY = busy_timeout
    // exhausted without getting the lock (someone outside the process touching the
    // db / id-segment starvation); CORRUPT/NOTADB = data-loss signal, tracked as a
    // dedicated corruption alert
    void classify_error() const {
        if (!db) return;
        int rc = sqlite3_extended_errcode(db) & 0xff;
        if (rc == SQLITE_BUSY && busy) busy->inc();
        if ((rc == SQLITE_CORRUPT || rc == SQLITE_NOTADB) && corrupt) {
            corrupt->inc();
            LOG_ERROR("duostore meta(sqlite): corruption detected (rc={}) — data loss signal",
                      rc);
        }
    }

    [[noreturn]] void raise(const char* what) const {
        std::string msg = db ? sqlite3_errmsg(db) : "no connection";
        classify_error();
        LOG_ERROR("duostore meta(sqlite): {}: {}", what, msg);
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("duostore meta(sqlite): ") + what + ": " + msg);
    }

    sqlite3_stmt* get(const char* sql) {
        auto it = stmts.find(sql);
        if (it != stmts.end()) return it->second;
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) raise("prepare");
        stmts.emplace(sql, st);
        return st;
    }

    // Direct execution of fixed statements (PRAGMA / BEGIN / DDL etc.); multi-statement scripts allowed
    void exec(const std::string& sql, const char* what) {
        char* err = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : sqlite3_errmsg(db);
            sqlite3_free(err);
            classify_error();
            LOG_ERROR("duostore meta(sqlite): {}: {}", what, msg);
            throw S3Error(S3ErrorCode::InternalError,
                          std::string("duostore meta(sqlite): ") + what + ": " + msg);
        }
    }
};

// Single-use RAII around a prepared statement: bind → step*, destructor does
// reset + clear_bindings. Bindings always use SQLITE_TRANSIENT (sqlite copies) —
// avoiding lifetime contracts entangled with reseek/exception paths; meta values
// are small, so the copy cost is negligible
class SqliteMetaStore::Stmt {
public:
    Stmt(Conn& c, const char* sql) : c_(c), s_(c.get(sql)) {}
    ~Stmt() {
        sqlite3_reset(s_);
        sqlite3_clear_bindings(s_);
    }
    Stmt(const Stmt&) = delete;

    Stmt& blob(int i, std::string_view v) {
        // An empty string must pass a non-null pointer: bind_blob(nullptr) means SQL NULL, not a zero-length BLOB
        if (sqlite3_bind_blob(s_, i, v.empty() ? "" : v.data(), int(v.size()),
                              SQLITE_TRANSIENT) != SQLITE_OK)
            c_.raise("bind blob");
        return *this;
    }
    Stmt& i64(int i, int64_t v) {
        if (sqlite3_bind_int64(s_, i, v) != SQLITE_OK) c_.raise("bind int");
        return *this;
    }

    bool step() {  // true = row available, false = done
        int rc = sqlite3_step(s_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        c_.raise("step");
    }
    // BUSY-tolerant variant (id-segment path only, §4): nullopt = SQLITE_BUSY — the
    // single-statement autocommit transaction failed while acquiring the write lock,
    // so it definitely did not execute and retrying is safe; otherwise same as step()
    std::optional<bool> step_busy() {
        int rc = sqlite3_step(s_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        if (rc == SQLITE_BUSY) {
            if (c_.busy) c_.busy->inc();  // count once per starvation round (S4 metric)
            return std::nullopt;
        }
        c_.raise("step");
    }
    void exec() {  // run to completion (DML / draining RETURNING)
        while (step()) {
        }
    }

    std::string_view col_blob(int i) {
        const void* p = sqlite3_column_blob(s_, i);
        int n = sqlite3_column_bytes(s_, i);
        return {static_cast<const char*>(p ? p : ""), size_t(n)};
    }
    int64_t col_i64(int i) { return sqlite3_column_int64(s_, i); }

private:
    Conn& c_;
    sqlite3_stmt* s_;
};

// Transaction RAII (§3.2): write transactions use BEGIN IMMEDIATE (already
// serialized in-process by mu_, so never BUSY), read transactions use BEGIN
// (WAL snapshot, consistent view); destruction without commit means ROLLBACK — when
// a semantic error throws S3Error out of a method the transaction rolls back
// automatically, ruling out half-done state residue
class SqliteMetaStore::Txn {
public:
    explicit Txn(Conn& c, bool immediate = true) : c_(c) {
        Stmt(c_, immediate ? kBeginImmediate : kBegin).exec();
    }
    ~Txn() {
        if (done_) return;
        try {
            Stmt(c_, kRollback).exec();
        } catch (const std::exception& e) {
            LOG_ERROR("duostore meta(sqlite): rollback: {}", e.what());
        }
    }
    void commit() {
        Stmt(c_, kCommit).exec();
        done_ = true;
    }

private:
    Conn& c_;
    bool done_ = false;
};

SqliteMetaStore::Lease::Lease(SqliteMetaStore* s, std::unique_ptr<Conn> c)
    : store(s), conn(std::move(c)) {}
SqliteMetaStore::Lease::Lease(Lease&&) noexcept = default;
SqliteMetaStore::Conn& SqliteMetaStore::Lease::operator*() const { return *conn; }

SqliteMetaStore::Lease::~Lease() {
    if (store && conn) store->release(std::move(conn));
}

// ---------- Open / schema / close ----------

std::unique_ptr<SqliteMetaStore::Conn> SqliteMetaStore::open_raw() {
    auto c = std::make_unique<Conn>();
    c->busy = m_busy_;
    c->corrupt = m_corrupt_;
    if (sqlite3_open_v2(opt_.path.c_str(), &c->db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        std::string msg = c->db ? sqlite3_errmsg(c->db) : "out of memory";
        LOG_ERROR("duostore meta(sqlite): open {}: {}", opt_.path, msg);
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): open " + opt_.path + ": " + msg);
    }
    // Defense in depth: in-process code should never see prolonged BUSY (§5.2); tests shorten busy_timeout_ms for injection
    sqlite3_busy_timeout(c->db, opt_.busy_timeout_ms);
    return c;
}

void SqliteMetaStore::apply_pragmas(Conn& c, bool full_sync) {
    // The page cache is per-connection — spread the cache_bytes budget across all
    // connections (1 write + 1 alloc + pool_size readers) so sqlite_cache means the
    // process-level total budget (matching the role of rocksdb_block_cache, §8);
    // a 256KiB floor covers any division edge cases (negative value is in KiB).
    // journal_mode is a persistent property of the database, written into the file
    // header by the first connection
    size_t per_conn_kib =
        std::max<size_t>(opt_.cache_bytes / size_t(opt_.pool_size + 2) / 1024, 256);
    // journal_size_limit (S4 tuning, §6): after auto-checkpoint (default 1000 pages)
    // drains the WAL, truncate the -wal file back to the limit — otherwise the WAL
    // stays at its high-water size for the whole run. Everything else keeps defaults
    // (assessment in §6: no extra checkpoint policy or optimize scheduling)
    c.exec("PRAGMA journal_mode=WAL;"
           "PRAGMA synchronous=" + std::string(full_sync ? "FULL" : "NORMAL") + ";" +
           "PRAGMA cache_size=-" + std::to_string(per_conn_kib) + ";" +
           "PRAGMA journal_size_limit=4194304;"
           "PRAGMA temp_store=MEMORY;"
           "PRAGMA foreign_keys=OFF;",
           "open pragmas");
}

std::unique_ptr<SqliteMetaStore::Conn> SqliteMetaStore::open_conn(bool full_sync) {
    auto c = open_raw();
    apply_pragmas(*c, full_sync);
    return c;
}

void SqliteMetaStore::check_lineage(Conn& c) {
    int64_t app_id = 0, ver = 0;
    {
        Stmt st(c, "PRAGMA application_id");
        if (st.step()) app_id = st.col_i64(0);
    }
    {
        Stmt st(c, "PRAGMA user_version");
        if (st.step()) ver = st.col_i64(0);
    }
    if (app_id == 0 && ver == 0) {
        // No lineage mark: a random SQLite database typically has exactly
        // app_id=0/ver=0 — if tables exist it is someone else's database, so refuse
        // (no write or WAL conversion has happened yet, the file is untouched);
        // only a truly empty database may get our schema
        Stmt st(c, kAnyTable);
        if (st.step())
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta(sqlite): not a duostore meta database "
                          "(existing tables without lineage mark): " + opt_.path);
        return;
    }
    if (app_id != kAppId)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): not a duostore meta database: " + opt_.path);
    // Version-evolution policy matches the other three engines (docs/archive/gaps.md §6.1):
    // a newer database refuses to run downgraded, an older one climbs the migration
    // chain step by step (user_version is an integer lineage, no string prefix)
    if (ver > kSchemaVersion)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): database schema v" + std::to_string(ver) +
                          " is newer than this build (v" + std::to_string(kSchemaVersion) +
                          "); refusing to run downgraded");
    if (ver < kSchemaVersion) migrate_schema(c, ver);
}

// Migration chain (in-place SQL transforms from version n → n+1; one transaction
// per step, including the user_version stamp — after a mid-way crash, restart
// resumes from the breakpoint). Register new layout changes here and bump
// kSchemaVersion by 1
void SqliteMetaStore::migrate_schema(Conn& c, int64_t ver) {
    using MigrateFn = void (*)(Conn&);
    static constexpr std::array<std::pair<int64_t, MigrateFn>, 0> kSchemaMigrations{
        // {{1, &migrate_v1_to_v2}}  // example: once registered, v1 databases upgrade automatically at startup
    };
    for (; ver < kSchemaVersion; ++ver) {
        MigrateFn fn = nullptr;
        for (auto& [from, f] : kSchemaMigrations)
            if (from == ver) fn = f;
        if (!fn) throw_no_migration(ver, kSchemaVersion, "duostore meta(sqlite)");
        Txn t(c);
        fn(c);
        c.exec("PRAGMA user_version=" + std::to_string(ver + 1) + ";", "stamp schema");
        t.commit();
        LOG_INFO("duostore meta(sqlite): schema migrated v{} -> v{}", ver, ver + 1);
    }
}

void SqliteMetaStore::init_schema(Conn& c) {
    int64_t ver = 0;
    {
        Stmt st(c, "PRAGMA user_version");
        if (st.step()) ver = st.col_i64(0);
    }
    if (ver != 0) return;  // already our database (version validated by check_lineage)
    Txn t(c);
    c.exec(kSchemaDdl, "create schema");
    for (const char* n : {kCtrChunk, kCtrPack}) {
        Stmt st(c, kCtrSeed);
        st.blob(1, n);
        st.exec();
    }
    c.exec("PRAGMA application_id=" + std::to_string(kAppId) + ";" +
           "PRAGMA user_version=" + std::to_string(kSchemaVersion) + ";",
           "stamp schema");
    t.commit();
}

SqliteMetaStore::SqliteMetaStore(SqliteMetaOptions opt) : opt_(std::move(opt)) {
    // S4 metrics: registered before any connection — NOTADB/CORRUPT on the open
    // path (wrong file, broken database) must also be counted; an empty scope
    // returns isolated instances, so tests construct directly with zero wiring cost
    m_busy_ = opt_.metrics.counter(
        "lights3_duostore_sqlite_busy_total",
        "Statements that saw SQLITE_BUSY (busy_timeout exhausted: external writer "
        "on the db file, or starved id reservation)");
    m_corrupt_ = opt_.metrics.counter(
        "lights3_duostore_sqlite_corruption_total",
        "SQLITE_CORRUPT/SQLITE_NOTADB errors observed (data loss signal)");

    // The parent directory is created by this store itself (the file belongs to us,
    // covering every caller; failures are left for open to report)
    std::error_code ec;
    auto parent = std::filesystem::path(opt_.path).parent_path();
    if (!parent.empty()) std::filesystem::create_directories(parent, ec);

    // Single-process exclusive fail-fast (enforcement of the §1 premise, counterpart
    // of RocksDB's LOCK file). PRAGMA locking_mode=EXCLUSIVE is not used — that is a
    // connection-level lock and would exclude our own connection pool
    std::string lock_path = opt_.path + ".lock";
    lock_fd_ = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (lock_fd_ < 0)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): cannot create " + lock_path);
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        ::close(lock_fd_);
        lock_fd_ = -1;
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): " + opt_.path +
                          " is locked by another process (single-process store)");
    }
    // After the lock is taken, any failure must go through close() — the destructor
    // does not run when the constructor throws, leaking the lock and connections
    try {
        wc_ = open_raw();
        check_lineage(*wc_);         // lineage check before any write/WAL conversion (§2.2)
        apply_pragmas(*wc_, opt_.sync);
        init_schema(*wc_);
        ac_ = open_conn(/*full_sync=*/true);  // id-segment connection is always FULL (§4)
    } catch (...) {
        shutdown(/*graceful=*/false);
        throw;
    }
}

SqliteMetaStore::~SqliteMetaStore() {
    try {
        close();
    } catch (const std::exception& e) {
        LOG_ERROR("duostore meta(sqlite): close in dtor failed: {}", e.what());
    }
}

void SqliteMetaStore::close() { shutdown(/*graceful=*/true); }

void SqliteMetaStore::shutdown(bool graceful) {
    std::scoped_lock lk(mu_, alloc_mu_, pool_mu_);
    closed_ = true;
    idle_.clear();
    ac_.reset();
    if (wc_ && !graceful) wc_.reset();  // constructor-failure cleanup: db never opened cleanly, skip graceful wrap-up
    if (wc_) {
        // Clean shutdown (§5.3): merge the WAL back into the main file and truncate,
        // leaving a single DB file in the directory — cold backup = copy that one
        // file. Use checkpoint_v2 rather than PRAGMA: when blocked by a reader the
        // PRAGMA returns OK via sqlite3_exec and the busy flag sits only in the
        // discarded result row — a silent failure; v2's return code + residual frame
        // count are detectable, and an incomplete truncation must be warned about
        // (the cold-backup contract would lose recent commits)
        try {
            wc_->exec("PRAGMA optimize", "optimize");
        } catch (const std::exception& e) {
            LOG_WARN("duostore meta(sqlite): optimize skipped: {}", e.what());
        }
        int n_log = 0, n_ckpt = 0;
        int rc = sqlite3_wal_checkpoint_v2(wc_->db, nullptr, SQLITE_CHECKPOINT_TRUNCATE,
                                           &n_log, &n_ckpt);
        if (rc != SQLITE_OK || n_log != 0)
            LOG_WARN("duostore meta(sqlite): final checkpoint incomplete "
                     "(rc={}, wal frames={}) — cold backup must include the -wal file",
                     rc, n_log);
        wc_.reset();
    }
    if (lock_fd_ >= 0) {
        ::close(lock_fd_);  // release the flock (§1 single-process exclusivity)
        lock_fd_ = -1;
    }
}

SqliteMetaStore::Conn& SqliteMetaStore::wconn() {
    // Throw InternalError after close — defense in depth: misuse becomes a 500
    // instead of a crash (the contract remains that close must be called after
    // in-flight requests finish)
    if (!wc_)
        throw S3Error(S3ErrorCode::InternalError, "duostore meta(sqlite): store is closed");
    // Defense: if COMMIT and the fallback ROLLBACK both fail (the Txn destructor
    // swallows the exception) an open transaction lingers — entering a new commit
    // with it hits "transaction within a transaction" and becomes permanent (the
    // write connection is unique and never rebuilt). Issue an extra ROLLBACK first;
    // if that also fails, throw a 500 now — never continue with a lingering txn
    if (!sqlite3_get_autocommit(wc_->db)) Stmt(*wc_, kRollback).exec();
    return *wc_;
}

SqliteMetaStore::Lease SqliteMetaStore::read_conn() {
    std::unique_ptr<Conn> c;
    {
        std::lock_guard lk(pool_mu_);
        if (closed_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta(sqlite): store is closed");
        if (!idle_.empty()) {
            c = std::move(idle_.back());
            idle_.pop_back();
        }
    }
    if (!c) {
        c = open_conn(/*full_sync=*/false);  // read-only connection, sync level irrelevant
        // TOCTOU defense: close() may have completed (checkpoint + truncate) while
        // the connection was being opened — discard the new connection (destruction
        // closes it; closing the last connection removes the recreated -wal/-shm)
        // and fail as already-closed
        std::lock_guard lk(pool_mu_);
        if (closed_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta(sqlite): store is closed");
    }
    return {this, std::move(c)};
}

void SqliteMetaStore::release(std::unique_ptr<Conn> c) {
    // A lingering open transaction (the extreme path where the Txn rollback also
    // failed) must not go back into the pool as-is: subsequent bare reads would
    // forever see that frozen snapshot (silent staleness) and transactional methods
    // would hit nested BEGIN. After S4 assessment, changed from "destroy outright"
    // to issuing a ROLLBACK first — on success autocommit is restored and the
    // connection can be reused safely (saving a rebuild); only if that still fails
    // is it destroyed (rebuild cost backstops correctness)
    if (c && !sqlite3_get_autocommit(c->db)) {
        try {
            Stmt(*c, kRollback).exec();
        } catch (const std::exception& e) {
            LOG_WARN("duostore meta(sqlite): rollback on release failed, dropping "
                     "connection: {}", e.what());
            c.reset();
        }
    }
    std::lock_guard lk(pool_mu_);
    if (!c || closed_ || int(idle_.size()) >= opt_.pool_size) return;  // destroy outright
    idle_.push_back(std::move(c));
}

// ---------- Id segments (§4) ----------

uint64_t SqliteMetaStore::alloc_id(std::string_view counter, IdRange& r, uint32_t n) {
    n = std::clamp<uint32_t>(n, 1, kMaxIdRun);  // run ≤ kMaxIdRun << kIdSegment
    std::lock_guard lk(alloc_mu_);  // lock order alloc_mu_ → mu_; no reverse nesting (alloc_mu_ only taken here)
    if (r.limit - r.next < n) {  // discard the remainder when switching segments (run batch dispatch requires contiguity within a segment, docs/archive/gaps.md §3.9)
        // The segment reservation must be persisted before dispensing — the dedicated
        // connection is always synchronous=FULL (independent of opt_.sync); otherwise
        // a crash losing the reservation would re-issue used file_ids after restart,
        // colliding via O_EXCL with chunk files already on disk. Wasting a segment on
        // crash is harmless (file_ids only need to be unique and monotonic, not
        // contiguous). Mutual exclusion with business write transactions on the
        // db-level write lock is made **deterministic** (docs/archive/gaps.md §3.9): mu_ is
        // held during reservation — the process's only writer is kept out, so we can
        // no longer end up in a busy_timeout lottery against our own business
        // transactions (the busy handler queues unfairly; under write hotspots,
        // losing 4 rounds in a row = 20 seconds turns a normal PUT into a 500).
        // Bounded retries are kept, but only for external writers that bypass the
        // single-process lock (flock cannot stop a bare sqlite3 tool; the BUSY row
        // of the §5.4 table)
        if (!ac_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta(sqlite): store is closed");
        std::lock_guard wl(mu_);
        uint64_t hi = 0;
        for (int attempt = 0;; ++attempt) {
            Stmt st(*ac_, kCtrReserve);
            st.i64(1, int64_t(kIdSegment)).blob(2, counter);
            auto row = st.step_busy();
            if (!row.has_value()) {
                if (attempt >= 3)
                    throw S3Error(S3ErrorCode::InternalError,
                                  "duostore meta(sqlite): id reservation starved "
                                  "(write lock busy — external writer?)");
                continue;
            }
            if (!*row)
                throw S3Error(S3ErrorCode::InternalError,
                              "duostore meta(sqlite): counter vanished");
            hi = uint64_t(st.col_i64(0));
            st.exec();  // drain RETURNING to DONE; the statement commits only when complete
            break;
        }
        r.limit = hi;
        r.next = hi - kIdSegment;
    }
    uint64_t first = r.next;
    r.next += n;
    return first;
}

uint64_t SqliteMetaStore::alloc_file_run(Extent::Kind kind, uint32_t n) {
    // kRados shares the segment with kChunk (same argument as the rocks version:
    // refs does not distinguish kind, preventing cross-kind id collisions)
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCtrChunk : kCtrPack,
                    file_ids_[size_t(kind)], n);
}

// ---------- Shared in-transaction pieces ----------

void SqliteMetaStore::require_bucket(Conn& c, std::string_view b) {
    Stmt st(c, kBucketGet);
    st.blob(1, b);
    if (!st.step())
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(b));
}

std::optional<std::string> SqliteMetaStore::object_raw(Conn& c, std::string_view b,
                                                       std::string_view k) {
    Stmt st(c, kObjGet);
    st.blob(1, b).blob(2, k);
    if (!st.step()) return std::nullopt;
    return std::string(st.col_blob(0));
}

void SqliteMetaStore::write_refs(Conn& c, const DataRef& ref, bool add,
                                 std::string_view owner) {
    for (const auto& e : ref.extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack liveness goes through pack_stats (P2);
                                                      // chunk/rados both enter refs by file_id
        if (add) {
            Stmt st(c, kRefPut);
            st.i64(1, int64_t(e.file_id)).blob(2, owner);
            st.exec();
        } else {
            Stmt st(c, kRefDel);
            st.i64(1, int64_t(e.file_id));
            st.exec();
        }
    }
}

void SqliteMetaStore::write_pack_delta(Conn& c, const DataRef& ref, int sign,
                                       int64_t rec_overhead) {
    // Aggregate multiple extents of the same pack first, then one arithmetic UPDATE
    // per pack (§9.1: increments/decrements batched with the business transaction);
    // each record counts payload + header overhead, same accounting basis as
    // file_size (docs/archive/gaps.md §2.3a)
    std::map<uint64_t, std::pair<int64_t, int64_t>> agg;  // pack_id -> (bytes, recs)
    for (const auto& e : ref.extents) {
        if (e.kind != Extent::Kind::kPack) continue;
        auto& [bytes, recs] = agg[e.file_id];
        bytes += sign * (int64_t(e.length) + rec_overhead);
        recs += sign;
    }
    for (const auto& [id, d] : agg) {
        Stmt st(c, kPackDelta);
        st.i64(1, int64_t(id)).i64(2, d.first).i64(3, d.second);
        st.exec();
    }
}

void SqliteMetaStore::enqueue_reclaim(Conn& c, const DataRef& ref, ReclaimReason reason) {
    if (ref.extents.empty()) return;
    // seq = AUTOINCREMENT rowid: allocated with the business transaction, committed/
    // rolled back in the same batch — a rolled-back transaction produces no
    // off-the-books seq, and restart neither rewinds nor re-issues (sqlite_sequence
    // shares the business transaction), sparing an id-segment counter. Oversized
    // DataRefs split into multiple entries (docs/archive/gaps.md §2.11): GC per-batch decode
    // memory stays bounded, and independent acks are harmless
    const int64_t ts = now_ms();
    for (size_t i = 0; i < ref.extents.size(); i += kReclaimMaxExtents) {
        size_t n = std::min(kReclaimMaxExtents, ref.extents.size() - i);
        Reclaim r;
        r.extents.assign(ref.extents.begin() + i, ref.extents.begin() + i + n);
        r.reason = reason;
        std::string v = codec::encode_reclaim(r, ts);
        Stmt st(c, kGcqPut);
        st.blob(1, v);
        st.exec();
    }
}

std::vector<PartRec> SqliteMetaStore::scan_parts(Conn& c, std::string_view b,
                                                 std::string_view k, std::string_view id) {
    std::vector<PartRec> out;
    Stmt st(c, kPartScan);
    st.blob(1, b).blob(2, k).blob(3, id);
    while (st.step())
        out.push_back(codec::decode_part(int(st.col_i64(0)), st.col_blob(1)));
    return out;  // the numeric part_no column is naturally ascending
}

UploadRec SqliteMetaStore::require_upload_in(Conn& c, std::string_view b, std::string_view k,
                                             std::string_view id) {
    auto missing = [&]() -> S3Error {
        return {S3ErrorCode::NoSuchUpload, "The specified multipart upload does not exist.",
                std::string(id)};
    };
    if (!is_valid_upload_id(id)) throw missing();
    Stmt st(c, kUpGet);
    st.blob(1, b).blob(2, k).blob(3, id);
    if (!st.step()) throw missing();
    return codec::decode_upload(std::string(k), std::string(id), st.col_blob(0));
}

// ---------- bucket ----------

void SqliteMetaStore::create_bucket(std::string_view b) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    {
        Stmt st(c, kBucketGet);
        st.blob(1, b);
        if (st.step())
            throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                          std::string(b));
    }
    Stmt st(c, kBucketPut);
    st.blob(1, b).blob(2, codec::encode_bucket(now_ms()));
    st.exec();
    t.commit();
}

void SqliteMetaStore::delete_bucket(std::string_view b) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_bucket(c, b);
    // The emptiness check covers both objects and uploads (an in-progress multipart
    // means BucketNotEmpty, matching AWS — same argument as the RocksDB version:
    // otherwise put_part could still write after bucket deletion and refs would
    // leak permanently)
    for (const char* sql : {kObjAny, kUpAny}) {
        Stmt st(c, sql);
        st.blob(1, b);
        if (st.step())
            throw S3Error(S3ErrorCode::BucketNotEmpty,
                          "The bucket you tried to delete is not empty", std::string(b));
    }
    Stmt st(c, kBucketDel);
    st.blob(1, b);
    st.exec();
    t.commit();
}

bool SqliteMetaStore::bucket_exists(std::string_view b) {
    auto lease = read_conn();
    Stmt st(*lease, kBucketGet);
    st.blob(1, b);
    return st.step();
}

std::vector<BucketInfo> SqliteMetaStore::list_buckets() {
    auto lease = read_conn();
    std::vector<BucketInfo> out;
    Stmt st(*lease, kBucketList);
    while (st.step())
        out.push_back({std::string(st.col_blob(0)),
                       codec::from_unix_ms(codec::decode_bucket(st.col_blob(1)))});
    return out;  // the primary-key B-tree provides name order for free
}

// ---------- object ----------

std::optional<ObjectRec> SqliteMetaStore::get_object(std::string_view b, std::string_view k) {
    auto lease = read_conn();
    auto v = object_raw(*lease, b, k);
    if (!v) return std::nullopt;
    return codec::decode_object(std::string(k), *v);
}

std::optional<ObjectMeta> SqliteMetaStore::head_object(std::string_view b, std::string_view k) {
    auto lease = read_conn();
    auto v = object_raw(*lease, b, k);
    if (!v) return std::nullopt;
    return codec::decode_object_meta(std::string(k), *v);
}

void SqliteMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec,
                                 PutCondition cond) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_bucket(c, b);
    std::optional<ObjectRec> old;
    if (auto v = object_raw(c, b, k)) old = codec::decode_object(std::string(k), *v);
    check_put_condition(cond, old, k);  // checked in-transaction; throwing rolls back (PutCondition contract)
    rec.version = old ? old->version + 1 : 1;

    std::string owner = std::string(b) + '/' + std::string(k);
    {
        Stmt st(c, kObjPut);
        st.blob(1, b).blob(2, k).blob(3, codec::encode_object(rec));
        st.exec();
    }
    write_refs(c, rec.data, /*add=*/true, owner);
    const int64_t ov = codec::pack_rec_overhead(b, k);
    write_pack_delta(c, rec.data, +1, ov);
    if (old) {
        enqueue_reclaim(c, old->data, ReclaimReason::kOverwrite);
        write_refs(c, old->data, /*add=*/false, {});
        write_pack_delta(c, old->data, -1, ov);
    }
    t.commit();
}

bool SqliteMetaStore::delete_object(std::string_view b, std::string_view k) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_bucket(c, b);
    auto v = object_raw(c, b, k);
    if (!v) return false;  // idempotent; Txn destructor rolls back (read-only up to here, nothing to undo)
    auto old = codec::decode_object(std::string(k), *v);
    {
        Stmt st(c, kObjDel);
        st.blob(1, b).blob(2, k);
        st.exec();
    }
    enqueue_reclaim(c, old.data, ReclaimReason::kDelete);
    write_refs(c, old.data, /*add=*/false, {});
    write_pack_delta(c, old.data, -1, codec::pack_rec_overhead(b, k));
    t.commit();
    return true;
}

// §2.3: primary-key range scan + delimiter group skipping, with the whole loop
// wrapped in one read transaction (WAL snapshot, consistent view). The algorithm
// corresponds line-by-line to the RocksDB version, with the iteration primitive
// swapped from Iterator to a range SELECT
ListResult SqliteMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    auto lease = read_conn();
    Conn& c = *lease;
    Txn t(c, /*immediate=*/false);
    require_bucket(c, b);
    ListResult out;
    // S3: max-keys=0 returns empty with IsTruncated=false
    if (opt.max_keys <= 0) {
        t.commit();
        return out;
    }
    const std::string& prefix = opt.prefix;
    const std::string& delim = opt.delimiter;
    // Seek start = max(prefix, successor of start_after): when the start is exactly
    // start_after we need strictly-greater — under BLOB memcmp order,
    // key > s ⇔ key >= s+'\0', so appending NUL gives the successor and a single
    // >= statement covers everything (corresponds to the RocksDB version's
    // "start_after hits itself, then one extra Next")
    std::string seek = std::max(prefix, opt.start_after);
    if (!opt.start_after.empty() && opt.start_after >= prefix) seek.push_back('\0');

    std::optional<Stmt> it;
    auto seek_to = [&](std::string_view from) {
        it.emplace(c, kObjScanGe);
        it->blob(1, b).blob(2, from);
    };
    seek_to(seek);

    std::string last_emitted;
    int count = 0;
    bool paused = false;
    // Consistent-view injection point (§9 S4, test-only): called once after the
    // first entry is emitted — the hook commits concurrently from the write
    // connection, and this read transaction's WAL snapshot must remain unmoved
    auto pause_hook = [&] {
        if (paused || !list_pause_for_test_) return;
        paused = true;
        list_pause_for_test_();
    };
    while (it->step()) {
        std::string uk(it->col_blob(0));
        if (uk.compare(0, prefix.size(), prefix) != 0) break;  // stop once past the prefix range
        if (count >= opt.max_keys) {
            out.is_truncated = true;
            out.next_token = last_emitted;
            break;
        }
        if (!delim.empty()) {
            auto pos = uk.find(delim, prefix.size());
            if (pos != std::string::npos) {
                std::string group = uk.substr(0, pos + delim.size());
                out.common_prefixes.push_back(group);
                ++count;
                pause_hook();
                // Group's last byte +1 = successor seek point, skipping the whole
                // group; token semantics must land on the group tail → one reverse
                // lookup for the last key inside the group (corresponds to RocksDB
                // SeekForPrev)
                std::string target = group;
                if (!bump_last_byte(target)) break;
                {
                    Stmt prev(c, kObjPrev);
                    prev.blob(1, b).blob(2, target);
                    if (prev.step()) last_emitted = std::string(prev.col_blob(0));
                }
                seek_to(target);
                continue;
            }
        }
        out.objects.push_back(codec::decode_object_meta(uk, it->col_blob(1)));
        last_emitted = std::move(uk);
        ++count;
        pause_hook();
    }
    it.reset();  // finish the statement before finishing the transaction
    t.commit();
    return out;
}

// ---------- multipart ----------

std::string SqliteMetaStore::create_upload(std::string_view b, std::string_view k,
                                           ObjectMeta meta) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_bucket(c, b);
    UploadRec rec;
    rec.upload_id = new_upload_id();
    rec.meta = std::move(meta);
    rec.meta.key = std::string(k);
    rec.initiated_ms = now_ms();
    Stmt st(c, kUpPut);
    st.blob(1, b).blob(2, k).blob(3, rec.upload_id).blob(4, codec::encode_upload(rec));
    st.exec();
    t.commit();
    return rec.upload_id;
}

UploadRec SqliteMetaStore::require_upload(std::string_view b, std::string_view k,
                                          std::string_view id) {
    auto lease = read_conn();
    return require_upload_in(*lease, b, k, id);
}

void SqliteMetaStore::put_part(std::string_view b, std::string_view k, std::string_view id,
                               PartRec p) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_upload_in(c, b, k, id);
    std::optional<PartRec> old;
    {
        Stmt st(c, kPartGet);
        st.blob(1, b).blob(2, k).blob(3, id).i64(4, p.part_no);
        if (st.step()) old = codec::decode_part(p.part_no, st.col_blob(0));
    }
    std::string owner = std::string(b) + '/' + std::string(k) + '/' + std::string(id) + '/' +
                        std::to_string(p.part_no);
    {
        Stmt st(c, kPartPut);
        st.blob(1, b).blob(2, k).blob(3, id).i64(4, p.part_no).blob(5, codec::encode_part(p));
        st.exec();
    }
    write_refs(c, p.data, /*add=*/true, owner);
    const int64_t ov = codec::pack_rec_overhead_part(b, k, id, p.part_no);
    write_pack_delta(c, p.data, +1, ov);
    if (old) {  // same-number re-upload is last-write-wins: old part enters GC accounting in the same batch
        enqueue_reclaim(c, old->data, ReclaimReason::kPartOverwrite);
        write_refs(c, old->data, /*add=*/false, {});
        write_pack_delta(c, old->data, -1, ov);
    }
    t.commit();
}

std::vector<PartRec> SqliteMetaStore::list_parts(std::string_view b, std::string_view k,
                                                 std::string_view id) {
    auto lease = read_conn();
    require_upload_in(*lease, b, k, id);
    return scan_parts(*lease, b, k, id);
}

std::vector<UploadInfo> SqliteMetaStore::list_uploads(std::string_view b,
                                                     std::string_view key_marker,
                                                     std::string_view id_marker, int limit) {
    auto lease = read_conn();
    require_bucket(*lease, b);
    std::vector<UploadInfo> out;
    Stmt st(*lease, kUpList);
    st.blob(1, b);
    st.blob(2, key_marker);
    st.blob(3, id_marker);
    st.i64(4, limit > 0 ? limit : -1);
    while (st.step()) {
        auto rec = codec::decode_upload(std::string(st.col_blob(0)),
                                        std::string(st.col_blob(1)), st.col_blob(2));
        out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
    }
    return out;  // primary-key order = (key, upload_id) order
}

// complete is a pure metadata transaction, zero data movement (main doc §8); the
// parts set is read inside the same transaction and is therefore naturally fresh —
// no Redis-version sha1 fingerprint, no RocksDB-version in-lock rescan concept (§3.4)
std::string SqliteMetaStore::complete_upload(std::string_view b, std::string_view k,
                                             std::string_view id,
                                             std::span<const PartInfo> parts) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    auto up = require_upload_in(c, b, k, id);
    require_bucket(c, b);

    std::map<int, PartRec> stored;
    for (auto& p : scan_parts(c, b, k, id)) stored.emplace(p.part_no, std::move(p));

    std::set<int> selected;
    ObjectRec rec = assemble_completed_object(std::move(up.meta), parts, stored, selected);

    std::optional<ObjectRec> old;
    if (auto v = object_raw(c, b, k)) old = codec::decode_object(std::string(k), *v);
    rec.version = old ? old->version + 1 : 1;

    std::string owner = std::string(b) + '/' + std::string(k);
    {
        Stmt st(c, kObjPut);
        st.blob(1, b).blob(2, k).blob(3, codec::encode_object(rec));
        st.exec();
    }
    {
        Stmt st(c, kUpDel);
        st.blob(1, b).blob(2, k).blob(3, id);
        st.exec();
    }
    {
        Stmt st(c, kPartDelAll);  // whole prefix in one statement (corresponds to RocksDB DeleteRange)
        st.blob(1, b).blob(2, k).blob(3, id);
        st.exec();
    }
    for (const auto& [no, p] : stored) {
        if (selected.count(no)) {
            // refs transfer: owner rewritten to the object. Pack liveness is
            // unchanged, but the accounting basis is rebalanced from part to object
            // (-part header overhead +object header overhead; recs cancel with one
            // decrement and one increment): guarantees a later object deletion,
            // deducting on the object basis, brings the account exactly to zero
            write_refs(c, p.data, /*add=*/true, owner);
            write_pack_delta(c, p.data, -1, codec::pack_rec_overhead_part(b, k, id, no));
            write_pack_delta(c, p.data, +1, codec::pack_rec_overhead(b, k));
        } else {  // unselected parts enter GC accounting
            enqueue_reclaim(c, p.data, ReclaimReason::kComplete);
            write_refs(c, p.data, /*add=*/false, {});
            write_pack_delta(c, p.data, -1, codec::pack_rec_overhead_part(b, k, id, no));
        }
    }
    if (old) {  // old same-name object enters GC accounting
        enqueue_reclaim(c, old->data, ReclaimReason::kOverwrite);
        write_refs(c, old->data, /*add=*/false, {});
        write_pack_delta(c, old->data, -1, codec::pack_rec_overhead(b, k));
    }
    t.commit();
    return rec.meta.etag;
}

void SqliteMetaStore::abort_upload(std::string_view b, std::string_view k,
                                   std::string_view id) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_upload_in(c, b, k, id);
    for (const auto& p : scan_parts(c, b, k, id)) {
        enqueue_reclaim(c, p.data, ReclaimReason::kAbort);
        write_refs(c, p.data, /*add=*/false, {});
        write_pack_delta(c, p.data, -1, codec::pack_rec_overhead_part(b, k, id, p.part_no));
    }
    {
        Stmt st(c, kUpDel);
        st.blob(1, b).blob(2, k).blob(3, id);
        st.exec();
    }
    Stmt st(c, kPartDelAll);
    st.blob(1, b).blob(2, k).blob(3, id);
    st.exec();
    t.commit();
}

// ---------- GC accounting ----------

std::vector<std::pair<uint64_t, Reclaim>> SqliteMetaStore::peek_reclaims(size_t max,
                                                                         uint64_t min_seq,
                                                                         size_t max_extents) {
    auto lease = read_conn();
    std::vector<std::pair<uint64_t, Reclaim>> out;
    Stmt st(*lease, kGcqPeek);
    st.i64(1, int64_t(std::min<size_t>(max, INT64_MAX)));
    st.i64(2, int64_t(std::min<uint64_t>(min_seq, INT64_MAX)));
    size_t extents = 0;
    while (st.step()) {
        out.emplace_back(uint64_t(st.col_i64(0)), codec::decode_reclaim(st.col_blob(1)));
        // Cumulative extent cap (gaps §2.11): at least 1 item is returned
        extents += out.back().second.extents.size();
        if (extents >= max_extents) break;
    }
    return out;
}

void SqliteMetaStore::ack_reclaim(uint64_t seq) {
    // Single-statement blind delete, but the write connection is unique — mu_ must
    // be held (otherwise the statement would slip into another thread's open
    // transaction; under SQLite's single-writer constraint, the RocksDB version's
    // property that "GC settlement does not queue behind business commits" cannot
    // be preserved). Each ack is an independent commit (including an fsync when
    // sync=true) — batch settlement goes through ack_reclaims
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Stmt st(c, kGcqDel);
    st.i64(1, int64_t(seq));
    st.exec();
}

void SqliteMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    // Overrides the interface's default per-item forwarding: one transaction, one
    // fsync — GC commits once per cycle instead of N times (§3.3)
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    for (uint64_t s : seqs) {
        Stmt st(c, kGcqDel);
        st.i64(1, int64_t(s));
        st.exec();
    }
    t.commit();
}

std::vector<PackStat> SqliteMetaStore::pack_stats() {
    // Returns every pack with an account (including live=0 and unsealed entries —
    // whole-pack deletion of empty packs and restart re-sealing depend on them)
    auto lease = read_conn();
    std::vector<PackStat> out;
    Stmt st(*lease, kPackList);
    while (st.step())
        out.push_back({uint64_t(st.col_i64(0)), uint64_t(st.col_i64(1)), st.col_i64(2),
                       st.col_i64(3), st.col_i64(4) != 0});
    return out;
}

void SqliteMetaStore::seal_pack(uint64_t pack_id, uint64_t file_size) {
    // Single-statement upsert (idempotent; 0 never overwrites a known size — the
    // CASE branch in kPackSeal); the write connection is unique, so mu_ must be
    // held (same argument as ack_reclaim)
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Stmt st(c, kPackSeal);
    st.i64(1, int64_t(pack_id)).i64(2, int64_t(file_size));
    st.exec();
}

void SqliteMetaStore::drop_pack_stat(uint64_t pack_id) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Stmt st(c, kPackDrop);
    st.i64(1, int64_t(pack_id));
    st.exec();
}

bool SqliteMetaStore::apply_swap(Conn& c, std::string_view b, std::string_view k,
                                 uint64_t expect_version, const DataRef& from,
                                 const DataRef& to) {
    auto v = object_raw(c, b, k);
    if (!v) return false;
    auto rec = codec::decode_object(std::string(k), *v);
    // Optimistic check: version or extent mismatch = overwritten/deleted in the meantime → abandon (main doc §9.2)
    if (rec.version != expect_version || rec.data.extents != from.extents) return false;
    rec.data = to;
    rec.version += 1;
    {
        Stmt st(c, kObjPut);
        st.blob(1, b).blob(2, k).blob(3, codec::encode_object(rec));
        st.exec();
    }
    std::string owner = std::string(b) + '/' + std::string(k);
    // refs operated on as a set difference (meta_util.h refs_delta): add-all then
    // remove-all would delete the refs entries of un-migrated chunks shared by
    // to/from → the orphan scan would wrongly delete live data
    auto rd = refs_delta(from, to);
    write_refs(c, rd.added, /*add=*/true, owner);
    write_refs(c, rd.removed, /*add=*/false, {});
    // Compaction ref swap: accounting migrates with the extents (§9.2); both sides
    // use the object basis (if the outgoing old record was in mpu form this slightly
    // under-deducts — the conservative direction)
    const int64_t ov = codec::pack_rec_overhead(b, k);
    write_pack_delta(c, to, +1, ov);
    write_pack_delta(c, from, -1, ov);
    return true;
}

bool SqliteMetaStore::swap_extents(std::string_view b, std::string_view k,
                                   uint64_t expect_version, const DataRef& from,
                                   const DataRef& to) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    if (!apply_swap(c, b, k, expect_version, from, to)) return false;  // Txn destructor rolls back
    t.commit();
    return true;
}

std::vector<bool> SqliteMetaStore::swap_extents_batch(std::span<const SwapReq> reqs) {
    // Batched compaction (gaps §2.13): the whole batch is one transaction with one
    // fsync — per-item swap would commit independently per item while contending
    // with business writes for the same db-level write lock. Per-item CAS stays
    // independent: a failed item writes nothing and does not affect the rest
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    std::vector<bool> out;
    out.reserve(reqs.size());
    Txn t(c);
    bool any = false;
    for (const auto& r : reqs) {
        bool ok = apply_swap(c, r.bucket, r.key, r.expect_version, r.from, r.to);
        any = any || ok;
        out.push_back(ok);
    }
    if (any) t.commit();  // all failed: Txn destructor rolls back (nothing written, pure no-op)
    return out;
}

bool SqliteMetaStore::chunk_referenced(uint64_t file_id) {
    auto lease = read_conn();
    Stmt st(*lease, kRefGet);
    st.i64(1, int64_t(file_id));
    return st.step();
}

void SqliteMetaStore::scan_refs(const std::function<void(uint64_t)>& cb) {
    // Iterate on a read connection (under WAL a single statement carries its own
    // consistent snapshot); the orphan scan tolerates a weakly consistent view
    auto lease = read_conn();
    Stmt st(*lease, kRefScan);
    while (st.step()) cb(uint64_t(st.col_i64(0)));
}

}  // namespace lights3::storage::duostore
