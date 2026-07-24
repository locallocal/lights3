#include "storage/duostore/sqlite_meta_store.h"

#include <fcntl.h>
#include <sqlite3.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
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

// 文件谱系（§2.2）：application_id 标记"这是 duostore 的 sqlite meta 文件"，
// user_version 是 schema 版本——SQLite 自带机制，零表实现
constexpr int64_t kAppId = 0x4C335351;  // "L3SQ"
constexpr int64_t kSchemaVersion = 1;

// counters 表只存 file_id 号段（'chunk' / 'pack'）；gcq seq 走 AUTOINCREMENT（§2.2）
constexpr const char* kCtrChunk = "chunk";
constexpr const char* kCtrPack = "pack";

// 建表 DDL（§2.2）：key 列一律 BLOB（memcmp 序 = S3 字典序）、STRICT 强制列型、
// 主键即聚簇索引（WITHOUT ROWID）。counters 种子不进 DDL——用绑定语句从
// kCtrChunk/kCtrPack 常量落 seed（init_schema），单一事实源，免手工 hex
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

// SQL 常量（§5.3）：每连接以字面量地址为键常驻缓存 prepared statement；
// 参数一律 ?N 绑定，禁止拼接（BLOB 截断源 + 注入面）
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
constexpr const char* kUpList =
    "SELECT key,id,val FROM uploads WHERE bucket=?1 ORDER BY key,id";
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
constexpr const char* kGcqPut = "INSERT INTO gcq(val) VALUES(?1)";
constexpr const char* kGcqPeek = "SELECT seq,val FROM gcq ORDER BY seq LIMIT ?1";
constexpr const char* kGcqDel = "DELETE FROM gcq WHERE seq=?1";
constexpr const char* kCtrReserve =
    "UPDATE counters SET val=val+?1 WHERE name=?2 RETURNING val";
constexpr const char* kCtrSeed = "INSERT OR IGNORE INTO counters(name,val) VALUES(?1,0)";
constexpr const char* kAnyTable = "SELECT 1 FROM sqlite_master LIMIT 1";

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

using codec::bump_last_byte;  // delimiter 跳组后继（codec.h，meta store 实现共用）

}  // namespace

// ---------- 连接 / 语句 / 事务基元 ----------

struct SqliteMetaStore::Conn {
    sqlite3* db = nullptr;
    std::map<const char*, sqlite3_stmt*> stmts;  // key = SQL 字面量地址（§5.3）

    ~Conn() {
        for (auto& [sql, st] : stmts) sqlite3_finalize(st);
        if (db && sqlite3_close(db) != SQLITE_OK)
            LOG_ERROR("duostore meta(sqlite): close connection: {}", sqlite3_errmsg(db));
    }

    [[noreturn]] void raise(const char* what) const {
        std::string msg = db ? sqlite3_errmsg(db) : "no connection";
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

    // 固定语句直通（PRAGMA / BEGIN / DDL 等）；允许多语句脚本
    void exec(const std::string& sql, const char* what) {
        char* err = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : sqlite3_errmsg(db);
            sqlite3_free(err);
            LOG_ERROR("duostore meta(sqlite): {}: {}", what, msg);
            throw S3Error(S3ErrorCode::InternalError,
                          std::string("duostore meta(sqlite): ") + what + ": " + msg);
        }
    }
};

// prepared statement 的单次使用 RAII：bind → step*，析构 reset + clear_bindings。
// 绑定一律 SQLITE_TRANSIENT（sqlite 自拷贝）——省去与 reseek/异常路径纠缠的
// 生命周期约定，meta 值体量小，拷贝成本可忽略
class SqliteMetaStore::Stmt {
public:
    Stmt(Conn& c, const char* sql) : c_(c), s_(c.get(sql)) {}
    ~Stmt() {
        sqlite3_reset(s_);
        sqlite3_clear_bindings(s_);
    }
    Stmt(const Stmt&) = delete;

    Stmt& blob(int i, std::string_view v) {
        // 空串必须给非空指针：bind_blob(nullptr) 语义是 SQL NULL，不是零长 BLOB
        if (sqlite3_bind_blob(s_, i, v.empty() ? "" : v.data(), int(v.size()),
                              SQLITE_TRANSIENT) != SQLITE_OK)
            c_.raise("bind blob");
        return *this;
    }
    Stmt& i64(int i, int64_t v) {
        if (sqlite3_bind_int64(s_, i, v) != SQLITE_OK) c_.raise("bind int");
        return *this;
    }

    bool step() {  // true = 有行，false = 结束
        int rc = sqlite3_step(s_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        c_.raise("step");
    }
    // BUSY 容忍变体（仅号段路径用，§4）：nullopt = SQLITE_BUSY——单语句自动提交
    // 事务在取写锁阶段失败，明确未执行，重试安全；其余语义同 step()
    std::optional<bool> step_busy() {
        int rc = sqlite3_step(s_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        if (rc == SQLITE_BUSY) return std::nullopt;
        c_.raise("step");
    }
    void exec() {  // 跑到底（DML / RETURNING 排空）
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

// 事务 RAII（§3.2）：写事务 BEGIN IMMEDIATE（进程内已被 mu_ 序列化，永不 BUSY）、
// 读事务 BEGIN（WAL snapshot，一致视图）；析构未 commit 即 ROLLBACK——语义错误
// 抛 S3Error 穿出方法时事务自动回滚，杜绝半程状态残留
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

// ---------- 打开 / schema / 关闭 ----------

std::unique_ptr<SqliteMetaStore::Conn> SqliteMetaStore::open_raw() {
    auto c = std::make_unique<Conn>();
    if (sqlite3_open_v2(opt_.path.c_str(), &c->db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        std::string msg = c->db ? sqlite3_errmsg(c->db) : "out of memory";
        LOG_ERROR("duostore meta(sqlite): open {}: {}", opt_.path, msg);
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): open " + opt_.path + ": " + msg);
    }
    sqlite3_busy_timeout(c->db, 5000);  // 防御纵深：进程内本不该长 BUSY（§5.2）
    return c;
}

void SqliteMetaStore::apply_pragmas(Conn& c, bool full_sync) {
    // 页缓存按连接生效——把 cache_bytes 预算摊到全部连接（1 写 + 1 alloc +
    // pool_size 读），使 sqlite_cache 语义 = 进程级总预算（对齐 rocksdb_block_cache
    // 的角色，§8）；地板 256KiB 兜底任何整除边角（负值单位 KiB）。
    // journal_mode 是库的持久属性，首连接写入文件头
    size_t per_conn_kib =
        std::max<size_t>(opt_.cache_bytes / size_t(opt_.pool_size + 2) / 1024, 256);
    c.exec("PRAGMA journal_mode=WAL;"
           "PRAGMA synchronous=" + std::string(full_sync ? "FULL" : "NORMAL") + ";" +
           "PRAGMA cache_size=-" + std::to_string(per_conn_kib) + ";" +
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
        // 无谱系标记：野生 SQLite 库的常态恰是 app_id=0/ver=0——有表即别人的库，
        // 拒绝（此刻尚未做任何写入或 WAL 转换，文件不留痕）；真空库才允许建表
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
    if (ver != kSchemaVersion)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta(sqlite): unsupported schema version " +
                          std::to_string(ver));
}

void SqliteMetaStore::init_schema(Conn& c) {
    int64_t ver = 0;
    {
        Stmt st(c, "PRAGMA user_version");
        if (st.step()) ver = st.col_i64(0);
    }
    if (ver != 0) return;  // 已是本店库（check_lineage 校验过版本）
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
    // 父目录由本店自建（文件归属本店，覆盖所有调用方；失败留给 open 报错）
    std::error_code ec;
    auto parent = std::filesystem::path(opt_.path).parent_path();
    if (!parent.empty()) std::filesystem::create_directories(parent, ec);

    // 单进程独占 fail-fast（§1 前提的 enforcement，对应 RocksDB 的 LOCK 文件）。
    // 不用 PRAGMA locking_mode=EXCLUSIVE——那是连接级锁，会与自身连接池互斥
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
    // 取锁成功后任何失败都必须走 close()——构造函数抛出时析构不运行，锁与连接泄漏
    try {
        wc_ = open_raw();
        check_lineage(*wc_);         // 谱系校验先于任何写入/WAL 转换（§2.2）
        apply_pragmas(*wc_, opt_.sync);
        init_schema(*wc_);
        ac_ = open_conn(/*full_sync=*/true);  // 号段连接恒 FULL（§4）
    } catch (...) {
        close();
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

void SqliteMetaStore::close() {
    std::scoped_lock lk(mu_, alloc_mu_, pool_mu_);
    closed_ = true;
    idle_.clear();
    ac_.reset();
    if (wc_) {
        // 干净关闭（§5.3）：WAL 合并回主文件并截断，目录里只剩单个 DB 文件——
        // 冷备 = 拷这一个文件。走 checkpoint_v2 而非 PRAGMA：被读者阻塞时 PRAGMA
        // 经 sqlite3_exec 返回 OK、busy 标志只在被丢弃的结果行里——静默失败；
        // v2 的返回码 + 残留帧数可检测，未截干净必须告警（冷备契约会丢最近提交）
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
        ::close(lock_fd_);  // 释放 flock（§1 单进程独占）
        lock_fd_ = -1;
    }
}

SqliteMetaStore::Conn& SqliteMetaStore::wconn() {
    // close 后抛 InternalError——防御纵深，误用变 500 而非崩溃（契约仍是
    // close 须在在途请求完成后调用）
    if (!wc_)
        throw S3Error(S3ErrorCode::InternalError, "duostore meta(sqlite): store is closed");
    // 防御：COMMIT 与兜底 ROLLBACK 相继失败（Txn 析构吞异常）会残留开放事务——
    // 带着它进新提交撞 "transaction within a transaction" 且永久化（写连接唯一、
    // 不重建）。先补一次 ROLLBACK，仍失败则本次抛 500，绝不带残留事务继续
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
        c = open_conn(/*full_sync=*/false);  // 纯读连接，sync 档位无关
        // TOCTOU 防御：建连期间 close() 可能已完成（checkpoint + 截断）——丢弃
        // 新连接（析构即关，末连接关闭会清掉重建的 -wal/-shm）并按已关闭失败
        std::lock_guard lk(pool_mu_);
        if (closed_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta(sqlite): store is closed");
    }
    return {this, std::move(c)};
}

void SqliteMetaStore::release(std::unique_ptr<Conn> c) {
    // 开放事务残留（Txn 回滚也失败的极端路径）不得回池：后续裸读会永远读该
    // 冻结 snapshot（静默陈旧）、事务方法撞嵌套 BEGIN——直接销毁
    if (c && !sqlite3_get_autocommit(c->db)) c.reset();
    std::lock_guard lk(pool_mu_);
    if (!c || closed_ || int(idle_.size()) >= opt_.pool_size) return;  // 直接销毁
    idle_.push_back(std::move(c));
}

// ---------- 号段（§4）----------

uint64_t SqliteMetaStore::alloc_id(std::string_view counter, IdRange& r) {
    std::lock_guard lk(alloc_mu_);  // 与 mu_ 无嵌套：常见路径是纯内存 next++
    if (r.next == r.limit) {
        // 号段预留必须先于派发持久化——专用连接恒 synchronous=FULL（独立于
        // opt_.sync），否则崩溃丢预留后重启重发已用 file_id，与已落盘的 chunk
        // 文件 O_EXCL 冲突。崩溃浪费号段无害（file_id 只需唯一单调，不需连续）。
        // 与并发业务事务撞写锁先由 busy_timeout(5s) 吸收；写热点下仍可能连续输掉
        // 锁竞争（busy handler 不公平排队）——BUSY 即语句明确未执行，有界重试而非
        // 立刻 500（每次重试自带一轮 5s 等待）
        if (!ac_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta(sqlite): store is closed");
        uint64_t hi = 0;
        for (int attempt = 0;; ++attempt) {
            Stmt st(*ac_, kCtrReserve);
            st.i64(1, int64_t(kIdSegment)).blob(2, counter);
            auto row = st.step_busy();
            if (!row.has_value()) {
                if (attempt >= 3)
                    throw S3Error(S3ErrorCode::InternalError,
                                  "duostore meta(sqlite): id reservation starved "
                                  "(write lock busy)");
                continue;
            }
            if (!*row)
                throw S3Error(S3ErrorCode::InternalError,
                              "duostore meta(sqlite): counter vanished");
            hi = uint64_t(st.col_i64(0));
            st.exec();  // RETURNING 排空到 DONE，语句完成才提交
            break;
        }
        r.limit = hi;
        r.next = hi - kIdSegment;
    }
    return r.next++;
}

uint64_t SqliteMetaStore::alloc_file_id(Extent::Kind kind) {
    // kRados 与 kChunk 共号段（同 rocks 版论证：refs 不分 kind，防跨 kind id 碰撞）
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCtrChunk : kCtrPack,
                    file_ids_[size_t(kind)]);
}

// ---------- 事务内共用件 ----------

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
        if (e.kind == Extent::Kind::kPack) continue;  // pack 存活走 pack_stats 账（P2）；
                                                      // chunk/rados 皆按 file_id 入 refs
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

void SqliteMetaStore::enqueue_reclaim(Conn& c, const DataRef& ref) {
    if (ref.extents.empty()) return;
    // seq = AUTOINCREMENT rowid：随业务事务分配、同批提交/回滚——事务回滚不产生
    // 账外 seq，重启不回退不重发（sqlite_sequence 与业务同事务），免号段计数器
    std::string v = codec::encode_reclaim(Reclaim{ref.extents}, now_ms());
    Stmt st(c, kGcqPut);
    st.blob(1, v);
    st.exec();
}

std::vector<PartRec> SqliteMetaStore::scan_parts(Conn& c, std::string_view b,
                                                 std::string_view k, std::string_view id) {
    std::vector<PartRec> out;
    Stmt st(c, kPartScan);
    st.blob(1, b).blob(2, k).blob(3, id);
    while (st.step())
        out.push_back(codec::decode_part(int(st.col_i64(0)), st.col_blob(1)));
    return out;  // part_no 数值列天然升序
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
    // 空检查覆盖 objects 与 uploads（有进行中 multipart 即 BucketNotEmpty，对齐
    // AWS——与 RocksDB 版同一论证：否则桶删后 put_part 仍可写、refs 永久泄漏）
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
    return out;  // 主键 B-tree 免费提供 name 序
}

// ---------- object ----------

std::optional<ObjectRec> SqliteMetaStore::get_object(std::string_view b, std::string_view k) {
    auto lease = read_conn();
    auto v = object_raw(*lease, b, k);
    if (!v) return std::nullopt;
    return codec::decode_object(std::string(k), *v);
}

void SqliteMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_bucket(c, b);
    std::optional<ObjectRec> old;
    if (auto v = object_raw(c, b, k)) old = codec::decode_object(std::string(k), *v);
    rec.version = old ? old->version + 1 : 1;

    std::string owner = std::string(b) + '/' + std::string(k);
    {
        Stmt st(c, kObjPut);
        st.blob(1, b).blob(2, k).blob(3, codec::encode_object(rec));
        st.exec();
    }
    write_refs(c, rec.data, /*add=*/true, owner);
    if (old) {
        enqueue_reclaim(c, old->data);
        write_refs(c, old->data, /*add=*/false, {});
    }
    t.commit();
}

bool SqliteMetaStore::delete_object(std::string_view b, std::string_view k) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    require_bucket(c, b);
    auto v = object_raw(c, b, k);
    if (!v) return false;  // 幂等；Txn 析构回滚（只读到这一步，无写可回）
    auto old = codec::decode_object(std::string(k), *v);
    {
        Stmt st(c, kObjDel);
        st.blob(1, b).blob(2, k);
        st.exec();
    }
    enqueue_reclaim(c, old.data);
    write_refs(c, old.data, /*add=*/false, {});
    t.commit();
    return true;
}

// §2.3：主键范围扫 + delimiter 跳组，整个循环包在一个读事务里（WAL snapshot，
// 一致视图）。算法与 RocksDB 版逐行对应，迭代原语从 Iterator 换成范围 SELECT
ListResult SqliteMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    auto lease = read_conn();
    Conn& c = *lease;
    Txn t(c, /*immediate=*/false);
    require_bucket(c, b);
    ListResult out;
    // S3：max-keys=0 返回空且 IsTruncated=false
    if (opt.max_keys <= 0) {
        t.commit();
        return out;
    }
    const std::string& prefix = opt.prefix;
    const std::string& delim = opt.delimiter;
    // seek 起点 = max(prefix, start_after 的后继)：起点恰为 start_after 时要严格
    // 大于——BLOB memcmp 序下 key > s ⇔ key >= s+'\0'，追加 NUL 即后继，单条
    // >= 语句通吃（对应 RocksDB 版"start_after 命中自身再 Next 一步"）
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
    while (it->step()) {
        std::string uk(it->col_blob(0));
        if (uk.compare(0, prefix.size(), prefix) != 0) break;  // 出前缀区间即止
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
                // 组末字节 +1 = 后继 seek 点，跳过整组；token 语义须落组尾 →
                // 反向单查组内最后一条 key（对应 RocksDB SeekForPrev）
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
    }
    it.reset();  // 先收语句再收事务
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
    if (old) {  // 同号重传 last-write-wins：旧分片同批入 GC 账
        enqueue_reclaim(c, old->data);
        write_refs(c, old->data, /*add=*/false, {});
    }
    t.commit();
}

std::vector<PartRec> SqliteMetaStore::list_parts(std::string_view b, std::string_view k,
                                                 std::string_view id) {
    auto lease = read_conn();
    require_upload_in(*lease, b, k, id);
    return scan_parts(*lease, b, k, id);
}

std::vector<UploadInfo> SqliteMetaStore::list_uploads(std::string_view b) {
    auto lease = read_conn();
    require_bucket(*lease, b);
    std::vector<UploadInfo> out;
    Stmt st(*lease, kUpList);
    st.blob(1, b);
    while (st.step()) {
        auto rec = codec::decode_upload(std::string(st.col_blob(0)),
                                        std::string(st.col_blob(1)), st.col_blob(2));
        out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
    }
    return out;  // 主键序 = (key, upload_id) 序
}

// complete 是纯元数据事务，零数据搬运（主文档 §8）；parts 集合在同一事务内读取，
// 天然最新——无 Redis 版的 sha1 指纹、无 RocksDB 版的锁内重扫概念（§3.4）
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
        Stmt st(c, kPartDelAll);  // 整前缀一条语句（对应 RocksDB DeleteRange）
        st.blob(1, b).blob(2, k).blob(3, id);
        st.exec();
    }
    for (const auto& [no, p] : stored) {
        if (selected.count(no)) {
            write_refs(c, p.data, /*add=*/true, owner);  // refs 转移：owner 改写为对象
        } else {  // 未选中分片入 GC 账
            enqueue_reclaim(c, p.data);
            write_refs(c, p.data, /*add=*/false, {});
        }
    }
    if (old) {  // 旧同名对象入 GC 账
        enqueue_reclaim(c, old->data);
        write_refs(c, old->data, /*add=*/false, {});
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
        enqueue_reclaim(c, p.data);
        write_refs(c, p.data, /*add=*/false, {});
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

// ---------- GC 记账 ----------

std::vector<std::pair<uint64_t, Reclaim>> SqliteMetaStore::peek_reclaims(size_t max) {
    auto lease = read_conn();
    std::vector<std::pair<uint64_t, Reclaim>> out;
    Stmt st(*lease, kGcqPeek);
    st.i64(1, int64_t(std::min<size_t>(max, INT64_MAX)));
    while (st.step())
        out.emplace_back(uint64_t(st.col_i64(0)), codec::decode_reclaim(st.col_blob(1)));
    return out;
}

void SqliteMetaStore::ack_reclaim(uint64_t seq) {
    // 单语句盲删，但写连接唯一——必须持 mu_（否则语句会插进另一线程的开放事务；
    // SQLite 单写者约束下"GC 销账不排队业务提交"的 RocksDB 版性质不可保留）。
    // 每条 ack 是一次独立提交（sync=true 时含 fsync）——批量销账走 ack_reclaims
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Stmt st(c, kGcqDel);
    st.i64(1, int64_t(seq));
    st.exec();
}

void SqliteMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    // 覆写接口默认的逐条转发：单事务单 fsync，GC 每周期一次提交而非 N 次（§3.3）
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
    return {};  // pack 存活账随 P2 pack 聚合引入（pack_stats 表已建）
}

bool SqliteMetaStore::swap_extents(std::string_view b, std::string_view k,
                                   uint64_t expect_version, const DataRef& from,
                                   const DataRef& to) {
    std::lock_guard lk(mu_);
    Conn& c = wconn();
    Txn t(c);
    auto v = object_raw(c, b, k);
    if (!v) return false;
    auto rec = codec::decode_object(std::string(k), *v);
    // 乐观校验：version 或 extent 不符 = 期间被覆盖/删除 → 放弃（主文档 §9.2）
    if (rec.version != expect_version || rec.data.extents != from.extents) return false;
    rec.data = to;
    rec.version += 1;
    {
        Stmt st(c, kObjPut);
        st.blob(1, b).blob(2, k).blob(3, codec::encode_object(rec));
        st.exec();
    }
    std::string owner = std::string(b) + '/' + std::string(k);
    write_refs(c, to, /*add=*/true, owner);
    write_refs(c, from, /*add=*/false, {});
    t.commit();
    return true;
}

bool SqliteMetaStore::chunk_referenced(uint64_t file_id) {
    auto lease = read_conn();
    Stmt st(*lease, kRefGet);
    st.i64(1, int64_t(file_id));
    return st.step();
}

}  // namespace lights3::storage::duostore
