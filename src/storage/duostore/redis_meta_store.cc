#include "storage/duostore/redis_meta_store.h"

#include <hiredis.h>
#include <openssl/evp.h>
#include <sys/time.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <set>
#include <stdexcept>
#include <thread>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/meta_util.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// Counter key suffixes (§2.2): file_id segments and gcq seq
constexpr const char* kCounterChunk = "ctr:chunk";
constexpr const char* kCounterPack = "ctr:pack";
constexpr const char* kCounterSeq = "ctr:seq";
// Distinguishes lineage from RocksDB's "1" (§2.2); see meta_util.h parse_schema_marker for the version evolution policy
constexpr int64_t kSchemaCurrent = 1;
constexpr const char* kSchemaValue = "r1";  // = "r" + kSchemaCurrent (written on first creation)
// CAS retry cap (§3.2): throw InternalError past the cap — under pathological hotspot contention,
// failing loudly beats a livelock. A tight loop without backoff would be starved by a peer's
// continuous commit stream (hot key across multiple gateways), so back off exponentially before retrying
constexpr int kMaxCasRetries = 16;

[[noreturn]] void throw_internal(const char* what, const std::string& detail) {
    LOG_ERROR("duostore redis meta: {}: {}", what, detail);
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore redis meta: ") + what + ": " + detail);
}

// Commit-class IO failure = the transaction may already have taken effect (§3.5): throw a distinguishable type so callers know not to delete data
[[noreturn]] void throw_undetermined(const char* what, const std::string& detail) {
    LOG_ERROR("duostore redis meta: {}: commit result undetermined: {}", what, detail);
    throw UndeterminedCommit(std::string("duostore redis meta: ") + what +
                             ": commit result undetermined: " + detail);
}

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

std::string sha1_hex(std::string_view data) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int n = 0;
    if (!EVP_Digest(data.data(), data.size(), md, &n, EVP_sha1(), nullptr))
        throw_internal("sha1", "EVP_Digest failed");
    static const char* hex = "0123456789abcdef";
    std::string out(n * 2, '\0');
    for (unsigned i = 0; i < n; ++i) {
        out[2 * i] = hex[md[i] >> 4];
        out[2 * i + 1] = hex[md[i] & 0xf];
    }
    return out;
}

// ---- Guarded-commit script (§3.2): checks and ops are both fixed-size 4-tuples; if any check
// fails, return 0 immediately (write nothing). Values are opaque to the script — byte-level comparison only (§3.1).
constexpr const char* kCommitScript = R"lua(
local i = 1
local nc = tonumber(ARGV[i]); i = i + 1
for _ = 1, nc do
  local t, kx, f, exp = ARGV[i], ARGV[i+1], ARGV[i+2], ARGV[i+3]
  i = i + 4
  local key = KEYS[tonumber(kx)]
  if t == 'eq' then
    local v = redis.call('HGET', key, f)
    if not v or redis.sha1hex(v) ~= exp then return 0 end
  elseif t == 'absent' then
    if redis.call('HGET', key, f) then return 0 end
  elseif t == 'exists' then
    if redis.call('HEXISTS', key, f) == 0 then return 0 end
  elseif t == 'hlen0' then
    if redis.call('HLEN', key) ~= 0 then return 0 end
  elseif t == 'zcard0' then
    if redis.call('ZCARD', key) ~= 0 then return 0 end
  else
    -- Fetch the whole hash with a single HGETALL (docs/gaps.md §3.9): previously HKEYS +
    -- one HGET per field, so completing a 10k-part upload meant 10k redis.calls while the
    -- script's atomic execution monopolized the entire Redis instance.
    -- Sorting/concatenation still happens in Lua (matches the numeric part_no ordering of
    -- scan_parts on the C++ side)
    local flat = redis.call('HGETALL', key)
    local fields, byf = {}, {}
    for j = 1, #flat, 2 do
      fields[#fields + 1] = flat[j]
      byf[flat[j]] = flat[j + 1]
    end
    table.sort(fields, function(a, b) return tonumber(a) < tonumber(b) end)
    local buf = {}
    for j = 1, #fields do buf[j] = byf[fields[j]] end
    if redis.sha1hex(table.concat(buf)) ~= exp then return 0 end
  end
end
local no = tonumber(ARGV[i]); i = i + 1
for _ = 1, no do
  local k, kx, a, b = ARGV[i], ARGV[i+1], ARGV[i+2], ARGV[i+3]
  i = i + 4
  local key = KEYS[tonumber(kx)]
  if k == 'hset' then redis.call('HSET', key, a, b)
  elseif k == 'hdel' then redis.call('HDEL', key, a)
  elseif k == 'hincr' then redis.call('HINCRBY', key, a, b)
  elseif k == 'zadd' then redis.call('ZADD', key, a, b)
  elseif k == 'zrem' then redis.call('ZREM', key, a)
  elseif k == 'del' then redis.call('DEL', key)
  elseif k == 'set' then redis.call('SET', key, a)
  end
end
return 1
)lua";

// ---- list_objects script (§2.3): algorithm copied from rocks_meta_store.cc §4.4 — seek start
// is max(prefix, start_after), skip a delimiter group by bumping its last byte +1, token lands
// at the group tail. The whole loop runs inside one script = 1 RTT + consistent view.
// KEYS[1]=oz(ZSET) KEYS[2]=o(HASH); ARGV = prefix, start_after, delimiter, max_keys.
// Returns { truncated, next_token, {k1,v1,...}, {group,...} }
constexpr const char* kListScript = R"lua(
local oz, oh = KEYS[1], KEYS[2]
local prefix, start_after, delim = ARGV[1], ARGV[2], ARGV[3]
local maxkeys = tonumber(ARGV[4])

local function bump(s)
  for i = #s, 1, -1 do
    local b = string.byte(s, i)
    if b ~= 255 then return string.sub(s, 1, i - 1) .. string.char(b + 1) end
  end
  return nil
end

local keys, groups = {}, {}
local truncated, next_token, last_emitted = 0, '', ''
local count = 0

local min
if start_after ~= '' and start_after >= prefix then min = '(' .. start_after
else min = '[' .. prefix end

-- Paged scan (docs/gaps.md §3.9): previously one ZRANGEBYLEX + one HGET per key, so
-- listing 1000 keys meant 2000 redis.calls while the script's atomic execution
-- monopolized the entire Redis instance. Now keys are fetched a page at a time
-- (dropping the page and re-seeking on group skips) and values are batch-HMGET'd at
-- the end -- everything still runs in one atomic execution, so the consistent view
-- is unchanged
local PAGE = 200
local page, pi = {}, 1
while true do
  if pi > #page then
    page = redis.call('ZRANGEBYLEX', oz, min, '+', 'LIMIT', 0, PAGE)
    pi = 1
    if #page == 0 then break end
  end
  local uk = page[pi]; pi = pi + 1
  if string.sub(uk, 1, #prefix) ~= prefix then break end
  if count >= maxkeys then
    truncated = 1
    next_token = last_emitted
    break
  end
  local grouped = false
  if delim ~= '' then
    local pos = string.find(uk, delim, #prefix + 1, true)
    if pos then
      groups[#groups + 1] = string.sub(uk, 1, pos + #delim - 1)
      count = count + 1
      local target = bump(string.sub(uk, 1, pos + #delim - 1))
      if not target then break end
      local tail = redis.call('ZREVRANGEBYLEX', oz, '(' .. target, '-', 'LIMIT', 0, 1)
      if tail[1] then last_emitted = tail[1] end
      min = '[' .. target
      page, pi = {}, 1
      grouped = true
    end
  end
  if not grouped then
    keys[#keys + 1] = uk
    last_emitted = uk
    count = count + 1
    min = '(' .. uk
  end
end

local objs = {}
local i = 1
while i <= #keys do
  local j = math.min(i + 499, #keys)  -- unpack is bounded by the Lua C stack limit; batch
  local vals = redis.call('HMGET', oh, unpack(keys, i, j))
  for x = 1, j - i + 1 do
    if vals[x] then
      objs[#objs + 1] = keys[i + x - 1]
      objs[#objs + 1] = vals[x]
    end
  end
  i = j + 1
end
return { truncated, next_token, objs, groups }
)lua";

}  // namespace

// ---------- Connection and reply ----------

struct RedisMetaStore::Conn {
    redisContext* ctx = nullptr;
    ~Conn() {
        if (ctx) redisFree(ctx);
    }
};

void RedisReplyDeleter::operator()(redisReply* r) const { freeReplyObject(r); }

namespace {

// Run one command on a single connection; a connection-level failure (IO/timeout/protocol) returns null and reports errstr
RedisReplyPtr run_on(redisContext* ctx, const std::vector<std::string>& args,
                     std::string* err) {
    std::vector<const char*> argv(args.size());
    std::vector<size_t> lens(args.size());
    for (size_t i = 0; i < args.size(); ++i) {
        argv[i] = args[i].data();
        lens[i] = args[i].size();
    }
    auto* r = static_cast<redisReply*>(
        redisCommandArgv(ctx, int(args.size()), argv.data(), lens.data()));
    if (!r || ctx->err) {
        *err = ctx->err ? ctx->errstr : "null reply";
        if (r) freeReplyObject(r);
        return nullptr;
    }
    return RedisReplyPtr(r);
}

// REDIS_REPLY_ERROR → InternalError (carrying the server error text, §5.3)
void check_reply_error(const char* what, const redisReply* r) {
    if (r->type == REDIS_REPLY_ERROR)
        throw_internal(what, std::string(r->str, r->len));
}

long long require_int(const char* what, const redisReply* r) {
    check_reply_error(what, r);
    if (r->type != REDIS_REPLY_INTEGER) throw_internal(what, "unexpected reply type");
    return r->integer;
}

std::string_view reply_str(const redisReply* r) { return {r->str, r->len}; }

}  // namespace

// ---------- RedisBatch: guarded-commit assembler (§3.2, mirrors WriteBatch's append interface) ----------

class RedisBatch {
public:
    explicit RedisBatch(RedisMetaStore& store) : store_(store) {}

    // The CAS witness ships a SHA1 fingerprint instead of the whole old value (docs/gaps.md
    // §3.9): for large manifests the witness shrinks from MB-scale to 40 bytes and retry rounds
    // no longer resend the full value; the comparison runs redis.sha1hex on the stored value
    // inside the script. A SHA1 collision would require a chosen-prefix attack on two existing
    // validly-encoded values of the same field, and the payoff is merely defeating one's own CAS
    // — not a protection target
    void expect_eq(const std::string& key, std::string_view field, std::string_view expected) {
        add_check("eq", key, field, sha1_hex(expected));
    }
    void expect_absent(const std::string& key, std::string_view field) {
        add_check("absent", key, field, {});
    }
    void expect_exists(const std::string& key, std::string_view field) {
        add_check("exists", key, field, {});
    }
    void expect_hlen0(const std::string& key) { add_check("hlen0", key, {}, {}); }
    void expect_sha1(const std::string& key, std::string_view sha1hex) {
        add_check("sha1", key, {}, sha1hex);
    }

    void hset(const std::string& key, std::string_view field, std::string_view value) {
        add_op("hset", key, field, value);
    }
    void hdel(const std::string& key, std::string_view field) {
        add_op("hdel", key, field, {});
    }
    void hincr(const std::string& key, std::string_view field, int64_t delta) {
        add_op("hincr", key, field, std::to_string(delta));
    }
    void zadd(const std::string& key, std::string_view score, std::string_view member) {
        add_op("zadd", key, score, member);
    }
    void zrem(const std::string& key, std::string_view member) {
        add_op("zrem", key, member, {});
    }
    void del(const std::string& key) { add_op("del", key, {}, {}); }

    // Single EVALSHA commit: returns true when all checks pass and the writes land; any check
    // failure (concurrent modification) returns false and the caller re-reads, rebuilds, retries (§3.2 CAS loop)
    bool commit() {
        std::vector<std::string> argv;
        argv.reserve(2 + checks_.size() + ops_.size());
        argv.push_back(std::to_string(checks_.size() / 4));
        for (auto& s : checks_) argv.push_back(std::move(s));
        argv.push_back(std::to_string(ops_.size() / 4));
        for (auto& s : ops_) argv.push_back(std::move(s));
        auto r = store_.eval(store_.sha_commit_, kCommitScript, keys_, std::move(argv),
                             /*read_retry=*/false);
        return require_int("commit", r.get()) == 1;
    }

private:
    // Dedup the KEYS table (referenced by 1-based index inside the script)
    std::string key_idx(const std::string& key) {
        for (size_t i = 0; i < keys_.size(); ++i)
            if (keys_[i] == key) return std::to_string(i + 1);
        keys_.push_back(key);
        return std::to_string(keys_.size());
    }
    void add_check(const char* type, const std::string& key, std::string_view a,
                   std::string_view b) {
        checks_.emplace_back(type);
        checks_.push_back(key_idx(key));
        checks_.emplace_back(a);
        checks_.emplace_back(b);
    }
    void add_op(const char* kind, const std::string& key, std::string_view a,
                std::string_view b) {
        ops_.emplace_back(kind);
        ops_.push_back(key_idx(key));
        ops_.emplace_back(a);
        ops_.emplace_back(b);
    }

    RedisMetaStore& store_;
    std::vector<std::string> keys_;
    std::vector<std::string> checks_;  // flattened 4-tuples
    std::vector<std::string> ops_;
};

// ---------- Construction / shutdown ----------

void RedisMetaStore::cas_backoff(int attempt) {
    if (attempt == 0) return;
    m_cas_retries_->inc();  // the previous commit round failed its guard; this round is the retry
    int shift = std::min(attempt - 1, 6);  // 100µs … 6.4ms, 16 attempts total ≈ 80ms
    std::this_thread::sleep_for(std::chrono::microseconds(100 << shift));
}

RedisMetaStore::RedisMetaStore(RedisMetaOptions opt) : opt_(std::move(opt)) {
    // R4 metrics: registering at construction makes zero values visible; an empty scope returns
    // detached instances, so direct construction in tests has zero wiring cost
    m_cas_retries_ = opt_.metrics.counter(
        "lights3_duostore_redis_cas_retries_total",
        "Guarded-commit rounds retried after a CAS check failed (concurrent modification)");
    m_reconnects_ = opt_.metrics.counter(
        "lights3_duostore_redis_reconnects_total",
        "Connections re-established after a pooled redis connection went bad");

    // URI parsing (§8): redis://[user][:pass]@host[:port][/db] or unix://<path>
    const std::string& uri = opt_.uri;
    auto bad_uri = [&] {
        throw std::runtime_error("duostore redis meta: invalid redis_uri: " + uri);
    };
    if (uri.rfind("unix://", 0) == 0) {
        unix_path_ = uri.substr(7);
        if (unix_path_.empty()) bad_uri();
    } else if (uri.rfind("redis://", 0) == 0) {
        std::string rest = uri.substr(8);
        if (auto slash = rest.find('/'); slash != std::string::npos) {
            std::string db = rest.substr(slash + 1);
            rest.resize(slash);
            if (!db.empty()) {
                try {
                    size_t pos = 0;
                    db_ = std::stoi(db, &pos);
                    if (pos != db.size() || db_ < 0) bad_uri();
                } catch (const std::exception&) {
                    bad_uri();
                }
            }
        }
        if (auto at = rest.rfind('@'); at != std::string::npos) {
            std::string userinfo = rest.substr(0, at);
            rest = rest.substr(at + 1);
            if (auto colon = userinfo.find(':'); colon != std::string::npos)
                password_ = userinfo.substr(colon + 1);  // user part ignored (no ACL requirement)
        }
        if (auto colon = rest.rfind(':'); colon != std::string::npos) {
            std::string port = rest.substr(colon + 1);
            rest.resize(colon);
            try {
                size_t pos = 0;
                port_ = std::stoi(port, &pos);
                if (pos != port.size() || port_ <= 0 || port_ > 65535) bad_uri();
            } catch (const std::exception&) {
                bad_uri();
            }
        }
        host_ = rest;
        if (host_.empty()) bad_uri();
    } else {
        bad_uri();
    }
    if (opt_.pool_size < 1) opt_.pool_size = 1;

    auto c = make_conn();  // an unreachable server fails loudly here
    std::string err;

    // Script preload (§3.5): SHAs are content-addressed; after a server restart/SCRIPT FLUSH, NOSCRIPT self-heals by reloading
    for (auto [body, sha] : {std::pair{kCommitScript, &sha_commit_},
                             std::pair{kListScript, &sha_list_}}) {
        auto r = run_on(c->ctx, {"SCRIPT", "LOAD", body}, &err);
        if (!r) throw_internal("script load", err);
        check_reply_error("script load", r.get());
        *sha = std::string(reply_str(r.get()));
    }

    // schema (§2.2): claim with SET NX; if it already exists, validate lineage + run version
    // evolution (docs/gaps.md §6.1: a stored version < current walks the migration chain,
    // > current rejects the downgrade — after an upgrade, old-version gateways are refused at
    // startup, naturally preventing mixed deployments from writing back and corrupting the new
    // layout). Every step in the chain must be idempotent: the shared engine has no global
    // migration lock, so multiple new-version gateways booting at once walk the chain
    // concurrently, and idempotent steps are mutually harmless
    auto r = run_on(c->ctx, {"SET", key("schema"), kSchemaValue, "NX"}, &err);
    if (!r) throw_internal("schema init", err);
    check_reply_error("schema init", r.get());
    if (r->type == REDIS_REPLY_NIL) {
        auto got = run_on(c->ctx, {"GET", key("schema")}, &err);
        if (!got) throw_internal("schema check", err);
        check_reply_error("schema check", got.get());
        if (got->type != REDIS_REPLY_STRING)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore redis meta: unsupported schema at prefix '" +
                              opt_.prefix + "'");
        std::string stored(reply_str(got.get()));
        int64_t ver = parse_schema_marker(stored, /*lineage=*/"r", kSchemaCurrent,
                                          "duostore redis meta");
        using MigrateFn = void (*)(RedisMetaStore&, Conn&);
        static constexpr std::array<std::pair<int64_t, MigrateFn>, 0> kSchemaMigrations{
            // {{1, &migrate_v1_to_v2}}  // example: once registered, v1 stores auto-upgrade at startup
        };
        for (; ver < kSchemaCurrent; ++ver) {
            MigrateFn fn = nullptr;
            for (auto& [from, f] : kSchemaMigrations)
                if (from == ver) fn = f;
            if (!fn) throw_no_migration(ver, kSchemaCurrent, "duostore redis meta");
            fn(*this, *c);
            auto stamp = run_on(c->ctx, {"SET", key("schema"), "r" + std::to_string(ver + 1)},
                                &err);
            if (!stamp) throw_internal("schema stamp", err);
            check_reply_error("schema stamp", stamp.get());
            LOG_INFO("duostore redis meta: schema migrated v{} -> v{}", ver, ver + 1);
        }
    }

    // AOF probe (§6): best-effort — managed Redis may disable CONFIG; failure only logs a hint
    if (auto probe = run_on(c->ctx, {"CONFIG", "GET", "appendonly"}, &err)) {
        if (probe->type == REDIS_REPLY_ARRAY && probe->elements == 2 &&
            reply_str(probe->element[1]) != "yes")
            LOG_WARN("duostore redis meta: appendonly=no -- a crash may roll back data; "
                     "deployment requires AOF (docs/duostore-redis-meta.md §6)");
    } else {
        LOG_INFO("duostore redis meta: CONFIG GET unavailable, skipping AOF probe");
    }

    release(std::move(c));
}

RedisMetaStore::~RedisMetaStore() {
    try {
        close();
    } catch (const std::exception& e) {
        LOG_ERROR("duostore redis meta: close in dtor failed: {}", e.what());
    }
}

void RedisMetaStore::close() {
    std::lock_guard lk(pool_mu_);
    closed_ = true;
    idle_.clear();  // redisFree all idle connections; calls after close fail cleanly in acquire() (500)
}

// ---------- Connection pool (§5.2) ----------

std::unique_ptr<RedisMetaStore::Conn> RedisMetaStore::make_conn() {
    timeval tv{opt_.timeout_ms / 1000, (opt_.timeout_ms % 1000) * 1000};
    redisContext* ctx = unix_path_.empty()
                            ? redisConnectWithTimeout(host_.c_str(), port_, tv)
                            : redisConnectUnixWithTimeout(unix_path_.c_str(), tv);
    if (!ctx || ctx->err) {
        std::string e = ctx ? ctx->errstr : "context alloc failed";
        if (ctx) redisFree(ctx);
        throw_internal("connect", e + " (" + opt_.uri + ")");
    }
    auto conn = std::make_unique<Conn>();
    conn->ctx = ctx;
    redisSetTimeout(ctx, tv);

    // Reconnect state replay order (§5.4): AUTH → SELECT (scripts are server-level; no per-connection load needed)
    std::string err;
    if (!password_.empty()) {
        auto r = run_on(ctx, {"AUTH", password_}, &err);
        if (!r) throw_internal("auth", err);
        check_reply_error("auth", r.get());
    }
    if (db_ != 0) {
        auto r = run_on(ctx, {"SELECT", std::to_string(db_)}, &err);
        if (!r) throw_internal("select", err);
        check_reply_error("select", r.get());
    }
    return conn;
}

std::unique_ptr<RedisMetaStore::Conn> RedisMetaStore::acquire() {
    {
        std::lock_guard lk(pool_mu_);
        if (closed_)
            throw S3Error(S3ErrorCode::InternalError, "duostore redis meta: store is closed");
        if (!idle_.empty()) {
            auto c = std::move(idle_.back());
            idle_.pop_back();
            return c;
        }
    }
    return make_conn();
}

void RedisMetaStore::release(std::unique_ptr<Conn> c) {
    std::lock_guard lk(pool_mu_);
    if (closed_ || idle_.size() >= size_t(opt_.pool_size)) return;  // redisFree directly
    idle_.push_back(std::move(c));
}

// ---------- Command execution ----------

RedisMetaStore::ReplyPtr RedisMetaStore::exec(const std::vector<std::string>& args,
                                              bool read_retry) {
    auto c = acquire();
    std::string err;
    auto r = run_on(c->ctx, args, &err);
    if (!r && read_retry) {
        // Read-only: drop the bad connection (usually a stale pooled one) and retry once on a fresh connection (§5.3)
        c = make_conn();
        m_reconnects_->inc();
        r = run_on(c->ctx, args, &err);
    }
    if (!r) {
        // Read-only still failing after the retry = definite failure; a commit-class command
        // reaching here = result unknown: no blind retry (§3.5) — throw a distinguishable type
        // (callers must not clean up by deleting data) and leave retrying to the upstream client
        const char* what = args.empty() ? "exec" : args[0].c_str();
        if (read_retry) throw_internal(what, err);
        throw_undetermined(what, err);
    }
    // After a successful commit-class command (!read_retry), WAIT on the same connection (§6) — WAIT only covers writes previously issued on this connection
    if (!read_retry && opt_.wait_replicas > 0 && !wait_for_replicas(*c))
        return r;  // connection is bad: do not return it to the pool (the write itself succeeded)
    release(std::move(c));
    return r;
}

bool RedisMetaStore::wait_for_replicas(Conn& c) {
    // Timeout is half the command timeout: the server must reply before the client's read
    // timeout (otherwise the connection is misjudged as bad). Insufficient replicas only WARN —
    // the write already took effect on the primary, and erroring would mislead S3 clients into
    // retrying (complete-class retries would even get a spurious NoSuchUpload); WAIT only
    // guarantees replication delivery, not replica fsync
    int timeout = std::max(1, opt_.timeout_ms / 2);
    std::string err;
    auto r = run_on(c.ctx, {"WAIT", std::to_string(opt_.wait_replicas),
                            std::to_string(timeout)}, &err);
    if (!r) {
        LOG_WARN("duostore redis meta: WAIT failed ({}), replication not confirmed", err);
        return false;
    }
    if (r->type == REDIS_REPLY_ERROR) {
        LOG_WARN("duostore redis meta: WAIT rejected: {}", std::string(r->str, r->len));
    } else if (r->type == REDIS_REPLY_INTEGER && r->integer < opt_.wait_replicas) {
        LOG_WARN("duostore redis meta: WAIT reached {}/{} replicas within {}ms", r->integer,
                 opt_.wait_replicas, timeout);
    }
    return true;
}

RedisMetaStore::ReplyPtr RedisMetaStore::eval(const std::string& sha, const char* body,
                                              std::vector<std::string> keys,
                                              std::vector<std::string> argv,
                                              bool read_retry) {
    std::vector<std::string> cmd;
    cmd.reserve(3 + keys.size() + argv.size());
    cmd.emplace_back("EVALSHA");
    cmd.push_back(sha);
    cmd.push_back(std::to_string(keys.size()));
    for (auto& k : keys) cmd.push_back(std::move(k));
    for (auto& a : argv) cmd.push_back(std::move(a));

    auto r = exec(cmd, read_retry);
    if (r->type == REDIS_REPLY_ERROR &&
        std::string_view(r->str, r->len).rfind("NOSCRIPT", 0) == 0) {
        // server restart / SCRIPT FLUSH: the script definitively did not run; reloading and resending is safe (§3.5)
        auto loaded = exec({"SCRIPT", "LOAD", body}, /*read_retry=*/true);
        check_reply_error("script reload", loaded.get());
        r = exec(cmd, read_retry);
    }
    return r;
}

// ---------- Key construction (§2.2) ----------

std::string RedisMetaStore::key(std::string_view suffix) const {
    std::string k = opt_.prefix;
    k += suffix;
    return k;
}
std::string RedisMetaStore::buckets_key() const { return key("buckets"); }
std::string RedisMetaStore::objects_key(std::string_view b) const {
    return key(std::string("o:") + std::string(b));
}
std::string RedisMetaStore::zindex_key(std::string_view b) const {
    return key(std::string("oz:") + std::string(b));
}
std::string RedisMetaStore::uploads_key(std::string_view b) const {
    return key(std::string("up:") + std::string(b));
}
std::string RedisMetaStore::parts_key(std::string_view b, std::string_view k,
                                      std::string_view id) const {
    // pt:<b>\0<key>\0<id>; segment validity is guaranteed by the shared validation layer + codec key builders (§2.1)
    std::string s = key("pt:");
    s += codec::upload_key(b, k, id);
    return s;
}
std::string RedisMetaStore::refs_key() const { return key("refs"); }
std::string RedisMetaStore::gcq_key() const { return key("gcq"); }
std::string RedisMetaStore::pack_key(uint64_t pack_id) const {
    return key("pack:" + std::to_string(pack_id));
}

// ---------- High-level helpers ----------

std::optional<std::string> RedisMetaStore::hget_raw(const std::string& k,
                                                    std::string_view field) {
    auto r = exec({"HGET", k, std::string(field)}, /*read_retry=*/true);
    check_reply_error("hget", r.get());
    if (r->type == REDIS_REPLY_NIL) return std::nullopt;
    if (r->type != REDIS_REPLY_STRING) throw_internal("hget", "unexpected reply type");
    return std::string(reply_str(r.get()));
}

void RedisMetaStore::require_bucket(std::string_view b) {
    if (!bucket_exists(b))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(b));
}

std::optional<std::string> RedisMetaStore::upload_raw(std::string_view b, std::string_view k,
                                                      std::string_view id) {
    std::string field = std::string(k) + '\0' + std::string(id);
    return hget_raw(uploads_key(b), field);
}

void RedisMetaStore::batch_refs(RedisBatch& bt, const DataRef& ref, bool add,
                                std::string_view owner) {
    for (const auto& e : ref.extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack liveness is tracked via stats (P2);
                                                      // chunk/rados both enter refs by file_id
        if (add)
            bt.hset(refs_key(), std::to_string(e.file_id), owner);
        else
            bt.hdel(refs_key(), std::to_string(e.file_id));
    }
}

void RedisMetaStore::batch_pack_delta(RedisBatch& bt, const DataRef& ref, int sign,
                                      int64_t rec_overhead) {
    // Aggregate multiple extents of the same pack first, then two HINCRBYs per pack (§9.1,
    // adjusted in the same batch as the business script); each record counts payload + header
    // overhead, the same accounting basis as file_size (docs/gaps.md §2.3a)
    std::map<uint64_t, std::pair<int64_t, int64_t>> agg;  // pack_id -> (bytes, recs)
    for (const auto& e : ref.extents) {
        if (e.kind != Extent::Kind::kPack) continue;
        auto& [bytes, recs] = agg[e.file_id];
        bytes += sign * (int64_t(e.length) + rec_overhead);
        recs += sign;
    }
    for (const auto& [id, d] : agg) {
        bt.hincr(pack_key(id), "live_bytes", d.first);
        bt.hincr(pack_key(id), "live_recs", d.second);
    }
}

void RedisMetaStore::enqueue_reclaim(RedisBatch& bt, const DataRef& ref,
                                     ReclaimReason reason) {
    if (ref.extents.empty()) return;
    // Pre-allocating seq (INCRBY segments) makes gcq enqueueing a pure write op, keeping the
    // script deterministic (§4). CAS retries waste seqs — harmless, seqs only need to be unique
    // and monotonic. Oversized DataRefs are split into multiple entries (docs/gaps.md §2.11):
    // GC per-batch decode memory stays bounded, and independent acks are harmless
    const int64_t ts = now_ms();
    for (size_t i = 0; i < ref.extents.size(); i += kReclaimMaxExtents) {
        size_t n = std::min(kReclaimMaxExtents, ref.extents.size() - i);
        Reclaim r;
        r.extents.assign(ref.extents.begin() + i, ref.extents.begin() + i + n);
        r.reason = reason;
        uint64_t seq = alloc_id(kCounterSeq, seqs_);
        std::string member = codec::be64_key(seq);
        member += codec::encode_reclaim(r, ts);
        bt.zadd(gcq_key(), std::to_string(seq), member);
    }
}

uint64_t RedisMetaStore::alloc_id(std::string_view counter_suffix, IdRange& r, uint32_t n) {
    n = std::clamp<uint32_t>(n, 1, kMaxIdRun);  // run ≤ kMaxIdRun << kIdSegment
    std::lock_guard lk(alloc_mu_);
    if (r.limit - r.next < n) {
        // The first reservation burns one extra segment (§4 mitigation 2): skips ids that may
        // have been handed out within the AOF everysec crash window but lost to counter
        // rollback. Wasting segments on crash/restart is harmless (only uniqueness and
        // monotonicity are needed); likewise the residue discarded on a segment switch
        // (run batch allocation requires contiguity within a segment, docs/gaps.md §3.9)
        uint64_t take = r.burned ? kIdSegment : 2 * kIdSegment;
        auto reply = exec({"INCRBY", key(counter_suffix), std::to_string(take)},
                          /*read_retry=*/false);
        uint64_t hi = uint64_t(require_int("reserve id segment", reply.get()));
        r.limit = hi;
        r.next = hi - kIdSegment;
        r.burned = true;
    }
    uint64_t first = r.next;
    r.next += n;
    return first;
}

uint64_t RedisMetaStore::alloc_file_run(Extent::Kind kind, uint32_t n) {
    // kRados shares the segment with kChunk (same rationale as the rocks version: refs are not split by kind, so this prevents cross-kind id collisions)
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCounterChunk : kCounterPack,
                    file_ids_[size_t(kind)], n);
}

// ---------- bucket ----------

void RedisMetaStore::create_bucket(std::string_view b) {
    // HSETNX is atomic as a single command (§2.2), no script needed
    auto r = exec({"HSETNX", buckets_key(), std::string(b), codec::encode_bucket(now_ms())},
                  /*read_retry=*/false);
    if (require_int("create_bucket", r.get()) == 0)
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(b));
}

void RedisMetaStore::delete_bucket(std::string_view b) {
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        require_bucket(b);
        // The precheck yields precise error codes; atomicity is guaranteed by the hlen0 recheck
        // inside the script (§3.3: the emptiness check covers both objects and in-progress
        // multipart uploads, matching AWS)
        for (const auto& k : {objects_key(b), uploads_key(b)}) {
            auto r = exec({"HLEN", k}, /*read_retry=*/true);
            if (require_int("delete_bucket", r.get()) != 0)
                throw S3Error(S3ErrorCode::BucketNotEmpty,
                              "The bucket you tried to delete is not empty", std::string(b));
        }
        RedisBatch bt(*this);
        bt.expect_exists(buckets_key(), b);
        bt.expect_hlen0(objects_key(b));
        bt.expect_hlen0(uploads_key(b));
        bt.hdel(buckets_key(), b);
        bt.del(zindex_key(b));
        if (bt.commit()) return;
    }
    throw_internal("delete_bucket", "too many CAS retries");
}

bool RedisMetaStore::bucket_exists(std::string_view b) {
    auto r = exec({"HEXISTS", buckets_key(), std::string(b)}, /*read_retry=*/true);
    return require_int("bucket_exists", r.get()) == 1;
}

std::vector<BucketInfo> RedisMetaStore::list_buckets() {
    auto r = exec({"HGETALL", buckets_key()}, /*read_retry=*/true);
    check_reply_error("list_buckets", r.get());
    if (r->type != REDIS_REPLY_ARRAY) throw_internal("list_buckets", "unexpected reply type");
    std::vector<BucketInfo> out;
    for (size_t i = 0; i + 1 < r->elements; i += 2) {
        int64_t created = codec::decode_bucket(reply_str(r->element[i + 1]));
        out.push_back({std::string(reply_str(r->element[i])), codec::from_unix_ms(created)});
    }
    std::sort(out.begin(), out.end(),
              [](const BucketInfo& a, const BucketInfo& x) { return a.name < x.name; });
    return out;
}

// ---------- object ----------

std::optional<ObjectRec> RedisMetaStore::get_object(std::string_view b, std::string_view k) {
    auto v = hget_raw(objects_key(b), k);
    if (!v) return std::nullopt;
    return codec::decode_object(std::string(k), *v);
}

std::optional<ObjectMeta> RedisMetaStore::head_object(std::string_view b, std::string_view k) {
    auto v = hget_raw(objects_key(b), k);
    if (!v) return std::nullopt;
    return codec::decode_object_meta(std::string(k), *v);
}

void RedisMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec,
                                PutCondition cond) {
    std::string owner = codec::object_key(b, k);
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        require_bucket(b);
        auto oldv = hget_raw(objects_key(b), k);
        std::optional<ObjectRec> old;
        if (oldv) old = codec::decode_object(std::string(k), *oldv);
        // The check is based on this round's CAS-witnessed old value: a concurrent writer's
        // insert makes expect_eq/expect_absent fail and retry, and the retry round re-checks —
        // check and commit are externally atomic (PutCondition contract)
        check_put_condition(cond, old, k);
        rec.version = old ? old->version + 1 : 1;

        RedisBatch bt(*this);
        bt.expect_exists(buckets_key(), b);  // bucket existence is atomic with the commit, same script (§3.3)
        if (oldv)
            bt.expect_eq(objects_key(b), k, *oldv);
        else
            bt.expect_absent(objects_key(b), k);
        bt.hset(objects_key(b), k, codec::encode_object(rec));
        bt.zadd(zindex_key(b), "0", k);
        batch_refs(bt, rec.data, /*add=*/true, owner);
        const int64_t ov = codec::pack_rec_overhead(b, k);
        batch_pack_delta(bt, rec.data, +1, ov);
        if (old) {
            enqueue_reclaim(bt, old->data, ReclaimReason::kOverwrite);
            batch_refs(bt, old->data, /*add=*/false, {});
            batch_pack_delta(bt, old->data, -1, ov);
        }
        if (bt.commit()) return;
    }
    throw_internal("put_object", "too many CAS retries");
}

bool RedisMetaStore::delete_object(std::string_view b, std::string_view k) {
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        require_bucket(b);
        auto oldv = hget_raw(objects_key(b), k);
        if (!oldv) return false;  // idempotent (when the object exists the bucket is necessarily non-empty; no bucket guard needed)
        auto old = codec::decode_object(std::string(k), *oldv);

        RedisBatch bt(*this);
        bt.expect_eq(objects_key(b), k, *oldv);
        bt.hdel(objects_key(b), k);
        bt.zrem(zindex_key(b), k);
        enqueue_reclaim(bt, old.data, ReclaimReason::kDelete);
        batch_refs(bt, old.data, /*add=*/false, {});
        batch_pack_delta(bt, old.data, -1, codec::pack_rec_overhead(b, k));
        if (bt.commit()) return true;
    }
    throw_internal("delete_object", "too many CAS retries");
}

ListResult RedisMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    require_bucket(b);
    ListResult out;
    // S3: max-keys=0 returns empty with IsTruncated=false (consistent across backends)
    if (opt.max_keys <= 0) return out;

    auto r = eval(sha_list_, kListScript, {zindex_key(b), objects_key(b)},
                  {opt.prefix, opt.start_after, opt.delimiter, std::to_string(opt.max_keys)},
                  /*read_retry=*/true);
    check_reply_error("list_objects", r.get());
    if (r->type != REDIS_REPLY_ARRAY || r->elements != 4)
        throw_internal("list_objects", "unexpected reply shape");
    out.is_truncated = require_int("list_objects", r->element[0]) == 1;
    if (r->element[1]->type == REDIS_REPLY_STRING)
        out.next_token = std::string(reply_str(r->element[1]));
    const redisReply* objs = r->element[2];
    for (size_t i = 0; i + 1 < objs->elements; i += 2)
        out.objects.push_back(codec::decode_object_meta(
            std::string(reply_str(objs->element[i])), reply_str(objs->element[i + 1])));
    const redisReply* groups = r->element[3];
    for (size_t i = 0; i < groups->elements; ++i)
        out.common_prefixes.emplace_back(reply_str(groups->element[i]));
    return out;
}

// ---------- multipart ----------

std::string RedisMetaStore::create_upload(std::string_view b, std::string_view k,
                                          ObjectMeta meta) {
    UploadRec rec;
    rec.upload_id = new_upload_id();
    rec.meta = std::move(meta);
    rec.meta.key = std::string(k);
    rec.initiated_ms = now_ms();
    std::string field = std::string(k) + '\0' + rec.upload_id;
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        require_bucket(b);
        RedisBatch bt(*this);
        bt.expect_exists(buckets_key(), b);
        bt.hset(uploads_key(b), field, codec::encode_upload(rec));
        if (bt.commit()) return rec.upload_id;
    }
    throw_internal("create_upload", "too many CAS retries");
}

UploadRec RedisMetaStore::require_upload(std::string_view b, std::string_view k,
                                         std::string_view id) {
    auto missing = [&]() -> S3Error {
        return {S3ErrorCode::NoSuchUpload, "The specified multipart upload does not exist.",
                std::string(id)};
    };
    if (!is_valid_upload_id(id)) throw missing();
    auto v = upload_raw(b, k, id);
    if (!v) throw missing();
    return codec::decode_upload(std::string(k), std::string(id), *v);
}

void RedisMetaStore::put_part(std::string_view b, std::string_view k, std::string_view id,
                              PartRec p) {
    require_upload(b, k, id);  // semantic validation (incl. id format); atomicity is rechecked by the script guards
    std::string ufield = std::string(k) + '\0' + std::string(id);
    std::string pkey = parts_key(b, k, id);
    std::string pfield = std::to_string(p.part_no);
    std::string owner = codec::part_key(b, k, id, p.part_no);
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        auto oldv = hget_raw(pkey, pfield);
        std::optional<PartRec> old;
        if (oldv) old = codec::decode_part(p.part_no, *oldv);

        RedisBatch bt(*this);
        // While the upload exists the bucket cannot be deleted (§3.3) — only the upload needs guarding; its value is immutable, so exists suffices
        bt.expect_exists(uploads_key(b), ufield);
        if (oldv)
            bt.expect_eq(pkey, pfield, *oldv);
        else
            bt.expect_absent(pkey, pfield);
        bt.hset(pkey, pfield, codec::encode_part(p));
        batch_refs(bt, p.data, /*add=*/true, owner);
        const int64_t ov = codec::pack_rec_overhead_part(b, k, id, p.part_no);
        batch_pack_delta(bt, p.data, +1, ov);
        if (old) {  // same-number re-upload is last-write-wins: the old part enters GC accounting in the same batch
            enqueue_reclaim(bt, old->data, ReclaimReason::kPartOverwrite);
            batch_refs(bt, old->data, /*add=*/false, {});
            batch_pack_delta(bt, old->data, -1, ov);
        }
        if (bt.commit()) return;
        if (!upload_raw(b, k, id))  // a concurrent complete/abort won
            throw S3Error(S3ErrorCode::NoSuchUpload,
                          "The specified multipart upload does not exist.", std::string(id));
    }
    throw_internal("put_part", "too many CAS retries");
}

std::vector<std::pair<std::string, PartRec>> RedisMetaStore::scan_parts(std::string_view b,
                                                                        std::string_view k,
                                                                        std::string_view id) {
    auto r = exec({"HGETALL", parts_key(b, k, id)}, /*read_retry=*/true);
    check_reply_error("scan parts", r.get());
    if (r->type != REDIS_REPLY_ARRAY) throw_internal("scan parts", "unexpected reply type");
    std::vector<std::pair<std::string, PartRec>> out;
    for (size_t i = 0; i + 1 < r->elements; i += 2) {
        int no = 0;
        try {
            no = std::stoi(std::string(reply_str(r->element[i])));
        } catch (const std::exception&) {
            throw_internal("scan parts", "bad part field");
        }
        out.emplace_back(std::string(reply_str(r->element[i + 1])),
                         codec::decode_part(no, reply_str(r->element[i + 1])));
    }
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& x) {
        return a.second.part_no < x.second.part_no;
    });
    return out;
}

std::vector<PartRec> RedisMetaStore::list_parts(std::string_view b, std::string_view k,
                                                std::string_view id) {
    require_upload(b, k, id);
    std::vector<PartRec> out;
    for (auto& [raw, p] : scan_parts(b, k, id)) out.push_back(std::move(p));
    return out;
}

std::vector<UploadInfo> RedisMetaStore::list_uploads(std::string_view b,
                                                    std::string_view key_marker,
                                                    std::string_view id_marker, int limit) {
    // Cursor hints can only be ignored here (docs/gaps.md §5.1): uploads are stored as a hash,
    // HSCAN's cursor is bucket-ordered not lexicographic, and there is no way to express
    // "start after some field". After the full HSCAN, the caller's apply_uploads_page trims —
    // pagination saves response body size, not this scan
    (void)key_marker;
    (void)id_marker;
    (void)limit;
    require_bucket(b);
    // HSCAN in batches (R4, §2.2): huge uploads tables are no longer materialized by a single
    // command. The cursor's weak consistency (concurrent add/remove during iteration may
    // miss/duplicate) is acceptable for ListMultipartUploads; the map doubles as dedup (HSCAN
    // may return the same field twice) and byte-order sorting of fields = (key, upload_id) order
    std::map<std::string, std::string> rows;
    std::string cursor = "0";
    do {
        auto r = exec({"HSCAN", uploads_key(b), cursor, "COUNT", "512"}, /*read_retry=*/true);
        check_reply_error("list_uploads", r.get());
        if (r->type != REDIS_REPLY_ARRAY || r->elements != 2)
            throw_internal("list_uploads", "unexpected HSCAN reply");
        cursor = std::string(reply_str(r->element[0]));
        const redisReply* kv = r->element[1];
        for (size_t i = 0; i + 1 < kv->elements; i += 2)
            rows.insert_or_assign(std::string(reply_str(kv->element[i])),
                                  std::string(reply_str(kv->element[i + 1])));
    } while (cursor != "0");
    std::vector<UploadInfo> out;
    for (auto& [field, val] : rows) {
        auto sep = field.rfind('\0');
        if (sep == std::string::npos) continue;
        auto rec = codec::decode_upload(field.substr(0, sep), field.substr(sep + 1), val);
        out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
    }
    return out;
}

// §8: complete is a pure metadata transaction, zero data movement — same helpers as the rocks
// version; "unchanged since read" for the parts set is guarded by a whole-set sha1 fingerprint
// (§3.3), recomputed and compared via redis.sha1hex inside the script
std::string RedisMetaStore::complete_upload(std::string_view b, std::string_view k,
                                            std::string_view id,
                                            std::span<const PartInfo> parts) {
    std::string ufield = std::string(k) + '\0' + std::string(id);
    std::string okey_owner = codec::object_key(b, k);
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        auto up = require_upload(b, k, id);
        require_bucket(b);

        auto scanned = scan_parts(b, k, id);
        std::string concat;
        for (const auto& [raw, p] : scanned) concat += raw;  // ascending part_no (same order as the script)
        std::string fingerprint = sha1_hex(concat);
        std::map<int, PartRec> stored;
        for (auto& [raw, p] : scanned) stored.emplace(p.part_no, std::move(p));

        std::set<int> selected;
        ObjectRec rec = assemble_completed_object(std::move(up.meta), parts, stored, selected);

        auto oldv = hget_raw(objects_key(b), k);
        std::optional<ObjectRec> old;
        if (oldv) old = codec::decode_object(std::string(k), *oldv);
        rec.version = old ? old->version + 1 : 1;

        RedisBatch bt(*this);
        bt.expect_exists(buckets_key(), b);
        bt.expect_exists(uploads_key(b), ufield);
        bt.expect_sha1(parts_key(b, k, id), fingerprint);
        if (oldv)
            bt.expect_eq(objects_key(b), k, *oldv);
        else
            bt.expect_absent(objects_key(b), k);
        bt.hset(objects_key(b), k, codec::encode_object(rec));
        bt.zadd(zindex_key(b), "0", k);
        bt.hdel(uploads_key(b), ufield);
        bt.del(parts_key(b, k, id));
        for (const auto& [no, p] : stored) {
            if (selected.count(no)) {
                // refs transfer to the object. Pack liveness stays unchanged, but the
                // accounting basis rebalances from part to object (-part header overhead
                // +object header overhead; recs -1/+1 cancel out): ensures a later object
                // delete, debited on the object basis, zeroes the account exactly
                batch_refs(bt, p.data, /*add=*/true, okey_owner);
                batch_pack_delta(bt, p.data, -1,
                                 codec::pack_rec_overhead_part(b, k, id, no));
                batch_pack_delta(bt, p.data, +1, codec::pack_rec_overhead(b, k));
            } else {  // unselected parts enter GC accounting
                enqueue_reclaim(bt, p.data, ReclaimReason::kComplete);
                batch_refs(bt, p.data, /*add=*/false, {});
                batch_pack_delta(bt, p.data, -1,
                                 codec::pack_rec_overhead_part(b, k, id, no));
            }
        }
        if (old) {  // the old same-name object enters GC accounting
            enqueue_reclaim(bt, old->data, ReclaimReason::kOverwrite);
            batch_refs(bt, old->data, /*add=*/false, {});
            batch_pack_delta(bt, old->data, -1, codec::pack_rec_overhead(b, k));
        }
        if (bt.commit()) return rec.meta.etag;
        // Guard failed: re-read to classify — upload gone → NoSuchUpload (thrown by
        // require_upload), everything else (concurrent put_part / object overwrite) → retry
    }
    throw_internal("complete_upload", "too many CAS retries");
}

void RedisMetaStore::abort_upload(std::string_view b, std::string_view k,
                                  std::string_view id) {
    std::string ufield = std::string(k) + '\0' + std::string(id);
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        require_upload(b, k, id);
        auto scanned = scan_parts(b, k, id);
        std::string concat;
        for (const auto& [raw, p] : scanned) concat += raw;
        std::string fingerprint = sha1_hex(concat);  // prevents unaccounted parts from a concurrent put_part

        RedisBatch bt(*this);
        bt.expect_exists(uploads_key(b), ufield);
        bt.expect_sha1(parts_key(b, k, id), fingerprint);
        bt.hdel(uploads_key(b), ufield);
        bt.del(parts_key(b, k, id));
        for (const auto& [raw, p] : scanned) {
            enqueue_reclaim(bt, p.data, ReclaimReason::kAbort);
            batch_refs(bt, p.data, /*add=*/false, {});
            batch_pack_delta(bt, p.data, -1,
                             codec::pack_rec_overhead_part(b, k, id, p.part_no));
        }
        if (bt.commit()) return;
    }
    throw_internal("abort_upload", "too many CAS retries");
}

// ---------- GC accounting ----------

std::vector<std::pair<uint64_t, Reclaim>> RedisMetaStore::peek_reclaims(size_t max,
                                                                        uint64_t min_seq,
                                                                        size_t max_extents) {
    if (max == 0) return {};
    // score = seq (≪ 2^53, exact as double); the range is inclusive starting at min_seq
    auto r = exec({"ZRANGEBYSCORE", gcq_key(), std::to_string(min_seq), "+inf", "LIMIT", "0",
                   std::to_string(max)},
                  /*read_retry=*/true);
    check_reply_error("peek_reclaims", r.get());
    if (r->type != REDIS_REPLY_ARRAY) throw_internal("peek_reclaims", "unexpected reply type");
    std::vector<std::pair<uint64_t, Reclaim>> out;
    size_t extents = 0;
    for (size_t i = 0; i < r->elements; ++i) {
        auto member = reply_str(r->element[i]);
        if (member.size() < 8) throw_internal("peek_reclaims", "bad gcq member");
        uint64_t seq = codec::parse_be64(member.substr(0, 8));  // first 8 bytes of the member are the seq
        out.emplace_back(seq, codec::decode_reclaim(member.substr(8)));
        // Cumulative extent cap (gaps §2.11): return at least 1 entry (over-fetched members are dropped in place)
        extents += out.back().second.extents.size();
        if (extents >= max_extents) break;
    }
    return out;
}

void RedisMetaStore::ack_reclaim(uint64_t seq) {
    // Blind delete is atomic as a single command (seq ≪ 2^53, score exact as double, §2.2)
    std::string s = std::to_string(seq);
    auto r = exec({"ZREMRANGEBYSCORE", gcq_key(), s, s}, /*read_retry=*/false);
    require_int("ack_reclaim", r.get());
}

// Batched ack (docs/gaps.md §6.1 four-engine matrix): previously not overridden, GC paid one RTT
// per entry — thousands of round trips per ack round after a large delete. One script, one RTT
// deletes them all; a lost ack is harmless (gcq leftovers get retried, unlink is idempotent),
// so the script needs no guards at all
void RedisMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    static const char* kBody = R"lua(
for i = 1, #ARGV do
  redis.call('ZREMRANGEBYSCORE', KEYS[1], ARGV[i], ARGV[i])
end
return #ARGV
)lua";
    static const std::string kSha = sha1_hex(kBody);
    std::vector<std::string> argv;
    argv.reserve(seqs.size());
    for (uint64_t s : seqs) argv.push_back(std::to_string(s));
    auto r = eval(kSha, kBody, {gcq_key()}, std::move(argv), /*read_retry=*/false);
    require_int("ack_reclaims", r.get());
}

// Multi-gateway GC lease (docs/gaps.md §6.1): SET NX semantics + same-owner renewal, atomic in a
// single script. TTL is handled by Redis expiry — a crashed holder naturally yields
bool RedisMetaStore::try_gc_lease(std::string_view owner, int64_t ttl_ms) {
    static const char* kBody = R"lua(
local cur = redis.call('GET', KEYS[1])
if cur and cur ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
)lua";
    static const std::string kSha = sha1_hex(kBody);
    auto r = eval(kSha, kBody, {key("gc_lease")},
                  {std::string(owner), std::to_string(ttl_ms)}, /*read_retry=*/false);
    return require_int("try_gc_lease", r.get()) == 1;
}

std::vector<PackStat> RedisMetaStore::pack_stats() {
    // SCAN MATCH <prefix>pack:* (cursor iteration, does not block the server) + per-key HGETALL.
    // Low-frequency GC path, per-key round trips acceptable; returns entries with live=0 and unsealed ones
    std::string pattern;
    for (char ch : key("pack:")) {  // escape glob metacharacters (the prefix may contain arbitrary bytes)
        if (ch == '*' || ch == '?' || ch == '[' || ch == ']' || ch == '\\')
            pattern.push_back('\\');
        pattern.push_back(ch);
    }
    pattern.push_back('*');
    const size_t skip = key("pack:").size();

    std::vector<PackStat> out;
    std::string cursor = "0";
    do {
        auto r = exec({"SCAN", cursor, "MATCH", pattern, "COUNT", "512"}, /*read_retry=*/true);
        check_reply_error("pack_stats scan", r.get());
        if (r->type != REDIS_REPLY_ARRAY || r->elements != 2)
            throw_internal("pack_stats", "unexpected SCAN reply");
        cursor = std::string(reply_str(r->element[0]));
        const redisReply* keys = r->element[1];
        for (size_t i = 0; i < keys->elements; ++i) {
            std::string kstr(reply_str(keys->element[i]));
            uint64_t id = 0;
            try {
                id = std::stoull(kstr.substr(skip));
            } catch (const std::exception&) {
                continue;  // not this store's format (prefix collision), skip
            }
            auto h = exec({"HGETALL", kstr}, /*read_retry=*/true);
            check_reply_error("pack_stats hgetall", h.get());
            if (h->type != REDIS_REPLY_ARRAY) continue;
            PackStat ps;
            ps.pack_id = id;
            for (size_t j = 0; j + 1 < h->elements; j += 2) {
                std::string_view f = reply_str(h->element[j]);
                int64_t v = 0;
                try {
                    v = std::stoll(std::string(reply_str(h->element[j + 1])));
                } catch (const std::exception&) {
                    continue;
                }
                if (f == "live_bytes") ps.live_bytes = v;
                else if (f == "live_recs") ps.live_recs = v;
                else if (f == "file_size") ps.file_size = uint64_t(v);
                else if (f == "sealed") ps.sealed = v != 0;
            }
            out.push_back(ps);
        }
    } while (cursor != "0");
    std::sort(out.begin(), out.end(),
              [](const PackStat& a, const PackStat& x) { return a.pack_id < x.pack_id; });
    return out;
}

void RedisMetaStore::seal_pack(uint64_t pack_id, uint64_t file_size) {
    // HSET always sets sealed=1 (idempotent); file_size=0 goes through HSETNX — never overwrite a known size (contract)
    auto r = exec({"HSET", pack_key(pack_id), "sealed", "1"}, /*read_retry=*/false);
    require_int("seal_pack", r.get());
    if (file_size > 0) {
        auto s = exec({"HSET", pack_key(pack_id), "file_size", std::to_string(file_size)},
                      /*read_retry=*/false);
        require_int("seal_pack", s.get());
    } else {
        auto s = exec({"HSETNX", pack_key(pack_id), "file_size", "0"}, /*read_retry=*/false);
        require_int("seal_pack", s.get());
    }
}

void RedisMetaStore::drop_pack_stat(uint64_t pack_id) {
    auto r = exec({"DEL", pack_key(pack_id)}, /*read_retry=*/false);
    require_int("drop_pack_stat", r.get());
}

bool RedisMetaStore::swap_extents(std::string_view b, std::string_view k,
                                  uint64_t expect_version, const DataRef& from,
                                  const DataRef& to) {
    std::string okey_owner = codec::object_key(b, k);
    auto oldv = hget_raw(objects_key(b), k);
    if (!oldv) return false;
    auto rec = codec::decode_object(std::string(k), *oldv);
    // Optimistic validation: version or extent mismatch = overwritten/deleted in the meantime → give up (§9.2)
    if (rec.version != expect_version || rec.data.extents != from.extents) return false;
    rec.data = to;
    rec.version += 1;

    RedisBatch bt(*this);
    // The whole old raw byte string is a natural CAS (§3.3): any concurrent change bumps the
    // version → guard fails. No retry on failure — semantically the same as a version mismatch,
    // give up on this entry (the new write does its own accounting)
    bt.expect_eq(objects_key(b), k, *oldv);
    bt.hset(objects_key(b), k, codec::encode_object(rec));
    // refs operate on the set difference (meta_util.h refs_delta): in Lua, hset→hdel on the same
    // field nets out to a delete; add-all-then-delete-all would wipe refs of unmigrated chunks →
    // the orphan scan would delete live data by mistake
    auto rd = refs_delta(from, to);
    batch_refs(bt, rd.added, /*add=*/true, okey_owner);
    batch_refs(bt, rd.removed, /*add=*/false, {});
    // Compaction swaps refs: the accounting migrates with the extents (§9.2); both sides use the
    // object basis (if the migrated-out old record was in mpu form this slightly under-debits —
    // the conservative direction)
    const int64_t ov = codec::pack_rec_overhead(b, k);
    batch_pack_delta(bt, to, +1, ov);
    batch_pack_delta(bt, from, -1, ov);
    return bt.commit();
}

bool RedisMetaStore::chunk_referenced(uint64_t file_id) {
    auto r = exec({"HEXISTS", refs_key(), std::to_string(file_id)}, /*read_retry=*/true);
    return require_int("chunk_referenced", r.get()) == 1;
}

void RedisMetaStore::scan_refs(const std::function<void(uint64_t)>& cb) {
    // HSCAN iterates the refs HASH in batches (the cursor snapshot is weakly consistent, tolerated by the orphan scan); field = decimal file_id
    std::string cursor = "0";
    do {
        auto r = exec({"HSCAN", refs_key(), cursor, "COUNT", "512"}, /*read_retry=*/true);
        check_reply_error("scan_refs", r.get());
        if (r->type != REDIS_REPLY_ARRAY || r->elements != 2)
            throw_internal("scan_refs", "unexpected HSCAN reply");
        cursor = std::string(reply_str(r->element[0]));
        const redisReply* kv = r->element[1];
        for (size_t i = 0; i + 1 < kv->elements; i += 2) {  // field,value pairs; only the field is used
            uint64_t id = 0;
            try {
                id = std::stoull(std::string(reply_str(kv->element[i])));
            } catch (const std::exception&) {
                continue;  // not this store's format (prefix collision), skip
            }
            cb(id);
        }
    } while (cursor != "0");
}

}  // namespace lights3::storage::duostore
