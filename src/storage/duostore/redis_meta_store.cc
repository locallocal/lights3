#include "storage/duostore/redis_meta_store.h"

#include <hiredis.h>
#include <openssl/evp.h>
#include <sys/time.h>

#include <algorithm>
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

// 计数器 key 后缀（§2.2）：file_id 号段与 gcq seq
constexpr const char* kCounterChunk = "ctr:chunk";
constexpr const char* kCounterPack = "ctr:pack";
constexpr const char* kCounterSeq = "ctr:seq";
constexpr const char* kSchemaValue = "r1";  // 与 RocksDB 的 "1" 区分谱系（§2.2）
// CAS 重试上限（§3.2）：超限抛 InternalError——病态热点竞争时响亮失败优于活锁。
// 无退避的紧循环会被对端的连续提交流饿死（多网关热 key），故重试前指数退避
constexpr int kMaxCasRetries = 16;

void cas_backoff(int attempt) {
    if (attempt == 0) return;
    int shift = std::min(attempt - 1, 6);  // 100µs … 6.4ms，16 次合计 ≈ 80ms
    std::this_thread::sleep_for(std::chrono::microseconds(100 << shift));
}

[[noreturn]] void throw_internal(const char* what, const std::string& detail) {
    LOG_ERROR("duostore redis meta: {}: {}", what, detail);
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore redis meta: ") + what + ": " + detail);
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

// ---- 守卫式提交脚本（§3.2）：check 与 op 均为定长 4 元组，任一 check 失败
// 立即返回 0（不写任何东西）。value 对脚本不透明——只做字节级比对（§3.1）。
constexpr const char* kCommitScript = R"lua(
local i = 1
local nc = tonumber(ARGV[i]); i = i + 1
for _ = 1, nc do
  local t, kx, f, exp = ARGV[i], ARGV[i+1], ARGV[i+2], ARGV[i+3]
  i = i + 4
  local key = KEYS[tonumber(kx)]
  if t == 'eq' then
    if redis.call('HGET', key, f) ~= exp then return 0 end
  elseif t == 'absent' then
    if redis.call('HGET', key, f) then return 0 end
  elseif t == 'exists' then
    if redis.call('HEXISTS', key, f) == 0 then return 0 end
  elseif t == 'hlen0' then
    if redis.call('HLEN', key) ~= 0 then return 0 end
  elseif t == 'zcard0' then
    if redis.call('ZCARD', key) ~= 0 then return 0 end
  else
    local fields = redis.call('HKEYS', key)
    table.sort(fields, function(a, b) return tonumber(a) < tonumber(b) end)
    local buf = {}
    for j = 1, #fields do buf[j] = redis.call('HGET', key, fields[j]) end
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
  elseif k == 'zadd' then redis.call('ZADD', key, a, b)
  elseif k == 'zrem' then redis.call('ZREM', key, a)
  elseif k == 'del' then redis.call('DEL', key)
  elseif k == 'set' then redis.call('SET', key, a)
  end
end
return 1
)lua";

// ---- list_objects 脚本（§2.3）：算法照搬 rocks_meta_store.cc §4.4——seek 起点
// max(prefix, start_after)、delimiter 组末字节 +1 跳组、token 落组尾。整个循环
// 在单脚本内执行 = 1 RTT + 一致视图。KEYS[1]=oz(ZSET) KEYS[2]=o(HASH)；
// ARGV = prefix, start_after, delimiter, max_keys。
// 返回 { truncated, next_token, {k1,v1,...}, {group,...} }
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

local objs, groups = {}, {}
local truncated, next_token, last_emitted = 0, '', ''
local count = 0

local min
if start_after ~= '' and start_after >= prefix then min = '(' .. start_after
else min = '[' .. prefix end

while true do
  local m = redis.call('ZRANGEBYLEX', oz, min, '+', 'LIMIT', 0, 1)
  local uk = m[1]
  if not uk then break end
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
      grouped = true
    end
  end
  if not grouped then
    local v = redis.call('HGET', oh, uk)
    if v then
      objs[#objs + 1] = uk
      objs[#objs + 1] = v
    end
    last_emitted = uk
    count = count + 1
    min = '(' .. uk
  end
end
return { truncated, next_token, objs, groups }
)lua";

}  // namespace

// ---------- 连接与 reply ----------

struct RedisMetaStore::Conn {
    redisContext* ctx = nullptr;
    ~Conn() {
        if (ctx) redisFree(ctx);
    }
};

void RedisReplyDeleter::operator()(redisReply* r) const { freeReplyObject(r); }

namespace {

// 单连接上执行一条命令；连接层失败（IO/超时/协议）返回空并带回 errstr
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

// REDIS_REPLY_ERROR → InternalError（携带 server 错误文本，§5.3）
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

// ---------- RedisBatch：guarded-commit 组装器（§3.2，镜像 WriteBatch 的追加接口）----------

class RedisBatch {
public:
    explicit RedisBatch(RedisMetaStore& store) : store_(store) {}

    void expect_eq(const std::string& key, std::string_view field, std::string_view expected) {
        add_check("eq", key, field, expected);
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
    void zadd(const std::string& key, std::string_view score, std::string_view member) {
        add_op("zadd", key, score, member);
    }
    void zrem(const std::string& key, std::string_view member) {
        add_op("zrem", key, member, {});
    }
    void del(const std::string& key) { add_op("del", key, {}, {}); }

    // 一次 EVALSHA 提交：全部 check 通过并落写返回 true；任一失败（并发修改）
    // 返回 false，调用方重读重建重试（§3.2 CAS 循环）
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
    // KEYS 表去重（脚本内以 1-based 下标引用）
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
    std::vector<std::string> checks_;  // 展平的 4 元组
    std::vector<std::string> ops_;
};

// ---------- 构造 / 关闭 ----------

RedisMetaStore::RedisMetaStore(RedisMetaOptions opt) : opt_(std::move(opt)) {
    // URI 解析（§8）：redis://[user][:pass]@host[:port][/db] 或 unix://<path>
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
                password_ = userinfo.substr(colon + 1);  // user 段忽略（无 ACL 需求）
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

    auto c = make_conn();  // 不可达在此响亮失败
    std::string err;

    // 脚本预载（§3.5）：SHA 内容寻址，server 重启/SCRIPT FLUSH 后 NOSCRIPT 自愈重载
    for (auto [body, sha] : {std::pair{kCommitScript, &sha_commit_},
                             std::pair{kListScript, &sha_list_}}) {
        auto r = run_on(c->ctx, {"SCRIPT", "LOAD", body}, &err);
        if (!r) throw_internal("script load", err);
        check_reply_error("script load", r.get());
        *sha = std::string(reply_str(r.get()));
    }

    // schema（§2.2）：SET NX 抢注，已存在则校验谱系
    auto r = run_on(c->ctx, {"SET", key("schema"), kSchemaValue, "NX"}, &err);
    if (!r) throw_internal("schema init", err);
    check_reply_error("schema init", r.get());
    if (r->type == REDIS_REPLY_NIL) {
        auto got = run_on(c->ctx, {"GET", key("schema")}, &err);
        if (!got) throw_internal("schema check", err);
        check_reply_error("schema check", got.get());
        if (got->type != REDIS_REPLY_STRING || reply_str(got.get()) != kSchemaValue)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore redis meta: unsupported schema at prefix '" +
                              opt_.prefix + "'");
    }

    // AOF 探测（§6）：尽力而为——托管 Redis 可能禁用 CONFIG，失败仅提示
    if (auto probe = run_on(c->ctx, {"CONFIG", "GET", "appendonly"}, &err)) {
        if (probe->type == REDIS_REPLY_ARRAY && probe->elements == 2 &&
            reply_str(probe->element[1]) != "yes")
            LOG_WARN("duostore redis meta: appendonly=no —— 崩溃可回档，部署要求 AOF"
                     "（docs/duostore-redis-meta.md §6）");
    } else {
        LOG_INFO("duostore redis meta: CONFIG GET 不可用，跳过 AOF 探测");
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
    idle_.clear();  // redisFree 全部空闲连接；close 后调用在 acquire() 干净失败（500）
}

// ---------- 连接池（§5.2）----------

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

    // 重连状态补齐顺序（§5.4）：AUTH → SELECT（脚本是 server 级，无需逐连接加载）
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
    if (closed_ || idle_.size() >= size_t(opt_.pool_size)) return;  // 直接 redisFree
    idle_.push_back(std::move(c));
}

// ---------- 命令执行 ----------

RedisMetaStore::ReplyPtr RedisMetaStore::exec(const std::vector<std::string>& args,
                                              bool read_retry) {
    auto c = acquire();
    std::string err;
    auto r = run_on(c->ctx, args, &err);
    if (!r && read_retry) {
        // 纯读：坏连接（多半是池中陈旧连接）丢弃，换新连接重试一次（§5.3）
        c = make_conn();
        r = run_on(c->ctx, args, &err);
    }
    if (!r) {
        // 提交类到这里 = 结果不明：不盲重试（§3.5），抛 500 交上层客户端重试
        throw_internal(args.empty() ? "exec" : args[0].c_str(), err);
    }
    release(std::move(c));
    return r;
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
        // server 重启 / SCRIPT FLUSH：脚本明确未执行，重载后重发安全（§3.5）
        auto loaded = exec({"SCRIPT", "LOAD", body}, /*read_retry=*/true);
        check_reply_error("script reload", loaded.get());
        r = exec(cmd, read_retry);
    }
    return r;
}

// ---------- key 构造（§2.2）----------

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
    // pt:<b>\0<key>\0<id>；段合法性由共享校验层 + codec 键构造器保证（§2.1）
    std::string s = key("pt:");
    s += codec::upload_key(b, k, id);
    return s;
}
std::string RedisMetaStore::refs_key() const { return key("refs"); }
std::string RedisMetaStore::gcq_key() const { return key("gcq"); }

// ---------- 高层辅助 ----------

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
        if (e.kind != Extent::Kind::kChunk) continue;  // pack 存活走 stats 账（P2）
        if (add)
            bt.hset(refs_key(), std::to_string(e.file_id), owner);
        else
            bt.hdel(refs_key(), std::to_string(e.file_id));
    }
}

void RedisMetaStore::enqueue_reclaim(RedisBatch& bt, const DataRef& ref) {
    if (ref.extents.empty()) return;
    // seq 预派发（INCRBY 号段）使 gcq 入账成为纯写 op，脚本保持确定性（§4）。
    // CAS 重试会浪费 seq——无害，seq 只需唯一单调
    uint64_t seq = alloc_id(kCounterSeq, seqs_);
    std::string member = codec::be64_key(seq);
    member += codec::encode_reclaim(Reclaim{ref.extents}, now_ms());
    bt.zadd(gcq_key(), std::to_string(seq), member);
}

uint64_t RedisMetaStore::alloc_id(std::string_view counter_suffix, IdRange& r) {
    std::lock_guard lk(alloc_mu_);
    if (r.next == r.limit) {
        // 首次预留空烧一个号段（§4 缓解 2）：跳过 AOF everysec 崩溃窗口内可能
        // 已派发、但计数器回滚丢失的 id。崩溃/重启浪费号段无害（只需唯一单调）
        uint64_t take = r.burned ? kIdSegment : 2 * kIdSegment;
        auto reply = exec({"INCRBY", key(counter_suffix), std::to_string(take)},
                          /*read_retry=*/false);
        uint64_t hi = uint64_t(require_int("reserve id segment", reply.get()));
        r.limit = hi;
        r.next = hi - kIdSegment;
        r.burned = true;
    }
    return r.next++;
}

uint64_t RedisMetaStore::alloc_file_id(Extent::Kind kind) {
    return alloc_id(kind == Extent::Kind::kChunk ? kCounterChunk : kCounterPack,
                    file_ids_[size_t(kind)]);
}

// ---------- bucket ----------

void RedisMetaStore::create_bucket(std::string_view b) {
    // HSETNX 单命令即原子（§2.2），无需脚本
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
        // 预检给出精确错误码；原子性由脚本内 hlen0 复查保证（§3.3：空检查同时
        // 覆盖 objects 与进行中 multipart，对齐 AWS）
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

void RedisMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec) {
    std::string owner = codec::object_key(b, k);
    for (int attempt = 0; attempt < kMaxCasRetries; ++attempt) {
        cas_backoff(attempt);
        require_bucket(b);
        auto oldv = hget_raw(objects_key(b), k);
        std::optional<ObjectRec> old;
        if (oldv) old = codec::decode_object(std::string(k), *oldv);
        rec.version = old ? old->version + 1 : 1;

        RedisBatch bt(*this);
        bt.expect_exists(buckets_key(), b);  // 桶存在性与提交同脚本原子（§3.3）
        if (oldv)
            bt.expect_eq(objects_key(b), k, *oldv);
        else
            bt.expect_absent(objects_key(b), k);
        bt.hset(objects_key(b), k, codec::encode_object(rec));
        bt.zadd(zindex_key(b), "0", k);
        batch_refs(bt, rec.data, /*add=*/true, owner);
        if (old) {
            enqueue_reclaim(bt, old->data);
            batch_refs(bt, old->data, /*add=*/false, {});
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
        if (!oldv) return false;  // 幂等（对象存在时桶必非空，无需桶守卫）
        auto old = codec::decode_object(std::string(k), *oldv);

        RedisBatch bt(*this);
        bt.expect_eq(objects_key(b), k, *oldv);
        bt.hdel(objects_key(b), k);
        bt.zrem(zindex_key(b), k);
        enqueue_reclaim(bt, old.data);
        batch_refs(bt, old.data, /*add=*/false, {});
        if (bt.commit()) return true;
    }
    throw_internal("delete_object", "too many CAS retries");
}

ListResult RedisMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    require_bucket(b);
    ListResult out;
    // S3：max-keys=0 返回空且 IsTruncated=false（与各后端一致）
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
    require_upload(b, k, id);  // 语义校验（含 id 格式）；原子性由脚本守卫复查
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
        // upload 存在则桶删不掉（§3.3）——只需守卫 upload；value 不可变，exists 即够
        bt.expect_exists(uploads_key(b), ufield);
        if (oldv)
            bt.expect_eq(pkey, pfield, *oldv);
        else
            bt.expect_absent(pkey, pfield);
        bt.hset(pkey, pfield, codec::encode_part(p));
        batch_refs(bt, p.data, /*add=*/true, owner);
        if (old) {  // 同号重传 last-write-wins：旧分片同批入 GC 账
            enqueue_reclaim(bt, old->data);
            batch_refs(bt, old->data, /*add=*/false, {});
        }
        if (bt.commit()) return;
        if (!upload_raw(b, k, id))  // 并发 complete/abort 赢了
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

std::vector<UploadInfo> RedisMetaStore::list_uploads(std::string_view b) {
    require_bucket(b);
    auto r = exec({"HGETALL", uploads_key(b)}, /*read_retry=*/true);
    check_reply_error("list_uploads", r.get());
    if (r->type != REDIS_REPLY_ARRAY) throw_internal("list_uploads", "unexpected reply type");
    // field = <key>\0<id>，按 field 字节序排序即 (key, upload_id) 序（§2.2）
    std::vector<std::pair<std::string, std::string>> rows;
    for (size_t i = 0; i + 1 < r->elements; i += 2)
        rows.emplace_back(std::string(reply_str(r->element[i])),
                          std::string(reply_str(r->element[i + 1])));
    std::sort(rows.begin(), rows.end());
    std::vector<UploadInfo> out;
    for (auto& [field, val] : rows) {
        auto sep = field.rfind('\0');
        if (sep == std::string::npos) continue;
        auto rec = codec::decode_upload(field.substr(0, sep), field.substr(sep + 1), val);
        out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
    }
    return out;
}

// §8：complete 是纯元数据事务，零数据搬运——与 rocks 版同一套 helper；parts 集合
// 的"读取后未变"用 sha1 整体指纹守卫（§3.3），脚本内 redis.sha1hex 重算比对
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
        for (const auto& [raw, p] : scanned) concat += raw;  // part_no 升序（脚本同序）
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
                batch_refs(bt, p.data, /*add=*/true, okey_owner);  // refs 转移到对象
            } else {  // 未选中分片入 GC 账
                enqueue_reclaim(bt, p.data);
                batch_refs(bt, p.data, /*add=*/false, {});
            }
        }
        if (old) {  // 旧同名对象入 GC 账
            enqueue_reclaim(bt, old->data);
            batch_refs(bt, old->data, /*add=*/false, {});
        }
        if (bt.commit()) return rec.meta.etag;
        // 守卫失败：重读分类——upload 消失 → NoSuchUpload（require_upload 抛出），
        // 其余（并发 put_part / 对象覆盖）→ 重试
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
        std::string fingerprint = sha1_hex(concat);  // 防并发 put_part 的分片漏账

        RedisBatch bt(*this);
        bt.expect_exists(uploads_key(b), ufield);
        bt.expect_sha1(parts_key(b, k, id), fingerprint);
        bt.hdel(uploads_key(b), ufield);
        bt.del(parts_key(b, k, id));
        for (const auto& [raw, p] : scanned) {
            enqueue_reclaim(bt, p.data);
            batch_refs(bt, p.data, /*add=*/false, {});
        }
        if (bt.commit()) return;
    }
    throw_internal("abort_upload", "too many CAS retries");
}

// ---------- GC 记账 ----------

std::vector<std::pair<uint64_t, Reclaim>> RedisMetaStore::peek_reclaims(size_t max) {
    if (max == 0) return {};
    auto r = exec({"ZRANGEBYSCORE", gcq_key(), "-inf", "+inf", "LIMIT", "0",
                   std::to_string(max)},
                  /*read_retry=*/true);
    check_reply_error("peek_reclaims", r.get());
    if (r->type != REDIS_REPLY_ARRAY) throw_internal("peek_reclaims", "unexpected reply type");
    std::vector<std::pair<uint64_t, Reclaim>> out;
    for (size_t i = 0; i < r->elements; ++i) {
        auto member = reply_str(r->element[i]);
        if (member.size() < 8) throw_internal("peek_reclaims", "bad gcq member");
        uint64_t seq = codec::parse_be64(member.substr(0, 8));  // member 前 8 字节即 seq
        out.emplace_back(seq, codec::decode_reclaim(member.substr(8)));
    }
    return out;
}

void RedisMetaStore::ack_reclaim(uint64_t seq) {
    // 盲删单命令即原子（seq ≪ 2^53，score double 精确，§2.2）
    std::string s = std::to_string(seq);
    auto r = exec({"ZREMRANGEBYSCORE", gcq_key(), s, s}, /*read_retry=*/false);
    require_int("ack_reclaim", r.get());
}

std::vector<PackStat> RedisMetaStore::pack_stats() {
    return {};  // pack 存活账（pack:<id> HASH）随 P2 pack 聚合引入（§2.2）
}

bool RedisMetaStore::swap_extents(std::string_view b, std::string_view k,
                                  uint64_t expect_version, const DataRef& from,
                                  const DataRef& to) {
    std::string okey_owner = codec::object_key(b, k);
    auto oldv = hget_raw(objects_key(b), k);
    if (!oldv) return false;
    auto rec = codec::decode_object(std::string(k), *oldv);
    // 乐观校验：version 或 extent 不符 = 期间被覆盖/删除 → 放弃（§9.2）
    if (rec.version != expect_version || rec.data.extents != from.extents) return false;
    rec.data = to;
    rec.version += 1;

    RedisBatch bt(*this);
    // 整段旧原始字节即天然 CAS（§3.3）：任何并发改动都 bump version → 守卫失败。
    // 失败不重试——语义上等同 version 不符，放弃本条（新写入自会记账）
    bt.expect_eq(objects_key(b), k, *oldv);
    bt.hset(objects_key(b), k, codec::encode_object(rec));
    batch_refs(bt, to, /*add=*/true, okey_owner);
    batch_refs(bt, from, /*add=*/false, {});
    return bt.commit();
}

bool RedisMetaStore::chunk_referenced(uint64_t file_id) {
    auto r = exec({"HEXISTS", refs_key(), std::to_string(file_id)}, /*read_retry=*/true);
    return require_int("chunk_referenced", r.get()) == 1;
}

}  // namespace lights3::storage::duostore
