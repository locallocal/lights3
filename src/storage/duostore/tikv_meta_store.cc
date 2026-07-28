#include "storage/duostore/tikv_meta_store.h"

#include <pingcap/Exception.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <thread>
#include <type_traits>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/meta_util.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// 计数器 kind 字符（'C' 表内）：chunk / pack 号段、gcq seq 与 pack 账 delta 行 id
constexpr char kCtrChunk = '0';
constexpr char kCtrPack = '1';
constexpr char kCtrSeq = 'q';
constexpr char kCtrPackDelta = 'd';

// pack 账 delta 行折叠阈值（§3.2）：单 pack 的 delta 行超过此数即在 pack_stats()
// 顺带折叠为一行——低频 GC 路径承担合并，业务写路径保持纯写无冲突
constexpr size_t kPackFoldThreshold = 16;

// delta 行值：le64 live_bytes ‖ le64 live_recs（encode_counter_delta 的 8B 编码 ×2）
std::string encode_pack_delta(int64_t bytes, int64_t recs) {
    return codec::encode_counter_delta(bytes) + codec::encode_counter_delta(recs);
}

std::pair<int64_t, int64_t> decode_pack_delta(std::string_view v) {
    if (v.size() != 16)
        throw S3Error(S3ErrorCode::InternalError, "duostore tikv meta: bad pack delta row");
    return {codec::decode_counter(v.substr(0, 8)), codec::decode_counter(v.substr(8, 8))};
}

// 冲突重试（§4.1，与 Redis 版 CAS 循环同构）：指数退避 100µs 起、上限 6.4ms、
// 最多 16 次——超限即病态热点竞争，响亮失败优于活锁
constexpr int kMaxTxnRetries = 16;

// 分页扫描的单页条数（Scanner 内部每批 256，此处是我方聚合页）
constexpr size_t kScanPage = 1024;

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

void conflict_backoff(int attempt) {
    auto us = std::chrono::microseconds(100) * (1 << std::min(attempt, 6));
    std::this_thread::sleep_for(us);
}

[[noreturn]] void throw_internal(const char* what, const std::string& detail) {
    LOG_ERROR("duostore tikv meta: {}: {}", what, detail);
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore tikv meta: ") + what + ": " + detail);
}

[[noreturn]] void throw_no_bucket(std::string_view b) {
    throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                  std::string(b));
}

[[noreturn]] void throw_no_upload(std::string_view id) {
    throw S3Error(S3ErrorCode::NoSuchUpload, "The specified multipart upload does not exist.",
                  std::string(id));
}

// FNV-1a：守卫分片散列（§4.3）。只需稳定散布，不需抗碰撞
uint64_t fnv1a(std::string_view k) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char c : k) h = (h ^ c) * 1099511628211ull;
    return h;
}

}  // namespace

// ---------- key 构造（§3.2）----------

std::string TikvMetaStore::tkey(char tag, std::string_view rest) const {
    std::string k;
    k.reserve(opt_.prefix.size() + 1 + rest.size());
    k += opt_.prefix;
    k += tag;
    k += rest;
    return k;
}

std::string TikvMetaStore::bucket_key(std::string_view b) const { return tkey('B', b); }

std::string TikvMetaStore::bucket_guard(std::string_view b, uint32_t shard) const {
    std::string rest(b);
    rest += '\0';
    rest += char(shard);
    return tkey('b', rest);
}

std::string TikvMetaStore::object_key(std::string_view b, std::string_view k) const {
    return tkey('O', codec::object_key(b, k));
}

std::string TikvMetaStore::upload_key(std::string_view b, std::string_view k,
                                      std::string_view id) const {
    return tkey('U', codec::upload_key(b, k, id));
}

std::string TikvMetaStore::upload_guard(std::string_view b, std::string_view k,
                                        std::string_view id, uint32_t shard) const {
    std::string rest = codec::upload_key(b, k, id);
    rest += '\0';
    rest += char(shard);
    return tkey('u', rest);
}

std::string TikvMetaStore::part_key(std::string_view b, std::string_view k, std::string_view id,
                                    int part_no) const {
    return tkey('P', codec::part_key(b, k, id, part_no));
}

std::string TikvMetaStore::refs_key(uint64_t file_id) const {
    return tkey('R', codec::be64_key(file_id));
}

std::string TikvMetaStore::gcq_key(uint64_t seq) const { return tkey('G', codec::be64_key(seq)); }

std::string TikvMetaStore::counter_key(char kind) const {
    return tkey('C', std::string_view(&kind, 1));
}

std::string TikvMetaStore::pack_delta_key(uint64_t pack_id, uint64_t delta_id) const {
    std::string rest = codec::be64_key(pack_id);
    rest += 'd';
    rest += codec::be64_key(delta_id);
    return tkey('S', rest);
}

std::string TikvMetaStore::pack_seal_key(uint64_t pack_id) const {
    std::string rest = codec::be64_key(pack_id);
    rest += 's';
    return tkey('S', rest);
}

std::pair<std::string, std::string> TikvMetaStore::range_of(char tag,
                                                            std::string_view rest) const {
    std::string lo = tkey(tag, rest);
    std::string hi = lo;
    codec::bump_last_byte(hi);  // key 尾字节非 0xff（表标签/复合段构造保证）
    return {std::move(lo), std::move(hi)};
}

// ---------- 事务与读辅助 ----------

// 统一异常翻译（§6.4）：S3Error 透传；Undetermined = 结果不明（§4.6 盲重试禁令）；
// 其余 pingcap 异常 → InternalError
template <typename Fn>
auto TikvMetaStore::guarded(const char* what, Fn&& fn) {
    try {
        return fn();
    } catch (const S3Error&) {
        throw;
    } catch (const TikvUndetermined& u) {
        throw_internal(what, "commit result undetermined: " + u.what);
    } catch (const pingcap::Exception& e) {
        throw_internal(what, e.displayText());
    }
}

// 乐观重试循环（§4.1）：body(ts, muts) 在 start_ts 快照上读算、组 mutation 批；
// muts 空 = 无需提交（纯读判定/幂等提前返回）。WriteConflict / prewrite 锁竞争
// 超预算 = 明确未提交 → 退避重试；其余异常经 guarded 翻译
template <typename Body>
auto TikvMetaStore::txn_retry(const char* what, Body&& body) {
    using R = std::invoke_result_t<Body&, uint64_t, std::vector<TikvMutation>&>;
    return guarded(what, [&]() -> R {
        for (int attempt = 0;; ++attempt) {
            uint64_t ts = client().get_ts();
            std::vector<TikvMutation> muts;
            // 提交成功（或 muts 空的纯读判定）→ true；冲突退避 → false 重试
            auto committed = [&] {
                if (muts.empty()) return true;
                try {
                    client().commit(ts, muts);
                    return true;
                } catch (const TikvConflict& c) {
                    if (attempt + 1 >= kMaxTxnRetries)
                        throw_internal(what, "txn conflict storm: " + c.what);
                    conflict_backoff(attempt);
                    return false;
                }
            };
            if constexpr (std::is_void_v<R>) {
                body(ts, muts);
                if (committed()) return;
            } else {
                R r = body(ts, muts);
                if (committed()) return r;
            }
        }
    });
}

// ---------- 构造 / 关闭 ----------

TikvMetaStore::TikvMetaStore(TikvMetaOptions opt) : opt_(std::move(opt)) {
    client_owned_ = std::make_unique<TikvClient>(
        TikvOptions{opt_.pd_endpoints, opt_.ca_path, opt_.cert_path, opt_.key_path});
    client_.store(client_owned_.get(), std::memory_order_release);
    // schema 谱系校验（§3.2）：Insert 表达"只允许首建"。多网关同前缀首次启动是
    // 受支持的合法竞态——冲突/撞键/结果不明（常量幂等写，重读即可判定）都进
    // 退避重试循环收敛，不能单发即弃（其余写路径由 txn_retry 提供同款循环）
    guarded("open schema", [&] {
        std::string skey = tkey('s', {});
        for (int attempt = 0;; ++attempt) {
            auto ts = client().get_ts();
            if (auto v = snap_get(ts, skey)) {
                if (*v != "t1")
                    throw S3Error(S3ErrorCode::InternalError,
                                  "duostore tikv meta: unsupported schema " + *v);
                return;
            }
            try {
                client().commit(ts, {{TikvOp::kInsert, skey, "t1"}});
                return;
            } catch (const TikvAlreadyExist&) {  // 并发首建已成，下轮读出校验
            } catch (const TikvConflict&) {      // 并发首建进行中
            } catch (const TikvUndetermined&) {  // 写入的是常量，下轮重读判定
            }
            if (attempt + 1 >= kMaxTxnRetries)
                throw S3Error(S3ErrorCode::InternalError,
                              "duostore tikv meta: schema init did not converge");
            conflict_backoff(attempt);
        }
    });
}

TikvMetaStore::~TikvMetaStore() {
    try {
        close();
    } catch (const std::exception& e) {
        LOG_ERROR("duostore tikv meta: close in dtor failed: {}", e.what());
    }
}

void TikvMetaStore::close() {
    // 先摘句柄再析构：close 后调用在 client() 处确定性抛 500（rocks 版同型）；
    // Cluster 析构停后台线程，须在无在途调用后进行（DuoStoreBackend close 顺序保证）
    TikvClient* c = client_.exchange(nullptr, std::memory_order_acq_rel);
    if (!c) return;
    client_owned_.reset();
}

TikvClient& TikvMetaStore::client() {
    TikvClient* c = client_.load(std::memory_order_acquire);
    if (!c) throw S3Error(S3ErrorCode::InternalError, "duostore tikv meta: store is closed");
    return *c;
}

std::optional<std::string> TikvMetaStore::snap_get(uint64_t ver, const std::string& key) {
    // 纯读重试一次（§6.4）：client-c 内部已带退避，这里只兜一次瞬时抖动
    try {
        return client().get(ver, key);
    } catch (const pingcap::Exception& e) {
        LOG_WARN("duostore tikv meta: get retry after: {}", e.displayText());
        return client().get(ver, key);
    }
}

std::vector<std::optional<std::string>> TikvMetaStore::snap_get_many(
    uint64_t ver, const std::vector<std::string>& keys) {
    try {
        return client().batch_get(ver, keys);
    } catch (const pingcap::Exception& e) {
        LOG_WARN("duostore tikv meta: batch_get retry after: {}", e.displayText());
        return client().batch_get(ver, keys);
    }
}

template <typename Fn>
void TikvMetaStore::scan_range(uint64_t ver, std::string lo, const std::string& hi, Fn&& cb) {
    for (;;) {
        auto page = client().scan(ver, lo, hi, kScanPage);
        for (auto& kv : page)
            if (!cb(kv.first, kv.second)) return;
        if (page.size() < kScanPage) return;
        lo = page.back().first + '\0';  // 字节序后继，无缝续页
    }
}

void TikvMetaStore::mut_refs(std::vector<TikvMutation>& muts, const DataRef& ref, bool add,
                             std::string_view owner) {
    for (const auto& e : ref.extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack 存活走 stats 账（P2）
        if (add)
            muts.push_back({TikvOp::kPut, refs_key(e.file_id), std::string(owner)});
        else
            muts.push_back({TikvOp::kDel, refs_key(e.file_id), {}});
    }
}

void TikvMetaStore::mut_pack_delta(std::vector<TikvMutation>& muts, const DataRef& ref,
                                   int sign) {
    // 同 pack 多 extent 先聚合，每 pack 一条唯一 delta 行（id 预派发，入账纯写——
    // 共享账行的读改写会让同 active-pack 的并发小对象 PUT prewrite 冲突，§3.2）
    std::map<uint64_t, std::pair<int64_t, int64_t>> agg;  // pack_id -> (bytes, recs)
    for (const auto& e : ref.extents) {
        if (e.kind != Extent::Kind::kPack) continue;
        auto& [bytes, recs] = agg[e.file_id];
        bytes += sign * int64_t(e.length);
        recs += sign;
    }
    for (const auto& [id, d] : agg) {
        uint64_t delta_id = alloc_id(kCtrPackDelta, pack_deltas_);
        muts.push_back(
            {TikvOp::kPut, pack_delta_key(id, delta_id), encode_pack_delta(d.first, d.second)});
    }
}

void TikvMetaStore::enqueue_reclaim(std::vector<TikvMutation>& muts, const DataRef& ref) {
    if (ref.extents.empty()) return;
    uint64_t seq = alloc_id(kCtrSeq, seqs_);  // 预派发（独立小事务），入账保持纯写
    muts.push_back(
        {TikvOp::kPut, gcq_key(seq), codec::encode_reclaim(Reclaim{ref.extents}, now_ms())});
}

uint64_t TikvMetaStore::alloc_id(char kind, IdRange& r) {
    {
        std::lock_guard lk(alloc_mu_);  // 常见路径：纯内存 next++
        if (r.next < r.limit) return r.next++;
    }
    // 段耗尽：计数器 RMW 小事务（§5）在锁外进行——TSO + 2PC + 冲突重试可达
    // 数十 ms，锁内会把所有 kind 的派发一并串行卡死。并发续段者经 WriteConflict
    // 仲裁各拿到不相交段；落败方整段弃置无害（id 只需唯一单调，不需连续）。
    // raft 多数派持久，无 Redis 版的崩溃回滚补偿
    std::string ck = counter_key(kind);
    uint64_t hi = txn_retry(
        "reserve id segment", [&](uint64_t ts, std::vector<TikvMutation>& muts) -> uint64_t {
            uint64_t cur = 0;
            if (auto v = snap_get(ts, ck)) cur = uint64_t(codec::decode_counter(*v));
            uint64_t next_hi = cur + kIdSegment;
            muts.push_back({TikvOp::kPut, ck, codec::encode_counter_delta(int64_t(next_hi))});
            return next_hi;
        });
    std::lock_guard lk(alloc_mu_);
    if (r.next == r.limit) {  // 他人已续上则用其段，本段弃置（空洞无害，同上）
        r.limit = hi;
        r.next = hi - kIdSegment;
    }
    return r.next++;
}

uint64_t TikvMetaStore::alloc_file_id(Extent::Kind kind) {
    // kRados 与 kChunk 共号段（rocks 版同一论证：refs 按裸 file_id 记账不分 kind）
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCtrChunk : kCtrPack,
                    file_ids_[size_t(kind)]);
}

// ---------- bucket ----------

void TikvMetaStore::create_bucket(std::string_view b) {
    try {
        txn_retry("create_bucket", [&](uint64_t, std::vector<TikvMutation>& muts) {
            // Insert 协议级表达"必须不存在"（§4.4）——无需读，撞键即结构化拒绝
            muts.push_back({TikvOp::kInsert, bucket_key(b), codec::encode_bucket(now_ms())});
        });
    } catch (const TikvAlreadyExist&) {
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(b));
    }
}

void TikvMetaStore::delete_bucket(std::string_view b) {
    txn_retry("delete_bucket", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        // 空检查（同快照）：objects / uploads 任一非空即拒绝（对齐 AWS；uploads
        // 检查同时封死"桶删后 put_part 复活幽灵上传"，rocks 版同一论证）
        for (char tag : {'O', 'U'}) {
            auto [lo, hi] = range_of(tag, std::string(b) + '\0');
            if (!client().scan(ts, lo, hi, 1).empty())
                throw S3Error(S3ErrorCode::BucketNotEmpty,
                              "The bucket you tried to delete is not empty", std::string(b));
        }
        // Del 桶 + 全量守卫分片：与并发 put_object/create_upload 的守卫 Lock
        // 构成写写冲突，物化空检查的写偏斜（§4.3）
        muts.push_back({TikvOp::kDel, bucket_key(b), {}});
        for (uint32_t s = 0; s < kGuardShards; ++s)
            muts.push_back({TikvOp::kLock, bucket_guard(b, s), {}});
    });
}

bool TikvMetaStore::bucket_exists(std::string_view b) {
    return guarded("bucket_exists",
                   [&] { return snap_get(client().get_ts(), bucket_key(b)).has_value(); });
}

std::vector<BucketInfo> TikvMetaStore::list_buckets() {
    return guarded("list_buckets", [&] {
        std::vector<BucketInfo> out;
        auto ts = client().get_ts();
        auto [lo, hi] = range_of('B', {});
        size_t plen = lo.size();  // prefix + 'B'
        scan_range(ts, lo, hi, [&](const std::string& k, const std::string& v) {
            out.push_back({k.substr(plen), codec::from_unix_ms(codec::decode_bucket(v))});
            return true;
        });
        return out;  // key 字节序即字典序
    });
}

// ---------- object ----------

std::optional<ObjectRec> TikvMetaStore::get_object(std::string_view b, std::string_view k) {
    return guarded("get_object", [&]() -> std::optional<ObjectRec> {
        auto v = snap_get(client().get_ts(), object_key(b, k));
        if (!v) return std::nullopt;
        return codec::decode_object(std::string(k), *v);
    });
}

void TikvMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec) {
    txn_retry("put_object", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string okey = object_key(b, k);
        auto vals = snap_get_many(ts, {bucket_key(b), okey});  // 一次往返读全前置
        if (!vals[0]) throw_no_bucket(b);
        std::optional<ObjectRec> old;
        if (vals[1]) old = codec::decode_object(std::string(k), *vals[1]);
        rec.version = old ? old->version + 1 : 1;

        // primary = 对象键（写写冲突的语义焦点）；守卫 Lock 物化桶存在性检查（§4.3）
        muts.push_back({TikvOp::kPut, okey, codec::encode_object(rec)});
        muts.push_back({TikvOp::kLock, bucket_guard(b, uint32_t(fnv1a(k) % kGuardShards)), {}});
        mut_refs(muts, rec.data, /*add=*/true, okey);
        mut_pack_delta(muts, rec.data, +1);
        if (old) {
            enqueue_reclaim(muts, old->data);
            mut_refs(muts, old->data, /*add=*/false, {});
            mut_pack_delta(muts, old->data, -1);
        }
    });
}

bool TikvMetaStore::delete_object(std::string_view b, std::string_view k) {
    return txn_retry("delete_object", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string okey = object_key(b, k);
        auto vals = snap_get_many(ts, {bucket_key(b), okey});
        if (!vals[0]) throw_no_bucket(b);
        if (!vals[1]) return false;  // 幂等：muts 空，不发事务
        auto old = codec::decode_object(std::string(k), *vals[1]);
        muts.push_back({TikvOp::kDel, okey, {}});
        enqueue_reclaim(muts, old.data);
        mut_refs(muts, old.data, /*add=*/false, {});
        mut_pack_delta(muts, old.data, -1);
        return true;
    });
}

// §3.3：Snapshot 一致视图 + 分页扫，算法照搬 rocks 版（主文档 §4.4）；delimiter
// 组末字节 +1 重 seek 跳过整组（每组一次 RTT）。组尾 token 无反向扫原语，延迟到
// 截断恰落组上时对该组做一次前向扫描取尾 key（每次 list 至多一次、组通常远小于桶）
ListResult TikvMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    return guarded("list_objects", [&] {
        ListResult out;
        uint64_t ts = client().get_ts();  // 整次 list 的一致视图（跨分页/跳组）
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        if (opt.max_keys <= 0) return out;  // S3：max-keys=0 空且不截断

        auto [base, upper] = range_of('O', std::string(b) + '\0');
        const std::string& prefix = opt.prefix;
        const std::string& delim = opt.delimiter;

        // 简易游标：页缓冲 + 续页（delimiter 需要随时改 seek 点，回调式不贴合）
        std::vector<std::pair<std::string, std::string>> page;
        size_t idx = 0;
        bool eof = false;
        auto seek = [&](const std::string& from) {
            page = client().scan(ts, from, upper, kScanPage);
            idx = 0;
            eof = page.empty();
        };
        auto advance = [&] {
            if (++idx < page.size()) return;
            if (page.size() < kScanPage) {
                eof = true;
                return;
            }
            seek(page.back().first + '\0');
        };

        seek(base + std::max(prefix, opt.start_after));
        if (!opt.start_after.empty() && !eof && page[idx].first == base + opt.start_after)
            advance();  // start_after 命中自身再走一步

        std::string last_emitted;
        std::string last_group;  // 非空 = 最近一次输出是 delimiter 组
        int count = 0;
        while (!eof) {
            std::string_view uk(page[idx].first);
            uk.remove_prefix(base.size());
            if (uk.compare(0, prefix.size(), prefix) != 0) break;  // 出前缀区间即止
            if (count >= opt.max_keys) {
                out.is_truncated = true;
                if (!last_group.empty()) {
                    // 截断恰落组上：key-only 反向扫取组尾（rocks 版 SeekForPrev 的
                    // 对应物，O(1) RPC，不随组大小放大）
                    std::string glo = base + last_group;
                    std::string ghi = glo;
                    codec::bump_last_byte(ghi);
                    if (auto tail = client().last_key(ts, glo, ghi))
                        last_emitted = tail->substr(base.size());
                }
                out.next_token = last_emitted;
                break;
            }
            if (!delim.empty()) {
                auto pos = uk.find(delim, prefix.size());
                if (pos != std::string_view::npos) {
                    std::string group(uk.substr(0, pos + delim.size()));
                    out.common_prefixes.push_back(group);
                    last_group = group;
                    ++count;
                    std::string target = base + group;
                    if (!codec::bump_last_byte(target)) break;
                    seek(target);  // 跳过整组（同快照，一致性不破）
                    continue;
                }
            }
            out.objects.push_back(codec::decode_object_meta(std::string(uk), page[idx].second));
            last_emitted = std::string(uk);
            last_group.clear();
            ++count;
            advance();
        }
        return out;
    });
}

// ---------- multipart ----------

std::string TikvMetaStore::create_upload(std::string_view b, std::string_view k,
                                         ObjectMeta meta) {
    UploadRec rec;
    rec.upload_id = new_upload_id();
    rec.meta = std::move(meta);
    rec.meta.key = std::string(k);
    rec.initiated_ms = now_ms();
    txn_retry("create_upload", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        muts.push_back(
            {TikvOp::kPut, upload_key(b, k, rec.upload_id), codec::encode_upload(rec)});
        // 守卫：物化 delete_bucket 空检查的写偏斜（§4.3，同 put_object）
        muts.push_back({TikvOp::kLock, bucket_guard(b, uint32_t(fnv1a(k) % kGuardShards)), {}});
    });
    return rec.upload_id;
}

UploadRec TikvMetaStore::require_upload(std::string_view b, std::string_view k,
                                        std::string_view id) {
    return guarded("require_upload", [&] {
        if (!is_valid_upload_id(id)) throw_no_upload(id);
        auto v = snap_get(client().get_ts(), upload_key(b, k, id));
        if (!v) throw_no_upload(id);
        return codec::decode_upload(std::string(k), std::string(id), *v);
    });
}

void TikvMetaStore::put_part(std::string_view b, std::string_view k, std::string_view id,
                             PartRec p) {
    txn_retry("put_part", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!is_valid_upload_id(id)) throw_no_upload(id);
        std::string pkey = part_key(b, k, id, p.part_no);
        auto vals = snap_get_many(ts, {upload_key(b, k, id), pkey});
        if (!vals[0]) throw_no_upload(id);
        std::optional<PartRec> old;
        if (vals[1]) old = codec::decode_part(p.part_no, *vals[1]);

        muts.push_back({TikvOp::kPut, pkey, codec::encode_part(p)});
        // 守卫：物化 complete/abort 删 upload 时的写偏斜（§4.3）；按 part_no 分片，
        // 同 upload 并发传不同号分片互不阻塞（1/16 概率误撞，重试廉价）
        muts.push_back(
            {TikvOp::kLock, upload_guard(b, k, id, uint32_t(p.part_no) % kGuardShards), {}});
        mut_refs(muts, p.data, /*add=*/true, pkey);
        mut_pack_delta(muts, p.data, +1);
        if (old) {  // 同号重传 last-write-wins：旧分片同批入 GC 账
            enqueue_reclaim(muts, old->data);
            mut_refs(muts, old->data, /*add=*/false, {});
            mut_pack_delta(muts, old->data, -1);
        }
    });
}

std::vector<PartRec> TikvMetaStore::scan_parts(uint64_t ver, std::string_view b,
                                               std::string_view k, std::string_view id) {
    std::vector<PartRec> out;
    auto [lo, hi] = range_of('P', codec::parts_prefix(b, k, id));
    size_t plen = opt_.prefix.size() + 1;
    scan_range(ver, lo, hi, [&](const std::string& key, const std::string& v) {
        int no = codec::part_no_of_key(std::string_view(key).substr(plen));
        out.push_back(codec::decode_part(no, v));
        return true;
    });
    return out;  // be16 part_no 保证升序
}

std::vector<PartRec> TikvMetaStore::list_parts(std::string_view b, std::string_view k,
                                               std::string_view id) {
    return guarded("list_parts", [&] {
        uint64_t ts = client().get_ts();
        if (!is_valid_upload_id(id) || !snap_get(ts, upload_key(b, k, id))) throw_no_upload(id);
        return scan_parts(ts, b, k, id);  // 同快照：upload 校验与 parts 读一致
    });
}

std::vector<UploadInfo> TikvMetaStore::list_uploads(std::string_view b) {
    return guarded("list_uploads", [&] {
        uint64_t ts = client().get_ts();
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        std::vector<UploadInfo> out;
        auto [lo, hi] = range_of('U', std::string(b) + '\0');
        size_t plen = lo.size();  // prefix + 'U' + b + '\0'
        scan_range(ts, lo, hi, [&](const std::string& key, const std::string& v) {
            // rest = <key>\0<upload_id>，前缀扫天然按 (key, upload_id) 排序
            std::string_view rest = std::string_view(key).substr(plen);
            auto sep = rest.rfind('\0');
            if (sep == std::string_view::npos) return true;
            auto rec = codec::decode_upload(std::string(rest.substr(0, sep)),
                                            std::string(rest.substr(sep + 1)), v);
            out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
            return true;
        });
        return out;
    });
}

// §8（主文档）：complete 纯元数据事务，零数据搬运。parts 全部进写集（逐个 Del）
// → 与并发同号 put_part 的冲突由 prewrite 天然校验；新号 put_part 由 Ug 守卫物化
std::string TikvMetaStore::complete_upload(std::string_view b, std::string_view k,
                                           std::string_view id,
                                           std::span<const PartInfo> parts) {
    return txn_retry("complete_upload", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!is_valid_upload_id(id)) throw_no_upload(id);
        std::string okey = object_key(b, k);
        auto vals = snap_get_many(ts, {upload_key(b, k, id), okey});
        if (!vals[0]) throw_no_upload(id);
        auto up = codec::decode_upload(std::string(k), std::string(id), *vals[0]);

        std::map<int, PartRec> stored;
        for (auto& p : scan_parts(ts, b, k, id)) stored.emplace(p.part_no, std::move(p));
        std::set<int> selected;
        ObjectRec rec = assemble_completed_object(std::move(up.meta), parts, stored, selected);

        std::optional<ObjectRec> old;
        if (vals[1]) old = codec::decode_object(std::string(k), *vals[1]);
        rec.version = old ? old->version + 1 : 1;

        muts.push_back({TikvOp::kPut, okey, codec::encode_object(rec)});  // primary
        muts.push_back({TikvOp::kDel, upload_key(b, k, id), {}});
        for (uint32_t s = 0; s < kGuardShards; ++s)
            muts.push_back({TikvOp::kLock, upload_guard(b, k, id, s), {}});
        for (const auto& [no, p] : stored) {
            muts.push_back({TikvOp::kDel, part_key(b, k, id, no), {}});
            if (selected.count(no)) {
                // refs 转移：owner 改写为对象；pack 账不动（put_part 已计，改
                // owner 不改存活）
                mut_refs(muts, p.data, /*add=*/true, okey);
            } else {  // 未选中分片入 GC 账
                enqueue_reclaim(muts, p.data);
                mut_refs(muts, p.data, /*add=*/false, {});
                mut_pack_delta(muts, p.data, -1);
            }
        }
        if (old) {  // 旧同名对象入 GC 账
            enqueue_reclaim(muts, old->data);
            mut_refs(muts, old->data, /*add=*/false, {});
            mut_pack_delta(muts, old->data, -1);
        }
        return rec.meta.etag;
    });
}

void TikvMetaStore::abort_upload(std::string_view b, std::string_view k, std::string_view id) {
    txn_retry("abort_upload", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!is_valid_upload_id(id) || !snap_get(ts, upload_key(b, k, id))) throw_no_upload(id);
        muts.push_back({TikvOp::kDel, upload_key(b, k, id), {}});  // primary
        for (uint32_t s = 0; s < kGuardShards; ++s)
            muts.push_back({TikvOp::kLock, upload_guard(b, k, id, s), {}});
        for (const auto& p : scan_parts(ts, b, k, id)) {
            muts.push_back({TikvOp::kDel, part_key(b, k, id, p.part_no), {}});
            enqueue_reclaim(muts, p.data);
            mut_refs(muts, p.data, /*add=*/false, {});
            mut_pack_delta(muts, p.data, -1);
        }
    });
}

// ---------- GC 记账 ----------

std::vector<std::pair<uint64_t, Reclaim>> TikvMetaStore::peek_reclaims(size_t max,
                                                                       uint64_t min_seq) {
    return guarded("peek_reclaims", [&] {
        std::vector<std::pair<uint64_t, Reclaim>> out;
        uint64_t ts = client().get_ts();
        auto [lo, hi] = range_of('G', {});
        (void)lo;  // 起点用 gcq_key(min_seq)：min_seq=0 时即 'G' 段首，等价 lo
        size_t plen = opt_.prefix.size() + 1;
        for (auto& [key, v] : client().scan(ts, gcq_key(min_seq), hi, max)) {
            uint64_t seq = codec::parse_be64(std::string_view(key).substr(plen));
            out.emplace_back(seq, codec::decode_reclaim(v));
        }
        return out;
    });
}

void TikvMetaStore::ack_reclaim(uint64_t seq) {
    txn_retry("ack_reclaim", [&](uint64_t, std::vector<TikvMutation>& muts) {
        muts.push_back({TikvOp::kDel, gcq_key(seq), {}});  // 盲删单 key，无跨 key 不变量
    });
}

void TikvMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    txn_retry("ack_reclaims", [&](uint64_t, std::vector<TikvMutation>& muts) {
        for (uint64_t s : seqs) muts.push_back({TikvOp::kDel, gcq_key(s), {}});
    });
}

std::vector<PackStat> TikvMetaStore::pack_stats() {
    // 'S' 表前缀扫（同 pack 的 delta/seal 行相邻：'d' < 's'），边扫边聚合。
    // 顺带折叠（§3.2 delta 行方案的另一半）：单 pack delta 行超阈值即合并为一行
    // ——GC 低频路径承担合并，业务写路径保持纯写无冲突
    struct Acc {
        PackStat ps;
        std::vector<std::string> delta_keys;  // 折叠候选
    };
    std::vector<Acc> accs;
    guarded("pack_stats", [&] {
        uint64_t ts = client().get_ts();
        auto [lo, hi] = range_of('S', {});
        size_t plen = opt_.prefix.size() + 1;  // prefix + 'S'
        scan_range(ts, lo, hi, [&](const std::string& key, const std::string& v) {
            std::string_view rest = std::string_view(key).substr(plen);
            if (rest.size() < 9) return true;  // 非本店格式，跳过
            uint64_t id = codec::parse_be64(rest.substr(0, 8));
            if (accs.empty() || accs.back().ps.pack_id != id) {
                accs.emplace_back();
                accs.back().ps.pack_id = id;
            }
            Acc& a = accs.back();
            if (rest[8] == 'd') {
                auto [bytes, recs] = decode_pack_delta(v);
                a.ps.live_bytes += bytes;
                a.ps.live_recs += recs;
                a.delta_keys.push_back(key);
            } else if (rest[8] == 's') {
                a.ps.sealed = true;
                a.ps.file_size = uint64_t(codec::decode_counter(v));
            }
            return true;
        });
    });
    for (Acc& a : accs) {
        if (a.delta_keys.size() <= kPackFoldThreshold) continue;
        // 折叠为一行：删除已读到的 delta 行 + 写合并行（新 delta_id）。并发业务
        // 事务只会新增其他 key 的行，不冲突；并发折叠（多网关）经 txn_retry 的
        // 写写冲突仲裁——失败方重读重算，收敛无双计
        try {
            txn_retry("fold pack stats", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
                int64_t bytes = 0, recs = 0;
                for (const auto& dk : a.delta_keys) {
                    auto v = snap_get(ts, dk);
                    if (!v) {  // 他人已折叠：放弃（清 muts 防半程提交丢账）
                        muts.clear();
                        return;
                    }
                    auto [db, dr] = decode_pack_delta(*v);
                    bytes += db;
                    recs += dr;
                    muts.push_back({TikvOp::kDel, dk, {}});
                }
                uint64_t delta_id = alloc_id(kCtrPackDelta, pack_deltas_);
                muts.push_back({TikvOp::kPut, pack_delta_key(a.ps.pack_id, delta_id),
                                encode_pack_delta(bytes, recs)});
            });
        } catch (const std::exception& e) {
            LOG_WARN("duostore tikv meta: pack {} fold skipped: {}", a.ps.pack_id, e.what());
        }
    }
    std::vector<PackStat> out;
    out.reserve(accs.size());
    for (auto& a : accs) out.push_back(a.ps);
    return out;
}

void TikvMetaStore::seal_pack(uint64_t pack_id, uint64_t file_size) {
    txn_retry("seal_pack", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        // 幂等；file_size=0 不覆盖已有记录（IMetaStore 契约）
        std::string skey = pack_seal_key(pack_id);
        if (file_size == 0 && snap_get(ts, skey)) return;  // muts 空，不发事务
        muts.push_back({TikvOp::kPut, skey, codec::encode_counter_delta(int64_t(file_size))});
    });
}

void TikvMetaStore::drop_pack_stat(uint64_t pack_id) {
    txn_retry("drop_pack_stat", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        // 删除该 pack 的全部账行（delta + seal）。前提：live_recs==0 且 pack 已删，
        // 不再有并发追加——快照读到的即全集
        auto [lo, hi] = range_of('S', codec::be64_key(pack_id));
        scan_range(ts, lo, hi, [&](const std::string& key, const std::string&) {
            muts.push_back({TikvOp::kDel, key, {}});
            return true;
        });
    });
}

bool TikvMetaStore::swap_extents(std::string_view b, std::string_view k,
                                 uint64_t expect_version, const DataRef& from,
                                 const DataRef& to) {
    return txn_retry("swap_extents", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string okey = object_key(b, k);
        auto v = snap_get(ts, okey);
        if (!v) return false;
        auto rec = codec::decode_object(std::string(k), *v);
        // 乐观校验：version/extent 不符 = 期间被覆盖/删除 → 放弃（muts 空不发事务）；
        // 校验通过后的"读到即提交"由 prewrite 对 okey 的冲突检测保证（§4.1 天然 CAS）
        if (rec.version != expect_version || rec.data.extents != from.extents) return false;
        rec.data = to;
        rec.version += 1;
        muts.push_back({TikvOp::kPut, okey, codec::encode_object(rec)});
        mut_refs(muts, to, /*add=*/true, okey);
        mut_refs(muts, from, /*add=*/false, {});
        mut_pack_delta(muts, to, +1);  // 压实换 ref：账随 extent 迁移（§9.2）
        mut_pack_delta(muts, from, -1);
        return true;
    });
}

bool TikvMetaStore::chunk_referenced(uint64_t file_id) {
    return guarded("chunk_referenced",
                   [&] { return snap_get(client().get_ts(), refs_key(file_id)).has_value(); });
}

void TikvMetaStore::scan_refs(const std::function<void(uint64_t)>& cb) {
    // 'R' 前缀快照分页扫（孤儿扫描容忍弱一致视图）；key 尾部 = be64 file_id
    guarded("scan_refs", [&] {
        auto [lo, hi] = range_of('R', {});
        const size_t suffix = codec::be64_key(0).size();
        scan_range(client().get_ts(), lo, hi, [&](const std::string& key, const std::string&) {
            if (key.size() >= suffix)
                cb(codec::parse_be64(std::string_view(key).substr(key.size() - suffix)));
            return true;
        });
        return 0;
    });
}

}  // namespace lights3::storage::duostore
