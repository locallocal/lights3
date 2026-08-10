#include "storage/duostore/rocks_meta_store.h"

#include <array>
#include <charconv>

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/meta_util.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// 计数器 key（stats CF，§4.1）：file_id 号段与 gcq seq
constexpr const char* kCounterChunk = "c0";
constexpr const char* kCounterPack = "c1";
constexpr const char* kCounterSeq = "q";

// pack 存活账 key（stats CF，§4.1 的 p<be64 pack_id> 按字段展开为子 key）：
// 'b' = live_bytes（merge 计数）、'r' = live_recs（merge 计数）、
// 's' = 封存标记（plain Put，值 = 8B 小端 file_size；存在即 sealed）。
// 同 pack 的三个 key 前缀相邻，pack_stats() 一次前缀扫聚合
std::string pack_stat_key(uint64_t pack_id, char field) {
    std::string k = "p";
    k += codec::be64_key(pack_id);
    k += field;
    return k;
}

[[noreturn]] void throw_status(const char* what, const rocksdb::Status& s) {
    LOG_ERROR("duostore meta: {}: {}", what, s.ToString());
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore meta: ") + what + ": " + s.ToString());
}

rocksdb::Slice slice(std::string_view s) { return {s.data(), s.size()}; }

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

// stats CF 的计数器 merge：8B 小端 i64 增量累加（§4.5 号段预留、pack 存活账 P2 扩展）
class CounterMerge final : public rocksdb::AssociativeMergeOperator {
public:
    bool Merge(const rocksdb::Slice& /*key*/, const rocksdb::Slice* existing,
               const rocksdb::Slice& value, std::string* new_value,
               rocksdb::Logger* /*logger*/) const override {
        int64_t base = 0;
        if (existing && existing->size() == 8)
            base = codec::decode_counter({existing->data(), existing->size()});
        int64_t d = value.size() == 8 ? codec::decode_counter({value.data(), value.size()}) : 0;
        *new_value = codec::encode_counter_delta(base + d);
        return true;
    }
    const char* Name() const override { return "duostore.counter"; }
};

struct SnapshotGuard {
    rocksdb::DB* db;
    const rocksdb::Snapshot* snap;
    ~SnapshotGuard() {
        if (snap) db->ReleaseSnapshot(snap);
    }
};

// user key（去掉 CF key 里的 "<bucket>\0" 前缀）
std::string_view strip_prefix(const rocksdb::Slice& k, size_t prefix_len) {
    return {k.data() + prefix_len, k.size() - prefix_len};
}

using codec::bump_last_byte;  // delimiter 跳组后继（codec.h，meta store 实现共用）

}  // namespace

RocksMetaStore::RocksMetaStore(RocksMetaOptions opt) : opt_(std::move(opt)) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.max_background_jobs = opt_.max_background_jobs;

    rocksdb::BlockBasedTableOptions table;
    table.block_cache = rocksdb::NewLRUCache(opt_.block_cache_bytes);
    auto table_factory = std::shared_ptr<rocksdb::TableFactory>(
        rocksdb::NewBlockBasedTableFactory(table));

    rocksdb::ColumnFamilyOptions cf_opt;
    cf_opt.table_factory = table_factory;
    cf_opt.compression = rocksdb::kNoCompression;  // 压缩全关（§13.3）
    cf_opt.write_buffer_size = opt_.write_buffer_bytes;
    cf_opt.max_write_buffer_number = opt_.max_write_buffers;
    rocksdb::ColumnFamilyOptions stats_opt = cf_opt;
    stats_opt.merge_operator = std::make_shared<CounterMerge>();

    std::vector<rocksdb::ColumnFamilyDescriptor> descs = {
        {rocksdb::kDefaultColumnFamilyName, cf_opt}, {"buckets", cf_opt},
        {"objects", cf_opt},                         {"uploads", cf_opt},
        {"parts", cf_opt},                           {"refs", cf_opt},
        {"gcq", cf_opt},                             {"stats", stats_opt},
    };
    rocksdb::DB* db = nullptr;
    auto s = rocksdb::DB::Open(options, opt_.path, descs, &cfs_, &db);
    if (!s.ok()) throw_status("open", s);
    db_.store(db, std::memory_order_release);

    // schema 版本校验 + 迁移钩子（§4.1 / docs/gaps.md §6.1）。此前是"必须精确等于
    // 当前常量"的硬校验——任何 value 布局变更都会让存量库无法启动。现在：
    //   存量版本 == 当前 → 直过；
    //   存量版本 < 当前  → 按注册的迁移链逐级升（kSchemaMigrations，当前为空——
    //                       record 级演进走 codec 的 read_ver 兼容读，§5.2；本链
    //                       留给未来的 CF/键布局变更），升完盖新版本号；
    //   存量版本 > 当前  → 库来自更新的程序版本，明确拒绝（降级运行会静默写坏）。
    // Open 成功后任何失败都必须走 close()——构造函数抛出时析构不会运行，否则
    // DB 句柄与 LOCK 文件泄漏
    try {
        auto schema = get_raw(kDefault, "schema");
        if (!schema) {
            rocksdb::WriteBatch batch;
            batch.Put(cfs_[kDefault], "schema", std::to_string(kSchemaCurrent));
            batch.Put(cfs_[kDefault], "instance", new_upload_id());
            commit(batch);
        } else {
            migrate_schema(*schema);
        }
    } catch (...) {
        close();
        throw;
    }

    // meta 引擎可观测（docs/gaps.md §6.1：默认引擎 rocksdb 此前完全无指标；
    // redis/sqlite/tikv 各有 busy/corruption/conflict 计数）。属性 gauge 渲染时
    // 现取；db 关闭后回 0（弱指针语义由 db_ 原子判空承担）
    auto prop = [this](const char* name) -> double {
        auto* d = db_.load(std::memory_order_acquire);
        uint64_t v = 0;
        if (d && d->GetAggregatedIntProperty(name, &v)) return double(v);
        return 0;
    };
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_estimate_num_keys",
                                "RocksDB estimated key count across all column families",
                                [prop] { return prop("rocksdb.estimate-num-keys"); });
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_block_cache_usage_bytes",
                                "RocksDB block cache memory usage",
                                [prop] { return prop("rocksdb.block-cache-usage"); });
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_sst_bytes",
                                "RocksDB total SST file size (meta store on-disk footprint)",
                                [prop] { return prop("rocksdb.total-sst-files-size"); });
    opt_.metrics.gauge_callback("lights3_duostore_rocksdb_memtable_bytes",
                                "RocksDB active+immutable memtable memory",
                                [prop] { return prop("rocksdb.cur-size-all-mem-tables"); });
}

int64_t RocksMetaStore::validate_schema_marker(const std::string& stored) {
    return parse_schema_marker(stored, /*lineage=*/"", kSchemaCurrent, "duostore meta");
}

// 迁移链（版本 n → n+1 的就地变换；持 db 已开）。新增布局变更时在此登记，并把
// kSchemaCurrent +1——绝不允许"改布局不留迁移"
void RocksMetaStore::migrate_schema(const std::string& stored) {
    int64_t ver = validate_schema_marker(stored);
    if (ver == kSchemaCurrent) return;
    using MigrateFn = void (*)(RocksMetaStore&);
    static constexpr std::array<std::pair<int64_t, MigrateFn>, 0> kSchemaMigrations{
        // {{1, &migrate_v1_to_v2}}  // 示例：登记后 v1 库开机自动升
    };
    for (; ver < kSchemaCurrent; ++ver) {
        MigrateFn fn = nullptr;
        for (auto& [from, f] : kSchemaMigrations)
            if (from == ver) fn = f;
        if (!fn) throw_no_migration(ver, kSchemaCurrent, "duostore meta");
        fn(*this);
        rocksdb::WriteBatch batch;
        batch.Put(cfs_[kDefault], "schema", std::to_string(ver + 1));
        commit(batch);
        LOG_INFO("duostore meta: schema migrated v{} -> v{}", ver, ver + 1);
    }
}

RocksMetaStore::~RocksMetaStore() {
    try {
        close();
    } catch (const std::exception& e) {
        LOG_ERROR("duostore meta: close in dtor failed: {}", e.what());
    }
}

void RocksMetaStore::close() {
    std::lock_guard lk(mu_);
    // 先摘 db_：close 返回后的调用在 db() 处干净失败（500），而非解引用悬垂指针
    rocksdb::DB* db = db_.exchange(nullptr, std::memory_order_acq_rel);
    if (!db) return;
    for (auto* h : cfs_) db->DestroyColumnFamilyHandle(h);
    cfs_.clear();
    auto s = db->Close();
    if (!s.ok()) LOG_ERROR("duostore meta: close: {}", s.ToString());
    delete db;
}

// ---------- 基础封装 ----------

rocksdb::DB* RocksMetaStore::db() const {
    auto* d = db_.load(std::memory_order_acquire);
    if (!d)
        throw S3Error(S3ErrorCode::InternalError, "duostore meta: store is closed");
    return d;
}

std::optional<std::string> RocksMetaStore::get_raw(int cf, std::string_view key) {
    auto* d = db();  // 先取句柄再碰 cfs_（close 置空 db_ 先于清理 cfs_）
    std::string v;
    auto s = d->Get(rocksdb::ReadOptions(), cfs_[cf], slice(key), &v);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) throw_status("get", s);
    return v;
}

void RocksMetaStore::commit(rocksdb::WriteBatch& batch) {
    rocksdb::WriteOptions wo;
    wo.sync = opt_.sync;
    auto s = db()->Write(wo, &batch);
    if (!s.ok()) throw_status("commit", s);
}

void RocksMetaStore::require_bucket_locked(std::string_view b) {
    if (!get_raw(kBuckets, b))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(b));
}

void RocksMetaStore::batch_refs(rocksdb::WriteBatch& batch, const DataRef& ref, bool add,
                                std::string_view owner) {
    for (const auto& e : ref.extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack 存活走 stats 账（P2）；
                                                      // chunk/rados 皆按 file_id 入 refs
        if (add)
            batch.Put(cfs_[kRefs], codec::be64_key(e.file_id), slice(owner));
        else
            batch.Delete(cfs_[kRefs], codec::be64_key(e.file_id));
    }
}

void RocksMetaStore::batch_pack_delta(rocksdb::WriteBatch& batch, const DataRef& ref,
                                      int sign, int64_t rec_overhead) {
    // 同 pack 多 extent 先聚合，每 pack 两次 merge（§9.1：随业务事务同批增减）；
    // 每条 record 计 payload + 头开销，与 file_size 同口径（docs/gaps.md §2.3a）
    std::map<uint64_t, std::pair<int64_t, int64_t>> agg;  // pack_id -> (bytes, recs)
    for (const auto& e : ref.extents) {
        if (e.kind != Extent::Kind::kPack) continue;
        auto& [bytes, recs] = agg[e.file_id];
        bytes += sign * (int64_t(e.length) + rec_overhead);
        recs += sign;
    }
    for (const auto& [id, d] : agg) {
        batch.Merge(cfs_[kStats], pack_stat_key(id, 'b'), codec::encode_counter_delta(d.first));
        batch.Merge(cfs_[kStats], pack_stat_key(id, 'r'), codec::encode_counter_delta(d.second));
    }
}

void RocksMetaStore::enqueue_reclaim_locked(rocksdb::WriteBatch& batch, const DataRef& ref,
                                            ReclaimReason reason) {
    if (ref.extents.empty()) return;
    // 超大 DataRef（TB 级对象 = 数十万 extent）拆成多条 gcq 项（docs/gaps.md §2.11）：
    // GC 单批的解码驻留内存有界；ack 逐条独立，拆分不改变崩溃语义（unlink 幂等）
    const int64_t ts = now_ms();
    for (size_t i = 0; i < ref.extents.size(); i += kReclaimMaxExtents) {
        size_t n = std::min(kReclaimMaxExtents, ref.extents.size() - i);
        Reclaim r;
        r.extents.assign(ref.extents.begin() + i, ref.extents.begin() + i + n);
        r.reason = reason;
        uint64_t seq = alloc_id(kCounterSeq, seqs_);
        batch.Put(cfs_[kGcq], codec::be64_key(seq), codec::encode_reclaim(r, ts));
    }
}

uint64_t RocksMetaStore::alloc_id(std::string_view counter_key, IdRange& r, uint32_t n) {
    n = std::clamp<uint32_t>(n, 1, kMaxIdRun);  // run ≤ kMaxIdRun << kIdSegment
    std::lock_guard lk(alloc_mu_);  // 独立于 mu_：常见路径是纯内存 next += n
    if (r.limit - r.next < n) {
        // 号段预留必须先于派发持久化，且恒 WAL fsync（独立于 meta_sync）——
        // 否则崩溃丢预留后重启重发已用 file_id，与已落盘的 chunk 文件
        // O_EXCL 冲突（§6.3 的"仍自洽"依赖这里恒 sync）。崩溃浪费号段无害；
        // 换段弃置的残段同理（run 批派发要求段内连续，docs/gaps.md §3.9）
        rocksdb::WriteBatch batch;
        batch.Merge(cfs_[kStats], slice(counter_key),
                    codec::encode_counter_delta(int64_t(kIdSegment)));
        rocksdb::WriteOptions wo;
        wo.sync = true;
        auto s = db()->Write(wo, &batch);
        if (!s.ok()) throw_status("reserve id segment", s);
        auto v = get_raw(kStats, counter_key);
        if (!v) throw S3Error(S3ErrorCode::InternalError, "duostore meta: counter vanished");
        uint64_t hi = uint64_t(codec::decode_counter(*v));
        r.limit = hi;
        r.next = hi - kIdSegment;
    }
    uint64_t first = r.next;
    r.next += n;
    return first;
}

uint64_t RocksMetaStore::alloc_file_run(Extent::Kind kind, uint32_t n) {
    // kRados 与 kChunk 共号段：refs 表按裸 file_id 记账不分 kind，独立计数器会在
    // 同一 meta 上切换 data 引擎（fs↔rados）时产生跨 kind id 碰撞
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCounterChunk : kCounterPack,
                    file_ids_[size_t(kind)], n);
}

// ---------- bucket ----------

void RocksMetaStore::create_bucket(std::string_view b) {
    std::lock_guard lk(mu_);
    if (get_raw(kBuckets, b))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(b));
    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kBuckets], slice(b), codec::encode_bucket(now_ms()));
    commit(batch);
}

void RocksMetaStore::delete_bucket(std::string_view b) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    // 空检查：objects 或 uploads 任一非空即拒绝（AWS 对有进行中 MPU 的桶同样
    // 拒绝删除）。uploads 检查同时封死"桶删后 put_part 继续写入、refs 永久
    // 泄漏、重建桶复活幽灵上传"整类问题——upload 存在则桶删不掉，put_part
    // 的 require_upload 就足以保证桶还在
    std::string prefix = std::string(b) + '\0';
    auto* d = db();
    for (int cf : {int(kObjects), int(kUploads)}) {
        auto it = std::unique_ptr<rocksdb::Iterator>(
            d->NewIterator(rocksdb::ReadOptions(), cfs_[cf]));
        it->Seek(prefix);
        if (it->Valid() && it->key().starts_with(slice(prefix)))
            throw S3Error(S3ErrorCode::BucketNotEmpty,
                          "The bucket you tried to delete is not empty", std::string(b));
        if (!it->status().ok()) throw_status("delete_bucket scan", it->status());
    }
    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kBuckets], slice(b));
    commit(batch);
}

bool RocksMetaStore::bucket_exists(std::string_view b) { return get_raw(kBuckets, b).has_value(); }

std::vector<BucketInfo> RocksMetaStore::list_buckets() {
    std::vector<BucketInfo> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kBuckets]));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        int64_t created = codec::decode_bucket({it->value().data(), it->value().size()});
        out.push_back({it->key().ToString(), codec::from_unix_ms(created)});
    }
    if (!it->status().ok()) throw_status("list_buckets", it->status());
    return out;  // key 字节序即字典序
}

// ---------- object ----------

std::optional<ObjectRec> RocksMetaStore::get_object(std::string_view b, std::string_view k) {
    auto v = get_raw(kObjects, codec::object_key(b, k));
    if (!v) return std::nullopt;
    return codec::decode_object(std::string(k), *v);
}

std::optional<ObjectMeta> RocksMetaStore::head_object(std::string_view b, std::string_view k) {
    auto v = get_raw(kObjects, codec::object_key(b, k));
    if (!v) return std::nullopt;
    return codec::decode_object_meta(std::string(k), *v);
}

void RocksMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec,
                                PutCondition cond) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    std::string okey = codec::object_key(b, k);
    std::optional<ObjectRec> old;
    if (auto oldv = get_raw(kObjects, okey)) old = codec::decode_object(std::string(k), *oldv);
    check_put_condition(cond, old, k);  // 与提交同锁（PutCondition 契约）
    rec.version = old ? old->version + 1 : 1;

    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kObjects], okey, codec::encode_object(rec));
    batch_refs(batch, rec.data, /*add=*/true, okey);
    const int64_t ov = codec::pack_rec_overhead(b, k);
    batch_pack_delta(batch, rec.data, +1, ov);
    if (old) {
        enqueue_reclaim_locked(batch, old->data, ReclaimReason::kOverwrite);
        batch_refs(batch, old->data, /*add=*/false, {});
        batch_pack_delta(batch, old->data, -1, ov);
    }
    commit(batch);
}

bool RocksMetaStore::delete_object(std::string_view b, std::string_view k) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    std::string okey = codec::object_key(b, k);
    auto oldv = get_raw(kObjects, okey);
    if (!oldv) return false;
    auto old = codec::decode_object(std::string(k), *oldv);

    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kObjects], okey);
    enqueue_reclaim_locked(batch, old.data, ReclaimReason::kDelete);
    batch_refs(batch, old.data, /*add=*/false, {});
    batch_pack_delta(batch, old.data, -1, codec::pack_rec_overhead(b, k));
    commit(batch);
    return true;
}

// §4.4：objects CF 原生有序迭代，delimiter 组用 Seek 跳过整组
ListResult RocksMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    require_bucket_locked(b);  // 纯读，锁外 get 幂等安全
    ListResult out;
    // S3：max-keys=0 返回空且 IsTruncated=false（与 apply_listing 一致）
    if (opt.max_keys <= 0) return out;
    std::string base = std::string(b) + '\0';
    std::string upper = std::string(b) + '\x01';  // bucket 名无 NUL，'\0'+1 即桶区间上界

    rocksdb::ReadOptions ro;
    auto* d = db();
    SnapshotGuard snap{d, d->GetSnapshot()};
    ro.snapshot = snap.snap;
    rocksdb::Slice upper_slice(upper);
    ro.iterate_upper_bound = &upper_slice;
    auto it = std::unique_ptr<rocksdb::Iterator>(d->NewIterator(ro, cfs_[kObjects]));

    const std::string& prefix = opt.prefix;
    const std::string& delim = opt.delimiter;
    std::string seek = base + std::max(prefix, opt.start_after);
    it->Seek(seek);
    if (!opt.start_after.empty() && it->Valid() &&
        it->key() == slice(base + opt.start_after))
        it->Next();  // start_after 命中自身再走一步

    std::string last_emitted;
    int count = 0;
    while (it->Valid()) {
        std::string_view uk = strip_prefix(it->key(), base.size());
        if (uk.compare(0, prefix.size(), prefix) != 0) break;  // 出前缀区间即止
        if (count >= opt.max_keys) {
            out.is_truncated = true;
            out.next_token = last_emitted;
            break;
        }
        if (!delim.empty()) {
            auto pos = uk.find(delim, prefix.size());
            if (pos != std::string_view::npos) {
                std::string group(uk.substr(0, pos + delim.size()));
                out.common_prefixes.push_back(group);
                ++count;
                // Seek 跳过整组（相对 localfs 目录遍历的实质优势，§4.4）；
                // token 语义须落在组尾 → Prev 取组内最后一个 key。
                // 不变量：Seek 落在 !Valid（组是桶内最后一段）时 last_emitted
                // 保持旧值，但此时循环必然不经截断分支退出，旧值不会被读到
                std::string target = base + group;
                if (!bump_last_byte(target)) break;
                it->Seek(target);
                if (it->Valid()) {
                    it->Prev();
                    last_emitted = std::string(strip_prefix(it->key(), base.size()));
                    it->Next();
                }
                continue;
            }
        }
        out.objects.push_back(codec::decode_object_meta(
            std::string(uk), {it->value().data(), it->value().size()}));
        last_emitted = std::string(uk);
        ++count;
        it->Next();
    }
    if (!it->status().ok()) throw_status("list_objects", it->status());
    return out;
}

// ---------- multipart ----------

std::string RocksMetaStore::create_upload(std::string_view b, std::string_view k,
                                          ObjectMeta meta) {
    std::lock_guard lk(mu_);
    require_bucket_locked(b);
    UploadRec rec;
    rec.upload_id = new_upload_id();
    rec.meta = std::move(meta);
    rec.meta.key = std::string(k);
    rec.initiated_ms = now_ms();
    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kUploads], codec::upload_key(b, k, rec.upload_id),
              codec::encode_upload(rec));
    commit(batch);
    return rec.upload_id;
}

UploadRec RocksMetaStore::require_upload(std::string_view b, std::string_view k,
                                         std::string_view id) {
    auto missing = [&]() -> S3Error {
        return {S3ErrorCode::NoSuchUpload, "The specified multipart upload does not exist.",
                std::string(id)};
    };
    if (!is_valid_upload_id(id)) throw missing();
    auto v = get_raw(kUploads, codec::upload_key(b, k, id));
    if (!v) throw missing();
    return codec::decode_upload(std::string(k), std::string(id), *v);
}

void RocksMetaStore::put_part(std::string_view b, std::string_view k, std::string_view id,
                              PartRec p) {
    std::lock_guard lk(mu_);
    require_upload(b, k, id);
    std::string pkey = codec::part_key(b, k, id, p.part_no);
    std::optional<PartRec> old;
    if (auto oldv = get_raw(kParts, pkey)) old = codec::decode_part(p.part_no, *oldv);

    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kParts], pkey, codec::encode_part(p));
    batch_refs(batch, p.data, /*add=*/true, pkey);
    const int64_t ov = codec::pack_rec_overhead_part(b, k, id, p.part_no);
    batch_pack_delta(batch, p.data, +1, ov);
    if (old) {  // 同号重传 last-write-wins：旧分片同批入 GC 账
        enqueue_reclaim_locked(batch, old->data, ReclaimReason::kPartOverwrite);
        batch_refs(batch, old->data, /*add=*/false, {});
        batch_pack_delta(batch, old->data, -1, ov);
    }
    commit(batch);
}

std::vector<PartRec> RocksMetaStore::scan_parts(std::string_view b, std::string_view k,
                                                std::string_view id) {
    std::string prefix = codec::parts_prefix(b, k, id);
    std::vector<PartRec> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kParts]));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(slice(prefix)); it->Next()) {
        int no = codec::part_no_of_key({it->key().data(), it->key().size()});
        out.push_back(codec::decode_part(no, {it->value().data(), it->value().size()}));
    }
    if (!it->status().ok()) throw_status("scan parts", it->status());
    return out;  // be16 part_no 保证升序（§4.1）
}

std::vector<PartRec> RocksMetaStore::list_parts(std::string_view b, std::string_view k,
                                                std::string_view id) {
    require_upload(b, k, id);
    return scan_parts(b, k, id);
}

std::vector<UploadInfo> RocksMetaStore::list_uploads(std::string_view b,
                                                    std::string_view key_marker,
                                                    std::string_view id_marker, int limit) {
    require_bucket_locked(b);  // 纯读，锁外 get 幂等安全
    std::string prefix = std::string(b) + '\0';
    // 游标下推（docs/gaps.md §5.1）：key 编码本身就是 (key, upload_id) 序，
    // seek 到 marker 之后即可，跳过的部分一条也不读
    std::string seek = prefix;
    if (!key_marker.empty() || !id_marker.empty()) {
        seek += std::string(key_marker);
        seek += '\0';
        seek += std::string(id_marker);
        seek += '\0';  // 尾部 '\0' 使 seek 落在该 (key,id) 之后
    }
    std::vector<UploadInfo> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kUploads]));
    for (it->Seek(seek); it->Valid() && it->key().starts_with(slice(prefix)); it->Next()) {
        if (limit > 0 && out.size() >= size_t(limit)) break;
        // key = <bucket>\0<key>\0<upload_id>，前缀扫天然按 (key, upload_id) 排序
        std::string_view rest = strip_prefix(it->key(), prefix.size());
        auto sep = rest.rfind('\0');
        if (sep == std::string_view::npos) continue;
        auto rec = codec::decode_upload(std::string(rest.substr(0, sep)),
                                        std::string(rest.substr(sep + 1)),
                                        {it->value().data(), it->value().size()});
        out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
    }
    if (!it->status().ok()) throw_status("list_uploads", it->status());
    return out;
}

// §8：complete 是纯元数据事务，零数据搬运——按序拼接各 part 的 extent runs
std::string RocksMetaStore::complete_upload(std::string_view b, std::string_view k,
                                            std::string_view id,
                                            std::span<const PartInfo> parts) {
    std::lock_guard lk(mu_);
    auto up = require_upload(b, k, id);
    require_bucket_locked(b);

    std::map<int, PartRec> stored;
    for (auto& p : scan_parts(b, k, id)) stored.emplace(p.part_no, std::move(p));

    std::set<int> selected;
    ObjectRec rec = assemble_completed_object(std::move(up.meta), parts, stored, selected);

    std::string okey = codec::object_key(b, k);
    std::optional<ObjectRec> old;
    if (auto oldv = get_raw(kObjects, okey)) old = codec::decode_object(std::string(k), *oldv);
    rec.version = old ? old->version + 1 : 1;

    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kObjects], okey, codec::encode_object(rec));
    batch.Delete(cfs_[kUploads], codec::upload_key(b, k, id));
    // 整段 DeleteRange 清 parts（免逐 part 重建 key 串、万分片时批次膨胀）；
    // gcq/refs 记账仍需逐 part 的 extents
    std::string pfx = codec::parts_prefix(b, k, id);
    std::string pfx_end = pfx;
    bump_last_byte(pfx_end);
    batch.DeleteRange(cfs_[kParts], pfx, pfx_end);
    for (const auto& [no, p] : stored) {
        if (selected.count(no)) {
            // refs 转移：owner 改写为对象。pack 账存活不变，但口径从分片重平衡为
            // 对象（-分片头开销 +对象头开销，recs 一减一增相抵）：保证后续对象
            // 删除按对象口径扣减后账精确归零
            batch_refs(batch, p.data, /*add=*/true, okey);
            batch_pack_delta(batch, p.data, -1,
                             codec::pack_rec_overhead_part(b, k, id, no));
            batch_pack_delta(batch, p.data, +1, codec::pack_rec_overhead(b, k));
        } else {  // 未选中分片入 GC 账
            enqueue_reclaim_locked(batch, p.data, ReclaimReason::kComplete);
            batch_refs(batch, p.data, /*add=*/false, {});
            batch_pack_delta(batch, p.data, -1,
                             codec::pack_rec_overhead_part(b, k, id, no));
        }
    }
    if (old) {  // 旧同名对象入 GC 账
        enqueue_reclaim_locked(batch, old->data, ReclaimReason::kOverwrite);
        batch_refs(batch, old->data, /*add=*/false, {});
        batch_pack_delta(batch, old->data, -1, codec::pack_rec_overhead(b, k));
    }
    commit(batch);
    return rec.meta.etag;
}

void RocksMetaStore::abort_upload(std::string_view b, std::string_view k,
                                  std::string_view id) {
    std::lock_guard lk(mu_);
    require_upload(b, k, id);
    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kUploads], codec::upload_key(b, k, id));
    std::string pfx = codec::parts_prefix(b, k, id);
    std::string pfx_end = pfx;
    bump_last_byte(pfx_end);
    batch.DeleteRange(cfs_[kParts], pfx, pfx_end);
    for (const auto& p : scan_parts(b, k, id)) {
        enqueue_reclaim_locked(batch, p.data, ReclaimReason::kAbort);
        batch_refs(batch, p.data, /*add=*/false, {});
        batch_pack_delta(batch, p.data, -1,
                         codec::pack_rec_overhead_part(b, k, id, p.part_no));
    }
    commit(batch);
}

// ---------- GC 记账 ----------

std::vector<std::pair<uint64_t, Reclaim>> RocksMetaStore::peek_reclaims(size_t max,
                                                                        uint64_t min_seq,
                                                                        size_t max_extents) {
    std::vector<std::pair<uint64_t, Reclaim>> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kGcq]));
    std::string start = codec::be64_key(min_seq);
    size_t extents = 0;
    for (it->Seek(start); it->Valid() && out.size() < max; it->Next()) {
        uint64_t seq = codec::parse_be64({it->key().data(), it->key().size()});
        out.emplace_back(seq,
                         codec::decode_reclaim({it->value().data(), it->value().size()}));
        // 累计 extent 上限（gaps §2.11）：至少返回 1 项（拆分前遗留的超大单项）
        extents += out.back().second.extents.size();
        if (extents >= max_extents) break;
    }
    if (!it->status().ok()) throw_status("peek_reclaims", it->status());
    return out;
}

void RocksMetaStore::ack_reclaim(uint64_t seq) {
    // 盲删单 key，无跨 key 不变量——不占 mu_，GC 销账不排队业务提交的 fsync
    rocksdb::WriteBatch batch;
    batch.Delete(cfs_[kGcq], codec::be64_key(seq));
    commit(batch);
}

void RocksMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    rocksdb::WriteBatch batch;  // 单批单提交（覆写接口默认的逐条转发）
    for (uint64_t s : seqs) batch.Delete(cfs_[kGcq], codec::be64_key(s));
    commit(batch);
}

std::vector<PackStat> RocksMetaStore::pack_stats() {
    // stats CF 前缀扫 'p'：同 pack 的 b/r/s 子 key 相邻，边扫边聚合。返回含
    // live=0 与未封存项（空 pack 整删与重启补封依赖能看见它们）
    std::vector<PackStat> out;
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kStats]));
    for (it->Seek("p"); it->Valid() && it->key().starts_with("p"); it->Next()) {
        std::string_view k(it->key().data(), it->key().size());
        if (k.size() != 10) continue;  // 'p' + be64 + field
        uint64_t id = codec::parse_be64(k.substr(1, 8));
        std::string_view v(it->value().data(), it->value().size());
        if (out.empty() || out.back().pack_id != id) out.push_back({.pack_id = id});
        switch (k[9]) {
            case 'b': out.back().live_bytes = codec::decode_counter(v); break;
            case 'r': out.back().live_recs = codec::decode_counter(v); break;
            case 's':
                out.back().sealed = true;
                out.back().file_size = uint64_t(codec::decode_counter(v));
                break;
            default: break;
        }
    }
    if (!it->status().ok()) throw_status("pack_stats", it->status());
    return out;
}

void RocksMetaStore::seal_pack(uint64_t pack_id, uint64_t file_size) {
    std::lock_guard lk(mu_);  // 读改写（保留已记录 size），与并发 seal 序列化
    std::string skey = pack_stat_key(pack_id, 's');
    if (file_size == 0 && get_raw(kStats, skey)) return;  // 幂等：0 不覆盖已知 size
    rocksdb::WriteBatch batch;
    batch.Put(cfs_[kStats], skey, codec::encode_counter_delta(int64_t(file_size)));
    commit(batch);
}

void RocksMetaStore::drop_pack_stat(uint64_t pack_id) {
    // 盲删三个子 key，无跨 key 不变量——同 ack_reclaim 不占 mu_
    rocksdb::WriteBatch batch;
    for (char f : {'b', 'r', 's'}) batch.Delete(cfs_[kStats], pack_stat_key(pack_id, f));
    commit(batch);
}

bool RocksMetaStore::stage_swap_locked(rocksdb::WriteBatch& batch, std::string_view b,
                                       std::string_view k, uint64_t expect_version,
                                       const DataRef& from, const DataRef& to) {
    std::string okey = codec::object_key(b, k);
    auto v = get_raw(kObjects, okey);
    if (!v) return false;
    auto rec = codec::decode_object(std::string(k), *v);
    // 乐观校验：version 或 extent 不符 = 期间被覆盖/删除 → 放弃（§9.2）
    if (rec.version != expect_version || rec.data.extents != from.extents) return false;
    rec.data = to;
    rec.version += 1;
    batch.Put(cfs_[kObjects], okey, codec::encode_object(rec));
    // refs 按差集操作：to/from 共享未迁移的 chunk，整加再整删在 last-wins 下
    // 净效果是删除，会抹掉活数据的 refs（meta_util.h refs_delta 详述）
    auto rd = refs_delta(from, to);
    batch_refs(batch, rd.added, /*add=*/true, okey);
    batch_refs(batch, rd.removed, /*add=*/false, {});
    // 压实换 ref：账随 extent 迁移（§9.2）；两侧都按对象口径（迁出的旧 record 若
    // 是 mpu 形态则轻微低扣，保守方向——见 codec.h pack_rec_overhead 注释）
    const int64_t ov = codec::pack_rec_overhead(b, k);
    batch_pack_delta(batch, to, +1, ov);
    batch_pack_delta(batch, from, -1, ov);
    return true;
}

bool RocksMetaStore::swap_extents(std::string_view b, std::string_view k,
                                  uint64_t expect_version, const DataRef& from,
                                  const DataRef& to) {
    std::lock_guard lk(mu_);
    rocksdb::WriteBatch batch;
    if (!stage_swap_locked(batch, b, k, expect_version, from, to)) return false;
    commit(batch);
    return true;
}

std::vector<bool> RocksMetaStore::swap_extents_batch(std::span<const SwapReq> reqs) {
    // 压实批量化（gaps §2.13）：整批一次 WriteBatch 提交。逐项 CAS 独立——失败项
    // 只是不入批，不殃及其余
    std::lock_guard lk(mu_);
    std::vector<bool> out;
    out.reserve(reqs.size());
    rocksdb::WriteBatch batch;
    bool any = false;
    for (const auto& r : reqs) {
        bool ok = stage_swap_locked(batch, r.bucket, r.key, r.expect_version, r.from, r.to);
        any = any || ok;
        out.push_back(ok);
    }
    if (any) commit(batch);
    return out;
}

bool RocksMetaStore::chunk_referenced(uint64_t file_id) {
    return get_raw(kRefs, codec::be64_key(file_id)).has_value();
}

void RocksMetaStore::scan_refs(const std::function<void(uint64_t)>& cb) {
    // 纯读走迭代器快照（§4.4 同款），不占 mu_；孤儿扫描容忍弱一致视图
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db()->NewIterator(rocksdb::ReadOptions(), cfs_[kRefs]));
    for (it->SeekToFirst(); it->Valid(); it->Next())
        cb(codec::parse_be64({it->key().data(), it->key().size()}));
    if (!it->status().ok()) throw_status("scan_refs", it->status());
}

}  // namespace lights3::storage::duostore
