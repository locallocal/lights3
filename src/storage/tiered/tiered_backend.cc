#include "storage/tiered/tiered_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cstdio>
#include <stdexcept>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/time.h"
#include "storage/multipart.h"

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using fsutil::Tier;
using fsutil::TierInfo;

namespace {

constexpr int64_t kAtimeSnapshotSec = 300;  // atime 快照周期（docs/08 §4.3）

// 只读 sidecar 的 tier 字段（数据文件可有可无；PUT/DELETE 前查旧云副本用）
TierInfo read_tier_only(const fs::path& data_path) {
    TierInfo t;
    for (auto& [k, v] : fsutil::read_tsv(data_path.string() + fsutil::kSidecarSuffix)) {
        if (k == "tier")
            t.tier = v == "remote" ? Tier::kRemote : v == "cached" ? Tier::kCached : Tier::kLocal;
        else if (k == "remote.etag") t.remote_etag = v;
        else if (k == "remote.at") t.remote_at = v;
    }
    return t;
}

// 下沉上传用：FdStreamReader 之上叠加同步 MD5 与字节计数（docs/08 §5.2 ③ 校验）
class HashingFdReader final : public http::BodyReader {
public:
    HashingFdReader(int fd, uint64_t size, std::shared_ptr<ThreadPool> pool)
        : inner_(fd, 0, size, std::move(pool)) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_.read(buf);
        if (n > 0) {
            md5_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
            bytes_ += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_.length(); }

    uint64_t bytes() const { return bytes_; }
    std::string md5_hex() { return md5_.final_hex(); }

private:
    fsutil::FdStreamReader inner_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    uint64_t bytes_ = 0;
};

// 自销毁的顶层协程：驱动一个后台 Task 并在结束时回调（异常只记日志）
struct Detached {
    struct promise_type {
        Detached get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // 协程体内已全捕获
    };
};

Detached run_detached(Task<void> t, std::function<void()> done) {
    try {
        co_await std::move(t);
    } catch (const std::exception& e) {
        LOG_WARN("tiered: background task failed: {}", e.what());
    } catch (...) {
        LOG_WARN("tiered: background task failed (unknown exception)");
    }
    done();
}

}  // namespace

// 在途表项的 RAII 释放（demote/promote 用；Tee 回填由 reader 析构释放）
struct InflightRelease {
    TieredBackend* owner;
    std::string ikey;
    ~InflightRelease() { owner->inflight_end(ikey); }
};

// Tee 透传 + 边下边缓存（docs/08 §6.2）：云端流照常返给客户端，同时写
// staging tmp 并增量算 MD5；EOF 校验通过则提交为 cached。写盘失败静默降级
// 纯透传；客户端断连时析构，TmpFile RAII 丢弃半截缓存。
class TeeCacheReader final : public http::BodyReader {
public:
    TeeCacheReader(std::shared_ptr<TieredBackend> owner, std::string bucket, std::string key,
                   ObjectMeta meta, TierInfo tier, std::unique_ptr<http::BodyReader> src,
                   std::unique_ptr<fsutil::TmpFile> tmp)
        : owner_(std::move(owner)), bucket_(std::move(bucket)), key_(std::move(key)),
          meta_(std::move(meta)), tier_(std::move(tier)), src_(std::move(src)),
          tmp_(std::move(tmp)) {}

    ~TeeCacheReader() override { release_inflight(); }  // 断连中途析构的兜底

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await src_->read(buf);
        if (finished_) co_return n;
        if (n > 0) {
            if (!degraded_) {
                md5_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
                const char* p = reinterpret_cast<const char*>(buf.data());
                size_t left = n;
                while (left > 0) {
                    ssize_t w = ::write(tmp_->fd, p, left);
                    if (w < 0) {  // ENOSPC 等：降级纯透传，客户端无感知
                        LOG_WARN("tiered: cache fill write failed for {}/{}, passthrough only",
                                 bucket_, key_);
                        degraded_ = true;
                        break;
                    }
                    p += w;
                    left -= static_cast<size_t>(w);
                }
                written_ += n;
            }
        } else {
            finished_ = true;
            if (!degraded_ && written_ == meta_.size) {
                ::close(tmp_->fd);
                tmp_->fd = -1;
                // 单段对象比对 MD5；multipart 对象只能校验字节数（etag 为 -N 形式）
                bool multipart = meta_.etag.find('-') != std::string::npos;
                if (multipart || md5_.final_hex() == meta_.etag)
                    co_await owner_->commit_cache_fill(bucket_, key_, meta_, tier_, *tmp_);
                else
                    LOG_WARN("tiered: cloud data checksum mismatch for {}/{}, cache dropped",
                             bucket_, key_);
            }
            // EOF 即释放 single-flight：reader 可能被响应链长期持有，
            // 不能让它继续挡住同 key 的下沉/回迁
            release_inflight();
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return meta_.size; }

private:
    void release_inflight() {
        if (released_) return;
        released_ = true;
        owner_->inflight_end(TieredBackend::make_ikey(bucket_, key_));
    }

    std::shared_ptr<TieredBackend> owner_;
    std::string bucket_, key_;
    ObjectMeta meta_;   // 本地 sidecar 快照（对外 meta 恒等原则）
    TierInfo tier_;     // 期望的 remote 版本，提交前复核
    std::unique_ptr<http::BodyReader> src_;
    std::unique_ptr<fsutil::TmpFile> tmp_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    uint64_t written_ = 0;
    bool degraded_ = false, finished_ = false, released_ = false;
};

// ---------- 构造 / 配置 ----------

TieredBackend::TieredBackend(std::shared_ptr<LocalFsBackend> local,
                             std::shared_ptr<IStorageBackend> cloud,
                             std::shared_ptr<ThreadPool> pool, TieredConfig cfg)
    : local_(std::move(local)), cloud_(std::move(cloud)), pool_(std::move(pool)), cfg_(cfg),
      tier_dir_(local_->staging() / "tier"), gc_dir_(tier_dir_ / "gc"),
      transfers_(std::max(1, cfg.max_concurrent_transfers)) {
    fs::create_directories(gc_dir_);
    key_locks_.reserve(kLockStripes);
    for (size_t i = 0; i < kLockStripes; ++i)
        key_locks_.push_back(std::make_unique<AsyncSemaphore>(1));

    // GC 序号接续既有条目，重启后不回绕
    uint64_t next_seq = 0;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(gc_dir_, ec)) {
        std::string name = e.path().filename().string();
        uint64_t v = 0;
        std::from_chars(name.data(), name.data() + name.size(), v);
        next_seq = std::max(next_seq, v + 1);
    }
    gc_seq_ = next_seq;

    load_atime_snapshot();
    schedule_scan();
    schedule_snapshot();
}

TieredBackend::~TieredBackend() {
    {
        std::lock_guard lk(bg_m_);
        closing_ = true;
        if (scan_timer_armed_) TimerQueue::instance().cancel(scan_timer_);
        if (snap_timer_armed_) TimerQueue::instance().cancel(snap_timer_);
    }
    // 后台协程持有裸 this，必须等它们归零后才能析构
    std::unique_lock lk(bg_m_);
    bg_cv_.wait(lk, [&] { return bg_count_ == 0; });
}

std::shared_ptr<TieredBackend> TieredBackend::from_config(
    const BackendConfig& cfg, const std::map<std::string, std::shared_ptr<IStorageBackend>>& built,
    std::shared_ptr<ThreadPool> pool) {
    auto param = [&](const char* k) -> std::string {
        auto it = cfg.params.find(k);
        return it == cfg.params.end() ? std::string{} : it->second;
    };
    auto parse_pct = [&](const std::string& s) {
        std::string t = s;
        if (!t.empty() && t.back() == '%') t.pop_back();
        double v = std::stod(t);
        return v > 1.0 ? v / 100.0 : v;  // "85%"/"85" 与 "0.85" 均接受
    };

    std::string local_name = param("local"), cloud_name = param("cloud");
    if (local_name.empty() || cloud_name.empty())
        throw std::runtime_error("tiered backend '" + cfg.name + "' needs local + cloud");
    auto local = std::dynamic_pointer_cast<LocalFsBackend>(built.at(local_name));
    if (!local)
        throw std::runtime_error("tiered backend '" + cfg.name + "': local '" + local_name +
                                 "' must be a localfs/xlocalfs backend (docs/08 §2)");
    auto cloud = built.at(cloud_name);
    if (cloud.get() == local.get())
        throw std::runtime_error("tiered backend '" + cfg.name + "': local and cloud must differ");

    TieredConfig tc;
    if (auto v = param("cold_after"); !v.empty()) tc.cold_after_sec = parse_duration_sec(v);
    if (auto v = param("scan_interval"); !v.empty()) tc.scan_interval_sec = parse_duration_sec(v);
    if (auto v = param("space_high_watermark"); !v.empty()) tc.space_high_watermark = parse_pct(v);
    if (auto v = param("space_low_watermark"); !v.empty()) tc.space_low_watermark = parse_pct(v);
    if (auto v = param("min_free_bytes"); !v.empty()) tc.min_free_bytes = parse_size(v);
    if (auto v = param("cache_fill_on_range"); !v.empty())
        tc.cache_fill_on_range = !(v == "false" || v == "0" || v == "off");
    if (auto v = param("max_concurrent_transfers"); !v.empty())
        tc.max_concurrent_transfers = std::stoi(v);
    if (auto v = param("quota_bytes"); !v.empty()) tc.quota_bytes = parse_size(v);
    if (tc.space_low_watermark > tc.space_high_watermark)
        throw std::runtime_error("tiered backend '" + cfg.name + "': low watermark > high");
    return std::make_shared<TieredBackend>(std::move(local), std::move(cloud), std::move(pool), tc);
}

// ---------- bucket：委托 local ----------

Task<void> TieredBackend::create_bucket(std::string_view bucket) {
    co_return co_await local_->create_bucket(bucket);
}
Task<void> TieredBackend::delete_bucket(std::string_view bucket) {
    // local 为空即无对象（stub 也是对象）；云端可能残留待 GC 的孤儿副本，由对账收敛
    co_return co_await local_->delete_bucket(bucket);
}
Task<bool> TieredBackend::bucket_exists(std::string_view bucket) {
    co_return co_await local_->bucket_exists(bucket);
}
Task<std::vector<BucketInfo>> TieredBackend::list_buckets() {
    co_return co_await local_->list_buckets();
}

// ---------- object ----------

Task<ObjectStream> TieredBackend::get_object(std::string_view bucket, std::string_view key,
                                             std::optional<ByteRange> range) {
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    for (int attempt = 0;; ++attempt) {
        co_await pool_->schedule();
        fs::path path = local_->object_data_path(bucket, key);
        TierInfo t;
        ObjectMeta m;
        bool have_meta = true;
        try {
            m = fsutil::load_object_meta(path, std::string(key), &t);
        } catch (const S3Error&) {
            have_meta = false;  // 不存在：交给 local 区分 NoSuchBucket/NoSuchKey
        }
        if (have_meta) touch_atime(bucket, key);

        if (!have_meta || t.tier != Tier::kRemote) {
            try {
                co_return co_await local_->get_object(bucket, key, range);
            } catch (const fsutil::StubRace&) {
                if (attempt >= 2) throw;
                continue;  // open 与 stub 化竞态：重读 tier 后改走云端
            }
        }

        // remote：云端流透传（docs/08 §6.2/§6.3），对外 meta 恒为本地原始值
        ObjectStream cs = co_await cloud_->get_object(bucket, key, range);
        ObjectStream out;
        out.meta = m;
        out.range = cs.range;
        if (range) {
            out.body = std::move(cs.body);  // Range 不做部分缓存（§6.3）
            if (cfg_.cache_fill_on_range)
                spawn_background(promote_quiet(std::string(bucket), std::string(key)));
            co_return out;
        }
        std::string ikey = make_ikey(bucket, key);
        if (m.size > 0 && cache_space_ok(m.size) && inflight_try_begin(ikey)) {
            auto tmp = std::make_unique<fsutil::TmpFile>();
            tmp->path = local_->staging() / "put" / fsutil::next_tmp_name();
            tmp->fd = ::open(tmp->path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (tmp->fd < 0) {  // 打不开 tmp：纯透传兜底（需求 3）
                inflight_end(ikey);
                out.body = std::move(cs.body);
            } else {
                out.body = std::make_unique<TeeCacheReader>(shared_from_this(),
                                                            std::string(bucket), std::string(key),
                                                            m, t, std::move(cs.body),
                                                            std::move(tmp));
            }
        } else {
            out.body = std::move(cs.body);  // 空间不足或已有回填在途：纯透传
        }
        co_return out;
    }
}

Task<PutResult> TieredBackend::put_object(std::string_view bucket, std::string_view key,
                                          ObjectMeta meta, http::BodyReader& body) {
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    co_await pool_->schedule();
    TierInfo prior = read_tier_only(local_->object_data_path(bucket, key));
    auto r = co_await local_->put_object(bucket, key, std::move(meta), body);
    touch_atime(bucket, key);
    // write-back：新数据只落本地，tier 回到 local；旧云副本成孤儿（§7.1）
    if (prior.tier != Tier::kLocal) enqueue_gc(bucket, key, prior.remote_etag);
    co_return r;
}

Task<ObjectMeta> TieredBackend::head_object(std::string_view bucket, std::string_view key) {
    // stub 的 sidecar 信息完备，HEAD 完全本地完成（§6.1）
    auto m = co_await local_->head_object(bucket, key);
    touch_atime(bucket, key);
    co_return m;
}

Task<void> TieredBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    fsutil::reject_reserved_key(key);
    co_await pool_->schedule();
    TierInfo prior = read_tier_only(local_->object_data_path(bucket, key));
    co_await local_->delete_object(bucket, key);
    erase_atime(bucket, key);
    // 响应不等云端：云副本入 GC 异步删除（§7.2）
    if (prior.tier != Tier::kLocal) enqueue_gc(bucket, key, prior.remote_etag);
    co_return;
}

Task<ListResult> TieredBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    co_return co_await local_->list_objects(bucket, opt);  // stub 即 0 长度文件，遍历原样复用
}

// ---------- multipart：委托 local ----------

Task<std::string> TieredBackend::create_multipart(std::string_view bucket, std::string_view key,
                                                  ObjectMeta meta) {
    co_return co_await local_->create_multipart(bucket, key, std::move(meta));
}
Task<PutResult> TieredBackend::upload_part(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id, int part_no,
                                           http::BodyReader& body) {
    co_return co_await local_->upload_part(bucket, key, upload_id, part_no, body);
}
Task<PutResult> TieredBackend::complete_multipart(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  std::span<const PartInfo> parts) {
    validate_object_key(key);
    co_await pool_->schedule();
    TierInfo prior = read_tier_only(local_->object_data_path(bucket, key));
    auto r = co_await local_->complete_multipart(bucket, key, upload_id, parts);
    touch_atime(bucket, key);
    if (prior.tier != Tier::kLocal) enqueue_gc(bucket, key, prior.remote_etag);  // 同 PUT 覆盖
    co_return r;
}
Task<void> TieredBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                          std::string_view upload_id) {
    co_return co_await local_->abort_multipart(bucket, key, upload_id);
}
Task<std::vector<PartMeta>> TieredBackend::list_parts(std::string_view bucket,
                                                      std::string_view key,
                                                      std::string_view upload_id) {
    co_return co_await local_->list_parts(bucket, key, upload_id);
}
Task<std::vector<UploadInfo>> TieredBackend::list_multipart_uploads(std::string_view bucket) {
    co_return co_await local_->list_multipart_uploads(bucket);
}

// ---------- 下沉（docs/08 §5）----------

Task<void> TieredBackend::demote_object(std::string bucket, std::string key) {
    auto permit = co_await transfers_.acquire();  // max_concurrent_transfers 限流
    co_await pool_->schedule();
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t0;
    ObjectMeta m0 = fsutil::load_object_meta(path, key, &t0);

    if (t0.tier == Tier::kRemote) {
        // 崩溃恢复（§5.2 b/c 之间）：sidecar 已 remote 但数据文件未回收 → 补做 stub 化
        struct stat st{};
        if (::stat(path.c_str(), &st) == 0 && st.st_size > 0) {
            auto lk = co_await key_lock(bucket, key).acquire();
            TierInfo t1;
            ObjectMeta m1;
            try {
                m1 = fsutil::load_object_meta(path, key, &t1);
            } catch (const S3Error&) {
                co_return;
            }
            struct stat st1{};
            if (t1.tier == Tier::kRemote && ::stat(path.c_str(), &st1) == 0 && st1.st_size > 0)
                fsutil::commit_stub(path, m1, t1, local_->staging() / "put");
        }
        co_return;
    }

    std::string ikey = make_ikey(bucket, key);
    if (!inflight_try_begin(ikey)) co_return;  // 已有下沉/回填在途（也挡住 GC 误删）
    InflightRelease rel{this, ikey};

    std::string remote_etag, remote_at;
    if (t0.tier == Tier::kCached) {
        // cached → remote：云副本仍有效则零流量 stub 化（§5.2 末）
        try {
            auto cm = co_await cloud_->head_object(bucket, key);
            if (std::string(strip_etag_quotes(cm.etag)) == t0.remote_etag) {
                remote_etag = t0.remote_etag;
                remote_at = t0.remote_at;
            }
        } catch (const S3Error&) {
            // 云副本失效/不可达：走完整上传
        }
    }

    if (remote_etag.empty()) {
        co_await ensure_cloud_bucket(bucket);
        // ① fd 快照 + ② 流式上传（不占额外内存），同步重算 MD5 用于 ③ 校验
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) fsutil::throw_errno("open for demote");
        HashingFdReader body(fd, m0.size, pool_);  // fd 所有权移交
        ObjectMeta cloud_meta = m0;
        // 冗余一份原始 meta 到云端，本地 stub 丢失时可对账重建（§4.2/§9）
        cloud_meta.user_meta["lights3-etag"] = m0.etag;
        cloud_meta.user_meta["lights3-content-type"] = m0.content_type;
        PutResult pr = co_await cloud_->put_object(bucket, key, std::move(cloud_meta), body);

        std::string md5 = body.md5_hex();
        std::string cloud_etag(strip_etag_quotes(pr.etag));
        bool multipart = m0.etag.find('-') != std::string::npos;
        bool ok = body.bytes() == m0.size && (multipart || md5 == m0.etag);
        // 云端返回的是单段 etag（纯 MD5）时可再对一遍传输完整性
        if (ok && cloud_etag.find('-') == std::string::npos && cloud_etag != md5) ok = false;
        if (!ok) {  // 上传期间对象被覆盖或云端校验失败：云副本入 GC，本轮放弃
            enqueue_gc(bucket, key, cloud_etag);
            co_return;
        }
        remote_etag = cloud_etag;
        remote_at = util::iso8601(std::chrono::system_clock::now());
    }

    // ④ per-key 锁内提交：复核未被并发写打败后 stub 化
    auto lk = co_await key_lock(bucket, key).acquire();
    TierInfo t1;
    ObjectMeta m1;
    try {
        m1 = fsutil::load_object_meta(path, key, &t1);
    } catch (const S3Error&) {  // 期间被 DELETE：DELETE 胜（§7.3）
        enqueue_gc(bucket, key, remote_etag);
        co_return;
    }
    if (m1.etag != m0.etag) {  // 期间被 PUT 覆盖：PUT 胜（§7.3）
        enqueue_gc(bucket, key, remote_etag);
        co_return;
    }
    if (t1.tier == Tier::kRemote) co_return;  // 已由他人 stub 化
    fsutil::commit_stub(path, m1, TierInfo{Tier::kRemote, remote_etag, remote_at},
                        local_->staging() / "put");
    co_return;
}

// ---------- 回迁（Range GET 的后台整对象回迁 + 测试钩子，docs/08 §6.3）----------

Task<void> TieredBackend::promote_object(std::string bucket, std::string key) {
    auto permit = co_await transfers_.acquire();
    co_await pool_->schedule();
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t0;
    ObjectMeta m0;
    try {
        m0 = fsutil::load_object_meta(path, key, &t0);
    } catch (const S3Error&) {
        co_return;
    }
    if (t0.tier != Tier::kRemote) co_return;
    if (m0.size > 0 && !cache_space_ok(m0.size)) co_return;  // 空间不足：放弃（需求 3）
    std::string ikey = make_ikey(bucket, key);
    if (!inflight_try_begin(ikey)) co_return;  // single-flight（§6.4）
    InflightRelease rel{this, ikey};

    ObjectStream cs = co_await cloud_->get_object(bucket, key, std::nullopt);
    fsutil::TmpFile tmp{local_->staging() / "put" / fsutil::next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) fsutil::throw_errno("open promote tmp");

    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t written = 0;
    std::vector<std::byte> buf(256 * 1024);
    for (;;) {
        size_t n = co_await cs.body->read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
        const char* p = reinterpret_cast<const char*>(buf.data());
        size_t left = n;
        while (left > 0) {
            ssize_t w = ::write(tmp.fd, p, left);
            if (w < 0) co_return;  // ENOSPC 等：TmpFile RAII 丢弃
            p += w;
            left -= static_cast<size_t>(w);
        }
        written += n;
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    bool multipart = m0.etag.find('-') != std::string::npos;
    if (written != m0.size || (!multipart && md5.final_hex() != m0.etag)) {
        LOG_WARN("tiered: promote checksum mismatch for {}/{}, dropped", bucket, key);
        co_return;
    }
    co_await commit_cache_fill(bucket, key, m0, t0, tmp);
    co_return;
}

Task<void> TieredBackend::commit_cache_fill(std::string bucket, std::string key,
                                            ObjectMeta expect, TierInfo expect_tier,
                                            fsutil::TmpFile& tmp) {
    auto lk = co_await key_lock(bucket, key).acquire();
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t1;
    ObjectMeta m1;
    try {
        m1 = fsutil::load_object_meta(path, key, &t1);
    } catch (const S3Error&) {
        co_return;  // 期间被 DELETE：丢弃 tmp（§7.3）
    }
    // 复核仍是同一 remote 版本；被 PUT 覆盖/已回填则丢弃（用户写胜出）
    if (t1.tier != Tier::kRemote || m1.etag != expect.etag ||
        t1.remote_etag != expect_tier.remote_etag)
        co_return;
    fsutil::commit_cached(path, tmp, m1, TierInfo{Tier::kCached, t1.remote_etag, t1.remote_at},
                          local_->staging() / "put");
    co_return;
}

// ---------- TierScanner（docs/08 §5.1）----------

Task<void> TieredBackend::scan_once() {
    co_await pool_->schedule();
    struct Cand {
        std::string bucket, key;
        Tier tier;
        uint64_t disk_size;
        int64_t atime;
    };
    std::vector<Cand> objs;
    uint64_t local_bytes = 0;  // 数据文件实际占用（quota 账）
    std::error_code ec;
    for (auto& be : fs::directory_iterator(local_->root(), ec)) {
        if (!be.is_directory() || !fs::exists(be.path() / fsutil::kBucketMarker)) continue;
        std::string bucket = be.path().filename().string();
        for (auto it = fs::recursive_directory_iterator(be.path(), ec);
             it != fs::recursive_directory_iterator(); ++it) {
            if (!it->is_regular_file()) continue;
            std::string name = it->path().filename().string();
            if (name == fsutil::kBucketMarker || name.ends_with(fsutil::kSidecarSuffix)) continue;
            std::string key = fs::relative(it->path(), be.path()).generic_string();
            TierInfo t;
            try {
                fsutil::load_object_meta(it->path(), key, &t);
            } catch (const S3Error&) {
                continue;  // 与并发删除竞态
            }
            struct stat st{};
            if (::stat(it->path().c_str(), &st) != 0) continue;
            local_bytes += static_cast<uint64_t>(st.st_size);
            objs.push_back({bucket, key, t.tier, static_cast<uint64_t>(st.st_size),
                            atime_or(make_ikey(bucket, key), int64_t(st.st_mtime))});
        }
    }

    int64_t now = ::time(nullptr);
    std::set<std::string> chosen;
    std::vector<Task<void>> work;
    auto pick = [&](const Cand& o) {
        if (chosen.insert(make_ikey(o.bucket, o.key)).second)
            work.push_back(demote_quiet(o.bucket, o.key));
    };

    // 崩溃恢复：remote 但数据文件未回收（demote_object 内补做 stub 化）
    for (auto& o : objs)
        if (o.tier == Tier::kRemote && o.disk_size > 0) pick(o);

    // 触发 1：判冷
    for (auto& o : objs)
        if (o.tier != Tier::kRemote && now - o.atime >= cfg_.cold_after_sec) pick(o);

    // 触发 2：空间水位（statvfs 为准，可选 quota 叠加）
    uint64_t need = 0;
    struct statvfs sv{};
    if (::statvfs(local_->root().c_str(), &sv) == 0 && sv.f_blocks > 0) {
        double used = 1.0 - double(sv.f_bavail) / double(sv.f_blocks);
        if (used > cfg_.space_high_watermark)
            need = uint64_t((used - cfg_.space_low_watermark) * double(sv.f_blocks) *
                            double(sv.f_frsize));
    }
    if (cfg_.quota_bytes > 0 &&
        double(local_bytes) > cfg_.space_high_watermark * double(cfg_.quota_bytes)) {
        uint64_t target = uint64_t(cfg_.space_low_watermark * double(cfg_.quota_bytes));
        need = std::max(need, local_bytes - target);
    }
    if (need > 0) {
        // 先 cached（stub 化零成本）再 local（需上传），各按 atime 升序（§5.1）
        std::vector<const Cand*> evict;
        for (auto& o : objs)
            if (o.tier != Tier::kRemote && o.disk_size > 0) evict.push_back(&o);
        std::sort(evict.begin(), evict.end(), [](const Cand* a, const Cand* b) {
            int ra = a->tier == Tier::kCached ? 0 : 1, rb = b->tier == Tier::kCached ? 0 : 1;
            return ra != rb ? ra < rb : a->atime < b->atime;
        });
        for (auto* o : evict) {
            if (need == 0) break;
            pick(*o);
            need -= std::min(need, o->disk_size);
        }
    }

    if (!work.empty()) co_await when_all(std::move(work));
    save_atime_snapshot();
    co_return;
}

Task<void> TieredBackend::demote_quiet(std::string bucket, std::string key) {
    try {
        co_await demote_object(bucket, key);
    } catch (const std::exception& e) {
        // 单对象失败只跳过，退避到下轮 scanner 重试（§5.2 ②）
        LOG_WARN("tiered: demote {}/{} failed: {}", bucket, key, e.what());
    }
}

Task<void> TieredBackend::promote_quiet(std::string bucket, std::string key) {
    try {
        co_await promote_object(bucket, key);
    } catch (const std::exception& e) {
        LOG_WARN("tiered: promote {}/{} failed: {}", bucket, key, e.what());
    }
}

// ---------- GC 队列（docs/08 §7.2）----------

void TieredBackend::enqueue_gc(std::string_view bucket, std::string_view key,
                               std::string_view remote_etag) {
    if (remote_etag.empty()) return;
    char name[32];
    std::snprintf(name, sizeof(name), "%020llu",
                  static_cast<unsigned long long>(gc_seq_.fetch_add(1)));
    try {
        fsutil::write_tsv(gc_dir_ / name, local_->staging() / "put",
                          {{"bucket", std::string(bucket)},
                           {"key", std::string(key)},
                           {"etag", std::string(remote_etag)}});
    } catch (const std::exception& e) {
        // 入队失败的代价只是一个待对账的云端孤儿（§9），不影响正确性
        LOG_WARN("tiered: enqueue gc for {}/{} failed: {}", bucket, key, e.what());
    }
}

Task<void> TieredBackend::run_gc_once() {
    co_await pool_->schedule();
    std::vector<fs::path> entries;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(gc_dir_, ec))
        if (e.is_regular_file()) entries.push_back(e.path());
    std::sort(entries.begin(), entries.end());

    for (auto& p : entries) {
        std::string bucket, key, etag;
        for (auto& [k, v] : fsutil::read_tsv(p)) {
            if (k == "bucket") bucket = v;
            else if (k == "key") key = v;
            else if (k == "etag") etag = v;
        }
        if (bucket.empty() || key.empty() || etag.empty()) {
            fs::remove(p, ec);  // 损坏条目
            continue;
        }
        if (inflight_contains(make_ikey(bucket, key))) continue;  // 下沉在途，下轮再看

        auto lk = co_await key_lock(bucket, key).acquire();
        // 本地 sidecar 仍引用该云副本 → 活引用（例如同内容重新下沉），条目作废
        TierInfo cur = read_tier_only(local_->object_data_path(bucket, key));
        if (cur.tier != Tier::kLocal && cur.remote_etag == etag) {
            fs::remove(p, ec);
            continue;
        }
        try {
            auto cm = co_await cloud_->head_object(bucket, key);
            // 只删 etag 吻合的孤儿；不符说明云端已是新副本，条目直接作废
            if (std::string(strip_etag_quotes(cm.etag)) == etag)
                co_await cloud_->delete_object(bucket, key);
            fs::remove(p, ec);
        } catch (const S3Error& e) {
            if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket)
                fs::remove(p, ec);  // 云端本就没有
            // 其他错误（云端不可达）：保留条目，下轮重试直至成功
        }
    }
    co_return;
}

// ---------- 杂项 ----------

Task<void> TieredBackend::ensure_cloud_bucket(std::string_view bucket) {
    if (co_await cloud_->bucket_exists(bucket)) co_return;
    try {
        co_await cloud_->create_bucket(bucket);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;  // 并发创建视为成功
    }
    co_return;
}

bool TieredBackend::cache_space_ok(uint64_t size) const {
    struct statvfs sv{};
    if (::statvfs(local_->root().c_str(), &sv) != 0) return false;
    uint64_t avail = uint64_t(sv.f_bavail) * sv.f_frsize;
    return avail > size + cfg_.min_free_bytes;
}

AsyncSemaphore& TieredBackend::key_lock(std::string_view bucket, std::string_view key) {
    size_t h = std::hash<std::string>{}(make_ikey(bucket, key));
    return *key_locks_[h % kLockStripes];
}

bool TieredBackend::inflight_try_begin(const std::string& ikey) {
    std::lock_guard lk(inflight_m_);
    return inflight_.insert(ikey).second;
}
void TieredBackend::inflight_end(const std::string& ikey) {
    std::lock_guard lk(inflight_m_);
    inflight_.erase(ikey);
}
bool TieredBackend::inflight_contains(const std::string& ikey) {
    std::lock_guard lk(inflight_m_);
    return inflight_.count(ikey) > 0;
}

// ---------- TierIndex：atime 表（docs/08 §4.3）----------

void TieredBackend::touch_atime(std::string_view bucket, std::string_view key) {
    std::lock_guard lk(atime_m_);
    atime_[make_ikey(bucket, key)] = ::time(nullptr);
}
void TieredBackend::erase_atime(std::string_view bucket, std::string_view key) {
    std::lock_guard lk(atime_m_);
    atime_.erase(make_ikey(bucket, key));
}
int64_t TieredBackend::atime_or(const std::string& ikey, int64_t fallback) {
    std::lock_guard lk(atime_m_);
    auto it = atime_.find(ikey);
    return it == atime_.end() ? fallback : it->second;
}

void TieredBackend::load_atime_snapshot() {
    for (auto& [k, v] : fsutil::read_tsv(tier_dir_ / "atime.tsv")) {
        int64_t t = 0;
        std::from_chars(v.data(), v.data() + v.size(), t);
        if (t > 0) atime_[k] = t;
    }
}

void TieredBackend::save_atime_snapshot() {
    std::vector<std::pair<std::string, std::string>> kv;
    {
        std::lock_guard lk(atime_m_);
        kv.reserve(atime_.size());
        for (auto& [k, v] : atime_)
            if (k.find('\t') == std::string::npos && k.find('\n') == std::string::npos)
                kv.emplace_back(k, std::to_string(v));
    }
    try {
        fsutil::write_tsv(tier_dir_ / "atime.tsv", local_->staging() / "put", kv);
    } catch (const std::exception& e) {
        // 快照失败只影响下次启动的判冷精度（mtime 兜底），可容忍
        LOG_WARN("tiered: atime snapshot failed: {}", e.what());
    }
}

// ---------- 后台任务管理 ----------

void TieredBackend::spawn_background(Task<void> t) {
    {
        std::lock_guard lk(bg_m_);
        if (closing_) return;
        ++bg_count_;
    }
    run_detached(std::move(t), [this] { on_background_done(); });
}

void TieredBackend::on_background_done() {
    std::lock_guard lk(bg_m_);
    --bg_count_;
    bg_cv_.notify_all();
}

void TieredBackend::schedule_scan() {
    std::lock_guard lk(bg_m_);
    if (closing_ || cfg_.scan_interval_sec <= 0) return;
    scan_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.scan_interval_sec),
                                             [this] {
                                                 spawn_background(scan_and_gc());
                                                 schedule_scan();
                                             });
    scan_timer_armed_ = true;
}

void TieredBackend::schedule_snapshot() {
    std::lock_guard lk(bg_m_);
    if (closing_ || cfg_.scan_interval_sec <= 0) return;
    snap_timer_ = TimerQueue::instance().add(std::chrono::seconds(kAtimeSnapshotSec), [this] {
        spawn_background(snapshot_task());
        schedule_snapshot();
    });
    snap_timer_armed_ = true;
}

Task<void> TieredBackend::snapshot_task() {
    co_await pool_->schedule();
    save_atime_snapshot();
}

Task<void> TieredBackend::scan_and_gc() {
    co_await run_gc_once();
    co_await scan_once();
}

Task<void> TieredBackend::close() {
    {
        std::lock_guard lk(bg_m_);
        closing_ = true;
        if (scan_timer_armed_) TimerQueue::instance().cancel(scan_timer_);
        if (snap_timer_armed_) TimerQueue::instance().cancel(snap_timer_);
    }
    {
        // 阻塞等待在调用方线程上进行；后台任务在池线程收尾，不会互相占用
        std::unique_lock lk(bg_m_);
        bg_cv_.wait(lk, [&] { return bg_count_ == 0; });
    }
    save_atime_snapshot();
    co_return;
}

}  // namespace lights3::storage
