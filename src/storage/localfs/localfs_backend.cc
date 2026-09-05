#include "storage/localfs/localfs_backend.h"

#include "core/fault.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <system_error>
#include <tuple>

#include "core/log.h"
#include "core/task.h"
#include "core/util/crypto.h"
#include "storage/listing.h"
#include "storage/multipart.h"
#include "storage/scrub_throttle.h"

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

// On-disk primitives live in fs_util, shared with xlocalfs
using fsutil::TmpFile;
using fsutil::commit_object_file;
using fsutil::load_manifest;
using fsutil::next_tmp_name;
using fsutil::part_file_name;
using fsutil::read_tsv;
using fsutil::reject_reserved_key;
using fsutil::require_upload;
using fsutil::throw_errno;
using fsutil::write_tsv;

LocalFsBackend::LocalFsBackend(fs::path root, fs::path staging, std::shared_ptr<ThreadPool> pool,
                               LocalFsOptions opt, MetricsScope metrics)
    : root_(std::move(root)), staging_(std::move(staging)), pool_(std::move(pool)), opt_(opt) {
    fs::create_directories(root_);
    fs::create_directories(staging_ / "put");
    fs::create_directories(staging_ / "mpu");
    init_metrics(metrics);
    commit_locks_.reserve(kLockStripes);
    for (size_t i = 0; i < kLockStripes; ++i)
        commit_locks_.push_back(std::make_unique<AsyncSemaphore>(1));
    // xattr capability probe (roadmap §3.5): staging shares root's filesystem (rename
    // atomicity), so a probe there answers for the whole layout. A negative result is
    // either fatal (require_xattr) or made visible on the metrics plane right away --
    // previously the only trace was a WARN at the first failing PUT
    xattr_.required = opt_.require_xattr;
    if (int err = fsutil::probe_meta_xattr(staging_ / "put"); err != 0) {
        if (opt_.require_xattr)
            throw std::runtime_error(
                std::string("localfs: filesystem under ") + root_.string() +
                " cannot store the metadata xattr (" + strerror(err) +
                ") and require_xattr is set");
        LOG_WARN("localfs: filesystem under {} has no usable xattr support ({}); object "
                 "metadata runs sidecar-only (two-rename consistency model), see "
                 "lights3_localfs_xattr_fallback",
                 root_.string(), strerror(err));
        xattr_.note_failure();
    }
    cleanup_stale_uploads();
    schedule_periodic(mpu_timer_, opt_.mpu_ttl_sec > 0 ? opt_.mpu_scan_interval_sec : 0,
                      &LocalFsBackend::mpu_scan_task);
    schedule_periodic(sidecar_timer_, opt_.sidecar_scan_interval_sec,
                      &LocalFsBackend::sidecar_sweep_task);
}

void LocalFsBackend::init_metrics(const MetricsScope& metrics) {
    // Full pre-registration across the op dimension (same reasoning as duostore's reason
    // buckets): a missing series reads as "no data" rather than "zero" in Prometheus,
    // making "errors suddenly vanished" look identical to "never had errors".
    // An empty scope returns detached instances (non-null pointers), so the hot path never
    // checks for null
    static constexpr std::array<const char*, kOpCount> kOpNames = {
        "put", "get", "head", "delete", "list", "copy", "upload_part", "complete_mpu"};
    for (size_t i = 0; i < kOpCount; ++i) {
        m_ops_[i] = metrics.counter("lights3_localfs_ops_total",
                                    "Data-path operations finished (success and failure)",
                                    {{"op", kOpNames[i]}});
        m_op_errors_[i] = metrics.counter(
            "lights3_localfs_op_errors_total",
            "Data-path operations that exited via an error (any exception, incl. client 4xx)",
            {{"op", kOpNames[i]}});
    }
    // Latency is measured only for put/get/list: they cover the three cost shapes "disk
    // write, disk read, directory walk", enough to localize disk degradation; the other
    // ops' latency mirrors one of the three, and a histogram for each would only bloat
    // /-/metrics
    for (Op op : {Op::kPut, Op::kGet, Op::kList})
        m_op_seconds_[size_t(op)] = metrics.histogram(
            "lights3_localfs_op_seconds", "Wall time of a data-path operation",
            {0.001, 0.005, 0.02, 0.1, 0.5, 2, 10}, {{"op", kOpNames[size_t(op)]}});
    // roadmap §3.5: xattr degradation as a resident gauge (same rationale as
    // lights3_xlocalfs_uring_fallback -- a startup WARN vanishes with log rotation, a gauge
    // stays on the dashboard), plus the sweep and directory-cache counters
    xattr_.fallback = metrics.gauge(
        "lights3_localfs_xattr_fallback",
        "1 = metadata xattr unavailable, objects rely on the sidecar (two-rename model)");
    xattr_.failures = metrics.counter("lights3_localfs_xattr_write_failures_total",
                                      "Object metadata xattr writes that failed");
    m_orphans_removed_ = metrics.counter("lights3_localfs_orphan_sidecars_removed_total",
                                         "Orphan sidecar files removed (listing + sweep)");
    dir_cache_ = std::make_unique<fsutil::DirListCache>(
        fsutil::DirListCache::Options{opt_.list_cache_entries, opt_.list_cache_min_dir_entries},
        metrics.counter("lights3_localfs_list_dir_cache_total",
                        "Listing directory-snapshot cache lookups", {{"result", "hit"}}),
        metrics.counter("lights3_localfs_list_dir_cache_total",
                        "Listing directory-snapshot cache lookups", {{"result", "miss"}}),
        metrics.gauge("lights3_localfs_list_dir_cache_entries",
                      "Directory entries resident in the listing cache"));
    // Object metadata cache (roadmap §3.8); metric family shared across backends
    // (lights3_meta_cache_*, distinguished by the backend label)
    meta_cache_ = std::make_unique<FsMetaCache>(
        MetaCacheOptions{opt_.meta_cache_entries,
                         std::chrono::seconds(std::max(0, opt_.meta_cache_ttl_sec))},
        metrics);
}

void LocalFsBackend::record_op(Op op, double secs, bool ok) {
    size_t i = size_t(op);
    m_ops_[i]->inc();
    if (!ok) m_op_errors_[i]->inc();
    if (m_op_seconds_[i]) m_op_seconds_[i]->observe(secs);
}

LocalFsBackend::~LocalFsBackend() {
    if (!closed_) shutdown_background();
}

Task<void> LocalFsBackend::close() {
    if (closed_.exchange(true)) co_return;
    shutdown_background();
    co_return;
}

// Periodic maintenance (docs/archive/gaps.md §6.3 for the mpu scan, roadmap §3.5 for the
// sidecar sweep): the mpu cleanup previously ran only once at startup, so a gateway running
// for months without a restart would accumulate never-completed/aborted upload directories
// without bound. Each task re-arms itself after completion (same as the duostore worker):
// runs never overlap/pile up, a slow run just pushes back the next trigger
void LocalFsBackend::schedule_periodic(TimerQueue::Id& id, int interval_sec,
                                       Task<void> (LocalFsBackend::*fn)()) {
    if (interval_sec <= 0) return;
    bg_.if_open([&] {
        id = TimerQueue::instance().add(std::chrono::seconds(interval_sec),
                                        [this, slot = &id, interval_sec, fn] {
                                            bg_.spawn(run_periodic(slot, interval_sec, fn));
                                        });
    });
}

Task<void> LocalFsBackend::run_periodic(TimerQueue::Id* slot, int interval_sec,
                                        Task<void> (LocalFsBackend::*fn)()) {
    // Two statements on purpose: GCC 15 ICEs (gimplify.cc gimple_add_tmp_var) on
    // co_await'ing a pointer-to-member call expression directly
    Task<void> run = (this->*fn)();
    co_await std::move(run);
    schedule_periodic(*slot, interval_sec, fn);
}

Task<void> LocalFsBackend::mpu_scan_task() {
    co_await pool_->schedule();  // directory walk is blocking IO, go to the pool
    cleanup_stale_uploads();
}

Task<void> LocalFsBackend::sidecar_sweep_task() { (void)co_await run_sidecar_sweep_once(); }

void LocalFsBackend::shutdown_background() {
    bg_.begin_close();
    // cancel must happen outside the group lock: TimerQueue::cancel blocks on in-flight
    // callbacks, and the callback takes the group lock
    TimerQueue::instance().cancel(mpu_timer_);
    TimerQueue::instance().cancel(sidecar_timer_);
    bg_.wait_idle();
}

void LocalFsBackend::defer_sidecar(fs::path dest, ObjectMeta meta) {
    // The sidecar is an auxiliary copy here (the xattr succeeded, or we would not be
    // deferring): ordering between two deferred writes of the same key is not enforced --
    // on an xattr filesystem the sidecar is never read back, and the next sync-mode or
    // tagging write realigns it. Closing backends write inline instead of dropping it
    bool spawned = bg_.spawn([](LocalFsBackend* self, fs::path d, ObjectMeta m) -> Task<void> {
        co_await self->pool_->schedule();
        fsutil::write_object_sidecar(d, m, self->staging_ / "put");
    }(this, dest, meta));
    if (!spawned) fsutil::write_object_sidecar(dest, meta, staging_ / "put");
}

AsyncSemaphore& LocalFsBackend::commit_lock(std::string_view bucket, std::string_view key) {
    size_t h = std::hash<std::string_view>()(bucket) * 1315423911u ^
               std::hash<std::string_view>()(key);
    return *commit_locks_[h % kLockStripes];
}

fs::path LocalFsBackend::bucket_dir(std::string_view bucket) const {
    return root_ / fs::path(std::string(bucket));
}

fs::path LocalFsBackend::object_path(std::string_view bucket, std::string_view key) const {
    // Directory-marker object (docs/archive/gaps.md §6.3): "a/b/" has no corresponding file name
    // on the filesystem; it lands on the reserved marker file inside the directory:
    // <bucket>/a/b/.lights3-dir
    std::string rel(key);
    if (rel.ends_with('/')) rel += fsutil::kDirMarker;
    fs::path p = bucket_dir(bucket) / fs::path(rel);
    // Defense in depth (docs/archive/gaps.md §1.1): bucket/key were already validated at L2 and at
    // each entry point, so the path should never escape root_. But fs::path::operator/
    // with an absolute right-hand operand **replaces the entire path**
    // (root_ / "/etc" == "/etc"), and the cost of a single slip is arbitrary file reads --
    // so confirm once more before actually touching the filesystem. lexically_normal
    // collapses "..", no disk access required
    fs::path norm = p.lexically_normal();
    fs::path base = root_.lexically_normal();
    auto [it, _] = std::mismatch(base.begin(), base.end(), norm.begin(), norm.end());
    if (it != base.end())
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "The specified bucket is not valid.", std::string(bucket));
    return p;
}

void LocalFsBackend::require_bucket(std::string_view bucket) const {
    if (!fs::exists(bucket_dir(bucket) / kBucketMarker))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(bucket));
}

ObjectMeta LocalFsBackend::load_meta(const fs::path& data_path, std::string key) const {
    // Tier awareness (a stub's size comes from the sidecar) is handled in the shared
    // implementation (docs/tiered-storage.md §4.1)
    return fsutil::load_object_meta(data_path, std::move(key));
}

// ---------- object metadata cache (roadmap §3.8) ----------

ObjectMeta LocalFsBackend::meta_from_stat(std::string_view bucket, std::string_view key,
                                          const fs::path& path, const struct stat& st,
                                          const FsMetaCache::Token& tok,
                                          fsutil::TierInfo* tier_out) const {
    auto rec = std::make_shared<FsCachedMeta>();
    rec->meta = fsutil::load_object_meta_stat(path, std::string(key), st, &rec->tier);
    rec->stamp = FsMetaStamp::of(st);
    if (tier_out) *tier_out = rec->tier;
    ObjectMeta out = rec->meta;
    meta_cache_->insert(tok, bucket, key, std::move(rec));
    return out;
}

// ---------- bucket ----------

Task<void> LocalFsBackend::create_bucket(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    fs::path dir = bucket_dir(bucket);
    if (fs::exists(dir / kBucketMarker))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(bucket));
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create bucket dir: " + ec.message());
    // fsync the marker and both levels of directory entries (docs/archive/gaps.md §4): the object
    // write path is strictly fsynced, and the inversion of "bucket vanished after power
    // loss while the client already got a 200 -- with objects still in it, even" is
    // unacceptable. The bucket directory's own dirent lives in root, so fsyncing only the
    // bucket directory does not buy that record's durability
    fs::path marker = dir / kBucketMarker;
    int fd = ::open(marker.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw_errno("create bucket marker");
    try {
        fsutil::fsync_file(fd);  // uses the shared switch, moves in lockstep with the object write path
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    fsutil::fsync_dir(dir);
    fsutil::fsync_dir(root_);
    co_return;
}

Task<void> LocalFsBackend::delete_bucket(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    require_bucket(bucket);
    fs::path dir = bucket_dir(bucket);
    for (auto& e : fs::directory_iterator(dir)) {
        if (e.path().filename() != kBucketMarker)
            throw S3Error(S3ErrorCode::BucketNotEmpty,
                          "The bucket you tried to delete is not empty", std::string(bucket));
    }
    // The throwing overloads would leak filesystem_error (not an S3Error) straight through
    // as a 500; and if the directory cannot be removed after the marker is deleted (a
    // concurrent write landed an object between the emptiness check and remove), the
    // bucket disappears from list/exists while the data remains -- invisible and
    // undeletable (docs/archive/gaps.md §3.9)
    std::error_code ec;
    fs::remove(dir / kBucketMarker, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "delete bucket marker: " + ec.message());
    fs::remove(dir, ec);
    if (ec) {
        // Non-empty directory means a concurrent write won the race: restore the marker so
        // the bucket stays visible, report NotEmpty
        std::ofstream marker(dir / kBucketMarker);
        throw S3Error(S3ErrorCode::BucketNotEmpty,
                      "The bucket you tried to delete is not empty", std::string(bucket));
    }
    co_return;
}

Task<bool> LocalFsBackend::bucket_exists(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    co_return fs::exists(bucket_dir(bucket) / kBucketMarker);
}

Task<std::vector<BucketInfo>> LocalFsBackend::list_buckets() {
    co_await pool_->schedule();
    std::vector<BucketInfo> out;
    for (auto& e : fs::directory_iterator(root_)) {
        if (!e.is_directory()) continue;
        struct stat st{};
        fs::path marker = e.path() / kBucketMarker;
        if (::stat(marker.c_str(), &st) != 0) continue;
        out.push_back({e.path().filename().string(),
                       std::chrono::system_clock::from_time_t(st.st_mtime)});
    }
    std::sort(out.begin(), out.end(),
              [](const BucketInfo& a, const BucketInfo& b) { return a.name < b.name; });
    co_return out;
}

// ---------- object ----------

Task<PutResult> LocalFsBackend::put_object(std::string_view bucket, std::string_view key,
                                           ObjectMeta meta, http::BodyReader& body,
                                           PutCondition cond) {
    OpGuard g{this, Op::kPut};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    // 1. Stream into a staging tmp file, computing MD5 as we write
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open staging tmp");

    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t total = 0;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        const char* p = reinterpret_cast<const char*>(buf);
        size_t left = n;
        while (left > 0) {
            if (int fe = fault::check("localfs.write")) {  // roadmap §6.1
                errno = fe;
                throw_errno("write staging tmp");
            }
            ssize_t w = ::write(tmp.fd, p, left);
            if (w < 0) throw_errno("write staging tmp");
            p += w;
            left -= static_cast<size_t>(w);
        }
        total += n;
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    meta.key = std::string(key);
    meta.size = total;
    meta.etag = md5.final_hex();
    meta.last_modified = std::chrono::system_clock::now();

    // 2. Conflict check + data rename + sidecar commit. Per-key lock: the commit section
    // is two renames, and interleaved concurrent PUTs of the same key would produce a torn
    // "data=A, etag=B" object.
    // The conditional PUT check sits inside the same lock (PutCondition contract): check
    // and commit are atomic with respect to concurrent writers
    auto lk = co_await commit_lock(bucket, key).acquire();
    co_await pool_->schedule();  // the lock wakeup may resume on another thread; do blocking IO back on a pool thread
    auto inv = invalidate_on_exit(bucket, key);  // cached record dropped after the commit (roadmap §3.8)
    fs::path dest = object_path(bucket, key);
    fsutil::check_put_condition(dest, cond, key);
    if (commit_object_file(dest, tmp, meta, staging_ / "put", key, commit_options()))
        defer_sidecar(std::move(dest), meta);
    g.ok = true;
    co_return PutResult{meta.etag};
}

Task<ObjectStream> LocalFsBackend::get_object(std::string_view bucket, std::string_view key,
                                              std::optional<ByteRange> range) {
    // GET latency stops at stream-handle readiness (open + metadata), excluding body
    // transfer -- the latter is dominated by the client's pull pace, and mixing it in
    // would drown disk latency in network latency
    OpGuard g{this, Op::kGet};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    // Bucket existence is an unconditional precondition on every path that reads
    // metadata from disk (previously it was only checked on the open-failure branch, so
    // the success path never checked the bucket at all, and any bucket that could
    // construct a path outside root_ could read files directly). The one exception is a
    // metadata-cache hit (roadmap §3.8): the record is only used when the inode stamp
    // still matches the fd's fstat, and it proves the bucket existed when the object was
    // last read from disk
    fs::path path = object_path(bucket, key);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        require_bucket(bucket);  // NoSuchBucket takes precedence over NoSuchKey
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        ::close(fd);
        require_bucket(bucket);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }

    ObjectStream out;
    try {
        fsutil::TierInfo tier;
        // Use fstat on the already-open fd (not a second stat on the path): after a
        // concurrent overwrite the path points at a new inode, and size/mtime would be
        // misaligned with the old body the fd holds (short read or truncation, silent
        // corruption). The cached record is trusted only when its stamp matches that
        // same fstat; otherwise the authoritative read refills it
        FsMetaCache::Token tok;
        const FsMetaStamp stamp = FsMetaStamp::of(st);
        if (auto c = meta_cache_->lookup(bucket, key, &tok, [&](const FsCachedMeta& v) {
                return v.stamp == stamp;
            })) {
            out.meta = c->meta;
            tier = c->tier;
        } else {
            require_bucket(bucket);
            out.meta = meta_from_stat(bucket, key, path, st, tok, &tier);
        }
        // Stubbed between open and reading the sidecar: the fd points at a 0-length new
        // inode and cannot deliver the size the sidecar claims -- report it so tiered
        // retries via the cloud (docs/tiered-storage.md §7.3 conflict matrix)
        if (tier.tier != fsutil::Tier::kLocal && out.meta.size > 0 &&
            static_cast<uint64_t>(st.st_size) != out.meta.size)
            throw fsutil::StubRace(std::string(key));
        uint64_t f = 0, l = out.meta.size ? out.meta.size - 1 : 0;
        uint64_t len = out.meta.size;
        if (range) {
            std::tie(f, l) = resolve_range(*range, out.meta.size);
            out.range = ByteRange{f, l};
            len = l - f + 1;
        } else if (out.meta.size == 0) {
            len = 0;
        }
        out.body = std::make_unique<fsutil::FdStreamReader>(fd, f, len, pool_);  // fd ownership transferred
    } catch (...) {
        ::close(fd);
        throw;
    }
    g.ok = true;
    co_return out;
}

// Same-backend copy fast path (docs/archive/gaps.md §6.3): previously CopyObject moved every byte
// "into the gateway and back out" even within one localfs. copy_file_range keeps the move
// in the kernel (direct page-cache copy; O(1) metadata clone on btrfs/xfs reflink).
// Unavailable (cross-device EXDEV, old-kernel ENOSYS, filesystem EINVAL) returns nullopt
// to fall back to the streaming path -- the fallback is semantically equivalent
Task<std::optional<PutResult>> LocalFsBackend::copy_object_fast(
    std::string_view src_bucket, std::string_view src_key, std::string_view dst_bucket,
    std::string_view dst_key, ObjectMeta meta) {
    // The nullopt fallback (mechanism unavailable / source concurrently shortened) is not
    // an error: the semantically equivalent streaming path takes over
    OpGuard g{this, Op::kCopy};
    validate_bucket_name(src_bucket, kAllowReserved);
    validate_bucket_name(dst_bucket, kAllowReserved);
    for (auto k : {src_key, dst_key}) {
        validate_object_key(k);
        validate_fs_object_key(k);
        reject_reserved_key(k);
    }
    co_await pool_->schedule();
    require_bucket(src_bucket);
    require_bucket(dst_bucket);

    fs::path src = object_path(src_bucket, src_key);
    fsutil::TierInfo tier;
    ObjectMeta sm = fsutil::load_object_meta(src, std::string(src_key), &tier);  // missing → NoSuchKey
    if (tier.tier != fsutil::Tier::kLocal) {  // data not local (tiered stub)
        g.ok = true;
        co_return std::nullopt;
    }

    int sfd = ::open(src.c_str(), O_RDONLY);
    if (sfd < 0)
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(src_key));
    struct stat st{};
    if (::fstat(sfd, &st) != 0 || !S_ISREG(st.st_mode)) {
        ::close(sfd);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(src_key));
    }

    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) {
        ::close(sfd);
        throw_errno("open copy tmp");
    }
    uint64_t remaining = uint64_t(st.st_size);
    off_t in_off = 0, out_off = 0;
    while (remaining > 0) {
        ssize_t n = ::copy_file_range(sfd, &in_off, tmp.fd, &out_off, remaining, 0);
        if (n < 0) {
            // Failure before the first byte = mechanism unavailable → fall back; mid-way
            // failure is treated as an IO error
            bool not_supported = (errno == EXDEV || errno == EINVAL || errno == ENOSYS ||
                                  errno == EOPNOTSUPP) &&
                                 in_off == 0;
            int err = errno;
            ::close(sfd);
            if (not_supported) {  // TmpFile RAII discards
                g.ok = true;
                co_return std::nullopt;
            }
            errno = err;
            throw_errno("copy_file_range");
        }
        if (n == 0) break;  // source truncated concurrently: go with what was actually copied (verified below via fstat)
        remaining -= uint64_t(n);
    }
    ::close(sfd);
    if (remaining != 0) {  // source shrank (concurrent overwrite): fall back to streaming for a consistent snapshot
        g.ok = true;
        co_return std::nullopt;
    }

    // Bytes unchanged ⇒ etag/size always equal the source's; REPLACE's new
    // user_meta/content_type are already in meta (assembled by the handler), only the
    // three content-bound fields are filled in
    meta.key = std::string(dst_key);
    meta.size = sm.size;
    meta.etag = sm.etag;
    meta.last_modified = std::chrono::system_clock::now();
    auto lk = co_await commit_lock(dst_bucket, dst_key).acquire();
    co_await pool_->schedule();
    auto inv = invalidate_on_exit(dst_bucket, dst_key);
    fs::path dest = object_path(dst_bucket, dst_key);
    if (commit_object_file(dest, tmp, meta, staging_ / "put", dst_key, commit_options()))
        defer_sidecar(std::move(dest), meta);
    g.ok = true;
    co_return PutResult{meta.etag};
}

Task<ObjectMeta> LocalFsBackend::head_object(std::string_view bucket, std::string_view key) {
    OpGuard g{this, Op::kHead};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    fs::path path = object_path(bucket, key);
    if (!meta_cache_->enabled()) {
        require_bucket(bucket);
        auto m = load_meta(path, std::string(key));
        g.ok = true;
        co_return m;
    }
    // Cache first (roadmap §3.8). Validated (default): one stat(2) taken before the
    // lookup, whose stamp must match the record -- one syscall instead of stat +
    // getxattr (+ sidecar read) + decode, and an external overwrite/removal is caught.
    // Unvalidated: a hit is no syscall at all. A miss goes on to the authoritative
    // read over the same stat (no second syscall) and fills
    FsMetaCache::Token tok;
    struct stat st{};
    bool present = false;
    if (!opt_.meta_cache_validate) {
        if (auto c = meta_cache_->lookup(bucket, key, &tok)) {
            g.ok = true;
            co_return c->meta;
        }
        present = ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
    } else {
        present = ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
        const FsMetaStamp stamp = FsMetaStamp::of(st);
        if (auto c = meta_cache_->lookup(bucket, key, &tok, [&](const FsCachedMeta& v) {
                return present && v.stamp == stamp;
            })) {
            g.ok = true;
            co_return c->meta;
        }
    }
    require_bucket(bucket);
    if (!present)
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    auto m = meta_from_stat(bucket, key, path, st, tok);
    g.ok = true;
    co_return m;
}

Task<void> LocalFsBackend::delete_object(std::string_view bucket, std::string_view key) {
    OpGuard g{this, Op::kDelete};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    auto inv = invalidate_on_exit(bucket, key);
    fs::path path = object_path(bucket, key);
    std::error_code ec;
    // Idempotent: absence is not an error (remove returns false with an empty ec); but
    // real failures like EACCES/EIO must propagate -- a silent 204 would convince the
    // client the delete happened
    fs::remove(path, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "delete object: " + ec.message());
    fs::remove(path.string() + kSidecarSuffix, ec);
    if (ec)
        throw S3Error(S3ErrorCode::InternalError, "delete object sidecar: " + ec.message());
    // Clean up empty parent directories up to the bucket root
    fs::path dir = path.parent_path(), root = bucket_dir(bucket);
    while (dir != root && dir.string().size() > root.string().size()) {
        if (!fs::is_empty(dir, ec) || ec) break;
        fs::remove(dir, ec);
        if (ec) break;
        dir = dir.parent_path();
    }
    g.ok = true;
    co_return;
}

namespace {

using fsutil::DirEntries;
using fsutil::DirEntry;

// One directory read for the listing walker: sorted entries (files by name, subdirectories
// by name+"/", the directory-marker object by ""), bucket marker and sidecars filtered out.
// Sidecars whose data file is absent from the **same readdir** are reported as orphan
// candidates (no extra stat per object: previously every sidecar cost one exists() call,
// a third of the walk's syscalls) and reaped afterwards under the per-key commit lock
DirEntries read_dir_sorted(const fs::path& dir, std::vector<fs::path>* orphans) {
    auto es = std::make_shared<std::vector<DirEntry>>();
    std::vector<std::string> sidecars;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        std::string name = e.path().filename().string();
        std::error_code tec;
        if (e.is_directory(tec)) {
            es->push_back({name + "/", true});
            continue;
        }
        if (name == fsutil::kBucketMarker) continue;
        // Directory-marker object (docs/archive/gaps.md §6.3): restored to the key "<rel>"
        // (already ends with '/'). Its sort key is the empty string -- it sorts right
        // before the other entries in the same directory, matching the
        // "collect everything + full sort" order where "a/b/" < "a/b/x"
        if (name == fsutil::kDirMarker) {
            if (e.is_regular_file(tec)) es->push_back({std::string(), false});
            continue;
        }
        if (name.ends_with(fsutil::kSidecarSuffix)) {
            if (orphans) sidecars.push_back(std::move(name));
            continue;
        }
        if (e.is_regular_file(tec)) es->push_back({std::move(name), false});
    }
    std::sort(es->begin(), es->end(),
              [](const DirEntry& a, const DirEntry& b) { return a.sort_key < b.sort_key; });
    if (!sidecars.empty()) {
        // Orphan sidecar detection: delete_object is a two-step "data first, then sidecar",
        // and a crash in between leaves an orphan that occupies space forever. Membership
        // test against the sorted names of this very directory (the marker object's data
        // file is the marker itself)
        auto has = [&](const std::string& data_name) {
            const std::string& sk = data_name == fsutil::kDirMarker ? std::string() : data_name;
            auto it = std::lower_bound(
                es->begin(), es->end(), sk,
                [](const DirEntry& e, const std::string& k) { return e.sort_key < k; });
            return it != es->end() && !it->is_dir && it->sort_key == sk;
        };
        for (auto& sc : sidecars) {
            std::string data = sc.substr(0, sc.size() - std::strlen(fsutil::kSidecarSuffix));
            if (!has(data)) orphans->push_back(dir / sc);
        }
    }
    return es;
}

// Per-list_objects directory reader: consults the backend's snapshot cache (roadmap
// §3.5 ②) and collects orphan candidates across the walk
struct DirReader {
    fsutil::DirListCache* cache;
    std::vector<fs::path> orphans;

    DirEntries read(const fs::path& dir) {
        if (!cache || !cache->enabled()) return read_dir_sorted(dir, &orphans);
        fsutil::DirListCache::Stamp st;
        // Stamp **before** the readdir: a modification racing with the read then always
        // shows as a stamp mismatch on the next page
        if (!fsutil::DirListCache::stamp_of(dir, st)) return read_dir_sorted(dir, &orphans);
        std::string key = dir.string();
        if (auto hit = cache->lookup(key, st)) return hit;
        auto started = std::chrono::system_clock::now();
        auto es = read_dir_sorted(dir, &orphans);
        cache->insert(key, st, es, started);
        return es;
    }
};

// Ordered directory walk for list_objects (docs/archive/gaps.md §2.7 pruning).
// Sort key of a directory entry: files use name, directories use name+"/" -- every key
// underneath has that string as a prefix, so the interleaved output order matches
// "collect everything + full sort" byte for byte, without materializing the whole tree.
struct ListWalker {
    const std::string& prefix;
    const std::string& start_after;  // only entries strictly greater than this key are visible
    DirReader& reader;
    // Returning false terminates the whole walk (truncation); the callback may set
    // skip_prefix to skip a delimiter group
    std::function<bool(std::string&&)> on_key;
    std::string skip_prefix;  // non-empty: every key/subtree with this prefix is skipped wholesale
    bool stopped = false;

    // Whether the subtree (key prefix q) may contain matching keys: it intersects prefix,
    // and start_after is not greater than every key of the subtree
    bool subtree_may_match(const std::string& q) const {
        size_t n = std::min(q.size(), prefix.size());
        if (q.compare(0, n, prefix, 0, n) != 0) return false;
        if (!start_after.empty() && start_after.compare(0, q.size(), q) > 0) return false;
        return true;
    }

    // Page start (roadmap §3.5 ②): index of the first entry of this (sorted) directory
    // that can still produce output after start_after -- a binary search instead of the
    // old linear "q <= start_after → continue" over every entry, which made page N cost
    // O(directory) even when the marker sat near the end
    size_t first_visible(const std::vector<DirEntry>& es, const std::string& rel) const {
        if (start_after.empty()) return 0;
        if (start_after.compare(0, rel.size(), rel) != 0)
            // Not under rel: every key here is either entirely above start_after (rel >
            // start_after, keep all) or entirely below it (skip all)
            return rel > start_after ? 0 : es.size();
        std::string_view tail(start_after);
        tail.remove_prefix(rel.size());
        size_t i = std::partition_point(es.begin(), es.end(),
                                        [&](const DirEntry& e) { return e.sort_key <= tail; }) -
                   es.begin();
        // The directory right before the boundary may be an ancestor of start_after (its
        // "name/" is a prefix of the tail): the marker lies inside it, keep descending
        if (i > 0 && es[i - 1].is_dir && tail.starts_with(es[i - 1].sort_key)) --i;
        return i;
    }

    // rel: the key prefix this directory corresponds to ("" or "a/b/")
    void walk(const fs::path& dir, const std::string& rel) {
        DirEntries es = reader.read(dir);
        for (size_t i = first_visible(*es, rel); i < es->size(); ++i) {
            const DirEntry& e = (*es)[i];
            if (stopped) return;
            std::string q = rel + e.sort_key;  // file: full key; directory: subtree prefix
            // Delimiter group skipping: an item fully inside the group is skipped
            // outright; when the group prefix continues into the subtree (skip_prefix is
            // longer than q and starts with q) we still need to descend
            if (!skip_prefix.empty()) {
                if (q.compare(0, skip_prefix.size(), skip_prefix) == 0) continue;
                if (!(e.is_dir && skip_prefix.compare(0, q.size(), q) == 0))
                    skip_prefix.clear();
            }
            if (e.is_dir) {
                if (!subtree_may_match(q)) continue;
                fs::path sub = dir / e.sort_key.substr(0, e.sort_key.size() - 1);
                walk(sub, q);
                continue;
            }
            if (q.compare(0, prefix.size(), prefix) != 0) {
                if (q > prefix) return;  // sorted: stop once past the prefix range (at this directory level)
                continue;
            }
            if (!start_after.empty() && q <= start_after) continue;
            if (!on_key(std::move(q))) {
                stopped = true;
                return;
            }
        }
    }
};

// The greatest key with prefix want (descending scan, first hit is the greatest); empty
// string if none. When the truncation boundary lands on a common prefix, this finds the
// group tail, keeping the next_token semantics ("last key inside the group") consistent
// with the full implementation
std::string max_key_with_prefix(DirReader& reader, const fs::path& dir, const std::string& rel,
                                const std::string& want) {
    DirEntries es = reader.read(dir);
    for (auto it = es->rbegin(); it != es->rend(); ++it) {
        const DirEntry& e = *it;
        std::string q = rel + e.sort_key;
        if (e.is_dir) {
            // The whole subtree is inside the group (q starts with want), or the group
            // prefix passes through the subtree (want starts with q)
            if (q.compare(0, want.size(), want) != 0 && want.compare(0, q.size(), q) != 0)
                continue;
            fs::path sub = dir / e.sort_key.substr(0, e.sort_key.size() - 1);
            auto r = max_key_with_prefix(reader, sub, q, want);
            if (!r.empty()) return r;
        } else if (q.compare(0, want.size(), want) == 0) {
            return q;
        }
    }
    return {};
}

// A directory-marker object's data file is the reserved marker inside the directory
// (same mapping as object_path)
fs::path key_data_path(const fs::path& base, const std::string& key) {
    return key.ends_with('/') ? base / fs::path(key + fsutil::kDirMarker) : base / fs::path(key);
}

// Keys per strided meta-load worker below which fanning out is not worth the pool hops
constexpr size_t kMinKeysPerMetaWorker = 32;

}  // namespace

Task<ListResult> LocalFsBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    OpGuard g{this, Op::kList};
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    require_bucket(bucket);

    fs::path base = bucket_dir(bucket);
    ListResult out;
    // S3: max-keys=0 returns an empty result with IsTruncated=false (consistent with apply_listing)
    if (opt.max_keys <= 0) {
        g.ok = true;
        co_return out;
    }

    // Prefix pruning: the part before the last '/' locates the starting directory
    // directly; if it doesn't exist there is no match
    std::string dir_rel;
    if (auto slash = opt.prefix.rfind('/'); slash != std::string::npos)
        dir_rel = opt.prefix.substr(0, slash + 1);
    fs::path start_dir = dir_rel.empty() ? base : base / fs::path(dir_rel);
    std::error_code ec;
    if (!fs::is_directory(start_dir, ec)) {
        g.ok = true;
        co_return out;
    }

    const std::string& delim = opt.delimiter;
    int count = 0;
    std::string last_emitted;   // last emitted entry (key or group name)
    bool last_is_group = false;
    std::vector<std::string> page_keys;  // metadata is loaded after the walk, in parallel

    DirReader reader{dir_cache_.get(), {}};
    ListWalker walker{opt.prefix, opt.start_after, reader, nullptr, {}, false};
    auto truncate = [&] {
        out.is_truncated = true;
        // The group tail is the token (consistent with the full implementation); if a
        // concurrent delete made the group vanish, fall back to the group name itself
        if (last_is_group) {
            std::string tail = max_key_with_prefix(reader, start_dir, dir_rel, last_emitted);
            out.next_token = tail.empty() ? last_emitted : tail;
        } else {
            out.next_token = last_emitted;
        }
    };
    walker.on_key = [&](std::string&& key) {
        if (!delim.empty()) {
            auto pos = key.find(delim, opt.prefix.size());
            if (pos != std::string::npos) {
                std::string group = key.substr(0, pos + delim.size());
                if (count >= opt.max_keys) {
                    truncate();
                    return false;
                }
                out.common_prefixes.push_back(group);
                walker.skip_prefix = group;  // prune the rest of the group's keys/subtrees wholesale
                last_emitted = std::move(group);
                last_is_group = true;
                ++count;
                return true;
            }
        }
        if (count >= opt.max_keys) {
            truncate();
            return false;
        }
        last_emitted = key;
        page_keys.push_back(std::move(key));
        last_is_group = false;
        ++count;
        return true;
    };
    walker.walk(start_dir, dir_rel);
    co_await load_page_meta(base, page_keys, out.objects);
    if (!reader.orphans.empty())
        co_await reap_orphan_sidecars(std::string(bucket), std::move(reader.orphans));
    g.ok = true;
    co_return out;
}

// roadmap §3.5 ①: one page = up to max_keys × (stat + getxattr), previously serial on a
// single pool thread (2000+ syscalls behind one request). Strided fan-out over the pool;
// results keep the walk order by index
Task<void> LocalFsBackend::load_page_meta(const fs::path& base,
                                          const std::vector<std::string>& keys,
                                          std::vector<ObjectMeta>& out) {
    const size_t n = keys.size();
    if (n == 0) co_return;
    std::vector<std::optional<ObjectMeta>> metas(n);
    size_t stride = size_t(std::max(1, opt_.list_meta_concurrency));
    stride = std::min({stride, pool_->size(),
                       (n + kMinKeysPerMetaWorker - 1) / kMinKeysPerMetaWorker});
    if (stride <= 1) {
        co_await load_meta_slice(base, keys, 0, 1, metas);  // already on a pool thread
    } else {
        std::vector<Task<void>> workers;
        workers.reserve(stride);
        for (size_t w = 0; w < stride; ++w)
            workers.push_back(load_meta_slice(base, keys, w, stride, metas));
        co_await when_all(std::move(workers));
        co_await pool_->schedule();  // when_all resumes on the last worker's thread; stay on the pool
    }
    out.reserve(out.size() + n);
    for (auto& m : metas)
        if (m) out.push_back(std::move(*m));
}

Task<void> LocalFsBackend::load_meta_slice(const fs::path& base,
                                           const std::vector<std::string>& keys, size_t first,
                                           size_t stride,
                                           std::vector<std::optional<ObjectMeta>>& metas) {
    if (stride > 1) co_await pool_->schedule();
    for (size_t i = first; i < keys.size(); i += stride) {
        try {
            metas[i] = load_meta(key_data_path(base, keys[i]), keys[i]);
        } catch (const S3Error& e) {
            // Deleted between readdir and stat: the key simply drops out of this page
            // (the old inline path turned it into a NoSuchKey for the whole LIST)
            if (e.code != S3ErrorCode::NoSuchKey) throw;
        }
    }
}

// ---------- orphan sidecars (roadmap §3.5) ----------

// Remove one "<key>.lights3-meta" whose data file is gone. Under the key's commit lock:
// PUT's commit section (data rename → sidecar write) holds the same lock, so the re-check
// and the unlink cannot straddle a fresh commit and delete the new object's sidecar --
// which on a filesystem without xattr would be its only metadata
Task<bool> LocalFsBackend::reap_orphan_sidecar(std::string bucket, fs::path sidecar) {
    std::string rel = sidecar.lexically_relative(bucket_dir(bucket)).generic_string();
    if (rel.empty() || rel.starts_with("..") || !rel.ends_with(kSidecarSuffix)) co_return false;
    std::string key = rel.substr(0, rel.size() - std::strlen(kSidecarSuffix));
    fs::path data = bucket_dir(bucket) / fs::path(key);
    if (key.ends_with(fsutil::kDirMarker))  // marker object: lock the "<dir>/" key PUT uses
        key.resize(key.size() - std::strlen(fsutil::kDirMarker));
    auto lk = co_await commit_lock(bucket, key).acquire();
    co_await pool_->schedule();
    std::error_code ec;
    if (fs::exists(data, ec)) co_return false;  // a PUT landed meanwhile: not an orphan any more
    bool removed = fs::remove(sidecar, ec) && !ec;
    if (removed) m_orphans_removed_->inc();
    co_return removed;
}

Task<void> LocalFsBackend::reap_orphan_sidecars(std::string bucket, std::vector<fs::path> list) {
    for (auto& p : list) {
        try {
            co_await reap_orphan_sidecar(bucket, p);
        } catch (const std::exception& e) {  // best effort: never fail the listing over it
            LOG_WARN("localfs: orphan sidecar {} not removed: {}", p.string(), e.what());
        }
    }
}

Task<uint64_t> LocalFsBackend::run_sidecar_sweep_once() {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    if (!scope.ok()) co_return 0;
    uint64_t removed = 0, seen = 0, candidates = 0;
    std::error_code ec;
    for (auto bit = fs::directory_iterator(root_, ec); !ec && bit != fs::directory_iterator();
         bit.increment(ec)) {
        std::error_code sec;
        if (bg_.closing()) break;
        if (!bit->is_directory(sec) || !fs::exists(bit->path() / kBucketMarker, sec)) continue;
        std::string bucket = bit->path().filename().string();
        std::error_code wec;
        for (auto it = fs::recursive_directory_iterator(bit->path(), wec);
             !wec && it != fs::recursive_directory_iterator(); it.increment(wec)) {
            if (bg_.closing()) break;
            // Yield periodically: a full-store walk must not pin one pool thread
            if (++seen % 256 == 0) co_await pool_->schedule();
            std::error_code tec;
            if (!it->is_regular_file(tec)) continue;
            std::string name = it->path().filename().string();
            if (!name.ends_with(kSidecarSuffix)) continue;
            fs::path data = it->path().parent_path() /
                            name.substr(0, name.size() - std::strlen(kSidecarSuffix));
            if (fs::exists(data, tec)) continue;
            ++candidates;
            try {
                if (co_await reap_orphan_sidecar(bucket, it->path())) ++removed;
            } catch (const std::exception& e) {
                LOG_WARN("localfs: sweep: orphan sidecar {} not removed: {}", it->path().string(),
                         e.what());
            }
        }
        if (wec) LOG_WARN("localfs: sweep {}: directory walk failed: {}", bucket, wec.message());
    }
    if (ec) LOG_WARN("localfs: sweep: root enumeration failed: {}", ec.message());
    if (candidates || removed)
        LOG_INFO("localfs: orphan sidecar sweep{}: {} entries seen, {} orphans, {} removed",
                 bg_.closing() ? " (interrupted)" : "", seen, candidates, removed);
    co_return removed;
}

Task<void> LocalFsBackend::set_object_tagging(std::string_view bucket, std::string_view key,
                                              std::string tagging) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);
    // Same per-key serialization as PUT commits: a concurrent overwrite must not
    // interleave with the xattr/sidecar rewrite pair
    auto lk = co_await commit_lock(bucket, key).acquire();
    co_await pool_->schedule();
    auto inv = invalidate_on_exit(bucket, key);
    fs::path p = object_path(bucket, key);
    fsutil::TierInfo tier;
    ObjectMeta meta = load_object_meta(p, std::string(key), &tier);  // missing -> NoSuchKey
    meta.tagging = std::move(tagging);
    fsutil::rewrite_object_meta(p, meta, tier, staging_ / "put", opt_.sidecar, &xattr_);
}

// ---------- multipart (docs/storage-backend.md §3.2) ----------
// Layout: <staging>/mpu/<upload_id>/{manifest, part.NNNNN, part.NNNNN.md5}
// A part fsyncs its data first, then writes .md5: the presence of .md5 means the part data
// is durable (complete trusts .md5 without recomputing the checksum, and that trust
// requires this ordering)

Task<std::string> LocalFsBackend::create_multipart(std::string_view bucket,
                                                   std::string_view key, ObjectMeta meta) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    std::string id = new_upload_id();
    fs::path dir = staging_ / "mpu" / id;
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create mpu dir: " + ec.message());

    std::vector<std::pair<std::string, std::string>> kv{
        {"bucket", std::string(bucket)},
        {"key", std::string(key)},
        {"content_type", meta.content_type}};
    if (!meta.checksum_algorithm.empty())
        kv.emplace_back("checksum_algorithm", meta.checksum_algorithm);
    // First-class metadata must also survive create→complete (docs/archive/gaps.md §5.2); key
    // names share their source with the sidecar
    for (auto& f : kStdMetaFields)
        if (!(meta.*f.field).empty()) kv.emplace_back(f.store_key, meta.*f.field);
    for (auto& [k, v] : meta.user_meta) kv.emplace_back("meta." + k, v);
    write_tsv(dir / "manifest", staging_ / "put", kv);
    co_return id;
}

Task<PutResult> LocalFsBackend::upload_part(std::string_view bucket, std::string_view key,
                                            std::string_view upload_id, int part_no,
                                            http::BodyReader& body,
                                            const std::optional<PartChecksum>& checksum) {
    OpGuard g{this, Op::kUploadPart};
    validate_part_number(part_no);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));

    // Stream into a staging tmp file, computing the part MD5 as we write (same as PUT)
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open part tmp");

    util::HashStream md5(util::HashStream::Algo::Md5);
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        const char* p = reinterpret_cast<const char*>(buf);
        size_t left = n;
        while (left > 0) {
            if (int fe = fault::check("localfs.write")) {  // roadmap §6.1
                errno = fe;
                throw_errno("write part tmp");
            }
            ssize_t w = ::write(tmp.fd, p, left);
            if (w < 0) throw_errno("write part tmp");
            p += w;
            left -= static_cast<size_t>(w);
        }
    }
    fsutil::fsync_file(tmp.fd);  // part data persisted first: only then is .md5's presence evidence of durable data
    ::close(tmp.fd);
    tmp.fd = -1;
    std::string etag = md5.final_hex();

    // Order: data rename first, .md5 written after (same-number re-upload is
    // last-write-wins, rename overwrites). In the reverse order (old implementation),
    // "part 200 → power loss" leaves an fsynced .md5 paired with zero-block data, and
    // since complete trusts .md5 without recomputation it would produce an object with a
    // valid ETag and zero-block content
    std::string name = part_file_name(part_no);
    std::error_code ec;
    fs::rename(tmp.path, up.dir / name, ec);
    if (ec) {
        // The upload may have been aborted (directory removed) while the body was being read
        if (!fs::exists(up.dir))
            throw S3Error(S3ErrorCode::NoSuchUpload,
                          "The specified multipart upload does not exist.",
                          std::string(upload_id));
        throw S3Error(S3ErrorCode::InternalError, "rename part failed");
    }
    tmp.committed = true;
    fsutil::fsync_dir(up.dir);
    // The .md5 sidecar is already a kv file: the verified part checksum rides along
    // (roadmap §2.2); resolved() only after the body was drained above (trailer form)
    std::vector<std::pair<std::string, std::string>> pkv{{"md5", etag}};
    if (checksum && !checksum->resolved().empty()) {
        pkv.emplace_back("checksum_algorithm", checksum->algorithm);
        pkv.emplace_back("checksum_value", checksum->resolved());
    }
    write_tsv(up.dir / (name + ".md5"), staging_ / "put", pkv);
    g.ok = true;
    co_return PutResult{etag};
}

Task<PutResult> LocalFsBackend::complete_multipart(std::string_view bucket,
                                                   std::string_view key,
                                                   std::string_view upload_id,
                                                   std::span<const PartInfo> parts) {
    OpGuard g{this, Op::kCompleteMpu};
    validate_part_order(parts);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    require_bucket(bucket);

    // 1. Validate every declared part: exists and ETag matches
    std::vector<std::string> md5s;
    std::vector<fs::path> paths;
    std::vector<PartDigest> digests;
    md5s.reserve(parts.size());
    for (auto& p : parts) {
        std::string name = part_file_name(p.part_no);
        std::string stored;
        PartDigest digest;
        for (auto& [k, v] : read_tsv(up.dir / (name + ".md5"))) {
            if (k == "md5") stored = v;
            else if (k == "checksum_algorithm") digest.algorithm = v;
            else if (k == "checksum_value") digest.value = v;
        }
        if (stored.empty() || !fs::exists(up.dir / name) ||
            stored != strip_etag_quotes(p.etag))
            throw S3Error(S3ErrorCode::InvalidPart,
                          "One or more of the specified parts could not be found or the "
                          "ETag did not match.",
                          std::string(key));
        md5s.push_back(stored);
        digests.push_back(std::move(digest));
        paths.push_back(up.dir / name);
    }

    // 2. Concatenate into the final tmp file in declared order
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open complete tmp");
    uint64_t total = 0;
    std::vector<uint64_t> sizes;  // per-part layout for GET ?partNumber (roadmap §2.5)
    std::vector<char> buf(256 * 1024);
    for (auto& path : paths) {
        uint64_t part_bytes = 0;
        int in = ::open(path.c_str(), O_RDONLY);
        if (in < 0) throw_errno("open part");
        for (;;) {
            ssize_t n = ::read(in, buf.data(), buf.size());
            if (n < 0) {
                ::close(in);
                throw_errno("read part");
            }
            if (n == 0) break;
            const char* p = buf.data();
            size_t left = static_cast<size_t>(n);
            while (left > 0) {
                int fe = fault::check("localfs.write");  // roadmap §6.1
                ssize_t w = fe ? -1 : ::write(tmp.fd, p, left);
                if (w < 0) {
                    if (fe) errno = fe;
                    ::close(in);
                    throw_errno("write complete tmp");
                }
                p += w;
                left -= static_cast<size_t>(w);
            }
            total += static_cast<uint64_t>(n);
            part_bytes += static_cast<uint64_t>(n);
        }
        ::close(in);
        sizes.push_back(part_bytes);
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    // 3. Commit (same atomic path as PUT), then clean up the mpu directory
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    meta.part_sizes = std::move(sizes);
    PutResult result{meta.etag};
    // Composite checksum from the stored, verified per-part values (roadmap §2.2)
    apply_composite_checksum(digests, meta, result);
    {
        auto lk = co_await commit_lock(bucket, key).acquire();  // same as PUT: serialize the commit section
        co_await pool_->schedule();
        auto inv = invalidate_on_exit(bucket, key);
        fs::path dest = object_path(bucket, key);
        if (commit_object_file(dest, tmp, meta, staging_ / "put", key, commit_options()))
            defer_sidecar(std::move(dest), meta);
    }

    std::error_code ec;
    fs::remove_all(up.dir, ec);
    g.ok = true;
    co_return result;
}

Task<void> LocalFsBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id) {
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    std::error_code ec;
    fs::remove_all(up.dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "remove mpu dir: " + ec.message());
    co_return;
}

Task<ListPartsResult> LocalFsBackend::list_parts(std::string_view bucket, std::string_view key,
                                                 std::string_view upload_id,
                                                 const ListPartsOptions& opt) {
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    // Directory enumeration is unordered: first collect and sort only the part numbers,
    // then stat + read .md5 only for **this page's** entries. Previously every part got a
    // stat and a sidecar read, wasting 9999 of them per page turn
    std::vector<int> nos;
    for (auto& e : fs::directory_iterator(up.dir)) {
        std::string name = e.path().filename().string();
        // part.NNNNN (skip the manifest and .md5 sidecars)
        if (name.rfind("part.", 0) != 0 || name.ends_with(".md5")) continue;
        try {
            int no = std::stoi(name.substr(5));
            if (no > opt.part_number_marker) nos.push_back(no);
        } catch (...) {
            continue;
        }
    }
    std::sort(nos.begin(), nos.end());
    // Fetch one extra to determine is_truncated, then hand off to apply_parts_page for the
    // uniform trim
    if (opt.max_parts > 0 && nos.size() > size_t(opt.max_parts) + 1)
        nos.resize(size_t(opt.max_parts) + 1);
    std::vector<PartMeta> out;
    for (int no : nos) {
        std::string name = part_file_name(no);
        struct stat st{};
        if (::stat((up.dir / name).c_str(), &st) != 0) continue;
        std::string etag, calgo, cval;
        for (auto& [k, v] : read_tsv(up.dir / (name + ".md5"))) {
            if (k == "md5") etag = v;
            else if (k == "checksum_algorithm") calgo = v;
            else if (k == "checksum_value") cval = v;
        }
        out.push_back({no, static_cast<uint64_t>(st.st_size), etag,
                       std::chrono::system_clock::from_time_t(st.st_mtime), calgo, cval});
    }
    co_return apply_parts_page(std::move(out), opt);
}

Task<ListUploadsResult> LocalFsBackend::list_multipart_uploads(std::string_view bucket,
                                                               const ListUploadsOptions& opt) {
    co_await pool_->schedule();
    require_bucket(bucket);
    // mpu is a single flat directory shared by the whole instance: knowing which bucket an
    // upload belongs to requires reading its manifest, so wherever the marker falls we
    // must scan everything first (pagination saves response size and downstream memory,
    // not this scan -- only a layout change to mpu/<bucket>/… would save it, and that is a
    // separate matter)
    std::vector<UploadInfo> all;
    for (auto& e : fs::directory_iterator(staging_ / "mpu")) {
        if (!e.is_directory()) continue;
        std::string id = e.path().filename().string();
        std::string m_bucket, m_key;
        for (auto& [k, v] : read_tsv(e.path() / "manifest")) {
            if (k == "bucket") m_bucket = v;
            else if (k == "key") m_key = v;
        }
        if (m_bucket != bucket) continue;
        struct stat st{};
        auto initiated = ::stat((e.path() / "manifest").c_str(), &st) == 0
                             ? std::chrono::system_clock::from_time_t(st.st_mtime)
                             : std::chrono::system_clock::now();
        all.push_back({m_key, id, initiated});
    }
    std::sort(all.begin(), all.end(), [](const UploadInfo& a, const UploadInfo& b) {
        return std::tie(a.key, a.upload_id) < std::tie(b.key, b.upload_id);
    });
    co_return apply_uploads_page(std::move(all), opt);
}

void LocalFsBackend::cleanup_stale_uploads() {
    std::error_code ec;
    auto now = fs::file_time_type::clock::now();
    for (auto& e : fs::directory_iterator(staging_ / "mpu", ec)) {
        if (!e.is_directory()) continue;
        std::error_code tec;
        fs::path manifest = e.path() / "manifest";
        auto t = fs::exists(manifest, tec) ? fs::last_write_time(manifest, tec)
                                           : fs::last_write_time(e.path(), tec);
        if (tec || now - t <= std::chrono::seconds(opt_.mpu_ttl_sec)) continue;
        fs::remove_all(e.path(), tec);
        if (!tec)
            LOG_INFO("localfs: removed stale multipart upload {}",
                     e.path().filename().string());
    }
}

// ---------- scrub (roadmap §3.1) ----------

Task<FsScrubStats> LocalFsBackend::run_scrub_once(FsScrubOptions opt) {
    co_await pool_->schedule();
    BackgroundTaskGroup::Scope scope(bg_);
    FsScrubStats st;
    if (!scope.ok()) {
        st.aborted = true;
        co_return st;
    }
    ScrubThrottle throttle(opt.max_bytes_per_sec, pool_, [this] { return bg_.closing(); });
    std::vector<uint8_t> buf(256 << 10);
    std::error_code ec;
    for (auto it = fs::directory_iterator(root_, ec); !ec && it != fs::directory_iterator();
         it.increment(ec)) {
        if (st.aborted || bg_.closing()) {
            st.aborted = true;
            break;
        }
        std::error_code sec;
        if (!it->is_directory(sec) || !fs::exists(it->path() / fsutil::kBucketMarker, sec))
            continue;
        co_await scrub_bucket(it->path().filename().string(), it->path(), throttle, buf, st);
    }
    if (ec) {
        ++st.read_errors;
        LOG_ERROR("localfs: scrub: root enumeration failed: {}", ec.message());
    }
    LOG_INFO("localfs: scrub{}: {} objects, {} bytes read; mismatches {}, read errors {}, "
             "unverifiable {}, stubs skipped {}, races skipped {}, orphan sidecars {}",
             st.aborted ? " (aborted)" : "", st.objects_scanned, st.bytes_read,
             st.etag_mismatches, st.read_errors, st.unverifiable, st.skipped_stubs,
             st.skipped_races, st.orphan_sidecars);
    co_return st;
}

Task<void> LocalFsBackend::scrub_bucket(const std::string& bucket, const fs::path& dir,
                                        ScrubThrottle& throttle, std::vector<uint8_t>& buf,
                                        FsScrubStats& st) {
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec);
         !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        if (bg_.closing()) {
            st.aborted = true;
            co_return;
        }
        std::error_code sec;
        if (!it->is_regular_file(sec)) continue;
        const fs::path& p = it->path();
        std::string name = p.filename().string();
        if (name == fsutil::kBucketMarker) continue;
        if (name.ends_with(fsutil::kSidecarSuffix)) {
            // Delete crashes leave "sidecar without data"; listing self-heals
            // only directories it visits, so the scrub reports the leftovers
            fs::path data = p.parent_path() /
                            name.substr(0, name.size() - strlen(fsutil::kSidecarSuffix));
            if (!fs::exists(data, sec)) {
                ++st.orphan_sidecars;
                LOG_WARN("localfs: scrub {}: orphan sidecar {}", bucket, p.string());
            }
            continue;
        }
        std::string rel = fs::relative(p, dir, sec).generic_string();
        if (sec) continue;
        // Directory-marker file maps back to the trailing-slash key it stands for
        std::string key = name == fsutil::kDirMarker
                              ? rel.substr(0, rel.size() - strlen(fsutil::kDirMarker))
                              : rel;
        co_await scrub_object(bucket, key, p, throttle, buf, st);
    }
    if (ec) {
        ++st.read_errors;
        LOG_ERROR("localfs: scrub {}: directory walk failed: {}", bucket, ec.message());
    }
}

Task<void> LocalFsBackend::scrub_object(const std::string& bucket, const std::string& key,
                                        const fs::path& path, ScrubThrottle& throttle,
                                        std::vector<uint8_t>& buf, FsScrubStats& st) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno != ENOENT) {  // ENOENT = deleted mid-walk, not a finding
            ++st.read_errors;
            LOG_ERROR("localfs: scrub {}/{}: open failed: {}", bucket, key, strerror(errno));
        }
        co_return;
    }
    struct FdGuard {
        int fd;
        ~FdGuard() { ::close(fd); }
    } guard{fd};
    struct stat sb{};
    if (::fstat(fd, &sb) != 0 || !S_ISREG(sb.st_mode)) co_return;
    fsutil::TierInfo tier;
    ObjectMeta meta;
    try {
        meta = fsutil::load_object_meta_stat(path, key, sb, &tier);
    } catch (const S3Error&) {
        // Data file readable but its metadata is not (torn xattr/sidecar): the
        // ETag cannot be known, which is itself worth surfacing
        ++st.objects_scanned;
        ++st.unverifiable;
        LOG_WARN("localfs: scrub {}/{}: metadata unreadable, cannot verify", bucket, key);
        co_return;
    }
    ++st.objects_scanned;
    if (tier.tier == fsutil::Tier::kRemote) {
        ++st.skipped_stubs;  // data lives on the cloud side; nothing local to hash
        co_return;
    }
    if (meta.etag.empty()) {
        ++st.unverifiable;
        LOG_WARN("localfs: scrub {}/{}: no ETag recorded, cannot verify", bucket, key);
        co_return;
    }
    std::string computed;
    try {
        auto dash = meta.etag.find('-');
        if (dash == std::string::npos) {
            computed = co_await md5_range(fd, 0, uint64_t(sb.st_size), throttle, buf, st);
        } else {
            // Multipart composite: recompute per-part MD5s over the recorded
            // part boundaries. Objects completed before part_sizes existed have
            // no recoverable boundaries — unverifiable, not a mismatch
            uint64_t declared_parts = strtoull(meta.etag.c_str() + dash + 1, nullptr, 10);
            uint64_t sum = 0;
            for (uint64_t s : meta.part_sizes) sum += s;
            if (meta.part_sizes.empty() || meta.part_sizes.size() != declared_parts ||
                sum != uint64_t(sb.st_size)) {
                ++st.unverifiable;
                LOG_WARN("localfs: scrub {}/{}: multipart ETag without a usable part layout "
                         "(legacy object), cannot verify",
                         bucket, key);
                co_return;
            }
            std::vector<std::string> md5s;
            md5s.reserve(meta.part_sizes.size());
            uint64_t off = 0;
            for (uint64_t s : meta.part_sizes) {
                md5s.push_back(co_await md5_range(fd, off, s, throttle, buf, st));
                off += s;
            }
            computed = combined_etag(md5s);
        }
    } catch (const std::exception& ex) {
        ++st.read_errors;
        LOG_ERROR("localfs: scrub {}/{}: read failed: {}", bucket, key, ex.what());
        co_return;
    }
    if (st.aborted) co_return;  // partial hash after an interrupted pace is meaningless
    if (computed == meta.etag) co_return;
    // The metadata is read by path while the content hash used the fd: a
    // concurrent overwrite between the two is a torn snapshot, not corruption
    struct stat sb2{};
    if (::stat(path.c_str(), &sb2) != 0 || sb2.st_ino != sb.st_ino ||
        sb2.st_size != sb.st_size || sb2.st_mtim.tv_sec != sb.st_mtim.tv_sec ||
        sb2.st_mtim.tv_nsec != sb.st_mtim.tv_nsec) {
        ++st.skipped_races;
        co_return;
    }
    ++st.etag_mismatches;
    LOG_ERROR("localfs: scrub {}/{}: ETag mismatch (stored {}, computed {}) — silent data "
              "corruption",
              bucket, key, meta.etag, computed);
}

Task<std::string> LocalFsBackend::md5_range(int fd, uint64_t off, uint64_t len,
                                            ScrubThrottle& throttle, std::vector<uint8_t>& buf,
                                            FsScrubStats& st) {
    util::HashStream md5(util::HashStream::Algo::Md5);
    while (len > 0 && !st.aborted) {  // an aborted multipart verify skips its remaining parts
        // Hop per buffer like FdStreamReader: a full-store scrub must not sit
        // on one pool thread for its whole duration
        co_await pool_->schedule();
        size_t want = size_t(std::min<uint64_t>(buf.size(), len));
        ssize_t n = ::pread(fd, buf.data(), want, off);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "pread");
        }
        if (n == 0) throw std::runtime_error("file shorter than expected (truncated under scrub)");
        md5.update(std::span(buf.data(), size_t(n)));
        off += uint64_t(n);
        len -= uint64_t(n);
        st.bytes_read += uint64_t(n);
        co_await throttle.pace(uint64_t(n));
        if (st.aborted || bg_.closing()) {
            st.aborted = true;
            break;  // caller sees st.aborted and discards the partial hash
        }
    }
    co_return md5.final_hex();
}

}  // namespace lights3::storage
