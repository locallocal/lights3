#include "storage/localfs/localfs_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <system_error>
#include <tuple>

#include "core/log.h"
#include "core/util/crypto.h"
#include "storage/listing.h"
#include "storage/multipart.h"

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
    init_metrics(metrics);
    fs::create_directories(root_);
    fs::create_directories(staging_ / "put");
    fs::create_directories(staging_ / "mpu");
    commit_locks_.reserve(kLockStripes);
    for (size_t i = 0; i < kLockStripes; ++i)
        commit_locks_.push_back(std::make_unique<AsyncSemaphore>(1));
    cleanup_stale_uploads();
    schedule_mpu_scan();
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

// Periodic cleanup (docs/archive/gaps.md §6.3): previously ran only once at startup, so a gateway
// running for months without a restart would accumulate never-completed/aborted upload
// directories without bound. Re-arms after completion (same as the duostore worker): scans
// never overlap/pile up, a slow scan just pushes back the next trigger
void LocalFsBackend::schedule_mpu_scan() {
    if (opt_.mpu_ttl_sec <= 0 || opt_.mpu_scan_interval_sec <= 0) return;
    bg_.if_open([&] {
        mpu_timer_ = TimerQueue::instance().add(
            std::chrono::seconds(opt_.mpu_scan_interval_sec), [this] {
                bg_.spawn([](LocalFsBackend* self) -> Task<void> {
                    co_await self->pool_->schedule();  // directory walk is blocking IO, go to the pool
                    self->cleanup_stale_uploads();
                    self->schedule_mpu_scan();
                }(this));
            });
    });
}

void LocalFsBackend::shutdown_background() {
    bg_.begin_close();
    // cancel must happen outside the group lock: TimerQueue::cancel blocks on in-flight
    // callbacks, and the callback takes the group lock
    TimerQueue::instance().cancel(mpu_timer_);
    bg_.wait_idle();
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
    fsutil::check_put_condition(object_path(bucket, key), cond, key);
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);
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
    // Bucket existence is an unconditional precondition: previously it was only checked on
    // the open-failure branch, so the success path never checked the bucket at all, and
    // any bucket that could construct a path outside root_ could read files directly
    require_bucket(bucket);

    fs::path path = object_path(bucket, key);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    struct stat st{};
    if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        ::close(fd);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }

    ObjectStream out;
    try {
        fsutil::TierInfo tier;
        // Use fstat on the already-open fd (not a second stat on the path): after a
        // concurrent overwrite the path points at a new inode, and size/mtime would be
        // misaligned with the old body the fd holds (short read or truncation, silent
        // corruption)
        out.meta = fsutil::load_object_meta_stat(path, std::string(key), st, &tier);
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
    commit_object_file(object_path(dst_bucket, dst_key), tmp, meta, staging_ / "put", dst_key);
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
    require_bucket(bucket);
    auto m = load_meta(object_path(bucket, key), std::string(key));
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

// Ordered directory walk for list_objects (docs/archive/gaps.md §2.7 pruning).
// Sort key of a directory entry: files use name, directories use name+"/" -- every key
// underneath has that string as a prefix, so the interleaved output order matches
// "collect everything + full sort" byte for byte, without materializing the whole tree.
struct ListWalker {
    const std::string& prefix;
    const std::string& start_after;  // only entries strictly greater than this key are visible
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

    // rel: the key prefix this directory corresponds to ("" or "a/b/")
    void walk(const fs::path& dir, const std::string& rel) {
        struct Entry {
            std::string sort_key;  // file: name; directory: name+"/"
            bool is_dir;
        };
        std::vector<Entry> es;
        std::error_code ec;
        for (auto& e : fs::directory_iterator(dir, ec)) {
            std::string name = e.path().filename().string();
            std::error_code tec;
            if (e.is_directory(tec)) {
                es.push_back({name + "/", true});
                continue;
            }
            if (name == fsutil::kBucketMarker) continue;
            // Directory-marker object (docs/archive/gaps.md §6.3): restored to the key "<rel>"
            // (already ends with '/'). Its sort key is the empty string -- it sorts right
            // before the other entries in the same directory, matching the
            // "collect everything + full sort" order where "a/b/" < "a/b/x"
            if (name == fsutil::kDirMarker) {
                if (e.is_regular_file(tec)) es.push_back({std::string(), false});
                continue;
            }
            if (name.ends_with(fsutil::kSidecarSuffix)) {
                // Orphan sidecar self-healing: delete_object is a two-step
                // "data first, then sidecar", and a crash in between leaves an orphan that
                // occupies space forever. With pruning this only covers visited
                // directories (best-effort; unvisited parts wait for a later list)
                std::string data = e.path().string();
                data.resize(data.size() - std::strlen(fsutil::kSidecarSuffix));
                if (!fs::exists(data, tec)) fs::remove(e.path(), tec);
                continue;
            }
            if (e.is_regular_file(tec)) es.push_back({std::move(name), false});
        }
        std::sort(es.begin(), es.end(),
                  [](const Entry& a, const Entry& b) { return a.sort_key < b.sort_key; });

        for (auto& e : es) {
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
std::string max_key_with_prefix(const fs::path& dir, const std::string& rel,
                                const std::string& want) {
    struct Entry {
        std::string sort_key;
        bool is_dir;
    };
    std::vector<Entry> es;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        std::string name = e.path().filename().string();
        std::error_code tec;
        if (e.is_directory(tec)) es.push_back({name + "/", true});
        else if (name == fsutil::kDirMarker) es.push_back({std::string(), false});
        else if (name != fsutil::kBucketMarker && !name.ends_with(fsutil::kSidecarSuffix) &&
                 e.is_regular_file(tec))
            es.push_back({std::move(name), false});
    }
    std::sort(es.begin(), es.end(),
              [](const Entry& a, const Entry& b) { return a.sort_key > b.sort_key; });
    for (auto& e : es) {
        std::string q = rel + e.sort_key;
        if (e.is_dir) {
            // The whole subtree is inside the group (q starts with want), or the group
            // prefix passes through the subtree (want starts with q)
            if (q.compare(0, want.size(), want) != 0 && want.compare(0, q.size(), q) != 0)
                continue;
            fs::path sub = dir / e.sort_key.substr(0, e.sort_key.size() - 1);
            auto r = max_key_with_prefix(sub, q, want);
            if (!r.empty()) return r;
        } else if (q.compare(0, want.size(), want) == 0) {
            return q;
        }
    }
    return {};
}

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

    ListWalker walker{opt.prefix, opt.start_after, nullptr, {}, false};
    auto truncate = [&] {
        out.is_truncated = true;
        // The group tail is the token (consistent with the full implementation); if a
        // concurrent delete made the group vanish, fall back to the group name itself
        if (last_is_group) {
            std::string tail = max_key_with_prefix(start_dir, dir_rel, last_emitted);
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
        // A directory-marker object's data file is the reserved marker inside the
        // directory (same mapping as object_path)
        fs::path data = key.ends_with('/') ? base / fs::path(key + fsutil::kDirMarker)
                                           : base / fs::path(key);
        out.objects.push_back(load_meta(data, key));
        last_emitted = std::move(key);
        last_is_group = false;
        ++count;
        return true;
    };
    walker.walk(start_dir, dir_rel);
    g.ok = true;
    co_return out;
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
                                            http::BodyReader& body) {
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
    write_tsv(up.dir / (name + ".md5"), staging_ / "put", {{"md5", etag}});
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
    md5s.reserve(parts.size());
    for (auto& p : parts) {
        std::string name = part_file_name(p.part_no);
        std::string stored;
        for (auto& [k, v] : read_tsv(up.dir / (name + ".md5")))
            if (k == "md5") stored = v;
        if (stored.empty() || !fs::exists(up.dir / name) ||
            stored != strip_etag_quotes(p.etag))
            throw S3Error(S3ErrorCode::InvalidPart,
                          "One or more of the specified parts could not be found or the "
                          "ETag did not match.",
                          std::string(key));
        md5s.push_back(stored);
        paths.push_back(up.dir / name);
    }

    // 2. Concatenate into the final tmp file in declared order
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open complete tmp");
    uint64_t total = 0;
    std::vector<char> buf(256 * 1024);
    for (auto& path : paths) {
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
                ssize_t w = ::write(tmp.fd, p, left);
                if (w < 0) {
                    ::close(in);
                    throw_errno("write complete tmp");
                }
                p += w;
                left -= static_cast<size_t>(w);
            }
            total += static_cast<uint64_t>(n);
        }
        ::close(in);
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    // 3. Commit (same atomic path as PUT), then clean up the mpu directory
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    {
        auto lk = co_await commit_lock(bucket, key).acquire();  // same as PUT: serialize the commit section
        co_await pool_->schedule();
        commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);
    }

    std::error_code ec;
    fs::remove_all(up.dir, ec);
    g.ok = true;
    co_return PutResult{meta.etag};
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
        std::string etag;
        for (auto& [k, v] : read_tsv(up.dir / (name + ".md5")))
            if (k == "md5") etag = v;
        out.push_back({no, static_cast<uint64_t>(st.st_size), etag,
                       std::chrono::system_clock::from_time_t(st.st_mtime)});
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

}  // namespace lights3::storage
