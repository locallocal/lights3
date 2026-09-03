#include "storage/xlocalfs/xlocalfs_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <tuple>

#include "core/util/crypto.h"
#include "storage/multipart.h"

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

using fsutil::TmpFile;
using fsutil::load_manifest;
using fsutil::next_tmp_name;
using fsutil::part_file_name;
using fsutil::read_tsv;
using fsutil::reject_reserved_key;
using fsutil::require_upload;
using fsutil::throw_errno;
using fsutil::write_tsv;

namespace {

[[noreturn]] void throw_uring(const char* what, int neg_errno) {
    throw S3Error(S3ErrorCode::InternalError,
                  std::string(what) + ": " + std::strerror(-neg_errno));
}

struct FdGuard {
    int fd = -1;
    ~FdGuard() {
        if (fd >= 0) ::close(fd);
    }
};

// ---- metadata ops through the ring (roadmap §3.4 ③), with blocking fallbacks ----
// All of these take const char*: the caller keeps the owning fs::path/string alive across
// the co_await (under SQPOLL the kernel copies the path only when the poll thread picks
// the SQE up). All return the syscall convention (fd / 0 on success, -errno on failure)

Task<int> uring_open(UringEngine& eng, const char* path, int flags, mode_t mode) {
    if (eng.features().op_openat && eng.options().meta_ops)
        co_return co_await eng.openat(AT_FDCWD, path, flags, mode);
    int fd = ::open(path, flags, mode);
    co_return fd < 0 ? -errno : fd;
}

Task<int> uring_rename(UringEngine& eng, const char* oldp, const char* newp) {
    if (eng.features().op_renameat && eng.options().meta_ops)
        co_return co_await eng.renameat(AT_FDCWD, oldp, AT_FDCWD, newp);
    co_return ::rename(oldp, newp) == 0 ? 0 : -errno;
}

Task<int> uring_unlink(UringEngine& eng, const char* path) {
    if (eng.features().op_unlinkat && eng.options().meta_ops)
        co_return co_await eng.unlinkat(AT_FDCWD, path, 0);
    co_return ::unlink(path) == 0 ? 0 : -errno;
}

Task<bool> uring_exists(UringEngine& eng, const char* path) {
    if (eng.features().op_statx && eng.options().meta_ops) {
        struct ::statx stx {};
        int r = co_await eng.statx(AT_FDCWD, path, 0, STATX_TYPE, &stx);
        co_return r == 0;
    }
    struct stat st{};
    co_return ::stat(path, &st) == 0;
}

// GET body: thin BodyReader over the read-ahead stream (roadmap §3.4 ①). Range comes
// naturally from the stream's [off, off+len) window; fd ownership transfers to the stream,
// which also keeps buffers/fd alive past an early destruction with reads still in flight
class UringStreamBodyReader final : public http::BodyReader {
public:
    UringStreamBodyReader(std::shared_ptr<UringEngine> eng, int fd, uint64_t off, uint64_t len)
        : stream_(std::move(eng), fd, off, len, /*own_fd=*/true), total_(len) {}

    Task<size_t> read(std::span<std::byte> buf) override { return stream_.read(buf); }
    std::optional<uint64_t> length() const override { return total_; }

private:
    UringReadStream stream_;
    uint64_t total_;
};

}  // namespace

XLocalFsBackend::XLocalFsBackend(fs::path root, fs::path staging,
                                 std::shared_ptr<ThreadPool> pool, UringOptions uring_opt,
                                 LocalFsOptions fs_opt, MetricsScope metrics)
    // Metrics pass through to the base class: xlocalfs and localfs share on-disk semantics,
    // so sharing the lights3_localfs_* namespace and distinguishing instances by backend
    // label is enough -- no need for a duplicate set of identical metrics
    : LocalFsBackend(std::move(root), std::move(staging), pool, fs_opt, std::move(metrics)),
      uring_(std::make_shared<UringEngine>(std::move(pool), uring_opt)) {}

Task<void> XLocalFsBackend::drain_to_tmp(http::BodyReader& body, UringWriteStream& ws,
                                         uint64_t& total_out, std::string& etag_out) {
    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t total = 0;
    for (;;) {
        // The body is read straight into the pipeline's block (a registered fixed buffer
        // when available) -- no bounce copy; MD5 runs on the pool thread as before
        std::span<std::byte> buf = co_await ws.acquire();
        size_t n = co_await body.read(buf);
        if (n == 0) {
            ws.commit(0);
            break;
        }
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
        ws.commit(n);
        total += n;
    }
    total_out = total;
    etag_out = md5.final_hex();
}

Task<void> XLocalFsBackend::sync_dir(fs::path dir) {
    if (!fsutil::fsync_enabled()) co_return;
    if (!uring_->features().op_fsync) {
        fsutil::fsync_dir(dir);
        co_return;
    }
    FdGuard d{::open(dir.c_str(), O_RDONLY | O_DIRECTORY)};
    if (d.fd < 0) co_return;  // same as fsync_dir: an unreadable directory must not take down the write path
    (void)co_await uring_->fsync(d.fd);  // failure silent, matching fsync_dir
}

Task<void> XLocalFsBackend::commit_prepared(fs::path dest, TmpFile& tmp, const ObjectMeta& meta,
                                            std::string_view key, bool xattr_ok) {
    fsutil::prepare_object_dest(dest, key);
    // Same ordering as fsutil::commit_object_file with prepared=true: data rename ->
    // directory fsync -> sidecar; only the first two go through the ring
    int r = co_await uring_rename(*uring_, tmp.path.c_str(), dest.c_str());
    if (r < 0) throw S3Error(S3ErrorCode::InternalError, "rename object failed");
    tmp.committed = true;
    co_await sync_dir(dest.parent_path());
    // Sidecar policy shared with localfs (roadmap §3.5): sync / deferred / lazy
    if (fsutil::finish_object_sidecar(dest, meta, staging_ / "put", opt_.sidecar, xattr_ok))
        defer_sidecar(std::move(dest), meta);
}

Task<PutResult> XLocalFsBackend::put_object(std::string_view bucket, std::string_view key,
                                            ObjectMeta meta, http::BodyReader& body,
                                            PutCondition cond) {
    // This override bypasses the base implementation, so the accounting entry point is
    // re-planted here (same base-class instance, no double counting)
    OpGuard g{this, Op::kPut};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    // 1. Stream into the staging temp file through the write pipeline, computing MD5 as we
    // go (roadmap §3.4 ①: up to write_depth blocks in flight while the next body block is
    // being received)
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = co_await uring_open(*uring_, tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_uring("open staging tmp", tmp.fd);

    UringWriteStream ws(uring_, tmp.fd, 0, body.length());
    uint64_t total = 0;
    std::string etag;
    co_await drain_to_tmp(body, ws, total, etag);

    meta.key = std::string(key);
    meta.size = total;
    meta.etag = etag;
    meta.last_modified = std::chrono::system_clock::now();

    // Original commit order preserved: write xattr -> persist data. The pipeline held the
    // final block back, so finish() can send it and the fdatasync as one linked chain
    // (roadmap §3.4 ③) -- for a small object that is the whole persistence in a single
    // submission
    bool xattr_ok = fsutil::set_meta_xattr(tmp.path, meta, fsutil::TierInfo{}, &xattr_);
    co_await ws.finish(fsutil::fsync_enabled());
    ::close(tmp.fd);
    tmp.fd = -1;

    // 2. Conflict check + data rename + sidecar commit. Per-key lock as in localfs: the
    // commit phase's two renames must not interleave with concurrent writes to the same
    // key; the conditional-PUT check is inside the same lock (PutCondition contract)
    auto lk = co_await commit_lock(bucket, key).acquire();
    co_await pool_->schedule();
    auto inv = invalidate_on_exit(bucket, key);  // roadmap §3.8
    fs::path dest = object_path(bucket, key);
    fsutil::check_put_condition(dest, cond, key);
    co_await commit_prepared(dest, tmp, meta, key, xattr_ok);
    g.ok = true;
    co_return PutResult{meta.etag};
}

Task<ObjectStream> XLocalFsBackend::get_object(std::string_view bucket, std::string_view key,
                                               std::optional<ByteRange> range) {
    // Same as localfs: latency measurement stops when the stream handle is ready, excluding
    // body transfer
    OpGuard g{this, Op::kGet};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    // Same as localfs: bucket existence is an unconditional precondition on every path
    // that reads metadata from disk, skipped only behind a cached record whose inode
    // stamp matches the fd's fstat (roadmap §3.8)
    fs::path path = object_path(bucket, key);
    // OPENAT via the ring (roadmap §3.4 ③): a cold dentry/inode lookup is disk work the
    // pool thread no longer waits on; fstat afterwards is in-memory (the open just
    // populated the inode) and stays a plain syscall
    int fd = co_await uring_open(*uring_, path.c_str(), O_RDONLY, 0);
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
        // Same as localfs: use the fd's fstat, so a concurrent overwrite cannot leave meta
        // and body coming from different inodes; the cached record must match that fstat
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
        // Same as localfs: stubbed between open and sidecar read -> report to tiered so it
        // goes to the cloud instead
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
        // fd ownership transfers to the reader's stream (read-ahead over [f, f+len))
        out.body = std::make_unique<UringStreamBodyReader>(uring_, fd, f, len);
    } catch (...) {
        ::close(fd);
        throw;
    }
    g.ok = true;
    co_return out;
}

Task<PutResult> XLocalFsBackend::upload_part(std::string_view bucket, std::string_view key,
                                             std::string_view upload_id, int part_no,
                                             http::BodyReader& body,
                                             const std::optional<PartChecksum>& checksum) {
    OpGuard g{this, Op::kUploadPart};
    validate_part_number(part_no);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));

    // Stream into the staging temp file through the write pipeline (same as PUT)
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = co_await uring_open(*uring_, tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_uring("open part tmp", tmp.fd);

    UringWriteStream ws(uring_, tmp.fd, 0, body.length());
    uint64_t total = 0;
    std::string etag;
    co_await drain_to_tmp(body, ws, total, etag);
    (void)total;
    // Same as localfs: part data persists first (final write + fdatasync linked); only
    // then is .md5 the readiness proof
    co_await ws.finish(fsutil::fsync_enabled());
    ::close(tmp.fd);
    tmp.fd = -1;

    // Order: data rename first, then write .md5 (re-upload of the same number is
    // last-write-wins, rename overwrites); rationale as in localfs -- the reverse order
    // could leave a valid .md5 paired with zero data blocks after a power loss
    std::string name = part_file_name(part_no);
    fs::path dest = up.dir / name;
    int r = co_await uring_rename(*uring_, tmp.path.c_str(), dest.c_str());
    if (r < 0) {
        // The upload may have been aborted (directory removed) while we were reading the body
        if (!fs::exists(up.dir))
            throw S3Error(S3ErrorCode::NoSuchUpload,
                          "The specified multipart upload does not exist.",
                          std::string(upload_id));
        throw S3Error(S3ErrorCode::InternalError, "rename part failed");
    }
    tmp.committed = true;
    co_await sync_dir(up.dir);
    // Verified part checksum rides in the .md5 kv sidecar (roadmap §2.2, same as localfs)
    std::vector<std::pair<std::string, std::string>> pkv{{"md5", etag}};
    if (checksum && !checksum->resolved().empty()) {
        pkv.emplace_back("checksum_algorithm", checksum->algorithm);
        pkv.emplace_back("checksum_value", checksum->resolved());
    }
    write_tsv(up.dir / (name + ".md5"), staging_ / "put", pkv);
    g.ok = true;
    co_return PutResult{etag};
}

Task<PutResult> XLocalFsBackend::complete_multipart(std::string_view bucket,
                                                    std::string_view key,
                                                    std::string_view upload_id,
                                                    std::span<const PartInfo> parts) {
    OpGuard g{this, Op::kCompleteMpu};
    validate_part_order(parts);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    require_bucket(bucket);

    // 1. Validate each declared part: it exists (STATX via the ring) and the ETag matches
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
        fs::path part_path = up.dir / name;
        if (stored.empty() || !co_await uring_exists(*uring_, part_path.c_str()) ||
            stored != strip_etag_quotes(p.etag))
            throw S3Error(S3ErrorCode::InvalidPart,
                          "One or more of the specified parts could not be found or the "
                          "ETag did not match.",
                          std::string(key));
        md5s.push_back(stored);
        digests.push_back(std::move(digest));
        paths.push_back(std::move(part_path));
    }

    // 2. Concatenate into the final temp file in declared order: per-part read-ahead
    // stream feeding the shared write pipeline -- part reads run ahead while previous
    // blocks are still being written (roadmap §3.4 ①)
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = co_await uring_open(*uring_, tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_uring("open complete tmp", tmp.fd);
    UringWriteStream ws(uring_, tmp.fd, 0, std::nullopt);
    uint64_t total = 0;
    std::vector<uint64_t> sizes;  // per-part layout for GET ?partNumber (roadmap §2.5)
    for (auto& path : paths) {
        int in = co_await uring_open(*uring_, path.c_str(), O_RDONLY, 0);
        if (in < 0) throw_uring("open part", in);
        struct stat pst{};
        if (::fstat(in, &pst) != 0) {
            ::close(in);
            throw_errno("fstat part");
        }
        UringReadStream rs(uring_, in, 0, uint64_t(pst.st_size));  // fd owned by the stream
        uint64_t part_bytes = 0;
        for (;;) {
            std::span<std::byte> wb = co_await ws.acquire();
            size_t n = co_await rs.read(wb);
            if (n == 0) {
                ws.commit(0);
                break;
            }
            ws.commit(n);
            part_bytes += n;
            total += n;
        }
        sizes.push_back(part_bytes);
    }
    // 3. Commit (same atomic path as PUT), then clean up the mpu directory
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    meta.part_sizes = std::move(sizes);
    PutResult result{meta.etag};
    apply_composite_checksum(digests, meta, result);  // roadmap §2.2
    bool xattr_ok = fsutil::set_meta_xattr(tmp.path, meta, fsutil::TierInfo{}, &xattr_);
    co_await ws.finish(fsutil::fsync_enabled());  // final write + fdatasync linked
    ::close(tmp.fd);
    tmp.fd = -1;
    {
        auto lk = co_await commit_lock(bucket, key).acquire();  // same as PUT
        co_await pool_->schedule();
        auto inv = invalidate_on_exit(bucket, key);
        co_await commit_prepared(object_path(bucket, key), tmp, meta, key, xattr_ok);
    }

    std::error_code ec;
    fs::remove_all(up.dir, ec);
    g.ok = true;
    co_return result;
}

Task<void> XLocalFsBackend::delete_object(std::string_view bucket, std::string_view key) {
    if (!uring_->features().op_unlinkat || !uring_->options().meta_ops)
        co_return co_await LocalFsBackend::delete_object(bucket, key);
    OpGuard g{this, Op::kDelete};
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    validate_fs_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    auto inv = invalidate_on_exit(bucket, key);
    fs::path path = object_path(bucket, key);
    std::string sidecar = path.string() + fsutil::kSidecarSuffix;
    // Idempotent like the base: absence is not an error, but real EACCES/EIO must
    // propagate -- a silent 204 would convince the client the delete happened
    int r = co_await uring_unlink(*uring_, path.c_str());
    if (r == -EISDIR) {
        // The key maps onto an existing prefix directory: match the base's fs::remove
        // semantics (an empty directory is removed, a non-empty one errors)
        std::error_code ec;
        fs::remove(path, ec);
        if (ec) throw S3Error(S3ErrorCode::InternalError, "delete object: " + ec.message());
        r = 0;
    }
    if (r < 0 && r != -ENOENT)
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("delete object: ") + std::strerror(-r));
    r = co_await uring_unlink(*uring_, sidecar.c_str());
    if (r < 0 && r != -ENOENT)
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("delete object sidecar: ") + std::strerror(-r));
    // Clean up empty parent directories up to the bucket root (same as the base)
    std::error_code ec;
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

Task<void> XLocalFsBackend::close() {
    uring_->shutdown();
    co_await LocalFsBackend::close();  // cancel the periodic mpu cleanup timer (base-class background task)
}

}  // namespace lights3::storage
