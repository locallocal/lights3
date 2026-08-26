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
using fsutil::commit_object_file;
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

// io_uring pread streaming reads (with an offset, so Range comes naturally);
// completion continuations resume via the thread pool, occupying no thread while reading disk
class UringBodyReader final : public http::BodyReader {
public:
    UringBodyReader(int fd, uint64_t offset, uint64_t remaining,
                    std::shared_ptr<UringEngine> eng)
        : fd_(fd), offset_(offset), remaining_(remaining), eng_(std::move(eng)) {}
    ~UringBodyReader() override { ::close(fd_); }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (remaining_ == 0) co_return 0;
        size_t want = std::min<uint64_t>(buf.size(), remaining_);
        int n = co_await eng_->read(fd_, buf.first(want), offset_);
        if (n < 0) throw_uring("io_uring read", n);
        if (n == 0) remaining_ = 0;  // file truncated externally, early EOF
        offset_ += static_cast<uint64_t>(n);
        remaining_ -= static_cast<uint64_t>(n);
        co_return static_cast<size_t>(n);
    }
    std::optional<uint64_t> length() const override { return total_; }
    void set_total(uint64_t t) { total_ = t; }

private:
    int fd_;
    uint64_t offset_;
    uint64_t remaining_;
    uint64_t total_ = 0;
    std::shared_ptr<UringEngine> eng_;
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

// Data persistence in the commit phase (docs/archive/gaps.md §6.3): this used to be a synchronous
// fdatasync -- pinning a pool thread waiting on disk, exactly what xlocalfs exists to
// eliminate. The FSYNC SQE takes the same completion path, and the thread returns to the
// pool while waiting
Task<void> XLocalFsBackend::sync_fd(int fd) {
    if (!fsutil::fsync_enabled()) co_return;
    if (!uring_->features().op_fsync) {
        co_await pool_->schedule();
        fsutil::fsync_file(fd);
        co_return;
    }
    int r = co_await uring_->fdatasync(fd);
    if (r < 0 && r != -EINVAL) throw_uring("io_uring fdatasync", r);  // EINVAL: fs unsupported
}

Task<void> XLocalFsBackend::write_all(int fd, std::span<const std::byte> data, uint64_t off) {
    while (!data.empty()) {
        int w = co_await uring_->write(fd, data, off);
        if (w < 0) throw_uring("io_uring write", w);
        if (w == 0) throw S3Error(S3ErrorCode::InternalError, "io_uring write returned 0");
        data = data.subspan(static_cast<size_t>(w));
        off += static_cast<uint64_t>(w);
    }
}

// Results come back through out-params rather than co_await-ing a pair carrying a
// std::string: when body.read throws (Content-MD5 mismatch, client disconnect), GCC still
// runs the destructor on the never-constructed binding target, presenting as a double free /
// SEGV on the put path. Out-params are fully constructed before the co_await, so unwinding
// destroys real objects (the test case in docs/archive/gaps.md §5.6 is exactly this shape)
Task<void> XLocalFsBackend::drain_to_tmp(http::BodyReader& body, int fd, uint64_t& total_out,
                                         std::string& etag_out) {
    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t total = 0;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        co_await write_all(fd, std::span<const std::byte>(buf, n), total);
        total += n;
    }
    total_out = total;
    etag_out = md5.final_hex();
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

    // 1. Stream into the staging temp file via io_uring, computing MD5 as we write
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open staging tmp");

    uint64_t total = 0;
    std::string etag;
    co_await drain_to_tmp(body, tmp.fd, total, etag);

    meta.key = std::string(key);
    meta.size = total;
    meta.etag = etag;
    meta.last_modified = std::chrono::system_clock::now();

    // The first two steps of commit_object_file (write xattr -> persist data) are done here
    // ourselves, in the same order, only swapping that blocking fdatasync for an FSYNC SQE;
    // then commit with prepared=true
    fsutil::set_meta_xattr(tmp.path, meta, fsutil::TierInfo{});
    co_await sync_fd(tmp.fd);
    ::close(tmp.fd);
    tmp.fd = -1;

    // 2. Conflict check + data rename + sidecar commit (synchronous calls, back on a pool
    // thread). Per-key lock as in localfs: the commit phase's two renames must not interleave
    // with concurrent writes to the same key; the conditional-PUT check is inside the same
    // lock (PutCondition contract)
    auto lk = co_await commit_lock(bucket, key).acquire();
    co_await pool_->schedule();
    fsutil::check_put_condition(object_path(bucket, key), cond, key);
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key,
                       /*prepared=*/true);
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
    require_bucket(bucket);  // same as localfs: bucket existence is an unconditional precondition, not something to check only on failure paths

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
        // Same as localfs: use the fd's fstat, so a concurrent overwrite cannot leave meta
        // and body coming from different inodes
        out.meta = fsutil::load_object_meta_stat(path, std::string(key), st, &tier);
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
        auto reader = std::make_unique<UringBodyReader>(fd, f, len, uring_);
        reader->set_total(len);
        out.body = std::move(reader);  // fd ownership transfers to the reader
    } catch (...) {
        ::close(fd);
        throw;
    }
    g.ok = true;
    co_return out;
}

Task<PutResult> XLocalFsBackend::upload_part(std::string_view bucket, std::string_view key,
                                             std::string_view upload_id, int part_no,
                                             http::BodyReader& body) {
    OpGuard g{this, Op::kUploadPart};
    validate_part_number(part_no);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));

    // Stream into the staging temp file via io_uring, computing the part MD5 as we write (same as PUT)
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open part tmp");

    uint64_t total = 0;
    std::string etag;
    co_await drain_to_tmp(body, tmp.fd, total, etag);
    (void)total;
    co_await sync_fd(tmp.fd);  // same as localfs: part data persists first; only then is .md5 the readiness proof
    co_await pool_->schedule();
    ::close(tmp.fd);
    tmp.fd = -1;

    // Order: data rename first, then write .md5 (re-upload of the same number is
    // last-write-wins, rename overwrites); rationale as in localfs -- the reverse order
    // could leave a valid .md5 paired with zero data blocks after a power loss
    std::string name = part_file_name(part_no);
    std::error_code ec;
    fs::rename(tmp.path, up.dir / name, ec);
    if (ec) {
        // The upload may have been aborted (directory removed) while we were reading the body
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

    // 1. Validate each declared part: it exists and the ETag matches
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

    // 2. Concatenate into the final temp file in declared order, reading and writing via io_uring
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open complete tmp");
    uint64_t total = 0;
    std::byte buf[256 * 1024];
    for (auto& path : paths) {
        FdGuard in{::open(path.c_str(), O_RDONLY)};
        if (in.fd < 0) throw_errno("open part");
        uint64_t off = 0;
        for (;;) {
            int n = co_await uring_->read(in.fd, std::span(buf), off);
            if (n < 0) throw_uring("io_uring read part", n);
            if (n == 0) break;
            co_await write_all(tmp.fd, std::span<const std::byte>(buf, n), total);
            off += static_cast<uint64_t>(n);
            total += static_cast<uint64_t>(n);
        }
    }
    // 3. Commit (same atomic path as PUT), then clean up the mpu directory
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    fsutil::set_meta_xattr(tmp.path, meta, fsutil::TierInfo{});
    co_await sync_fd(tmp.fd);
    ::close(tmp.fd);
    tmp.fd = -1;
    {
        auto lk = co_await commit_lock(bucket, key).acquire();  // same as PUT
        co_await pool_->schedule();
        commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key,
                           /*prepared=*/true);
    }

    std::error_code ec;
    fs::remove_all(up.dir, ec);
    g.ok = true;
    co_return PutResult{meta.etag};
}

Task<void> XLocalFsBackend::close() {
    uring_->shutdown();
    co_await LocalFsBackend::close();  // cancel the periodic mpu cleanup timer (base-class background task)
}

}  // namespace lights3::storage
