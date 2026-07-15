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

// io_uring pread 流式读取（带偏移，天然支持 Range）；
// 完成续体经线程池恢复，读盘期间不占任何线程
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
        if (n == 0) remaining_ = 0;  // 文件被外部截断，提前 EOF
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
                                 std::shared_ptr<ThreadPool> pool, unsigned queue_depth)
    : LocalFsBackend(std::move(root), std::move(staging), pool),
      uring_(std::make_shared<UringEngine>(std::move(pool), queue_depth)) {}

Task<void> XLocalFsBackend::write_all(int fd, std::span<const std::byte> data, uint64_t off) {
    while (!data.empty()) {
        int w = co_await uring_->write(fd, data, off);
        if (w < 0) throw_uring("io_uring write", w);
        if (w == 0) throw S3Error(S3ErrorCode::InternalError, "io_uring write returned 0");
        data = data.subspan(static_cast<size_t>(w));
        off += static_cast<uint64_t>(w);
    }
}

Task<std::pair<uint64_t, std::string>> XLocalFsBackend::drain_to_tmp(http::BodyReader& body,
                                                                     int fd) {
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
    co_return std::pair{total, md5.final_hex()};
}

Task<PutResult> XLocalFsBackend::put_object(std::string_view bucket, std::string_view key,
                                            ObjectMeta meta, http::BodyReader& body) {
    validate_bucket_name(bucket);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    // 1. 流式经 io_uring 写入 staging 临时文件，边写边算 MD5
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open staging tmp");

    auto [total, etag] = co_await drain_to_tmp(body, tmp.fd);
    ::close(tmp.fd);
    tmp.fd = -1;

    meta.key = std::string(key);
    meta.size = total;
    meta.etag = etag;
    meta.last_modified = std::chrono::system_clock::now();

    // 2. 冲突检查 + sidecar + rename 原子提交（同步调用，回到池线程）
    co_await pool_->schedule();
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);
    co_return PutResult{meta.etag};
}

Task<ObjectStream> XLocalFsBackend::get_object(std::string_view bucket, std::string_view key,
                                               std::optional<ByteRange> range) {
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();

    fs::path path = object_path(bucket, key);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        require_bucket(bucket);  // 区分 NoSuchBucket / NoSuchKey
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        ::close(fd);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }

    ObjectStream out;
    try {
        out.meta = load_meta(path, std::string(key));
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
        out.body = std::move(reader);  // fd 所有权交给 reader
    } catch (...) {
        ::close(fd);
        throw;
    }
    co_return out;
}

Task<PutResult> XLocalFsBackend::upload_part(std::string_view bucket, std::string_view key,
                                             std::string_view upload_id, int part_no,
                                             http::BodyReader& body) {
    validate_part_number(part_no);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));

    // 流式经 io_uring 写 staging 临时文件，边写边算分片 MD5（同 PUT）
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open part tmp");

    auto [total, etag] = co_await drain_to_tmp(body, tmp.fd);
    (void)total;
    ::close(tmp.fd);
    tmp.fd = -1;

    // 先 md5 后数据文件；同号重传 last-write-wins（rename 覆盖）
    co_await pool_->schedule();
    std::string name = part_file_name(part_no);
    write_tsv(up.dir / (name + ".md5"), staging_ / "put", {{"md5", etag}});
    std::error_code ec;
    fs::rename(tmp.path, up.dir / name, ec);
    if (ec) {
        // 读 body 期间上传可能已被 abort（目录被删）
        if (!fs::exists(up.dir))
            throw S3Error(S3ErrorCode::NoSuchUpload,
                          "The specified multipart upload does not exist.",
                          std::string(upload_id));
        throw S3Error(S3ErrorCode::InternalError, "rename part failed");
    }
    tmp.committed = true;
    co_return PutResult{etag};
}

Task<PutResult> XLocalFsBackend::complete_multipart(std::string_view bucket,
                                                    std::string_view key,
                                                    std::string_view upload_id,
                                                    std::span<const PartInfo> parts) {
    validate_part_order(parts);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    require_bucket(bucket);

    // 1. 校验每个声明的分片：存在且 ETag 匹配
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

    // 2. 按声明顺序经 io_uring 读写拼接到最终临时文件
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
    ::close(tmp.fd);
    tmp.fd = -1;

    // 3. 提交（与 PUT 相同的原子路径），随后清理 mpu 目录
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    co_await pool_->schedule();
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);

    std::error_code ec;
    fs::remove_all(up.dir, ec);
    co_return PutResult{meta.etag};
}

Task<void> XLocalFsBackend::close() {
    uring_->shutdown();
    co_return;
}

}  // namespace lights3::storage
