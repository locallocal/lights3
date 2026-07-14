#include "storage/localfs/localfs_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <fstream>
#include <sstream>
#include <system_error>

#include "core/log.h"
#include "core/util/crypto.h"
#include "storage/listing.h"
#include "storage/multipart.h"

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

std::string next_tmp_name() {
    static std::atomic<uint64_t> seq{0};
    std::ostringstream os;
    os << ::getpid() << "-" << std::chrono::steady_clock::now().time_since_epoch().count()
       << "-" << seq.fetch_add(1);
    return os.str();
}

// 未提交则析构时删除
struct TmpFile {
    fs::path path;
    int fd = -1;
    bool committed = false;

    ~TmpFile() {
        if (fd >= 0) ::close(fd);
        if (!committed) {
            std::error_code ec;
            fs::remove(path, ec);
        }
    }
};

// key 不得使用内部保留名（sidecar/marker），避免与数据文件冲突
void reject_reserved_key(std::string_view key) {
    if (key.ends_with(LocalFsBackend::kSidecarSuffix) ||
        key.find(LocalFsBackend::kBucketMarker) != std::string_view::npos)
        throw S3Error(S3ErrorCode::InvalidArgument, "Object key uses a reserved name");
}

[[noreturn]] void throw_errno(const std::string& what) {
    throw S3Error(S3ErrorCode::InternalError, what + ": " + std::strerror(errno));
}

// pread 流式读取；每块经线程池执行（阻塞 IO 不占 HTTP 执行环境）
class FdBodyReader final : public http::BodyReader {
public:
    FdBodyReader(int fd, uint64_t offset, uint64_t remaining, std::shared_ptr<ThreadPool> pool)
        : fd_(fd), offset_(offset), remaining_(remaining), pool_(std::move(pool)) {}
    ~FdBodyReader() override { ::close(fd_); }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (remaining_ == 0) co_return 0;
        co_await pool_->schedule();
        size_t want = std::min<uint64_t>(buf.size(), remaining_);
        ssize_t n = ::pread(fd_, buf.data(), want, static_cast<off_t>(offset_));
        if (n < 0) throw_errno("pread");
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
    std::shared_ptr<ThreadPool> pool_;
};

}  // namespace

LocalFsBackend::LocalFsBackend(fs::path root, fs::path staging, std::shared_ptr<ThreadPool> pool)
    : root_(std::move(root)), staging_(std::move(staging)), pool_(std::move(pool)) {
    fs::create_directories(root_);
    fs::create_directories(staging_ / "put");
    fs::create_directories(staging_ / "mpu");
    cleanup_stale_uploads();
}

fs::path LocalFsBackend::bucket_dir(std::string_view bucket) const {
    return root_ / fs::path(std::string(bucket));
}

fs::path LocalFsBackend::object_path(std::string_view bucket, std::string_view key) const {
    return bucket_dir(bucket) / fs::path(std::string(key));
}

void LocalFsBackend::require_bucket(std::string_view bucket) const {
    if (!fs::exists(bucket_dir(bucket) / kBucketMarker))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(bucket));
}

// ---------- sidecar / manifest：k<TAB>v 行格式，tmp+rename 原子写 ----------

static void write_tsv(const fs::path& dest, const fs::path& tmp_dir,
                      const std::vector<std::pair<std::string, std::string>>& kv) {
    fs::path tmp = tmp_dir / ("meta-" + next_tmp_name());
    {
        std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
        if (!f) throw_errno("open meta tmp");
        for (auto& [k, v] : kv) f << k << "\t" << v << "\n";
        if (!f.flush()) throw_errno("write meta");
    }
    std::error_code ec;
    fs::rename(tmp, dest, ec);
    if (ec) {
        fs::remove(tmp, ec);
        throw S3Error(S3ErrorCode::InternalError, "rename meta file failed");
    }
}

static std::vector<std::pair<std::string, std::string>> read_tsv(const fs::path& path) {
    std::vector<std::pair<std::string, std::string>> out;
    std::ifstream f(path, std::ios::binary);
    std::string line;
    while (std::getline(f, line)) {
        auto tab = line.find('\t');
        if (tab == std::string::npos) continue;
        out.emplace_back(line.substr(0, tab), line.substr(tab + 1));
    }
    return out;
}

static void write_sidecar(const fs::path& sidecar, const ObjectMeta& meta,
                          const fs::path& staging_dir) {
    std::vector<std::pair<std::string, std::string>> kv{{"etag", meta.etag},
                                                        {"content_type", meta.content_type}};
    for (auto& [k, v] : meta.user_meta) kv.emplace_back("meta." + k, v);
    write_tsv(sidecar, staging_dir, kv);
}

// 建父目录 + 目录冲突检查 + 先 sidecar 后数据 rename（docs/04 §3.1 写入原子性）；
// PUT 与 complete_multipart 共用
static void commit_object_file(const fs::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                               const fs::path& staging_put, std::string_view key) {
    std::error_code ec;
    fs::create_directories(dest.parent_path(), ec);
    if (ec)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key conflicts with an existing object path", std::string(key));
    if (fs::is_directory(dest))
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key conflicts with an existing key prefix", std::string(key));

    write_sidecar(fs::path(dest.string() + LocalFsBackend::kSidecarSuffix), meta, staging_put);
    fs::rename(tmp.path, dest, ec);
    if (ec) {
        fs::remove(dest.string() + LocalFsBackend::kSidecarSuffix, ec);
        throw S3Error(S3ErrorCode::InternalError, "rename object failed");
    }
    tmp.committed = true;
}

ObjectMeta LocalFsBackend::load_meta(const fs::path& data_path, std::string key) const {
    ObjectMeta meta;
    meta.key = std::move(key);
    struct stat st{};
    if (::stat(data_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode))
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist", meta.key);
    meta.size = static_cast<uint64_t>(st.st_size);
    meta.last_modified = std::chrono::system_clock::from_time_t(st.st_mtime);

    std::ifstream f(data_path.string() + kSidecarSuffix, std::ios::binary);
    std::string line;
    while (std::getline(f, line)) {
        auto tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string k = line.substr(0, tab), v = line.substr(tab + 1);
        if (k == "etag") meta.etag = v;
        else if (k == "content_type") meta.content_type = v;
        else if (k.rfind("meta.", 0) == 0) meta.user_meta[k.substr(5)] = v;
    }
    return meta;
}

// ---------- bucket ----------

Task<void> LocalFsBackend::create_bucket(std::string_view bucket) {
    validate_bucket_name(bucket);
    co_await pool_->schedule();
    fs::path dir = bucket_dir(bucket);
    if (fs::exists(dir / kBucketMarker))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(bucket));
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create bucket dir: " + ec.message());
    std::ofstream marker(dir / kBucketMarker);
    if (!marker) throw_errno("create bucket marker");
    co_return;
}

Task<void> LocalFsBackend::delete_bucket(std::string_view bucket) {
    co_await pool_->schedule();
    require_bucket(bucket);
    fs::path dir = bucket_dir(bucket);
    for (auto& e : fs::directory_iterator(dir)) {
        if (e.path().filename() != kBucketMarker)
            throw S3Error(S3ErrorCode::BucketNotEmpty,
                          "The bucket you tried to delete is not empty", std::string(bucket));
    }
    fs::remove(dir / kBucketMarker);
    fs::remove(dir);
    co_return;
}

Task<bool> LocalFsBackend::bucket_exists(std::string_view bucket) {
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
                                           ObjectMeta meta, http::BodyReader& body) {
    validate_bucket_name(bucket);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    // 1. 流式写入 staging 临时文件，边写边算 MD5
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

    // 2. 冲突检查 + sidecar + rename 原子提交
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);
    co_return PutResult{meta.etag};
}

Task<ObjectStream> LocalFsBackend::get_object(std::string_view bucket, std::string_view key,
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
        auto reader = std::make_unique<FdBodyReader>(fd, f, len, pool_);
        reader->set_total(len);
        out.body = std::move(reader);  // fd 所有权交给 reader
    } catch (...) {
        ::close(fd);
        throw;
    }
    co_return out;
}

Task<ObjectMeta> LocalFsBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);
    co_return load_meta(object_path(bucket, key), std::string(key));
}

Task<void> LocalFsBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    fs::path path = object_path(bucket, key);
    std::error_code ec;
    fs::remove(path, ec);                             // 幂等：不存在也算成功
    fs::remove(path.string() + kSidecarSuffix, ec);
    // 清理空父目录直到 bucket 根
    fs::path dir = path.parent_path(), root = bucket_dir(bucket);
    while (dir != root && dir.string().size() > root.string().size()) {
        if (!fs::is_empty(dir, ec) || ec) break;
        fs::remove(dir, ec);
        if (ec) break;
        dir = dir.parent_path();
    }
    co_return;
}

Task<ListResult> LocalFsBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    co_await pool_->schedule();
    require_bucket(bucket);

    fs::path base = bucket_dir(bucket);
    std::vector<std::string> keys;
    for (auto it = fs::recursive_directory_iterator(base); it != fs::recursive_directory_iterator();
         ++it) {
        if (!it->is_regular_file()) continue;
        std::string name = it->path().filename().string();
        if (name == kBucketMarker || name.ends_with(kSidecarSuffix)) continue;
        keys.push_back(fs::relative(it->path(), base).generic_string());
    }
    std::sort(keys.begin(), keys.end());
    co_return apply_listing(keys, opt, [&](const std::string& k) {
        return load_meta(base / fs::path(k), k);
    });
}

// ---------- multipart（docs/04 §3.2）----------
// 布局：<staging>/mpu/<upload_id>/{manifest, part.NNNNN, part.NNNNN.md5}
// 分片先 md5 后数据文件（与 sidecar-先-data-后 一致，数据文件出现即分片就绪）

namespace {

std::string part_file_name(int part_no) {
    char buf[16];
    snprintf(buf, sizeof(buf), "part.%05d", part_no);
    return buf;
}

struct UploadState {
    fs::path dir;
    ObjectMeta meta;  // manifest 中记录的 content_type / user_meta
};

// upload_id 合法性 + manifest 存在 + bucket/key 匹配，任一不满足视为 NoSuchUpload
UploadState require_upload(const fs::path& staging, std::string_view bucket,
                           std::string_view key, std::string_view upload_id,
                           const std::vector<std::pair<std::string, std::string>>& manifest) {
    UploadState up;
    up.dir = staging / "mpu" / std::string(upload_id);
    std::string m_bucket, m_key;
    for (auto& [k, v] : manifest) {
        if (k == "bucket") m_bucket = v;
        else if (k == "key") m_key = v;
        else if (k == "content_type") up.meta.content_type = v;
        else if (k.rfind("meta.", 0) == 0) up.meta.user_meta[k.substr(5)] = v;
    }
    if (manifest.empty() || m_bucket != bucket || m_key != key)
        throw S3Error(S3ErrorCode::NoSuchUpload,
                      "The specified multipart upload does not exist.", std::string(upload_id));
    return up;
}

}  // namespace

// 读 manifest 前先做 id 格式与存在性检查（id 会拼进路径，格式校验兼防逃逸）
static std::vector<std::pair<std::string, std::string>> load_manifest(
    const fs::path& staging, std::string_view upload_id) {
    if (!is_valid_upload_id(upload_id)) return {};
    fs::path manifest = staging / "mpu" / std::string(upload_id) / "manifest";
    if (!fs::exists(manifest)) return {};
    return read_tsv(manifest);
}

Task<std::string> LocalFsBackend::create_multipart(std::string_view bucket,
                                                   std::string_view key, ObjectMeta meta) {
    validate_bucket_name(bucket);
    validate_object_key(key);
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
    for (auto& [k, v] : meta.user_meta) kv.emplace_back("meta." + k, v);
    write_tsv(dir / "manifest", staging_ / "put", kv);
    co_return id;
}

Task<PutResult> LocalFsBackend::upload_part(std::string_view bucket, std::string_view key,
                                            std::string_view upload_id, int part_no,
                                            http::BodyReader& body) {
    validate_part_number(part_no);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));

    // 流式写 staging 临时文件，边写边算分片 MD5（同 PUT）
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
    ::close(tmp.fd);
    tmp.fd = -1;
    std::string etag = md5.final_hex();

    // 先 md5 后数据文件；同号重传 last-write-wins（rename 覆盖）
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

Task<PutResult> LocalFsBackend::complete_multipart(std::string_view bucket,
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

    // 2. 按声明顺序拼接到最终临时文件
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

    // 3. 提交（与 PUT 相同的原子路径），随后清理 mpu 目录
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);

    std::error_code ec;
    fs::remove_all(up.dir, ec);
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

void LocalFsBackend::cleanup_stale_uploads() {
    std::error_code ec;
    auto now = fs::file_time_type::clock::now();
    for (auto& e : fs::directory_iterator(staging_ / "mpu", ec)) {
        if (!e.is_directory()) continue;
        std::error_code tec;
        fs::path manifest = e.path() / "manifest";
        auto t = fs::exists(manifest, tec) ? fs::last_write_time(manifest, tec)
                                           : fs::last_write_time(e.path(), tec);
        if (tec || now - t <= kMpuTtl) continue;
        fs::remove_all(e.path(), tec);
        if (!tec)
            LOG_INFO("localfs: removed stale multipart upload ",
                     e.path().filename().string());
    }
}

}  // namespace lights3::storage
