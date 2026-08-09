#include "storage/localfs/fs_util.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <system_error>

#include "core/log.h"
#include "storage/multipart.h"

namespace fs = std::filesystem;

namespace lights3::storage::fsutil {

using s3::S3Error;
using s3::S3ErrorCode;

std::string next_tmp_name() {
    static std::atomic<uint64_t> seq{0};
    std::ostringstream os;
    os << ::getpid() << "-" << std::chrono::steady_clock::now().time_since_epoch().count()
       << "-" << seq.fetch_add(1);
    return os.str();
}

TmpFile::~TmpFile() {
    if (fd >= 0) ::close(fd);
    if (!committed) {
        std::error_code ec;
        fs::remove(path, ec);
    }
}

void reject_reserved_key(std::string_view key) {
    if (key.ends_with(kSidecarSuffix) || key.find(kBucketMarker) != std::string_view::npos ||
        key.find(kDirMarker) != std::string_view::npos)
        throw S3Error(S3ErrorCode::InvalidArgument, "Object key uses a reserved name");
}

void throw_errno(const std::string& what) {
    throw S3Error(S3ErrorCode::InternalError, what + ": " + std::strerror(errno));
}

// 持久性开关（docs/storage-backend.md §3.1）：默认开——已 200 应答的写掉电不丢是
// S3 语义的一部分。吞吐优先的部署可用 LIGHTS3_FSYNC=0 关掉（测试夹具亦用它提速）
bool fsync_enabled() {
    static const bool on = [] {
        const char* v = std::getenv("LIGHTS3_FSYNC");
        return !(v && (std::string_view(v) == "0" || std::string_view(v) == "false"));
    }();
    return on;
}

void fsync_file(int fd) {
    if (!fsync_enabled()) return;
    if (::fdatasync(fd) != 0 && errno != EINVAL)  // EINVAL: 目标 fs 不支持，忽略
        throw_errno("fdatasync");
}

void fsync_dir(const fs::path& dir) {
    if (!fsync_enabled()) return;
    // 目录项落盘：rename 本身只保证原子，不保证父目录已持久化
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return;  // 目录不可读不该拖垮写路径
    ::fsync(fd);
    ::close(fd);
}

void fsync_path(const fs::path& path) {
    if (!fsync_enabled()) return;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    ::fdatasync(fd);
    ::close(fd);
}

void write_tsv(const fs::path& dest, const fs::path& tmp_dir,
               const std::vector<std::pair<std::string, std::string>>& kv) {
    fs::path tmp = tmp_dir / ("meta-" + next_tmp_name());
    {
        std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
        if (!f) throw_errno("open meta tmp");
        for (auto& [k, v] : kv) f << k << "\t" << v << "\n";
        if (!f.flush()) throw_errno("write meta");
    }
    fsync_path(tmp);  // 内容先落盘，再让 rename 把它接进目录树
    std::error_code ec;
    fs::rename(tmp, dest, ec);
    if (ec) {
        fs::remove(tmp, ec);
        throw S3Error(S3ErrorCode::InternalError, "rename meta file failed");
    }
    fsync_dir(dest.parent_path());
}

std::vector<std::pair<std::string, std::string>> read_tsv(const fs::path& path) {
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

std::vector<std::pair<std::string, std::string>> meta_kv(const ObjectMeta& meta,
                                                         const TierInfo& tier) {
    std::vector<std::pair<std::string, std::string>> kv{{"etag", meta.etag},
                                                        {"content_type", meta.content_type}};
    if (tier.tier != Tier::kLocal) {
        kv.emplace_back("tier", tier.tier == Tier::kRemote ? "remote" : "cached");
        kv.emplace_back("size", std::to_string(meta.size));
        kv.emplace_back("remote.etag", tier.remote_etag);
        kv.emplace_back("remote.at", tier.remote_at);
    }
    // 一等元数据（docs/gaps.md §5.2）：空值不落盘，存量 sidecar 因此逐字节不变
    for (auto& f : kStdMetaFields)
        if (!(meta.*f.field).empty()) kv.emplace_back(f.store_key, meta.*f.field);
    for (auto& [k, v] : meta.user_meta) kv.emplace_back("meta." + k, v);
    return kv;
}

std::string kv_to_tsv(const std::vector<std::pair<std::string, std::string>>& kv) {
    std::string out;
    for (auto& [k, v] : kv) out += k + "\t" + v + "\n";
    return out;
}

// tier=local 时不写 tier/size/remote.* 键：与存量 sidecar 格式保持一致
static void write_sidecar(const fs::path& sidecar, const ObjectMeta& meta,
                          const fs::path& staging_dir,
                          const TierInfo& tier = TierInfo{}) {
    write_tsv(sidecar, staging_dir, meta_kv(meta, tier));
}

// 元数据随数据文件一同原子提交（docs/storage-backend.md §3.1）：把 sidecar 的 TSV
// 同时写进数据文件的扩展属性，rename 一次即同时提交数据与元数据——sidecar 与数据
// 是两次 rename，中间崩溃会留下"新 etag + 旧数据"（或反之）的不一致对象，而 xattr
// 随 inode 走，绝不可能与它所描述的数据错位。sidecar 继续写（外部工具/存量兼容，
// 也是不支持 xattr 的文件系统上的唯一来源）。
// 失败（ENOTSUP/E2BIG 等）降级为 sidecar-only，不拖垮写路径——但必须留痕：
// 降级意味着退回"两次 rename"一致性模型，静默发生的话运维无从得知
//（docs/gaps.md §3.9）。同类 errno 只告警一次，防写路径刷屏
void set_meta_xattr(const fs::path& path, const ObjectMeta& meta, const TierInfo& tier) {
    std::string blob = kv_to_tsv(meta_kv(meta, tier));
    if (::setxattr(path.c_str(), kMetaXattr, blob.data(), blob.size(), 0) != 0) {
        static std::atomic<int> last_errno{0};
        int e = errno;
        if (last_errno.exchange(e) != e)
            LOG_WARN("setxattr({}) failed: {} — object metadata falls back to sidecar-only "
                     "(two-rename consistency model)",
                     path.string(), strerror(e));
    }
}

// 返回 nullopt = 无 xattr（存量对象 / 不支持的文件系统）→ 调用方回落 sidecar
std::optional<std::string> get_meta_xattr(const fs::path& path) {
    char buf[8192];
    ssize_t n = ::getxattr(path.c_str(), kMetaXattr, buf, sizeof(buf));
    if (n >= 0) return std::string(buf, static_cast<size_t>(n));
    if (errno != ERANGE) return std::nullopt;
    // 超出栈缓冲：按实际大小重取。此处静默回落 sidecar 的话，读到的可能是
    // 一次崩溃窗口里的旧元数据——xattr 存在即以它为准（docs/gaps.md §3.9）
    ssize_t sz = ::getxattr(path.c_str(), kMetaXattr, nullptr, 0);
    if (sz < 0) return std::nullopt;
    std::string out(static_cast<size_t>(sz), '\0');
    n = ::getxattr(path.c_str(), kMetaXattr, out.data(), out.size());
    if (n < 0) return std::nullopt;
    out.resize(static_cast<size_t>(n));
    return out;
}

void commit_object_file(const fs::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                        const fs::path& staging_put, std::string_view key, bool prepared) {
    std::error_code ec;
    fs::create_directories(dest.parent_path(), ec);
    if (ec) {
        // 只有"前缀撞上既有文件"是客户端错误；ENOSPC/EACCES/EIO 一律 500——
        // 映射成 400 的话客户端不会重试、运维拿不到磁盘满的信号（docs/gaps.md §3.9）
        if (ec == std::errc::not_a_directory || ec == std::errc::file_exists)
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Object key conflicts with an existing object path", std::string(key));
        throw S3Error(S3ErrorCode::InternalError,
                      "create object directory: " + ec.message(), std::string(key));
    }
    if (fs::is_directory(dest))
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key conflicts with an existing key prefix", std::string(key));

    // 顺序：先数据后 sidecar（与 commit_cached 一致）。反序（旧实现）的崩溃窗口是
    // "sidecar 新 etag/size + 数据仍旧"——GET 返回的 body 与 ETag 不符，静默损坏；
    // 本序的窗口是"数据新 + sidecar 旧"，读到的是旧 etag 配新 body，同样不一致但
    // 有 GET 时校验的余地，且覆盖写场景下更常见的是两者皆新。调用方持 per-key 锁
    // 保证这一对 rename 之间无并发写者插入（撕裂由锁消除，此处只余单写者崩溃窗口）
    // 元数据先进数据文件的 xattr：这一次 rename 即同时提交数据与元数据，
    // 崩溃不可能留下"新 etag 配旧数据"的不一致对象（sidecar 随后写，仅供
    // 外部工具与不支持 xattr 的文件系统回落）
    if (!prepared) {
        set_meta_xattr(tmp.path, meta, TierInfo{});
        fsync_path(tmp.path);  // 数据内容先落盘，再接进目录树
    }
    fs::rename(tmp.path, dest, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "rename object failed");
    tmp.committed = true;
    fsync_dir(dest.parent_path());
    write_sidecar(fs::path(dest.string() + kSidecarSuffix), meta, staging_put);
}

void check_put_condition(const fs::path& data_path, const PutCondition& cond,
                         std::string_view key) {
    if (!cond.active()) return;
    std::optional<ObjectMeta> cur;
    try {
        cur = load_object_meta(data_path, std::string(key));
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::NoSuchKey) throw;
    }
    if (cond.if_none_match && cur)
        throw S3Error(S3ErrorCode::PreconditionFailed,
                      "At least one of the pre-conditions you specified did not hold",
                      std::string(key));
    if (cond.if_match_etag) {
        if (!cur)
            throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                          std::string(key));
        if (*cond.if_match_etag != cur->etag)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold",
                          std::string(key));
    }
}

// ---- 分层存储扩展（docs/tiered-storage.md §4）----

// 元数据 TSV 解析（xattr 与 sidecar 同一格式）
static void parse_meta_tsv(std::istream& in, ObjectMeta& meta, TierInfo& tier,
                           uint64_t& declared_size) {
    std::string line;
    while (std::getline(in, line)) {
        auto tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string k = line.substr(0, tab), v = line.substr(tab + 1);
        if (k == "etag") meta.etag = v;
        else if (k == "content_type") meta.content_type = v;
        else if (k == "tier") tier.tier = (v == "remote") ? Tier::kRemote
                                          : (v == "cached") ? Tier::kCached
                                                            : Tier::kLocal;
        else if (k == "size") std::from_chars(v.data(), v.data() + v.size(), declared_size);
        else if (k == "remote.etag") tier.remote_etag = v;
        else if (k == "remote.at") tier.remote_at = v;
        else if (k.rfind("meta.", 0) == 0) meta.user_meta[k.substr(5)] = v;
        else {
            for (auto& f : kStdMetaFields)
                if (k == f.store_key) meta.*f.field = v;
        }
    }
}

ObjectMeta load_object_meta(const fs::path& data_path, std::string key, TierInfo* tier_out) {
    struct stat st{};
    if (::stat(data_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode))
        throw s3::S3Error(s3::S3ErrorCode::NoSuchKey, "The specified key does not exist", key);
    return load_object_meta_stat(data_path, std::move(key), st, tier_out);
}

ObjectMeta load_object_meta_stat(const fs::path& data_path, std::string key,
                                 const struct stat& st, TierInfo* tier_out) {
    ObjectMeta meta;
    meta.key = std::move(key);
    meta.size = static_cast<uint64_t>(st.st_size);
    meta.last_modified = std::chrono::system_clock::from_time_t(st.st_mtime);

    TierInfo tier;
    uint64_t declared_size = 0;
    // xattr 优先：它与数据同一次 rename 提交，绝不会描述别的 inode 的内容；
    // 缺失（存量对象 / 文件系统不支持 / tiered 的 stub-cached 提交）回落 sidecar
    if (auto blob = get_meta_xattr(data_path)) {
        std::istringstream in(*blob);
        parse_meta_tsv(in, meta, tier, declared_size);
    } else {
        std::ifstream f(data_path.string() + kSidecarSuffix, std::ios::binary);
        parse_meta_tsv(f, meta, tier, declared_size);
    }
    // stub 数据文件为 0 长度，真实大小以元数据为准；local 沿用 stat（兼容存量）
    if (tier.tier != Tier::kLocal) meta.size = declared_size;
    if (tier_out) *tier_out = tier;
    return meta;
}

void commit_stub(const fs::path& dest, const ObjectMeta& meta, const TierInfo& tier,
                 const fs::path& staging_put) {
    write_sidecar(fs::path(dest.string() + kSidecarSuffix), meta, staging_put, tier);
    TmpFile tmp{staging_put / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open stub tmp");
    ::close(tmp.fd);
    tmp.fd = -1;
    std::error_code ec;
    fs::rename(tmp.path, dest, ec);
    if (ec) throw s3::S3Error(s3::S3ErrorCode::InternalError, "rename stub failed");
    tmp.committed = true;
}

void commit_cached(const fs::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                   const TierInfo& tier, const fs::path& staging_put) {
    // 与 stub 化相反：先 rename 数据、后写 sidecar。中间崩溃时 sidecar 仍为
    // remote（读走云端，正确），数据文件由 scanner 按"remote 但 size>0"回收；
    // 反序则存在 sidecar=cached 而数据仍为 0 长度 stub 的截断窗口。
    //
    // 数据必须在 rename 之前落盘：随后写的 sidecar 是 fsync 过的，若数据块还在
    // page cache 就掉电，重启后 sidecar 说 tier=cached/size=N 而文件是 N 字节的
    // 零块——rename 已提交 inode size，get_object 的 StubRace 检查（比 st_size
    // 与声明 size）因此通过，把零块当对象内容返回且 ETag 正确。与
    // commit_object_file 的顺序逐字对齐
    fsync_path(tmp.path);
    std::error_code ec;
    fs::rename(tmp.path, dest, ec);
    if (ec) throw s3::S3Error(s3::S3ErrorCode::InternalError, "rename cached data failed");
    tmp.committed = true;
    fsync_dir(dest.parent_path());
    write_sidecar(fs::path(dest.string() + kSidecarSuffix), meta, staging_put, tier);
}

FdStreamReader::~FdStreamReader() { ::close(fd_); }

Task<size_t> FdStreamReader::read(std::span<std::byte> buf) {
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

std::string part_file_name(int part_no) {
    char buf[16];
    snprintf(buf, sizeof(buf), "part.%05d", part_no);
    return buf;
}

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
        else {
            for (auto& f : kStdMetaFields)
                if (k == f.store_key) up.meta.*f.field = v;
        }
    }
    if (manifest.empty() || m_bucket != bucket || m_key != key)
        throw S3Error(S3ErrorCode::NoSuchUpload,
                      "The specified multipart upload does not exist.", std::string(upload_id));
    return up;
}

std::vector<std::pair<std::string, std::string>> load_manifest(const fs::path& staging,
                                                               std::string_view upload_id) {
    if (!is_valid_upload_id(upload_id)) return {};
    fs::path manifest = staging / "mpu" / std::string(upload_id) / "manifest";
    if (!fs::exists(manifest)) return {};
    return read_tsv(manifest);
}

}  // namespace lights3::storage::fsutil
