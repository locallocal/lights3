#include "storage/localfs/fs_util.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <fstream>
#include <sstream>
#include <system_error>

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
    if (key.ends_with(kSidecarSuffix) || key.find(kBucketMarker) != std::string_view::npos)
        throw S3Error(S3ErrorCode::InvalidArgument, "Object key uses a reserved name");
}

void throw_errno(const std::string& what) {
    throw S3Error(S3ErrorCode::InternalError, what + ": " + std::strerror(errno));
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
    std::error_code ec;
    fs::rename(tmp, dest, ec);
    if (ec) {
        fs::remove(tmp, ec);
        throw S3Error(S3ErrorCode::InternalError, "rename meta file failed");
    }
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

// tier=local 时不写 tier/size/remote.* 键：与存量 sidecar 格式保持一致
static void write_sidecar(const fs::path& sidecar, const ObjectMeta& meta,
                          const fs::path& staging_dir,
                          const TierInfo& tier = TierInfo{}) {
    std::vector<std::pair<std::string, std::string>> kv{{"etag", meta.etag},
                                                        {"content_type", meta.content_type}};
    if (tier.tier != Tier::kLocal) {
        kv.emplace_back("tier", tier.tier == Tier::kRemote ? "remote" : "cached");
        kv.emplace_back("size", std::to_string(meta.size));
        kv.emplace_back("remote.etag", tier.remote_etag);
        kv.emplace_back("remote.at", tier.remote_at);
    }
    for (auto& [k, v] : meta.user_meta) kv.emplace_back("meta." + k, v);
    write_tsv(sidecar, staging_dir, kv);
}

void commit_object_file(const fs::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                        const fs::path& staging_put, std::string_view key) {
    std::error_code ec;
    fs::create_directories(dest.parent_path(), ec);
    if (ec)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key conflicts with an existing object path", std::string(key));
    if (fs::is_directory(dest))
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key conflicts with an existing key prefix", std::string(key));

    write_sidecar(fs::path(dest.string() + kSidecarSuffix), meta, staging_put);
    fs::rename(tmp.path, dest, ec);
    if (ec) {
        fs::remove(dest.string() + kSidecarSuffix, ec);
        throw S3Error(S3ErrorCode::InternalError, "rename object failed");
    }
    tmp.committed = true;
}

// ---- 分层存储扩展（docs/tiered-storage.md §4）----

ObjectMeta load_object_meta(const fs::path& data_path, std::string key, TierInfo* tier_out) {
    ObjectMeta meta;
    meta.key = std::move(key);
    struct stat st{};
    if (::stat(data_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode))
        throw s3::S3Error(s3::S3ErrorCode::NoSuchKey, "The specified key does not exist",
                          meta.key);
    meta.size = static_cast<uint64_t>(st.st_size);
    meta.last_modified = std::chrono::system_clock::from_time_t(st.st_mtime);

    TierInfo tier;
    uint64_t sidecar_size = 0;
    std::ifstream f(data_path.string() + kSidecarSuffix, std::ios::binary);
    std::string line;
    while (std::getline(f, line)) {
        auto tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string k = line.substr(0, tab), v = line.substr(tab + 1);
        if (k == "etag") meta.etag = v;
        else if (k == "content_type") meta.content_type = v;
        else if (k == "tier") tier.tier = (v == "remote") ? Tier::kRemote
                                          : (v == "cached") ? Tier::kCached
                                                            : Tier::kLocal;
        else if (k == "size") std::from_chars(v.data(), v.data() + v.size(), sidecar_size);
        else if (k == "remote.etag") tier.remote_etag = v;
        else if (k == "remote.at") tier.remote_at = v;
        else if (k.rfind("meta.", 0) == 0) meta.user_meta[k.substr(5)] = v;
    }
    // stub 数据文件为 0 长度，真实大小以 sidecar 为准；local 沿用 stat（兼容存量）
    if (tier.tier != Tier::kLocal) meta.size = sidecar_size;
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
    // 反序则存在 sidecar=cached 而数据仍为 0 长度 stub 的截断窗口
    std::error_code ec;
    fs::rename(tmp.path, dest, ec);
    if (ec) throw s3::S3Error(s3::S3ErrorCode::InternalError, "rename cached data failed");
    tmp.committed = true;
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
