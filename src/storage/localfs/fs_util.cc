#include "storage/localfs/fs_util.h"

#include <unistd.h>

#include <atomic>
#include <cerrno>
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

static void write_sidecar(const fs::path& sidecar, const ObjectMeta& meta,
                          const fs::path& staging_dir) {
    std::vector<std::pair<std::string, std::string>> kv{{"etag", meta.etag},
                                                        {"content_type", meta.content_type}};
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
