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

// Durability switch (docs/storage-backend.md §3.1): on by default -- a write already
// acknowledged with 200 surviving power loss is part of S3 semantics. Throughput-first
// deployments can turn it off with LIGHTS3_FSYNC=0 (test fixtures also use it for speed)
bool fsync_enabled() {
    static const bool on = [] {
        const char* v = std::getenv("LIGHTS3_FSYNC");
        return !(v && (std::string_view(v) == "0" || std::string_view(v) == "false"));
    }();
    return on;
}

void fsync_file(int fd) {
    if (!fsync_enabled()) return;
    if (::fdatasync(fd) != 0 && errno != EINVAL)  // EINVAL: target fs unsupported, ignore
        throw_errno("fdatasync");
}

void fsync_dir(const fs::path& dir) {
    if (!fsync_enabled()) return;
    // Persist the directory entry: rename itself only guarantees atomicity, not that the
    // parent directory has been persisted
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return;  // an unreadable directory should not take down the write path
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
    fsync_path(tmp);  // persist the content first, then let rename splice it into the tree
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
    // First-class metadata (docs/archive/gaps.md §5.2): empty values are not written, so existing
    // sidecars stay byte-for-byte identical
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

// When tier=local, do not write the tier/size/remote.* keys: keeps the format of existing
// sidecars unchanged
static void write_sidecar(const fs::path& sidecar, const ObjectMeta& meta,
                          const fs::path& staging_dir,
                          const TierInfo& tier = TierInfo{}) {
    write_tsv(sidecar, staging_dir, meta_kv(meta, tier));
}

// Metadata committed atomically together with the data file (docs/storage-backend.md
// §3.1): write the sidecar's TSV into the data file's extended attribute as well, so one
// rename commits data and metadata at once -- sidecar and data are two renames, and a
// crash in between leaves an inconsistent object of "new etag + old data" (or vice versa),
// whereas the xattr travels with the inode and can never be misaligned with the data it
// describes. The sidecar is still written (external tools / legacy compatibility, and the
// only source on filesystems without xattr support).
// Failure (ENOTSUP/E2BIG etc.) degrades to sidecar-only without taking down the write path
// -- but must leave a trace: degradation means falling back to the "two-rename"
// consistency model, and if it happens silently operators have no way to know
// (docs/archive/gaps.md §3.9). Warn only once per errno kind to avoid flooding the write path
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

// Returns nullopt = no xattr (legacy object / unsupported filesystem) → caller falls back
// to the sidecar
std::optional<std::string> get_meta_xattr(const fs::path& path) {
    char buf[8192];
    ssize_t n = ::getxattr(path.c_str(), kMetaXattr, buf, sizeof(buf));
    if (n >= 0) return std::string(buf, static_cast<size_t>(n));
    if (errno != ERANGE) return std::nullopt;
    // Exceeds the stack buffer: refetch at the actual size. Silently falling back to the
    // sidecar here could read stale metadata from a crash window -- if the xattr exists,
    // it is authoritative (docs/archive/gaps.md §3.9)
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
        // Only "prefix collides with an existing file" is a client error; ENOSPC/EACCES/EIO
        // are all 500 -- mapping them to 400 means clients won't retry and operators never
        // get the disk-full signal (docs/archive/gaps.md §3.9)
        if (ec == std::errc::not_a_directory || ec == std::errc::file_exists)
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Object key conflicts with an existing object path", std::string(key));
        throw S3Error(S3ErrorCode::InternalError,
                      "create object directory: " + ec.message(), std::string(key));
    }
    if (fs::is_directory(dest))
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key conflicts with an existing key prefix", std::string(key));

    // Order: data first, then sidecar (consistent with commit_cached). The reverse order
    // (old implementation) had a crash window of "sidecar with new etag/size + old data"
    // -- GET returns a body that does not match the ETag, silent corruption; this order's
    // window is "new data + old sidecar", reading an old etag with a new body, still
    // inconsistent but at least verifiable at GET time, and in the overwrite case the
    // common outcome is both being new. The caller holds the per-key lock so no concurrent
    // writer can slip between this pair of renames (tearing is eliminated by the lock;
    // only the single-writer crash window remains).
    // Metadata goes into the data file's xattr first: that single rename then commits data
    // and metadata together, so a crash can never leave an inconsistent "new etag with old
    // data" object (the sidecar is written afterwards, only for external tools and as the
    // fallback on filesystems without xattr support)
    if (!prepared) {
        set_meta_xattr(tmp.path, meta, TierInfo{});
        fsync_path(tmp.path);  // persist the data content first, then splice it into the tree
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

// ---- Tiered storage extensions (docs/tiered-storage.md §4) ----

// Metadata TSV parsing (xattr and sidecar share the same format)
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
    // xattr takes priority: it was committed in the same rename as the data and can never
    // describe another inode's content; when absent (legacy object / filesystem
    // unsupported / tiered's stub-cached commit) fall back to the sidecar
    if (auto blob = get_meta_xattr(data_path)) {
        std::istringstream in(*blob);
        parse_meta_tsv(in, meta, tier, declared_size);
    } else {
        std::ifstream f(data_path.string() + kSidecarSuffix, std::ios::binary);
        parse_meta_tsv(f, meta, tier, declared_size);
    }
    // A stub data file has zero length; the real size comes from the metadata; local keeps
    // using stat (legacy compatibility)
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
    // The opposite of stubbing: rename the data first, then write the sidecar. On a crash
    // in between, the sidecar still says remote (reads go to the cloud, correct), and the
    // data file is reclaimed by the scanner via "remote but size>0"; the reverse order has
    // a truncation window where sidecar=cached while the data is still a 0-length stub.
    //
    // The data must be persisted before the rename: the sidecar written afterwards is
    // fsynced, so if the data blocks are still in the page cache at power loss, after
    // restart the sidecar says tier=cached/size=N while the file is N bytes of zeros --
    // rename already committed the inode size, so get_object's StubRace check (comparing
    // st_size with the declared size) passes and the zero blocks are returned as object
    // content with a correct-looking ETag. Aligned word-for-word with
    // commit_object_file's ordering
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
    if (n == 0) remaining_ = 0;  // file truncated externally, early EOF
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
