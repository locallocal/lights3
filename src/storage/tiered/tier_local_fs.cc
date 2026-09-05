#include "storage/tiered/tier_local_fs.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <charconv>
#include <cstring>
#include <fstream>
#include <sstream>

#include "core/log.h"

namespace fs = std::filesystem;

namespace lights3::storage::tier {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

std::string ikey_of(std::string_view b, std::string_view k) {
    return std::string(b) + "/" + std::string(k);
}

// user xattr capability probe on the staging filesystem (same filesystem as root)
bool xattr_supported(const fs::path& dir) {
    fs::path p = dir / ("access-probe-" + fsutil::next_tmp_name());
    int fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) return false;
    bool ok = ::fsetxattr(fd, kAccessXattr, "0 0 -1", 6, 0) == 0;
    ::close(fd);
    ::unlink(p.c_str());
    return ok;
}

std::string encode_access(const AccessRec& r) {
    return std::to_string(r.atime) + " " + std::to_string(r.hits) + " " +
           std::to_string(r.enrolled);
}

std::optional<AccessRec> decode_access(std::string_view s) {
    AccessRec r;
    const char* p = s.data();
    const char* end = s.data() + s.size();
    auto num = [&](auto& out) {
        while (p < end && *p == ' ') ++p;
        auto res = std::from_chars(p, end, out);
        if (res.ec != std::errc()) return false;
        p = res.ptr;
        return true;
    };
    if (!num(r.atime)) return std::nullopt;
    if (!num(r.hits)) r.hits = 0;         // legacy "atime only" values
    if (!num(r.enrolled)) r.enrolled = -1;
    return r;
}

// Data file relative path -> object key (directory marker maps back to "dir/")
std::string key_of_rel(std::string rel) {
    const std::string marker = std::string("/") + fsutil::kDirMarker;
    if (rel == fsutil::kDirMarker) return "/";
    if (rel.ends_with(marker)) rel.resize(rel.size() - std::strlen(fsutil::kDirMarker));
    return rel;
}

}  // namespace

// ---------- construction ----------

LocalFsTierLocal::LocalFsTierLocal(std::shared_ptr<LocalFsBackend> local)
    : local_(std::move(local)), state_dir_(local_->staging() / "tier") {
    fs::create_directories(state_dir_);
    fs::create_directories(state_dir_ / "rcache" / "data");
    fs::create_directories(state_dir_ / "rcache" / "map");
    resident_ = !xattr_supported(local_->staging() / "put");
    if (resident_)
        LOG_WARN("tiered: filesystem under {} has no xattr support; access records stay in a "
                 "resident table + atime.tsv snapshot (memory grows with the object count, "
                 "docs/tiered-storage.md §4.3)",
                 local_->root().string());
    load_access_table();
}

LocalFsTierLocal::~LocalFsTierLocal() = default;

Task<void> LocalFsTierLocal::close() {
    flush_access();
    co_await local_->close();
}

void LocalFsTierLocal::load_access_table() {
    // atime.tsv (legacy format "ikey\tatime", extended "ikey\tatime\thits\tenrolled"): the
    // whole table in table mode; in xattr mode the pre-upgrade snapshot serves as a
    // read-through fallback until each key's record has been rewritten into its xattr
    for (auto& [k, v] : fsutil::read_tsv(state_dir_ / "atime.tsv")) {
        std::string fields = v;
        for (auto& c : fields)
            if (c == '\t') c = ' ';
        if (auto r = decode_access(fields); r && r->atime > 0) table_[k] = *r;
    }
}

// ---------- state ----------

std::optional<LocalObject> LocalFsTierLocal::read(std::string_view bucket, std::string_view key) {
    fs::path path = local_->object_data_path(bucket, key);
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) return std::nullopt;
    LocalObject o;
    try {
        o.meta = fsutil::load_object_meta_stat(path, std::string(key), st, &o.tier);
    } catch (const S3Error&) {
        return std::nullopt;
    }
    o.local_bytes = static_cast<uint64_t>(st.st_size);
    o.mtime = static_cast<int64_t>(st.st_mtime);
    return o;
}

TierInfo LocalFsTierLocal::read_tier_only(std::string_view bucket, std::string_view key) {
    fs::path path = local_->object_data_path(bucket, key);
    TierInfo t;
    try {
        fsutil::load_object_meta(path, std::string(key), &t);
        return t;
    } catch (const S3Error&) {
    }
    // No data file: fall back to the sidecar alone (an orphaned stub sidecar still names
    // the cloud replica that must be queued for GC)
    for (auto& [k, v] : fsutil::read_tsv(path.string() + fsutil::kSidecarSuffix)) {
        if (k == "tier")
            t.tier = v == "remote" ? Tier::kRemote : v == "cached" ? Tier::kCached : Tier::kLocal;
        else if (k == "remote.etag") t.remote_etag = v;
        else if (k == "remote.at") t.remote_at = v;
    }
    return t;
}

// ---------- access records ----------

std::optional<AccessRec> LocalFsTierLocal::load_access(std::string_view bucket,
                                                       std::string_view key) {
    if (!resident_) {
        char buf[96];
        ssize_t n = ::getxattr(local_->object_data_path(bucket, key).c_str(), kAccessXattr, buf,
                               sizeof(buf));
        if (n > 0) return decode_access(std::string_view(buf, size_t(n)));
    }
    std::lock_guard lk(table_m_);
    auto it = table_.find(ikey_of(bucket, key));
    if (it == table_.end()) return std::nullopt;
    return it->second;
}

void LocalFsTierLocal::store_access(std::string_view bucket, std::string_view key,
                                    const AccessRec& rec) {
    std::string ik = ikey_of(bucket, key);
    if (!resident_) {
        std::string v = encode_access(rec);
        // Failure (object just deleted, ENOSPC on the xattr block) only costs coldness
        // precision — the mtime fallback still reaches a conclusion
        if (::setxattr(local_->object_data_path(bucket, key).c_str(), kAccessXattr, v.data(),
                       v.size(), 0) == 0) {
            std::lock_guard lk(table_m_);
            if (table_.erase(ik) > 0) table_dirty_ = true;  // legacy fallback entry superseded
            return;
        }
    }
    std::lock_guard lk(table_m_);
    table_[ik] = rec;
    table_dirty_ = true;
}

void LocalFsTierLocal::erase_access(std::string_view bucket, std::string_view key) {
    // xattr mode: the record dies with the inode (delete) or is meaningless on a fresh
    // stub inode; only the table entry needs removing
    std::lock_guard lk(table_m_);
    if (table_.erase(ikey_of(bucket, key)) > 0) table_dirty_ = true;
}

void LocalFsTierLocal::flush_access() {
    std::vector<std::pair<std::string, std::string>> kv;
    {
        std::lock_guard lk(table_m_);
        if (!table_dirty_) return;
        table_dirty_ = false;
        kv.reserve(table_.size());
        for (auto& [k, r] : table_) {
            if (k.find('\t') != std::string::npos || k.find('\n') != std::string::npos) continue;
            kv.emplace_back(k, std::to_string(r.atime) + "\t" + std::to_string(r.hits) + "\t" +
                                   std::to_string(r.enrolled));
        }
    }
    fs::path snap = state_dir_ / "atime.tsv";
    try {
        if (kv.empty()) {
            std::error_code ec;
            fs::remove(snap, ec);
        } else {
            fsutil::write_tsv(snap, tmp_dir(), kv);
        }
    } catch (const std::exception& e) {
        LOG_WARN("tiered: access table snapshot failed: {}", e.what());
        std::lock_guard lk(table_m_);
        table_dirty_ = true;  // retry next round
    }
}

// ---------- data plane ----------

Task<std::unique_ptr<http::BodyReader>> LocalFsTierLocal::open_snapshot(std::string_view bucket,
                                                                        std::string_view key,
                                                                        uint64_t size) {
    fs::path path = local_->object_data_path(bucket, key);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) fsutil::throw_errno("open for demote");
    struct stat st{};
    if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        ::close(fd);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist", std::string(key));
    }
    // fd ownership transfers to the reader: a concurrent overwrite/stubbing renames a new
    // inode over the path, the reader keeps streaming the old one to the end
    co_return std::make_unique<fsutil::FdStreamReader>(fd, 0, size, local_->pool());
}

Task<void> LocalFsTierLocal::commit_stub(std::string_view bucket, std::string_view key,
                                         const ObjectMeta& meta, const TierInfo& tier) {
    fs::path path = local_->object_data_path(bucket, key);
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);  // reconcile rebuilds into a possibly missing tree
    fsutil::commit_stub(path, meta, tier, tmp_dir());
    local_->invalidate_object_meta(bucket, key);  // roadmap §3.8: the record changed under the backend
    co_return;
}

namespace {

class FsCacheFill final : public ICacheFill {
public:
    FsCacheFill(LocalFsTierLocal& owner, std::string bucket, std::string key, fs::path tmp_path)
        : owner_(owner), bucket_(std::move(bucket)), key_(std::move(key)) {
        tmp_.path = std::move(tmp_path);
        tmp_.fd = ::open(tmp_.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    }
    bool ok() const { return tmp_.fd >= 0; }

    bool write(const std::byte* p, size_t n) override {
        const char* c = reinterpret_cast<const char*>(p);
        while (n > 0) {
            ssize_t w = ::write(tmp_.fd, c, n);
            if (w < 0) return false;
            c += w;
            n -= static_cast<size_t>(w);
        }
        return true;
    }

    Task<void> commit(const ObjectMeta& meta, const TierInfo& tier) override {
        ::close(tmp_.fd);
        tmp_.fd = -1;
        fsutil::commit_cached(owner_.data_path(bucket_, key_), tmp_, meta, tier, owner_.tmp_dir());
        owner_.localfs()->invalidate_object_meta(bucket_, key_);  // roadmap §3.8
        owner_.drop_range_cache(bucket_, key_);  // the whole object is local now
        co_return;
    }

private:
    LocalFsTierLocal& owner_;
    std::string bucket_, key_;
    fsutil::TmpFile tmp_;
};

}  // namespace

std::unique_ptr<ICacheFill> LocalFsTierLocal::begin_cache_fill(std::string_view bucket,
                                                               std::string_view key) {
    auto f = std::make_unique<FsCacheFill>(*this, std::string(bucket), std::string(key),
                                           tmp_dir() / fsutil::next_tmp_name());
    if (!f->ok()) return nullptr;
    return f;
}

// ---------- space ----------

bool LocalFsTierLocal::cache_space_ok(uint64_t size, uint64_t min_free_bytes) const {
    auto s = probe_space(local_->root());
    return s && s->avail_bytes > size + min_free_bytes;
}

std::optional<SpaceUsage> LocalFsTierLocal::space_usage() const {
    return probe_space(local_->root());
}

// ---------- enumeration ----------

class FsWalker final : public IWalker {
public:
    explicit FsWalker(LocalFsTierLocal& owner) : owner_(owner) {
        std::error_code ec;
        buckets_ = fs::directory_iterator(owner_.local_->root(), ec);
    }

    Task<std::vector<WalkEntry>> next() override {
        co_await owner_.local_->pool()->schedule();
        std::vector<WalkEntry> out;
        constexpr size_t kBatch = 256;
        while (out.size() < kBatch) {
            if (files_ == fs::recursive_directory_iterator()) {
                if (!open_next_bucket()) break;
                continue;
            }
            std::error_code ec;
            auto entry = *files_;
            files_.increment(ec);
            if (ec) {
                files_ = fs::recursive_directory_iterator();
                continue;
            }
            if (!entry.is_regular_file(ec)) continue;
            std::string name = entry.path().filename().string();
            if (name == fsutil::kBucketMarker || name.ends_with(fsutil::kSidecarSuffix)) continue;
            std::string key =
                key_of_rel(entry.path().lexically_relative(bucket_dir_).generic_string());
            if (key == "/") continue;
            struct stat st{};
            if (::stat(entry.path().c_str(), &st) != 0) continue;
            WalkEntry w;
            w.bucket = bucket_;
            w.key = std::move(key);
            TierInfo t;
            try {
                ObjectMeta m = fsutil::load_object_meta_stat(entry.path(), w.key, st, &t);
                w.size = m.size;
            } catch (const S3Error&) {
                continue;  // raced with a concurrent delete
            }
            w.tier = t.tier;
            w.local_bytes = static_cast<uint64_t>(st.st_size);
            w.mtime = static_cast<int64_t>(st.st_mtime);
            out.push_back(std::move(w));
        }
        co_return out;
    }

private:
    bool open_next_bucket() {
        std::error_code ec;
        for (; buckets_ != fs::directory_iterator(); buckets_.increment(ec)) {
            if (ec) return false;
            const auto& be = *buckets_;
            if (!be.is_directory(ec) || !fs::exists(be.path() / fsutil::kBucketMarker, ec)) continue;
            bucket_ = be.path().filename().string();
            bucket_dir_ = be.path();
            files_ = fs::recursive_directory_iterator(bucket_dir_, ec);
            buckets_.increment(ec);
            return !ec;
        }
        return false;
    }

    LocalFsTierLocal& owner_;
    fs::directory_iterator buckets_;
    fs::recursive_directory_iterator files_;
    std::string bucket_;
    fs::path bucket_dir_;
};

std::unique_ptr<IWalker> LocalFsTierLocal::walk() { return std::make_unique<FsWalker>(*this); }

// ---------- range cache (roadmap §3.6 ⑦) ----------
// Layout: <state>/rcache/data/<bucket>/<key> is a sparse file of the object's size whose
// present blocks hold cloud bytes; <state>/rcache/map/<bucket>/<key> is
// "v1\t<remote_etag>\t<block>\t<size>\n<bitmap hex>\n". The map is rewritten whole on
// mark_present (tmp+rename, no fsync: losing it after a crash just drops a cache). Two
// parallel trees rather than suffixes so no key can collide with another key's map

fs::path LocalFsTierLocal::rcache_data_path(std::string_view bucket, std::string_view key) const {
    return state_dir_ / "rcache" / "data" / fs::path(std::string(bucket)) / fs::path(std::string(key));
}
fs::path LocalFsTierLocal::rcache_map_path(std::string_view bucket, std::string_view key) const {
    return state_dir_ / "rcache" / "map" / fs::path(std::string(bucket)) / fs::path(std::string(key));
}

class FsRangeCache final : public IRangeCache {
public:
    FsRangeCache(LocalFsTierLocal& owner, std::string bucket, std::string key, uint64_t size,
                 std::string etag, uint64_t block)
        : owner_(owner), bucket_(std::move(bucket)), key_(std::move(key)), size_(size),
          etag_(std::move(etag)), block_(block), nblocks_((size + block - 1) / block),
          data_(owner.rcache_data_path(bucket_, key_)), map_(owner.rcache_map_path(bucket_, key_)) {
        bits_.assign((nblocks_ + 7) / 8, 0);
        if (!load_map()) {  // missing / other replica / other geometry: start empty
            owner_.drop_range_cache(bucket_, key_);
            std::fill(bits_.begin(), bits_.end(), 0);
        }
    }
    ~FsRangeCache() override {
        if (fd_ >= 0) ::close(fd_);
    }

    uint64_t block_size() const override { return block_; }
    uint64_t size() const override { return size_; }

    bool has(uint64_t first, uint64_t last) const override {
        if (last >= size_ || first > last) return false;
        for (uint64_t b = first / block_; b <= last / block_; ++b)
            if (!bit(b)) return false;
        return true;
    }

    std::unique_ptr<http::BodyReader> open(uint64_t first, uint64_t last) override {
        int fd = ::open(data_.c_str(), O_RDONLY);
        if (fd < 0) return nullptr;
        return std::make_unique<fsutil::FdStreamReader>(fd, first, last - first + 1,
                                                        owner_.local_->pool());
    }

    bool write(uint64_t off, const std::byte* p, size_t n) override {
        if (fd_ < 0) {
            std::error_code ec;
            fs::create_directories(data_.parent_path(), ec);
            fd_ = ::open(data_.c_str(), O_WRONLY | O_CREAT, 0644);
            if (fd_ < 0) return false;
            if (::ftruncate(fd_, off_t(size_)) != 0) return false;  // sparse container of the object's size
        }
        const char* c = reinterpret_cast<const char*>(p);
        while (n > 0) {
            ssize_t w = ::pwrite(fd_, c, n, off_t(off));
            if (w < 0) return false;
            c += w;
            off += static_cast<uint64_t>(w);
            n -= static_cast<size_t>(w);
        }
        return true;
    }

    void mark_present(uint64_t first_block, uint64_t last_block) override {
        // Merge with the on-disk map first: another filler of the same key may have
        // landed other blocks since we loaded (bits only ever get added)
        std::vector<uint8_t> mine = bits_;
        if (load_map())
            for (size_t i = 0; i < bits_.size() && i < mine.size(); ++i) bits_[i] |= mine[i];
        else
            bits_ = mine;
        for (uint64_t b = first_block; b <= last_block && b < nblocks_; ++b) bits_[b / 8] |= uint8_t(1u << (b % 8));
        static constexpr char kHex[] = "0123456789abcdef";
        std::string hex;
        hex.reserve(bits_.size() * 2);
        for (uint8_t v : bits_) {
            hex.push_back(kHex[v >> 4]);
            hex.push_back(kHex[v & 15]);
        }
        std::error_code ec;
        fs::create_directories(map_.parent_path(), ec);
        fs::path tmp = owner_.tmp_dir() / ("rcmap-" + fsutil::next_tmp_name());
        {
            std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
            f << "v1\t" << etag_ << "\t" << block_ << "\t" << size_ << "\n" << hex << "\n";
            if (!f) return;
        }
        fs::rename(tmp, map_, ec);
        if (ec) fs::remove(tmp, ec);
    }

    uint64_t resident_bytes() const override {
        struct stat st{};
        if (::stat(data_.c_str(), &st) != 0) return 0;
        return uint64_t(st.st_blocks) * 512;
    }

private:
    bool bit(uint64_t b) const { return b < nblocks_ && (bits_[b / 8] >> (b % 8)) & 1u; }

    // Parse the map; false when absent or it describes another replica/geometry
    bool load_map() {
        std::ifstream f(map_, std::ios::binary);
        if (!f) return false;
        std::string head, hex;
        if (!std::getline(f, head) || !std::getline(f, hex)) return false;
        std::istringstream hs(head);
        std::string ver, etag;
        uint64_t block = 0, size = 0;
        hs >> ver >> etag >> block >> size;
        if (ver != "v1" || etag != etag_ || block != block_ || size != size_) return false;
        if (hex.size() != bits_.size() * 2) return false;
        auto nib = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            return -1;
        };
        for (size_t i = 0; i < bits_.size(); ++i) {
            int hi = nib(hex[2 * i]), lo = nib(hex[2 * i + 1]);
            if (hi < 0 || lo < 0) return false;
            bits_[i] = uint8_t(hi << 4 | lo);
        }
        return true;
    }

    LocalFsTierLocal& owner_;
    std::string bucket_, key_;
    uint64_t size_;
    std::string etag_;
    uint64_t block_, nblocks_;
    fs::path data_, map_;
    std::vector<uint8_t> bits_;
    int fd_ = -1;
};

std::unique_ptr<IRangeCache> LocalFsTierLocal::open_range_cache(std::string_view bucket,
                                                                std::string_view key,
                                                                const LocalObject& obj,
                                                                uint64_t block_size) {
    if (obj.tier.tier != Tier::kRemote || obj.meta.size == 0 || block_size == 0) return nullptr;
    return std::make_unique<FsRangeCache>(*this, std::string(bucket), std::string(key),
                                          obj.meta.size, obj.tier.remote_etag, block_size);
}

void LocalFsTierLocal::drop_range_cache(std::string_view bucket, std::string_view key) {
    std::error_code ec;
    for (fs::path p : {rcache_data_path(bucket, key), rcache_map_path(bucket, key)}) {
        fs::remove(p, ec);
        // Prune the emptied mirror directories up to the bucket level
        fs::path dir = p.parent_path();
        fs::path stop = p.parent_path();
        for (int depth = 0; depth < 64; ++depth) {
            if (dir.filename() == std::string(bucket) || dir == state_dir_) break;
            if (!fs::is_empty(dir, ec) || ec) break;
            fs::remove(dir, ec);
            if (ec) break;
            dir = dir.parent_path();
        }
    }
}

uint64_t LocalFsTierLocal::range_cache_bytes(std::string_view bucket, std::string_view key) const {
    struct stat st{};
    if (::stat(rcache_data_path(bucket, key).c_str(), &st) != 0) return 0;
    return uint64_t(st.st_blocks) * 512;
}

uint64_t LocalFsTierLocal::sweep_range_cache() {
    uint64_t resident = 0;
    fs::path root = state_dir_ / "rcache" / "data";
    std::error_code ec;
    std::vector<std::pair<std::string, std::string>> drop;
    for (auto it = fs::recursive_directory_iterator(root, ec);
         !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        std::error_code tec;
        if (!it->is_regular_file(tec)) continue;
        fs::path rel = it->path().lexically_relative(root);
        auto rit = rel.begin();
        if (rit == rel.end()) continue;
        std::string bucket = rit->string();
        std::string key = rel.lexically_relative(fs::path(bucket)).generic_string();
        auto obj = read(bucket, key);
        bool keep = obj && obj->tier.tier == Tier::kRemote;
        if (keep) {  // the map must still describe this replica
            std::ifstream f(rcache_map_path(bucket, key), std::ios::binary);
            std::string head;
            std::string ver, etag;
            if (f && std::getline(f, head)) {
                std::istringstream hs(head);
                hs >> ver >> etag;
            }
            keep = etag == obj->tier.remote_etag;
        }
        if (!keep) {
            drop.emplace_back(bucket, key);
            continue;
        }
        struct stat st{};
        if (::stat(it->path().c_str(), &st) == 0) resident += uint64_t(st.st_blocks) * 512;
    }
    for (auto& [b, k] : drop) drop_range_cache(b, k);
    return resident;
}

}  // namespace lights3::storage::tier
