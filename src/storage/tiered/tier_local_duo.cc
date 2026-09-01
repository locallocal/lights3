#include "storage/tiered/tier_local_duo.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include "core/log.h"

namespace fs = std::filesystem;

namespace lights3::storage::tier {

using s3::S3Error;
using s3::S3ErrorCode;
using duostore::TierState;

namespace {

std::string ikey_of(std::string_view b, std::string_view k) {
    return std::string(b) + "/" + std::string(k);
}

TierInfo to_tier_info(const TierState& ts) {
    TierInfo t;
    t.tier = ts.tier == TierState::kRemote   ? Tier::kRemote
             : ts.tier == TierState::kCached ? Tier::kCached
                                             : Tier::kLocal;
    t.remote_etag = ts.remote_etag;
    t.remote_at = ts.remote_at;
    return t;
}

TierState to_tier_state(const TierInfo& t) {
    TierState ts;
    ts.tier = t.tier == Tier::kRemote   ? TierState::kRemote
              : t.tier == Tier::kCached ? TierState::kCached
                                        : TierState::kLocal;
    ts.remote_etag = t.remote_etag;
    ts.remote_at = t.remote_at;
    return ts;
}

}  // namespace

DuoStoreTierLocal::DuoStoreTierLocal(std::shared_ptr<DuoStoreBackend> duo)
    : duo_(std::move(duo)), state_dir_(duo_->root() / "tier") {
    fs::create_directories(state_dir_ / "tmp");
    table_.load(state_dir_ / "atime.tsv");
}

DuoStoreTierLocal::~DuoStoreTierLocal() = default;

Task<void> DuoStoreTierLocal::close() {
    flush_access();
    co_await duo_->close();
}

std::optional<LocalObject> DuoStoreTierLocal::read(std::string_view bucket, std::string_view key) {
    auto rec = duo_->tier_read(bucket, key);
    if (!rec) return std::nullopt;
    LocalObject o;
    o.meta = std::move(rec->meta);
    o.tier = to_tier_info(rec->tier);
    o.local_bytes = rec->data.total();  // a stub has no extents
    o.mtime = std::chrono::system_clock::to_time_t(o.meta.last_modified);
    return o;
}

TierInfo DuoStoreTierLocal::read_tier_only(std::string_view bucket, std::string_view key) {
    auto rec = duo_->tier_read(bucket, key);
    return rec ? to_tier_info(rec->tier) : TierInfo{};
}

std::optional<AccessRec> DuoStoreTierLocal::load_access(std::string_view bucket,
                                                        std::string_view key) {
    return table_.get(ikey_of(bucket, key));
}
void DuoStoreTierLocal::store_access(std::string_view bucket, std::string_view key,
                                     const AccessRec& rec) {
    table_.set(ikey_of(bucket, key), rec);
}
void DuoStoreTierLocal::erase_access(std::string_view bucket, std::string_view key) {
    table_.erase(ikey_of(bucket, key));
}
void DuoStoreTierLocal::flush_access() { table_.flush(state_dir_ / "atime.tsv", tmp_dir()); }

Task<std::unique_ptr<http::BodyReader>> DuoStoreTierLocal::open_snapshot(std::string_view bucket,
                                                                         std::string_view key,
                                                                         uint64_t) {
    // Extents are immutable and pinned by the reader; an overwrite in the meantime lands
    // new extents and sends these to the gcq, which grace-waits for the pin
    auto os = co_await duo_->get_object(bucket, key, std::nullopt);
    co_return std::move(os.body);
}

Task<void> DuoStoreTierLocal::commit_stub(std::string_view bucket, std::string_view key,
                                          const ObjectMeta& meta, const TierInfo& tier) {
    co_await duo_->tier_commit_stub(bucket, key, meta, to_tier_state(tier));
}

namespace {

// Bytes are staged in a scratch file (the data plane's writer is pull-based and its
// extents cannot stay open across the client's pull pace), then pumped into
// chunks/packs at commit time
class DuoCacheFill final : public ICacheFill {
public:
    DuoCacheFill(DuoStoreTierLocal& owner, std::string bucket, std::string key, fs::path tmp_path)
        : owner_(owner), bucket_(std::move(bucket)), key_(std::move(key)) {
        tmp_.path = std::move(tmp_path);
        tmp_.fd = ::open(tmp_.path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
    }
    bool ok() const { return tmp_.fd >= 0; }

    bool write(const std::byte* p, size_t n) override {
        const char* c = reinterpret_cast<const char*>(p);
        while (n > 0) {
            ssize_t w = ::write(tmp_.fd, c, n);
            if (w < 0) return false;
            c += w;
            n -= static_cast<size_t>(w);
            written_ += static_cast<uint64_t>(w);
        }
        return true;
    }

    Task<void> commit(const ObjectMeta& meta, const TierInfo& tier) override {
        int fd = ::dup(tmp_.fd);  // the reader owns its fd; TmpFile keeps unlinking on destruction
        if (fd < 0) fsutil::throw_errno("dup cache fill");
        fsutil::FdStreamReader body(fd, 0, written_, owner_.duostore()->pool());
        co_await owner_.duostore()->tier_commit_cached(bucket_, key_, body, meta, to_tier_state(tier));
    }

private:
    DuoStoreTierLocal& owner_;
    std::string bucket_, key_;
    fsutil::TmpFile tmp_;
    uint64_t written_ = 0;
};

}  // namespace

std::unique_ptr<ICacheFill> DuoStoreTierLocal::begin_cache_fill(std::string_view bucket,
                                                                std::string_view key) {
    auto f = std::make_unique<DuoCacheFill>(*this, std::string(bucket), std::string(key),
                                            tmp_dir() / fsutil::next_tmp_name());
    if (!f->ok()) return nullptr;
    return f;
}

bool DuoStoreTierLocal::cache_space_ok(uint64_t size, uint64_t min_free_bytes) const {
    struct statvfs sv{};
    if (::statvfs(duo_->root().c_str(), &sv) != 0) return false;
    uint64_t avail = uint64_t(sv.f_bavail) * sv.f_frsize;
    return avail > size + min_free_bytes;
}

std::optional<std::pair<double, uint64_t>> DuoStoreTierLocal::disk_usage() const {
    struct statvfs sv{};
    if (::statvfs(duo_->root().c_str(), &sv) != 0 || sv.f_blocks == 0) return std::nullopt;
    double used = 1.0 - double(sv.f_bavail) / double(sv.f_blocks);
    return std::pair(used, uint64_t(sv.f_blocks) * uint64_t(sv.f_frsize));
}

// Meta-driven enumeration: buckets → paged key listing → one record read per key for
// the tier/extent view (the listing carries metadata only)
class DuoWalker final : public IWalker {
public:
    explicit DuoWalker(DuoStoreTierLocal& owner) : owner_(owner) {}

    Task<std::vector<WalkEntry>> next() override {
        co_await owner_.duostore()->pool()->schedule();
        std::vector<WalkEntry> out;
        if (!started_) {
            started_ = true;
            buckets_ = co_await owner_.duo_->list_buckets();
        }
        while (out.empty() && bi_ < buckets_.size()) {
            const std::string& bucket = buckets_[bi_].name;
            ListOptions lopt;
            lopt.max_keys = 256;
            lopt.start_after = cursor_;
            ListResult page;
            try {
                page = co_await owner_.duo_->list_objects(bucket, lopt);
            } catch (const S3Error&) {  // bucket deleted mid-walk
                ++bi_;
                cursor_.clear();
                continue;
            }
            for (auto& m : page.objects) {
                auto rec = owner_.duo_->tier_read(bucket, m.key);
                if (!rec) continue;
                WalkEntry w;
                w.bucket = bucket;
                w.key = m.key;
                w.tier = to_tier_info(rec->tier).tier;
                w.size = rec->meta.size;
                w.local_bytes = rec->data.total();
                w.mtime = std::chrono::system_clock::to_time_t(rec->meta.last_modified);
                out.push_back(std::move(w));
            }
            if (page.is_truncated && !page.next_token.empty()) {
                cursor_ = page.next_token;
            } else {
                ++bi_;
                cursor_.clear();
            }
        }
        co_return out;
    }

private:
    DuoStoreTierLocal& owner_;
    bool started_ = false;
    std::vector<BucketInfo> buckets_;
    size_t bi_ = 0;
    std::string cursor_;
};

std::unique_ptr<IWalker> DuoStoreTierLocal::walk() { return std::make_unique<DuoWalker>(*this); }

}  // namespace lights3::storage::tier
