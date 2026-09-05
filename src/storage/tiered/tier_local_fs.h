// L3: ITierLocal over the localfs/xlocalfs disk layout (docs/tiered-storage.md §4,
// docs/storage/tiered.md §2). Tier state rides in the object's xattr/sidecar
// (fs_util TierInfo), the stub is a 0-length data file, commits are the fs_util
// rename primitives; access records live in a second xattr on the data file
// (user.lights3.access) — or in a resident table + TSV snapshot when the filesystem
// has no xattr support; the range cache is a sparse-file mirror tree under
// <staging>/tier/rcache.
#pragma once

#include <mutex>
#include <unordered_map>

#include "storage/localfs/localfs_backend.h"
#include "storage/tiered/tier_local.h"

namespace lights3::storage::tier {

inline constexpr const char* kAccessXattr = "user.lights3.access";

class LocalFsTierLocal final : public ITierLocal {
public:
    explicit LocalFsTierLocal(std::shared_ptr<LocalFsBackend> local);
    ~LocalFsTierLocal() override;

    IStorageBackend& backend() override { return *local_; }
    const std::shared_ptr<LocalFsBackend>& localfs() const { return local_; }
    const std::filesystem::path& state_dir() const override { return state_dir_; }
    std::filesystem::path tmp_dir() const override { return local_->staging() / "put"; }
    const char* kind() const override { return "localfs"; }
    Task<void> close() override;

    std::optional<LocalObject> read(std::string_view bucket, std::string_view key) override;
    TierInfo read_tier_only(std::string_view bucket, std::string_view key) override;

    std::optional<AccessRec> load_access(std::string_view bucket, std::string_view key) override;
    void store_access(std::string_view bucket, std::string_view key, const AccessRec& rec) override;
    void erase_access(std::string_view bucket, std::string_view key) override;
    bool access_resident() const override { return resident_; }
    void flush_access() override;

    Task<std::unique_ptr<http::BodyReader>> open_snapshot(std::string_view bucket,
                                                          std::string_view key,
                                                          uint64_t size) override;
    Task<void> commit_stub(std::string_view bucket, std::string_view key, const ObjectMeta& meta,
                           const TierInfo& tier) override;
    std::unique_ptr<ICacheFill> begin_cache_fill(std::string_view bucket,
                                                 std::string_view key) override;

    bool cache_space_ok(uint64_t size, uint64_t min_free_bytes) const override;
    std::optional<SpaceUsage> space_usage() const override;

    std::unique_ptr<IWalker> walk() override;

    bool supports_range_cache() const override { return true; }
    std::unique_ptr<IRangeCache> open_range_cache(std::string_view bucket, std::string_view key,
                                                  const LocalObject& obj,
                                                  uint64_t block_size) override;
    void drop_range_cache(std::string_view bucket, std::string_view key) override;
    uint64_t range_cache_bytes(std::string_view bucket, std::string_view key) const override;
    uint64_t sweep_range_cache() override;

    // Test/diagnostic hooks
    std::filesystem::path data_path(std::string_view bucket, std::string_view key) const {
        return local_->object_data_path(bucket, key);
    }
    std::filesystem::path rcache_data_path(std::string_view bucket, std::string_view key) const;
    std::filesystem::path rcache_map_path(std::string_view bucket, std::string_view key) const;

private:
    friend class FsRangeCache;
    friend class FsWalker;
    void load_access_table();  // resident mode + legacy atime.tsv migration

    std::shared_ptr<LocalFsBackend> local_;
    std::filesystem::path state_dir_;  // <staging>/tier
    bool resident_ = false;            // no xattr support → table mode
    // Resident table (table mode), or the legacy atime.tsv contents used as a
    // read-through fallback while xattr records are being populated (xattr mode)
    mutable std::mutex table_m_;
    std::unordered_map<std::string, AccessRec> table_;
    bool table_dirty_ = false;
};

}  // namespace lights3::storage::tier
