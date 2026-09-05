// L3: ITierLocal over DuoStoreBackend (roadmap §3.6 ⑥): tier state lives in the object
// record (meta_store.h TierState, codec v3), a stub is a record without extents, the two
// commits are CAS meta transactions (the old extents enter duostore's own gcq), the
// upload snapshot is a normal duostore GET (extents are immutable, GC grace covers the
// read). Access records use the resident table (no per-object in-place update primitive
// in the meta engines). No range cache.
#pragma once

#include "storage/duostore/duostore_backend.h"
#include "storage/tiered/tier_access_table.h"
#include "storage/tiered/tier_local.h"

namespace lights3::storage::tier {

class DuoStoreTierLocal final : public ITierLocal {
public:
    explicit DuoStoreTierLocal(std::shared_ptr<DuoStoreBackend> duo);
    ~DuoStoreTierLocal() override;

    IStorageBackend& backend() override { return *duo_; }
    const std::filesystem::path& state_dir() const override { return state_dir_; }
    std::filesystem::path tmp_dir() const override { return state_dir_ / "tmp"; }
    const char* kind() const override { return "duostore"; }
    Task<void> close() override;

    std::optional<LocalObject> read(std::string_view bucket, std::string_view key) override;
    TierInfo read_tier_only(std::string_view bucket, std::string_view key) override;

    std::optional<AccessRec> load_access(std::string_view bucket, std::string_view key) override;
    void store_access(std::string_view bucket, std::string_view key, const AccessRec& rec) override;
    void erase_access(std::string_view bucket, std::string_view key) override;
    bool access_resident() const override { return true; }
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

    const std::shared_ptr<DuoStoreBackend>& duostore() const { return duo_; }

private:
    friend class DuoCacheFill;
    friend class DuoWalker;
    std::shared_ptr<DuoStoreBackend> duo_;
    std::filesystem::path state_dir_;  // <root>/tier
    AccessTable table_;
};

}  // namespace lights3::storage::tier
