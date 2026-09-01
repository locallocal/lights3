// L3: resident access-record table with a TSV snapshot (docs/tiered-storage.md §4.3).
// The fallback store for local sides that cannot attach a record to the object itself
// (duostore's meta has no in-place update primitive; a filesystem without xattrs):
// memory grows with the tracked object count, which the xattr store avoids
#pragma once

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "core/log.h"
#include "storage/localfs/fs_util.h"
#include "storage/tiered/tier_local.h"

namespace lights3::storage::tier {

class AccessTable {
public:
    // "ikey\tatime[\thits\tenrolled]" lines (legacy two-field snapshots load too)
    void load(const std::filesystem::path& snapshot) {
        std::lock_guard lk(m_);
        for (auto& [k, v] : fsutil::read_tsv(snapshot)) {
            AccessRec r;
            r.enrolled = -1;
            size_t a = v.find('\t');
            r.atime = std::atoll(v.substr(0, a).c_str());
            if (a != std::string::npos) {
                size_t b = v.find('\t', a + 1);
                r.hits = uint32_t(std::atoll(v.substr(a + 1, b == std::string::npos ? b : b - a - 1).c_str()));
                if (b != std::string::npos) r.enrolled = std::atoll(v.substr(b + 1).c_str());
            }
            if (r.atime > 0) map_[k] = r;
        }
    }
    std::optional<AccessRec> get(const std::string& ikey) const {
        std::lock_guard lk(m_);
        auto it = map_.find(ikey);
        if (it == map_.end()) return std::nullopt;
        return it->second;
    }
    void set(const std::string& ikey, const AccessRec& r) {
        std::lock_guard lk(m_);
        map_[ikey] = r;
        dirty_ = true;
    }
    void erase(const std::string& ikey) {
        std::lock_guard lk(m_);
        if (map_.erase(ikey) > 0) dirty_ = true;
    }
    size_t size() const {
        std::lock_guard lk(m_);
        return map_.size();
    }
    // tmp+rename write when dirty; an empty table removes the snapshot
    void flush(const std::filesystem::path& snapshot, const std::filesystem::path& tmp_dir) {
        std::vector<std::pair<std::string, std::string>> kv;
        {
            std::lock_guard lk(m_);
            if (!dirty_) return;
            dirty_ = false;
            for (auto& [k, r] : map_) {
                if (k.find('\t') != std::string::npos || k.find('\n') != std::string::npos) continue;
                kv.emplace_back(k, std::to_string(r.atime) + "\t" + std::to_string(r.hits) + "\t" +
                                       std::to_string(r.enrolled));
            }
        }
        try {
            if (kv.empty()) {
                std::error_code ec;
                std::filesystem::remove(snapshot, ec);
            } else {
                fsutil::write_tsv(snapshot, tmp_dir, kv);
            }
        } catch (const std::exception& e) {
            LOG_WARN("tiered: access table snapshot failed: {}", e.what());
            std::lock_guard lk(m_);
            dirty_ = true;  // retry next round
        }
    }

private:
    mutable std::mutex m_;
    std::unordered_map<std::string, AccessRec> map_;
    bool dirty_ = false;
};

}  // namespace lights3::storage::tier
