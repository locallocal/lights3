// L3: per-directory sorted-entry cache for localfs listing (roadmap §3.5 ②).
// Paging through a large directory used to redo readdir + sort of the whole directory
// on every page, so the cost of page N grew with N. The name list of a directory is
// invalidated by the directory's own inode identity + mtime/ctime (POSIX updates both on
// every entry add/remove/rename in that directory), which makes it cacheable with one
// stat(2) per directory per page instead of a full readdir. Only names and types are
// cached -- never per-object metadata, which changes without touching the directory.
#pragma once

#include <sys/stat.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/metrics.h"

namespace lights3::storage::fsutil {

// One directory entry as the listing walker sees it: files sort by name, subdirectories
// by name+"/", the directory-marker object by "" (see localfs_backend.cc ListWalker)
struct DirEntry {
    std::string sort_key;
    bool is_dir;
};
using DirEntries = std::shared_ptr<const std::vector<DirEntry>>;

class DirListCache {
public:
    struct Options {
        size_t max_entries = size_t(1) << 20;  // total cached DirEntry budget (0 = disabled)
        size_t min_dir_entries = 256;          // smaller directories are cheap to re-read; not cached
        // A directory whose mtime falls within this window before the readdir is not
        // cached: with coarse timestamp granularity a modification in the same tick as the
        // stamp would be indistinguishable from the stamped state (git's racy-index rule)
        std::chrono::seconds racy_window{2};
    };

    // Directory identity as observed by stat(2); a mismatch on any field means the name
    // list may have changed
    struct Stamp {
        dev_t dev = 0;
        ino_t ino = 0;
        struct timespec mtime{};
        struct timespec ctime{};
        bool operator==(const Stamp& o) const {
            return dev == o.dev && ino == o.ino && mtime.tv_sec == o.mtime.tv_sec &&
                   mtime.tv_nsec == o.mtime.tv_nsec && ctime.tv_sec == o.ctime.tv_sec &&
                   ctime.tv_nsec == o.ctime.tv_nsec;
        }
    };
    // false = stat failed (directory vanished); the caller falls through to a plain readdir
    static bool stamp_of(const std::filesystem::path& dir, Stamp& out);

    struct Stats {
        uint64_t hits = 0, misses = 0;
        size_t dirs = 0, entries = 0;
    };

    explicit DirListCache(Options opt, std::shared_ptr<MetricCounter> hits = {},
                          std::shared_ptr<MetricCounter> misses = {},
                          std::shared_ptr<MetricGauge> resident = {});

    bool enabled() const { return opt_.max_entries > 0; }
    // Snapshot matching (dir, stamp) or null. Counts a hit/miss either way
    DirEntries lookup(const std::string& dir, const Stamp& stamp);
    // stamp must have been taken **before** the readdir that produced entries (a
    // concurrent modification during the read then always shows as a stamp mismatch on
    // the next lookup); readdir_started is that moment, used for the racy-window rule.
    // Silently skips directories below min_dir_entries / inside the racy window
    void insert(const std::string& dir, const Stamp& stamp, DirEntries entries,
                std::chrono::system_clock::time_point readdir_started);
    Stats stats() const;

private:
    struct Node {
        Stamp stamp;
        DirEntries entries;
        std::list<std::string>::iterator lru;
    };
    void evict_locked(size_t need);

    Options opt_;
    std::shared_ptr<MetricCounter> m_hits_, m_misses_;
    std::shared_ptr<MetricGauge> m_resident_;
    mutable std::mutex m_;
    std::unordered_map<std::string, Node> map_;
    std::list<std::string> lru_;  // front = most recent
    size_t total_entries_ = 0;
    uint64_t hits_ = 0, misses_ = 0;
};

}  // namespace lights3::storage::fsutil
