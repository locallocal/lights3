#include "storage/localfs/list_cache.h"

namespace lights3::storage::fsutil {

bool DirListCache::stamp_of(const std::filesystem::path& dir, Stamp& out) {
    struct stat st{};
    if (::stat(dir.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) return false;
    out.dev = st.st_dev;
    out.ino = st.st_ino;
    out.mtime = st.st_mtim;
    out.ctime = st.st_ctim;
    return true;
}

DirListCache::DirListCache(Options opt, std::shared_ptr<MetricCounter> hits,
                           std::shared_ptr<MetricCounter> misses,
                           std::shared_ptr<MetricGauge> resident)
    : opt_(opt), m_hits_(std::move(hits)), m_misses_(std::move(misses)),
      m_resident_(std::move(resident)) {}

DirEntries DirListCache::lookup(const std::string& dir, const Stamp& stamp) {
    if (!enabled()) return nullptr;
    std::lock_guard lk(m_);
    auto it = map_.find(dir);
    if (it == map_.end() || !(it->second.stamp == stamp)) {
        ++misses_;
        if (m_misses_) m_misses_->inc();
        if (it != map_.end()) {  // stale snapshot: drop now rather than waiting for LRU pressure
            total_entries_ -= it->second.entries->size();
            lru_.erase(it->second.lru);
            map_.erase(it);
            if (m_resident_) m_resident_->set(int64_t(total_entries_));
        }
        return nullptr;
    }
    ++hits_;
    if (m_hits_) m_hits_->inc();
    lru_.splice(lru_.begin(), lru_, it->second.lru);
    return it->second.entries;
}

void DirListCache::insert(const std::string& dir, const Stamp& stamp, DirEntries entries,
                          std::chrono::system_clock::time_point readdir_started) {
    if (!enabled() || !entries || entries->size() < opt_.min_dir_entries ||
        entries->size() > opt_.max_entries)
        return;
    // Racy-window rule: only cache a directory whose last modification is comfortably
    // older than the read; anything being written right now would be re-read anyway.
    // mtime only: entry add/remove/rename always moves mtime, whereas ctime also moves
    // on chmod/chown/utimensat -- changes that leave the name list intact
    auto started = std::chrono::system_clock::to_time_t(readdir_started);
    if (started - stamp.mtime.tv_sec < opt_.racy_window.count()) return;

    std::lock_guard lk(m_);
    if (auto it = map_.find(dir); it != map_.end()) {
        total_entries_ -= it->second.entries->size();
        lru_.erase(it->second.lru);
        map_.erase(it);
    }
    evict_locked(entries->size());
    lru_.push_front(dir);
    total_entries_ += entries->size();
    map_.emplace(dir, Node{stamp, std::move(entries), lru_.begin()});
    if (m_resident_) m_resident_->set(int64_t(total_entries_));
}

void DirListCache::evict_locked(size_t need) {
    while (!lru_.empty() && total_entries_ + need > opt_.max_entries) {
        auto it = map_.find(lru_.back());
        total_entries_ -= it->second.entries->size();
        map_.erase(it);
        lru_.pop_back();
    }
    if (m_resident_) m_resident_->set(int64_t(total_entries_));
}

DirListCache::Stats DirListCache::stats() const {
    std::lock_guard lk(m_);
    return {hits_, misses_, map_.size(), total_entries_};
}

}  // namespace lights3::storage::fsutil
