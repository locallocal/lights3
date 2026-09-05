#include "app/fsck_jobs.h"

#include <chrono>
#include <stdexcept>

#include "core/log.h"
#include "core/task.h"
#include "storage/localfs/localfs_backend.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif

namespace lights3 {

using nlohmann::json;

// duostore gets the deep manifest/crc/refs scrub, localfs/xlocalfs the ETag
// full-verify. Findings follow the fsck convention: warning-grade counters
// (refs_stale, unverifiable, orphan sidecars) are reported but not counted --
// they can be transient or expected on legacy data
FsckOutcome run_scrub(storage::IStorageBackend& backend, uint64_t max_bytes_per_sec) {
    FsckOutcome out;
#ifdef LIGHTS3_DUOSTORE
    if (auto* duo = dynamic_cast<storage::DuoStoreBackend*>(&backend)) {
        storage::duostore::DuoScrubOptions opt;
        opt.max_bytes_per_sec = max_bytes_per_sec;
        auto st = sync_wait(duo->run_scrub_once(opt));
        out.kind = "duostore";
        out.stats = {{"objects_scanned", st.objects_scanned},
                     {"parts_scanned", st.parts_scanned},
                     {"extents_checked", st.extents_checked},
                     {"bytes_read", st.bytes_read},
                     {"corrupt_extents", st.corrupt_extents},
                     {"unreadable_extents", st.unreadable_extents},
                     {"objects_bad", st.objects_bad},
                     {"refs_missing", st.refs_missing},
                     {"refs_stale", st.refs_stale},
                     {"meta_errors", st.meta_errors}};
        out.findings =
            st.corrupt_extents + st.unreadable_extents + st.refs_missing + st.meta_errors;
        out.aborted = st.aborted;
        return out;
    }
#endif
    if (auto* lfs = dynamic_cast<storage::LocalFsBackend*>(&backend)) {
        storage::FsScrubOptions opt;
        opt.max_bytes_per_sec = max_bytes_per_sec;
        auto st = sync_wait(lfs->run_scrub_once(opt));
        out.kind = "localfs";
        out.stats = {{"objects_scanned", st.objects_scanned},
                     {"bytes_read", st.bytes_read},
                     {"etag_mismatches", st.etag_mismatches},
                     {"read_errors", st.read_errors},
                     {"unverifiable", st.unverifiable},
                     {"skipped_stubs", st.skipped_stubs},
                     {"skipped_races", st.skipped_races},
                     {"orphan_sidecars", st.orphan_sidecars}};
        out.findings = st.etag_mismatches + st.read_errors;
        out.aborted = st.aborted;
        return out;
    }
    throw std::invalid_argument(
        "this backend type has no offline fsck (duostore, localfs and xlocalfs do)");
}

namespace {
int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

uint64_t FsckJobs::start(const std::string& backend, uint64_t max_bytes_per_sec) {
    auto it = backends_.find(backend);
    if (it == backends_.end())
        throw Failure{Error::NoSuchBackend, "no backend named '" + backend + "'"};
    std::shared_ptr<storage::IStorageBackend> b = it->second;
    // Type check before taking the slot: an unsupported backend never becomes "busy"
    if (!dynamic_cast<storage::LocalFsBackend*>(b.get())
#ifdef LIGHTS3_DUOSTORE
        && !dynamic_cast<storage::DuoStoreBackend*>(b.get())
#endif
    )
        throw Failure{Error::Unsupported,
                      "backend '" + backend +
                          "' has no offline fsck (duostore, localfs and xlocalfs do)"};

    std::lock_guard lk(m_);
    Job& j = jobs_[backend];
    if (j.running)
        throw Failure{Error::Busy, "fsck job " + std::to_string(j.id) + " is still running on '" +
                                       backend + "'"};
    if (j.thread.joinable()) j.thread.join();  // reap the previous run's thread
    j.id = next_id_++;
    j.running = true;
    j.max_mbps = max_bytes_per_sec / (1000 * 1000);
    j.started_ms = now_ms();
    j.finished_ms = 0;
    j.error.clear();
    uint64_t id = j.id;
    LOG_INFO("fsck job {} started on '{}' (max {} MB/s)", id, backend, j.max_mbps);
    j.thread = std::thread([this, backend, b, max_bytes_per_sec, id] {
        FsckOutcome out;
        std::string error;
        try {
            out = run_scrub(*b, max_bytes_per_sec);
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard lk2(m_);
        auto jt = jobs_.find(backend);
        if (jt == jobs_.end() || jt->second.id != id) return;
        finish_job(jt->second, std::move(out), std::move(error));
    });
    return id;
}

void FsckJobs::finish_job(Job& j, FsckOutcome out, std::string error) {
    j.running = false;
    j.finished_ms = now_ms();
    j.error = std::move(error);
    j.has_outcome = j.error.empty();
    if (j.has_outcome) j.outcome = std::move(out);
    if (!j.error.empty())
        LOG_ERROR("fsck job {} failed: {}", j.id, j.error);
    else
        LOG_INFO("fsck job {} finished: kind={} findings={} aborted={} ({} ms)", j.id,
                 j.outcome.kind, j.outcome.findings, j.outcome.aborted,
                 j.finished_ms - j.started_ms);
}

json FsckJobs::status(const std::string& backend) const {
    if (!backends_.count(backend))
        throw Failure{Error::NoSuchBackend, "no backend named '" + backend + "'"};
    json s;
    s["backend"] = backend;
    std::lock_guard lk(m_);
    auto it = jobs_.find(backend);
    if (it == jobs_.end()) {
        s["running"] = false;
        s["job_id"] = nullptr;
        return s;
    }
    const Job& j = it->second;
    s["running"] = j.running;
    s["job_id"] = j.id;
    s["started_at_ms"] = j.started_ms;
    s["max_mbps"] = j.max_mbps;
    if (j.running) {
        s["duration_ms"] = now_ms() - j.started_ms;
        return s;
    }
    s["finished_at_ms"] = j.finished_ms;
    s["duration_ms"] = j.finished_ms - j.started_ms;
    if (!j.error.empty()) {
        s["error"] = j.error;
        return s;
    }
    s["kind"] = j.outcome.kind;
    s["findings"] = j.outcome.findings;
    s["aborted"] = j.outcome.aborted;
    s["stats"] = j.outcome.stats;
    return s;
}

void FsckJobs::shutdown() {
    std::vector<std::thread> threads;
    {
        std::lock_guard lk(m_);
        for (auto& [name, j] : jobs_)
            if (j.thread.joinable()) threads.push_back(std::move(j.thread));
    }
    for (auto& t : threads) t.join();
}

}  // namespace lights3
