#include "app/admin_jobs.h"

#include <chrono>
#include <stdexcept>

#include "core/log.h"
#include "core/task.h"
#include "storage/localfs/localfs_backend.h"
#include "storage/tiered/tiered_backend.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif

namespace lights3 {

using nlohmann::json;

const char* job_op_name(JobOp op) {
    switch (op) {
        case JobOp::Fsck: return "fsck";
        case JobOp::DuoGc: return "gc";
        case JobOp::DuoScan: return "scan";
        case JobOp::TierScan: return "scan";
        case JobOp::TierGc: return "gc";
        case JobOp::TierReconcile: return "reconcile";
    }
    return "?";
}

const char* job_group_name(JobOp op) {
    switch (op) {
        case JobOp::Fsck: return "fsck";
        case JobOp::DuoGc:
        case JobOp::DuoScan: return "duostore";
        case JobOp::TierScan:
        case JobOp::TierGc:
        case JobOp::TierReconcile: return "tier";
    }
    return "?";
}

std::optional<JobOp> parse_job_op(std::string_view group, std::string_view op) {
    if (group == "fsck" && op == "fsck") return JobOp::Fsck;
    if (group == "duostore") {
        if (op == "gc") return JobOp::DuoGc;
        if (op == "scan") return JobOp::DuoScan;
    } else if (group == "tier") {
        if (op == "scan") return JobOp::TierScan;
        if (op == "gc") return JobOp::TierGc;
        if (op == "reconcile") return JobOp::TierReconcile;
    }
    return std::nullopt;
}

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

// The rounds are the ones the timers run (docs/cli.md §2.4): the stats they
// return are flattened field by field so the JSON mirrors the *Stats structs
JobOutcome run_tier_job(JobOp op, storage::TieredBackend& t) {
    JobOutcome out;
    out.kind = "tiered";
    switch (op) {
        case JobOp::TierScan: {
            auto st = sync_wait(t.scan_once());
            out.stats = {{"full", st.full},
                         {"walked", st.walked},
                         {"cold_picked", st.cold_picked},
                         {"recovered", st.recovered},
                         {"enrolled", st.enrolled},
                         {"stale", st.stale},
                         {"evicted", st.evicted},
                         {"evicted_bytes", st.evicted_bytes},
                         {"need_remaining", st.need_remaining}};
            return out;
        }
        case JobOp::TierGc: {
            auto st = sync_wait(t.run_gc_once());
            out.stats = {{"resolved", st.resolved},
                         {"removed_cloud", st.removed_cloud},
                         {"deferred", st.deferred},
                         {"failed", st.failed}};
            return out;
        }
        case JobOp::TierReconcile: {
            auto st = sync_wait(t.run_reconcile_once());
            out.stats = {{"cloud_objects", st.cloud_objects},
                         {"stubs_rebuilt", st.stubs_rebuilt},
                         {"orphans_deleted", st.orphans_deleted},
                         {"orphans_skipped", st.orphans_skipped},
                         {"refs_missing", st.refs_missing},
                         {"quarantined_new", st.quarantined_new},
                         {"quarantined_resolved", st.quarantined_resolved}};
            out.findings = st.refs_missing;
            return out;
        }
        default: break;
    }
    throw std::invalid_argument("not a tiered op");
}

#ifdef LIGHTS3_DUOSTORE
JobOutcome run_duo_job(JobOp op, storage::DuoStoreBackend& duo) {
    JobOutcome out;
    out.kind = "duostore";
    switch (op) {
        case JobOp::DuoGc: {
            auto st = sync_wait(duo.run_gc_once());
            out.stats = {{"reclaims_acked", st.reclaims_acked},
                         {"files_removed", st.files_removed},
                         {"skipped_grace", st.skipped_grace},
                         {"skipped_pinned", st.skipped_pinned},
                         {"skipped_leased", st.skipped_leased},
                         {"packs_removed", st.packs_removed},
                         {"uploads_expired", st.uploads_expired},
                         {"packs_sealed_aged", st.packs_sealed_aged},
                         {"packs_compacted", st.packs_compacted},
                         {"packs_compact_deferred", st.packs_compact_deferred},
                         {"records_migrated", st.records_migrated},
                         {"records_corrupt", st.records_corrupt},
                         {"packs_quarantined", st.packs_quarantined}};
            out.findings = st.records_corrupt + st.packs_quarantined;
            return out;
        }
        case JobOp::DuoScan: {
            auto st = sync_wait(duo.run_orphan_scan_once());
            out.stats = {{"chunks_scanned", st.chunks_scanned},
                         {"chunk_bytes", st.chunk_bytes},
                         {"packs_scanned", st.packs_scanned},
                         {"pack_bytes", st.pack_bytes},
                         {"orphans_removed", st.orphans_removed},
                         {"skipped_grace", st.skipped_grace},
                         {"skipped_pinned", st.skipped_pinned},
                         {"skipped_gcq", st.skipped_gcq},
                         {"skipped_leased", st.skipped_leased},
                         {"orphan_packs_removed", st.orphan_packs_removed},
                         {"packs_skipped_active", st.packs_skipped_active},
                         {"refs_missing", st.refs_missing},
                         {"pack_stats_missing", st.pack_stats_missing}};
            out.findings = st.refs_missing + st.pack_stats_missing;
            return out;
        }
        default: break;
    }
    throw std::invalid_argument("not a duostore op");
}
#endif

std::string hex16(uint64_t v) {
    char buf[17];
    snprintf(buf, sizeof buf, "%016llx", static_cast<unsigned long long>(v));
    return buf;
}

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

JobOutcome run_job(JobOp op, storage::IStorageBackend& backend, uint64_t max_bytes_per_sec) {
    switch (op) {
        case JobOp::Fsck: return run_scrub(backend, max_bytes_per_sec);
        case JobOp::DuoGc:
        case JobOp::DuoScan: {
#ifdef LIGHTS3_DUOSTORE
            if (auto* duo = dynamic_cast<storage::DuoStoreBackend*>(&backend))
                return run_duo_job(op, *duo);
#endif
            throw std::invalid_argument("this backend is not a duostore backend");
        }
        case JobOp::TierScan:
        case JobOp::TierGc:
        case JobOp::TierReconcile: {
            if (auto* t = dynamic_cast<storage::TieredBackend*>(&backend))
                return run_tier_job(op, *t);
            throw std::invalid_argument("this backend is not a tiered backend");
        }
    }
    throw std::invalid_argument("unknown op");
}

json quarantine_ledger(std::string_view group, storage::IStorageBackend& backend) {
    json entries = json::array();
    if (group == "duostore") {
#ifdef LIGHTS3_DUOSTORE
        if (auto* duo = dynamic_cast<storage::DuoStoreBackend*>(&backend)) {
            for (const auto& e : duo->quarantine_list())
                entries.push_back({{"pack_id", hex16(e.pack_id)},
                                   {"live_recs", e.live_recs},
                                   {"corrupt_records", e.corrupt_records},
                                   {"quarantined_at_ms", e.quarantined_ms},
                                   {"purged", e.purged}});
            return {{"kind", "duostore"}, {"entries", entries}};
        }
#endif
        throw std::invalid_argument("this backend is not a duostore backend");
    }
    if (group == "tier") {
        if (auto* t = dynamic_cast<storage::TieredBackend*>(&backend)) {
            for (const auto& e : t->quarantine_list())
                entries.push_back({{"kind", e.kind},
                                   {"bucket", e.bucket},
                                   {"key", e.key},
                                   {"etag", e.etag},
                                   {"first_seen_ms", e.first_seen * 1000},
                                   {"last_seen_ms", e.last_seen * 1000},
                                   {"count", e.count}});
            return {{"kind", "tiered"}, {"entries", entries}};
        }
        throw std::invalid_argument("this backend is not a tiered backend");
    }
    throw std::invalid_argument("no quarantine ledger for group '" + std::string(group) + "'");
}

// ---------- AdminJobs ----------

const AdminJobs::Job* AdminJobs::running_of(const Slots& s) {
    for (const auto& [op, j] : s)
        if (j.running) return &j;
    return nullptr;
}

uint64_t AdminJobs::start(const std::string& backend, JobOp op, uint64_t max_bytes_per_sec) {
    std::shared_ptr<storage::IStorageBackend> b;
    {
        std::lock_guard lk(m_);  // the set changes under backend hot add / remove
        auto it = backends_.find(backend);
        if (it == backends_.end())
            throw Failure{Error::NoSuchBackend, "no backend named '" + backend + "'"};
        b = it->second;
    }
    // Type check before taking the slot: an unsupported backend never becomes "busy"
    bool supported = false;
    switch (op) {
        case JobOp::Fsck:
            supported = dynamic_cast<storage::LocalFsBackend*>(b.get()) != nullptr;
#ifdef LIGHTS3_DUOSTORE
            supported = supported || dynamic_cast<storage::DuoStoreBackend*>(b.get()) != nullptr;
#endif
            if (!supported)
                throw Failure{Error::Unsupported,
                              "backend '" + backend +
                                  "' has no offline fsck (duostore, localfs and xlocalfs do)"};
            break;
        case JobOp::DuoGc:
        case JobOp::DuoScan:
#ifdef LIGHTS3_DUOSTORE
            supported = dynamic_cast<storage::DuoStoreBackend*>(b.get()) != nullptr;
#endif
            if (!supported)
                throw Failure{Error::Unsupported,
                              "backend '" + backend + "' is not a duostore backend"};
            break;
        case JobOp::TierScan:
        case JobOp::TierGc:
        case JobOp::TierReconcile:
            supported = dynamic_cast<storage::TieredBackend*>(b.get()) != nullptr;
            if (!supported)
                throw Failure{Error::Unsupported,
                              "backend '" + backend + "' is not a tiered backend"};
            break;
    }

    std::lock_guard lk(m_);
    Slots& slots = jobs_[backend];
    if (const Job* r = running_of(slots))
        throw Failure{Error::Busy, std::string(job_group_name(op)) + " " + job_op_name(op) +
                                       ": job " + std::to_string(r->id) +
                                       " is still running on '" + backend + "'"};
    Job& j = slots[op];
    if (j.thread.joinable()) j.thread.join();  // reap the previous run's thread
    j.id = next_id_++;
    j.running = true;
    j.max_mbps = max_bytes_per_sec / (1000 * 1000);
    j.started_ms = now_ms();
    j.finished_ms = 0;
    j.error.clear();
    uint64_t id = j.id;
    LOG_INFO("{} {} job {} started on '{}' (max {} MB/s)", job_group_name(op), job_op_name(op), id,
             backend, j.max_mbps);
    j.thread = std::thread([this, backend, op, b, max_bytes_per_sec, id] {
        JobOutcome out;
        std::string error;
        try {
            out = run_job(op, *b, max_bytes_per_sec);
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard lk2(m_);
        auto bt = jobs_.find(backend);
        if (bt == jobs_.end()) return;
        auto jt = bt->second.find(op);
        if (jt == bt->second.end() || jt->second.id != id) return;
        finish_job(jt->second, op, std::move(out), std::move(error));
    });
    return id;
}

void AdminJobs::finish_job(Job& j, JobOp op, JobOutcome out, std::string error) {
    j.running = false;
    j.finished_ms = now_ms();
    j.error = std::move(error);
    j.has_outcome = j.error.empty();
    if (j.has_outcome) j.outcome = std::move(out);
    if (!j.error.empty())
        LOG_ERROR("{} {} job {} failed: {}", job_group_name(op), job_op_name(op), j.id, j.error);
    else
        LOG_INFO("{} {} job {} finished: kind={} findings={} aborted={} ({} ms)",
                 job_group_name(op), job_op_name(op), j.id, j.outcome.kind, j.outcome.findings,
                 j.outcome.aborted, j.finished_ms - j.started_ms);
}

json AdminJobs::status(const std::string& backend, JobOp op) const {
    std::lock_guard lk(m_);
    if (!backends_.count(backend))
        throw Failure{Error::NoSuchBackend, "no backend named '" + backend + "'"};
    json s;
    s["backend"] = backend;
    s["op"] = job_op_name(op);
    auto bt = jobs_.find(backend);
    const Job* j = nullptr;
    if (bt != jobs_.end()) {
        auto jt = bt->second.find(op);
        if (jt != bt->second.end()) j = &jt->second;
        s["busy"] = running_of(bt->second) != nullptr;
    } else {
        s["busy"] = false;
    }
    if (!j) {
        s["running"] = false;
        s["job_id"] = nullptr;
        return s;
    }
    s["running"] = j->running;
    s["job_id"] = j->id;
    s["started_at_ms"] = j->started_ms;
    if (op == JobOp::Fsck) s["max_mbps"] = j->max_mbps;
    if (j->running) {
        s["duration_ms"] = now_ms() - j->started_ms;
        return s;
    }
    s["finished_at_ms"] = j->finished_ms;
    s["duration_ms"] = j->finished_ms - j->started_ms;
    if (!j->error.empty()) {
        s["error"] = j->error;
        return s;
    }
    s["kind"] = j->outcome.kind;
    s["findings"] = j->outcome.findings;
    s["aborted"] = j->outcome.aborted;
    s["stats"] = j->outcome.stats;
    return s;
}

json AdminJobs::quarantine(const std::string& backend, std::string_view group) const {
    std::shared_ptr<storage::IStorageBackend> b;
    {
        std::lock_guard lk(m_);
        auto it = backends_.find(backend);
        if (it == backends_.end())
            throw Failure{Error::NoSuchBackend, "no backend named '" + backend + "'"};
        b = it->second;
    }
    try {
        json j = quarantine_ledger(group, *b);
        j["backend"] = backend;
        return j;
    } catch (const std::invalid_argument& e) {
        throw Failure{Error::Unsupported, "backend '" + backend + "': " + e.what()};
    }
}

void AdminJobs::shutdown() {
    std::vector<std::thread> threads;
    {
        std::lock_guard lk(m_);
        for (auto& [name, slots] : jobs_)
            for (auto& [op, j] : slots)
                if (j.thread.joinable()) threads.push_back(std::move(j.thread));
    }
    for (auto& t : threads) t.join();
}

// ---------- backend hot add / remove (backlog-sequence ⑦) ----------

void AdminJobs::add_backend(const std::string& name, std::shared_ptr<storage::IStorageBackend> b) {
    std::lock_guard lk(m_);
    backends_[name] = std::move(b);
}

bool AdminJobs::busy(const std::string& name) const {
    std::lock_guard lk(m_);
    auto it = jobs_.find(name);
    return it != jobs_.end() && running_of(it->second) != nullptr;
}

bool AdminJobs::remove_backend(const std::string& name) {
    std::lock_guard lk(m_);
    auto it = jobs_.find(name);
    if (it != jobs_.end()) {
        if (running_of(it->second)) return false;
        for (auto& [op, j] : it->second)
            if (j.thread.joinable()) j.thread.join();
        jobs_.erase(it);
    }
    backends_.erase(name);
    return true;
}

}  // namespace lights3
