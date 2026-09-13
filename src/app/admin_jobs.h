// Application-level maintenance jobs on a live gateway: the offline integrity
// scrub (`run_scrub_once`, roadmap §3.1, backlog-sequence ③) plus the on-demand
// background rounds the offline CLI already exposes (`lights3 duostore gc|scan`,
// `lights3 tier scan|gc|reconcile`, docs/usage/cli.md §2.4) -- each run against the
// application's backends, one job per backend at a time, on a dedicated thread,
// with the outcome kept for polling. Drives `lights3 fsck` (run_scrub,
// synchronous) and the admin endpoints (AdminJobs, asynchronous):
//   POST/GET /-/admin/fsck/<backend>
//   POST/GET /-/admin/duostore/<backend>/gc|scan    GET /-/admin/duostore/<backend>/quarantine
//   POST/GET /-/admin/tier/<backend>/scan|gc|reconcile  GET /-/admin/tier/<backend>/quarantine
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "storage/backend.h"

namespace lights3 {

// One completed job. `kind` is the backend type the round ran on ("duostore" |
// "localfs" | "tiered"); `stats` is the backend's report as JSON (field names =
// the *Stats members); `findings` is the sum of the "data is in danger"
// counters (exit code 1 for the CLIs)
struct JobOutcome {
    std::string kind;
    nlohmann::json stats;
    uint64_t findings = 0;
    // backend close interrupted the round (stats are partial)
    bool aborted = false;
};
using FsckOutcome = JobOutcome;

// The maintenance operations. Fsck runs on duostore / localfs / xlocalfs, the
// Duo* ops on duostore, the Tier* ops on tiered; anything else is Unsupported.
// The Table* ops are resource-level custom jobs (docs/architecture/s3-tables-design.md §9):
// their resource is "tables:<bucket>/<ns-path>/<t>" and the work is a function
enum class JobOp { Fsck, DuoGc, DuoScan, TierScan, TierGc, TierReconcile, TablePlan, TableRun, TablePurge };
// "fsck" | "gc" | "scan" | "reconcile" | "plan" | "run" | "purge": the op's name on the
// admin plane and in the status document ("op"); the group ("fsck" | "duostore" |
// "tier" | "tables") is the endpoint prefix
const char* job_op_name(JobOp op);
const char* job_group_name(JobOp op);
// The (group, op) pair of an endpoint, or nullopt when the group has no such op
std::optional<JobOp> parse_job_op(std::string_view group, std::string_view op);

// Dispatch on the backend type and run one scrub round synchronously (the caller
// is a thread that may block). Throws std::invalid_argument for a backend type
// without an offline scrub (memory, cloudproxy, tiered)
FsckOutcome run_scrub(storage::IStorageBackend& backend, uint64_t max_bytes_per_sec);
// Fold an extension's outcome into a scrub's: findings add up, the extension's stats
// land under stats[<ext.kind>]
void merge_outcome(JobOutcome& base, const JobOutcome& ext);
// Same for every op (Fsck delegates to run_scrub; max_bytes_per_sec only
// applies to Fsck). Findings: duostore gc = records_corrupt + packs_quarantined,
// duostore scan = refs_missing + pack_stats_missing, tier reconcile =
// refs_missing, tier scan / gc = 0 (nothing they count is a loss signal)
JobOutcome run_job(JobOp op, storage::IStorageBackend& backend, uint64_t max_bytes_per_sec);
// The corrupt-pack (duostore) / reconciliation (tiered) quarantine ledger as
// {"backend"?, "kind", "entries": [...]} -- synchronous and read-only. Throws
// std::invalid_argument when `group` ("duostore" | "tier") does not match the
// backend type
nlohmann::json quarantine_ledger(std::string_view group, storage::IStorageBackend& backend);

class AdminJobs {
public:
    enum class Error { NoSuchBackend, Unsupported, Busy };
    struct Failure {
        Error code;
        std::string message;
    };

    explicit AdminJobs(std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends)
        : backends_(std::move(backends)) {}
    ~AdminJobs() { shutdown(); }

    // Start a job; returns its id. Throws Failure{Busy} while any job runs on the
    // backend (the rounds share the backend's maintenance state, so they are
    // serialized whatever their op), {NoSuchBackend} / {Unsupported} before
    // anything starts
    uint64_t start(const std::string& backend, JobOp op, uint64_t max_bytes_per_sec = 0);
    // A custom job on an arbitrary resource key (no backend check): fn runs on a
    // dedicated thread, its outcome is kept like any other job. Throws Failure{Busy}
    // while any job runs on the resource
    uint64_t start_custom(const std::string& resource, JobOp op, std::function<JobOutcome()> fn);
    // {"backend","op","running","job_id","started_at_ms","finished_at_ms",
    //  "duration_ms","max_mbps"(fsck),"kind","findings","aborted","stats"}: the
    // most recent job of that op on that backend (job_id null before the first);
    // "running" is true only while that op's job runs -- "busy" says whether any
    // op does. Throws Failure{NoSuchBackend}
    // For a custom resource (never a backend) the same document with "backend" = resource
    nlohmann::json status(const std::string& backend, JobOp op) const;
    // The job with that id, whatever its resource / op; nullopt = unknown (ids of jobs
    // whose resource was removed are forgotten too)
    std::optional<nlohmann::json> status_by_id(uint64_t id) const;
    // The quarantine ledger of the backend (see quarantine_ledger). Throws
    // Failure{NoSuchBackend} / {Unsupported}
    nlohmann::json quarantine(const std::string& backend, std::string_view group) const;
    // Wait for every running job (backends are closed by the caller first, which
    // makes a round abort promptly)
    void shutdown();
    // Backend hot add / remove (backlog-sequence ⑦). remove returns false while a
    // job runs on that backend (the caller refuses the removal); the outcomes are
    // dropped with the backend
    void add_backend(const std::string& name, std::shared_ptr<storage::IStorageBackend> b);
    bool remove_backend(const std::string& name);
    // a job of any op is running on that backend
    bool busy(const std::string& name) const;
    // Extra fsck work after run_scrub (S3 Tables catalog reconciliation,
    // docs/architecture/s3-tables-design.md §14 ③): called with the backend name inside the job
    // thread; a returned outcome is merged into the scrub's (findings added, stats under
    // its kind). nullopt = nothing to add for that backend
    using FsckExtension = std::function<std::optional<JobOutcome>(const std::string& backend)>;
    void set_fsck_extension(FsckExtension ext) {
        std::lock_guard lk(m_);
        fsck_extension_ = std::move(ext);
    }

private:
    struct Job {
        uint64_t id = 0;
        bool running = false;
        uint64_t max_mbps = 0;
        // unix ms
        int64_t started_ms = 0;
        // 0 while running
        int64_t finished_ms = 0;
        // of the last completed job
        JobOutcome outcome;
        bool has_outcome = false;
        // exception text if the last job threw
        std::string error;
        std::thread thread;
    };
    // one slot per op, at most one running per backend
    using Slots = std::map<JobOp, Job>;
    void finish_job(Job& j, JobOp op, JobOutcome out, std::string error);
    static const Job* running_of(const Slots& s);
    // take the slot and spawn the thread; the common tail of start / start_custom
    uint64_t launch(const std::string& resource, JobOp op, uint64_t max_bytes_per_sec, std::function<JobOutcome()> fn);
    nlohmann::json status_locked(const std::string& resource, JobOp op, const Job* j, const Slots* slots) const;

    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends_;
    FsckExtension fsck_extension_;
    mutable std::mutex m_;
    std::map<std::string, Slots> jobs_;
    uint64_t next_id_ = 1;
};

}  // namespace lights3
