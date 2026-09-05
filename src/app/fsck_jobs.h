// Application-level scrub/fsck jobs (backlog-sequence ③): the offline integrity
// scrub (`run_scrub_once`, roadmap §3.1) run against a live gateway's backends,
// one job per backend at a time, on a dedicated thread, with the outcome kept for
// polling. Drives both `lights3 fsck` (run_scrub, synchronous) and the admin
// endpoints POST/GET /-/admin/fsck/<backend> (FsckJobs, asynchronous).
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "storage/backend.h"

namespace lights3 {

// One completed scrub. `stats` is the backend's report as JSON (field names =
// the FsScrubStats / DuoScrubStats members); `findings` is the fsck-convention
// sum of the "data is in danger" counters (exit code 1 offline)
struct FsckOutcome {
    std::string kind;  // "duostore" | "localfs"
    nlohmann::json stats;
    uint64_t findings = 0;
    bool aborted = false;  // backend close interrupted the scrub (stats are partial)
};

// Dispatch on the backend type and run one scrub round synchronously (the caller
// is a thread that may block). Throws std::invalid_argument for a backend type
// without an offline scrub (memory, cloudproxy, tiered)
FsckOutcome run_scrub(storage::IStorageBackend& backend, uint64_t max_bytes_per_sec);

class FsckJobs {
public:
    enum class Error { NoSuchBackend, Unsupported, Busy };
    struct Failure {
        Error code;
        std::string message;
    };

    explicit FsckJobs(std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends)
        : backends_(std::move(backends)) {}
    ~FsckJobs() { shutdown(); }

    // Start a job; returns its id. Throws Failure{Busy} while one runs on the
    // backend, {NoSuchBackend} / {Unsupported} before anything starts
    uint64_t start(const std::string& backend, uint64_t max_bytes_per_sec);
    // {"backend","running","job_id","started_at","finished_at","duration_ms",
    //  "max_mbps","kind","findings","aborted","stats"}; the last-* fields describe
    // the most recent completed job (absent before the first). Throws
    // Failure{NoSuchBackend}
    nlohmann::json status(const std::string& backend) const;
    // Wait for every running job (backends are closed by the caller first, which
    // makes a scrub abort promptly)
    void shutdown();

private:
    struct Job {
        uint64_t id = 0;
        bool running = false;
        uint64_t max_mbps = 0;
        int64_t started_ms = 0;   // unix ms
        int64_t finished_ms = 0;  // 0 while running
        FsckOutcome outcome;      // of the last completed job
        bool has_outcome = false;
        std::string error;  // exception text if the last job threw
        std::thread thread;
    };
    void finish_job(Job& j, FsckOutcome out, std::string error);

    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends_;
    mutable std::mutex m_;
    std::map<std::string, Job> jobs_;
    uint64_t next_id_ = 1;
};

}  // namespace lights3
