#include "cli/cli_fsck.h"

#include <cstdint>
#include <stdexcept>

#include "app/admin_jobs.h"
#include "core/log.h"
#include "storage/bucket_router.h"
#ifdef LIGHTS3_TABLES
#include "core/task.h"
#include "tables/fsck.h"
#endif

namespace lights3_cli {

namespace {

// Offline integrity scrub (roadmap §3.1): backends built, no server listening —
// same shape as duostore dump/load. Dispatches on the backend's concrete type:
// duostore gets the deep manifest/crc/refs scrub, localfs/xlocalfs the ETag
// full-verify. Findings set exit code 1 (fsck convention); warning-grade
// counters (refs_stale, unverifiable, orphan sidecars) do not — they are logged
// and can be transient or expected on legacy data
void run_fsck(const Cmd& c) {
    using namespace lights3;
    std::string backend = one_backend_arg(c);
    int mbps = c->var<int>("max-mbps");
    if (mbps < 0) {
        g_exit = 2;
        throw std::runtime_error("fsck: --max-mbps must be >= 0");
    }
    const uint64_t bps = uint64_t(mbps) * 1000 * 1000;

    Application app(c->var<std::string>("config"));
    app.open_storage();
    const auto& backends = app.backends();
    auto it = backends.find(backend);
    if (it == backends.end()) throw std::runtime_error("fsck: no backend named '" + backend + "'");
    // Same dispatch as the admin endpoint (app/admin_jobs.h); the report goes to the log
    FsckOutcome out;
    try {
        out = run_scrub(*it->second, bps);
    } catch (const std::invalid_argument& e) {
        throw std::runtime_error("fsck: backend '" + backend + "': " + e.what());
    }
#ifdef LIGHTS3_TABLES
    // S3 Tables catalog reconciliation (docs/architecture/s3-tables-design.md §14 ③):
    // the catalog lives in .sys of the default backend, so only that backend's scrub
    // carries it; table buckets on other backends are reached through the router
    if (app.config().tables.enabled && backend == app.config().buckets.default_backend) {
        auto router = storage::BucketRouter::build(app.config().buckets, backends);
        auto rep = sync_wait(tables::reconcile_catalog(it->second, router));
        JobOutcome ext;
        ext.kind = "tables";
        ext.stats = rep.to_json();
        ext.findings = rep.findings.size();
        merge_outcome(out, ext);
    }
#endif
    LOG_INFO("fsck '{}' ({}): findings {} aborted {} stats {}", backend, out.kind, out.findings, out.aborted,
             out.stats.dump());
    app.shutdown();
    if (out.aborted) throw std::runtime_error("fsck: scrub aborted before completion");
    if (out.findings > 0) g_exit = 1;
}

}  // namespace

Cmd make_fsck() {
    auto cmd = make_backend_leaf("fsck", "lights3 fsck local --max-mbps=100 --config=config/lights3.yaml",
                                 "lights3 fsck <backend> [--max-mbps=<n>] [--config=<path>]",
                                 "Offline data-integrity scrub (read-only). duostore: read back every extent of "
                                 "every object and in-flight multipart part, recompute crc32c against the "
                                 "manifest, and reconcile the refs ledger both ways. localfs/xlocalfs: re-read "
                                 "every object and compare the recomputed MD5 with the stored ETag (multipart "
                                 "composites via the recorded part layout). Runs with the backends built but no "
                                 "server listening; exit code 1 when integrity findings exist.",
                                 "offline data-integrity scrub", run_fsck);
    cmd->var<int>("max-mbps", 0, "read throttle in MB/s (0 = unthrottled)");
    return cmd;
}

}  // namespace lights3_cli
