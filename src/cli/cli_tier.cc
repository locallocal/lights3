#include "cli/cli_tier.h"

#include <chrono>
#include <cstdio>
#include <string>
#include <tuple>
#include <vector>

#include "core/log.h"
#include "core/util/time.h"
#include "storage/tiered/tiered_backend.h"

namespace lights3_cli {

namespace {

// Tiered background tasks on demand (roadmap §3.2; same offline pattern as the
// duostore admin commands — tiered sits over localfs + a cloud backend, no
// exclusive lock issue, but the coldness verdict is atime-based and an offline
// run sees its own process's access table, so `tier scan` demotes by
// mtime/atime as recorded on disk)
lights3::storage::TieredBackend* find_tiered(lights3::Application& app, const std::string& name) {
    return find_backend_as<lights3::storage::TieredBackend>(app, name, "tier", "tiered");
}

void run_tier_scan(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto st = sync_wait(t->scan_once());  // demotions/reclaim are logged by the scan itself
    LOG_INFO("tier admin: {} scan round complete: {} {}, cold {}, recovered {}, enrolled {}, "
             "evicted {} ({} bytes), watermark short by {}",
             st.full ? "full" : "incremental", st.walked,
             st.full ? "objects walked" : "wheel candidates", st.cold_picked, st.recovered,
             st.enrolled, st.evicted, st.evicted_bytes, st.need_remaining);
    app.shutdown();
}

void run_tier_gc(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto st = sync_wait(t->run_gc_once());
    LOG_INFO("tier admin: gc round: resolved {} (cloud replicas removed {}), deferred {}, "
             "failed {}",
             st.resolved, st.removed_cloud, st.deferred, st.failed);
    app.shutdown();
}

void run_tier_reconcile(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto st = sync_wait(t->run_reconcile_once());
    LOG_INFO("tier admin: reconcile round: {} cloud objects walked; stubs rebuilt {}, "
             "orphans deleted {}, orphans skipped {}, refs missing {}; quarantine new {}, "
             "resolved {}",
             st.cloud_objects, st.stubs_rebuilt, st.orphans_deleted, st.orphans_skipped,
             st.refs_missing, st.quarantined_new, st.quarantined_resolved);
    app.shutdown();
}

// Quarantine ledger (roadmap §3.6 ④): list / forget / purge. forget and purge take
// `<backend> <bucket> <key>` positionals (or --backend= plus two positionals)
std::tuple<std::string, std::string, std::string> backend_key_args(const Cmd& c) {
    std::string backend = c->var<std::string>("backend");
    std::vector<std::string> pos = c->args();
    if (!backend.empty() && pos.size() == 2) pos.insert(pos.begin(), backend);
    if (pos.size() != 3) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error(c->name() + ": expected <backend> <bucket> <key>");
    }
    return {pos[0], pos[1], pos[2]};
}

void run_tier_quarantine_list(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto entries = t->quarantine_list();
    if (entries.empty()) {
        std::printf("no quarantined findings\n");
    } else {
        std::printf("%-13s %-20s %-40s %-34s %-20s %-20s %s\n", "KIND", "BUCKET", "KEY", "ETAG",
                    "FIRST_SEEN", "LAST_SEEN", "COUNT");
        for (const auto& e : entries)
            std::printf("%-13s %-20s %-40s %-34s %-20s %-20s %llu\n", e.kind.c_str(),
                        e.bucket.c_str(), e.key.c_str(), e.etag.c_str(),
                        util::iso8601(std::chrono::system_clock::from_time_t(e.first_seen)).c_str(),
                        util::iso8601(std::chrono::system_clock::from_time_t(e.last_seen)).c_str(),
                        static_cast<unsigned long long>(e.count));
    }
    app.shutdown();
}

void run_tier_quarantine_forget(const Cmd& c) {
    using namespace lights3;
    auto [backend, bucket, key] = backend_key_args(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, backend);
    if (t->quarantine_forget(bucket, key)) {
        LOG_INFO("tier admin: quarantine entry {}/{} forgotten", bucket, key);
    } else {
        LOG_WARN("tier admin: {}/{} is not quarantined", bucket, key);
        g_exit = 1;
    }
    app.shutdown();
}

void run_tier_quarantine_purge(const Cmd& c) {
    using namespace lights3;
    auto [backend, bucket, key] = backend_key_args(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, backend);
    if (sync_wait(t->quarantine_purge(bucket, key))) {
        LOG_WARN("tier admin: dead stub {}/{} purged (data loss acknowledged)", bucket, key);
    } else {
        LOG_WARN("tier admin: {}/{} not purged (not a refs_missing finding, or the cloud copy "
                 "is back — entry dropped)",
                 bucket, key);
        g_exit = 1;
    }
    app.shutdown();
}

Cmd make_tier_quarantine() {
    auto cmd = make_group(
        "quarantine", "lights3 tier quarantine list tierdata",
        "lights3 tier quarantine <list|forget|purge> <backend> [<bucket> <key>] [--config=<path>]",
        "Reconciliation quarantine ledger (docs/storage/tiered-design.md §9): findings that "
        "repeat every round (stub whose cloud copy is gone; foreign cloud object without "
        "lights3 headers) are recorded once and stop re-alerting. list shows them; forget "
        "drops an entry without touching data; purge deletes the dead local stub of a "
        "refs_missing finding after re-verifying the cloud copy is still gone.",
        "tiered quarantine ledger (list/forget/purge)");
    cmd->add_subcommand(make_backend_leaf(
        "list", "lights3 tier quarantine list tierdata",
        "lights3 tier quarantine list <backend> [--config=<path>]",
        "Print every quarantined finding with its first/last sighting and repeat count.",
        "list quarantined findings", run_tier_quarantine_list));
    cmd->add_subcommand(make_backend_leaf(
        "forget", "lights3 tier quarantine forget tierdata archive photos/2024/a.jpg",
        "lights3 tier quarantine forget <backend> <bucket> <key> [--config=<path>]",
        "Drop the entry for <bucket>/<key> (all kinds) without touching any data; it "
        "comes back on the next reconcile round if the finding still reproduces.",
        "drop a quarantine entry", run_tier_quarantine_forget));
    cmd->add_subcommand(make_backend_leaf(
        "purge", "lights3 tier quarantine purge tierdata archive photos/2024/a.jpg",
        "lights3 tier quarantine purge <backend> <bucket> <key> [--config=<path>]",
        "Resolve a refs_missing finding by deleting the dead local stub — the object "
        "disappears from listings. Re-verifies with a HEAD first: if the cloud copy is back "
        "the stub is kept and the entry dropped instead.",
        "delete the dead stub of a refs_missing finding", run_tier_quarantine_purge));
    return cmd;
}

}  // namespace

Cmd make_tier() {
    auto cmd = make_group(
        "tier", "lights3 tier scan tierdata --config=config/lights3.yaml",
        "lights3 tier <scan|gc|reconcile|quarantine> <backend> [--config=<path>]",
        "Tiered-storage background tasks on demand (docs/storage/tiered-design.md §9): the "
        "same rounds the timers run, for operators who cannot wait for the next tick. "
        "Runs with the backends built but no server listening, then exits.",
        "tiered background tasks (scan/gc/reconcile) + quarantine ledger");
    cmd->add_subcommand(make_backend_leaf(
        "scan", "lights3 tier scan tierdata",
        "lights3 tier scan <backend> [--config=<path>]",
        "Run one scan round now: coldness detection + space-watermark reclamation + "
        "crash recovery + atime snapshot.",
        "run one tiered scan round", run_tier_scan));
    cmd->add_subcommand(make_backend_leaf(
        "gc", "lights3 tier gc tierdata",
        "lights3 tier gc <backend> [--config=<path>]",
        "Consume one round of the tiered GC queue now: delete orphan cloud replicas "
        "(etag-verified); failures reschedule with their persisted backoff.",
        "run one tiered GC round", run_tier_gc));
    cmd->add_subcommand(make_backend_leaf(
        "reconcile", "lights3 tier reconcile tierdata",
        "lights3 tier reconcile <backend> [--config=<path>]",
        "Run one bidirectional local/cloud reconciliation now: rebuild missing stubs "
        "from redundant headers (or delete orphans when configured); local-remote with "
        "cloud missing warns and never deletes the stub.",
        "run one tiered reconcile round", run_tier_reconcile));
    cmd->add_subcommand(make_tier_quarantine());
    return cmd;
}

}  // namespace lights3_cli
