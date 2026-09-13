// lights3-ctl `duostore gc|scan|quarantine` / `tier scan|gc|reconcile|quarantine`
// (docs/usage/cli.md §3.12): the background rounds the offline `lights3 duostore|tier`
// commands run in a separate process (docs/usage/cli.md §2.4), here triggered on the
// running gateway through the admin plane and polled to completion -- the same
// job model as `fsck --offline` (one job per backend, 202 + job id, GET status),
// whose driver lives here as well. The duostore quarantine ledger's mutating
// verbs (release / purge) and tier's (forget / purge) stay offline-only: they
// need the operator's judgment on a specific pack / stub, not a polling loop.
#include "tools/lights3_ctl_jobs.h"

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "tools/lights3_ctl_common.h"

namespace lights3_ctl {

namespace {

// The final document: printed verbatim; the exit code follows the fsck
// convention (findings are the loss-signal counters the application sums)
int print_doc(const httplib::Result& r) {
    auto doc = nlohmann::json::parse(r->body, nullptr, false);
    fputs(r->body.c_str(), stdout);
    if (!r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
    if (doc.is_discarded()) return 1;
    if (doc.contains("error")) return 1;
    if (doc.value("aborted", false)) return 1;
    return doc.value("findings", uint64_t(0)) > 0 ? 1 : 0;
}

}  // namespace

int run_job(SignedClient& cli, const std::string& path, const std::string& label, uint64_t mbps, bool wait,
            bool status_only) {
    if (status_only) {
        auto r = cli.get(path, "");
        if (!r || r->status != 200) return finish(r, 200);
        return print_doc(r);
    }
    auto r = cli.post_empty(path, mbps ? "max_mbps=" + std::to_string(mbps) : "");
    if (!r || r->status != 202) return finish(r, 202);
    if (!wait) {
        fputs(r->body.c_str(), stdout);
        return 0;
    }
    uint64_t job = nlohmann::json::parse(r->body).value("job_id", uint64_t(0));
    for (;;) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto st = cli.get(path, "");
        if (!st || st->status != 200) return finish(st, 200);
        auto doc = nlohmann::json::parse(st->body, nullptr, false);
        if (doc.is_discarded()) {
            fprintf(stderr, "lights3-ctl: %s: unparsable status document\n", label.c_str());
            return 1;
        }
        if (doc.value("running", false)) continue;
        // a newer job replaced ours
        if (doc.value("job_id", uint64_t(0)) != job) continue;
        return print_doc(st);
    }
}

int print_ledger(SignedClient& cli, const std::string& path) {
    auto r = cli.get(path, "");
    if (!r || r->status != 200) return finish(r, 200);
    fputs(r->body.c_str(), stdout);
    if (!r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
    return 0;
}

namespace {

// One round leaf: `lights3-ctl <group> <op> <backend> [--no-wait | --status]`
std::shared_ptr<ccmd::command> make_round(const std::string& group, const std::string& op, const std::string& what,
                                          const std::string& findings) {
    std::string label = group + " " + op;
    auto cmd = std::make_shared<ccmd::command>(
        op, "lights3-ctl " + label + " " + group + "data", "lights3-ctl " + label + " <backend> [--no-wait | --status]",
        "Run one " + what + " on the running gateway (POST /-/admin/" + group + "/<backend>/" + op +
            ", root credential), wait for it and print the outcome document " +
            "(\"stats\" mirrors the round's statistics struct). Exit code 1 when the job " +
            "failed, was aborted by a gateway shutdown, or " + findings +
            ". --no-wait returns the job id at once; --status prints the running/last " +
            "outcome instead of starting anything. One job per backend at a time, whatever " +
            "the operation (409 JobInProgress).",
        "run one " + what + ".", [group, op, label](const std::shared_ptr<ccmd::command>& c) {
            bool status = c->var<bool>("status");
            bool wait = !c->var<bool>("no-wait");
            if (c->args().size() != 1 || (status && !wait)) {
                fprintf(stderr, "lights3-ctl: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            std::string path = "/-/admin/" + group + "/" + c->args().front() + "/" + op;
            run_admin(c, [&](SignedClient& cli) { return run_job(cli, path, label, 0, wait, status); });
        });
    cmd->var<bool>("no-wait", false, "return right after starting the job (prints the job id).");
    cmd->var<bool>("status", false, "print the running/last outcome instead of starting a job.");
    add_conn_flags(cmd);
    return cmd;
}

// `lights3-ctl <group> quarantine list <backend>`: the ledger, read-only
std::shared_ptr<ccmd::command> make_quarantine(const std::string& group, const std::string& what,
                                               const std::string& fields) {
    auto list = std::make_shared<ccmd::command>(
        "list", "lights3-ctl " + group + " quarantine list " + group + "data",
        "lights3-ctl " + group + " quarantine list <backend>",
        "Print the " + what + " of a backend on the running gateway as JSON (GET /-/admin/" + group +
            "/<backend>/quarantine, root credential): {\"backend\",\"kind\"," + "\"entries\":[{" + fields +
            "}]}. Read-only; acting on an entry stays with the " + "offline `lights3 " + group + " quarantine` verbs.",
        "print the " + what + ".", [group](const std::shared_ptr<ccmd::command>& c) {
            if (c->args().size() != 1) {
                fprintf(stderr, "lights3-ctl: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            std::string path = "/-/admin/" + group + "/" + c->args().front() + "/quarantine";
            run_admin(c, [&](SignedClient& cli) { return print_ledger(cli, path); });
        });
    add_conn_flags(list);
    auto cmd = std::make_shared<ccmd::command>(
        "quarantine", "lights3-ctl " + group + " quarantine list " + group + "data",
        "lights3-ctl " + group + " quarantine list <backend>", "The " + what + " (read-only).",
        "inspect the " + what + ".", [](const std::shared_ptr<ccmd::command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(list);
    return cmd;
}

}  // namespace

std::shared_ptr<ccmd::command> make_duostore() {
    auto cmd = std::make_shared<ccmd::command>(
        "duostore", "lights3-ctl duostore gc duodata", "lights3-ctl duostore <command> [options]",
        "duostore maintenance on the running gateway through the admin plane (root "
        "credential): one GC round (gc), one orphan scan (scan), the corrupt-pack "
        "quarantine ledger (quarantine list). The same rounds the timers run and the "
        "offline `lights3 duostore gc|scan` commands run in a separate process; here "
        "they run inside the gateway, so a local meta engine needs no downtime. Options "
        "must follow the leaf subcommand as --name=value.",
        "duostore gc / orphan scan / quarantine ledger on a live gateway.",
        [](const std::shared_ptr<ccmd::command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_round("duostore", "gc",
                                   "duostore GC round (mpu_ttl expiry, gcq consumption, "
                                   "pack sealing / compaction, empty-pack removal)",
                                   "records_corrupt + packs_quarantined > 0"));
    cmd->add_subcommand(make_round("duostore", "scan", "duostore orphan scan (disk vs refs / packstat, both ways)",
                                   "refs_missing + pack_stats_missing > 0"));
    cmd->add_subcommand(
        make_quarantine("duostore", "corrupt-pack quarantine ledger",
                        "\"pack_id\",\"live_recs\",\"corrupt_records\",\"quarantined_at_ms\",\"purged\""));
    return cmd;
}

std::shared_ptr<ccmd::command> make_tier() {
    auto cmd = std::make_shared<ccmd::command>(
        "tier", "lights3-ctl tier reconcile tierdata", "lights3-ctl tier <command> [options]",
        "tiered-backend maintenance on the running gateway through the admin plane "
        "(root credential): one scan round (scan: coldness demotion, watermark eviction, "
        "crash recovery), one GC round (gc: orphan cloud replicas), one local/cloud "
        "reconciliation (reconcile), and the reconciliation quarantine ledger "
        "(quarantine list). The same rounds the timers and the offline `lights3 tier` "
        "commands run. Options must follow the leaf subcommand as --name=value.",
        "tiered scan / gc / reconcile / quarantine ledger on a live gateway.",
        [](const std::shared_ptr<ccmd::command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_round("tier", "scan",
                                   "tiered scan round (coldness demotion, watermark eviction, "
                                   "crash recovery, access-record flush)",
                                   "never for findings (the scan counts no loss signal)"));
    cmd->add_subcommand(make_round("tier", "gc", "tiered GC round (orphan cloud replica deletion)",
                                   "never for findings (the GC counts no loss signal)"));
    cmd->add_subcommand(make_round("tier", "reconcile",
                                   "local/cloud reconciliation round (stubs rebuilt, cloud "
                                   "orphans, refs_missing findings into the quarantine ledger)",
                                   "refs_missing > 0"));
    cmd->add_subcommand(
        make_quarantine("tier", "reconciliation quarantine ledger",
                        "\"kind\",\"bucket\",\"key\",\"etag\",\"first_seen_ms\",\"last_seen_ms\",\"count\""));
    return cmd;
}

}  // namespace lights3_ctl
