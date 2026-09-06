// lights3-ctl maintenance jobs through the admin plane (docs/cli.md §3.5 / §3.12):
// the shared start / poll / print driver behind `fsck --offline`, and the
// `duostore gc|scan|quarantine` and `tier scan|gc|reconcile|quarantine` command
// groups that run the background rounds on a live gateway. Implementation in
// lights3_ctl_jobs.cc.
#pragma once

#include <ccmd.h>

#include <cstdint>
#include <memory>
#include <string>

namespace lights3_ctl {

class SignedClient;

// One job endpoint (POST starts, GET polls; e.g. /-/admin/tier/<backend>/gc).
// status_only: one GET, print the document. Otherwise POST (with ?max_mbps=N
// when mbps > 0), then -- unless wait is false, which prints the 202 body and
// returns 0 -- poll every 0.5 s until our job stops running and print the
// outcome document. Exit code 1 on a transport / HTTP failure, an "error"
// field, "aborted" or "findings" > 0; `label` names the command in messages
int run_job(SignedClient& cli, const std::string& path, const std::string& label, uint64_t mbps,
            bool wait, bool status_only);
// GET a quarantine ledger endpoint and print it; 0 unless the request failed
int print_ledger(SignedClient& cli, const std::string& path);

// The command groups (both built on run_job / print_ledger)
std::shared_ptr<ccmd::c_command> make_duostore();
std::shared_ptr<ccmd::c_command> make_tier();

}  // namespace lights3_ctl
