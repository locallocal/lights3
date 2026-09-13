// Ops CLI: lights3-ctl — lights3 operations tooling built on the ccmd subcommand
// framework (third_party/ccmd). This file holds only the root command and
// main; each command group lives in its own file (cred: lights3_ctl_cred.cc,
// bench: lights3_ctl_bench.cc; shared client/flags: lights3_ctl_common.cc).
// ccmd's root options do not propagate down — options must follow the leaf
// subcommand, and long options only accept values in --name=value form.
#include <ccmd.h>

#include <cstdio>
#include <memory>

#include "core/version.h"

#include "tools/lights3_ctl_bench.h"
#include "tools/lights3_ctl_common.h"
#include "tools/lights3_ctl_cred.h"
#include "tools/lights3_ctl_fsck.h"
#include "tools/lights3_ctl_jobs.h"
#include "tools/lights3_ctl_mpu.h"
#include "tools/lights3_ctl_object.h"
#include "tools/lights3_ctl_quota.h"
#include "tools/lights3_ctl_reload.h"
#include "tools/lights3_ctl_tables.h"
#include "tools/lights3_ctl_tenant.h"
#include "tools/lights3_ctl_usage.h"
#include "tools/lights3_ctl_website.h"

namespace lights3_ctl {

// ccmd callbacks return nothing; the process exit code is carried out through this (0 success / 1 request failure / 2
// usage error)
int g_exit = 0;

}  // namespace lights3_ctl

int main(int argc, char* argv[]) {
    auto root = std::make_shared<ccmd::command>(
        "lights3-ctl", "lights3-ctl cred list --endpoint=http://127.0.0.1:9000", "lights3-ctl <command> [options]",
        "lights3 ops CLI (docs/architecture/credential-management.md). Credential management "
        "lives under the `cred` command group, benchmarking under `bench`, bucket "
        "website configuration under `website`, online object verification under "
        "`fsck`, bucket quotas under `quota`, tenants under `tenant`, usage "
        "counters under `usage`, configuration hot reload under `reload`, object layout "
        "introspection under `object`, multipart cleanup under `mpu`, duostore / tiered "
        "maintenance rounds on the live gateway under `duostore` and `tier`, the S3 Tables "
        "catalog (table buckets, listing, maintenance, diagnostics) under `tables`; run "
        "`lights3-ctl help <command>` for details.",
        "lights3 ops CLI.",
        // Bare lights3-ctl / lights3-ctl -x: nothing actionable to run; print help and exit as a
        // usage error. `lights3-ctl --version` is the one root-level flag (roadmap §6.3)
        [](const std::shared_ptr<ccmd::command>& c) {
            if (c->var<bool>("version")) {
                fputs(lights3::version_report("lights3-ctl").c_str(), stdout);
                return;
            }
            c->print_help();
            lights3_ctl::g_exit = 2;
        });
    root->var<bool>("version", false,
                    "Print version, git commit, build type and the compiled-in drivers / "
                    "backends, then exit");
    root->add_subcommand(lights3_ctl::make_cred());
    root->add_subcommand(lights3_ctl::make_bench());
    root->add_subcommand(lights3_ctl::make_website());
    root->add_subcommand(lights3_ctl::make_fsck());
    root->add_subcommand(lights3_ctl::make_quota());
    root->add_subcommand(lights3_ctl::make_tenant());
    root->add_subcommand(lights3_ctl::make_usage());
    root->add_subcommand(lights3_ctl::make_reload());
    root->add_subcommand(lights3_ctl::make_object());
    root->add_subcommand(lights3_ctl::make_mpu());
    root->add_subcommand(lights3_ctl::make_duostore());
    root->add_subcommand(lights3_ctl::make_tier());
    root->add_subcommand(lights3_ctl::make_tables());
    root->execute(argc, argv);
    return lights3_ctl::g_exit;
}
