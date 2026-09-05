// Ops CLI: s3adm — lights3 operations tooling built on the ccmd subcommand
// framework (third_party/ccmd). This file holds only the root command and
// main; each command group lives in its own file (cred: s3adm_cred.cc,
// bench: s3adm_bench.cc; shared client/flags: s3adm_common.cc).
// ccmd's root options do not propagate down — options must follow the leaf
// subcommand, and long options only accept values in --name=value form.
#include <ccmd.h>

#include <cstdio>
#include <memory>

#include "core/version.h"

#include "tools/s3adm_bench.h"
#include "tools/s3adm_common.h"
#include "tools/s3adm_cred.h"
#include "tools/s3adm_fsck.h"
#include "tools/s3adm_quota.h"
#include "tools/s3adm_reload.h"
#include "tools/s3adm_tenant.h"
#include "tools/s3adm_mpu.h"
#include "tools/s3adm_object.h"
#include "tools/s3adm_usage.h"
#include "tools/s3adm_website.h"

namespace s3adm {

// ccmd callbacks return nothing; the process exit code is carried out through this (0 success / 1 request failure / 2 usage error)
int g_exit = 0;

}  // namespace s3adm

int main(int argc, char* argv[]) {
    auto root = std::make_shared<ccmd::c_command>(
        "s3adm", "s3adm cred list --endpoint=http://127.0.0.1:9000",
        "s3adm <command> [options]",
        "lights3 ops CLI (docs/credential-management.md). Credential management "
        "lives under the `cred` command group, benchmarking under `bench`, bucket "
        "website configuration under `website`, online object verification under "
        "`fsck`, bucket quotas under `quota`, tenants under `tenant`, usage "
        "counters under `usage`, configuration hot reload under `reload`, object layout "
        "introspection under `object`, multipart cleanup under `mpu`; run "
        "`s3adm help <command>` for details.",
        "lights3 ops CLI.",
        // Bare s3adm / s3adm -x: nothing actionable to run; print help and exit as a
        // usage error. `s3adm --version` is the one root-level flag (roadmap §6.3)
        [](const std::shared_ptr<ccmd::c_command>& c) {
            if (c->var<bool>("version")) {
                fputs(lights3::version_report("s3adm").c_str(), stdout);
                return;
            }
            c->print_help();
            s3adm::g_exit = 2;
        });
    root->var<bool>("version", false,
                    "Print version, git commit, build type and the compiled-in drivers / "
                    "backends, then exit");
    root->add_subcommand(s3adm::make_cred());
    root->add_subcommand(s3adm::make_bench());
    root->add_subcommand(s3adm::make_website());
    root->add_subcommand(s3adm::make_fsck());
    root->add_subcommand(s3adm::make_quota());
    root->add_subcommand(s3adm::make_tenant());
    root->add_subcommand(s3adm::make_usage());
    root->add_subcommand(s3adm::make_reload());
    root->add_subcommand(s3adm::make_object());
    root->add_subcommand(s3adm::make_mpu());
    root->execute(argc, argv);
    return s3adm::g_exit;
}
