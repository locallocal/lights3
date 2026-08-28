// Ops CLI: s3adm — lights3 operations tooling built on the ccmd subcommand
// framework (third_party/ccmd). This file holds only the root command and
// main; each command group lives in its own file (cred: s3adm_cred.cc,
// bench: s3adm_bench.cc; shared client/flags: s3adm_common.cc).
// ccmd's root options do not propagate down — options must follow the leaf
// subcommand, and long options only accept values in --name=value form.
#include <ccmd.h>

#include <memory>

#include "tools/s3adm_bench.h"
#include "tools/s3adm_common.h"
#include "tools/s3adm_cred.h"
#include "tools/s3adm_fsck.h"
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
        "`fsck`; run `s3adm help <command>` for details.",
        "lights3 ops CLI.",
        // Bare s3adm / s3adm -x: nothing actionable to run; print help and exit as a usage error
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            s3adm::g_exit = 2;
        });
    root->add_subcommand(s3adm::make_cred());
    root->add_subcommand(s3adm::make_bench());
    root->add_subcommand(s3adm::make_website());
    root->add_subcommand(s3adm::make_fsck());
    root->execute(argc, argv);
    return s3adm::g_exit;
}
