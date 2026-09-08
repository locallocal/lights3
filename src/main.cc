// Process entry point: command-line parsing (ccmd, third_party/ccmd) +
// Application lifecycle (src/app/app.h). Assembly and startup/shutdown
// ordering live in lights3::Application (docs/architecture.md §4).
//
// Command tree:
//   lights3 [--config=<path>]                     start the server        (cli/cli_server.cc)
//   lights3 duostore dump <backend> <file> [...]  duostore meta admin     (cli/cli_duostore.cc)
//   lights3 duostore load <backend> <file> [...]  (docs/storage/duostore-core.md §11)
//   lights3 duostore backup|restore <backend> ...  meta backup chains / PITR (§11.1)
//   lights3 duostore gc|scan <backend> [...]      run one GC / orphan-scan round (roadmap §3.2)
//   lights3 duostore quarantine list|release|purge ...  corrupt-pack quarantine (roadmap §3.7)
//   lights3 tier scan|gc|reconcile <backend> [..] tiered background tasks (cli/cli_tier.cc)
//   lights3 tier quarantine list|forget|purge ...  tiered reconcile quarantine ledger (roadmap §3.6)
//   lights3 fsck <backend> [--max-mbps=<n>] [...] offline integrity scrub (cli/cli_fsck.cc)
//
// This file holds only the root command and main; each command group lives in
// its own src/cli/cli_<group>.cc with a make_<group>() factory, shared helpers
// in cli/cli_common.h. ccmd's root options do not propagate down, so --config
// is registered on every leaf. cflag accepts long-option values only as
// --name=value; the `--name value` form used by the e2e scripts and older docs
// is folded into that shape by normalize_argv below, so both keep working.
#include <ccmd.h>

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "cli/cli_common.h"
#include "cli/cli_fsck.h"
#include "cli/cli_server.h"
#include "cli/cli_tier.h"
#ifdef LIGHTS3_DUOSTORE
#include "cli/cli_duostore.h"
#endif

namespace lights3_cli {

// ccmd callbacks return nothing; the process exit code is carried out through
// this (0 success / 1 runtime failure / 2 usage error)
int g_exit = 0;

}  // namespace lights3_cli

namespace {

// Rewrite `--config <v>` into `--config=<v>` (the only form cflag understands
// for non-bool long options). Applies to the value-taking long options the
// command tree registers; everything after `--` is left untouched.
std::vector<std::string> normalize_argv(int argc, char** argv) {
    static const char* const kValueFlags[] = {"--config", "--backend", "--file", "--max-mbps"};
    std::vector<std::string> out;
    out.reserve(static_cast<size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--") {
            for (; i < argc; ++i) out.emplace_back(argv[i]);
            break;
        }
        bool folded = false;
        for (const char* f : kValueFlags) {
            if (a == f && i + 1 < argc) {
                out.push_back(a + "=" + argv[i + 1]);
                ++i;
                folded = true;
                break;
            }
        }
        if (!folded) out.push_back(std::move(a));
    }
    return out;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace lights3_cli;
    auto root = std::make_shared<ccmd::c_command>(
        "lights3", "lights3 --config=config/lights3.yaml", "lights3 [--config=<path>] | lights3 <command> ...",
        "S3-compatible object storage server. With no command the server starts and "
        "runs until SIGINT/SIGTERM; run `lights3 help <command>` for the admin commands.",
        "S3-compatible object storage server", run_server);
    add_config_flag(root);
    root->var<bool>("check-config", false,
                    "Parse and validate the config, print what it resolves to, exit 0/1 "
                    "without opening backends or binding a port (roadmap §6.2)");
    root->var<bool>("version", false,
                    "Print version, git commit, build type and the compiled-in drivers / "
                    "backends, then exit (roadmap §6.3)");
#ifdef LIGHTS3_DUOSTORE
    root->add_subcommand(make_duostore());
#endif
    root->add_subcommand(make_tier());
    root->add_subcommand(make_fsck());

    try {
        root->execute(normalize_argv(argc, argv));
        return g_exit;
    } catch (const std::exception& e) {
        fprintf(stderr, "fatal: %s\n", e.what());
        return g_exit == 0 ? 1 : g_exit;
    }
}
