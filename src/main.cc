// Process entry point: command-line parsing (ccmd, third_party/ccmd) +
// Application lifecycle (src/app/app.h). Assembly and startup/shutdown
// ordering live in lights3::Application (docs/architecture.md §4).
//
// Command tree:
//   lights3 [--config=<path>]                     start the server
//   lights3 duostore dump <backend> <file> [...]  duostore meta admin
//   lights3 duostore load <backend> <file> [...]  (docs/storage/duostore-core.md §11)
//
// ccmd's root options do not propagate down, so --config is registered on
// every leaf. cflag accepts long-option values only as --name=value; the
// `--name value` form used by the e2e scripts and older docs is folded into
// that shape by normalize_argv below, so both keep working.
#include <ccmd.h>

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "app/app.h"
#include "core/log.h"
#ifdef LIGHTS3_DUOSTORE
#include <fstream>

#include "storage/duostore/duostore_backend.h"
#endif

namespace {

using Cmd = std::shared_ptr<ccmd::c_command>;

constexpr const char* kDefaultConfig = "config/lights3.yaml";

// ccmd callbacks return nothing; the process exit code is carried out through
// this (0 success / 1 runtime failure / 2 usage error)
int g_exit = 0;

void add_config_flag(const Cmd& cmd) {
    cmd->varp<std::string>("config", "c", kDefaultConfig, "Path to the lights3 YAML config file");
}

// Rewrite `--config <v>` into `--config=<v>` (the only form cflag understands
// for non-bool long options). Applies to the value-taking long options
// registered in this file; everything after `--` is left untouched.
std::vector<std::string> normalize_argv(int argc, char** argv) {
    static const char* const kValueFlags[] = {"--config", "--backend", "--file"};
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

// Default action: full server lifecycle. A late startup failure unwinds
// through ~Application, which closes every backend already built: duostore's
// active pack gets sealed and rados flushed even on the error path (see
// Application::shutdown)
void run_server(const Cmd& c) {
    lights3::Application app(c->var<std::string>("config"));
    app.open_storage();
    app.start_server();
    g_exit = app.run();
}

#ifdef LIGHTS3_DUOSTORE
// duostore meta admin: backends are built, the server has not started (write
// quiescence holds trivially). Any failure throws loudly and converges
// through main's fallback path
struct AdminArgs {
    std::string config, backend, file;
};

// `<backend> <file>` positionals, or --backend=/--file= (either spelling,
// positionals win when both are given)
AdminArgs admin_args(const Cmd& c) {
    AdminArgs a;
    a.config = c->var<std::string>("config");
    a.backend = c->var<std::string>("backend");
    a.file = c->var<std::string>("file");
    const auto& pos = c->args();
    if (pos.size() > 2) {
        g_exit = 2;
        throw std::runtime_error("duostore " + c->name() + ": too many arguments");
    }
    if (pos.size() >= 1) a.backend = pos[0];
    if (pos.size() == 2) a.file = pos[1];
    if (a.backend.empty() || a.file.empty()) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error("duostore " + c->name() + ": <backend> and <file> are required");
    }
    return a;
}

lights3::storage::DuoStoreBackend* find_duostore(lights3::Application& app, const std::string& name) {
    const auto& backends = app.backends();
    auto it = backends.find(name);
    if (it == backends.end()) throw std::runtime_error("duostore: no backend named '" + name + "'");
    auto* duo = dynamic_cast<lights3::storage::DuoStoreBackend*>(it->second.get());
    if (!duo) throw std::runtime_error("duostore: backend '" + name + "' is not duostore");
    return duo;
}

void run_dump(const Cmd& c) {
    using namespace lights3;
    AdminArgs a = admin_args(c);
    Application app(a.config);
    app.open_storage();
    auto* duo = find_duostore(app, a.backend);
    std::ofstream f(a.file, std::ios::binary | std::ios::trunc);
    if (!f) throw std::runtime_error("duostore dump: cannot open for write: " + a.file);
    auto st = sync_wait(duo->run_meta_dump(f));
    LOG_INFO("duostore admin: dumped {} buckets / {} objects / {} sealed packs to {}",
             st.buckets, st.objects, st.sealed_packs, a.file);
    app.shutdown();
}

void run_load(const Cmd& c) {
    using namespace lights3;
    AdminArgs a = admin_args(c);
    Application app(a.config);
    app.open_storage();
    auto* duo = find_duostore(app, a.backend);
    std::ifstream f(a.file, std::ios::binary);
    if (!f) throw std::runtime_error("duostore load: cannot open for read: " + a.file);
    auto st = sync_wait(duo->run_meta_load(f));
    LOG_INFO("duostore admin: loaded {} buckets / {} objects / {} sealed packs from {}",
             st.buckets, st.objects, st.sealed_packs, a.file);
    app.shutdown();
}

Cmd make_admin_leaf(const char* name, const char* example, const char* usage, const char* help_long,
                    const char* help_short, void (*run)(const Cmd&)) {
    auto cmd = std::make_shared<ccmd::c_command>(name, example, usage, help_long, help_short, run);
    add_config_flag(cmd);
    cmd->var<std::string>("backend", "", "duostore backend name (alternative to the positional)");
    cmd->var<std::string>("file", "", "dump file path (alternative to the positional)");
    return cmd;
}

Cmd make_duostore() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "duostore", "lights3 duostore dump local meta.dump --config=config/lights3.yaml",
        "lights3 duostore <dump|load> <backend> <file> [--config=<path>]",
        "DuoStore meta dump/load (docs/storage/duostore-core.md §11). Runs with the "
        "backends built but no server listening (no traffic), then exits; load ends "
        "with a forced orphan scan. Backup order: copy the data dir first, then dump "
        "meta; restore data first, then load.",
        "duostore meta admin (dump/load)",
        [](const Cmd& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_admin_leaf(
        "dump", "lights3 duostore dump local meta.dump",
        "lights3 duostore dump <backend> <file> [--config=<path>]",
        "Write the backend's full meta (buckets, objects, sealed packs) to <file>.",
        "dump duostore meta to a file", run_dump));
    cmd->add_subcommand(make_admin_leaf(
        "load", "lights3 duostore load local meta.dump",
        "lights3 duostore load <backend> <file> [--config=<path>]",
        "Replay a meta dump from <file> into the backend, then run an orphan scan.",
        "load duostore meta from a file", run_load));
    return cmd;
}
#endif  // LIGHTS3_DUOSTORE

}  // namespace

int main(int argc, char** argv) {
    auto root = std::make_shared<ccmd::c_command>(
        "lights3", "lights3 --config=config/lights3.yaml", "lights3 [--config=<path>] | lights3 <command> ...",
        "S3-compatible object storage server. With no command the server starts and "
        "runs until SIGINT/SIGTERM; run `lights3 help <command>` for the admin commands.",
        "S3-compatible object storage server", run_server);
    add_config_flag(root);
#ifdef LIGHTS3_DUOSTORE
    root->add_subcommand(make_duostore());
#endif

    try {
        root->execute(normalize_argv(argc, argv));
        return g_exit;
    } catch (const std::exception& e) {
        fprintf(stderr, "fatal: %s\n", e.what());
        return g_exit == 0 ? 1 : g_exit;
    }
}
