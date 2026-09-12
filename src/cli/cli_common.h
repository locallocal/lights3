// lights3 entry-point command tree, shared support: process exit code, the
// --config flag every leaf registers, `<backend>` argument parsing and the
// leaf factory used by every single-backend admin command. Each command group
// lives in its own file (cli_server.cc, cli_duostore.cc, cli_tier.cc,
// cli_fsck.cc); src/main.cc holds only the root command and main.
#pragma once

#include <ccmd.h>

#include <memory>
#include <stdexcept>
#include <string>

#include "app/app.h"

namespace lights3_cli {

using Cmd = std::shared_ptr<ccmd::command>;

// ccmd callbacks return nothing; the process exit code is carried out through
// this (0 success / 1 runtime failure / 2 usage error). Defined in main.cc.
extern int g_exit;

constexpr const char* kDefaultConfig = "config/lights3.yaml";

// ccmd's root options do not propagate down, so --config is registered on every leaf
void add_config_flag(const Cmd& cmd);

// Shared by every single-backend admin leaf (duostore gc/scan, tier *, fsck):
// `<backend>` positional or --backend=, positional wins. Usage errors print the
// leaf's help, set g_exit = 2 and throw
std::string one_backend_arg(const Cmd& c);

// Leaf factory for commands taking only `<backend>` (+ --config)
Cmd make_backend_leaf(const char* name, const char* example, const char* usage, const char* help_long,
                      const char* help_short, void (*run)(const Cmd&));

// Group node whose own invocation is a usage error: prints help, exit code 2
Cmd make_group(const char* name, const char* example, const char* usage, const char* help_long, const char* help_short);

// Looks up a built backend by name and downcasts it to the concrete type the
// command operates on; `tag` prefixes the error messages ("duostore" / "tier"),
// `kind` names the expected type in them
template <class Backend>
Backend* find_backend_as(lights3::Application& app, const std::string& name, const char* tag, const char* kind) {
    const auto& backends = app.backends();
    auto it = backends.find(name);
    if (it == backends.end()) throw std::runtime_error(std::string(tag) + ": no backend named '" + name + "'");
    auto* b = dynamic_cast<Backend*>(it->second.get());
    if (!b) throw std::runtime_error(std::string(tag) + ": backend '" + name + "' is not " + kind);
    return b;
}

}  // namespace lights3_cli
