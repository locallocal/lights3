#include "cli/cli_common.h"

namespace lights3_cli {

void add_config_flag(const Cmd& cmd) {
    cmd->varp<std::string>("config", "c", kDefaultConfig, "Path to the lights3 YAML config file");
}

std::string one_backend_arg(const Cmd& c) {
    std::string backend = c->var<std::string>("backend");
    const auto& pos = c->args();
    if (pos.size() > 1) {
        g_exit = 2;
        throw std::runtime_error(c->name() + ": too many arguments");
    }
    if (pos.size() == 1) backend = pos[0];
    if (backend.empty()) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error(c->name() + ": <backend> is required");
    }
    return backend;
}

Cmd make_backend_leaf(const char* name, const char* example, const char* usage, const char* help_long,
                      const char* help_short, void (*run)(const Cmd&)) {
    auto cmd = std::make_shared<ccmd::c_command>(name, example, usage, help_long, help_short, run);
    add_config_flag(cmd);
    cmd->var<std::string>("backend", "", "backend name (alternative to the positional)");
    return cmd;
}

Cmd make_group(const char* name, const char* example, const char* usage, const char* help_long,
               const char* help_short) {
    return std::make_shared<ccmd::c_command>(name, example, usage, help_long, help_short, [](const Cmd& c) {
        c->print_help();
        g_exit = 2;
    });
}

}  // namespace lights3_cli
