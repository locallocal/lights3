// Default action of the lights3 binary: the full server lifecycle, plus the
// two root-level flags that short-circuit it (--version, --check-config).
// Implementation in cli_server.cc.
#pragma once

#include <string>

#include "cli/cli_common.h"

namespace lights3_cli {

// `lights3 --check-config` (roadmap §6.2): parse + validate the configuration and
// print what it resolves to, without opening a backend or binding a port. Exit 0 =
// the server would start with this file (modulo runtime failures such as an
// unreachable data directory), 1 = the file is rejected, with the same message the
// server would print as `fatal:`
int check_config(const std::string& path);

// Root command callback: --version / --check-config, otherwise run the server
// until SIGINT/SIGTERM (exit code through g_exit)
void run_server(const Cmd& c);

}  // namespace lights3_cli
