// lights3-ctl `reload` — asks the server to re-read its configuration file and prints
// the report: what was applied at runtime and what still needs a restart
// (docs/config-reload.md). Root credential only; same effect as SIGHUP, but with
// the outcome returned instead of only logged.
#include "tools/lights3_ctl_reload.h"

#include <cstdio>
#include <memory>

#include "tools/lights3_ctl_common.h"

namespace lights3_ctl {

std::shared_ptr<ccmd::command> make_reload() {
    auto cmd = std::make_shared<ccmd::command>(
        "reload", "lights3-ctl reload --endpoint=http://127.0.0.1:9000", "lights3-ctl reload [options]",
        "Reload the server configuration file (POST /-/admin/config/reload, root "
        "credential). Applies the runtime-changeable subset (log level, request/stall "
        "timeouts, max_inflight_requests, min_part_size, rate limits, bucket routing "
        "rules, added/removed backend instances, TLS certificate material) and lists keys that changed but need a "
        "restart; a file that fails validation changes nothing (exit 1).",
        "reload the server configuration.", [](const std::shared_ptr<ccmd::command>& c) {
            if (!c->args().empty()) {
                fprintf(stderr, "lights3-ctl: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            run_admin(c, [](SignedClient& cli) { return finish(cli.post_empty("/-/admin/config/reload"), 200); });
        });
    add_conn_flags(cmd);
    return cmd;
}

}  // namespace lights3_ctl
