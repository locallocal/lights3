// s3adm `usage` command — bucket usage counters via /-/admin/usage
// (docs/multi-tenancy.md §2/§6). Root sees every bucket; a tenant admin sees its
// own tenant's buckets. --rescan triggers a synchronous full count of one bucket.
#include "tools/s3adm_usage.h"

#include <cstdio>
#include <memory>
#include <string>

#include "core/util/uri.h"
#include "tools/s3adm_common.h"

namespace s3adm {

namespace util = lights3::util;

std::shared_ptr<ccmd::c_command> make_usage() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "usage", "s3adm usage logs --rescan", "s3adm usage [bucket] [options]",
        "Show bucket usage counters (objects, committed bytes, in-flight multipart bytes, "
        "last full count). Without a bucket every visible bucket is listed (--tenant "
        "filters by owner, root only). --rescan runs a full count of the given bucket "
        "now and prints the result (roadmap §3.9 ①).",
        "show bucket usage counters.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (c->args().size() > 1) {
                fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            bool rescan = c->var<bool>("rescan");
            auto tenant = c->var<std::string>("tenant");
            if (rescan && c->args().empty()) {
                fprintf(stderr, "s3adm: --rescan needs a bucket\n");
                g_exit = 2;
                return;
            }
            run_admin(c, [&](SignedClient& cli) {
                if (c->args().empty()) {
                    std::string q = tenant.empty()
                                        ? ""
                                        : "tenant=" + util::aws_uri_encode(tenant, true);
                    return finish(cli.get("/-/admin/usage", q), 200);
                }
                std::string path =
                    "/-/admin/usage/" + util::aws_uri_encode(c->args().front(), true);
                if (rescan) return finish(cli.post_empty(path + "/rescan"), 200);
                return finish(cli.get(path, ""), 200);
            });
        });
    cmd->varp<bool>("rescan", "r", false, "run a full count of the bucket now (root or the owner tenant's admin).");
    cmd->varp<std::string>("tenant", "t", "", "list only buckets owned by this tenant (root).");
    add_conn_flags(cmd);
    return cmd;
}

}  // namespace s3adm
