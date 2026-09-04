// s3adm `quota` command group — Get/Put/DeleteBucketQuota against the ?quota
// subresource (docs/multi-tenancy.md §3). `set`/`clear` need the root credential;
// `get` works for any credential admitted to the bucket. Responses print the
// server's XML verbatim.
#include "tools/s3adm_quota.h"

#include <cstdio>
#include <memory>
#include <string>

#include "core/config.h"
#include "s3/quota.h"
#include "tools/s3adm_common.h"

namespace {

using s3adm::finish;
using s3adm::g_exit;
using s3adm::run_admin;
using s3adm::SignedClient;

bool one_bucket_arg(const std::shared_ptr<ccmd::c_command>& cmd, std::string& bucket) {
    if (cmd->args().size() != 1) {
        fprintf(stderr, "s3adm: usage: %s\n", cmd->usage().c_str());
        g_exit = 2;
        return false;
    }
    bucket = cmd->args().front();
    return true;
}

std::shared_ptr<ccmd::c_command> make_get() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "get", "s3adm quota get logs", "s3adm quota get <bucket> [options]",
        "Print a bucket's quota XML (404 NoSuchQuotaConfiguration when none is set).",
        "show a bucket's quota.", [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            run_admin(c, [&](SignedClient& cli) {
                auto r = cli.get("/" + bucket, "quota");
                int rc = finish(r, 200);
                if (rc == 0 && r && !r->body.empty() && r->body.back() != '\n')
                    fputc('\n', stdout);
                return rc;
            });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_set() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "set", "s3adm quota set logs --max-bytes=50GiB --max-objects=1000000",
        "s3adm quota set <bucket> [options]",
        "Set (replace) a bucket's quota. At least one of --max-bytes / --max-objects "
        "must be > 0; sizes accept KiB/MiB/GiB units. Enforced on PutObject, "
        "CopyObject, UploadPart and CompleteMultipartUpload (QuotaExceeded, 403).",
        "set a bucket's quota.", [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            auto bytes = c->var<std::string>("max-bytes");
            auto objects = c->var<std::string>("max-objects");
            run_admin(c, [&](SignedClient& cli) {
                lights3::s3::BucketQuota q;
                if (!bytes.empty()) q.max_bytes = lights3::parse_size(bytes);
                if (!objects.empty()) q.max_objects = lights3::parse_size(objects);
                if (!q.max_bytes && !q.max_objects) {
                    fprintf(stderr, "s3adm: give --max-bytes and/or --max-objects (> 0)\n");
                    return 2;
                }
                return finish(cli.put_unsigned("/" + bucket, lights3::s3::quota_xml(q), "quota"),
                              200, "quota set for " + bucket);
            });
        });
    cmd->varp<std::string>("max-bytes", "b", "", "byte limit (committed + in-flight multipart bytes); 0 = unlimited.");
    cmd->varp<std::string>("max-objects", "o", "", "object count limit; 0 = unlimited.");
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_clear() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "clear", "s3adm quota clear logs", "s3adm quota clear <bucket> [options]",
        "Remove a bucket's quota (idempotent).", "remove a bucket's quota.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            run_admin(c, [&](SignedClient& cli) {
                return finish(cli.del("/" + bucket, "quota"), 204, "quota cleared for " + bucket);
            });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

}  // namespace

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_quota() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "quota", "s3adm quota set logs --max-bytes=50GiB", "s3adm quota <command> [options]",
        "Manage per-bucket quotas via the ?quota subresource (docs/multi-tenancy.md §3; "
        "set/clear need the root static credential). Tenant-level quotas are set with "
        "`s3adm tenant create|update`. Options must follow the leaf subcommand as "
        "--name=value.",
        "manage bucket quotas.", [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_get());
    cmd->add_subcommand(make_set());
    cmd->add_subcommand(make_clear());
    return cmd;
}

}  // namespace s3adm
