// s3adm `tenant` command group — tenant lifecycle and bucket ownership against
// /-/admin/tenants (docs/multi-tenancy.md §6). Root credential for mutations;
// a tenant admin may `get`/`list` its own tenant. Responses print the server's
// JSON verbatim.
#include "tools/s3adm_tenant.h"

#include <cstdio>
#include <memory>
#include <string>

#include <nlohmann/json.hpp>

#include "core/util/uri.h"
#include "tools/s3adm_common.h"

namespace {

using nlohmann::json;
using s3adm::finish;
using s3adm::g_exit;
using s3adm::run_admin;
using s3adm::SignedClient;
namespace util = lights3::util;

constexpr const char* kBase = "/-/admin/tenants";

std::string tenant_path(const std::string& id) {
    return std::string(kBase) + "/" + util::aws_uri_encode(id, /*encode_slash=*/true);
}

bool n_args(const std::shared_ptr<ccmd::c_command>& cmd, size_t n) {
    if (cmd->args().size() != n) {
        fprintf(stderr, "s3adm: usage: %s\n", cmd->usage().c_str());
        g_exit = 2;
        return false;
    }
    return true;
}

// --max-bytes/--max-objects/--max-buckets -> "quota" object; only flags that were
// given are emitted (create) or all three are emitted (update = replace semantics)
json quota_from_flags(const std::shared_ptr<ccmd::c_command>& c, bool all) {
    json q = json::object();
    auto put = [&](const char* flag, const char* field) {
        auto v = c->var<std::string>(flag);
        if (v.empty() && !all) return;
        q[field] = v.empty() ? 0 : lights3::parse_size(v);
    };
    put("max-bytes", "max_bytes");
    put("max-objects", "max_objects");
    put("max-buckets", "max_buckets");
    return q;
}

void add_quota_flags(const std::shared_ptr<ccmd::c_command>& cmd) {
    cmd->var<std::string>("max-bytes", "", "byte limit over all owned buckets (e.g. 100GiB); 0/empty = unlimited.");
    cmd->var<std::string>("max-objects", "", "object limit over all owned buckets; 0/empty = unlimited.");
    cmd->var<std::string>("max-buckets", "", "bucket count limit; 0/empty = unlimited.");
}

std::shared_ptr<ccmd::c_command> make_list() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "list", "s3adm tenant list", "s3adm tenant list [options]",
        "List tenants with their quota, buckets and aggregate usage (a tenant admin sees "
        "only its own tenant).",
        "list tenants.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 0)) return;
            run_admin(c, [](SignedClient& cli) { return finish(cli.get(kBase, ""), 200); });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_get() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "get", "s3adm tenant get acme", "s3adm tenant get <id> [options]",
        "Show one tenant: quota, owned buckets, aggregate usage, credential count.",
        "show one tenant.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 1)) return;
            std::string id = c->args().front();
            run_admin(c, [&](SignedClient& cli) { return finish(cli.get(tenant_path(id), ""), 200); });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_create() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "create", "s3adm tenant create acme --display-name='ACME Corp' --max-bytes=1TiB",
        "s3adm tenant create <id> [options]",
        "Create a tenant (id: [a-z0-9][a-z0-9._-]{0,63}). Quota flags are optional; "
        "sizes accept KiB/MiB/GiB units.",
        "create a tenant.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 1)) return;
            run_admin(c, [&](SignedClient& cli) {
                json body;
                body["id"] = c->args().front();
                auto name = c->var<std::string>("display-name");
                if (!name.empty()) body["display_name"] = name;
                json q = quota_from_flags(c, /*all=*/false);
                if (!q.empty()) body["quota"] = q;
                return finish(cli.post_json(kBase, body.dump()), 201);
            });
        });
    cmd->var<std::string>("display-name", "", "human-readable name (Owner/DisplayName); defaults to the id.");
    add_quota_flags(cmd);
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_update() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "update", "s3adm tenant update acme --max-bytes=2TiB --max-buckets=50",
        "s3adm tenant update <id> [options]",
        "Replace a tenant's quota and/or display name. The quota is replaced as a whole: "
        "axes not given become unlimited.",
        "update a tenant's quota / display name.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 1)) return;
            std::string id = c->args().front();
            run_admin(c, [&](SignedClient& cli) {
                json body = json::object();
                auto name = c->var<std::string>("display-name");
                if (!name.empty()) body["display_name"] = name;
                if (!c->var<std::string>("max-bytes").empty() ||
                    !c->var<std::string>("max-objects").empty() ||
                    !c->var<std::string>("max-buckets").empty() || c->var<bool>("clear-quota"))
                    body["quota"] = quota_from_flags(c, /*all=*/true);
                if (body.empty()) {
                    fprintf(stderr, "s3adm: nothing to update (give --display-name and/or quota flags)\n");
                    return 2;
                }
                return finish(cli.put_json(tenant_path(id), body.dump()), 200);
            });
        });
    cmd->var<std::string>("display-name", "", "new display name.");
    add_quota_flags(cmd);
    cmd->var<bool>("clear-quota", false, "remove every limit (quota = unlimited).");
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_delete() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "delete", "s3adm tenant delete acme", "s3adm tenant delete <id> [options]",
        "Delete a tenant. Refused while it still owns buckets or has credentials.",
        "delete a tenant.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 1)) return;
            std::string id = c->args().front();
            run_admin(c, [&](SignedClient& cli) {
                return finish(cli.del(tenant_path(id)), 204, "deleted tenant " + id);
            });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_assign() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "assign", "s3adm tenant assign acme logs-bucket", "s3adm tenant assign <id> <bucket> [options]",
        "Make an existing bucket owned by the tenant. A bucket owned by another tenant is "
        "refused unless --force.",
        "assign a bucket to a tenant.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 2)) return;
            std::string id = c->args()[0], bucket = c->args()[1];
            bool force = c->var<bool>("force");
            run_admin(c, [&](SignedClient& cli) {
                std::string path = tenant_path(id) + "/buckets/" +
                                   util::aws_uri_encode(bucket, /*encode_slash=*/true);
                return finish(cli.put_json(path, "", force ? "force=true" : ""), 200);
            });
        });
    cmd->var<bool>("force", false, "take the bucket over from its current owner tenant.");
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_unassign() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "unassign", "s3adm tenant unassign acme logs-bucket",
        "s3adm tenant unassign <id> <bucket> [options]",
        "Detach a bucket from the tenant (it becomes unowned: visible to root and legacy "
        "credentials only).",
        "detach a bucket from a tenant.", [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!n_args(c, 2)) return;
            std::string id = c->args()[0], bucket = c->args()[1];
            run_admin(c, [&](SignedClient& cli) {
                std::string path = tenant_path(id) + "/buckets/" +
                                   util::aws_uri_encode(bucket, /*encode_slash=*/true);
                return finish(cli.del(path), 204, "detached " + bucket + " from " + id);
            });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

}  // namespace

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_tenant() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "tenant", "s3adm tenant list --endpoint=http://127.0.0.1:9000",
        "s3adm tenant <command> [options]",
        "Manage tenants and bucket ownership via /-/admin/tenants (docs/multi-tenancy.md). "
        "Mutations need the root credential; `list`/`get` also work for a tenant admin "
        "on its own tenant. Options must follow the leaf subcommand; long options take "
        "values as --name=value.",
        "manage tenants and bucket ownership.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_list());
    cmd->add_subcommand(make_get());
    cmd->add_subcommand(make_create());
    cmd->add_subcommand(make_update());
    cmd->add_subcommand(make_delete());
    cmd->add_subcommand(make_assign());
    cmd->add_subcommand(make_unassign());
    return cmd;
}

}  // namespace s3adm
