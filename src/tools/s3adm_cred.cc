// s3adm `cred` command group — credential operations against
// /-/admin/credentials (docs/credential-management.md §2/§3).
// Each subcommand (cred list / get / create / delete) has its own option set —
// ccmd's root options do not propagate down, connection options must follow
// the leaf subcommand (s3adm cred list --endpoint=...), and long options only
// accept values in --name=value form.
// Requests go through the shared SigV4 self-signing client (s3adm_common.h);
// responses print the server's JSON verbatim.
#include "tools/s3adm_cred.h"

#include <cstdio>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

#include "core/util/uri.h"
#include "s3/auth/policy.h"
#include "s3/errors.h"
#include "tools/s3adm_common.h"

namespace {

using nlohmann::json;
using s3adm::g_exit;
using s3adm::SignedClient;
namespace s3 = lights3::s3;
namespace util = lights3::util;

constexpr const char* kBase = "/-/admin/credentials";

// Unified wrap-up: expected status code -> print the response body (already indented JSON from the server); otherwise stderr + nonzero
int finish(const httplib::Result& r, int expect, const std::string& ok_note = "") {
    if (!r) {
        fprintf(stderr, "s3adm: transport error: %s\n", httplib::to_string(r.error()).c_str());
        return 1;
    }
    if (r->status != expect) {
        fprintf(stderr, "s3adm: HTTP %d\n%s", r->status, r->body.c_str());
        if (!r->body.empty() && r->body.back() != '\n') fputc('\n', stderr);
        return 1;
    }
    if (!r->body.empty())
        fputs(r->body.c_str(), stdout);
    else if (!ok_note.empty())
        printf("%s\n", ok_note.c_str());
    return 0;
}

std::string ak_path(const std::string& ak) {
    return std::string(kBase) + "/" + util::aws_uri_encode(ak, /*encode_slash=*/true);
}

std::string load_policy_arg(const std::string& arg) {
    std::string text = arg;
    if (!arg.empty() && arg.front() == '@') {  // @file: curl convention
        std::ifstream f(arg.substr(1));
        if (!f) throw std::runtime_error("cannot read policy file: " + arg.substr(1));
        std::ostringstream ss;
        ss << f.rdbuf();
        text = ss.str();
    }
    // The client runs the same parsing as the server first; a local error is faster than a round trip
    s3::parse_policy_json(text);
    return text;
}

// Reads connection options + env-var fallback, builds the client and runs fn; exceptions all land here as exit codes
template <class Fn>
void run_admin(const std::shared_ptr<ccmd::c_command>& cmd, Fn&& fn) {
    try {
        s3adm::ConnOpts conn;
        if (!s3adm::read_conn_opts(cmd, conn)) return;
        SignedClient cli(conn);
        g_exit = fn(cli);
    } catch (const s3::S3Error& e) {
        fprintf(stderr, "s3adm: %s\n", e.message.c_str());
        g_exit = 1;
    } catch (const std::exception& e) {
        fprintf(stderr, "s3adm: %s\n", e.what());
        g_exit = 1;
    }
}

// Positional arguments must be exactly one AK; otherwise print the subcommand usage and set the usage-error exit code
bool one_ak_arg(const std::shared_ptr<ccmd::c_command>& cmd, std::string& ak) {
    if (cmd->args().size() != 1) {
        fprintf(stderr, "s3adm: usage: %s\n", cmd->usage().c_str());
        g_exit = 2;
        return false;
    }
    ak = cmd->args().front();
    return true;
}

std::shared_ptr<ccmd::c_command> make_list() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "list", "s3adm cred list --endpoint=http://127.0.0.1:9000",
        "s3adm cred list [options]",
        "List all credentials (secret keys masked; includes static and file-based ones).",
        "list all credentials (secret keys masked).",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            run_admin(c, [](SignedClient& cli) { return finish(cli.get(kBase, ""), 200); });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_get() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "get", "s3adm cred get L3AKXXXX --show-secret", "s3adm cred get <ak> [options]",
        "Show one credential's metadata; --show-secret returns the plaintext secret key "
        "(dynamic/file credentials only; sensitive - the server logs an audit line).",
        "show one credential.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string ak;
            if (!one_ak_arg(c, ak)) return;
            bool show = c->var<bool>("show-secret");
            run_admin(c, [&](SignedClient& cli) {
                return finish(cli.get(ak_path(ak), show ? "show-secret=true" : ""), 200);
            });
        });
    cmd->varp<bool>("show-secret", "s", false, "return the plaintext secret key (dynamic/file credentials only).");
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_create() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "create",
        R"(s3adm cred create --comment=tenant-a --policy='{"buckets":["tenant-a-*"]}')",
        "s3adm cred create [options]",
        "Create a tenant access/secret key pair (the response is the only time the "
        "full secret key is returned). --policy takes policy JSON "
        "({\"buckets\":[...],\"prefixes\":[...],\"readonly\":bool,\"actions\":[...]}) "
        "or @file to read it from a file.",
        "create a tenant access/secret key pair.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!c->args().empty()) {
                fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            run_admin(c, [&](SignedClient& cli) {
                json body = json::object();
                auto comment = c->var<std::string>("comment");
                auto policy = c->var<std::string>("policy");
                if (!comment.empty()) body["comment"] = comment;
                if (!policy.empty()) body["policy"] = json::parse(load_policy_arg(policy));
                // Send the body even when the object is empty: POST semantics stay uniform, and the server treats {} the same as no body
                return finish(cli.post_json(kBase, body.dump()), 201);
            });
        });
    cmd->varp<std::string>("comment", "c", "", "credential comment.");
    cmd->varp<std::string>("policy", "p", "", "policy JSON, or @file to read from a file.");
    s3adm::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_delete() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "delete", "s3adm cred delete L3AKXXXX", "s3adm cred delete <ak> [options]",
        "Revoke a dynamic credential (static ones belong to the config file; the "
        "server refuses them).",
        "revoke a dynamic credential.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string ak;
            if (!one_ak_arg(c, ak)) return;
            run_admin(c, [&](SignedClient& cli) {
                return finish(cli.del(ak_path(ak)), 204, "revoked " + ak);
            });
        });
    s3adm::add_conn_flags(cmd);
    return cmd;
}

}  // namespace

namespace s3adm {

// `cred` command group: pure dispatcher, holds no options of its own.
// Bare `s3adm cred` / `s3adm cred -x` has nothing actionable to run, so it
// prints its own help and exits as a usage error (same convention as root).
std::shared_ptr<ccmd::c_command> make_cred() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "cred", "s3adm cred list --endpoint=http://127.0.0.1:9000",
        "s3adm cred <command> [options]",
        "Manage tenant credentials via /-/admin/credentials with the root (static) "
        "access/secret key (docs/credential-management.md). Credentials come from each "
        "subcommand's --ak=/--sk= or from env LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK. "
        "Options must follow the leaf subcommand; long options take values as "
        "--name=value.",
        "manage tenant credentials.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_list());
    cmd->add_subcommand(make_get());
    cmd->add_subcommand(make_create());
    cmd->add_subcommand(make_delete());
    return cmd;
}

}  // namespace s3adm
