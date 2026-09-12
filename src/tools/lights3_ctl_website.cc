// lights3-ctl `website` command group — Get/Put/DeleteBucketWebsite against the
// ?website subresource (docs/static-website.md phase ③). Requires the root
// (static) credential, same as `cred`; responses print the server's XML verbatim.
#include "tools/lights3_ctl_website.h"

#include <cstdio>
#include <memory>
#include <string>

#include "s3/xml.h"
#include "tools/lights3_ctl_common.h"

namespace {

using lights3_ctl::finish;
using lights3_ctl::g_exit;
using lights3_ctl::run_admin;
using lights3_ctl::SignedClient;

// Positional arguments must be exactly one bucket; same convention as cred's one_ak_arg
bool one_bucket_arg(const std::shared_ptr<ccmd::command>& cmd, std::string& bucket) {
    if (cmd->args().size() != 1) {
        fprintf(stderr, "lights3-ctl: usage: %s\n", cmd->usage().c_str());
        g_exit = 2;
        return false;
    }
    bucket = cmd->args().front();
    return true;
}

std::shared_ptr<ccmd::command> make_get() {
    auto cmd = std::make_shared<ccmd::command>(
        "get", "lights3-ctl website get my-site", "lights3-ctl website get <bucket> [options]",
        "Print a bucket's website configuration XML (404 when none is set).", "show a bucket's website configuration.",
        [](const std::shared_ptr<ccmd::command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            run_admin(c, [&](SignedClient& cli) {
                auto r = cli.get("/" + bucket, "website");
                int rc = finish(r, 200);
                if (rc == 0 && r && !r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
                return rc;
            });
        });
    lights3_ctl::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::command> make_set() {
    auto cmd = std::make_shared<ccmd::command>(
        "set", "lights3-ctl website set my-site --index-suffix=index.html --error-key=error.html",
        "lights3-ctl website set <bucket> [options]",
        "Enable/replace a bucket's website configuration: the bucket becomes anonymously "
        "readable (GET/HEAD objects only) with index/error document semantics "
        "(docs/static-website.md). Buckets configured statically in the server config "
        "are refused (405).",
        "set a bucket's website configuration.", [](const std::shared_ptr<ccmd::command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            auto suffix = c->var<std::string>("index-suffix");
            auto error_key = c->var<std::string>("error-key");
            run_admin(c, [&](SignedClient& cli) {
                // Same XML shape the server round-trips on `website get`
                lights3::s3::XmlWriter x;
                x.open("WebsiteConfiguration", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
                x.open("IndexDocument");
                x.element("Suffix", suffix);
                x.close();
                if (!error_key.empty()) {
                    x.open("ErrorDocument");
                    x.element("Key", error_key);
                    x.close();
                }
                x.close();
                return finish(cli.put_unsigned("/" + bucket, x.str(), "website"), 200,
                              "website configuration set for " + bucket);
            });
        });
    cmd->varp<std::string>("index-suffix", "i", "index.html", "index document suffix (no '/').");
    cmd->varp<std::string>("error-key", "k", "",
                           "error document key (served on anonymous 4xx/5xx); empty = "
                           "built-in error page.");
    lights3_ctl::add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::command> make_delete() {
    auto cmd = std::make_shared<ccmd::command>(
        "delete", "lights3-ctl website delete my-site", "lights3-ctl website delete <bucket> [options]",
        "Remove a bucket's website configuration (idempotent); the bucket stops being "
        "anonymously readable.",
        "remove a bucket's website configuration.", [](const std::shared_ptr<ccmd::command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            run_admin(c, [&](SignedClient& cli) {
                return finish(cli.del("/" + bucket, "website"), 204, "website configuration deleted for " + bucket);
            });
        });
    lights3_ctl::add_conn_flags(cmd);
    return cmd;
}

}  // namespace

namespace lights3_ctl {

// `website` command group: pure dispatcher, holds no options of its own (same
// convention as `cred`)
std::shared_ptr<ccmd::command> make_website() {
    auto cmd = std::make_shared<ccmd::command>(
        "website", "lights3-ctl website set my-site --error-key=error.html", "lights3-ctl website <command> [options]",
        "Manage per-bucket static website configuration via the ?website subresource "
        "(docs/static-website.md; requires the root static credential, like `cred`). "
        "Credentials come from each subcommand's --ak=/--sk= or from env "
        "LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK; options must follow the leaf subcommand as "
        "--name=value.",
        "manage bucket website configuration.", [](const std::shared_ptr<ccmd::command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_get());
    cmd->add_subcommand(make_set());
    cmd->add_subcommand(make_delete());
    return cmd;
}

}  // namespace lights3_ctl
