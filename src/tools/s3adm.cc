// Ops CLI: s3adm — manages tenant credentials with the ops-plane (root
// static credential) AK/SK, talking to /-/admin/credentials
// (docs/credential-management.md §2/§3).
// The subcommand framework is ccmd (third_party/ccmd): credential operations
// live under the `cred` command group (cred list / get / create / delete),
// each with its own option set — ccmd's root options do not propagate down,
// connection options must follow the leaf subcommand
// (s3adm cred list --endpoint=...), and long options only accept values in
// --name=value form.
// Requests are SigV4 self-signed (reusing the signing side of s3/auth/sigv4)
// + the httplib synchronous client; responses print the server's JSON verbatim.
#include <ccmd.h>
#include <httplib/httplib.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

#include "core/config.h"
#include "core/util/crypto.h"
#include "core/util/uri.h"
#include "http/model.h"
#include "s3/auth/policy.h"
#include "s3/auth/sigv4.h"
#include "s3/errors.h"

namespace {

using lights3::Credential;
using nlohmann::json;
namespace s3 = lights3::s3;
namespace util = lights3::util;

constexpr const char* kBase = "/-/admin/credentials";

// ccmd callbacks return nothing; the process exit code is carried out through this (0 success / 1 request failure / 2 usage error)
int g_exit = 0;

// Endpoint parsing. signed_host matches the Host header httplib actually
// sends byte for byte (default port sends only host, otherwise host:port) —
// the same convention as cloudproxy Endpoint::parse; SigV4's SignedHeaders
// includes host, and any mismatch means SignatureDoesNotMatch
struct Endpoint {
    bool https = false;
    std::string host;
    int port = 0;
    std::string signed_host;
    std::string base_url;

    static Endpoint parse(const std::string& url) {
        Endpoint ep;
        std::string rest;
        if (url.rfind("https://", 0) == 0) {
            ep.https = true;
            rest = url.substr(8);
        } else if (url.rfind("http://", 0) == 0) {
            rest = url.substr(7);
        } else {
            throw std::runtime_error("endpoint must start with http:// or https://: " + url);
        }
        if (!rest.empty() && rest.back() == '/') rest.pop_back();
        if (rest.empty() || rest.find('/') != std::string::npos)
            throw std::runtime_error("endpoint must be scheme://host[:port]: " + url);
        auto colon = rest.find(':');
        if (colon == std::string::npos) {
            ep.host = rest;
            ep.port = ep.https ? 443 : 80;
        } else {
            ep.host = rest.substr(0, colon);
            try {
                ep.port = std::stoi(rest.substr(colon + 1));
            } catch (...) {
                throw std::runtime_error("endpoint has an invalid port: " + url);
            }
            if (ep.port < 1 || ep.port > 65535)
                throw std::runtime_error("endpoint has an invalid port: " + url);
        }
        if (ep.host.empty()) throw std::runtime_error("endpoint has an empty host: " + url);
        bool default_port = ep.port == (ep.https ? 443 : 80);
        ep.signed_host = default_port ? ep.host : ep.host + ":" + std::to_string(ep.port);
        ep.base_url = std::string(ep.https ? "https://" : "http://") + ep.host + ":" +
                      std::to_string(ep.port);
        return ep;
    }
};

class AdminClient {
public:
    AdminClient(const Endpoint& ep, Credential cred, const std::string& region,
                int timeout_sec, bool insecure)
        : ep_(ep),
          cli_(ep.base_url),
          auth_(s3::SigV4Authenticator::build(lights3::AuthConfig{
              .credentials = {}, .region = region, .service = "s3"})),
          cred_(std::move(cred)) {
        auto t = std::chrono::seconds(timeout_sec);
        cli_.set_connection_timeout(t);
        cli_.set_read_timeout(t);
        cli_.set_write_timeout(t);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        if (ep.https) cli_.enable_server_certificate_verification(!insecure);
#else
        (void)insecure;
#endif
    }

    httplib::Result get(const std::string& path, const std::string& query) {
        return cli_.Get(path + (query.empty() ? "" : "?" + query),
                        sign("GET", path, query, ""));
    }
    httplib::Result post(const std::string& path, const std::string& body) {
        // Content-Type does not go into SignedHeaders (sign only takes host + x-amz-*); passed as an httplib parameter
        return cli_.Post(path, sign("POST", path, "", util::sha256_hex(body)), body,
                         "application/json");
    }
    httplib::Result del(const std::string& path) {
        return cli_.Delete(path, sign("DELETE", path, "", ""));
    }

private:
    // Builds a minimal HttpRequest solely for signing (same technique as
    // cloudproxy RemoteContext::signed_headers): empty payload_hash = empty body
    httplib::Headers sign(const std::string& method, const std::string& raw_path,
                          const std::string& raw_query, const std::string& payload_hash) {
        lights3::http::HttpRequest req;
        req.method = method;
        req.raw_path = raw_path;
        req.raw_query = raw_query;
        req.headers.set("Host", ep_.signed_host);
        auth_.sign(req, cred_, payload_hash);
        httplib::Headers out;
        for (auto& [k, v] : req.headers.items()) out.emplace(k, v);
        return out;
    }

    Endpoint ep_;
    httplib::Client cli_;
    s3::SigV4Authenticator auth_;
    Credential cred_;
};

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

// Common connection options: each ccmd subcommand's option set is independent, so register them one by one
void add_conn_flags(const std::shared_ptr<ccmd::c_command>& cmd) {
    cmd->varp<std::string>("endpoint", "e", "http://127.0.0.1:9000",
                           "lights3 endpoint (scheme://host[:port]).");
    cmd->var<std::string>("ak", "", "root (static) access key; falls back to env LIGHTS3_ADMIN_AK.");
    cmd->var<std::string>("sk", "",
                          "root (static) secret key; falls back to env LIGHTS3_ADMIN_SK "
                          "(preferred: argv is visible to local ps).");
    cmd->var<std::string>("region", "us-east-1", "SigV4 region; must match the server auth.region.");
    cmd->var<bool>("insecure", false, "skip server certificate verification for https (self-signed deployments).");
    cmd->var<int>("timeout-sec", 10, "connect/read/write timeout in seconds.");
}

// Reads connection options + env-var fallback, builds the client and runs fn; exceptions all land here as exit codes
template <class Fn>
void run_admin(const std::shared_ptr<ccmd::c_command>& cmd, Fn&& fn) {
    try {
        std::string ak = cmd->var<std::string>("ak");
        std::string sk = cmd->var<std::string>("sk");
        if (ak.empty())
            if (const char* e = std::getenv("LIGHTS3_ADMIN_AK")) ak = e;
        if (sk.empty())
            if (const char* e = std::getenv("LIGHTS3_ADMIN_SK")) sk = e;
        if (ak.empty() || sk.empty()) {
            fprintf(stderr,
                    "s3adm: missing credentials; pass --ak=/--sk= or set env "
                    "LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK\n");
            g_exit = 2;
            return;
        }
        auto ep = Endpoint::parse(cmd->var<std::string>("endpoint"));
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
        if (ep.https) {
            fprintf(stderr, "s3adm: this build lacks OpenSSL support; https endpoints are unavailable\n");
            g_exit = 2;
            return;
        }
#endif
        AdminClient cli(ep, Credential{ak, util::SecretString(std::move(sk))},
                        cmd->var<std::string>("region"), cmd->var<int>("timeout-sec"),
                        cmd->var<bool>("insecure"));
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
            run_admin(c, [](AdminClient& cli) { return finish(cli.get(kBase, ""), 200); });
        });
    add_conn_flags(cmd);
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
            run_admin(c, [&](AdminClient& cli) {
                return finish(cli.get(ak_path(ak), show ? "show-secret=true" : ""), 200);
            });
        });
    cmd->varp<bool>("show-secret", "s", false, "return the plaintext secret key (dynamic/file credentials only).");
    add_conn_flags(cmd);
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
            run_admin(c, [&](AdminClient& cli) {
                json body = json::object();
                auto comment = c->var<std::string>("comment");
                auto policy = c->var<std::string>("policy");
                if (!comment.empty()) body["comment"] = comment;
                if (!policy.empty()) body["policy"] = json::parse(load_policy_arg(policy));
                // Send the body even when the object is empty: POST semantics stay uniform, and the server treats {} the same as no body
                return finish(cli.post(kBase, body.dump()), 201);
            });
        });
    cmd->varp<std::string>("comment", "c", "", "credential comment.");
    cmd->varp<std::string>("policy", "p", "", "policy JSON, or @file to read from a file.");
    add_conn_flags(cmd);
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
            run_admin(c, [&](AdminClient& cli) {
                return finish(cli.del(ak_path(ak)), 204, "revoked " + ak);
            });
        });
    add_conn_flags(cmd);
    return cmd;
}

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

}  // namespace

int main(int argc, char* argv[]) {
    auto root = std::make_shared<ccmd::c_command>(
        "s3adm", "s3adm cred list --endpoint=http://127.0.0.1:9000",
        "s3adm <command> [options]",
        "lights3 ops CLI (docs/credential-management.md). Credential management "
        "lives under the `cred` command group; run `s3adm help cred` for details.",
        "lights3 ops CLI.",
        // Bare s3adm / s3adm -x: nothing actionable to run; print help and exit as a usage error
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    root->add_subcommand(make_cred());
    root->execute(argc, argv);
    return g_exit;
}
