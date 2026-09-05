// s3adm `object inspect` — where an object's bytes live inside the routed backend
// (data path / pack, chunk or rados extents with offsets and CRCs / tier state /
// metadata source), for troubleshooting without reading logs or hexdumps
// (roadmap §6.2, docs/cli.md §3.10). Root credential only. Prints the server's
// JSON verbatim, or a readable table with --output=text.
#include "tools/s3adm_object.h"

#include <cstdio>
#include <memory>
#include <string>

#include <nlohmann/json.hpp>

#include "core/util/uri.h"
#include "tools/s3adm_common.h"

namespace {

using s3adm::finish;
using s3adm::g_exit;
using s3adm::run_admin;
using s3adm::SignedClient;
namespace util = lights3::util;

void print_text(const nlohmann::json& j) {
    printf("bucket   %s\nkey      %s\nbackend  %s\n", j.value("bucket", "").c_str(),
           j.value("key", "").c_str(), j.value("backend", "").c_str());
    if (!j.contains("layout") || j["layout"].is_null()) {
        printf("layout   (none) %s\n", j.value("note", "").c_str());
        return;
    }
    auto& l = j["layout"];
    printf("engine   %s\n", l.value("engine", "").c_str());
    for (auto& [k, v] : l["attrs"].items())
        printf("  %-16s %s\n", k.c_str(), v.is_string() ? v.get<std::string>().c_str() : v.dump().c_str());
    if (l.contains("extents") && !l["extents"].empty()) {
        printf("extents  %zu\n  %-7s %20s %12s %12s %10s\n", l["extents"].size(), "kind", "id",
               "offset", "length", "crc32c");
        for (auto& e : l["extents"])
            printf("  %-7s %20llu %12llu %12llu %10u\n", e.value("kind", "").c_str(),
                   (unsigned long long)e.value("id", 0ULL), (unsigned long long)e.value("offset", 0ULL),
                   (unsigned long long)e.value("length", 0ULL), e.value("crc32c", 0U));
    }
}

std::shared_ptr<ccmd::c_command> make_inspect() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "inspect", "s3adm object inspect photos 2026/01/a.jpg --output=text",
        "s3adm object inspect <bucket> <key> [options]",
        "Print the object's internal layout as the routed backend reports it: data path, "
        "metadata source and tier for localfs/xlocalfs; meta version, tier and the "
        "chunk/pack/rados extents (file id, offset, length, crc32c) for duostore; the "
        "tiering view plus the local engine's layout for tiered. memory and cloudproxy "
        "expose no layout. Requires the root credential.",
        "print an object's internal layout.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            if (c->args().size() != 2) {
                fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            auto output = c->var<std::string>("output");
            if (output != "json" && output != "text") {
                fprintf(stderr, "s3adm: --output must be json|text\n");
                g_exit = 2;
                return;
            }
            std::string bucket = c->args()[0], key = c->args()[1];
            run_admin(c, [&](SignedClient& cli) {
                std::string path = "/-/admin/objects/" + util::aws_uri_encode(bucket, true) + "/" +
                                   util::aws_uri_encode(key, /*encode_slash=*/false);
                auto r = cli.get(path, "");
                if (output == "json") return finish(r, 200);
                if (!r || r->status != 200) return finish(r, 200);
                print_text(nlohmann::json::parse(r->body));
                return 0;
            });
        });
    cmd->varp<std::string>("output", "o", "json", "json (server response verbatim) | text (table).");
    s3adm::add_conn_flags(cmd);
    return cmd;
}

}  // namespace

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_object() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "object", "s3adm object inspect photos 2026/01/a.jpg",
        "s3adm object <command> [options]",
        "Object-level operator commands (roadmap §6.2). `inspect` prints the internal "
        "layout of one object; requires the root static credential like `cred`. Options "
        "must follow the leaf subcommand as --name=value.",
        "object-level operator commands.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_inspect());
    return cmd;
}

}  // namespace s3adm
