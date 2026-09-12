// lights3-ctl `tables` -- the S3 Tables catalog from the operator's side (docs/cli.md
// §3.13, docs/s3-tables/step-4-maintenance.md §7): table buckets (enable / disable /
// status), listing, maintenance (plan / run / purge / config), diagnostics and recovery.
// Catalog calls go to "<prefix>/v1/..." signed with service "s3"; plan and run use the
// admin plane (POST /-/admin/tables/<bucket>/<ns-path>/<t>/<op>, root) and poll the job
// like `lights3-ctl duostore gc`. Exit code 0 success / 1 request or job failure / 2 usage
#include "tools/lights3_ctl_tables.h"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "tools/lights3_ctl_common.h"

namespace lights3_ctl {

namespace {

using nlohmann::json;

struct TableRef {
    std::string bucket;
    std::vector<std::string> ns;
    std::string table;
};

// "<bucket> <ns.table>" positional pair
bool table_args(const std::shared_ptr<ccmd::c_command>& c, TableRef& out) {
    if (c->args().size() != 2) {
        fprintf(stderr, "lights3-ctl: usage: %s\n", c->usage().c_str());
        g_exit = 2;
        return false;
    }
    out.bucket = c->args()[0];
    std::string id = c->args()[1];
    auto dot = id.rfind('.');
    if (dot == std::string::npos || dot == 0 || dot + 1 == id.size()) {
        fprintf(stderr, "lights3-ctl: table must be given as <namespace>.<table> (namespace levels joined by '.')\n");
        g_exit = 2;
        return false;
    }
    out.table = id.substr(dot + 1);
    std::string ns = id.substr(0, dot);
    size_t pos = 0;
    while (pos <= ns.size()) {
        size_t next = ns.find('.', pos);
        if (next == std::string::npos) next = ns.size();
        if (next > pos) out.ns.push_back(ns.substr(pos, next - pos));
        pos = next + 1;
    }
    return !out.ns.empty();
}

bool one_bucket_arg(const std::shared_ptr<ccmd::c_command>& c, std::string& bucket) {
    if (c->args().size() != 1) {
        fprintf(stderr, "lights3-ctl: usage: %s\n", c->usage().c_str());
        g_exit = 2;
        return false;
    }
    bucket = c->args().front();
    return true;
}

std::string join(const std::vector<std::string>& v, const std::string& sep) {
    std::string s;
    for (auto& x : v) {
        if (!s.empty()) s += sep;
        s += x;
    }
    return s;
}

std::string catalog_root(const std::shared_ptr<ccmd::c_command>& c) {
    std::string p = c->var<std::string>("catalog-prefix");
    if (p.empty()) p = "/iceberg";
    if (p.front() != '/') p = "/" + p;
    while (p.size() > 1 && p.back() == '/') p.pop_back();
    return p + "/v1";
}

// "<prefix>/v1/<bucket>/namespaces/<a%1Fb>/tables/<t>"
std::string table_path(const std::string& root, const TableRef& t) {
    return root + "/" + t.bucket + "/namespaces/" + join(t.ns, "%1F") + "/tables/" + t.table;
}

std::string admin_path(const TableRef& t, const std::string& op) {
    return "/-/admin/tables/" + t.bucket + "/" + join(t.ns, "/") + "/" + t.table + "/" + op;
}

void print_body(const std::string& body) {
    fputs(body.c_str(), stdout);
    if (!body.empty() && body.back() != '\n') fputc('\n', stdout);
}

// Poll a job's status document until it is finished; prints it. 1 = the job failed
int wait_job(SignedClient& cli, const std::string& status_path, uint64_t job, const std::string& label) {
    for (;;) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        auto st = cli.get(status_path, "");
        if (!st || st->status != 200) return finish(st, 200);
        auto doc = json::parse(st->body, nullptr, false);
        if (doc.is_discarded()) {
            fprintf(stderr, "lights3-ctl: %s: unparsable status document\n", label.c_str());
            return 1;
        }
        if (doc.value("running", false)) continue;
        if (doc.value("job_id", uint64_t(0)) != job) continue;
        print_body(st->body);
        return doc.contains("error") ? 1 : 0;
    }
}

// POST an admin-plane job and (unless no_wait) follow it to the end
int admin_job(SignedClient& cli, const TableRef& t, const std::string& op, const std::string& body, bool no_wait) {
    std::string path = admin_path(t, op);
    auto r = body.empty() ? cli.post_empty(path, "") : cli.post_json(path, body);
    if (!r || r->status != 202) return finish(r, 202);
    if (no_wait) {
        print_body(r->body);
        return 0;
    }
    uint64_t job = json::parse(r->body, nullptr, false).value("job_id", uint64_t(0));
    return wait_job(cli, path, job, "tables " + op);
}

void add_common(const std::shared_ptr<ccmd::c_command>& cmd) {
    cmd->var<std::string>("catalog-prefix", "/iceberg", "the catalog's path prefix (tables.path_prefix).");
    add_conn_flags(cmd);
}

std::shared_ptr<ccmd::c_command> make_bucket_cmd(const std::string& name, const std::string& method,
                                                 const std::string& what) {
    auto cmd = std::make_shared<ccmd::c_command>(
        name, "lights3-ctl tables " + name + " lake", "lights3-ctl tables " + name + " <bucket> [options]",
        what + " (" + method + " <prefix>/v1/buckets/<bucket>" + (name == "status" ? "" : ", root credential") + ").",
        what + ".", [name, method](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            run_admin(c, [&](SignedClient& cli) {
                std::string path = catalog_root(c) + "/buckets/" + bucket;
                if (method == "PUT") return finish(cli.put_json(path, "{}"), 200);
                if (method == "DELETE") return finish(cli.del(path), 204, "table bucket " + bucket + " disabled");
                auto r = cli.get(path, "");
                int rc = finish(r, 200);
                if (rc == 0 && r && !r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
                return rc;
            });
        });
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_list() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "list", "lights3-ctl tables list lake --namespace=sales", "lights3-ctl tables list <bucket> [options]",
        "List the tables of a table bucket, one \"<namespace>\\t<table>\" line each (namespace "
        "levels joined by '.'); --namespace restricts to that namespace, otherwise every "
        "namespace is walked. --json prints the identifiers as a JSON array instead.",
        "list tables.", [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket;
            if (!one_bucket_arg(c, bucket)) return;
            std::string only = c->var<std::string>("namespace");
            bool as_json = c->var<bool>("json");
            run_admin(c, [&](SignedClient& cli) {
                std::string root = catalog_root(c) + "/" + bucket;
                std::vector<std::vector<std::string>> namespaces;
                if (!only.empty()) {
                    std::vector<std::string> levels;
                    size_t pos = 0;
                    while (pos <= only.size()) {
                        size_t next = only.find('.', pos);
                        if (next == std::string::npos) next = only.size();
                        if (next > pos) levels.push_back(only.substr(pos, next - pos));
                        pos = next + 1;
                    }
                    namespaces.push_back(levels);
                } else {
                    // breadth-first over the namespace tree
                    std::vector<std::vector<std::string>> queue{{}};
                    while (!queue.empty()) {
                        auto parent = queue.front();
                        queue.erase(queue.begin());
                        auto r = cli.get(root + "/namespaces", parent.empty() ? "" : "parent=" + join(parent, "%1F"));
                        if (!r || r->status != 200) return finish(r, 200);
                        auto doc = json::parse(r->body, nullptr, false);
                        if (doc.is_discarded()) return 1;
                        for (auto& n : doc.value("namespaces", json::array())) {
                            std::vector<std::string> levels;
                            for (auto& l : n) levels.push_back(l.get<std::string>());
                            namespaces.push_back(levels);
                            queue.push_back(levels);
                        }
                    }
                }
                json all = json::array();
                for (auto& ns : namespaces) {
                    auto r = cli.get(root + "/namespaces/" + join(ns, "%1F") + "/tables", "");
                    if (!r || r->status != 200) return finish(r, 200);
                    auto doc = json::parse(r->body, nullptr, false);
                    if (doc.is_discarded()) return 1;
                    for (auto& id : doc.value("identifiers", json::array())) {
                        all.push_back(id);
                        if (!as_json) printf("%s\t%s\n", join(ns, ".").c_str(), id.value("name", "").c_str());
                    }
                }
                if (as_json) print_body(all.dump());
                return 0;
            });
        });
    cmd->var<std::string>("namespace", "", "only this namespace (levels joined by '.').");
    cmd->var<bool>("json", false, "print a JSON array of {namespace, name} instead of lines.");
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_plan() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "plan", "lights3-ctl tables plan lake sales.orders", "lights3-ctl tables plan <bucket> <ns.table> [options]",
        "Plan the maintenance of a table (POST /-/admin/tables/<bucket>/<ns>/<table>/plan, root "
        "credential): metadata files past the retention set and safety window, snapshots past "
        "history.expire.max-snapshot-age-ms, data files no retained metadata reaches. Read-only; "
        "waits for the job and prints its document (\"stats\" is the plan, \"manual-review\" "
        "true means run will refuse it). --no-wait returns the job id at once.",
        "plan a table's maintenance.", [](const std::shared_ptr<ccmd::c_command>& c) {
            TableRef t;
            if (!table_args(c, t)) return;
            bool no_wait = c->var<bool>("no-wait");
            run_admin(c, [&](SignedClient& cli) { return admin_job(cli, t, "plan", "", no_wait); });
        });
    cmd->var<bool>("no-wait", false, "return right after starting the job (prints the job id).");
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_run() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "run", "lights3-ctl tables run lake sales.orders --yes", "lights3-ctl tables run <bucket> <ns.table> [options]",
        "Execute the table's most recent plan (or the plan job named by --plan-job): the "
        "snapshot expiry is a regular commit, then -- only when delete_enabled is true for the "
        "table -- the candidate files are deleted after a fresh safety-window check. A table "
        "that changed since the plan fails with StalePlan (plan again). When delete_enabled is "
        "true the command refuses to start without --yes. Waits for the job and prints its "
        "document; --no-wait returns the job id at once.",
        "run a table's maintenance plan.", [](const std::shared_ptr<ccmd::c_command>& c) {
            TableRef t;
            if (!table_args(c, t)) return;
            bool no_wait = c->var<bool>("no-wait");
            bool yes = c->var<bool>("yes");
            uint64_t plan_job = static_cast<uint64_t>(c->var<int>("plan-job"));
            run_admin(c, [&](SignedClient& cli) {
                if (!yes) {
                    auto r = cli.get(table_path(catalog_root(c), t) + "/maintenance/config", "");
                    if (!r || r->status != 200) return finish(r, 200);
                    auto doc = json::parse(r->body, nullptr, false);
                    if (!doc.is_discarded() && doc["effective"].value("delete_enabled", false)) {
                        fprintf(stderr,
                                "lights3-ctl: delete_enabled is true for this table: run deletes files; add --yes\n");
                        return 2;
                    }
                }
                std::string body;
                if (plan_job) body = json({{"job_id", plan_job}}).dump();
                return admin_job(cli, t, "run", body, no_wait);
            });
        });
    cmd->var<int>("plan-job", 0, "the plan job id to execute (default: the table's latest plan).");
    cmd->var<bool>("yes", false, "confirm file deletion when delete_enabled is true.");
    cmd->var<bool>("no-wait", false, "return right after starting the job (prints the job id).");
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_purge() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "purge", "lights3-ctl tables purge lake sales.orders --yes",
        "lights3-ctl tables purge <bucket> <ns.table> --yes [options]",
        "Drop a table and delete everything it owns (DELETE <prefix>/v1/<bucket>/namespaces/<ns>/"
        "tables/<table>?purgeRequested=true): the tombstone is written at once, then a job "
        "removes the reserved metadata directory, the location prefix, the commit records and "
        "the tombstone. Irreversible and unaware of readers -- --yes is mandatory. Waits for the "
        "job (GET .../maintenance/jobs/<id>) and prints its document.",
        "drop a table and purge its files.", [](const std::shared_ptr<ccmd::c_command>& c) {
            TableRef t;
            if (!table_args(c, t)) return;
            if (!c->var<bool>("yes")) {
                fprintf(stderr, "lights3-ctl: purge deletes the table's data; add --yes\n");
                g_exit = 2;
                return;
            }
            bool no_wait = c->var<bool>("no-wait");
            run_admin(c, [&](SignedClient& cli) {
                std::string path = table_path(catalog_root(c), t);
                auto r = cli.del(path, "purgeRequested=true");
                if (!r || r->status != 204) return finish(r, 204);
                std::string id = r->get_header_value("x-lights3-job-id");
                if (id.empty()) {
                    fprintf(stderr, "lights3-ctl: dropped, but no purge job id came back\n");
                    return 1;
                }
                if (no_wait) {
                    print_body(json({{"job_id", std::stoull(id)}}).dump());
                    return 0;
                }
                return wait_job(cli, path + "/maintenance/jobs/" + id, std::stoull(id), "tables purge");
            });
        });
    cmd->var<bool>("yes", false, "confirm the irreversible deletion.");
    cmd->var<bool>("no-wait", false, "return right after the drop (prints the job id).");
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_config() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "config", "lights3-ctl tables config lake sales.orders --set='{\"delete_enabled\":true}'",
        "lights3-ctl tables config <bucket> <ns.table> [--set=<json>] [options]",
        "Show a table's maintenance settings (GET .../maintenance/config: the effective values, "
        "the table's own object and the tables.maintenance defaults) or replace the table's "
        "object with --set (PUT; keys retain_recent_metadata_files, delete_enabled, "
        "max_snapshot_age_ms, min_snapshots_to_keep, orphan_cleanup; omitted keys fall back).",
        "show or set a table's maintenance settings.", [](const std::shared_ptr<ccmd::c_command>& c) {
            TableRef t;
            if (!table_args(c, t)) return;
            std::string set = c->var<std::string>("set");
            run_admin(c, [&](SignedClient& cli) {
                std::string path = table_path(catalog_root(c), t) + "/maintenance/config";
                auto r = set.empty() ? cli.get(path, "") : cli.put_json(path, set);
                int rc = finish(r, 200);
                if (rc == 0 && r && !r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
                return rc;
            });
        });
    cmd->var<std::string>("set", "", "JSON object of settings to store for the table.");
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_diagnose() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "diagnose", "lights3-ctl tables diagnose lake sales.orders",
        "lights3-ctl tables diagnose <bucket> <ns.table> [options]",
        "Classify the table's commit records against its pointer (GET .../catalog/diagnostics: "
        "Committed / StagedBeforeTableUpdate / FinalizationRequired / Superseded / ManualReview) "
        "and list metadata files nothing references. Read-only.",
        "diagnose a table's commit log.", [](const std::shared_ptr<ccmd::c_command>& c) {
            TableRef t;
            if (!table_args(c, t)) return;
            run_admin(c, [&](SignedClient& cli) {
                auto r = cli.get(table_path(catalog_root(c), t) + "/catalog/diagnostics", "");
                int rc = finish(r, 200);
                if (rc == 0 && r && !r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
                return rc;
            });
        });
    add_common(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_recover() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "recover", "lights3-ctl tables recover lake sales.orders --prune",
        "lights3-ctl tables recover <bucket> <ns.table> [--prune] [options]",
        "Repair the table's commit log (POST .../catalog/recovery): records the pointer already "
        "carries are finalized; with --prune the superseded and stale staged records are "
        "deleted. Never moves the pointer.",
        "finalize / prune a table's commit records.", [](const std::shared_ptr<ccmd::c_command>& c) {
            TableRef t;
            if (!table_args(c, t)) return;
            bool prune = c->var<bool>("prune");
            run_admin(c, [&](SignedClient& cli) {
                auto r = cli.post_json(table_path(catalog_root(c), t) + "/catalog/recovery",
                                       json({{"prune", prune}}).dump());
                int rc = finish(r, 200);
                if (rc == 0 && r && !r->body.empty() && r->body.back() != '\n') fputc('\n', stdout);
                return rc;
            });
        });
    cmd->var<bool>("prune", false, "also delete superseded / stale staged records.");
    add_common(cmd);
    return cmd;
}

}  // namespace

std::shared_ptr<ccmd::c_command> make_tables() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "tables", "lights3-ctl tables list lake", "lights3-ctl tables <command> [options]",
        "S3 Tables / Iceberg REST catalog operations (docs/s3-tables-design.md): table buckets "
        "(enable / disable / status), listing, maintenance (config / plan / run / purge) and "
        "the commit-log diagnostics (diagnose / recover). Catalog calls go to "
        "<prefix>/v1 (--catalog-prefix, default /iceberg); plan and run are admin-plane jobs "
        "(root credential). Options must follow the leaf subcommand as --name=value.",
        "S3 Tables catalog: buckets, listing, maintenance, diagnostics.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_bucket_cmd("enable", "PUT", "Enable a bucket as a table bucket"));
    cmd->add_subcommand(make_bucket_cmd("disable", "DELETE", "Disable an empty table bucket"));
    cmd->add_subcommand(make_bucket_cmd("status", "GET", "Show a table bucket's marker"));
    cmd->add_subcommand(make_list());
    cmd->add_subcommand(make_config());
    cmd->add_subcommand(make_plan());
    cmd->add_subcommand(make_run());
    cmd->add_subcommand(make_purge());
    cmd->add_subcommand(make_diagnose());
    cmd->add_subcommand(make_recover());
    return cmd;
}

}  // namespace lights3_ctl
