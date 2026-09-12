#include "tables/rest_api.h"

#include <algorithm>
#include <map>
#include <set>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/uri.h"
#include "s3/errors.h"
#include "tables/iceberg/metadata.h"
#include "tables/maintenance.h"
#include "tables/rest_error.h"

namespace lights3::tables {

using nlohmann::json;
using s3::Action;

// ---------- route table ----------

namespace {

constexpr RestApi::Route kRoutes[] = {
    // config lives above the warehouse level; matched specially in dispatch
    {"GET", "config", Action::Read, RestApi::KeyKind::None, false, "GetConfig", true, &RestApi::get_config},
    {"PUT", "buckets/{w2}", Action::Write, RestApi::KeyKind::None, true, "EnableTableBucket", false,
     &RestApi::enable_bucket},
    {"GET", "buckets/{w2}", Action::Read, RestApi::KeyKind::None, false, "GetTableBucket", false, &RestApi::get_bucket},
    {"DELETE", "buckets/{w2}", Action::Delete, RestApi::KeyKind::None, true, "DisableTableBucket", false,
     &RestApi::disable_bucket},
    {"GET", "namespaces", Action::Read, RestApi::KeyKind::None, false, "ListNamespaces", true,
     &RestApi::list_namespaces},
    {"POST", "namespaces", Action::Write, RestApi::KeyKind::Namespace, false, "CreateNamespace", true,
     &RestApi::create_namespace},
    {"GET", "namespaces/{ns}", Action::Read, RestApi::KeyKind::Namespace, false, "LoadNamespace", true,
     &RestApi::load_namespace},
    {"HEAD", "namespaces/{ns}", Action::Read, RestApi::KeyKind::Namespace, false, "NamespaceExists", true,
     &RestApi::namespace_exists},
    {"DELETE", "namespaces/{ns}", Action::Delete, RestApi::KeyKind::Namespace, false, "DropNamespace", true,
     &RestApi::drop_namespace},
    {"POST", "namespaces/{ns}/properties", Action::Write, RestApi::KeyKind::Namespace, false,
     "UpdateNamespaceProperties", true, &RestApi::update_namespace_properties},
    {"GET", "namespaces/{ns}/tables", Action::Read, RestApi::KeyKind::Namespace, false, "ListTables", true,
     &RestApi::list_tables},
    {"POST", "namespaces/{ns}/tables", Action::Write, RestApi::KeyKind::Namespace, false, "CreateTable", true,
     &RestApi::create_table},
    {"POST", "namespaces/{ns}/register", Action::Write, RestApi::KeyKind::Namespace, false, "RegisterTable", true,
     &RestApi::register_table},
    {"GET", "namespaces/{ns}/tables/{t}", Action::Read, RestApi::KeyKind::Table, false, "LoadTable", true,
     &RestApi::load_table},
    {"HEAD", "namespaces/{ns}/tables/{t}", Action::Read, RestApi::KeyKind::Table, false, "TableExists", true,
     &RestApi::table_exists},
    {"POST", "namespaces/{ns}/tables/{t}", Action::Write, RestApi::KeyKind::Table, false, "CommitTable", true,
     &RestApi::commit_table},
    {"DELETE", "namespaces/{ns}/tables/{t}", Action::Delete, RestApi::KeyKind::Table, false, "DropTable", true,
     &RestApi::drop_table},
    {"POST", "tables/rename", Action::Write, RestApi::KeyKind::None, false, "RenameTable", true,
     &RestApi::rename_table},
    // Iceberg views (step ⑥ §1): a view is addressed like a table for authorization
    {"GET", "namespaces/{ns}/views", Action::Read, RestApi::KeyKind::Namespace, false, "ListViews", true,
     &RestApi::list_views},
    {"POST", "namespaces/{ns}/views", Action::Write, RestApi::KeyKind::Namespace, false, "CreateView", true,
     &RestApi::create_view},
    {"GET", "namespaces/{ns}/views/{t}", Action::Read, RestApi::KeyKind::Table, false, "LoadView", true,
     &RestApi::load_view},
    {"HEAD", "namespaces/{ns}/views/{t}", Action::Read, RestApi::KeyKind::Table, false, "ViewExists", true,
     &RestApi::view_exists},
    {"POST", "namespaces/{ns}/views/{t}", Action::Write, RestApi::KeyKind::Table, false, "ReplaceView", true,
     &RestApi::replace_view},
    {"DELETE", "namespaces/{ns}/views/{t}", Action::Delete, RestApi::KeyKind::Table, false, "DropView", true,
     &RestApi::drop_view},
    {"POST", "views/rename", Action::Write, RestApi::KeyKind::None, false, "RenameView", true, &RestApi::rename_view},
    {"POST", "namespaces/{ns}/tables/{t}/metrics", Action::Read, RestApi::KeyKind::Table, false, "ReportMetrics", true,
     &RestApi::report_metrics},
    {"GET", "namespaces/{ns}/tables/{t}/credentials", Action::Read, RestApi::KeyKind::Table, false, "LoadCredentials",
     true, &RestApi::load_credentials},
    {"GET", "namespaces/{ns}/tables/{t}/metadata-location", Action::Read, RestApi::KeyKind::Table, false,
     "GetTableMetadataLocation", false, &RestApi::get_metadata_location},
    {"PUT", "namespaces/{ns}/tables/{t}/metadata-location", Action::Write, RestApi::KeyKind::Table, false,
     "UpdateTableMetadataLocation", false, &RestApi::put_metadata_location},
    // commit-record diagnostics and recovery (design §5.4, step ③ §6)
    {"GET", "namespaces/{ns}/tables/{t}/catalog/diagnostics", Action::Read, RestApi::KeyKind::Table, false,
     "DiagnoseTable", false, &RestApi::diagnose_table},
    {"POST", "namespaces/{ns}/tables/{t}/catalog/recovery", Action::Write, RestApi::KeyKind::Table, false,
     "RecoverTable", false, &RestApi::recover_table},
    // maintenance (design §9, step ④): settings, plan / run jobs, job status
    {"GET", "namespaces/{ns}/tables/{t}/maintenance/config", Action::Read, RestApi::KeyKind::Table, false,
     "GetMaintenanceConfig", false, &RestApi::get_maintenance_config},
    {"PUT", "namespaces/{ns}/tables/{t}/maintenance/config", Action::Write, RestApi::KeyKind::Table, false,
     "PutMaintenanceConfig", false, &RestApi::put_maintenance_config},
    {"POST", "namespaces/{ns}/tables/{t}/maintenance/plan", Action::Write, RestApi::KeyKind::Table, false,
     "PlanMaintenance", false, &RestApi::plan_maintenance},
    {"POST", "namespaces/{ns}/tables/{t}/maintenance/run", Action::Write, RestApi::KeyKind::Table, false,
     "RunMaintenance", false, &RestApi::run_maintenance},
    {"GET", "namespaces/{ns}/tables/{t}/maintenance/jobs/{w2}", Action::Read, RestApi::KeyKind::Table, false,
     "GetMaintenanceJob", false, &RestApi::maintenance_job},
};

std::vector<std::string_view> split(std::string_view s, char sep) {
    std::vector<std::string_view> out;
    size_t pos = 0;
    while (pos <= s.size()) {
        size_t next = s.find(sep, pos);
        if (next == std::string_view::npos) next = s.size();
        out.push_back(s.substr(pos, next - pos));
        pos = next + 1;
    }
    return out;
}

std::map<std::string, std::string> string_map(const json& j, const char* what) {
    std::map<std::string, std::string> out;
    if (j.is_null()) return out;
    if (!j.is_object()) throw bad_request(std::string(what) + " must be an object of strings");
    for (auto& [k, v] : j.items()) {
        if (!v.is_string()) throw bad_request(std::string(what) + "." + k + " must be a string");
        out[k] = v.get<std::string>();
    }
    return out;
}

json levels_json(const Levels& l) {
    json j = json::array();
    for (auto& s : l) j.push_back(s);
    return j;
}

}  // namespace

std::span<const RestApi::Route> RestApi::routes() { return kRoutes; }

// A table is addressed by "<ns>/<t>"; a policy scoped to the table's own prefix
// ("<ns>/<t>/", what a vended session carries) must reach it too
bool RestApi::allows_table(const s3::CredentialPolicy& p, std::string_view bucket, const Levels& ns,
                           std::string_view table, s3::Action action) {
    std::string key = ns_path(ns) + "/" + std::string(table);
    return p.allows(bucket, key, action) || p.allows(bucket, key + "/", action);
}

std::vector<std::string> RestApi::advertised_endpoints() {
    std::vector<std::string> out;
    for (auto& r : kRoutes) {
        if (!r.standard) continue;
        std::string p = std::string(r.pattern);
        if (p == "config") {
            out.push_back("GET /v1/config");
            continue;
        }
        size_t pos;
        bool view = p.find("views") != std::string::npos;
        while ((pos = p.find("{ns}")) != std::string::npos) p.replace(pos, 4, "{namespace}");
        while ((pos = p.find("{t}")) != std::string::npos) p.replace(pos, 3, view ? "{view}" : "{table}");
        out.push_back(std::string(r.method) + " /v1/{prefix}/" + p);
    }
    return out;
}

RestApi::RestApi(std::shared_ptr<Catalog> catalog, TablesConfig cfg, MetricsScope metrics)
    : catalog_(std::move(catalog)), cfg_(std::move(cfg)), metrics_(std::move(metrics)) {
    requests_ = metrics_.counter("lights3_tables_requests_total", "Iceberg REST catalog requests");
}

std::string RestApi::matched_prefix(std::string_view path) const {
    for (const std::string* prefix : {&cfg_.path_prefix, &cfg_.compat_prefix}) {
        if (prefix->empty()) continue;
        std::string base = *prefix + "/v1";
        if (path == base) return *prefix;
        if (path.size() > base.size() && path.compare(0, base.size(), base) == 0 && path[base.size()] == '/')
            return *prefix;
    }
    return {};
}

bool RestApi::matches(std::string_view path) const { return !matched_prefix(path).empty(); }

std::vector<std::string> RestApi::split_path(std::string_view raw_path, size_t skip) {
    std::vector<std::string> out;
    auto parts = split(raw_path, '/');
    size_t seen = 0;
    for (auto p : parts) {
        if (p.empty()) continue;
        if (seen++ < skip) continue;
        out.push_back(util::percent_decode(p));
    }
    return out;
}

bool RestApi::match_route(const Route& r, const std::vector<std::string>& segs, Match& m) const {
    auto pat = split(r.pattern, '/');
    if (pat.size() != segs.size()) return false;
    Match tmp;
    tmp.route = &r;
    for (size_t i = 0; i < pat.size(); ++i) {
        if (pat[i] == "{ns}") {
            // the segment is already percent-decoded; multi-level namespaces carry U+001F
            tmp.ns = parse_namespace_path(segs[i]);
        } else if (pat[i] == "{t}") {
            tmp.table = segs[i];
        } else if (pat[i] == "{w2}") {
            tmp.extra = segs[i];
        } else if (pat[i] != segs[i]) {
            return false;
        }
    }
    m = std::move(tmp);
    return true;
}

http::HttpResponse RestApi::json_response(int status, const json& j) {
    http::HttpResponse resp;
    resp.status = status;
    resp.small_body = j.dump();
    resp.headers.set("Content-Type", "application/json");
    return resp;
}

http::HttpResponse RestApi::empty_response(int status) {
    http::HttpResponse resp;
    resp.status = status;
    return resp;
}

Task<json> RestApi::read_json(http::HttpRequest& req, bool allow_empty) const {
    std::string text;
    if (req.body) {
        std::byte buf[16 * 1024];
        for (;;) {
            size_t n = co_await req.body->read(std::span(buf));
            if (n == 0) break;
            if (text.size() + n > cfg_.request_max_size) throw bad_request("request body exceeds the size limit");
            text.append(reinterpret_cast<const char*>(buf), n);
        }
    }
    if (text.empty()) {
        if (allow_empty) co_return json::object();
        throw bad_request("request requires a JSON body");
    }
    json j;
    try {
        j = json::parse(text);
    } catch (const json::exception&) {
        throw bad_request("request body is not valid JSON");
    }
    if (!j.is_object()) throw bad_request("request body must be a JSON object");
    co_return j;
}

// ---------- pagination (design §4.6) ----------

namespace {

std::string page_context(std::string_view op, std::string_view bucket, const Levels& ns) {
    std::string s(op);
    s.push_back('\0');
    s += bucket;
    s.push_back('\0');
    s += ns_path(ns);
    return util::sha256_hex(s).substr(0, 32);
}

}  // namespace

PageCursor RestApi::page_cursor(const http::HttpRequest& req, std::string_view op, const Match& m) const {
    PageCursor c;
    auto size = req.query_get("pageSize");
    auto token = req.query_get("pageToken");
    if (!size && !token) {
        // unpaginated: everything the store can return (bounded by the max page size)
        c.limit = cfg_.max_page_size;
        return c;
    }
    c.limit = cfg_.max_page_size;
    if (size) {
        int n = 0;
        try {
            size_t pos = 0;
            n = std::stoi(*size, &pos);
            if (pos != size->size()) n = 0;
        } catch (const std::exception&) {
            n = 0;
        }
        if (n <= 0) throw bad_request("pageSize must be a positive integer");
        c.limit = std::min(n, cfg_.max_page_size);
    }
    if (token && !token->empty()) {
        if (token->size() > 4096) throw bad_request("pageToken is too long");
        json j;
        try {
            j = json::parse(base64url_decode(*token));
        } catch (const std::exception&) {
            throw bad_request("pageToken is malformed");
        }
        if (!j.is_object() || j.value("v", 0) != 1 || j.value("ctx", "") != page_context(op, m.bucket, m.ns))
            throw bad_request("pageToken query parameter does not match this list operation");
        c.after = j.value("after", "");
    }
    return c;
}

std::string RestApi::page_token(std::string_view op, const Match& m, std::string_view after) const {
    json j;
    j["v"] = 1;
    j["ctx"] = page_context(op, m.bucket, m.ns);
    j["after"] = std::string(after);
    return base64url(j.dump());
}

// ---------- dispatch ----------

void RestApi::audit(Hooks& hooks, std::string_view op, const Match& m, std::string detail) const {
    if (!hooks.audit) return;
    std::string key = ns_path(m.ns);
    if (!m.table.empty()) key += "/" + m.table;
    s3::AuditEvent e;
    std::string event = "tables." + std::string(op);
    e.event = event;
    e.actor = hooks.access_key;
    e.request_id = hooks.request_id;
    e.bucket = m.bucket;
    e.key = key;
    e.detail = detail;
    hooks.audit(e);
}

Task<http::HttpResponse> RestApi::dispatch(http::HttpRequest& req, Hooks& hooks, std::string& access_key,
                                           std::string& api_name) {
    http::HttpResponse resp;
    std::optional<RestError> failure;
    try {
        // "<prefix>/v1" has prefix_segments + 1 segments; the compat prefix (step ⑥ §2) is
        // an alias of the same routes
        size_t skip = 0;
        std::string prefix = matched_prefix(req.path);
        if (prefix.empty()) prefix = cfg_.path_prefix;
        for (auto p : split(prefix, '/'))
            if (!p.empty()) ++skip;
        skip += 1;
        auto segs = split_path(req.raw_path.empty() ? req.path : req.raw_path, skip);
        Match m;
        bool found = false;
        bool method_mismatch = false;
        if (segs.size() == 1 && segs[0] == "config") {
            for (auto& r : kRoutes)
                if (r.pattern == "config") {
                    if (req.method == r.method) {
                        m.route = &r;
                        found = true;
                    } else {
                        method_mismatch = true;
                    }
                }
        } else if (!segs.empty()) {
            // "<prefix>/v1/buckets/{bucket}" (table-bucket admin, design §6.3) has no
            // warehouse segment of its own: the bucket is the second segment
            bool bucket_op = segs.size() == 2 && segs[0] == "buckets";
            std::vector<std::string> rest(segs.begin() + (bucket_op ? 0 : 1), segs.end());
            std::string bucket = bucket_op ? segs[1] : segs[0];
            for (auto& r : kRoutes) {
                if (r.pattern == "config") continue;
                Match tmp;
                if (!match_route(r, rest, tmp)) continue;
                if (req.method != r.method) {
                    method_mismatch = true;
                    continue;
                }
                m = std::move(tmp);
                m.bucket = bucket;
                found = true;
                break;
            }
        }
        if (!found) {
            if (method_mismatch)
                throw RestError(405, "MethodNotAllowedException", "method not allowed on this resource");
            throw not_found_resource("no such catalog resource");
        }
        api_name = "Iceberg." + std::string(m.route->name);
        if (!m.bucket.empty()) storage::validate_bucket_name(m.bucket);
        // Generic SigV4 clients (PyIceberg / Spark with rest.sigv4-enabled, AWS SDKs
        // signing for s3tables) hash the payload into the canonical request but send no
        // x-amz-content-sha256 header, which the S3-plane verifier requires. Catalog
        // bodies are small and bounded: buffer, hash, and present the hash as the header
        // so the signature check sees what the client signed (docs/s3-tables-design.md §6.2)
        if (req.body && !req.headers.has("x-amz-content-sha256") && !req.query_has("X-Amz-Algorithm")) {
            std::string text;
            std::byte buf[16 * 1024];
            for (;;) {
                size_t n = co_await req.body->read(std::span(buf));
                if (n == 0) break;
                if (text.size() + n > cfg_.request_max_size) throw bad_request("request body exceeds the size limit");
                text.append(reinterpret_cast<const char*>(buf), n);
            }
            req.headers.set("x-amz-content-sha256", util::sha256_hex(text));
            req.body = std::make_unique<http::StringBodyReader>(std::move(text));
        }
        // authentication
        s3::VerifiedIdentity ident = hooks.verify(req);
        access_key = ident.access_key;
        hooks.access_key = access_key;
        hooks.tenant = ident.tenant;
        hooks.policy = ident.policy;
        // authorization (design §6.2): catalog operations map onto (bucket, key, action)
        if (m.route->root_only && !hooks.is_root(access_key))
            throw forbidden("this operation requires a root (statically configured) credential");
        if (ident.policy && !m.bucket.empty()) {
            const s3::CredentialPolicy& p = *ident.policy;
            bool ok = false;
            switch (m.route->key_kind) {
                case KeyKind::None:
                    ok = p.allows(m.bucket, "", m.route->action);
                    break;
                case KeyKind::Namespace: {
                    // reads follow the ListObjects CommonPrefixes rule (a credential scoped to
                    // "sales/orders/" may look into namespace "sales"); writes need the prefix itself
                    std::string key = ns_path(m.ns) + "/";
                    ok = m.route->action == Action::Read
                             ? p.allows_bucket(m.bucket) && p.allows_action(m.route->action) &&
                                   p.prefix_may_contain(key)
                             : p.allows(m.bucket, key, m.route->action);
                    break;
                }
                case KeyKind::Table:
                    ok = allows_table(p, m.bucket, m.ns, m.table, m.route->action);
                    break;
            }
            if (!ok) throw forbidden("Access denied by credential policy.");
        }
        if (!ident.tenant.empty() && hooks.tenant_gate && !m.bucket.empty())
            co_await hooks.tenant_gate(m.bucket, ident.tenant);
        // GCC ICEs on co_await of a member-pointer call: materialize the task first
        Handler fn = m.route->fn;
        Task<http::HttpResponse> task = (this->*fn)(req, hooks, m);
        resp = co_await std::move(task);
    } catch (const RestError& e) {
        failure = e;
    } catch (const s3::S3Error& e) {
        failure = from_s3_error(e, req.path);
        if (e.code == s3::S3ErrorCode::InternalError)
            LOG_ERROR("tables: {} {} internal error: {}", req.method, req.path, e.message);
    } catch (const std::exception& e) {
        LOG_ERROR("tables: {} {} internal error: {}", req.method, req.path, e.what());
        failure = internal("internal error");
    }
    if (failure) {
        resp = json_response(failure->status, to_json(*failure));
        for (auto& [k, v] : failure->headers) resp.headers.set(k, v);
        if (req.method == "HEAD") resp.small_body.clear();
    }
    requests_->inc();
    metrics_
        .counter(
            "lights3_tables_requests_by_op_total", "Iceberg REST catalog requests by operation and status",
            {{"op", api_name.empty() ? std::string("unmatched") : api_name}, {"status", std::to_string(resp.status)}})
        ->inc();
    co_return resp;
}

// ---------- handlers ----------

Task<http::HttpResponse> RestApi::get_config(http::HttpRequest& req, Hooks&, const Match&) {
    json j;
    j["defaults"] = json::object();
    j["defaults"]["lights3.catalog-prefix"] = cfg_.path_prefix + "/v1";
    if (!cfg_.compat_prefix.empty()) j["defaults"]["lights3.catalog-compat-prefix"] = cfg_.compat_prefix + "/v1";
    j["overrides"] = json::object();
    j["overrides"]["namespace-separator"] = "%1F";
    if (auto w = req.query_get("warehouse")) {
        if (w->empty()) throw bad_request("warehouse must not be empty");
        storage::validate_bucket_name(*w);
        j["defaults"]["warehouse"] = *w;
        j["overrides"]["prefix"] = *w;
    }
    j["endpoints"] = advertised_endpoints();
    co_return json_response(200, j);
}

namespace {

json bucket_json(const std::string& bucket, const TableBucketEntry& e, const std::string& prefix) {
    json j;
    j["table-bucket"] = bucket;
    j["enabled"] = e.enabled;
    j["reserved-prefix"] = e.reserved_prefix;
    j["warehouse-location"] = "s3://" + bucket + "/";
    j["catalog-uri"] = prefix + "/v1/" + bucket;
    j["properties"] = e.properties;
    j["created-unix"] = e.created_unix;
    return j;
}

}  // namespace

Task<http::HttpResponse> RestApi::enable_bucket(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    if (m.extra != m.bucket) throw bad_request("warehouse and bucket in the path must match");
    co_await read_json(req, /*allow_empty=*/true);
    auto e = co_await catalog_->enable_bucket(m.bucket);
    audit(hooks, "enable_bucket", m, "");
    co_return json_response(200, bucket_json(m.bucket, e, cfg_.path_prefix));
}

Task<http::HttpResponse> RestApi::get_bucket(http::HttpRequest&, Hooks&, const Match& m) {
    if (m.extra != m.bucket) throw bad_request("warehouse and bucket in the path must match");
    auto e = co_await catalog_->require_table_bucket(m.bucket);
    co_return json_response(200, bucket_json(m.bucket, e, cfg_.path_prefix));
}

Task<http::HttpResponse> RestApi::disable_bucket(http::HttpRequest&, Hooks& hooks, const Match& m) {
    if (m.extra != m.bucket) throw bad_request("warehouse and bucket in the path must match");
    co_await catalog_->disable_bucket(m.bucket);
    audit(hooks, "disable_bucket", m, "");
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::list_namespaces(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    Levels parent;
    if (auto p = req.query_get("parent"); p && !p->empty()) parent = parse_namespace_path(*p);
    Match ctx = m;
    ctx.ns = parent;
    PageCursor c = page_cursor(req, "namespaces", ctx);
    auto page = co_await catalog_->list_namespaces(m.bucket, parent, c);
    json j;
    j["namespaces"] = json::array();
    // policy filtering (design §6.2): a prefix-scoped credential only sees namespaces
    // that may hold keys under its prefixes (the ListObjects CommonPrefixes rule)
    for (auto& l : page.items)
        if (!hooks.policy || hooks.policy->prefix_may_contain(ns_path(l) + "/"))
            j["namespaces"].push_back(levels_json(l));
    if (page.next_after.empty())
        j["next-page-token"] = nullptr;
    else
        j["next-page-token"] = page_token("namespaces", ctx, page.next_after);
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::create_namespace(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    if (!body.contains("namespace")) throw bad_request("'namespace' is required");
    Levels levels = parse_namespace_json(body["namespace"]);
    auto props = string_map(body.value("properties", json::object()), "properties");
    auto e = co_await catalog_->create_namespace(m.bucket, levels, props);
    Match ctx = m;
    ctx.ns = levels;
    audit(hooks, "create_namespace", ctx, "");
    json j;
    j["namespace"] = levels_json(e.levels);
    j["properties"] = e.properties;
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::load_namespace(http::HttpRequest&, Hooks&, const Match& m) {
    auto e = co_await catalog_->load_namespace(m.bucket, m.ns);
    if (!e) throw not_found_ns("namespace " + ns_display(m.ns) + " does not exist");
    json j;
    j["namespace"] = levels_json(e->levels);
    j["properties"] = e->properties;
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::namespace_exists(http::HttpRequest&, Hooks&, const Match& m) {
    if (!co_await catalog_->namespace_exists(m.bucket, m.ns))
        throw not_found_ns("namespace " + ns_display(m.ns) + " does not exist");
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::drop_namespace(http::HttpRequest&, Hooks& hooks, const Match& m) {
    co_await catalog_->drop_namespace(m.bucket, m.ns);
    audit(hooks, "drop_namespace", m, "");
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::update_namespace_properties(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    std::vector<std::string> removals;
    if (body.contains("removals") && !body["removals"].is_null()) {
        if (!body["removals"].is_array()) throw bad_request("'removals' must be a list");
        for (auto& r : body["removals"]) {
            if (!r.is_string()) throw bad_request("removals must be strings");
            removals.push_back(r.get<std::string>());
        }
    }
    auto updates = string_map(body.value("updates", json::object()), "updates");
    auto res = co_await catalog_->update_namespace_properties(m.bucket, m.ns, removals, updates);
    audit(hooks, "update_namespace_properties", m, "");
    json j;
    j["updated"] = res.updated;
    j["removed"] = res.removed;
    j["missing"] = res.missing;
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::list_tables(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    PageCursor c = page_cursor(req, "tables", m);
    auto page = co_await catalog_->list_tables(m.bucket, m.ns, c);
    json j;
    j["identifiers"] = json::array();
    for (auto& n : page.items) {
        if (hooks.policy && !hooks.policy->allows_key(ns_path(m.ns) + "/" + n)) continue;
        json id;
        id["namespace"] = levels_json(m.ns);
        id["name"] = n;
        j["identifiers"].push_back(id);
    }
    if (page.next_after.empty())
        j["next-page-token"] = nullptr;
    else
        j["next-page-token"] = page_token("tables", m, page.next_after);
    co_return json_response(200, j);
}

json RestApi::load_table_result(std::string_view bucket, const Catalog::LoadedTable& t) const {
    json j;
    j["metadata-location"] = Catalog::to_client_location(bucket, t.entry.metadata_location);
    j["metadata"] = catalog_->client_metadata(bucket, t.metadata);
    json cfg;
    cfg["s3.path-style-access"] = "true";
    cfg["lights3.credential-vending"] = cfg_.credential_vending ? "supported" : "disabled";
    cfg["lights3.credential-scope"] = "table-prefix";
    cfg["lights3.table-location"] = t.entry.location;
    cfg["lights3.version-token"] = t.entry.version_token;
    cfg["lights3.snapshot-validation"] = t.validation;
    cfg["lights3.catalog-etag"] = t.etag;
    j["config"] = cfg;
    return j;
}

// ---------- credential vending (design §8.4) ----------

namespace {

bool wants_vended_credentials(const http::HttpRequest& req) {
    auto h = req.headers.get("X-Iceberg-Access-Delegation");
    if (!h) return false;
    size_t pos = 0;
    while (pos <= h->size()) {
        size_t comma = h->find(',', pos);
        if (comma == std::string::npos) comma = h->size();
        std::string tok = h->substr(pos, comma - pos);
        while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t')) tok.erase(0, 1);
        while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t')) tok.pop_back();
        for (char& c : tok) c = http::HeaderMap::lower(c);
        if (tok == "vended-credentials") return true;
        pos = comma + 1;
    }
    return false;
}

}  // namespace

Task<RestApi::Vending> RestApi::vend(Hooks& hooks, std::string_view bucket, const TableEntry& entry) {
    Vending v;
    v.prefix = entry.location + "/";
    if (!cfg_.credential_vending || !hooks.mint) {
        v.reason = "credential-vending-disabled";
        co_return v;
    }
    // a read-only caller gets a read-only session; a session credential may not mint again
    bool readonly = hooks.policy && !hooks.policy->allows_action(Action::Write);
    s3::CredentialPolicy policy = co_await catalog_->vending_policy(bucket, entry, readonly);
    std::optional<VendedSession> session;
    std::string denied;
    try {
        session = co_await hooks.mint(hooks.access_key, std::move(policy), cfg_.credential_ttl_sec);
    } catch (const s3::S3Error& e) {
        if (e.code != s3::S3ErrorCode::AccessDenied) throw;
        denied = e.message;
    }
    if (!session) {
        v.reason = "credential-vending-not-authorized";
        LOG_INFO("tables: credential vending refused for {} on {}: {}", hooks.access_key, entry.name, denied);
        co_return v;
    }
    v.session = std::move(session);
    co_return v;
}

void RestApi::add_vending(json& result, const Vending& v) {
    json& cfg = result["config"];
    if (!v.session) {
        cfg["lights3.credential-vending"] = "disabled";
        cfg["lights3.credential-vending-reason"] = v.reason;
        cfg["lights3.credential-mode"] = "client-provided-s3-credentials-required";
        return;
    }
    json c;
    c["s3.access-key-id"] = v.session->access_key;
    c["s3.secret-access-key"] = v.session->secret_key;
    c["s3.session-token"] = v.session->token;
    c["expiration-ms"] = std::to_string(v.session->expires_unix * 1000);
    cfg["lights3.credential-vending"] = "supported";
    cfg["lights3.credential-mode"] = "catalog-vended-temporary-credentials";
    for (auto& [k, val] : c.items()) cfg[k] = val;
    json sc;
    sc["prefix"] = v.prefix;
    sc["config"] = c;
    result["storage-credentials"] = json::array({sc});
}

Task<http::HttpResponse> RestApi::load_credentials(http::HttpRequest&, Hooks& hooks, const Match& m) {
    if (!cfg_.credential_vending) throw unsupported("credential vending is disabled on this deployment");
    auto p = co_await catalog_->table_pointer(m.bucket, m.ns, m.table);
    if (!p) throw not_found_table("table " + ns_display(m.ns) + "." + m.table + " does not exist");
    Vending v = co_await vend(hooks, m.bucket, p->value);
    if (!v.session) throw forbidden("credential vending is not authorized for this credential");
    json j;
    j["config"] = json::object();
    add_vending(j, v);
    j.erase("config");
    audit(hooks, "vend_credentials", m, "session " + v.session->access_key);
    auto resp = json_response(200, j);
    resp.headers.set("Cache-Control", "no-store, private");
    co_return resp;
}

Task<http::HttpResponse> RestApi::create_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    if (body.value("stage-create", false)) throw unsupported("stage-create is not supported");
    CreateTableRequest r;
    if (!body.contains("name") || !body["name"].is_string()) throw bad_request("'name' is required");
    r.name = body["name"].get<std::string>();
    if (body.contains("location") && !body["location"].is_null()) {
        if (!body["location"].is_string()) throw bad_request("'location' must be a string");
        r.location = body["location"].get<std::string>();
    }
    if (!body.contains("schema") || !body["schema"].is_object()) throw bad_request("'schema' is required");
    r.schema = body["schema"];
    if (body.contains("partition-spec") && !body["partition-spec"].is_null()) r.partition_spec = body["partition-spec"];
    if (body.contains("write-order") && !body["write-order"].is_null()) r.write_order = body["write-order"];
    r.properties = string_map(body.value("properties", json::object()), "properties");
    CommitHooks ch = hooks.commit;
    auto t = co_await catalog_->create_table(m.bucket, m.ns, r, ch);
    Match ctx = m;
    ctx.table = r.name;
    audit(hooks, "create_table", ctx, "table_id " + t.entry.table_id);
    json j = load_table_result(m.bucket, t);
    auto resp = json_response(200, j);
    resp.headers.set("ETag", "\"" + t.etag + "\"");
    co_return resp;
}

Task<http::HttpResponse> RestApi::register_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    if (body.value("overwrite", false)) throw unsupported("register with overwrite=true is not supported");
    if (!body.contains("name") || !body["name"].is_string()) throw bad_request("'name' is required");
    if (!body.contains("metadata-location") || !body["metadata-location"].is_string())
        throw bad_request("'metadata-location' is required");
    std::string name = body["name"].get<std::string>();
    auto t = co_await catalog_->register_table(m.bucket, m.ns, name, body["metadata-location"].get<std::string>(),
                                               hooks.commit);
    Match ctx = m;
    ctx.table = name;
    audit(hooks, "register_table", ctx, "table_id " + t.entry.table_id);
    auto resp = json_response(200, load_table_result(m.bucket, t));
    resp.headers.set("ETag", "\"" + t.etag + "\"");
    co_return resp;
}

Task<http::HttpResponse> RestApi::load_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    std::string mode = "all";
    if (auto s = req.query_get("snapshots")) {
        if (*s != "all" && *s != "refs") throw bad_request("snapshots must be 'all' or 'refs'");
        mode = *s;
    }
    std::string inm = req.headers.get("If-None-Match").value_or("");
    auto t = co_await catalog_->load_table(m.bucket, m.ns, m.table, inm);
    if (t.not_modified) {
        // step ③ §8: the client's cached copy is current; no metadata was read
        auto resp = empty_response(304);
        resp.headers.set("ETag", "\"" + t.etag + "\"");
        co_return resp;
    }
    if (mode == "refs") {
        std::set<int64_t> keep;
        keep.insert(iceberg::current_snapshot_id(t.metadata));
        if (t.metadata.contains("refs"))
            for (auto& [n, r] : t.metadata["refs"].items()) keep.insert(r.value("snapshot-id", int64_t(-1)));
        json snaps = json::array();
        for (auto& s : t.metadata["snapshots"])
            if (keep.count(s["snapshot-id"].get<int64_t>())) snaps.push_back(s);
        t.metadata["snapshots"] = snaps;
    }
    json result = load_table_result(m.bucket, t);
    bool vended = false;
    if (wants_vended_credentials(req)) {
        Vending v = co_await vend(hooks, m.bucket, t.entry);
        add_vending(result, v);
        vended = v.session.has_value();
        if (vended) audit(hooks, "vend_credentials", m, "session " + v.session->access_key);
    }
    auto resp = json_response(200, result);
    resp.headers.set("ETag", "\"" + t.etag + "\"");
    if (vended) resp.headers.set("Cache-Control", "no-store, private");
    co_return resp;
}

Task<http::HttpResponse> RestApi::table_exists(http::HttpRequest&, Hooks&, const Match& m) {
    if (!co_await catalog_->table_exists(m.bucket, m.ns, m.table))
        throw not_found_table("table " + ns_display(m.ns) + "." + m.table + " does not exist");
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::commit_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    if (body.contains("identifier") && !body["identifier"].is_null()) {
        const json& id = body["identifier"];
        bool same = id.is_object() && id.contains("namespace") && id.contains("name") && id["name"].is_string() &&
                    id["name"].get<std::string>() == m.table && parse_namespace_json(id["namespace"]) == m.ns;
        if (!same) throw bad_request("request identifier must match the resource URL");
    }
    CommitRequest r;
    if (body.contains("commit-id") && body["commit-id"].is_string())
        r.commit_id = body["commit-id"].get<std::string>();
    else if (body.contains("idempotency-key") && body["idempotency-key"].is_string())
        r.commit_id = "ik-" + util::sha256_hex(body["idempotency-key"].get<std::string>()).substr(0, 32);
    if (!r.commit_id.empty()) {
        if (r.commit_id.size() > 128) throw bad_request("commit-id is too long");
        for (char c : r.commit_id)
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'))
                throw bad_request("commit-id may only contain letters, digits, '-' and '_'");
    }
    r.requirements = body.value("requirements", json::array());
    r.updates = body.value("updates", json::array());
    if (!r.requirements.is_array() || !r.updates.is_array())
        throw bad_request("'requirements' and 'updates' must be lists");
    if (r.requirements.size() > 1024 || r.updates.size() > 1024)
        throw bad_request("at most 1024 requirements and 1024 updates per commit");
    auto t = co_await catalog_->commit_table(m.bucket, m.ns, m.table, r, hooks.commit);
    audit(hooks, "commit_table", m,
          "generation " + std::to_string(t.entry.generation) + (r.commit_id.empty() ? "" : " commit " + r.commit_id));
    json j;
    j["metadata-location"] = Catalog::to_client_location(m.bucket, t.entry.metadata_location);
    j["metadata"] = catalog_->client_metadata(m.bucket, t.metadata);
    j["version-token"] = t.entry.version_token;
    j["generation"] = t.entry.generation;
    json cfg;
    cfg["lights3.snapshot-validation"] = t.validation;
    cfg["lights3.catalog-etag"] = t.etag;
    j["config"] = cfg;
    auto resp = json_response(200, j);
    resp.headers.set("ETag", "\"" + t.etag + "\"");
    co_return resp;
}

Task<http::HttpResponse> RestApi::drop_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    bool purge = false;
    if (auto p = req.query_get("purgeRequested")) {
        if (*p == "true")
            purge = true;
        else if (*p != "false")
            throw bad_request("purgeRequested must be true or false");
    }
    if (purge && !jobs_) throw unsupported("purgeRequested=true needs the maintenance job framework");
    co_await catalog_->drop_table(m.bucket, m.ns, m.table);
    audit(hooks, "drop_table", m, purge ? "purge requested" : "");
    auto resp = empty_response(204);
    if (purge) {
        // the tombstone is written; the data goes in a job (design §5.7 / step ④ §5)
        json j = start_purge(m.bucket, m.ns, m.table, hooks.commit);
        resp.headers.set("x-lights3-job-id", std::to_string(j.value("job_id", uint64_t(0))));
        audit(hooks, "purge_table", m, "job " + std::to_string(j.value("job_id", uint64_t(0))));
    }
    co_return resp;
}

Task<http::HttpResponse> RestApi::rename_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    auto ident = [&](const char* which) {
        if (!body.contains(which) || !body[which].is_object() || !body[which].contains("namespace") ||
            !body[which].contains("name") || !body[which]["name"].is_string())
            throw bad_request(std::string("'") + which + "' must be {namespace, name}");
        return std::make_pair(parse_namespace_json(body[which]["namespace"]), body[which]["name"].get<std::string>());
    };
    auto [src_ns, src_name] = ident("source");
    auto [dst_ns, dst_name] = ident("destination");
    // policy: delete on the source, write on the destination (design §6.2)
    if (hooks.policy) {
        if (!allows_table(*hooks.policy, m.bucket, src_ns, src_name, Action::Delete) ||
            !allows_table(*hooks.policy, m.bucket, dst_ns, dst_name, Action::Write))
            throw forbidden("Access denied by credential policy.");
    }
    co_await catalog_->rename_table(m.bucket, src_ns, src_name, dst_ns, dst_name);
    Match ctx = m;
    ctx.ns = src_ns;
    ctx.table = src_name;
    audit(hooks, "rename_table", ctx, "to " + ns_display(dst_ns) + "." + dst_name);
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::get_metadata_location(http::HttpRequest&, Hooks&, const Match& m) {
    auto p = co_await catalog_->table_pointer(m.bucket, m.ns, m.table);
    if (!p) throw not_found_table("table " + ns_display(m.ns) + "." + m.table + " does not exist");
    json j;
    j["metadataLocation"] = Catalog::to_client_location(m.bucket, p->value.metadata_location);
    j["versionToken"] = p->value.version_token;
    j["generation"] = p->value.generation;
    j["warehouseLocation"] = p->value.location;
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::put_metadata_location(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    auto field = [&](const char* camel, const char* kebab) -> std::string {
        if (body.contains(camel) && body[camel].is_string()) return body[camel].get<std::string>();
        if (body.contains(kebab) && body[kebab].is_string()) return body[kebab].get<std::string>();
        throw bad_request(std::string("'") + camel + "' is required");
    };
    std::string loc = field("metadataLocation", "metadata-location");
    std::string token = field("versionToken", "version-token");
    auto e = co_await catalog_->update_metadata_location(m.bucket, m.ns, m.table, loc, token);
    audit(hooks, "update_metadata_location", m, "generation " + std::to_string(e.generation));
    json j;
    j["metadataLocation"] = Catalog::to_client_location(m.bucket, e.metadata_location);
    j["versionToken"] = e.version_token;
    j["generation"] = e.generation;
    j["warehouseLocation"] = e.location;
    co_return json_response(200, j);
}

// reportMetrics (step ⑥ §3): the engine's scan / commit report becomes an audit event
// tables.metrics with a compact detail (filter, projection, the counters); reports over
// 64 KiB are accepted but not recorded
Task<http::HttpResponse> RestApi::report_metrics(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    std::string text;
    if (req.body) {
        std::byte buf[16 * 1024];
        for (;;) {
            size_t n = co_await req.body->read(std::span(buf));
            if (n == 0) break;
            if (text.size() + n > cfg_.request_max_size) throw bad_request("request body exceeds the size limit");
            text.append(reinterpret_cast<const char*>(buf), n);
        }
    }
    if (text.empty() || text.size() > 64 * 1024) co_return empty_response(204);
    json report = json::parse(text, nullptr, false);
    if (!report.is_object() || !report.contains("report-type")) co_return empty_response(204);
    json detail;
    detail["report-type"] = report.value("report-type", "");
    detail["table-name"] = report.value("table-name", "");
    if (report.contains("snapshot-id")) detail["snapshot-id"] = report["snapshot-id"];
    if (report.contains("sequence-number")) detail["sequence-number"] = report["sequence-number"];
    if (report.contains("operation")) detail["operation"] = report["operation"];
    if (report.contains("filter")) detail["filter"] = report["filter"];
    if (report.contains("projected-field-names")) detail["projected-field-names"] = report["projected-field-names"];
    if (report.contains("schema-id")) detail["schema-id"] = report["schema-id"];
    json metrics = json::object();
    if (report.contains("metrics") && report["metrics"].is_object()) {
        for (auto& [k, v] : report["metrics"].items()) {
            // counters {"unit","value"}, timers {"count","time-unit","total-duration"}
            if (v.is_object() && v.contains("value"))
                metrics[k] = v["value"];
            else if (v.is_object() && v.contains("total-duration"))
                metrics[k] = json{{"count", v.value("count", 0)},
                                  {"total-duration", v["total-duration"]},
                                  {"time-unit", v.value("time-unit", "")}};
            else if (v.is_number())
                metrics[k] = v;
        }
    }
    detail["metrics"] = metrics;
    if (report.contains("metadata") && report["metadata"].is_object()) detail["metadata"] = report["metadata"];
    audit(hooks, "metrics", m, detail.dump());
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::diagnose_table(http::HttpRequest&, Hooks&, const Match& m) {
    auto d = co_await catalog_->diagnose(m.bucket, m.ns, m.table);
    co_return json_response(200, d.to_json(m.bucket));
}

Task<http::HttpResponse> RestApi::recover_table(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, true);
    bool prune = false;
    if (body.contains("prune") && !body["prune"].is_null()) {
        if (!body["prune"].is_boolean()) throw bad_request("'prune' must be a boolean");
        prune = body["prune"].get<bool>();
    }
    auto rep = co_await catalog_->recover(m.bucket, m.ns, m.table, prune);
    audit(hooks, "recovery", m,
          "finalized " + std::to_string(rep.finalized) + " pruned " + std::to_string(rep.pruned) + " manual " +
              std::to_string(rep.manual));
    co_return json_response(200, rep.to_json());
}

// ---------- views (step ⑥ §1) ----------

json RestApi::load_view_result(std::string_view bucket, const Catalog::LoadedView& v) const {
    json j;
    j["metadata-location"] = Catalog::to_client_location(bucket, v.entry.metadata_location);
    j["metadata"] = v.metadata;
    json cfg;
    cfg["lights3.version-token"] = v.entry.version_token;
    cfg["lights3.catalog-etag"] = v.etag;
    j["config"] = cfg;
    return j;
}

Task<http::HttpResponse> RestApi::list_views(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    PageCursor c = page_cursor(req, "views", m);
    auto page = co_await catalog_->list_views(m.bucket, m.ns, c);
    json j;
    j["identifiers"] = json::array();
    for (auto& n : page.items) {
        if (hooks.policy && !hooks.policy->allows_key(ns_path(m.ns) + "/" + n)) continue;
        json id;
        id["namespace"] = levels_json(m.ns);
        id["name"] = n;
        j["identifiers"].push_back(id);
    }
    if (page.next_after.empty())
        j["next-page-token"] = nullptr;
    else
        j["next-page-token"] = page_token("views", m, page.next_after);
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::create_view(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    CreateViewRequest r;
    if (!body.contains("name") || !body["name"].is_string()) throw bad_request("'name' is required");
    r.name = body["name"].get<std::string>();
    if (body.contains("location") && !body["location"].is_null()) {
        if (!body["location"].is_string()) throw bad_request("'location' must be a string");
        r.location = body["location"].get<std::string>();
    }
    if (!body.contains("schema") || !body["schema"].is_object()) throw bad_request("'schema' is required");
    r.schema = body["schema"];
    if (!body.contains("view-version") || !body["view-version"].is_object())
        throw bad_request("'view-version' is required");
    r.view_version = body["view-version"];
    r.properties = string_map(body.value("properties", json::object()), "properties");
    auto v = co_await catalog_->create_view(m.bucket, m.ns, r);
    Match ctx = m;
    ctx.table = r.name;
    audit(hooks, "create_view", ctx, "view_id " + v.entry.view_id);
    auto resp = json_response(200, load_view_result(m.bucket, v));
    resp.headers.set("ETag", "\"" + v.etag + "\"");
    co_return resp;
}

Task<http::HttpResponse> RestApi::load_view(http::HttpRequest&, Hooks&, const Match& m) {
    auto v = co_await catalog_->load_view(m.bucket, m.ns, m.table);
    auto resp = json_response(200, load_view_result(m.bucket, v));
    resp.headers.set("ETag", "\"" + v.etag + "\"");
    co_return resp;
}

Task<http::HttpResponse> RestApi::view_exists(http::HttpRequest&, Hooks&, const Match& m) {
    if (!co_await catalog_->view_exists(m.bucket, m.ns, m.table))
        throw not_found_view("view " + ns_display(m.ns) + "." + m.table + " does not exist");
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::replace_view(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    if (body.contains("identifier") && !body["identifier"].is_null()) {
        const json& id = body["identifier"];
        bool same = id.is_object() && id.contains("namespace") && id.contains("name") && id["name"].is_string() &&
                    id["name"].get<std::string>() == m.table && parse_namespace_json(id["namespace"]) == m.ns;
        if (!same) throw bad_request("request identifier must match the resource URL");
    }
    json requirements = body.value("requirements", json::array());
    json updates = body.value("updates", json::array());
    if (!requirements.is_array() || !updates.is_array())
        throw bad_request("'requirements' and 'updates' must be lists");
    if (requirements.size() > 1024 || updates.size() > 1024)
        throw bad_request("at most 1024 requirements and 1024 updates per commit");
    auto v = co_await catalog_->replace_view(m.bucket, m.ns, m.table, requirements, updates);
    audit(hooks, "replace_view", m, "generation " + std::to_string(v.entry.generation));
    auto resp = json_response(200, load_view_result(m.bucket, v));
    resp.headers.set("ETag", "\"" + v.etag + "\"");
    co_return resp;
}

Task<http::HttpResponse> RestApi::drop_view(http::HttpRequest&, Hooks& hooks, const Match& m) {
    co_await catalog_->drop_view(m.bucket, m.ns, m.table);
    audit(hooks, "drop_view", m, "");
    co_return empty_response(204);
}

Task<http::HttpResponse> RestApi::rename_view(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    auto ident = [&](const char* which) {
        if (!body.contains(which) || !body[which].is_object() || !body[which].contains("namespace") ||
            !body[which].contains("name") || !body[which]["name"].is_string())
            throw bad_request(std::string("'") + which + "' must be {namespace, name}");
        return std::make_pair(parse_namespace_json(body[which]["namespace"]), body[which]["name"].get<std::string>());
    };
    auto [src_ns, src_name] = ident("source");
    auto [dst_ns, dst_name] = ident("destination");
    if (hooks.policy) {
        if (!allows_table(*hooks.policy, m.bucket, src_ns, src_name, Action::Delete) ||
            !allows_table(*hooks.policy, m.bucket, dst_ns, dst_name, Action::Write))
            throw forbidden("Access denied by credential policy.");
    }
    co_await catalog_->rename_view(m.bucket, src_ns, src_name, dst_ns, dst_name);
    Match ctx = m;
    ctx.ns = src_ns;
    ctx.table = src_name;
    audit(hooks, "rename_view", ctx, "to " + ns_display(dst_ns) + "." + dst_name);
    co_return empty_response(204);
}

// ---------- maintenance (design §9, step ④) ----------

Task<EffectiveMaintenance> RestApi::effective_maintenance(std::string_view bucket, const Levels& levels,
                                                          std::string_view table) {
    auto t = co_await catalog_->load_table(bucket, levels, table);
    auto table_cfg = co_await catalog_->store()->get_maintenance_config(bucket, levels, table);
    co_return resolve_maintenance(cfg_, table_cfg, t.metadata.value("properties", json::object()));
}

Task<http::HttpResponse> RestApi::get_maintenance_config(http::HttpRequest&, Hooks&, const Match& m) {
    EffectiveMaintenance eff = co_await effective_maintenance(m.bucket, m.ns, m.table);
    auto table_cfg = co_await catalog_->store()->get_maintenance_config(m.bucket, m.ns, m.table);
    json j;
    j["effective"] = eff.to_json();
    j["table-config"] = table_cfg ? tables::to_json(*table_cfg) : json();
    j["defaults"]["retain_recent_metadata_files"] = cfg_.maintenance.retain_recent_metadata_files;
    j["defaults"]["delete_enabled"] = cfg_.maintenance.delete_enabled;
    j["defaults"]["safety_window_sec"] = cfg_.maintenance.safety_window_sec;
    co_return json_response(200, j);
}

Task<http::HttpResponse> RestApi::put_maintenance_config(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, false);
    if (!co_await catalog_->table_exists(m.bucket, m.ns, m.table))
        throw not_found_table("table " + ns_display(m.ns) + "." + m.table + " does not exist");
    MaintenanceConfig c;
    for (auto& [k, v] : body.items()) {
        if (v.is_null()) continue;
        if (k == "retain_recent_metadata_files") {
            if (!v.is_number_integer() || v.get<int64_t>() < 0 || v.get<int64_t>() > 100000)
                throw bad_request("retain_recent_metadata_files must be an integer in [0, 100000]");
            c.retain_recent_metadata_files = v.get<int>();
        } else if (k == "delete_enabled") {
            if (!v.is_boolean()) throw bad_request("delete_enabled must be a boolean");
            c.delete_enabled = v.get<bool>();
        } else if (k == "max_snapshot_age_ms") {
            if (!v.is_number_integer() || v.get<int64_t>() <= 0)
                throw bad_request("max_snapshot_age_ms must be a positive integer");
            c.max_snapshot_age_ms = v.get<int64_t>();
        } else if (k == "min_snapshots_to_keep") {
            if (!v.is_number_integer() || v.get<int64_t>() < 1 || v.get<int64_t>() > 100000)
                throw bad_request("min_snapshots_to_keep must be an integer in [1, 100000]");
            c.min_snapshots_to_keep = v.get<int>();
        } else if (k == "orphan_cleanup") {
            if (!v.is_boolean()) throw bad_request("orphan_cleanup must be a boolean");
            c.orphan_cleanup = v.get<bool>();
        } else if (k != "version") {
            throw bad_request("unknown maintenance setting '" + k + "'");
        }
    }
    co_await catalog_->store()->put_maintenance_config(m.bucket, m.ns, m.table, c);
    audit(hooks, "put_maintenance_config", m, tables::to_json(c).dump());
    EffectiveMaintenance eff = co_await effective_maintenance(m.bucket, m.ns, m.table);
    json j;
    j["effective"] = eff.to_json();
    j["table-config"] = tables::to_json(c);
    co_return json_response(200, j);
}

json RestApi::start_plan(std::string_view bucket, const Levels& levels, std::string_view table) {
    if (!jobs_) throw unsupported("maintenance jobs are not available on this deployment");
    auto catalog = catalog_;
    TablesConfig cfg = cfg_;
    std::string b(bucket), t(table);
    Levels l = levels;
    JobHooks::Fn fn = [catalog, cfg, b, l, t]() -> json {
        auto task = [&]() -> Task<json> {
            auto loaded = co_await catalog->load_table(b, l, t);
            auto table_cfg = co_await catalog->store()->get_maintenance_config(b, l, t);
            EffectiveMaintenance eff = resolve_maintenance(cfg, table_cfg,
                                                           loaded.metadata.value("properties", json::object()));
            MaintenancePlan plan = co_await plan_table(*catalog, b, l, t, eff.planner, now_unix());
            if (eff.conflict) {
                plan.manual_review = true;
                for (auto& n : eff.notes) plan.notes.push_back(n);
            }
            json out = plan.to_json();
            out["effective"] = eff.to_json();
            co_return out;
        };
        return sync_wait(task());
    };
    return jobs_.start(job_resource(bucket, levels, table), "plan", std::move(fn));
}

Task<json> RestApi::start_run(std::string_view bucket, const Levels& levels, std::string_view table, const json& body,
                              CommitHooks commit) {
    if (!jobs_) throw unsupported("maintenance jobs are not available on this deployment");
    std::string resource = job_resource(bucket, levels, table);
    // the plan: inline, by plan job id, or the table's most recent plan job
    json plan_json;
    if (body.contains("plan") && !body["plan"].is_null()) {
        plan_json = body["plan"];
    } else {
        std::optional<json> st;
        if (body.contains("job_id") && !body["job_id"].is_null()) {
            if (!body["job_id"].is_number_unsigned()) throw bad_request("'job_id' must be a job id");
            st = jobs_.status_by_id(body["job_id"].get<uint64_t>());
            if (!st || st->value("backend", "") != resource || st->value("op", "") != "plan")
                throw bad_request("'job_id' is not a plan job of this table");
        } else {
            st = jobs_.status(resource, "plan");
        }
        if (st->value("running", false)) throw bad_request("the plan job is still running");
        if (!st->contains("stats")) throw bad_request("no completed plan for this table; run plan first");
        plan_json = (*st)["stats"];
    }
    auto plan = MaintenancePlan::from_json(plan_json);
    if (!plan) throw bad_request("'plan' is not a maintenance plan document");
    if (plan->bucket != bucket || plan->levels != levels || plan->name != table)
        throw bad_request("the plan belongs to another table");
    if (plan->manual_review) throw bad_request("the plan is marked for manual review; it cannot be run");
    EffectiveMaintenance eff = co_await effective_maintenance(bucket, levels, table);
    RunOptions ro;
    ro.delete_enabled = eff.delete_enabled;
    ro.safety_window_sec = eff.planner.safety_window_sec;
    ro.note_usage = commit.note_usage;
    auto catalog = catalog_;
    MaintenancePlan p = *plan;
    JobHooks::Fn fn = [catalog, p, ro]() -> json {
        RunReport rep = sync_wait(run_table(*catalog, p, ro, now_unix()));
        return rep.to_json();
    };
    co_return jobs_.start(resource, "run", std::move(fn));
}

json RestApi::start_purge(std::string_view bucket, const Levels& levels, std::string_view table, CommitHooks commit) {
    if (!jobs_) throw unsupported("maintenance jobs are not available on this deployment");
    auto catalog = catalog_;
    std::string b(bucket), t(table);
    Levels l = levels;
    RunOptions ro;
    ro.note_usage = commit.note_usage;
    JobHooks::Fn fn = [catalog, b, l, t, ro]() -> json {
        PurgeReport rep = sync_wait(purge_table(*catalog, b, l, t, ro));
        return rep.to_json();
    };
    return jobs_.start(job_resource(bucket, levels, table), "purge", std::move(fn));
}

Task<http::HttpResponse> RestApi::plan_maintenance(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    co_await read_json(req, true);
    if (!co_await catalog_->table_exists(m.bucket, m.ns, m.table))
        throw not_found_table("table " + ns_display(m.ns) + "." + m.table + " does not exist");
    json j = start_plan(m.bucket, m.ns, m.table);
    audit(hooks, "maintenance_plan", m, "job " + std::to_string(j.value("job_id", uint64_t(0))));
    co_return json_response(202, j);
}

Task<http::HttpResponse> RestApi::run_maintenance(http::HttpRequest& req, Hooks& hooks, const Match& m) {
    json body = co_await read_json(req, true);
    if (!co_await catalog_->table_exists(m.bucket, m.ns, m.table))
        throw not_found_table("table " + ns_display(m.ns) + "." + m.table + " does not exist");
    json j = co_await start_run(m.bucket, m.ns, m.table, body, hooks.commit);
    audit(hooks, "maintenance_run", m, "job " + std::to_string(j.value("job_id", uint64_t(0))));
    co_return json_response(202, j);
}

Task<http::HttpResponse> RestApi::maintenance_job(http::HttpRequest&, Hooks&, const Match& m) {
    if (!jobs_) throw unsupported("maintenance jobs are not available on this deployment");
    uint64_t id = 0;
    try {
        size_t pos = 0;
        id = std::stoull(m.extra, &pos);
        if (pos != m.extra.size()) id = 0;
    } catch (const std::exception&) {
        id = 0;
    }
    if (id == 0) throw bad_request("job id must be a positive integer");
    auto st = jobs_.status_by_id(id);
    if (!st || st->value("backend", "") != job_resource(m.bucket, m.ns, m.table))
        throw not_found_resource("no job " + m.extra + " on this table");
    co_return json_response(200, *st);
}

Task<json> RestApi::admin_job(std::string_view method, std::string_view bucket, const Levels& levels,
                              std::string_view table, std::string_view op, const json& body) {
    using s3::S3Error;
    using s3::S3ErrorCode;
    if (op != "plan" && op != "run" && op != "purge")
        throw S3Error(S3ErrorCode::InvalidRequest, "no operation '" + std::string(op) + "' in 'tables'.");
    if (!jobs_) throw S3Error(S3ErrorCode::InvalidRequest, "Maintenance jobs are not available on this deployment.");
    require_namespace(levels);
    require_segment("table name", table);
    std::string resource = job_resource(bucket, levels, table);
    if (method == "GET") co_return jobs_.status(resource, std::string(op));
    if (method != "POST")
        throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed against this resource.");
    std::optional<RestError> failure;
    json out;
    try {
        if (op == "plan") {
            if (!co_await catalog_->table_exists(bucket, levels, table))
                throw not_found_table("table " + ns_display(levels) + "." + std::string(table) + " does not exist");
            out = start_plan(bucket, levels, table);
        } else if (op == "run") {
            if (!co_await catalog_->table_exists(bucket, levels, table))
                throw not_found_table("table " + ns_display(levels) + "." + std::string(table) + " does not exist");
            out = co_await start_run(bucket, levels, table, body, CommitHooks{});
        } else {
            out = start_purge(bucket, levels, table, CommitHooks{});
        }
    } catch (const RestError& e) {
        failure = e;
    }
    if (failure) {
        // the admin plane speaks S3 errors
        S3ErrorCode code = failure->status == 404 ? S3ErrorCode::NoSuchKey : S3ErrorCode::InvalidRequest;
        throw S3Error(code, failure->message);
    }
    co_return out;
}

}  // namespace lights3::tables
