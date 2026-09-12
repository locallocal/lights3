// Iceberg REST catalog surface (docs/s3-tables-design.md §6, docs/s3-tables/step-1-catalog-core.md
// §10): path matching, JSON codec, authorization mapping onto (bucket, key, Action) and
// the error envelope. Independent of s3/service.h -- dispatch injects what it needs
// through Hooks so the dependency runs service → tables only
#pragma once

#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/config.h"
#include "core/metrics.h"
#include "core/task.h"
#include "http/model.h"
#include "s3/audit.h"
#include "s3/auth/policy.h"
#include "s3/auth/sigv4.h"
#include "tables/catalog.h"
#include "tables/jobs.h"
#include "tables/maintenance.h"

namespace lights3::tables {

class RestApi {
public:
    // A session minted for credential vending (design §8.4); mirrors CredentialStore's
    // SessionCredential without depending on its header
    struct VendedSession {
        std::string access_key;
        std::string secret_key;
        std::string token;
        int64_t expires_unix = 0;
    };
    struct Hooks {
        std::function<s3::VerifiedIdentity(http::HttpRequest&)> verify;
        std::function<bool(std::string_view ak)> is_root;
        std::function<void(const s3::AuditEvent&)> audit;
        // tenant ownership gate (design §6.2); empty = no tenancy
        std::function<Task<void>(std::string_view bucket, std::string_view tenant)> tenant_gate;
        // credential vending (design §8.4): mint a session for `caller` narrowed to `policy`;
        // empty = vending unavailable on this deployment
        std::function<Task<VendedSession>(std::string_view caller, s3::CredentialPolicy policy, int ttl_sec)> mint;
        // quota / usage (may be empty)
        CommitHooks commit;
        std::string region;
        std::string_view request_id;
        // set by dispatch after verification
        std::string access_key;
        std::string tenant;
        std::optional<s3::CredentialPolicy> policy;
    };

    RestApi(std::shared_ptr<Catalog> catalog, TablesConfig cfg, MetricsScope metrics);

    // The job framework for maintenance (step ④ §5); without it plan / run / purge
    // answer 406. Set once by the application, not per request
    void set_job_hooks(JobHooks hooks) { jobs_ = std::move(hooks); }
    const JobHooks& job_hooks() const { return jobs_; }
    // The admin-plane entry (POST/GET /-/admin/tables/<bucket>/<ns-path>/<t>/<op>, root
    // only -- the service verified that): start (POST) or query (GET) the job; `body` is
    // the request's JSON (run takes {"plan"} / {"job_id"}). Throws s3::S3Error
    Task<nlohmann::json> admin_job(std::string_view method, std::string_view bucket, const Levels& levels,
                                   std::string_view table, std::string_view op, const nlohmann::json& body);

    // True when the path belongs to the catalog: "<prefix>/v1" or "<prefix>/v1/..."
    bool matches(std::string_view path) const;
    // dispatch entry: never throws; access_key / api_name are out-params for the access log
    Task<http::HttpResponse> dispatch(http::HttpRequest& req, Hooks& hooks, std::string& access_key,
                                      std::string& api_name);

    // The standard endpoints advertised by GET /v1/config (same source as the route table)
    static std::vector<std::string> advertised_endpoints();
    const std::string& prefix() const { return cfg_.path_prefix; }
    const TablesConfig& config() const { return cfg_; }

    enum class KeyKind { None, Namespace, Table };
    struct Match;
    using Handler = Task<http::HttpResponse> (RestApi::*)(http::HttpRequest&, Hooks&, const Match&);
    struct Route {
        std::string_view method;
        // e.g. "namespaces/{ns}/tables/{t}" (after "<prefix>/v1/{w}/")
        std::string_view pattern;
        s3::Action action;
        KeyKind key_kind;
        bool root_only;
        // Iceberg operation name for the access log / audit
        std::string_view name;
        // advertised in GET /config
        bool standard;
        Handler fn;
    };
    struct Match {
        const Route* route = nullptr;
        std::string bucket;
        Levels ns;
        std::string table;
        // second capture ({w2} of buckets/{w2})
        std::string extra;
    };
    static std::span<const Route> routes();
    static bool allows_table(const s3::CredentialPolicy& p, std::string_view bucket, const Levels& ns,
                             std::string_view table, s3::Action action);

    // handlers (public so the route table can name them; not part of the API proper)
    Task<http::HttpResponse> get_config(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> enable_bucket(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> get_bucket(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> disable_bucket(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> list_namespaces(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> create_namespace(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> load_namespace(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> namespace_exists(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> drop_namespace(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> update_namespace_properties(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> list_tables(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> create_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> register_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> load_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> table_exists(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> commit_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> drop_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> rename_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> get_metadata_location(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> put_metadata_location(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> report_metrics(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> load_credentials(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> diagnose_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> recover_table(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> get_maintenance_config(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> put_maintenance_config(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> plan_maintenance(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> run_maintenance(http::HttpRequest&, Hooks&, const Match&);
    Task<http::HttpResponse> maintenance_job(http::HttpRequest&, Hooks&, const Match&);

private:
    // path after "<prefix>/v1/" split on '/', percent-decoded per segment
    static std::vector<std::string> split_path(std::string_view raw_path, size_t skip);
    bool match_route(const Route& r, const std::vector<std::string>& segs, Match& m) const;
    Task<nlohmann::json> read_json(http::HttpRequest& req, bool allow_empty) const;
    static http::HttpResponse json_response(int status, const nlohmann::json& j);
    static http::HttpResponse empty_response(int status);
    PageCursor page_cursor(const http::HttpRequest& req, std::string_view op, const Match& m) const;
    std::string page_token(std::string_view op, const Match& m, std::string_view after) const;
    nlohmann::json load_table_result(std::string_view bucket, const Catalog::LoadedTable& t) const;
    // Credential vending outcome for one LoadTable / LoadCredentials (design §8.4)
    struct Vending {
        // nullopt = not vended; `reason` explains why
        std::optional<VendedSession> session;
        std::string reason;
        std::string prefix;
    };
    Task<Vending> vend(Hooks& hooks, std::string_view bucket, const TableEntry& entry);
    static void add_vending(nlohmann::json& result, const Vending& v);
    void audit(Hooks& hooks, std::string_view op, const Match& m, std::string detail) const;
    // the effective settings of a table (properties > table object > tables.maintenance)
    Task<EffectiveMaintenance> effective_maintenance(std::string_view bucket, const Levels& levels,
                                                     std::string_view table);
    // maintenance jobs (the JSON status documents); throw S3Error(JobInProgress) when busy
    nlohmann::json start_plan(std::string_view bucket, const Levels& levels, std::string_view table);
    Task<nlohmann::json> start_run(std::string_view bucket, const Levels& levels, std::string_view table,
                                   const nlohmann::json& body, CommitHooks commit);
    nlohmann::json start_purge(std::string_view bucket, const Levels& levels, std::string_view table,
                               CommitHooks commit);

    std::shared_ptr<Catalog> catalog_;
    TablesConfig cfg_;
    MetricsScope metrics_;
    std::shared_ptr<MetricCounter> requests_;
    JobHooks jobs_;
};

}  // namespace lights3::tables
