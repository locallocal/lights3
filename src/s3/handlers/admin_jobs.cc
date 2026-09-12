// L2: the background rounds on demand and the quarantine ledgers of a live
// gateway (docs/cli.md §3.12, `lights3-ctl duostore|tier ...`), the same job
// model as admin_fsck.cc (one job per backend, POST starts, GET polls):
//   POST/GET /-/admin/duostore/<backend>/gc|scan
//   POST/GET /-/admin/tier/<backend>/scan|gc|reconcile
//   GET      /-/admin/duostore|tier/<backend>/quarantine
// Root only; the application supplies the job manager through hooks (the
// service never sees backend types -- an op the group does not have, or a
// backend of another type, comes back from the hooks as InvalidRequest). POST
// answers 202 with the job id, 409 JobInProgress while any job runs on that
// backend; GET reports running / last outcome, or the ledger document
#include "core/log.h"
#include "s3/handlers/admin_json.h"
#include "s3/service.h"
#ifdef LIGHTS3_TABLES
#include "tables/rest_api.h"
#include "tables/rest_error.h"
#endif

namespace lights3::s3 {

using namespace handlers;
using nlohmann::json;

Task<http::HttpResponse> S3Service::admin_jobs(http::HttpRequest& req, std::string& access_key,
                                               const RequestContext& ctx) {
    try {
        auto ident = verify_identity(req);
        access_key = ident.access_key;
        if (!is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Running maintenance jobs requires a root (statically configured) "
                          "credential.");
        // /-/admin/<group>/<backend>/<op>: the dispatcher guarantees the group
        constexpr std::string_view kBase = "/-/admin/";
        std::string rest = req.path.substr(kBase.size());
        std::string group = rest.substr(0, rest.find('/'));
        std::string usage = "Usage: /-/admin/" + group + "/<backend>/<op>.";
        std::string tail = rest.substr(group.size() + 1);
        auto slash = tail.find('/');
        if (slash == std::string::npos || slash == 0 || slash + 1 == tail.size() ||
            tail.find('/', slash + 1) != std::string::npos)
            throw S3Error(S3ErrorCode::InvalidRequest, usage);
        std::string backend = tail.substr(0, slash);
        std::string op = tail.substr(slash + 1);
        if (!job_start_ || !job_status_ || !job_ledger_)
            throw S3Error(S3ErrorCode::InvalidRequest, "Maintenance jobs are not available on this deployment.");
        if (op == "quarantine") {
            if (req.method != "GET")
                throw S3Error(S3ErrorCode::MethodNotAllowed,
                              "The specified method is not allowed against this resource.");
            co_return json_response(200, job_ledger_(backend, group));
        }
        if (req.method == "GET") {
            co_return json_response(200, job_status_(backend, group, op));
        }
        if (req.method != "POST")
            throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed against this resource.");
        json j = job_start_(backend, group, op, 0);
        AuditEvent e;
        e.event = group + "." + op + ".start";
        e.actor = access_key;
        e.request_id = ctx.request_id;
        e.detail = "backend " + backend + " job " + j.value("job_id", json(0)).dump();
        audit(e);
        co_return json_response(202, j);
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        co_return admin_error(e, req);
    } catch (const std::exception& e) {
        LOG_ERROR("admin api {} {} internal error: {}", req.method, req.path, e.what());
        metrics_.s3_error(S3ErrorCode::InternalError);
        co_return admin_error(S3Error(S3ErrorCode::InternalError, e.what()), req);
    }
}

#ifdef LIGHTS3_TABLES
// /-/admin/tables/<bucket>/<ns-path>/<t>/<op>: the catalog's job entry (RestApi::admin_job)
// behind the same root gate and error rendering as the backend jobs
Task<http::HttpResponse> S3Service::admin_tables_jobs(http::HttpRequest& req, std::string& access_key,
                                                      const RequestContext& ctx) {
    try {
        auto ident = verify_identity(req);
        access_key = ident.access_key;
        if (!is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Running maintenance jobs requires a root (statically configured) "
                          "credential.");
        constexpr std::string_view kBase = "/-/admin/tables/";
        std::string rest = req.path.substr(kBase.size());
        std::vector<std::string> segs;
        size_t pos = 0;
        while (pos <= rest.size()) {
            size_t next = rest.find('/', pos);
            if (next == std::string::npos) next = rest.size();
            if (next > pos) segs.push_back(rest.substr(pos, next - pos));
            pos = next + 1;
        }
        if (segs.size() < 4)
            throw S3Error(S3ErrorCode::InvalidRequest, "Usage: /-/admin/tables/<bucket>/<namespace...>/<table>/<op>.");
        std::string bucket = segs.front();
        std::string op = segs.back();
        std::string table = segs[segs.size() - 2];
        tables::Levels levels(segs.begin() + 1, segs.end() - 2);
        nlohmann::json body = nlohmann::json::object();
        if (req.method == "POST" && req.body) {
            std::string text;
            std::byte buf[16 * 1024];
            for (;;) {
                size_t n = co_await req.body->read(std::span(buf));
                if (n == 0) break;
                if (text.size() + n > 1024 * 1024) throw S3Error(S3ErrorCode::EntityTooLarge, "Body too large.");
                text.append(reinterpret_cast<const char*>(buf), n);
            }
            if (!text.empty()) {
                body = nlohmann::json::parse(text, nullptr, false);
                if (!body.is_object()) throw S3Error(S3ErrorCode::InvalidRequest, "The body must be a JSON object.");
            }
        }
        json j;
        try {
            j = co_await tables_api_->admin_job(req.method, bucket, levels, table, op, body);
        } catch (const tables::RestError& e) {
            throw S3Error(e.status == 404 ? S3ErrorCode::NoSuchKey : S3ErrorCode::InvalidRequest, e.message);
        }
        if (req.method == "GET") co_return json_response(200, j);
        AuditEvent e;
        e.event = "tables." + op + ".start";
        e.actor = access_key;
        e.request_id = ctx.request_id;
        e.bucket = bucket;
        e.key = tables::ns_path(levels) + "/" + table;
        e.detail = "job " + j.value("job_id", json(0)).dump();
        audit(e);
        co_return json_response(202, j);
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        co_return admin_error(e, req);
    } catch (const std::exception& e) {
        LOG_ERROR("admin api {} {} internal error: {}", req.method, req.path, e.what());
        metrics_.s3_error(S3ErrorCode::InternalError);
        co_return admin_error(S3Error(S3ErrorCode::InternalError, e.what()), req);
    }
}
#endif

}  // namespace lights3::s3
