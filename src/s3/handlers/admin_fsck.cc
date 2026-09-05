// L2: POST/GET /-/admin/fsck/<backend> (backlog-sequence ③, `s3adm fsck --offline`):
// the offline integrity scrub (roadmap §3.1) triggered on a live gateway and
// polled for its outcome. Root only; the application supplies the job manager
// through hooks (the service never sees backend types). POST starts one round
// in the background and answers 202 with the job id -- 409 while a round runs
// on that backend; GET reports running/last outcome
#include "core/log.h"
#include "s3/handlers/admin_json.h"
#include "s3/service.h"

namespace lights3::s3 {

using namespace handlers;
using nlohmann::json;

Task<http::HttpResponse> S3Service::admin_fsck(http::HttpRequest& req, std::string& access_key,
                                               const RequestContext& ctx) {
    try {
        auto ident = auth_.verify(req);
        access_key = ident.access_key;
        if (!is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Running fsck requires a root (statically configured) credential.");
        constexpr std::string_view kBase = "/-/admin/fsck";
        std::string backend = req.path.substr(kBase.size());
        if (backend.size() < 2 || backend.front() != '/' ||
            backend.find('/', 1) != std::string::npos)
            throw S3Error(S3ErrorCode::InvalidRequest, "Usage: /-/admin/fsck/<backend>.");
        backend.erase(0, 1);
        if (!fsck_start_ || !fsck_status_)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "fsck is not available on this deployment.");
        if (req.method == "GET") {
            co_return json_response(200, fsck_status_(backend));
        }
        if (req.method != "POST")
            throw S3Error(S3ErrorCode::MethodNotAllowed,
                          "The specified method is not allowed against this resource.");
        uint64_t mbps = 0;
        if (auto v = req.query_get("max_mbps")) {
            if (v->empty() || v->find_first_not_of("0123456789") != std::string::npos)
                throw S3Error(S3ErrorCode::InvalidArgument, "max_mbps must be a non-negative integer.");
            mbps = std::stoull(*v);
        }
        json j = fsck_start_(backend, mbps * 1000 * 1000);
        AuditEvent e;
        e.event = "fsck.start";
        e.actor = access_key;
        e.request_id = ctx.request_id;
        e.detail = "backend " + backend + " job " + j.value("job_id", json(0)).dump() +
                   " max_mbps " + std::to_string(mbps);
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

}  // namespace lights3::s3
