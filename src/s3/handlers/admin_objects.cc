// L2: GET /-/admin/objects/<bucket>/<key> (roadmap §6.2, `s3adm object inspect`):
// the object's internal layout as the routed backend reports it — data path or
// pack/chunk/rados extents with offsets and checksums, metadata source, tier state.
// Root only (the layout names paths and backend topology); JSON conventions of the
// admin plane; a backend without an inspectable layout answers {"layout": null}
#include "core/log.h"
#include "s3/handlers/admin_json.h"
#include "s3/service.h"

namespace lights3::s3 {

using namespace handlers;
using nlohmann::json;

Task<http::HttpResponse> S3Service::admin_object_inspect(http::HttpRequest& req,
                                                         std::string& access_key,
                                                         const RequestContext& ctx) {
    (void)ctx;
    try {
        auto ident = auth_.verify(req);
        access_key = ident.access_key;
        if (!is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Inspecting object layout requires a root (statically configured) "
                          "credential.");
        if (req.method != "GET")
            throw S3Error(S3ErrorCode::MethodNotAllowed,
                          "The specified method is not allowed against this resource.");
        constexpr std::string_view kBase = "/-/admin/objects";
        std::string rest = req.path.substr(kBase.size());
        if (rest.size() < 2 || rest.front() != '/')
            throw S3Error(S3ErrorCode::InvalidRequest, "Malformed admin API path.");
        rest.erase(0, 1);
        size_t slash = rest.find('/');
        if (slash == std::string::npos || slash == 0 || slash + 1 >= rest.size())
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Usage: /-/admin/objects/<bucket>/<key>.");
        std::string bucket = rest.substr(0, slash);
        std::string key = rest.substr(slash + 1);
        storage::validate_bucket_name(bucket);
        auto& backend = router_.resolve(bucket);
        auto layout = co_await backend.inspect_object(bucket, key);
        // Engines without a layout do not look the key up: surface NoSuchBucket/NoSuchKey
        // the same way before answering "no layout"
        if (!layout) co_await backend.head_object(bucket, key);
        json j;
        j["bucket"] = bucket;
        j["key"] = key;
        j["backend"] = router_.backend_name(bucket);
        if (!layout) {
            j["layout"] = nullptr;
            j["note"] = "this backend exposes no internal layout (memory / cloudproxy)";
        } else {
            json l;
            l["engine"] = layout->engine;
            json attrs = json::object();
            for (auto& [k, v] : layout->attrs) attrs[k] = v;
            l["attrs"] = attrs;
            json ext = json::array();
            for (auto& e : layout->extents) {
                json x;
                x["kind"] = e.kind;
                x["id"] = e.id;
                x["offset"] = e.offset;
                x["length"] = e.length;
                x["crc32c"] = e.crc32c;
                ext.push_back(x);
            }
            l["extents"] = ext;
            j["layout"] = l;
        }
        co_return json_response(200, j);
    } catch (const S3Error& e) {
        co_return admin_error(e, req);
    }
}

}  // namespace lights3::s3
