#include "s3/service.h"

#include <cctype>
#include <cstring>
#include <random>

#include "core/log.h"
#include "core/util/hex.h"
#include "s3/errors.h"
#include "s3/router.h"

namespace lights3::s3 {

namespace {

std::string make_request_id() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    uint64_t v = rng();
    uint8_t bytes[8];
    memcpy(bytes, &v, 8);
    std::string hex = util::to_hex(std::span(bytes, 8));
    for (char& c : hex) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    return hex;
}

http::HttpResponse error_response(const S3Error& e, const std::string& request_id,
                                  bool head_only) {
    http::HttpResponse resp;
    resp.status = http_status(e.code);
    resp.headers.set("Content-Type", "application/xml");
    if (!head_only) resp.small_body = error_xml(e, request_id);
    return resp;
}

}  // namespace

Task<http::HttpResponse> S3Service::dispatch(http::HttpRequest req) {
    RequestContext ctx{make_request_id()};
    bool head = req.method == "HEAD";
    http::HttpResponse resp;
    try {
        if (req.path == "/-/healthz") {
            resp.small_body = "ok\n";
            resp.headers.set("Content-Type", "text/plain");
        } else {
            auth_.verify(req);
            auto [bucket, key] = parse_bucket_key(req.path);
            resp = co_await route(req, std::move(bucket), std::move(key));
        }
    } catch (const S3Error& e) {
        LOG_INFO("req ", ctx.request_id, " ", req.method, " ", req.path, " -> ",
                 wire_code(e.code), ": ", e.message);
        resp = error_response(e, ctx.request_id, head);
    } catch (const std::exception& e) {
        LOG_ERROR("req ", ctx.request_id, " ", req.method, " ", req.path,
                  " internal error: ", e.what());
        resp = error_response(S3Error(S3ErrorCode::InternalError, "We encountered an internal error."),
                              ctx.request_id, head);
    }
    resp.headers.set("x-amz-request-id", ctx.request_id);
    resp.headers.set("Server", "lights3");
    co_return resp;
}

Task<http::HttpResponse> S3Service::route(http::HttpRequest& req, std::string bucket,
                                          std::string key) {
    const std::string& m = req.method;

    if (bucket.empty()) {  // service 级
        if (m == "GET") co_return co_await list_buckets();
        throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed.");
    }

    if (key.empty()) {  // bucket 级
        if (m == "PUT") co_return co_await create_bucket(std::move(bucket));
        if (m == "HEAD") co_return co_await head_bucket(std::move(bucket));
        if (m == "DELETE") co_return co_await delete_bucket(std::move(bucket));
        if (m == "GET") co_return co_await list_objects(req, std::move(bucket));
        if (m == "POST" && req.query_has("delete"))
            throw S3Error(S3ErrorCode::NotImplemented, "DeleteObjects is not implemented yet.");
        throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed.");
    }

    // object 级
    if (req.query_has("uploads") || req.query_has("uploadId"))
        throw S3Error(S3ErrorCode::NotImplemented, "Multipart upload is not implemented yet.");
    if (m == "PUT") {
        if (req.headers.has("x-amz-copy-source"))
            throw S3Error(S3ErrorCode::NotImplemented, "CopyObject is not implemented yet.");
        co_return co_await put_object(req, std::move(bucket), std::move(key));
    }
    if (m == "GET") co_return co_await get_object(req, std::move(bucket), std::move(key), false);
    if (m == "HEAD") co_return co_await get_object(req, std::move(bucket), std::move(key), true);
    if (m == "DELETE") co_return co_await delete_object(std::move(bucket), std::move(key));
    throw S3Error(S3ErrorCode::MethodNotAllowed, "The specified method is not allowed.");
}

}  // namespace lights3::s3
