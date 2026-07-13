// L1: 各 HTTP 驱动共享的适配辅助（不属于 L1/L2 边界，仅驱动内部复用）
#pragma once

#include <string_view>

#include "core/util/uri.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::http::driver {

// 把请求行的 target（"/a%2Fb?x=1&y"）拆成中立模型的四个字段：
// raw_path / raw_query 保留原文（SigV4 需要），path / query 为解码结果（保序）
inline void parse_target(std::string_view target, HttpRequest& req) {
    auto qpos = target.find('?');
    req.raw_path = std::string(qpos == std::string_view::npos ? target : target.substr(0, qpos));
    req.raw_query = qpos == std::string_view::npos ? "" : std::string(target.substr(qpos + 1));
    req.path = util::percent_decode(req.raw_path);
    size_t start = 0;
    while (start < req.raw_query.size()) {
        auto amp = req.raw_query.find('&', start);
        if (amp == std::string::npos) amp = req.raw_query.size();
        std::string kv = req.raw_query.substr(start, amp - start);
        if (!kv.empty()) {
            auto eq = kv.find('=');
            if (eq == std::string::npos)
                req.query.emplace_back(util::percent_decode(kv), "");
            else
                req.query.emplace_back(util::percent_decode(kv.substr(0, eq)),
                                       util::percent_decode(kv.substr(eq + 1)));
        }
        start = amp + 1;
    }
}

// 契约 2（docs/02 §4）：handler 逃逸异常时驱动统一回 500 + S3 InternalError XML
inline HttpResponse internal_error_response() {
    s3::S3Error err(s3::S3ErrorCode::InternalError, "We encountered an internal error.");
    HttpResponse resp;
    resp.status = s3::http_status(err.code);
    resp.small_body = s3::error_xml(err, "");
    resp.headers.set("Content-Type", "application/xml");
    return resp;
}

}  // namespace lights3::http::driver
