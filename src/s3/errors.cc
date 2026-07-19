#include "s3/errors.h"

#include "s3/xml.h"

namespace lights3::s3 {

namespace {
struct Entry {
    int status;
    const char* code;
};

// 与枚举同源生成（errors.h 的 LIGHTS3_S3_ERROR_CODES），按枚举值索引
constexpr Entry kEntries[] = {
#define LIGHTS3_S3_ERROR_ENTRY(name, st) {st, #name},
    LIGHTS3_S3_ERROR_CODES(LIGHTS3_S3_ERROR_ENTRY)
#undef LIGHTS3_S3_ERROR_ENTRY
};

constexpr Entry entry(S3ErrorCode c) { return kEntries[static_cast<size_t>(c)]; }
}  // namespace

int http_status(S3ErrorCode code) { return entry(code).status; }
const char* wire_code(S3ErrorCode code) { return entry(code).code; }

std::optional<S3ErrorCode> code_from_wire(std::string_view wire) {
    for (size_t i = 0; i < std::size(kEntries); ++i)
        if (wire == kEntries[i].code) return static_cast<S3ErrorCode>(i);
    return std::nullopt;
}

std::string error_xml(const S3Error& e, const std::string& request_id) {
    XmlWriter w;
    w.open("Error");
    w.element("Code", wire_code(e.code));
    w.element("Message", e.message);
    if (!e.resource.empty()) w.element("Resource", e.resource);
    w.element("RequestId", request_id);
    w.close();
    return w.str();
}

}  // namespace lights3::s3
