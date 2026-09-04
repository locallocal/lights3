#include "s3/quota.h"

#include <charconv>
#include <nlohmann/json.hpp>

#include "s3/errors.h"
#include "s3/xml.h"

namespace lights3::s3 {

using nlohmann::json;

std::string QuotaTraits::serialize(const Entry& q) {
    json j;
    j["max_bytes"] = q.max_bytes;
    j["max_objects"] = q.max_objects;
    return j.dump() + "\n";
}

std::optional<BucketQuota> QuotaTraits::deserialize(const std::string&, const std::string& body) {
    try {
        json j = json::parse(body);
        BucketQuota q;
        q.max_bytes = j.value("max_bytes", uint64_t{0});
        q.max_objects = j.value("max_objects", uint64_t{0});
        if (!q.max_bytes && !q.max_objects) return std::nullopt;  // never a valid live entry
        return q;
    } catch (const json::exception&) {
        return std::nullopt;
    }
}

std::string quota_xml(const BucketQuota& q) {
    XmlWriter x;
    x.open("QuotaConfiguration", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    x.element("MaxBytes", q.max_bytes);
    x.element("MaxObjects", q.max_objects);
    x.close();
    return x.str();
}

BucketQuota parse_quota_xml(const std::string& body) {
    auto root = xml_parse(body);
    if (root.name != "QuotaConfiguration")
        throw S3Error(S3ErrorCode::MalformedXML, "Expected <QuotaConfiguration> root element.");
    auto num = [&](const char* tag) -> uint64_t {
        std::string v = root.get(tag);
        if (v.empty()) return 0;
        uint64_t out = 0;
        auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), out);
        if (ec != std::errc() || p != v.data() + v.size())
            throw S3Error(S3ErrorCode::MalformedXML,
                          std::string("Invalid ") + tag + " value (non-negative integer).");
        return out;
    };
    BucketQuota q;
    q.max_bytes = num("MaxBytes");
    q.max_objects = num("MaxObjects");
    if (!q.max_bytes && !q.max_objects)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "A quota needs MaxBytes and/or MaxObjects > 0; DELETE ?quota removes it.");
    return q;
}

}  // namespace lights3::s3
