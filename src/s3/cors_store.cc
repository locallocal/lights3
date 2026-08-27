#include "s3/cors_store.h"

#include <nlohmann/json.hpp>

namespace lights3::s3 {

using nlohmann::json;

std::string CorsTraits::serialize(const Entry& rules) {
    json arr = json::array();
    for (auto& r : rules) {
        json j;
        if (!r.id.empty()) j["id"] = r.id;
        j["allowed_origins"] = r.allowed_origins;
        j["allowed_methods"] = r.allowed_methods;
        if (!r.allowed_headers.empty()) j["allowed_headers"] = r.allowed_headers;
        if (!r.expose_headers.empty()) j["expose_headers"] = r.expose_headers;
        if (r.max_age_seconds >= 0) j["max_age_seconds"] = r.max_age_seconds;
        arr.push_back(std::move(j));
    }
    return json{{"rules", std::move(arr)}}.dump();
}

// nullopt on malformed content: a hand-edited .sys object with no valid rule set must
// not become live config (same philosophy as the website store)
std::optional<CorsTraits::Entry> CorsTraits::deserialize(const std::string&,
                                                         const std::string& body) {
    try {
        auto j = json::parse(body);
        Entry rules;
        for (auto& jr : j.at("rules")) {
            CorsRule r;
            r.id = jr.value("id", "");
            r.allowed_origins = jr.at("allowed_origins").get<std::vector<std::string>>();
            r.allowed_methods = jr.at("allowed_methods").get<std::vector<std::string>>();
            if (jr.contains("allowed_headers"))
                r.allowed_headers = jr["allowed_headers"].get<std::vector<std::string>>();
            if (jr.contains("expose_headers"))
                r.expose_headers = jr["expose_headers"].get<std::vector<std::string>>();
            r.max_age_seconds = jr.value("max_age_seconds", -1);
            if (r.allowed_origins.empty() || r.allowed_methods.empty()) return std::nullopt;
            rules.push_back(std::move(r));
        }
        if (rules.empty()) return std::nullopt;
        return rules;
    } catch (...) {
        return std::nullopt;
    }
}

bool cors_pattern_matches(std::string_view pattern, std::string_view value) {
    auto star = pattern.find('*');
    if (star == std::string_view::npos) return pattern == value;
    std::string_view prefix = pattern.substr(0, star);
    std::string_view suffix = pattern.substr(star + 1);
    return value.size() >= prefix.size() + suffix.size() && value.starts_with(prefix) &&
           value.ends_with(suffix);
}

const CorsRule* match_cors_rule(const std::vector<CorsRule>& rules, const std::string& origin,
                                const std::string& method,
                                const std::vector<std::string>& req_headers) {
    for (auto& r : rules) {
        bool origin_ok = false;
        for (auto& o : r.allowed_origins)
            if (cors_pattern_matches(o, origin)) {
                origin_ok = true;
                break;
            }
        if (!origin_ok) continue;
        bool method_ok = false;
        for (auto& m : r.allowed_methods)
            if (m == method) {
                method_ok = true;
                break;
            }
        if (!method_ok) continue;
        // Every requested header must be admitted by some AllowedHeader pattern
        bool headers_ok = true;
        for (auto& h : req_headers) {
            bool hit = false;
            for (auto& a : r.allowed_headers)
                if (cors_pattern_matches(a, h)) {
                    hit = true;
                    break;
                }
            if (!hit) {
                headers_ok = false;
                break;
            }
        }
        if (headers_ok) return &r;
    }
    return nullptr;
}

}  // namespace lights3::s3
