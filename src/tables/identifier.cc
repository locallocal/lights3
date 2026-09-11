#include "tables/identifier.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>

#include "core/util/checksum.h"
#include "core/util/uri.h"
#include "s3/errors.h"
#include "tables/rest_error.h"

namespace lights3::tables {

namespace {

bool seg_char(char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-'; }
bool edge_char(char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); }

}  // namespace

bool valid_segment(std::string_view s) {
    if (s.empty() || s.size() > kMaxSegment) return false;
    if (!edge_char(s.front()) || !edge_char(s.back())) return false;
    for (char c : s)
        if (!seg_char(c)) return false;
    // reserved layout segments (design §4.3); "_ns.json" already fails on '.'
    if (s == "tbl" || s == "view") return false;
    return true;
}

void require_segment(std::string_view what, std::string_view s) {
    if (!valid_segment(s))
        throw bad_request(std::string(what) + " '" + std::string(s) +
                          "' is invalid: 1-64 characters of [a-z0-9_-], starting and ending with a letter or digit");
}

void require_namespace(const Levels& levels) {
    if (levels.empty()) throw bad_request("namespace must have at least one level");
    size_t total = 0;
    for (auto& l : levels) {
        require_segment("namespace level", l);
        total += l.size() + 1;
    }
    if (total > kMaxNamespace + 1) throw bad_request("namespace exceeds 512 characters");
}

Levels parse_namespace_path(std::string_view raw_segment) {
    std::string decoded = util::percent_decode(raw_segment);
    Levels out;
    char sep = '\x1f';
    if (decoded.find(sep) == std::string::npos && decoded.find('.') != std::string::npos) sep = '.';
    size_t pos = 0;
    while (pos <= decoded.size()) {
        size_t next = decoded.find(sep, pos);
        if (next == std::string::npos) next = decoded.size();
        out.push_back(decoded.substr(pos, next - pos));
        pos = next + 1;
    }
    require_namespace(out);
    return out;
}

Levels parse_namespace_json(const nlohmann::json& v) {
    Levels out;
    if (v.is_string()) {
        std::string s = v.get<std::string>();
        size_t pos = 0;
        while (pos <= s.size()) {
            size_t next = s.find('.', pos);
            if (next == std::string::npos) next = s.size();
            out.push_back(s.substr(pos, next - pos));
            pos = next + 1;
        }
    } else if (v.is_array()) {
        for (auto& e : v) {
            if (!e.is_string()) throw bad_request("namespace levels must be strings");
            out.push_back(e.get<std::string>());
        }
    } else {
        throw bad_request("namespace must be an array of strings");
    }
    require_namespace(out);
    return out;
}

std::string ns_path(const Levels& levels) {
    std::string out;
    for (auto& l : levels) {
        if (!out.empty()) out += '/';
        out += l;
    }
    return out;
}

std::string ns_display(const Levels& levels) {
    std::string out;
    for (auto& l : levels) {
        if (!out.empty()) out += '.';
        out += l;
    }
    return out;
}

namespace {

// s3://<bucket>/<key> or s3a://; nullopt when the scheme or bucket differs
std::optional<std::string> strip_bucket(std::string_view bucket, std::string_view path) {
    std::string_view rest;
    if (path.rfind("s3://", 0) == 0)
        rest = path.substr(5);
    else if (path.rfind("s3a://", 0) == 0)
        rest = path.substr(6);
    else
        return std::nullopt;
    if (rest.size() < bucket.size() || rest.substr(0, bucket.size()) != bucket) return std::nullopt;
    rest.remove_prefix(bucket.size());
    if (rest.empty()) return std::string();
    if (rest.front() != '/') return std::nullopt;
    rest.remove_prefix(1);
    return std::string(rest);
}

bool has_dot_segment(std::string_view key) {
    size_t pos = 0;
    while (pos <= key.size()) {
        size_t next = key.find('/', pos);
        if (next == std::string::npos) next = key.size();
        auto seg = key.substr(pos, next - pos);
        if (seg == "." || seg == "..") return true;
        pos = next + 1;
    }
    return false;
}

}  // namespace

std::string location_to_key(std::string_view bucket, std::string_view reserved_prefix, std::string_view location) {
    auto key = strip_bucket(bucket, location);
    if (!key)
        throw bad_request("location '" + std::string(location) + "' must be s3://" + std::string(bucket) + "/<path>");
    while (!key->empty() && key->back() == '/') key->pop_back();
    if (key->empty()) throw bad_request("location must point below the bucket root");
    if (has_dot_segment(*key) || key->find("//") != std::string::npos || key->front() == '/')
        throw bad_request("location contains an invalid path segment");
    if (key->rfind(reserved_prefix, 0) == 0 || *key + "/" == reserved_prefix)
        throw bad_request("location must not lie under the reserved catalog prefix");
    for (char c : *key)
        if (static_cast<unsigned char>(c) < 0x20 || c == 0x7f) throw bad_request("location contains control characters");
    return *key;
}

std::string path_to_key(std::string_view bucket, std::string_view path) {
    auto key = strip_bucket(bucket, path);
    if (!key || key->empty() || key->front() == '/' || has_dot_segment(*key))
        throw commit_failed("referenced file '" + std::string(path) + "' is not inside bucket " + std::string(bucket));
    return *key;
}

std::string base64url(std::string_view raw) {
    std::string b = util::base64_encode(raw);
    for (char& c : b) {
        if (c == '+') c = '-';
        if (c == '/') c = '_';
    }
    while (!b.empty() && b.back() == '=') b.pop_back();
    return b;
}

std::string base64url_decode(std::string_view text) {
    std::string b(text);
    for (char& c : b) {
        if (c == '-') c = '+';
        if (c == '_') c = '/';
    }
    while (b.size() % 4) b += '=';
    auto out = util::base64_decode(b);
    if (!out) throw bad_request("malformed base64url value");
    return *out;
}

std::string random_hex16() {
    uint8_t bytes[16];
    if (::getentropy(bytes, sizeof(bytes)) != 0)
        throw s3::S3Error(s3::S3ErrorCode::InternalError, "cannot generate random identifier");
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(32);
    for (uint8_t b : bytes) {
        out.push_back(hex[b >> 4]);
        out.push_back(hex[b & 15]);
    }
    return out;
}

std::string new_uuid() {
    uint8_t bytes[16];
    if (::getentropy(bytes, sizeof(bytes)) != 0)
        throw s3::S3Error(s3::S3ErrorCode::InternalError, "cannot generate uuid");
    bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3f) | 0x80);
    char buf[37];
    std::snprintf(buf, sizeof(buf), "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", bytes[0],
                  bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10],
                  bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
    return buf;
}

bool looks_like_uuid(std::string_view s) {
    if (s.size() != 36) return false;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
            continue;
        }
        bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!hex) return false;
    }
    return true;
}

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t now_unix() {
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace lights3::tables
