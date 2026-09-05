#include "core/version.h"

namespace lights3 {

namespace build_info {
extern const char* const version;
extern const char* const git_commit;
extern const char* const build_type;
extern const char* const build_date;
}  // namespace build_info

const char* version() { return build_info::version; }
const char* git_commit() { return build_info::git_commit; }
const char* build_type() { return build_info::build_type; }
const char* build_date() { return build_info::build_date; }

std::vector<std::string> built_drivers() {
    std::vector<std::string> out;
#ifdef LIGHTS3_DRIVER_BUILTIN
    out.push_back("builtin");
#endif
#ifdef LIGHTS3_DRIVER_BEAST
    out.push_back("beast");
#endif
#ifdef LIGHTS3_DRIVER_HTTPLIB
    out.push_back("httplib");
#endif
#ifdef LIGHTS3_DRIVER_SEASTAR
    out.push_back("seastar");
#endif
    return out;
}

std::vector<std::string> built_features() {
    std::vector<std::string> out = {"memory", "localfs", "xlocalfs", "tiered"};
#ifdef LIGHTS3_CLOUDPROXY
    out.push_back("cloudproxy");
#endif
#ifdef LIGHTS3_DUOSTORE
    out.push_back("duostore");
#ifdef LIGHTS3_DUOSTORE_REDIS_META
    out.push_back("duostore-redis-meta");
#endif
#ifdef LIGHTS3_DUOSTORE_SQLITE_META
    out.push_back("duostore-sqlite-meta");
#endif
#ifdef LIGHTS3_DUOSTORE_TIKV_META
    out.push_back("duostore-tikv-meta");
#endif
#ifdef LIGHTS3_DUOSTORE_RADOS_DATA
    out.push_back("duostore-rados-data");
#endif
#endif
    return out;
}

static std::string join(const std::vector<std::string>& v) {
    std::string s;
    for (auto& x : v) {
        if (!s.empty()) s += ' ';
        s += x;
    }
    return s;
}

std::string version_line(const char* program) {
    std::string s = program;
    s += ' ';
    s += build_info::version;
    s += " (git ";
    s += build_info::git_commit;
    s += ", ";
    s += build_info::build_type;
    s += ", ";
    s += build_info::build_date;
    s += ')';
    return s;
}

std::string version_report(const char* program) {
    std::string s = version_line(program);
    s += "\ndrivers:  " + join(built_drivers());
    s += "\nfeatures: " + join(built_features());
    s += '\n';
    return s;
}

}  // namespace lights3
