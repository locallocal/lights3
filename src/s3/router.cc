#include "s3/router.h"

namespace lights3::s3 {

std::pair<std::string, std::string> parse_bucket_key(const std::string& path) {
    std::string p = path;
    if (!p.empty() && p.front() == '/') p.erase(0, 1);
    auto slash = p.find('/');
    if (slash == std::string::npos) return {p, ""};
    return {p.substr(0, slash), p.substr(slash + 1)};
}

}  // namespace lights3::s3
