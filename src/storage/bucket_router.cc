#include "storage/bucket_router.h"

#include <fnmatch.h>

#include <stdexcept>

namespace lights3::storage {

BucketRouter BucketRouter::build(
    const BucketsConfig& cfg, std::map<std::string, std::shared_ptr<IStorageBackend>> backends) {
    BucketRouter r;
    r.backends_ = std::move(backends);
    auto find = [&](const std::string& name) {
        auto it = r.backends_.find(name);
        if (it == r.backends_.end())
            throw std::runtime_error("bucket rule references unknown backend: " + name);
        return it->second;
    };
    for (auto& rule : cfg.rules) r.rules_.push_back({rule.match, find(rule.backend)});
    r.default_ = find(cfg.default_backend);
    return r;
}

IStorageBackend& BucketRouter::resolve(std::string_view bucket) const {
    std::string b(bucket);
    for (auto& rule : rules_)
        if (fnmatch(rule.glob.c_str(), b.c_str(), 0) == 0) return *rule.backend;
    return *default_;
}

}  // namespace lights3::storage
