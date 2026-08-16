// L3: bucket → backend routing (glob rules, matched in declaration order, see
// docs/storage-backend.md §2).
// Build-time validation (docs/gaps.md §6.3): bad glob syntax / literal characters
// impossible in a bucket name / unreachable rules (placed after a catch-all, or duplicating
// an earlier rule) all fail at startup -- a mistyped pattern that silently never matches
// would quietly route the bucket to the default backend. "!pattern" is a negated rule.
// Key-prefix routing is deliberately not offered: bucket-level operations
// (list/delete-bucket) cannot be aggregated across backends, and letting one bucket span
// two backends would break their atomicity and consistency semantics
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "core/config.h"
#include "storage/backend.h"

namespace lights3::storage {

class BucketRouter {
public:
    static BucketRouter build(const BucketsConfig& cfg,
                              std::map<std::string, std::shared_ptr<IStorageBackend>> backends);

    IStorageBackend& resolve(std::string_view bucket) const;
    const std::map<std::string, std::shared_ptr<IStorageBackend>>& backends() const {
        return backends_;
    }
    // Internal data (e.g. credential persistence, docs/credential-management.md §4.1)
    // always lands on the default backend
    std::shared_ptr<IStorageBackend> default_backend() const { return default_; }

private:
    struct Rule {
        std::string glob;
        bool negate = false;  // "!pattern": buckets NOT matching pattern hit this rule
        std::shared_ptr<IStorageBackend> backend;
    };
    std::vector<Rule> rules_;
    std::shared_ptr<IStorageBackend> default_;
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends_;
};

}  // namespace lights3::storage
