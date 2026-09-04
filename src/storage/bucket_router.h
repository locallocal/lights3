// L3: bucket → backend routing (glob rules, matched in declaration order, see
// docs/storage-backend.md §2).
// Build-time validation (docs/archive/gaps.md §6.3): bad glob syntax / literal characters
// impossible in a bucket name / unreachable rules (placed after a catch-all, or duplicating
// an earlier rule) all fail at startup -- a mistyped pattern that silently never matches
// would quietly route the bucket to the default backend. "!pattern" is a negated rule.
// Key-prefix routing is deliberately not offered: bucket-level operations
// (list/delete-bucket) cannot be aggregated across backends, and letting one bucket span
// two backends would break their atomicity and consistency semantics
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "core/config.h"
#include "storage/backend.h"

namespace lights3::storage {

// Copies of a router share one rule table (config hot reload, roadmap §4.4):
// update() swaps the table atomically for every holder — S3Service, the lifecycle
// runner, the usage tracker — while a request in flight keeps the table it
// resolved against. The backend set and the default backend are fixed for the
// process (they carry state and host .sys); update() refuses to change them
class BucketRouter {
public:
    static BucketRouter build(const BucketsConfig& cfg,
                              std::map<std::string, std::shared_ptr<IStorageBackend>> backends);

    IStorageBackend& resolve(std::string_view bucket) const;
    const std::map<std::string, std::shared_ptr<IStorageBackend>>& backends() const {
        return shared_->backends;
    }
    // Internal data (e.g. credential persistence, docs/credential-management.md §4.1)
    // always lands on the default backend
    std::shared_ptr<IStorageBackend> default_backend() const { return shared_->default_backend; }

    // Replace the rule table (same validation as build); throws std::runtime_error
    // and leaves the current table in force on any problem, including a changed
    // default_backend or a rule naming a backend not present at startup
    void update(const BucketsConfig& cfg);
    size_t rule_count() const { return table()->rules.size(); }

private:
    struct Rule {
        std::string glob;
        bool negate = false;  // "!pattern": buckets NOT matching pattern hit this rule
        std::shared_ptr<IStorageBackend> backend;
    };
    struct Table {
        std::vector<Rule> rules;
    };
    struct Shared {
        std::map<std::string, std::shared_ptr<IStorageBackend>> backends;
        std::shared_ptr<IStorageBackend> default_backend;
        std::string default_name;
        std::atomic<std::shared_ptr<const Table>> table;
    };
    static std::shared_ptr<const Table> compile(const BucketsConfig& cfg, const Shared& sh);
    std::shared_ptr<const Table> table() const { return shared_->table.load(std::memory_order_acquire); }

    std::shared_ptr<Shared> shared_;
};

}  // namespace lights3::storage
