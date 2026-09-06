// L3: bucket → backend routing (glob rules, matched in declaration order, see
// docs/storage/storage-backend.md §2).
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
// resolved against. The backend set can be replaced the same way (backend
// instances added / removed at runtime, backlog-sequence ⑦): rules and backend
// set travel in one snapshot, so a table never names a backend outside its set.
// The default backend is fixed for the process (it hosts .sys and the stores
// loaded from it); update() refuses to change it
class BucketRouter {
public:
    using BackendMap = std::map<std::string, std::shared_ptr<IStorageBackend>>;

    static BucketRouter build(const BucketsConfig& cfg, BackendMap backends);

    IStorageBackend& resolve(std::string_view bucket) const;
    // Snapshot of the backend set (name -> instance): stable for the caller's
    // iteration even while a reload swaps the set underneath
    std::shared_ptr<const BackendMap> backends() const { return table()->backends; }
    // Internal data (e.g. credential persistence, docs/credential-management.md §4.1)
    // always lands on the default backend
    std::shared_ptr<IStorageBackend> default_backend() const { return shared_->default_backend; }

    // Configured name of the backend a bucket routes to (metrics label, roadmap §5.1)
    std::string backend_name(std::string_view bucket) const;
    const std::string& default_backend_name() const { return shared_->default_name; }

    // Replace the rule table (same validation as build); throws std::runtime_error
    // and leaves the current table in force on any problem, including a changed
    // default_backend or a rule naming a backend outside the current set
    void update(const BucketsConfig& cfg);
    // Replace rules and backend set together (backend hot add / remove): the new
    // set must still contain the default backend (same instance); rules are
    // validated against the new set before anything is swapped
    void update(const BucketsConfig& cfg, BackendMap backends);
    size_t rule_count() const { return table()->rules.size(); }

private:
    struct Rule {
        std::string glob;
        bool negate = false;  // "!pattern": buckets NOT matching pattern hit this rule
        std::shared_ptr<IStorageBackend> backend;
        std::string backend_name;
    };
    struct Table {
        std::vector<Rule> rules;
        std::shared_ptr<const BackendMap> backends;
    };
    struct Shared {
        std::shared_ptr<IStorageBackend> default_backend;
        std::string default_name;
        std::atomic<std::shared_ptr<const Table>> table;
    };
    static std::shared_ptr<const Table> compile(const BucketsConfig& cfg,
                                                std::shared_ptr<const BackendMap> backends);
    std::shared_ptr<const Table> table() const { return shared_->table.load(std::memory_order_acquire); }

    std::shared_ptr<Shared> shared_;
};

}  // namespace lights3::storage
