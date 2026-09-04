#include "storage/bucket_router.h"

#include <fnmatch.h>

#include <stdexcept>

namespace lights3::storage {

namespace {

// Glob syntax validation (docs/archive/gaps.md §6.3): fnmatch does not report a bad pattern as an
// error, only as "no match", so a mistyped rule silently never matches -- the bucket is
// quietly routed to the default backend, and migrating the data once it lands in the wrong
// engine means a full copy. Any error determinable at build time is rejected at build time:
// (1) unclosed '[' character class; (2) pattern contains literal characters outside the
// bucket-name character set (lowercase/digits/'-'/'.') -- uppercase, '_', '/', etc. --
// which a valid bucket name can never match
void validate_glob(const std::string& pattern) {
    auto fail = [&](const std::string& why) {
        throw std::runtime_error("bucket rule glob '" + pattern + "': " + why);
    };
    if (pattern.empty()) fail("empty pattern");
    bool in_class = false;
    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (in_class) {
            if (c == ']') in_class = false;
            continue;
        }
        if (c == '[') {
            in_class = true;
            // '[]a]' form: an immediately following ']' is a literal member, does not close the class
            if (i + 1 < pattern.size() && pattern[i + 1] == ']') ++i;
            continue;
        }
        if (c == '*' || c == '?' || c == '\\') continue;
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.'))
            fail(std::string("literal character '") + c +
                 "' can never appear in a valid bucket name (rule would silently never match)");
    }
    if (in_class) fail("unclosed '[' character class");
}

bool glob_match(const std::string& glob, std::string_view bucket) {
    // Bucket names are ≤ 63 bytes (validate_bucket_name); a stack buffer avoids a heap
    // allocation per request
    char buf[64];
    size_t n = std::min(bucket.size(), sizeof(buf) - 1);
    bucket.copy(buf, n);
    buf[n] = '\0';
    return ::fnmatch(glob.c_str(), buf, 0) == 0;
}

}  // namespace

std::shared_ptr<const BucketRouter::Table> BucketRouter::compile(const BucketsConfig& cfg,
                                                                 const Shared& sh) {
    auto t = std::make_shared<Table>();
    auto find = [&](const std::string& name) {
        auto it = sh.backends.find(name);
        if (it == sh.backends.end())
            throw std::runtime_error("bucket rule references unknown backend: " + name);
        return it->second;
    };
    bool saw_catch_all = false;
    for (auto& rule : cfg.rules) {
        // Negated rule (docs/archive/gaps.md §6.3): "!pattern" = buckets NOT matching pattern hit this rule
        bool negate = !rule.match.empty() && rule.match.front() == '!';
        std::string glob = negate ? rule.match.substr(1) : rule.match;
        validate_glob(glob);
        // Unreachability check: with declaration-order matching, a rule placed after a
        // catch-all can never be reached -- that is a configuration error (most likely the
        // default rule was written first), and ignoring it silently only breeds confusion
        if (saw_catch_all)
            throw std::runtime_error("bucket rule '" + rule.match +
                                     "' is unreachable: it follows a catch-all rule");
        for (auto& prev : t->rules)
            if (prev.glob == glob && prev.negate == negate)
                throw std::runtime_error("bucket rule '" + rule.match +
                                         "' is unreachable: duplicate of an earlier rule");
        if (!negate && (glob == "*" || glob == "**")) saw_catch_all = true;
        if (negate && glob.find_first_of("*?[") == std::string::npos &&
            glob.find('\\') == std::string::npos) {
            // "!fixed-string" matches every bucket except one name -- it is itself a catch-all
            saw_catch_all = true;
        }
        t->rules.push_back({std::move(glob), negate, find(rule.backend)});
    }
    return t;
}

BucketRouter BucketRouter::build(
    const BucketsConfig& cfg, std::map<std::string, std::shared_ptr<IStorageBackend>> backends) {
    BucketRouter r;
    r.shared_ = std::make_shared<Shared>();
    r.shared_->backends = std::move(backends);
    auto it = r.shared_->backends.find(cfg.default_backend);
    if (it == r.shared_->backends.end())
        throw std::runtime_error("bucket rule references unknown backend: " + cfg.default_backend);
    r.shared_->default_backend = it->second;
    r.shared_->default_name = cfg.default_backend;
    r.shared_->table.store(compile(cfg, *r.shared_), std::memory_order_release);
    return r;
}

void BucketRouter::update(const BucketsConfig& cfg) {
    if (cfg.default_backend != shared_->default_name)
        throw std::runtime_error("buckets.default_backend cannot change at runtime (" +
                                 shared_->default_name + " -> " + cfg.default_backend +
                                 "): it hosts .sys and the stores loaded from it");
    auto fresh = compile(cfg, *shared_);  // validates before anything is swapped
    shared_->table.store(std::move(fresh), std::memory_order_release);
}

IStorageBackend& BucketRouter::resolve(std::string_view bucket) const {
    auto t = table();
    for (auto& rule : t->rules)
        if (glob_match(rule.glob, bucket) != rule.negate) return *rule.backend;
    return *shared_->default_backend;
}

}  // namespace lights3::storage
