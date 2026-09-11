#include "tables/iceberg/snapshots.h"

#include <set>

#include "s3/errors.h"
#include "tables/identifier.h"
#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

Task<void> require_object(const SnapshotCheckContext& ctx, const std::string& path) {
    std::string key = path_to_key(ctx.bucket, path);
    if (key.rfind(ctx.reserved_prefix, 0) == 0)
        throw commit_failed("referenced file '" + path + "' lies under the reserved catalog prefix");
    bool exists = true;
    try {
        co_await ctx.backend.head_object(ctx.bucket, key);
    } catch (const s3::S3Error& e) {
        if (e.code != s3::S3ErrorCode::NoSuchKey && e.code != s3::S3ErrorCode::NoSuchBucket) throw;
        exists = false;
    }
    if (!exists) throw commit_failed("referenced file '" + path + "' does not exist");
}

}  // namespace

Task<void> check_new_snapshots_shallow(const SnapshotCheckContext& ctx, const Json& current, const Json& next) {
    std::set<int64_t> known;
    if (current.contains("snapshots"))
        for (auto& s : current["snapshots"]) known.insert(s["snapshot-id"].get<int64_t>());
    for (auto& s : next["snapshots"]) {
        if (known.count(s["snapshot-id"].get<int64_t>())) continue;
        if (s.contains("manifest-list") && s["manifest-list"].is_string()) {
            co_await require_object(ctx, s["manifest-list"].get<std::string>());
        } else if (s.contains("manifests") && s["manifests"].is_array()) {
            for (auto& m : s["manifests"]) {
                if (!m.is_string()) throw bad_request("snapshot manifests must be strings");
                co_await require_object(ctx, m.get<std::string>());
            }
        }
    }
}

}  // namespace lights3::tables::iceberg
