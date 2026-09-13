// The job framework as the catalog sees it (docs/architecture/s3-tables-design.md §9): the
// application owns AdminJobs; the catalog only needs "run this function on a dedicated
// thread under a resource key, one job per resource at a time" plus status lookups. The
// REST layer, the admin plane and the periodic runner all go through these hooks, so a
// manual job and the runner never work on one table concurrently
#pragma once

#include <cstdint>
#include <functional>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "tables/identifier.h"

namespace lights3::tables {

struct JobHooks {
    // runs on the job thread; returns the job's stats document; throwing fails the job
    using Fn = std::function<nlohmann::json()>;
    // start a job on `resource` for `op` ("plan" | "run" | "purge"); returns the status
    // document (with job_id); throws s3::S3Error(JobInProgress) while a job runs on the
    // resource
    std::function<nlohmann::json(const std::string& resource, const std::string& op, Fn fn)> start;
    // the most recent job of (resource, op)
    std::function<nlohmann::json(const std::string& resource, const std::string& op)> status;
    // a job by id (any resource); nullopt = unknown id
    std::function<std::optional<nlohmann::json>(uint64_t job_id)> status_by_id;

    explicit operator bool() const { return start && status && status_by_id; }
};

// "tables:<bucket>/<ns-path>/<t>" -- the resource key of a table's jobs
inline std::string job_resource(std::string_view bucket, const Levels& levels, std::string_view name) {
    return "tables:" + std::string(bucket) + "/" + ns_path(levels) + "/" + std::string(name);
}

}  // namespace lights3::tables
