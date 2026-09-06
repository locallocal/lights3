// lights3-ctl `bench` command group: closed-loop load generator against the S3 data
// plane (bench put / get) and non-IO APIs (bench stat / list / list-buckets),
// printing per-second throughput and latency. Implementation in lights3_ctl_bench.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `bench` command group.
std::shared_ptr<ccmd::c_command> make_bench();

}  // namespace lights3_ctl
