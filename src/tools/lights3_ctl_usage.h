// lights3-ctl `usage` command: bucket usage counters via /-/admin/usage
// (docs/multi-tenancy.md §2/§6). Implementation in lights3_ctl_usage.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `usage` command (usage [bucket] [--rescan] [--tenant=]).
std::shared_ptr<ccmd::c_command> make_usage();

}  // namespace lights3_ctl
