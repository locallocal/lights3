// lights3-ctl `quota` command group: bucket quotas via the ?quota subresource
// (docs/multi-tenancy.md §3). Implementation in lights3_ctl_quota.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `quota` command group (get / set / clear).
std::shared_ptr<ccmd::command> make_quota();

}  // namespace lights3_ctl
