// s3adm `quota` command group: bucket quotas via the ?quota subresource
// (docs/multi-tenancy.md §3). Implementation in s3adm_quota.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

// Builds the `quota` command group (get / set / clear).
std::shared_ptr<ccmd::c_command> make_quota();

}  // namespace s3adm
