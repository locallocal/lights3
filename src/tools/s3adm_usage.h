// s3adm `usage` command: bucket usage counters via /-/admin/usage
// (docs/multi-tenancy.md §2/§6). Implementation in s3adm_usage.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

// Builds the `usage` command (usage [bucket] [--rescan] [--tenant=]).
std::shared_ptr<ccmd::c_command> make_usage();

}  // namespace s3adm
