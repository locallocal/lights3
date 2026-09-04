// s3adm `tenant` command group: tenant lifecycle and bucket ownership via
// /-/admin/tenants (docs/multi-tenancy.md §6). Implementation in s3adm_tenant.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

// Builds the `tenant` command group (list / get / create / update / delete / assign / unassign).
std::shared_ptr<ccmd::c_command> make_tenant();

}  // namespace s3adm
