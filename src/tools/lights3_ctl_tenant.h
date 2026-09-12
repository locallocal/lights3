// lights3-ctl `tenant` command group: tenant lifecycle and bucket ownership via
// /-/admin/tenants (docs/multi-tenancy.md §6). Implementation in lights3_ctl_tenant.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `tenant` command group (list / get / create / update / delete / assign / unassign).
std::shared_ptr<ccmd::command> make_tenant();

}  // namespace lights3_ctl
