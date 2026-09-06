// lights3-ctl `cred` command group: manages tenant credentials with the ops-plane
// (root static credential) AK/SK, talking to /-/admin/credentials
// (docs/credential-management.md §2/§3). Implementation in lights3_ctl_cred.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `cred` command group (cred list / get / create / delete,
// bind-cert / unbind-cert / list-certs for mTLS identity bindings).
std::shared_ptr<ccmd::c_command> make_cred();

}  // namespace lights3_ctl
