// s3adm `cred` command group: manages tenant credentials with the ops-plane
// (root static credential) AK/SK, talking to /-/admin/credentials
// (docs/credential-management.md §2/§3). Implementation in s3adm_cred.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

// Builds the `cred` command group (cred list / get / create / delete).
std::shared_ptr<ccmd::c_command> make_cred();

}  // namespace s3adm
