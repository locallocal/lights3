// s3adm `object` command group: `object inspect <bucket> <key>` prints the
// object's internal layout via GET /-/admin/objects (roadmap §6.2).
// Implementation in s3adm_object.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_object();

}  // namespace s3adm
