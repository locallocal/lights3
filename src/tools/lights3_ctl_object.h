// lights3-ctl `object` command group: `object inspect <bucket> <key>` prints the
// object's internal layout via GET /-/admin/objects.
// Implementation in lights3_ctl_object.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

std::shared_ptr<ccmd::command> make_object();

}  // namespace lights3_ctl
