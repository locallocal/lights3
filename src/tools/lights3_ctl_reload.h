// lights3-ctl `reload`: POST /-/admin/config/reload (config hot reload, roadmap §4.4,
// docs/config-reload.md). Implementation in lights3_ctl_reload.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

std::shared_ptr<ccmd::c_command> make_reload();

}  // namespace lights3_ctl
