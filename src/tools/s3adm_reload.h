// s3adm `reload`: POST /-/admin/config/reload (config hot reload, roadmap §4.4,
// docs/config-reload.md). Implementation in s3adm_reload.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_reload();

}  // namespace s3adm
