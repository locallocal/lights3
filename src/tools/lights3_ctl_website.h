// lights3-ctl `website` command group: manages per-bucket static website configuration
// via the ?website subresource (docs/static-website.md phase ③, root credential
// only). Implementation in lights3_ctl_website.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `website` command group (website get / set / delete).
std::shared_ptr<ccmd::c_command> make_website();

}  // namespace lights3_ctl
