// s3adm `website` command group: manages per-bucket static website configuration
// via the ?website subresource (docs/static-website.md phase ③, root credential
// only). Implementation in s3adm_website.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

// Builds the `website` command group (website get / set / delete).
std::shared_ptr<ccmd::c_command> make_website();

}  // namespace s3adm
