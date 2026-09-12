// lights3-ctl `mpu` command group: list / abort multipart uploads (zombie cleanup,
// roadmap §6.2). Implementation in lights3_ctl_mpu.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

std::shared_ptr<ccmd::command> make_mpu();

}  // namespace lights3_ctl
