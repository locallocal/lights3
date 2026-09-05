// s3adm `mpu` command group: list / abort multipart uploads (zombie cleanup,
// roadmap §6.2). Implementation in s3adm_mpu.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_mpu();

}  // namespace s3adm
