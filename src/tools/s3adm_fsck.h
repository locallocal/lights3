// s3adm `fsck` command: online end-to-end object verification over the S3 API
// (roadmap §3.1) — list a bucket, GET every object back and compare the
// recomputed MD5 against its ETag. Implementation in s3adm_fsck.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace s3adm {

// Builds the `fsck` leaf command.
std::shared_ptr<ccmd::c_command> make_fsck();

}  // namespace s3adm
