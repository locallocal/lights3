// lights3-ctl `fsck` command: online end-to-end object verification over the S3 API
// (roadmap §3.1) — list a bucket, GET every object back and compare the
// recomputed MD5 against its ETag. Implementation in lights3_ctl_fsck.cc.
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

// Builds the `fsck` leaf command.
std::shared_ptr<ccmd::command> make_fsck();

}  // namespace lights3_ctl
