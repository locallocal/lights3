// `lights3 fsck <backend> [--max-mbps=<n>]`: offline integrity scrub (roadmap
// §3.1). Implementation in cli_fsck.cc.
#pragma once

#include "cli/cli_common.h"

namespace lights3_cli {

Cmd make_fsck();

}  // namespace lights3_cli
