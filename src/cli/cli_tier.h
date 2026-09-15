// `lights3 tier scan|gc|reconcile <backend>` and `lights3 tier quarantine
// list|forget|purge`: tiered background tasks on demand and the
// reconcile quarantine ledger. Implementation in cli_tier.cc.
#pragma once

#include "cli/cli_common.h"

namespace lights3_cli {

Cmd make_tier();

}  // namespace lights3_cli
