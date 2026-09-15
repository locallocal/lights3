// `lights3 duostore ...`: duostore meta admin — dump/load
// (docs/architecture/storage/duostore-core.md §11), backup chains / PITR (§11.1), on-demand
// GC / orphan-scan rounds and the corrupt-pack quarantine. Compiled only with LIGHTS3_DUOSTORE (cli_duostore.cc is
// added to the target inside that switch); main.cc registers it under the same guard.
#pragma once

#include "cli/cli_common.h"

namespace lights3_cli {

Cmd make_duostore();

}  // namespace lights3_cli
