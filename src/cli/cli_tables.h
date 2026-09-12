// lights3 `tables export|import` (docs/s3-tables-design.md §12, docs/cli.md §2.6):
// offline migration of the S3 Tables catalog between the object and duostore-meta backings
#pragma once

#include "cli/cli_common.h"

namespace lights3_cli {

Cmd make_tables();

}  // namespace lights3_cli
