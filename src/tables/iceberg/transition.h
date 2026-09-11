// Transition invariants current → next (docs/s3-tables-design.md §7.3): the last line of
// defence when concurrent writers bypass requirements. Failure → 409
#pragma once

#include "tables/iceberg/metadata.h"

namespace lights3::tables::iceberg {

void check_transition(const Json& current, const Json& next);

}  // namespace lights3::tables::iceberg
