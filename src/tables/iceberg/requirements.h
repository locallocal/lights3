// CommitTable requirements (docs/architecture/s3-tables-design.md §7.1): the 8 assert-* checks against
// the current metadata; a failed assertion is a 409 CommitFailedException, an unknown
// type a 400
#pragma once

#include "tables/iceberg/metadata.h"

namespace lights3::tables::iceberg {

void check_requirements(const Json& current, const Json& requirements, bool table_exists);

}  // namespace lights3::tables::iceberg
