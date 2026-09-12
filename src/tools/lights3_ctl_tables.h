// lights3-ctl `tables` command group (docs/cli.md §3.13, docs/s3-tables-design.md §9)
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

std::shared_ptr<ccmd::command> make_tables();

}  // namespace lights3_ctl
