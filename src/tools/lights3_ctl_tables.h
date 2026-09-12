// lights3-ctl `tables` command group (docs/cli.md §3.13, docs/s3-tables/step-4-maintenance.md §7)
#pragma once

#include <ccmd.h>

#include <memory>

namespace lights3_ctl {

std::shared_ptr<ccmd::c_command> make_tables();

}  // namespace lights3_ctl
