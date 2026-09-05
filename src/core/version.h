// Build identity (roadmap §6.3): the version from CMake's project(VERSION), the
// git commit stamped at build time (cmake/GenerateVersion.cmake), and the set of
// drivers / backends compiled into this binary. Surfaces through `--version` on
// lights3 and s3adm, the startup log line, and the lights3_build_info gauge, so
// an operator can tell which build a running instance is.
#pragma once

#include <string>
#include <vector>

namespace lights3 {

// "0.1.0"
const char* version();
// "d6f3829a1b2c" / "d6f3829a1b2c-dirty" / "unknown" (no git at build time)
const char* git_commit();
// CMAKE_BUILD_TYPE at build time ("RelWithDebInfo", ...)
const char* build_type();
// UTC date of the build, "YYYY-MM-DD"
const char* build_date();
// HTTP drivers compiled in, in registration order ("builtin", "beast", ...)
std::vector<std::string> built_drivers();
// Storage backend types / duostore engines compiled in ("localfs", "duostore",
// "duostore-redis-meta", ...): the feature switches, not the runtime registry
std::vector<std::string> built_features();
// One line: "lights3 0.1.0 (git d6f3829a1b2c, RelWithDebInfo, 2026-09-05)"
std::string version_line(const char* program);
// The full --version text: version line plus drivers and features
std::string version_report(const char* program);

}  // namespace lights3
