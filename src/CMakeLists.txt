# The core library and the two executables. Sources are contributed by the module
# directories below (target_sources on lights3_core), so a new translation unit is
# added next to its code rather than in a list here; the same goes for the options,
# definitions and third-party links a module needs.

# ---- Core library: L2/L3/L4 + HTTP boundary ----
add_library(lights3_core STATIC)
target_include_directories(lights3_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# libssl is unconditional since roadmap §4.1: the shared TLS material/holder
# (src/http/tls.cc) serves every OpenSSL-backed driver, builtin included
target_link_libraries(lights3_core PUBLIC OpenSSL::SSL OpenSSL::Crypto Threads::Threads spdlog::spdlog)
# JSON is used only in the admin handler / CredentialStore .cc files, never in public headers (docs/architecture/credential-management.md §5.4)
target_link_libraries(lights3_core PUBLIC nlohmann_json::nlohmann_json)  # json in public headers (service.h, app/admin_jobs.h)

add_subdirectory(core)
add_subdirectory(http)
add_subdirectory(s3)
add_subdirectory(storage)
add_subdirectory(tables)
add_subdirectory(app)

# ---- Build identity (roadmap §6.3, src/core/version.h) ----
# The git commit is stamped at *build* time by a script-mode CMake run so that it
# follows the checkout rather than the last configure; the file is rewritten only
# when its content changes. LIGHTS3_GIT_COMMIT overrides the probe for builds
# without a .git directory (source tarballs, Docker build contexts)
set(LIGHTS3_GIT_COMMIT "" CACHE STRING "Git commit to embed when the source tree has no .git (tarball / container builds)")
set(LIGHTS3_VERSION_CC ${PROJECT_BINARY_DIR}/generated/version_info.cc)
add_custom_target(lights3_version_gen
  COMMAND ${CMAKE_COMMAND}
          -DLIGHTS3_VERSION=${PROJECT_VERSION}
          -DSOURCE_DIR=${PROJECT_SOURCE_DIR}
          -DOUTPUT=${LIGHTS3_VERSION_CC}
          -DBUILD_TYPE=${CMAKE_BUILD_TYPE}
          -DGIT_COMMIT_OVERRIDE=${LIGHTS3_GIT_COMMIT}
          -P ${PROJECT_SOURCE_DIR}/cmake/GenerateVersion.cmake
  BYPRODUCTS ${LIGHTS3_VERSION_CC}
  COMMENT "Refreshing build version stamp"
  VERBATIM)
target_sources(lights3_core PRIVATE core/version.cc ${LIGHTS3_VERSION_CC})
add_dependencies(lights3_core lights3_version_gen)

# ---- Main executable ----
# src/main.cc holds the root command + main; each command group is one
# src/cli/cli_<group>.cc (make_<group>()), shared helpers in cli_common
add_executable(lights3 main.cc)
target_link_libraries(lights3 PRIVATE lights3_core ccmd::ccmd)
# Executables stay in the build root even though they are declared in subdirectories:
# scripts/, docker/, the Makefile, the docs and every `ctest` recipe refer to
# build*/<binary>, and a per-directory output tree would break all of them
set_target_properties(lights3 PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
add_subdirectory(cli)

# ---- Ops tooling ----
add_subdirectory(tools)
