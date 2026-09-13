# lights3-ctl: the ops CLI (docs/usage/cli.md §3). CPPHTTPLIB_OPENSSL_SUPPORT
# propagates via lights3_core's PUBLIC definition, staying target-level consistent
# with the core library (ODR); in trimmed builds without httplib/cloudproxy it
# automatically degrades to http-only
add_executable(lights3-ctl
  lights3_ctl.cc
  lights3_ctl_common.cc
  lights3_ctl_cred.cc
  lights3_ctl_bench.cc
  lights3_ctl_website.cc
  lights3_ctl_fsck.cc
  lights3_ctl_tenant.cc
  lights3_ctl_quota.cc
  lights3_ctl_usage.cc
  lights3_ctl_reload.cc
  lights3_ctl_object.cc
  lights3_ctl_mpu.cc
  lights3_ctl_jobs.cc
  lights3_ctl_tables.cc
)
target_link_libraries(lights3-ctl PRIVATE lights3_core ccmd::ccmd
                                    nlohmann_json::nlohmann_json)
target_include_directories(lights3-ctl SYSTEM PRIVATE ${PROJECT_SOURCE_DIR}/third_party)
# see the note in ../CMakeLists.txt: the binary belongs in the build root
set_target_properties(lights3-ctl PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
