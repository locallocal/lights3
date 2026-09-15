# Sync driver on the vendored cpp-httplib header: thread-per-request
# (docs/architecture/http-adapter.md §3.2)
if(LIGHTS3_DRIVER_HTTPLIB)
  target_sources(lights3_core PRIVATE httplib_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_HTTPLIB)
  target_include_directories(lights3_core SYSTEM PRIVATE ${PROJECT_SOURCE_DIR}/third_party)
  # Server-side TLS needs SSLServer. The OPENSSL_SUPPORT macro must be
  # uniform target-wide (ODR, see the cloudproxy branch comment below), hence also defined PUBLIC here
  target_compile_definitions(lights3_core PUBLIC CPPHTTPLIB_OPENSSL_SUPPORT)
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
endif()
