# Proxy backend: a remote S3 endpoint reached with self-signed SigV4
# (docs/architecture/storage/cloudproxy-design.md)
if(LIGHTS3_CLOUDPROXY)
  target_sources(lights3_core PRIVATE
    remote_client.cc
    aws_credentials.cc
    cloudproxy_backend.cc
  )
  # OPENSSL_SUPPORT must be uniform target-wide: having it on in httplib_server.cc but
  # off in the cloudproxy TUs is an ODR violation (docs/architecture/storage/cloudproxy-design.md §8.3)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_CLOUDPROXY CPPHTTPLIB_OPENSSL_SUPPORT)
  target_include_directories(lights3_core SYSTEM PRIVATE ${PROJECT_SOURCE_DIR}/third_party)
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
endif()
