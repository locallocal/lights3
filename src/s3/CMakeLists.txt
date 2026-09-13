# L2: the S3 protocol surface -- routing, error model, XML, the request pipeline and
# the stores behind the bucket subresources (docs/architecture/s3-protocol.md)
target_sources(lights3_core PRIVATE
  errors.cc
  xml.cc
  router.cc
  service.cc
  metrics.cc
  website_store.cc
  cors_store.cc
  tls_identity_store.cc
  lifecycle.cc
  audit.cc
  quota.cc
  tenant.cc
  usage.cc
  ratelimit.cc
)

add_subdirectory(auth)
add_subdirectory(handlers)
