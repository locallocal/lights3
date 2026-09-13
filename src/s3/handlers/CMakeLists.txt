# One translation unit per API family; the admin plane (/-/admin/*) sits next to the
# S3 operations because both are dispatched by the same router
target_sources(lights3_core PRIVATE
  admin_credentials.cc
  admin_tenants.cc
  admin_objects.cc
  admin_fsck.cc
  admin_jobs.cc
  admin_tls_identities.cc
  bucket_quota.cc
  quota_gate.cc
  bucket_website.cc
  bucket_cors.cc
  bucket_lifecycle.cc
  sts.cc
  buckets.cc
  objects.cc
  list_objects.cc
  multipart.cc
)
