# SigV4 verification and the credential store behind it
# (docs/architecture/credential-management.md)
target_sources(lights3_core PRIVATE
  sigv4.cc
  credential_store.cc
)
