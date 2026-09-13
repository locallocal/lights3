# L3: the backend abstraction, bucket routing and the pieces every backend shares
# (docs/architecture/storage/storage-backend.md). Each backend is a subdirectory;
# the optional ones gate on their own option
target_sources(lights3_core PRIVATE
  validate.cc
  listing.cc
  multipart.cc
  registry.cc
  bucket_router.cc
  metered_backend.cc
)

add_subdirectory(localfs)
add_subdirectory(memory)
add_subdirectory(xlocalfs)
add_subdirectory(tiered)
add_subdirectory(cloudproxy)
add_subdirectory(duostore)
