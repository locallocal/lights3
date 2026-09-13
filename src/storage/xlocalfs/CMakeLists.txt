# localfs with an io_uring data plane (raw syscalls, no liburing dependency)
target_sources(lights3_core PRIVATE
  uring.cc
  uring_stream.cc
  xlocalfs_backend.cc
)
