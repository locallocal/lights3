# Reference backend: one file per object plus a sidecar, on any POSIX filesystem
target_sources(lights3_core PRIVATE
  fs_util.cc
  list_cache.cc
  localfs_backend.cc
)
