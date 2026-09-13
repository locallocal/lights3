cmake_minimum_required(VERSION 3.20)
project(lights3 VERSION 0.1.0 LANGUAGES CXX)
include(GNUInstallDirs)

# Layout: this file owns the project-wide settings (standard, warnings, feature
# options) and nothing else. Targets are declared by the directory that owns their
# sources -- src/ builds lights3_core plus the two executables, one CMakeLists.txt per
# module directory appending its own translation units; tests/ declares the test
# targets and the ctest registrations. Third-party projects are configured in
# cmake/Dependencies.cmake, which documents why they cannot be spread out.

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  # Must go into the cache: the seastar subdirectory does set(CMAKE_BUILD_TYPE ... CACHE),
  # and a plain variable here would be clobbered by its own default (Debug + sanitizers).
  # CMake pre-seeds an empty CMAKE_BUILD_TYPE cache entry at startup, and a
  # set(CACHE) without FORCE cannot override it
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()
add_compile_options(-Wall -Wextra)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# Compile-time trimming of HTTP drivers (dependency policy, docs/architecture/overview.md §6)
option(LIGHTS3_DRIVER_BUILTIN "Build the built-in POSIX socket HTTP driver" ON)
option(LIGHTS3_DRIVER_BEAST "Build the Boost.Beast async HTTP driver (requires Boost headers)" ON)
option(LIGHTS3_DRIVER_HTTPLIB "Build the cpp-httplib sync HTTP driver (vendored header)" ON)
option(LIGHTS3_DRIVER_SEASTAR "Build the Seastar shard-per-core HTTP driver (heavy deps)" OFF)
option(LIGHTS3_CLOUDPROXY "Build the cloudproxy storage backend (self-signed SigV4 + vendored httplib, docs/architecture/storage/cloudproxy-design.md)" ON)
option(LIGHTS3_DUOSTORE "Build the duostore storage backend (RocksDB metadata + chunk/pack data engine, docs/architecture/storage/duostore-design.md)" ON)
option(LIGHTS3_DUOSTORE_REDIS_META "Build the Redis-backed IMetaStore for duostore (hiredis submodule, docs/architecture/storage/duostore-meta-redis-design.md)" OFF)
option(LIGHTS3_DUOSTORE_SQLITE_META "Build the SQLite-backed IMetaStore for duostore (sqlite submodule, docs/architecture/storage/duostore-meta-sqlite-design.md)" OFF)
option(LIGHTS3_DUOSTORE_RADOS_DATA "Build the Ceph/RADOS-backed IDataStore for duostore (system librados, docs/architecture/storage/duostore-data-rados-design.md)" OFF)
option(LIGHTS3_DUOSTORE_TIKV_META "Build the TiKV-backed IMetaStore for duostore (client-c submodule, docs/architecture/storage/duostore-meta-tikv-design.md)" OFF)
option(LIGHTS3_TABLES "Build the S3 Tables / Iceberg REST catalog (docs/architecture/s3-tables-design.md)" ON)
option(LIGHTS3_BUILD_TESTS "Build unit tests" ON)
option(LIGHTS3_FUZZ_LIBFUZZER "Link the fuzz harnesses against libFuzzer (clang, -fsanitize=fuzzer); off = standalone corpus replay (docs/development/testing.md §3)" OFF)

include(cmake/Dependencies.cmake)

add_subdirectory(src)

if(LIGHTS3_BUILD_TESTS)
  # enable_testing() belongs to the top level: ctest driven from the build root only
  # walks subdirectories registered under an enabled top-level directory
  enable_testing()
  add_subdirectory(tests)
endif()

# ---- Install tree + packages (roadmap §6.3) ----
include(cmake/Packaging.cmake)
