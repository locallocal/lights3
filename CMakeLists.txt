cmake_minimum_required(VERSION 3.20)
project(lights3 CXX)

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

# gflags: command-line flag parsing (git submodule, statically linked into the main binary)
set(GFLAGS_BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_PACKAGING OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/gflags EXCLUDE_FROM_ALL)

# Compile-time trimming of HTTP drivers (dependency policy, docs/architecture.md §6)
option(LIGHTS3_DRIVER_BUILTIN "Build the built-in POSIX socket HTTP driver" ON)
option(LIGHTS3_DRIVER_BEAST "Build the Boost.Beast async HTTP driver (requires Boost headers)" ON)
option(LIGHTS3_DRIVER_HTTPLIB "Build the cpp-httplib sync HTTP driver (vendored header)" ON)
option(LIGHTS3_DRIVER_SEASTAR "Build the Seastar shard-per-core HTTP driver (heavy deps)" OFF)
option(LIGHTS3_CLOUDPROXY "Build the cloudproxy storage backend (self-signed SigV4 + vendored httplib, docs/cloudproxy-backend.md)" ON)
option(LIGHTS3_DUOSTORE "Build the duostore storage backend (RocksDB metadata + chunk/pack data engine, docs/duostore-backend.md)" ON)
option(LIGHTS3_DUOSTORE_REDIS_META "Build the Redis-backed IMetaStore for duostore (hiredis submodule, docs/duostore-redis-meta.md)" OFF)
option(LIGHTS3_DUOSTORE_SQLITE_META "Build the SQLite-backed IMetaStore for duostore (sqlite submodule, docs/duostore-sqlite-meta.md)" OFF)
option(LIGHTS3_DUOSTORE_RADOS_DATA "Build the Ceph/RADOS-backed IDataStore for duostore (system librados, docs/duostore-rados-data.md)" OFF)
option(LIGHTS3_DUOSTORE_TIKV_META "Build the TiKV-backed IMetaStore for duostore (client-c submodule, docs/duostore-tikv-meta.md)" OFF)
option(LIGHTS3_BUILD_TESTS "Build unit tests" ON)

if(LIGHTS3_DUOSTORE)
  # RocksDB trimming presets (docs/duostore-backend.md §13.3, modeled on the gflags/seastar template)
  set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)      # the repo already has a gflags target; only rocksdb tools need it -- OFF avoids target clashes
  set(WITH_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_TRACE_TOOLS OFF CACHE BOOL "" FORCE)
  set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)      # all compression off: metadata is small, traded for zero external deps
  set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
  set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
  set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
  set(WITH_LIBURING OFF CACHE BOOL "" FORCE)    # machine has no liburing dev headers (same note as seastar)
  set(PORTABLE ON CACHE BOOL "" FORCE)          # no -march=native
  set(USE_RTTI 1 CACHE BOOL "" FORCE)           # lights3 uses exceptions + RTTI throughout
  set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
  # add_subdirectory's SYSTEM keyword needs CMake >= 3.25 (this project's minimum is 3.20);
  # older versions misparse SYSTEM as the binary_dir argument, so branch on version
  if(CMAKE_VERSION VERSION_LESS 3.25)
    add_subdirectory(third_party/rocksdb EXCLUDE_FROM_ALL)
  else()
    add_subdirectory(third_party/rocksdb EXCLUDE_FROM_ALL SYSTEM)
  endif()
endif()

if(LIGHTS3_DUOSTORE_REDIS_META)
  if(NOT LIGHTS3_DUOSTORE)
    message(FATAL_ERROR "LIGHTS3_DUOSTORE_REDIS_META requires LIGHTS3_DUOSTORE")
  endif()
  # hiredis trimming presets (docs/duostore-redis-meta.md §7.2)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)  # hiredis defaults to shared; repo-wide convention is static
  set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
  set(ENABLE_SSL OFF CACHE BOOL "" FORCE)         # TLS not enabled in the first phase (§5.5)
  set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
  if(CMAKE_VERSION VERSION_LESS 3.25)
    add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL)
  else()
    add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL SYSTEM)
  endif()
endif()

if(LIGHTS3_DUOSTORE_SQLITE_META)
  if(NOT LIGHTS3_DUOSTORE)
    message(FATAL_ERROR "LIGHTS3_DUOSTORE_SQLITE_META requires LIGHTS3_DUOSTORE")
  endif()
  # sqlite's canonical source tree ships no prebuilt amalgamation; it must be generated
  # via configure && make sqlite3.c (the script runs on tclsh, bootstrapping with the
  # bundled jimsh on a fresh tree without tclsh; docs/duostore-sqlite-meta.md §7).
  # Generated out-of-tree into the build dir; DEPENDS on VERSION makes a submodule
  # upgrade trigger regeneration
  set(SQLITE_GEN_DIR ${CMAKE_BINARY_DIR}/sqlite-amalgamation)
  add_custom_command(
    OUTPUT ${SQLITE_GEN_DIR}/sqlite3.c ${SQLITE_GEN_DIR}/sqlite3.h
    COMMAND ${CMAKE_COMMAND} -E make_directory ${SQLITE_GEN_DIR}
    COMMAND ${CMAKE_COMMAND} -E chdir ${SQLITE_GEN_DIR}
            ${CMAKE_SOURCE_DIR}/third_party/sqlite/configure
    COMMAND ${CMAKE_COMMAND} -E chdir ${SQLITE_GEN_DIR} make sqlite3.c
    DEPENDS ${CMAKE_SOURCE_DIR}/third_party/sqlite/VERSION
    COMMENT "Generating SQLite amalgamation (third_party/sqlite)"
    VERBATIM)
  # Compile-time options = trimmed set of the official Recommended Compile-time Options
  # (docs/duostore-sqlite-meta.md §5.1); THREADSAFE=1 (serialized) is defense in depth
  enable_language(C)  # the project itself is CXX-only; the amalgamation is the sole C translation unit
  add_library(lights3_sqlite3 STATIC ${SQLITE_GEN_DIR}/sqlite3.c)
  target_include_directories(lights3_sqlite3 PUBLIC ${SQLITE_GEN_DIR})
  target_compile_definitions(lights3_sqlite3 PRIVATE
    SQLITE_THREADSAFE=1 SQLITE_OMIT_LOAD_EXTENSION SQLITE_DQS=0
    SQLITE_DEFAULT_MEMSTATUS=0 SQLITE_LIKE_DOESNT_MATCH_BLOBS
    SQLITE_MAX_EXPR_DEPTH=0 SQLITE_OMIT_DEPRECATED
    SQLITE_OMIT_SHARED_CACHE SQLITE_USE_ALLOCA)
  target_compile_options(lights3_sqlite3 PRIVATE -w)  # third-party generated code is exempt from -Wall -Wextra
endif()

if(LIGHTS3_DUOSTORE_RADOS_DATA)
  if(NOT LIGHTS3_DUOSTORE)
    message(FATAL_ERROR "LIGHTS3_DUOSTORE_RADOS_DATA requires LIGHTS3_DUOSTORE")
  endif()
  # librados comes from the system package, not a submodule (the Ceph repo is several GiB,
  # docs/duostore-rados-data.md §9.1): prefer pkg-config, fall back to find_path/find_library;
  # without sudo, dpkg -x can unpack librados-dev/librados2 into ~/.local/opt/ceph,
  # or point LIGHTS3_RADOS_ROOT there explicitly
  find_package(PkgConfig QUIET)
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(RADOS QUIET rados)
  endif()
  if(RADOS_FOUND)
    set(LIGHTS3_RADOS_INCLUDE ${RADOS_INCLUDE_DIRS})
    list(GET RADOS_LINK_LIBRARIES 0 LIGHTS3_RADOS_LIB)
  else()
    find_path(LIGHTS3_RADOS_INCLUDE rados/librados.h
              HINTS "${LIGHTS3_RADOS_ROOT}/include" "${LIGHTS3_RADOS_ROOT}/usr/include"
                    "$ENV{HOME}/.local/opt/ceph/include" "$ENV{HOME}/.local/opt/ceph/usr/include")
    find_library(LIGHTS3_RADOS_LIB rados
                 HINTS "${LIGHTS3_RADOS_ROOT}/lib" "${LIGHTS3_RADOS_ROOT}/usr/lib"
                       "${LIGHTS3_RADOS_ROOT}/usr/lib/${CMAKE_LIBRARY_ARCHITECTURE}"
                       "$ENV{HOME}/.local/opt/ceph/lib" "$ENV{HOME}/.local/opt/ceph/usr/lib"
                       "$ENV{HOME}/.local/opt/ceph/usr/lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    if(NOT LIGHTS3_RADOS_INCLUDE OR NOT LIGHTS3_RADOS_LIB)
      message(FATAL_ERROR "LIGHTS3_DUOSTORE_RADOS_DATA=ON but librados not found; "
                          "install librados-dev or set LIGHTS3_RADOS_ROOT")
    endif()
  endif()
  # The deb layout installs the DT_NEEDED libceph-common into the private <libdir>/ceph
  # directory, and librados' own RUNPATH is the absolute path /usr/lib/... -- non-system
  # prefixes (~/.local unpacks) need extra rpath-link (link-time resolution of libceph-common
  # and its transitive deps: boost/rdmacm etc. unpacked under the same prefix)
  # plus rpath (runtime loading)
  get_filename_component(LIGHTS3_RADOS_LIBDIR ${LIGHTS3_RADOS_LIB} DIRECTORY)
  if(EXISTS "${LIGHTS3_RADOS_LIBDIR}/ceph" AND NOT LIGHTS3_RADOS_LIBDIR MATCHES "^/usr/lib")
    # --disable-new-dtags: emit DT_RPATH instead of DT_RUNPATH -- librados' own RUNPATH
    # points at absolute system paths, and when loading its NEEDED libceph-common the
    # executable's RUNPATH is not consulted (RUNPATH does not propagate); only the
    # old-style RPATH applies to the whole dependency tree
    set(LIGHTS3_RADOS_LINKOPTS
        "-Wl,--disable-new-dtags"
        "-Wl,-rpath-link,${LIGHTS3_RADOS_LIBDIR}/ceph"
        "-Wl,-rpath-link,${LIGHTS3_RADOS_LIBDIR}"
        "-Wl,-rpath,${LIGHTS3_RADOS_LIBDIR}/ceph"
        "-Wl,-rpath,${LIGHTS3_RADOS_LIBDIR}")
  endif()
endif()

if(LIGHTS3_DUOSTORE_TIKV_META)
  if(NOT LIGHTS3_DUOSTORE)
    message(FATAL_ERROR "LIGHTS3_DUOSTORE_TIKV_META requires LIGHTS3_DUOSTORE")
  endif()
  # client-c depends on gRPC/protobuf (build time additionally needs protoc +
  # grpc_cpp_plugin to generate kvproto) /Poco/abseil, taken from system packages;
  # without sudo, apt-get download + dpkg -x unpack into ~/.local/opt/tikv-deps
  # (docs/duostore-tikv-meta.md §8.2), or set LIGHTS3_TIKV_DEPS_ROOT explicitly to the
  # unpack prefix (<root>/bin/protoc, <root>/include, <root>/lib/<arch>). All of
  # client-c's dependency discovery is guarded by if(NOT ...), so pre-seeding variables
  # in the parent scope short-circuits its find modules wholesale
  if(NOT LIGHTS3_TIKV_DEPS_ROOT AND EXISTS "$ENV{HOME}/.local/opt/tikv-deps/usr")
    set(LIGHTS3_TIKV_DEPS_ROOT "$ENV{HOME}/.local/opt/tikv-deps/usr")
  endif()
  if(LIGHTS3_TIKV_DEPS_ROOT)
    set(LIGHTS3_TIKV_LIBDIR "${LIGHTS3_TIKV_DEPS_ROOT}/lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    set(LIGHTS3_TIKV_INCLUDE "${LIGHTS3_TIKV_DEPS_ROOT}/include")
    # The unpacked protoc / grpc_cpp_plugin depend at runtime on .so files from the same
    # tree -- generate LD_LIBRARY_PATH wrappers; what kvproto's codegen custom command
    # gets through the variable references is the wrapper
    foreach(tool protoc grpc_cpp_plugin)
      file(WRITE "${CMAKE_BINARY_DIR}/tikv-tools/${tool}"
           "#!/bin/sh\nexport LD_LIBRARY_PATH='${LIGHTS3_TIKV_LIBDIR}'\nexec '${LIGHTS3_TIKV_DEPS_ROOT}/bin/${tool}' \"$@\"\n")
      file(CHMOD "${CMAKE_BINARY_DIR}/tikv-tools/${tool}" PERMISSIONS
           OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE
           WORLD_READ WORLD_EXECUTE)
    endforeach()
    set(Protobuf_INCLUDE_DIR "${LIGHTS3_TIKV_INCLUDE}")
    set(Protobuf_LIBRARY "${LIGHTS3_TIKV_LIBDIR}/libprotobuf.so")
    set(Protobuf_PROTOC_EXECUTABLE "${CMAKE_BINARY_DIR}/tikv-tools/protoc")
    set(gRPC_FOUND TRUE)
    set(gRPC_INCLUDE_DIRS "${LIGHTS3_TIKV_INCLUDE}")
    set(gRPC_LIBRARIES
        "${LIGHTS3_TIKV_LIBDIR}/libgrpc++.so"
        "${LIGHTS3_TIKV_LIBDIR}/libgrpc.so"
        "${LIGHTS3_TIKV_LIBDIR}/libgpr.so")
    set(gRPC_CPP_PLUGIN "${CMAKE_BINARY_DIR}/tikv-tools/grpc_cpp_plugin")
    set(Poco_Foundation_LIBRARY "${LIGHTS3_TIKV_LIBDIR}/libPocoFoundation.so")
    set(Poco_Net_LIBRARY "${LIGHTS3_TIKV_LIBDIR}/libPocoNet.so")
    set(Poco_JSON_LIBRARY "${LIGHTS3_TIKV_LIBDIR}/libPocoJSON.so")
    set(Poco_Util_LIBRARY "${LIGHTS3_TIKV_LIBDIR}/libPocoUtil.so")
    # kvproto links absl::synchronization -- use the unpacked tree's absl config (same ABI as the unpacked grpc)
    find_package(absl REQUIRED CONFIG PATHS "${LIGHTS3_TIKV_LIBDIR}/cmake/absl" NO_DEFAULT_PATH)
    # Link-time resolution of grpc/Poco's NEEDED (absl/re2/cares unpacked in the same tree)
    # + runtime loading: RUNPATH does not propagate deep into the dependency tree, so the
    # old-style RPATH must apply to the whole tree (same argument as librados)
    set(LIGHTS3_TIKV_LINKOPTS
        "-Wl,--disable-new-dtags"
        "-Wl,-rpath-link,${LIGHTS3_TIKV_LIBDIR}"
        "-Wl,-rpath,${LIGHTS3_TIKV_LIBDIR}")
  else()
    # System-package route: find Poco first in the parent scope using client-c's bundled
    # FindPoco (its variables also short-circuit client-c's internal find_poco); absl uses
    # the system config. Restore the module path right after use (client-c's
    # FindgRPC/FindPoco and their bundled FPHSA must not leak into later find_package calls)
    set(LIGHTS3_SAVED_MODULE_PATH "${CMAKE_MODULE_PATH}")
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/third_party/client-c/cmake/Modules")
    find_package(Poco REQUIRED Foundation Net JSON Util)
    find_package(absl REQUIRED)
    set(CMAKE_MODULE_PATH "${LIGHTS3_SAVED_MODULE_PATH}")
    unset(LIGHTS3_SAVED_MODULE_PATH)
  endif()
  # Any non-empty value blocks client-c's built-in abseil-cpp submodule and kvproto's
  # find_abseil -- uniformly use the absl found above, avoiding a second absl copy
  # coexisting with the one grpc links
  set(ABSL_ROOT_DIR "/usr")
  set(ENABLE_TESTS OFF)  # client-c ships gtest cases (upstream #104: ON is known to break the build)
  if(CMAKE_VERSION VERSION_LESS 3.25)
    add_subdirectory(third_party/client-c EXCLUDE_FROM_ALL)
  else()
    add_subdirectory(third_party/client-c EXCLUDE_FROM_ALL SYSTEM)
  endif()
  # Upstream kv_client under-links/under-propagates: it links only PocoFoundation while
  # the pd/coprocessor sources actually use Net/JSON/Util symbols; the Poco includes
  # are also missing from its transitive include dirs
  target_link_libraries(kv_client ${Poco_Net_LIBRARY} ${Poco_JSON_LIBRARY} ${Poco_Util_LIBRARY})
  if(LIGHTS3_TIKV_DEPS_ROOT)
    target_include_directories(kv_client SYSTEM PUBLIC "${LIGHTS3_TIKV_INCLUDE}")
  endif()
  # Third-party sources are exempt from -Wall -Wextra (same convention as sqlite)
  target_compile_options(kv_client PRIVATE -w)
  target_compile_options(kvproto PRIVATE -w)
  target_compile_options(fiu PRIVATE -w)
  # The pre-seeded/short-circuit dependency variables serve only the client-c
  # subdirectory configure step (target properties are already baked in), so clear them
  # right after use -- Protobuf_*/gRPC_* in the top-level directory scope would
  # short-circuit dependency discovery in later sibling subdirectories (e.g. seastar's
  # SeastarDependencies going through module-mode FindProtobuf), silently causing
  # cross-dependency-tree ABI mixing
  unset(Protobuf_INCLUDE_DIR)
  unset(Protobuf_LIBRARY)
  unset(Protobuf_PROTOC_EXECUTABLE)
  unset(gRPC_FOUND)
  unset(gRPC_INCLUDE_DIRS)
  unset(gRPC_LIBRARIES)
  unset(gRPC_CPP_PLUGIN)
  unset(Poco_Foundation_LIBRARY)
  unset(Poco_Net_LIBRARY)
  unset(Poco_JSON_LIBRARY)
  unset(Poco_Util_LIBRARY)
  unset(ABSL_ROOT_DIR)
  unset(ENABLE_TESTS)
endif()

if(LIGHTS3_DRIVER_SEASTAR)
  # seastar's dependencies are heavy (compiled Boost, fmt, c-ares, lz4, yaml-cpp,
  # protobuf, ragel, xfs headers); machines without root can unpack them into
  # ~/.local/opt/seastar-deps (apt-get download + dpkg -x)
  if(EXISTS "$ENV{HOME}/.local/opt/seastar-deps")
    list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local/opt/seastar-deps")
    list(APPEND CMAKE_PROGRAM_PATH "$ENV{HOME}/.local/opt/seastar-deps/bin")
  endif()
  if(EXISTS "$ENV{HOME}/.local/opt/boost-1.90")
    list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local/opt/boost-1.90")
  endif()
  # spdlog's bundled fmt (v12) and the fmt seastar uses (v11) have identically named
  # include guards; within one TU whichever comes first shadows the other, producing
  # ABI-torn undefined symbols -- with seastar enabled the whole project uniformly
  # uses seastar's external fmt copy
  set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
endif()

# spdlog: logging implementation (git submodule, static lib; core/log.h is the facade)
add_subdirectory(third_party/spdlog EXCLUDE_FROM_ALL)

# nlohmann/json: JSON encode/decode for the admin credential API (header-only, docs/credential-management.md §5.4)
add_subdirectory(third_party/json EXCLUDE_FROM_ALL)

# ccmd: subcommand framework for s3adm (header-only + static cflag, ops tooling only)
add_subdirectory(third_party/ccmd EXCLUDE_FROM_ALL)

# ---- Core library: L2/L3/L4 + HTTP boundary ----
add_library(lights3_core STATIC
  src/app/app.cc
  src/core/background.cc
  src/core/thread_pool.cc
  src/core/timer.cc
  src/core/config.cc
  src/core/metrics.cc
  src/core/util/crypto.cc
  src/core/util/checksum.cc
  src/core/util/uri.cc
  src/core/util/time.cc
  src/http/server.cc
  src/s3/errors.cc
  src/s3/xml.cc
  src/s3/router.cc
  src/s3/service.cc
  src/s3/metrics.cc
  src/s3/auth/sigv4.cc
  src/s3/auth/credential_store.cc
  src/s3/handlers/admin_credentials.cc
  src/s3/handlers/buckets.cc
  src/s3/handlers/objects.cc
  src/s3/handlers/list_objects.cc
  src/s3/handlers/multipart.cc
  src/storage/validate.cc
  src/storage/listing.cc
  src/storage/multipart.cc
  src/storage/registry.cc
  src/storage/bucket_router.cc
  src/storage/localfs/fs_util.cc
  src/storage/localfs/localfs_backend.cc
  src/storage/memory/memory_backend.cc
  src/storage/tiered/tiered_backend.cc
  src/storage/xlocalfs/uring.cc
  src/storage/xlocalfs/xlocalfs_backend.cc
)
target_include_directories(lights3_core PUBLIC src)
target_link_libraries(lights3_core PUBLIC OpenSSL::Crypto Threads::Threads spdlog::spdlog)
# JSON is used only in the admin handler / CredentialStore .cc files, never in public headers (docs/credential-management.md §5.4)
target_link_libraries(lights3_core PRIVATE nlohmann_json::nlohmann_json)

if(LIGHTS3_DRIVER_BUILTIN)
  target_sources(lights3_core PRIVATE src/http/drivers/builtin/builtin_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_BUILTIN)
endif()

if(LIGHTS3_DRIVER_BEAST)
  # Beast/Asio are both header-only; BOOST_ROOT or the system path both work.
  # TLS (docs/gaps.md §7): asio::ssl needs libssl
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
  find_package(Boost 1.75 QUIET)
  if(NOT Boost_FOUND)
    find_path(LIGHTS3_BOOST_INCLUDE boost/beast.hpp
              HINTS "$ENV{BOOST_ROOT}" "$ENV{HOME}/.local/opt/boost-1.90")
    if(NOT LIGHTS3_BOOST_INCLUDE)
      message(FATAL_ERROR "LIGHTS3_DRIVER_BEAST=ON but Boost headers not found; "
                          "set BOOST_ROOT or install libboost-dev")
    endif()
  endif()
  target_sources(lights3_core PRIVATE src/http/drivers/beast/beast_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_BEAST)
  if(Boost_FOUND)
    target_link_libraries(lights3_core PRIVATE Boost::headers)
  else()
    target_include_directories(lights3_core SYSTEM PRIVATE ${LIGHTS3_BOOST_INCLUDE})
  endif()
endif()

if(LIGHTS3_DRIVER_HTTPLIB)
  target_sources(lights3_core PRIVATE src/http/drivers/httplib/httplib_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_HTTPLIB)
  target_include_directories(lights3_core SYSTEM PRIVATE third_party)
  # Server-side TLS (docs/gaps.md §7) needs SSLServer. The OPENSSL_SUPPORT macro must be
  # uniform target-wide (ODR, see the cloudproxy branch comment below), hence also defined PUBLIC here
  target_compile_definitions(lights3_core PUBLIC CPPHTTPLIB_OPENSSL_SUPPORT)
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
endif()

if(LIGHTS3_CLOUDPROXY)
  target_sources(lights3_core PRIVATE
    src/storage/cloudproxy/remote_client.cc
    src/storage/cloudproxy/cloudproxy_backend.cc
  )
  # OPENSSL_SUPPORT must be uniform target-wide: having it on in httplib_server.cc but
  # off in the cloudproxy TUs is an ODR violation (docs/cloudproxy-backend.md §8.3)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_CLOUDPROXY CPPHTTPLIB_OPENSSL_SUPPORT)
  target_include_directories(lights3_core SYSTEM PRIVATE third_party)
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
endif()

if(LIGHTS3_DUOSTORE)
  # Trimming template follows cloudproxy (docs/duostore-backend.md §13.3)
  target_sources(lights3_core PRIVATE
    src/storage/duostore/codec.cc
    src/storage/duostore/meta_dump.cc
    src/storage/duostore/rocks_meta_store.cc
    src/storage/duostore/fs_data_store.cc
    src/storage/duostore/duostore_backend.cc
  )
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE)
  target_link_libraries(lights3_core PRIVATE rocksdb)
  if(LIGHTS3_DUOSTORE_REDIS_META)
    target_sources(lights3_core PRIVATE src/storage/duostore/redis_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_REDIS_META)
    target_link_libraries(lights3_core PRIVATE hiredis::hiredis)
  endif()
  if(LIGHTS3_DUOSTORE_SQLITE_META)
    target_sources(lights3_core PRIVATE src/storage/duostore/sqlite_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_SQLITE_META)
    target_link_libraries(lights3_core PRIVATE lights3_sqlite3)
  endif()
  if(LIGHTS3_DUOSTORE_RADOS_DATA)
    target_sources(lights3_core PRIVATE src/storage/duostore/rados_data_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_RADOS_DATA)
    target_include_directories(lights3_core SYSTEM PRIVATE ${LIGHTS3_RADOS_INCLUDE})
    target_link_libraries(lights3_core PUBLIC ${LIGHTS3_RADOS_LIB})
    if(DEFINED LIGHTS3_RADOS_LINKOPTS)
      target_link_options(lights3_core PUBLIC ${LIGHTS3_RADOS_LINKOPTS})
    endif()
  endif()
  if(LIGHTS3_DUOSTORE_TIKV_META)
    target_sources(lights3_core PRIVATE
      src/storage/duostore/tikv_client.cc
      src/storage/duostore/tikv_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_TIKV_META)
    target_link_libraries(lights3_core PRIVATE kv_client)
    if(DEFINED LIGHTS3_TIKV_LINKOPTS)
      target_link_options(lights3_core PUBLIC ${LIGHTS3_TIKV_LINKOPTS})
    endif()
  endif()
endif()

if(LIGHTS3_DRIVER_SEASTAR)
  # When the beast block's find_package(Boost QUIET) goes through legacy FindBoost it
  # defaults Boost_USE_DEBUG_RUNTIME to TRUE, polluting BoostConfig's variant selection
  set(Boost_USE_DEBUG_RUNTIME OFF)
  # The top-level -Wall -Wextra leaks into subdirectories and collides with seastar's
  # own -Werror; use seastar's provided knob to demote its warnings back to non-fatal
  set(Seastar_CXX_FLAGS "-Wno-error" CACHE STRING "" FORCE)
  set(Seastar_APPS OFF CACHE BOOL "" FORCE)
  set(Seastar_DEMOS OFF CACHE BOOL "" FORCE)
  set(Seastar_DOCS OFF CACHE BOOL "" FORCE)
  set(Seastar_TESTING OFF CACHE BOOL "" FORCE)
  set(Seastar_HWLOC OFF CACHE BOOL "" FORCE)
  set(Seastar_DPDK OFF CACHE BOOL "" FORCE)
  set(Seastar_IO_URING OFF CACHE BOOL "" FORCE)  # machine has no liburing dev headers
  set(Seastar_LTTNG OFF CACHE BOOL "" FORCE)
  set(Seastar_INSTALL OFF CACHE BOOL "" FORCE)
  # The seastar subdirectory defaults the CMAKE_CXX_STANDARD cache to 26; pin it explicitly to 20 to match the project
  set(CMAKE_CXX_STANDARD 20 CACHE STRING "" FORCE)
  add_subdirectory(third_party/seastar EXCLUDE_FROM_ALL SYSTEM)
  # The process has its own thread pools besides the reactor, making cross-thread
  # new/free routine: force seastar onto the system allocator, bypassing its own
  # allocator's thread-ownership constraints
  target_compile_definitions(seastar PUBLIC SEASTAR_DEFAULT_ALLOCATOR)
  target_sources(lights3_core PRIVATE src/http/drivers/seastar/seastar_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_SEASTAR)
  target_link_libraries(lights3_core PRIVATE seastar)
endif()

# ---- Main executable ----
add_executable(lights3 src/main.cc)
target_link_libraries(lights3 PRIVATE lights3_core gflags::gflags)

# ---- Ops tooling ----
# s3adm: CLI client for /-/admin/credentials (docs/credential-management.md §2).
# CPPHTTPLIB_OPENSSL_SUPPORT propagates via lights3_core's PUBLIC definition, staying
# target-level consistent with the core library (ODR); in trimmed builds without
# httplib/cloudproxy it automatically degrades to http-only
add_executable(s3adm src/tools/s3adm.cc src/tools/s3adm_common.cc
                     src/tools/s3adm_cred.cc src/tools/s3adm_bench.cc)
target_link_libraries(s3adm PRIVATE lights3_core ccmd::ccmd
                                    nlohmann_json::nlohmann_json)
target_include_directories(s3adm SYSTEM PRIVATE third_party)

# ---- Tests ----
if(LIGHTS3_BUILD_TESTS)
  enable_testing()
  add_executable(unit_tests
    tests/unit/main.cc
    tests/unit/test_task.cc
    tests/unit/test_timer.cc
    tests/unit/test_metrics.cc
    tests/unit/test_concurrency.cc
    tests/unit/test_config.cc
    tests/unit/test_sigv4.cc
    tests/unit/test_credentials.cc
    tests/unit/test_xml.cc
    tests/unit/test_storage.cc
    tests/unit/test_tiered.cc
    tests/unit/test_cloudproxy.cc
    tests/unit/test_service.cc
    tests/unit/test_http_drivers.cc
    tests/unit/test_admission.cc
  )
  if(LIGHTS3_DUOSTORE)
    target_sources(unit_tests PRIVATE tests/unit/test_duostore.cc)
    if(LIGHTS3_DUOSTORE_REDIS_META)
      # redis cases need hiredis headers (the test harness connects directly to the private redis-server it spawns)
      target_sources(unit_tests PRIVATE tests/unit/test_duostore_redis.cc)
      target_link_libraries(unit_tests PRIVATE hiredis::hiredis)
    endif()
    if(LIGHTS3_DUOSTORE_SQLITE_META)
      # sqlite cases need sqlite3.h (the test harness directly builds "someone else's database" to verify lineage rejection)
      target_sources(unit_tests PRIVATE tests/unit/test_duostore_sqlite.cc)
      target_link_libraries(unit_tests PRIVATE lights3_sqlite3)
    endif()
    if(LIGHTS3_DUOSTORE_RADOS_DATA)
      # rados cases need librados headers (namespace cleanup + fault injection connect to
      # the cluster directly); without a cluster all cases SKIP explicitly (docs/duostore-rados-data.md §11)
      target_sources(unit_tests PRIVATE tests/unit/test_duostore_rados.cc)
      target_include_directories(unit_tests SYSTEM PRIVATE ${LIGHTS3_RADOS_INCLUDE})
    endif()
    if(LIGHTS3_DUOSTORE_TIKV_META)
      # tikv cases need a real PD/TiKV cluster (LIGHTS3_TEST_PD_ADDR), otherwise explicit
      # SKIP (docs/duostore-tikv-meta.md §10); dedicated cases talk to kv_client directly
      # for guard/conflict verification
      target_sources(unit_tests PRIVATE tests/unit/test_duostore_tikv.cc)
      target_link_libraries(unit_tests PRIVATE kv_client)
    endif()
  endif()
  target_link_libraries(unit_tests PRIVATE lights3_core nlohmann_json::nlohmann_json)
  target_include_directories(unit_tests PRIVATE tests)
  add_test(NAME unit_tests COMMAND unit_tests)

  # e2e parameterized by driver: the same set of real S3 flow cases runs against every compiled driver
  if(LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_builtin
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh $<TARGET_FILE:lights3> builtin)
  endif()
  if(LIGHTS3_DRIVER_BEAST)
    add_test(NAME e2e_beast
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh $<TARGET_FILE:lights3> beast)
  endif()
  if(LIGHTS3_DRIVER_HTTPLIB)
    add_test(NAME e2e_httplib
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh $<TARGET_FILE:lights3> httplib)
  endif()
  if(LIGHTS3_DRIVER_SEASTAR)
    add_test(NAME e2e_seastar
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh $<TARGET_FILE:lights3> seastar)
  endif()
  # The xlocalfs backend reuses the same e2e cases (io_uring data plane)
  if(LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_xlocalfs
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin xlocalfs)
    # The tiered composite backend (localfs+memory, docs/tiered-storage.md) also runs the full protocol cases
    add_test(NAME e2e_tiered
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin tiered)
  endif()
  # cloudproxy two-instance scenario (docs/cloudproxy-backend.md §10): instance B acts as the "cloud", A proxies through cloudproxy;
  # tiered-cloudproxy is the docs/tiered-storage.md P5 combination (tiered's cloud side wired to a real CloudProxyBackend)
  # The duostore backend (RocksDB meta + chunk/pack data plane) runs the same e2e cases
  if(LIGHTS3_DUOSTORE AND LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_duostore
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin duostore)
    # tiered combination (docs/duostore-backend.md §13.1/§15 P5): duostore as tiered's
    # cloud side (cloud goes only through the IStorageBackend abstraction, re-validating the component relationship)
    add_test(NAME e2e_tiered_duostore
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin tiered-duostore)
    # duostore + Redis meta (docs/duostore-redis-meta.md §9): the script probes for
    # redis-server, explicit SKIP if missing
    if(LIGHTS3_DUOSTORE_REDIS_META)
      add_test(NAME e2e_duostore_redis
               COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                       $<TARGET_FILE:lights3> builtin duostore-redis)
    endif()
    # duostore + SQLite meta (docs/duostore-sqlite-meta.md §9): zero external deps
    if(LIGHTS3_DUOSTORE_SQLITE_META)
      add_test(NAME e2e_duostore_sqlite
               COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                       $<TARGET_FILE:lights3> builtin duostore-sqlite)
    endif()
    # duostore + RADOS data (docs/duostore-rados-data.md §11): the script probes
    # LIGHTS3_TEST_RADOS_CONF/POOL, explicit SKIP if missing (real cluster dependency)
    if(LIGHTS3_DUOSTORE_RADOS_DATA)
      add_test(NAME e2e_duostore_rados
               COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                       $<TARGET_FILE:lights3> builtin duostore-rados)
    endif()
    # duostore + TiKV meta (docs/duostore-tikv-meta.md §10): the script probes
    # LIGHTS3_TEST_PD_ADDR, explicit SKIP if missing (real cluster dependency)
    if(LIGHTS3_DUOSTORE_TIKV_META)
      add_test(NAME e2e_duostore_tikv
               COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                       $<TARGET_FILE:lights3> builtin duostore-tikv)
    endif()
  endif()
  if(LIGHTS3_CLOUDPROXY AND LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_cloudproxy
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin cloudproxy)
    add_test(NAME e2e_tiered_cloudproxy
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin tiered-cloudproxy)
  endif()
endif()
