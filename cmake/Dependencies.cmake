# Third-party acquisition and trimming, in one place because the order matters.
#
# Why these blocks are not in the subdirectory that consumes them: every one of them
# configures another project, and the constraints run across features --
#   * the seastar preparation seeds SPDLOG_FMT_EXTERNAL, so it has to precede the
#     spdlog subdirectory (one fmt copy per process, see the block's comment);
#   * CMAKE_PREFIX_PATH / CMAKE_PROGRAM_PATH additions must be visible to every later
#     find_package, i.e. they belong to the top-level scope;
#   * the librados / client-c probes publish LIGHTS3_RADOS_* and LIGHTS3_TIKV_* to the
#     src/ *and* tests/ trees.
# Consuming a dependency (target_link_libraries, definitions, sources) does live with
# the code: src/storage/duostore, src/tables, src/http/drivers/*. The one exception is
# seastar, whose own add_subdirectory sits in src/http/drivers/seastar because it must
# run after the beast driver's Boost probe (the block there explains why).

if(LIGHTS3_DUOSTORE)
  # RocksDB trimming presets (docs/architecture/storage/duostore-design.md §13.3, modeled on the gflags/seastar template)
  set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)      # only rocksdb tools need gflags; the repo does not ship it
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
  # Third-party sources are exempt from -Wall -Wextra (same convention as sqlite /
  # client-c): the top-level flags leak into the subdirectory and surface upstream
  # -Wmaybe-uninitialized noise in the release build
  target_compile_options(rocksdb PRIVATE -w)
endif()

if(LIGHTS3_DUOSTORE_REDIS_META)
  if(NOT LIGHTS3_DUOSTORE)
    message(FATAL_ERROR "LIGHTS3_DUOSTORE_REDIS_META requires LIGHTS3_DUOSTORE")
  endif()
  # hiredis trimming presets (docs/architecture/storage/duostore-meta-redis-design.md §7.2)
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
  # bundled jimsh on a fresh tree without tclsh; docs/architecture/storage/duostore-meta-sqlite-design.md §7).
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
  # (docs/architecture/storage/duostore-meta-sqlite-design.md §5.1); THREADSAFE=1 (serialized) is defense in depth
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
  # docs/architecture/storage/duostore-data-rados-design.md §9.1): prefer pkg-config, fall back to find_path/find_library;
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
  # (docs/architecture/storage/duostore-meta-tikv-design.md §8.2), or set LIGHTS3_TIKV_DEPS_ROOT explicitly to the
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

# nlohmann/json: JSON encode/decode for the admin credential API (header-only, docs/architecture/credential-management.md §5.4)
add_subdirectory(third_party/json EXCLUDE_FROM_ALL)

# ccmd v0.0.2: command/subcommand framework (git submodule, header-only together with its
# cflag v0.0.2 submodule; needs --recursive init). Used by the lights3 entry point
# (src/main.cc + src/cli/: --config, `duostore ...`, `tier ...`, `fsck`) and the lights3-ctl ops CLI
add_subdirectory(third_party/ccmd EXCLUDE_FROM_ALL)

