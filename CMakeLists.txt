cmake_minimum_required(VERSION 3.20)
project(lights3 CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  # 必须进缓存：seastar 子目录会 set(CMAKE_BUILD_TYPE ... CACHE)，若此处只是
  # 普通变量，会被其自家默认值（Debug + sanitizers）顶掉。CMake 启动时会预置
  # 一个空的 CMAKE_BUILD_TYPE 缓存项，不带 FORCE 的 set(CACHE) 覆盖不了它
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()
add_compile_options(-Wall -Wextra)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# gflags：命令行参数解析（git submodule，静态链接进主程序）
set(GFLAGS_BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_PACKAGING OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/gflags EXCLUDE_FROM_ALL)

# HTTP 驱动的编译期裁剪（docs/architecture.md §6 依赖策略）
option(LIGHTS3_DRIVER_BUILTIN "Build the built-in POSIX socket HTTP driver" ON)
option(LIGHTS3_DRIVER_BEAST "Build the Boost.Beast async HTTP driver (requires Boost headers)" ON)
option(LIGHTS3_DRIVER_HTTPLIB "Build the cpp-httplib sync HTTP driver (vendored header)" ON)
option(LIGHTS3_DRIVER_SEASTAR "Build the Seastar shard-per-core HTTP driver (heavy deps)" OFF)
option(LIGHTS3_CLOUDPROXY "Build the cloudproxy storage backend (self-signed SigV4 + vendored httplib, docs/cloudproxy-backend.md)" ON)
option(LIGHTS3_DUOSTORE "Build the duostore storage backend (RocksDB metadata + chunk/pack data engine, docs/duostore-backend.md)" ON)
option(LIGHTS3_DUOSTORE_REDIS_META "Build the Redis-backed IMetaStore for duostore (hiredis submodule, docs/duostore-redis-meta.md)" OFF)
option(LIGHTS3_BUILD_TESTS "Build unit tests" ON)

if(LIGHTS3_DUOSTORE)
  # RocksDB 裁剪预设（docs/duostore-backend.md §13.3，仿 gflags/seastar 模板）
  set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)      # 仓库已有 gflags 目标；仅 rocksdb 工具需要——OFF 防目标冲突
  set(WITH_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_TRACE_TOOLS OFF CACHE BOOL "" FORCE)
  set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)      # 压缩全关：元数据体量小，换零外部依赖
  set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
  set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
  set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
  set(WITH_LIBURING OFF CACHE BOOL "" FORCE)    # 机器无 liburing 开发头（seastar 同款注释）
  set(PORTABLE ON CACHE BOOL "" FORCE)          # 不用 -march=native
  set(USE_RTTI 1 CACHE BOOL "" FORCE)           # lights3 全程异常 + RTTI
  set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
  # add_subdirectory 的 SYSTEM 关键字需 CMake >= 3.25（本项目最低 3.20）；
  # 老版本会把 SYSTEM 误解析为 binary_dir 参数，必须按版本分支
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
  # hiredis 裁剪预设（docs/duostore-redis-meta.md §7.2）
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)  # hiredis 默认 shared；全仓惯例静态
  set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
  set(ENABLE_SSL OFF CACHE BOOL "" FORCE)         # 首期不启用 TLS（§5.5）
  set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
  if(CMAKE_VERSION VERSION_LESS 3.25)
    add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL)
  else()
    add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL SYSTEM)
  endif()
endif()

if(LIGHTS3_DRIVER_SEASTAR)
  # seastar 依赖较重（编译版 Boost、fmt、c-ares、lz4、yaml-cpp、protobuf、ragel、
  # xfs 头）；无 root 的机器可解包到 ~/.local/opt/seastar-deps（apt-get download + dpkg -x）
  if(EXISTS "$ENV{HOME}/.local/opt/seastar-deps")
    list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local/opt/seastar-deps")
    list(APPEND CMAKE_PROGRAM_PATH "$ENV{HOME}/.local/opt/seastar-deps/bin")
  endif()
  if(EXISTS "$ENV{HOME}/.local/opt/boost-1.90")
    list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local/opt/boost-1.90")
  endif()
  # spdlog 自带的 bundled fmt(v12) 与 seastar 用的 fmt(v11) 的 include 守卫同名，
  # 同一 TU 里先到者屏蔽后者，产生 ABI 撕裂的未定义符号——seastar 开启时全项目
  # 统一用 seastar 那份外部 fmt
  set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
endif()

# spdlog：日志实现（git submodule，静态库；core/log.h 为门面）
add_subdirectory(third_party/spdlog EXCLUDE_FROM_ALL)

# nlohmann/json：admin 凭证 API 的 JSON 编解码（header-only，docs/credential-management.md §5.4）
add_subdirectory(third_party/json EXCLUDE_FROM_ALL)

# ---- 核心库：L2/L3/L4 + HTTP 边界 ----
add_library(lights3_core STATIC
  src/core/thread_pool.cc
  src/core/timer.cc
  src/core/config.cc
  src/core/util/crypto.cc
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
# JSON 只在 admin handler / CredentialStore 的 .cc 里用，不进公共头（docs/credential-management.md §5.4）
target_link_libraries(lights3_core PRIVATE nlohmann_json::nlohmann_json)

if(LIGHTS3_DRIVER_BUILTIN)
  target_sources(lights3_core PRIVATE src/http/drivers/builtin/builtin_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_BUILTIN)
endif()

if(LIGHTS3_DRIVER_BEAST)
  # Beast/Asio 均为 header-only；BOOST_ROOT 或系统路径均可
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
endif()

if(LIGHTS3_CLOUDPROXY)
  target_sources(lights3_core PRIVATE
    src/storage/cloudproxy/remote_client.cc
    src/storage/cloudproxy/cloudproxy_backend.cc
  )
  # OPENSSL_SUPPORT 必须目标级统一：httplib_server.cc 与 cloudproxy 的 TU 一开一关
  # 是 ODR 违规（docs/cloudproxy-backend.md §8.3）
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_CLOUDPROXY CPPHTTPLIB_OPENSSL_SUPPORT)
  target_include_directories(lights3_core SYSTEM PRIVATE third_party)
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
endif()

if(LIGHTS3_DUOSTORE)
  # 裁剪模板照 cloudproxy（docs/duostore-backend.md §13.3）
  target_sources(lights3_core PRIVATE
    src/storage/duostore/codec.cc
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
endif()

if(LIGHTS3_DRIVER_SEASTAR)
  # beast 块的 find_package(Boost QUIET) 走老式 FindBoost 时会把
  # Boost_USE_DEBUG_RUNTIME 默认置 TRUE，污染 BoostConfig 的变体选择
  set(Boost_USE_DEBUG_RUNTIME OFF)
  # 顶层的 -Wall -Wextra 会漏进子目录，撞上 seastar 自己的 -Werror；
  # 用 seastar 提供的旋钮把它的告警降回非致命
  set(Seastar_CXX_FLAGS "-Wno-error" CACHE STRING "" FORCE)
  set(Seastar_APPS OFF CACHE BOOL "" FORCE)
  set(Seastar_DEMOS OFF CACHE BOOL "" FORCE)
  set(Seastar_DOCS OFF CACHE BOOL "" FORCE)
  set(Seastar_TESTING OFF CACHE BOOL "" FORCE)
  set(Seastar_HWLOC OFF CACHE BOOL "" FORCE)
  set(Seastar_DPDK OFF CACHE BOOL "" FORCE)
  set(Seastar_IO_URING OFF CACHE BOOL "" FORCE)  # 机器无 liburing 开发头
  set(Seastar_LTTNG OFF CACHE BOOL "" FORCE)
  set(Seastar_INSTALL OFF CACHE BOOL "" FORCE)
  # seastar 子目录把 CMAKE_CXX_STANDARD 的缓存默认为 26；显式钉在与项目一致的 20
  set(CMAKE_CXX_STANDARD 20 CACHE STRING "" FORCE)
  add_subdirectory(third_party/seastar EXCLUDE_FROM_ALL SYSTEM)
  # 进程里 reactor 之外还有自有线程池，跨线程 new/free 常态化：
  # 强制 seastar 用系统分配器，绕开其自带 allocator 的线程归属约束
  target_compile_definitions(seastar PUBLIC SEASTAR_DEFAULT_ALLOCATOR)
  target_sources(lights3_core PRIVATE src/http/drivers/seastar/seastar_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_SEASTAR)
  target_link_libraries(lights3_core PRIVATE seastar)
endif()

# ---- 主程序 ----
add_executable(lights3 src/main.cc)
target_link_libraries(lights3 PRIVATE lights3_core gflags::gflags)

# ---- 测试 ----
if(LIGHTS3_BUILD_TESTS)
  enable_testing()
  add_executable(unit_tests
    tests/unit/main.cc
    tests/unit/test_task.cc
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
  )
  if(LIGHTS3_DUOSTORE)
    target_sources(unit_tests PRIVATE tests/unit/test_duostore.cc)
    if(LIGHTS3_DUOSTORE_REDIS_META)
      # redis 用例需 hiredis 头（测试 harness 直连拉起的私有 redis-server）
      target_sources(unit_tests PRIVATE tests/unit/test_duostore_redis.cc)
      target_link_libraries(unit_tests PRIVATE hiredis::hiredis)
    endif()
  endif()
  target_link_libraries(unit_tests PRIVATE lights3_core nlohmann_json::nlohmann_json)
  target_include_directories(unit_tests PRIVATE tests)
  add_test(NAME unit_tests COMMAND unit_tests)

  # e2e 按驱动参数化：同一组真实 S3 流程用例跑每个已编译驱动
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
  # xlocalfs 后端复用同一组 e2e 用例（io_uring 数据面）
  if(LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_xlocalfs
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin xlocalfs)
    # tiered 组合后端（localfs+memory，docs/tiered-storage.md）同样跑全协议用例
    add_test(NAME e2e_tiered
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin tiered)
  endif()
  # cloudproxy 双实例场景（docs/cloudproxy-backend.md §10）：实例 B 充当"云端"，A 经 cloudproxy 代理；
  # tiered-cloudproxy 即 docs/tiered-storage.md P5 组合（tiered 的 cloud 侧接真实 CloudProxyBackend）
  # duostore 后端（RocksDB meta + chunk/pack 数据面）跑同一组 e2e 用例
  if(LIGHTS3_DUOSTORE AND LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_duostore
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin duostore)
    # duostore + Redis meta（docs/duostore-redis-meta.md §9）：脚本内探测
    # redis-server，缺则显式 SKIP
    if(LIGHTS3_DUOSTORE_REDIS_META)
      add_test(NAME e2e_duostore_redis
               COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                       $<TARGET_FILE:lights3> builtin duostore-redis)
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
