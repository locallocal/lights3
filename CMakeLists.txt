cmake_minimum_required(VERSION 3.20)
project(lights3 CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
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

# spdlog：日志实现（git submodule，静态库；core/log.h 为门面）
add_subdirectory(third_party/spdlog EXCLUDE_FROM_ALL)

# HTTP 驱动的编译期裁剪（docs/01 §6 依赖策略）
option(LIGHTS3_DRIVER_BUILTIN "Build the built-in POSIX socket HTTP driver" ON)
option(LIGHTS3_DRIVER_BEAST "Build the Boost.Beast async HTTP driver (requires Boost headers)" ON)
option(LIGHTS3_DRIVER_HTTPLIB "Build the cpp-httplib sync HTTP driver (vendored header)" ON)
option(LIGHTS3_BUILD_TESTS "Build unit tests" ON)

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
  src/storage/xlocalfs/uring.cc
  src/storage/xlocalfs/xlocalfs_backend.cc
)
target_include_directories(lights3_core PUBLIC src)
target_link_libraries(lights3_core PUBLIC OpenSSL::Crypto Threads::Threads spdlog::spdlog)

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
    tests/unit/test_xml.cc
    tests/unit/test_storage.cc
    tests/unit/test_service.cc
    tests/unit/test_http_drivers.cc
  )
  target_link_libraries(unit_tests PRIVATE lights3_core)
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
  # xlocalfs 后端复用同一组 e2e 用例（io_uring 数据面）
  if(LIGHTS3_DRIVER_BUILTIN)
    add_test(NAME e2e_xlocalfs
             COMMAND ${CMAKE_SOURCE_DIR}/tests/e2e/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin xlocalfs)
  endif()
endif()
