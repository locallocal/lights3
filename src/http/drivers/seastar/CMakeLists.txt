# The engine itself is configured here rather than in cmake/Dependencies.cmake: it has
# to be added *after* the beast driver's find_package(Boost) (see the first comment
# below). The prerequisites that must precede spdlog -- prefix paths and
# SPDLOG_FMT_EXTERNAL -- do live in cmake/Dependencies.cmake
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
  add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/seastar ${PROJECT_BINARY_DIR}/third_party/seastar
                   EXCLUDE_FROM_ALL SYSTEM)
  # The process has its own thread pools besides the reactor, making cross-thread
  # new/free routine: force seastar onto the system allocator, bypassing its own
  # allocator's thread-ownership constraints
  target_compile_definitions(seastar PUBLIC SEASTAR_DEFAULT_ALLOCATOR)
  target_sources(lights3_core PRIVATE seastar_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_SEASTAR)
  target_link_libraries(lights3_core PRIVATE seastar)
endif()
