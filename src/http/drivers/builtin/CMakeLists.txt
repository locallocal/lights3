# Default driver: thread-per-connection over POSIX sockets, no third-party code
if(LIGHTS3_DRIVER_BUILTIN)
  target_sources(lights3_core PRIVATE builtin_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_BUILTIN)
endif()
