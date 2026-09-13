# L1: the neutral HTTP model and the TLS material shared by the OpenSSL-backed
# drivers; each driver is a subdirectory gated by its own option
# (docs/architecture/http-adapter.md)
target_sources(lights3_core PRIVATE
  server.cc
  tls.cc
)

# Order matters: the beast driver probes Boost, and the seastar subdirectory has to
# see the result of that probe (its CMakeLists.txt explains what it repairs)
add_subdirectory(drivers/builtin)
add_subdirectory(drivers/beast)
add_subdirectory(drivers/httplib)
add_subdirectory(drivers/seastar)
