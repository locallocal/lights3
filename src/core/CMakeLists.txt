# Execution substrate shared by every layer: coroutine scheduling, timers, config,
# logging, tracing, metrics, fault injection (docs/architecture/concurrency.md).
# version.cc is added with the build stamp in ../CMakeLists.txt
target_sources(lights3_core PRIVATE
  background.cc
  thread_pool.cc
  timer.cc
  config.cc
  log.cc
  trace.cc
  fault.cc
  metrics.cc
  util/crypto.cc
  util/checksum.cc
  util/uri.cc
  util/time.cc
)
