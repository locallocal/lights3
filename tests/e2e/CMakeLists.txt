# Everything that drives a real gateway process: the protocol suite per driver and
# backend, the mint compatibility subset, the S3 Tables client smoke test, and the
# performance gate / soak (their scripts live in scripts/, the cases belong here)

# MinIO mint compatibility suite (roadmap §6.1): needs docker; SKIP (77) without
# it. The s3cmd + awscli subset is the regression gate (docs/development/testing.md §6)
add_test(NAME mint
         COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_mint.sh $<TARGET_FILE:lights3> s3cmd awscli)
set_tests_properties(mint PROPERTIES SKIP_RETURN_CODE 77 LABELS mint TIMEOUT 3600)

# Performance gate + short soak (roadmap §6.1, docs/development/testing.md §5): lights3-ctl bench
# against a memory-backend gateway with throughput / latency floors, and a
# 30-second soak asserting RSS / fd stability. Both labeled so `ctest -LE perf`
# skips them on a loaded box
if(LIGHTS3_DRIVER_BUILTIN)
  add_test(NAME bench_gate
           COMMAND ${PROJECT_SOURCE_DIR}/scripts/bench_gate.sh $<TARGET_FILE:lights3> $<TARGET_FILE:lights3-ctl> --duration 3)
  set_tests_properties(bench_gate PROPERTIES LABELS perf TIMEOUT 300)
  add_test(NAME soak_smoke
           COMMAND ${PROJECT_SOURCE_DIR}/scripts/soak.sh $<TARGET_FILE:lights3> $<TARGET_FILE:lights3-ctl> --seconds 30)
  set_tests_properties(soak_smoke PROPERTIES LABELS "perf;soak" TIMEOUT 900)
endif()

# S3 Tables client smoke (docs/architecture/s3-tables-design.md §13): PyIceberg
# and DuckDB against a live gateway. Opt-in through
# LIGHTS3_TABLES_SMOKE=1 (python packages required); SKIP (77) otherwise
if(LIGHTS3_TABLES AND LIGHTS3_DRIVER_BUILTIN)
  add_test(NAME tables_smoke
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_tables_smoke.sh $<TARGET_FILE:lights3>)
  set_tests_properties(tables_smoke PROPERTIES SKIP_RETURN_CODE 77 LABELS tables-smoke TIMEOUT 900)
endif()

# e2e parameterized by driver: the same set of real S3 flow cases runs against every compiled driver
if(LIGHTS3_DRIVER_BUILTIN)
  add_test(NAME e2e_builtin
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh $<TARGET_FILE:lights3> builtin)
endif()
if(LIGHTS3_DRIVER_BEAST)
  add_test(NAME e2e_beast
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh $<TARGET_FILE:lights3> beast)
endif()
if(LIGHTS3_DRIVER_HTTPLIB)
  add_test(NAME e2e_httplib
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh $<TARGET_FILE:lights3> httplib)
endif()
if(LIGHTS3_DRIVER_SEASTAR)
  add_test(NAME e2e_seastar
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh $<TARGET_FILE:lights3> seastar)
endif()
# The xlocalfs backend reuses the same e2e cases (io_uring data plane)
if(LIGHTS3_DRIVER_BUILTIN)
  add_test(NAME e2e_xlocalfs
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin xlocalfs)
  # The tiered composite backend (localfs+memory, docs/architecture/storage/tiered-design.md) also runs the full protocol cases
  add_test(NAME e2e_tiered
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin tiered)
endif()
# cloudproxy two-instance scenario (docs/architecture/storage/cloudproxy-design.md §10): instance B acts as the "cloud", A proxies through cloudproxy;
# tiered-cloudproxy is the docs/architecture/storage/tiered-design.md P5 combination (tiered's cloud side wired to a real CloudProxyBackend)
# The duostore backend (RocksDB meta + chunk/pack data plane) runs the same e2e cases
if(LIGHTS3_DUOSTORE AND LIGHTS3_DRIVER_BUILTIN)
  add_test(NAME e2e_duostore
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin duostore)
  # duostore with the io_uring fs data plane (roadmap §3.4 ⑤); the backend falls back
  # to synchronous IO where io_uring is unavailable, so the case stays green everywhere
  add_test(NAME e2e_duostore_uring
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin duostore-uring)
  # tiered combination (docs/architecture/storage/duostore-design.md §13.1/§15 P5): duostore as tiered's
  # cloud side (cloud goes only through the IStorageBackend abstraction, re-validating the component relationship)
  add_test(NAME e2e_tiered_duostore
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin tiered-duostore)
  # duostore as the tiered *local* side (roadmap §3.6 ⑥): tier state in the object
  # record, stubs without extents; the full protocol suite over that composition
  add_test(NAME e2e_tiered_duolocal
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin tiered-duolocal)
  # duostore + Redis meta (docs/architecture/storage/duostore-meta-redis-design.md §9): the script probes for
  # redis-server, explicit SKIP if missing
  if(LIGHTS3_DUOSTORE_REDIS_META)
    add_test(NAME e2e_duostore_redis
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin duostore-redis)
  endif()
  # duostore + SQLite meta (docs/architecture/storage/duostore-meta-sqlite-design.md §9): zero external deps
  if(LIGHTS3_DUOSTORE_SQLITE_META)
    add_test(NAME e2e_duostore_sqlite
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin duostore-sqlite)
  endif()
  # duostore + RADOS data (docs/architecture/storage/duostore-data-rados-design.md §11): the script probes
  # LIGHTS3_TEST_RADOS_CONF/POOL, explicit SKIP if missing (real cluster dependency)
  if(LIGHTS3_DUOSTORE_RADOS_DATA)
    add_test(NAME e2e_duostore_rados
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin duostore-rados)
  endif()
  # duostore + TiKV meta (docs/architecture/storage/duostore-meta-tikv-design.md §10): the script probes
  # LIGHTS3_TEST_PD_ADDR, explicit SKIP if missing (real cluster dependency)
  if(LIGHTS3_DUOSTORE_TIKV_META)
    add_test(NAME e2e_duostore_tikv
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                     $<TARGET_FILE:lights3> builtin duostore-tikv)
  endif()
endif()
if(LIGHTS3_CLOUDPROXY AND LIGHTS3_DRIVER_BUILTIN)
  add_test(NAME e2e_cloudproxy
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin cloudproxy)
  add_test(NAME e2e_tiered_cloudproxy
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_e2e.sh
                   $<TARGET_FILE:lights3> builtin tiered-cloudproxy)
endif()
