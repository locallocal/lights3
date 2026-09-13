# One binary for every unit case (mini_test.h); optional suites follow the feature
# options, and the ones that need a third-party header of their own link it here
add_executable(unit_tests
  main.cc
  test_task.cc
  test_timer.cc
  test_metrics.cc
  test_concurrency.cc
  test_config.cc
  test_sigv4.cc
  test_credentials.cc
  test_xml.cc
  test_storage.cc
  test_tiered.cc
  test_cloudproxy.cc
  test_service.cc
  test_tenancy.cc
  test_tls.cc
  test_tls_identity.cc
  test_ratelimit.cc
  test_reload.cc
  test_admin_jobs.cc
  test_api_metrics.cc
  test_access_log.cc
  test_trace.cc
  test_fault.cc
  test_admin_inspect.cc
  test_http_drivers.cc
  test_admission.cc
)
if(LIGHTS3_TABLES)
  target_sources(unit_tests PRIVATE
    test_tables_iceberg.cc
    test_tables_avro.cc
    test_tables_catalog.cc
    test_tables_rest.cc
    test_tables_maintenance.cc
    test_tables_multi_gateway.cc
    test_tables_optional.cc
  )
endif()
if(LIGHTS3_DUOSTORE)
  target_sources(unit_tests PRIVATE test_duostore.cc)
  if(LIGHTS3_DUOSTORE_REDIS_META)
    # redis cases need hiredis headers (the test harness connects directly to the private redis-server it spawns)
    target_sources(unit_tests PRIVATE test_duostore_redis.cc)
    target_link_libraries(unit_tests PRIVATE hiredis::hiredis)
  endif()
  if(LIGHTS3_DUOSTORE_SQLITE_META)
    # sqlite cases need sqlite3.h (the test harness directly builds "someone else's database" to verify lineage rejection)
    target_sources(unit_tests PRIVATE test_duostore_sqlite.cc)
    target_link_libraries(unit_tests PRIVATE lights3_sqlite3)
  endif()
  if(LIGHTS3_DUOSTORE_RADOS_DATA)
    # rados cases need librados headers (namespace cleanup + fault injection connect to
    # the cluster directly); without a cluster all cases SKIP explicitly (docs/architecture/storage/duostore-data-rados-design.md §11)
    target_sources(unit_tests PRIVATE test_duostore_rados.cc)
    target_include_directories(unit_tests SYSTEM PRIVATE ${LIGHTS3_RADOS_INCLUDE})
  endif()
  if(LIGHTS3_DUOSTORE_TIKV_META)
    # tikv cases need a real PD/TiKV cluster (LIGHTS3_TEST_PD_ADDR), otherwise explicit
    # SKIP (docs/architecture/storage/duostore-meta-tikv-design.md §10); dedicated cases talk to kv_client directly
    # for guard/conflict verification
    target_sources(unit_tests PRIVATE test_duostore_tikv.cc)
    target_link_libraries(unit_tests PRIVATE kv_client)
  endif()
endif()
target_link_libraries(unit_tests PRIVATE lights3_core nlohmann_json::nlohmann_json)
target_include_directories(unit_tests PRIVATE ${PROJECT_SOURCE_DIR}/tests)
if(LIGHTS3_CLOUDPROXY)
  # test_cloudproxy drives ClientPool/RemoteContext directly (remote_client.h includes httplib)
  target_include_directories(unit_tests SYSTEM PRIVATE ${PROJECT_SOURCE_DIR}/third_party)
endif()
# `./build/unit_tests` is how the suite is run by hand and in docs/development/testing.md
set_target_properties(unit_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
add_test(NAME unit_tests COMMAND unit_tests)
