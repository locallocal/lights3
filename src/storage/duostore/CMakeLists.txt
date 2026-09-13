# Metadata/data split engine: a KV meta store (RocksDB by default, redis / sqlite /
# tikv optional) in front of a chunk+pack data store (local fs or RADOS).
# The external projects themselves are configured in cmake/Dependencies.cmake
if(LIGHTS3_DUOSTORE)
  target_sources(lights3_core PRIVATE
    codec.cc
    meta_dump.cc
    meta_backup.cc
    rocks_meta_store.cc
    fs_data_store.cc
    duostore_backend.cc
  )
  # ../tiered/tier_local_duo.cc is contributed by the tiered backend's own CMakeLists
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE)
  target_link_libraries(lights3_core PRIVATE rocksdb)
  if(LIGHTS3_DUOSTORE_REDIS_META)
    target_sources(lights3_core PRIVATE redis_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_REDIS_META)
    target_link_libraries(lights3_core PRIVATE hiredis::hiredis)
  endif()
  if(LIGHTS3_DUOSTORE_SQLITE_META)
    target_sources(lights3_core PRIVATE sqlite_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_SQLITE_META)
    target_link_libraries(lights3_core PRIVATE lights3_sqlite3)
  endif()
  if(LIGHTS3_DUOSTORE_RADOS_DATA)
    target_sources(lights3_core PRIVATE rados_data_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_RADOS_DATA)
    target_include_directories(lights3_core SYSTEM PRIVATE ${LIGHTS3_RADOS_INCLUDE})
    target_link_libraries(lights3_core PUBLIC ${LIGHTS3_RADOS_LIB})
    if(DEFINED LIGHTS3_RADOS_LINKOPTS)
      target_link_options(lights3_core PUBLIC ${LIGHTS3_RADOS_LINKOPTS})
    endif()
  endif()
  if(LIGHTS3_DUOSTORE_TIKV_META)
    target_sources(lights3_core PRIVATE
      tikv_client.cc
      tikv_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_TIKV_META)
    target_link_libraries(lights3_core PRIVATE kv_client)
    if(DEFINED LIGHTS3_TIKV_LINKOPTS)
      target_link_options(lights3_core PUBLIC ${LIGHTS3_TIKV_LINKOPTS})
    endif()
  endif()
endif()
