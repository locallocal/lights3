# S3 Tables: the Iceberg REST catalog, its maintenance jobs and the S3-plane guard
# over table buckets (docs/architecture/s3-tables-design.md)
if(LIGHTS3_TABLES)
  target_sources(lights3_core PRIVATE
    identifier.cc
    object_catalog_store.cc
    table_bucket_store.cc
    catalog.cc
    rest_api.cc
    bucket_guard.cc
    diagnostics.cc
    fsck.cc
    maintenance.cc
    maintenance_runner.cc
  )
  # the Iceberg format itself (metadata, updates, snapshots, Avro, views)
  add_subdirectory(iceberg)
  if(LIGHTS3_DUOSTORE)
    # the duostore-meta catalog backing (docs/architecture/s3-tables-design.md §12)
    target_sources(lights3_core PRIVATE duo_meta_catalog_store.cc)
  endif()
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_TABLES)
  # deflate-coded Avro manifests need zlib (docs/architecture/s3-tables-design.md §7.4); without it the
  # deep check skips such snapshots and reports "skipped-codec"
  find_package(ZLIB QUIET)
  if(ZLIB_FOUND)
    target_link_libraries(lights3_core PRIVATE ZLIB::ZLIB)
    target_compile_definitions(lights3_core PRIVATE LIGHTS3_TABLES_ZLIB)
    message(STATUS "tables: zlib found, deflate Avro manifests are validated")
  else()
    message(STATUS "tables: zlib not found, deflate Avro manifests are skipped at validation")
  endif()
endif()
