# Iceberg format handling, independent of the catalog: metadata documents, the
# update/requirement algebra, the snapshot graph, the read-only Avro decoder and the
# gzip reader for engine-written metadata (design §7)
target_sources(lights3_core PRIVATE
  metadata.cc
  requirements.cc
  updates.cc
  transition.cc
  snapshots.cc
  avro_reader.cc
  gzip.cc
  manifest.cc
  view_metadata.cc
)
