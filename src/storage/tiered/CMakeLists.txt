# Composite backend: a local tier in front of a cloud tier
# (docs/architecture/storage/tiered-design.md)
target_sources(lights3_core PRIVATE
  tier_local_fs.cc
  tiered_backend.cc
)

if(LIGHTS3_DUOSTORE)
  # duostore as the tiered *local* side: tier state lives in the
  # object record, so this translation unit needs the duostore engine
  target_sources(lights3_core PRIVATE tier_local_duo.cc)
endif()
