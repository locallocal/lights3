# Command groups of the `lights3` binary: one cli_<group>.cc per group, each
# exporting make_<group>() for the root command in ../main.cc
target_sources(lights3 PRIVATE
  cli_common.cc
  cli_server.cc
  cli_tier.cc
  cli_fsck.cc
)

if(LIGHTS3_TABLES)
  target_sources(lights3 PRIVATE cli_tables.cc)
endif()
if(LIGHTS3_DUOSTORE)
  target_sources(lights3 PRIVATE cli_duostore.cc)
endif()
