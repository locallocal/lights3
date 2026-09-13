# Monitoring assets (roadmap §5.5, docs/usage/monitoring.md §5): rules / scrape config /
# dashboard validated against the source metric catalog and a live scrape
find_program(LIGHTS3_PYTHON3 python3)
if(LIGHTS3_DRIVER_BUILTIN AND LIGHTS3_PYTHON3)
  add_test(NAME monitoring_assets
           COMMAND ${LIGHTS3_PYTHON3} ${CMAKE_CURRENT_SOURCE_DIR}/check_assets.py
                   --repo ${PROJECT_SOURCE_DIR} --binary $<TARGET_FILE:lights3>)
endif()
