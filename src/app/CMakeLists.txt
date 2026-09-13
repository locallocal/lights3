# Process assembly: wiring the stores, backends, HTTP server and admin job framework
# into a running gateway (docs/architecture/overview.md §4)
target_sources(lights3_core PRIVATE
  app.cc
  admin_jobs.cc
)
