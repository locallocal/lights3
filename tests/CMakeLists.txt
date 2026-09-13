# Test targets and their ctest registrations, one directory per kind: unit_tests and
# the fuzz harnesses are built here, the rest register scripts that drive a real
# gateway binary. enable_testing() is called by the top-level CMakeLists.txt
add_subdirectory(unit)
add_subdirectory(fuzz)
add_subdirectory(e2e)
add_subdirectory(monitoring)
add_subdirectory(packaging)
