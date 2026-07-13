# CMake generated Testfile for 
# Source directory: /home/earthwind/project/lights3
# Build directory: /home/earthwind/project/lights3/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[unit_tests]=] "/home/earthwind/project/lights3/build/unit_tests")
set_tests_properties([=[unit_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/earthwind/project/lights3/CMakeLists.txt;95;add_test;/home/earthwind/project/lights3/CMakeLists.txt;0;")
add_test([=[e2e_builtin]=] "/home/earthwind/project/lights3/tests/e2e/run_e2e.sh" "/home/earthwind/project/lights3/build/lights3" "builtin")
set_tests_properties([=[e2e_builtin]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/earthwind/project/lights3/CMakeLists.txt;99;add_test;/home/earthwind/project/lights3/CMakeLists.txt;0;")
add_test([=[e2e_beast]=] "/home/earthwind/project/lights3/tests/e2e/run_e2e.sh" "/home/earthwind/project/lights3/build/lights3" "beast")
set_tests_properties([=[e2e_beast]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/earthwind/project/lights3/CMakeLists.txt;103;add_test;/home/earthwind/project/lights3/CMakeLists.txt;0;")
add_test([=[e2e_httplib]=] "/home/earthwind/project/lights3/tests/e2e/run_e2e.sh" "/home/earthwind/project/lights3/build/lights3" "httplib")
set_tests_properties([=[e2e_httplib]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/earthwind/project/lights3/CMakeLists.txt;107;add_test;/home/earthwind/project/lights3/CMakeLists.txt;0;")
