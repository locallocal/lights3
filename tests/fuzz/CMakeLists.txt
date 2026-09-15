# Fuzz harnesses (docs/development/testing.md §3): one LLVMFuzzerTestOneInput
# per parser reachable without authentication. Default = standalone binaries
# replaying tests/fuzz/corpus/<name>/ (a crash-regression gate under any
# compiler / sanitizer); LIGHTS3_FUZZ_LIBFUZZER=ON (clang) links libFuzzer and
# the same sources become real fuzzers (first directory = libFuzzer's working
# corpus, keep it out of the tree): build-fuzz/fuzz_xml build-fuzz/corpus-xml tests/fuzz/corpus/xml
set(LIGHTS3_FUZZ_TARGETS xml uri http_parse sigv4 aws_chunked)
if(LIGHTS3_DUOSTORE)
  list(APPEND LIGHTS3_FUZZ_TARGETS duostore_codec)
endif()
foreach(fz IN LISTS LIGHTS3_FUZZ_TARGETS)
  add_executable(fuzz_${fz} fuzz_${fz}.cc)
  target_link_libraries(fuzz_${fz} PRIVATE lights3_core)
  target_include_directories(fuzz_${fz} PRIVATE ${PROJECT_SOURCE_DIR}/tests)
  # build-fuzz/fuzz_<name> is the documented invocation (testing.md §3)
  set_target_properties(fuzz_${fz} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
  if(LIGHTS3_FUZZ_LIBFUZZER)
    target_compile_options(fuzz_${fz} PRIVATE -fsanitize=fuzzer)
    target_link_options(fuzz_${fz} PRIVATE -fsanitize=fuzzer)
  else()
    target_sources(fuzz_${fz} PRIVATE fuzz_driver.cc)
    add_test(NAME fuzz_regression_${fz}
             COMMAND fuzz_${fz} ${CMAKE_CURRENT_SOURCE_DIR}/corpus/${fz})
    set_tests_properties(fuzz_regression_${fz} PROPERTIES LABELS fuzz)
  endif()
endforeach()
