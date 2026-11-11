cmake_minimum_required(VERSION 4.0)

add_executable(test_probes test_probes.cc)
add_dependencies(test_probes greptimedb_proto_codegen)
target_link_libraries(test_probes PRIVATE
  greptimedb::probes greptimedb::batch greptimedb::row greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.library.probes.hot COMMAND test_probes)
set_tests_properties(unit.library.probes.hot PROPERTIES
  LABELS "unit;library;probes;scenario")

add_test(NAME unit.library.probes.smoke
  COMMAND bench_hot_probes --probe null_mask --iterations 0000000032)
set_tests_properties(unit.library.probes.smoke PROPERTIES
  LABELS "unit;library;probes;smoke"
  PASS_REGULAR_EXPRESSION "checksum=0*[1-9][0-9]*")
