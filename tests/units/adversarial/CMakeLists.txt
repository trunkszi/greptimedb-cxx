add_executable(test_adversarial_regressions test_adversarial_regressions.cc)
add_dependencies(test_adversarial_regressions greptimedb_proto_codegen)
target_link_libraries(test_adversarial_regressions PRIVATE
  greptimedb::batch greptimedb::row greptimedb::flight_ipc
  greptimedb::streaming greptimedb::transport
  greptimedb_compile_options GTest::gtest_main)
target_compile_definitions(test_adversarial_regressions PRIVATE
  ADVERSARIAL_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../flight/fixtures")
add_test(NAME unit.adversarial.regressions COMMAND test_adversarial_regressions)
set_tests_properties(unit.adversarial.regressions PROPERTIES
  LABELS "unit;adversarial;regression;scenario" TIMEOUT 60)
