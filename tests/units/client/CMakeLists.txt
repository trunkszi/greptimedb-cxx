add_executable(test_unary_client_operations
  test_unary_client_operations.cc test_client_session_factories.cc test_client_audit.cc)
add_dependencies(test_unary_client_operations greptimedb_proto_codegen)
target_link_libraries(test_unary_client_operations PRIVATE
  greptimedb::client greptimedb_compile_options GTest::gtest_main)
add_test(NAME integration.client.unary.scenario COMMAND test_unary_client_operations
  --gtest_filter=UnaryClientIntegrationScenario.*)
set_tests_properties(integration.client.unary.scenario PROPERTIES
  LABELS "integration;client;unary;scenario")
add_test(NAME unit.client.unary.scenario COMMAND test_unary_client_operations
  --gtest_filter=UnaryClientUnitScenario.*)
set_tests_properties(unit.client.unary.scenario PROPERTIES
  LABELS "unit;client;unary;scenario")

add_executable(test_ddl_and_prom test_ddl_and_prom.cc)
add_dependencies(test_ddl_and_prom greptimedb_proto_codegen)
target_link_libraries(test_ddl_and_prom PRIVATE
  greptimedb::client greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.client.ddl_and_prom.scenario COMMAND test_ddl_and_prom)
set_tests_properties(unit.client.ddl_and_prom.scenario PROPERTIES
  LABELS "unit;client;ddl;prom;scenario")
