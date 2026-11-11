cmake_minimum_required(VERSION 4.0)

find_package(Threads REQUIRED)
add_executable(test_failover_integration test_failover_integration.cc)
target_link_libraries(test_failover_integration PRIVATE greptimedb::threading
  grpcxx::sync GTest::gtest_main Threads::Threads)
add_test(NAME integration.failover.threading COMMAND test_failover_integration)
set_tests_properties(integration.failover.threading PROPERTIES TIMEOUT 20 LABELS "integration;failover;threading;lifecycle")
