add_executable(test_docker_greptimedb_e2e test_docker_greptimedb_e2e.cc)
add_dependencies(test_docker_greptimedb_e2e greptimedb_proto_codegen)
find_package(Threads REQUIRED)
target_link_libraries(test_docker_greptimedb_e2e PRIVATE
  greptimedb::client greptimedb::row greptimedb::streaming
  greptimedb_compile_options GTest::gtest_main Threads::Threads)
foreach(optional_target greptimedb::failover greptimedb::threading)
  if(TARGET ${optional_target})
    target_link_libraries(test_docker_greptimedb_e2e PRIVATE ${optional_target})
  endif()
endforeach()
add_test(NAME e2e.greptimedb.docker COMMAND test_docker_greptimedb_e2e)
set_tests_properties(e2e.greptimedb.docker PROPERTIES
  LABELS "e2e;integration;docker;live" TIMEOUT 600 RESOURCE_LOCK greptimedb_cxx_e2e
  SKIP_REGULAR_EXPRESSION "\\[  SKIPPED \\]")
