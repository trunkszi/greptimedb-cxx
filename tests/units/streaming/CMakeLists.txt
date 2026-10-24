add_executable(test_streaming_sessions test_streaming_sessions.cc test_streaming_edges.cc)
add_dependencies(test_streaming_sessions greptimedb_proto_codegen)
target_link_libraries(test_streaming_sessions PRIVATE
  greptimedb::streaming greptimedb::transport greptimedb::flight_ipc
  greptimedb_compile_options GTest::gtest_main)
add_test(NAME integration.streaming.sessions COMMAND test_streaming_sessions)
set_tests_properties(integration.streaming.sessions PROPERTIES
  LABELS "integration;library;streaming;scenario;lifecycle" TIMEOUT 60)
