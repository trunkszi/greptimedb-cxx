add_executable(test_ipc_codec test_ipc_codec.cc test_ipc_adversarial.cc test_ipc_views.cc)
add_dependencies(test_ipc_codec greptimedb_proto_codegen)
target_include_directories(test_ipc_codec PRIVATE
  "${GREPTIMEDB_LZ4_INCLUDE_DIR}" "${GREPTIMEDB_ZSTD_INCLUDE_DIR}")
target_compile_definitions(test_ipc_codec PRIVATE
  IPC_VIEW_REFERENCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
  IPC_VIEW_OUTPUT_DIR="${CMAKE_CURRENT_BINARY_DIR}/interop")
target_link_libraries(test_ipc_codec PRIVATE
  greptimedb::flight_ipc greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.flight.ipc.scenario COMMAND test_ipc_codec)
set_tests_properties(unit.flight.ipc.scenario PROPERTIES LABELS "unit;flight;ipc;scenario")

add_executable(test_ipc_interop test_ipc_interop.cc)
target_link_libraries(test_ipc_interop PRIVATE greptimedb::flight_ipc GTest::gtest_main)
target_compile_definitions(test_ipc_interop PRIVATE
  IPC_REFERENCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
  IPC_REFERENCE_OUTPUT_DIR="${CMAKE_CURRENT_BINARY_DIR}/interop")
add_test(NAME integration.flight.ipc.interop COMMAND test_ipc_interop)
set_tests_properties(integration.flight.ipc.interop PROPERTIES LABELS "integration;flight;ipc;interop")
