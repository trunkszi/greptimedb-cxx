find_package(OpenSSL REQUIRED)
add_executable(test_transport_and_tls
  test_transport_and_tls.cc
  test_rpc_shapes.cc
  test_connect_facade.cc
  test_transport_hardening.cc
  test_protocol_faults.cc
  test_transport_options.cc)
target_link_libraries(test_transport_and_tls PRIVATE
  greptimedb::transport greptimedb_compile_options GTest::gtest_main OpenSSL::SSL)
find_path(GREPTIMEDB_TEST_NGHTTP2_INCLUDE_DIR nghttp2/nghttp2.h REQUIRED)
target_include_directories(test_transport_and_tls PRIVATE "${GREPTIMEDB_TEST_NGHTTP2_INCLUDE_DIR}")
target_compile_definitions(test_transport_and_tls PRIVATE
  GREPTIMEDB_TLS_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
  GREPTIMEDB_TEST_SCRATCH_DIR="${PROJECT_SOURCE_DIR}/.scratch/transport-runtime")
add_test(NAME unit.library.transport.tls COMMAND test_transport_and_tls)
set_tests_properties(unit.library.transport.tls PROPERTIES LABELS "unit;library;transport;tls" TIMEOUT 90)
