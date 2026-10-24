add_executable(test_proto_smoke test_proto_smoke.cc)
add_dependencies(test_proto_smoke greptimedb_proto_codegen)
target_link_libraries(test_proto_smoke PRIVATE
  greptimedb::proto grpcxx::h2 greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.library.proto.smoke COMMAND test_proto_smoke)
set_tests_properties(unit.library.proto.smoke PROPERTIES LABELS "unit;library;proto;smoke")
