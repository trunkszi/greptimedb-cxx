add_executable(test_column_buf_and_buffer_policy test_column_buf_and_buffer_policy.cc)
add_dependencies(test_column_buf_and_buffer_policy greptimedb_proto_codegen)
target_link_libraries(test_column_buf_and_buffer_policy PRIVATE
  greptimedb::batch greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.library.batch.column_buf_and_buffer_policy
  COMMAND test_column_buf_and_buffer_policy)
set_tests_properties(unit.library.batch.column_buf_and_buffer_policy
  PROPERTIES LABELS "unit;library;batch;scenario;lifecycle")

add_executable(test_columnar_and_row_batch test_columnar_and_row_batch.cc)
add_dependencies(test_columnar_and_row_batch greptimedb_proto_codegen)
target_link_libraries(test_columnar_and_row_batch PRIVATE
  greptimedb::batch greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.library.batch.columnar_and_row COMMAND test_columnar_and_row_batch)
set_tests_properties(unit.library.batch.columnar_and_row
  PROPERTIES LABELS "unit;library;batch;scenario;lifecycle")

add_executable(test_atomic_append test_atomic_append.cc)
target_link_libraries(test_atomic_append PRIVATE
  greptimedb::batch greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.library.batch.atomic_append COMMAND test_atomic_append)
set_tests_properties(unit.library.batch.atomic_append
  PROPERTIES LABELS "unit;library;batch;allocation")
