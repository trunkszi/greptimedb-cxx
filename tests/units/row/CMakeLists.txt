add_executable(test_reflected_row test_reflected_row.cc test_multi_table_reflected.cc
  test_row_hardening.cc)
target_link_libraries(test_reflected_row PRIVATE
  greptimedb::row greptimedb_compile_options GTest::gtest_main)
add_test(NAME unit.library.row.reflected COMMAND test_reflected_row)
set_tests_properties(unit.library.row.reflected PROPERTIES LABELS "unit;library;row;lifecycle;scenario")
