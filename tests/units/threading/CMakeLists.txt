cmake_minimum_required(VERSION 4.0)

find_package(Threads REQUIRED)
if(NOT TARGET nexora::nexora)
  find_package(nexora CONFIG QUIET)
endif()
if(NOT TARGET nexora::nexora)
  message(FATAL_ERROR "nexora::nexora is required for threading pool policy tests")
endif()
add_executable(test_threading_pools test_threading_pools.cc)
target_include_directories(test_threading_pools PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(test_threading_pools PRIVATE
  greptimedb::core greptimedb_compile_options nexora::nexora GTest::gtest_main Threads::Threads)
add_test(NAME unit.library.threading.pools COMMAND test_threading_pools)
set_tests_properties(unit.library.threading.pools PROPERTIES
  LABELS "unit;library;threading;pools;scenario;lifecycle" TIMEOUT 20)
