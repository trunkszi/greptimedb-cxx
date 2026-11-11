cmake_minimum_required(VERSION 4.0)

add_library(greptimedb_batch STATIC column_buf.cc columnar_batch.cc row_batch.cc)
add_library(greptimedb::batch ALIAS greptimedb_batch)
target_include_directories(greptimedb_batch PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_batch PUBLIC greptimedb::core)

# Leaf probes stay out of the installed SDK. The root export list does not name this target.
add_library(greptimedb_probes STATIC batch_row_probes.cc)
add_library(greptimedb::probes ALIAS greptimedb_probes)
target_include_directories(greptimedb_probes PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_probes PUBLIC uarch::uarch PRIVATE greptimedb_compile_options)
target_compile_definitions(greptimedb_probes PRIVATE NDEBUG)
target_compile_options(greptimedb_probes PRIVATE
  -O3 -fno-lto -fno-rtti -fno-sanitize=all -fno-unwind-tables -fno-asynchronous-unwind-tables)
set_target_properties(greptimedb_probes PROPERTIES EXCLUDE_FROM_ALL FALSE)

add_executable(bench_hot_probes ${PROJECT_SOURCE_DIR}/benches/bench_hot_probes.cc)
target_link_libraries(bench_hot_probes PRIVATE greptimedb::probes greptimedb_compile_options)
target_compile_definitions(bench_hot_probes PRIVATE NDEBUG)

if(GREPTIMEDB_BUILD_TESTING)
  enable_testing()
  find_package(GTest CONFIG REQUIRED)
  add_subdirectory(${PROJECT_SOURCE_DIR}/tests/probes ${CMAKE_CURRENT_BINARY_DIR}/tests_probes)
endif()
