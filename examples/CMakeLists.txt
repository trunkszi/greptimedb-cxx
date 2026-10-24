find_package(fmt CONFIG REQUIRED)
find_package(Threads REQUIRED)

set(greptimedb_examples
  01_quickstart_unary_ddl_and_crud
  02_reflected_row_and_zero_copy_views
  03_memory_policy_and_batch_reuse
  04_full_column_type_matrix
  05_stream_inserter_and_backpressure
  06_arrow_flight_doput_bulk_writer
  07_arrow_flight_doget_and_promql
  08_thread_local_and_worker_striping
  09_tls_and_failover_circuit_breaker
  10_hft_market_data_pipeline)

foreach(example IN LISTS greptimedb_examples)
  add_executable(example_${example} "${example}.cc")
  target_link_libraries(example_${example} PRIVATE
    greptimedb::row greptimedb::streaming greptimedb_compile_options)
  if(example MATCHES "^(01_|07_|08_|09_)")
    target_link_libraries(example_${example} PRIVATE
      greptimedb::client fmt::fmt-header-only)
  endif()
  if(GREPTIMEDB_BUILD_TESTING)
    add_test(NAME example.${example} COMMAND example_${example})
    set_tests_properties(example.${example} PROPERTIES
      LABELS "integration;example;offline" TIMEOUT 30
      ENVIRONMENT "GREPTIMEDB_ENDPOINT=;GREPTIMEDB_CA_PEM=")
  endif()
endforeach()

target_link_libraries(example_08_thread_local_and_worker_striping PRIVATE
  greptimedb::threading Threads::Threads)
target_link_libraries(example_09_tls_and_failover_circuit_breaker PRIVATE
  greptimedb::failover)
target_compile_definitions(example_09_tls_and_failover_circuit_breaker PRIVATE
  GREPTIMEDB_EXAMPLE_TLS_DIR="${PROJECT_SOURCE_DIR}/tests/units/transport/fixtures")
