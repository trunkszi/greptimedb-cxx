find_package(benchmark CONFIG REQUIRED)

add_executable(bench_column_buf bench_column_buf.cc)
target_link_libraries(bench_column_buf PRIVATE
  greptimedb::batch benchmark::benchmark_main greptimedb_compile_options)

foreach(suite IN ITEMS reflected_row arrow_ipc_codec streaming_loopback)
  add_executable(bench_${suite} bench_${suite}.cc)
  target_link_libraries(bench_${suite} PRIVATE
    greptimedb::row greptimedb::flight_ipc benchmark::benchmark_main
    greptimedb_compile_options)
endforeach()
target_link_libraries(bench_streaming_loopback PRIVATE greptimedb::streaming)
