add_library(greptimedb_failover STATIC failover.cc)
add_library(greptimedb::failover ALIAS greptimedb_failover)

target_include_directories(greptimedb_failover PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_failover PUBLIC
  greptimedb_compile_options
  grpcxx::core greptimedb::core)
