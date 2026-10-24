add_library(greptimedb_streaming STATIC
  stream_inserter.cc
  ../flight/bulk_writer.cc
  ../flight/query_stream.cc)
add_library(greptimedb::streaming ALIAS greptimedb_streaming)
add_dependencies(greptimedb_streaming greptimedb_proto_codegen)
target_link_libraries(greptimedb_streaming PUBLIC
  greptimedb::core greptimedb::transport greptimedb::flight_ipc
  greptimedb::proto greptimedb_compile_options)

add_library(greptimedb_client STATIC client.cc ddl.cc prom.cc)
add_library(greptimedb::client ALIAS greptimedb_client)
target_include_directories(greptimedb_client PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_client PUBLIC
  greptimedb::core greptimedb::batch greptimedb::transport greptimedb::proto
  greptimedb::streaming
  greptimedb_compile_options)
