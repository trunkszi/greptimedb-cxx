add_library(greptimedb_batch STATIC column_buf.cc columnar_batch.cc row_batch.cc)
add_library(greptimedb::batch ALIAS greptimedb_batch)
target_include_directories(greptimedb_batch PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_batch PUBLIC greptimedb::core)
