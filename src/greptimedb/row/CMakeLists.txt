add_library(greptimedb_row INTERFACE)
add_library(greptimedb::row ALIAS greptimedb_row)
target_include_directories(greptimedb_row INTERFACE
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_row INTERFACE
  greptimedb::batch greptimedb::proto greptimedb_compile_options)
