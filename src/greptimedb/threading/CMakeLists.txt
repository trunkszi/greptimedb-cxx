add_library(greptimedb_threading INTERFACE)
add_library(greptimedb::threading ALIAS greptimedb_threading)
target_include_directories(greptimedb_threading INTERFACE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(greptimedb_threading INTERFACE greptimedb::failover greptimedb::client greptimedb_compile_options)
