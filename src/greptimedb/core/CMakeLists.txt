add_library(greptimedb_core STATIC config.cc error.cc hints.cc types.cc)
add_library(greptimedb::core ALIAS greptimedb_core)
add_dependencies(greptimedb_core greptimedb_proto_codegen)
target_include_directories(greptimedb_core PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
target_link_libraries(greptimedb_core PUBLIC greptimedb::proto grpcxx::core)
if(TARGET greptimedb_compile_options)
  target_link_libraries(greptimedb_core PUBLIC greptimedb_compile_options)
endif()
