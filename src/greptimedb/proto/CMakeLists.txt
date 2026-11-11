cmake_minimum_required(VERSION 4.0)

include("${PROJECT_SOURCE_DIR}/cmake/GreptimeProtoGen.cmake")
greptimedb_generate_proto_library()
