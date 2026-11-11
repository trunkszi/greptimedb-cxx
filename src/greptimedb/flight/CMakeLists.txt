cmake_minimum_required(VERSION 4.0)

add_library(greptimedb_flight_ipc STATIC
  ipc_flatbuffers_encode.cc
  ipc_flatbuffers_decode.cc
  ipc_codec_columns.cc
  ipc_codec_encode.cc
  ipc_codec_decode.cc
  ipc_compression.cc
  ipc_views.cc)
add_library(greptimedb::flight_ipc ALIAS greptimedb_flight_ipc)
target_link_libraries(greptimedb_flight_ipc PUBLIC greptimedb::proto
  greptimedb_dep::lz4 greptimedb_dep::zstd)
