find_path(GREPTIMEDB_LZ4_INCLUDE_DIR lz4frame.h REQUIRED)
find_library(GREPTIMEDB_LZ4_LIBRARY NAMES lz4 REQUIRED)
find_path(GREPTIMEDB_ZSTD_INCLUDE_DIR zstd.h REQUIRED)
find_library(GREPTIMEDB_ZSTD_LIBRARY NAMES zstd REQUIRED)
add_library(greptimedb_flight_ipc STATIC ipc_flatbuffers.cc ipc_codec.cc ipc_compression.cc ipc_views.cc)
add_library(greptimedb::flight_ipc ALIAS greptimedb_flight_ipc)
target_include_directories(greptimedb_flight_ipc PRIVATE
  "${GREPTIMEDB_LZ4_INCLUDE_DIR}" "${GREPTIMEDB_ZSTD_INCLUDE_DIR}")
target_link_libraries(greptimedb_flight_ipc PUBLIC greptimedb::proto
  "${GREPTIMEDB_LZ4_LIBRARY}" "${GREPTIMEDB_ZSTD_LIBRARY}")
