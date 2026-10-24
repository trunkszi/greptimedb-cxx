find_path(GREPTIMEDB_NGHTTP2_INCLUDE_DIR nghttp2/nghttp2.h REQUIRED)
find_library(GREPTIMEDB_NGHTTP2_LIBRARY NAMES nghttp2 REQUIRED)

add_library(greptimedb_transport
  endpoint_transport.cc
  transport_options.cc
  tls_transport.cc
  tls_options.cc
  synchronous_channel.cc
  channel_h2.cc
  mock_grpc_server.cc
  in_memory_channel_pair.cc)
add_library(greptimedb::transport ALIAS greptimedb_transport)
target_include_directories(greptimedb_transport
  PUBLIC "${PROJECT_SOURCE_DIR}/src"
  PRIVATE "${GREPTIMEDB_NGHTTP2_INCLUDE_DIR}")
target_link_libraries(greptimedb_transport
  PUBLIC greptimedb_compile_options greptimedb::core grpcxx::core grpcxx::h2 grpcxx::streaming
    grpcxx::compression grpcxx::tls grpcxx::uring
  PRIVATE "${GREPTIMEDB_NGHTTP2_LIBRARY}")
