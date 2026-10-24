#pragma once

/** Public GreptimeDB SDK, including optional failover and worker pools. */
#include <greptimedb/core/buffer_policy.h>
#include <greptimedb/core/config.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/hints.h>
#include <greptimedb/core/types.h>
#include <greptimedb/proto/greptime_proto.h>

#include <greptimedb/batch/batch_encode.h>
#include <greptimedb/batch/column_buf.h>
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/row/annotations.h>
#include <greptimedb/row/reflected_column.h>
#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>

#include <greptimedb/client/client.h>
#include <greptimedb/client/ddl.h>
#include <greptimedb/client/prom.h>
#include <greptimedb/client/scope.h>
#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/ipc_codec.h>
#include <greptimedb/flight/ipc_flatbuffers.h>
#include <greptimedb/flight/query_stream.h>
#include <greptimedb/transport/endpoint_transport.h>
#include <greptimedb/transport/in_memory_channel_pair.h>
#include <greptimedb/transport/mock_grpc_server.h>
#include <greptimedb/transport/synchronous_channel.h>
#include <greptimedb/transport/tls_options.h>

#if __has_include(<greptimedb/failover/failover.h>)
#include <greptimedb/failover/failover.h>
#endif
#if __has_include(<greptimedb/threading/thread_local_pool.h>)
#include <greptimedb/threading/thread_local_pool.h>
#endif
#if __has_include(<greptimedb/threading/worker_pool.h>)
#include <greptimedb/threading/worker_pool.h>
#endif
