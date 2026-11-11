# SDK examples

Each executable performs runtime checks and exits nonzero on a failed result or
business invariant. Offline RPCs use real HTTP/2/gRPC peers through `grpcxx::InMemoryChannelPair`,
`nexora::MemoryPipe`, or loopback TCP and `grpcxx::SynchronousServer`. Each client advances its
server on the calling thread. No external database or server process is needed.
Example 08 creates three application OS workers; each owns and drives its own
TCP clients and server peers.

Build and run all ten examples with the SDK tests:

```sh
./build.py test -j 2
./build.py test --label example -j 2
```

Examples are built by default; configure `GREPTIMEDB_BUILD_EXAMPLES=OFF` to omit
them. Each CMake executable target starts with `example_`; each CTest name starts
with `example.`. CTest sets `GREPTIMEDB_ENDPOINT` and `GREPTIMEDB_CA_PEM` to empty strings for
every example. Inherited live endpoint and CA settings therefore cannot cause
these tests to contact an external server.

| Source | Demonstrated behavior |
| --- | --- |
| `01_quickstart_unary_ddl_and_crud.cc` | HealthCheck, database/table DDL, alter, columnar and row insert/delete, multi-table ingestion, truncate/drop, and checked RPC order/affected rows. |
| `02_reflected_row_and_zero_copy_views.cc` | C++26 annotations, borrowed input, typed SoA buffers, nullable fields, JSON/decimal metadata, name-bound views into decoded Protobuf, compressed Arrow round-trip. |
| `03_memory_policy_and_batch_reuse.cc` | Inclusive policy watermarks, capacity/byte measurements, warm reuse and reclamation for typed columns, dynamic columnar/row batches, multitable snapshots, and reflected batches. |
| `04_full_column_type_matrix.cc` | All 21 supported logical types, one non-null and one nullable row, every compression mode, then reconstruction of a dynamic batch with byte-identical insert columns. |
| `05_stream_inserter_and_backpressure.cc` | Eight ingestion messages, a reusable 512-row source exceeding the initial HTTP/2 window, finish ACKs, rejected-request retention, cancellation, and channel reuse. |
| `06_arrow_flight_doput_bulk_writer.cc` | Schema request 0 and ACK, LZ4 Frame/Zstandard bodies, two-frame ACK window with the first ACK withheld until the second batch, and three writes partitioned into nine hourly batches. |
| `07_arrow_flight_doget_and_promql.cc` | Client SQL DoGet with Zstd-compressed IPC bodies, metadata-only DML affected rows/metrics, and unary PromQL instant/vector and range/matrix responses. |
| `08_thread_local_and_worker_striping.cc` | Shared thread-local and worker managers, per-thread TCP clients, deterministic keyed slots, endpoint rotation, duplicate lease rejection, RAII release, and cache teardown. |
| `09_tls_and_failover_circuit_breaker.cc` | Nexora endpoint quarantine and injected-clock cooldown recovery, safe write replay rules, and an HTTP/2 mutual TLS HealthCheck. |
| `10_high_throughput_telemetry_pipeline.cc` | 256 borrowed host/network telemetry samples into warmed storage, two hourly DoPut batches, ACK-before-reuse, compressed DoGet, and typed zero-copy replay with exact sequence/payload checks. |

The source files follow the corresponding numbered Rust SDK examples. Public C++
APIs at this revision use these names:

- `ViewDecodedRow<T>` constructs a wire-backed `RowView<T>`; there is no separate
  `WireRowView<T>` class. Its Protobuf columns must outlive the view. It validates
  schema order; example 02 explicitly indexes reordered columns by name using
  fixed non-owning pointers before calling it.
- `RecordBatchFromProtoColumns` constructs the Arrow `flight::RecordBatch`.
  `FlightIpcEncoder` and `DecodeFlightRecordBatch` provide IPC round trips whose
  decoded columns are packed GreptimeDB Protobuf columns.
- The type matrix rebuilds `ColumnarBatch` from those decoded values. This
  normalizes the decoder's JSON datatype/native-JSON extension to the insert
  encoder's Binary datatype/`JSON_BINARY` extension before asserting exact
  request equality. Both represent the same logical JSON column.

Borrowing does not make the whole pipeline allocation-free. `BorrowedRow`,
`RowView`, and `ViewDecodedRow` refer to existing strings and bytes. Staging
copies payloads into owned buffers; the high-throughput telemetry example reserves and warms that
storage and checks unchanged capacity/byte accounting. IPC, Protobuf, mock server
storage, and transport can allocate. Mutating or clearing an owner invalidates
its borrowed views.

To run one executable directly:

```sh
./build.py build --target example_05_stream_inserter_and_backpressure -j 2
./build/gcc-glibc/bin/example_05_stream_inserter_and_backpressure
```

Example 05 optionally connects to a real server when `GREPTIMEDB_ENDPOINT` is
nonempty. It inserts one row into `example_stream_samples` in `greptime.public`,
using the auto-create-table hint. The example does not delete that table or replay
uncertain writes. The live path requires an operator-provided reachable endpoint
and is not part of the offline verification:

```sh
GREPTIMEDB_ENDPOINT=grpc://127.0.0.1:4001 \
  ./build/gcc-glibc/bin/example_05_stream_inserter_and_backpressure
```

Examples 01, 07, 08, and 09 optionally perform a **read-only health probe** when
run directly with a nonempty `GREPTIMEDB_ENDPOINT`. For a private TLS CA,
`GREPTIMEDB_CA_PEM` is the path to its PEM file. These examples send their DDL,
CRUD, SQL DML, and PromQL demonstration requests only to the offline peers.

```sh
GREPTIMEDB_ENDPOINT=grpcs://db.example.com:4001 \
  GREPTIMEDB_CA_PEM=/path/to/ca.pem \
  ./build/gcc-glibc/bin/example_09_tls_and_failover_circuit_breaker
```

Example 09 configures `nexora::EndpointFailover` with injected timestamps,
quarantines an endpoint, selects a healthy peer, and recovers after cooldown.
`nexora::ShouldFailoverWrite` requires both idempotency and proof that no bytes
were sent before permitting replay. The mutual TLS demonstration uses
`nexora::TlsClientConfig`, `nexora::TlsServerConfig`, and a TLS
`grpcxx::InMemoryChannelPair` with HTTP/2 ALPN and mutual certificate verification.
