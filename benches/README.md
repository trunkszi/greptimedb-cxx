# SDK microbenchmarks

Run benchmarks through the build CLI. The default remains `bench_column_buf`.

```sh
./build.sh bench
./build.sh bench --list
./build.sh bench -t bench_reflected_row
./build.sh bench -t bench_arrow_ipc_codec
./build.sh bench -t bench_streaming_loopback
./build.sh bench -t bench_arrow_ipc_codec -- --benchmark_filter=Zstd
```

Each target links Google Benchmark and inherits the SDK's C++26 reflection and
`-fno-exceptions` compile options. Failures mark the affected benchmark with
`SkipWithError`; inspect benchmark errors as well as the process exit status.
The CLI's default GCC preset is a Debug build. These are local development
measurements; compare results using the same build configuration and host.

| Target | Measured operations |
| --- | --- |
| `bench_column_buf` | Null masks, typed column ingestion, buffer retention, protobuf serialization |
| `bench_reflected_row` | `TypedColumnarBatch<T>::Push`, warmed `Clear`/`PushRef`, `Views`, decoded protobuf row projection, typed/dynamic/row insert and delete encoding |
| `bench_arrow_ipc_codec` | Schema encoding/decoding, record batch encoding/decoding with no compression, LZ4 Frame and Zstandard, hour partitioning, protobuf column conversion/copy |
| `bench_streaming_loopback` | `StreamInserter`, `BulkStreamWriter` with ACK windows of one/four batches, `QueryStream` decoding and typed row projection |

Reflected benchmarks use the same tag, millisecond timestamp and floating-point
field as the dynamic and row batches. Fixtures and payload capacity are warmed
before ingestion timing. The `BorrowedPushRefWarmClear` case includes `Clear()`
and counts C++ allocation calls only within those warmed ingestion cycles; any
allocation or retained-capacity change rejects the case. The allocator probe is
linked only into `bench_reflected_row`; it counts global C++ `new` overloads,
including aligned and nothrow forms, rather than direct C allocation calls.

`Views()` returns batch projections. The current decoded protobuf projection API
is `ViewDecodedRow<T>`; `WireRowViews` first serializes and parses the fixture
outside timing, then measures validation and zero-copy row projection.

The public IPC methods are `FlightIpcEncoder::EncodeSchema`, `EncodeBatch`,
`DecodeFlightSchema`, `DecodeFlightRecordBatch` and `PartitionRecordBatchByHour`.
Schema IPC has no body compression, so schema cases run once. Record-batch cases
run all three compression modes at 64 and 4,096 rows and report `wire_bytes`
separately from logical throughput. Hour partitioning spans multiple hours and
checks that every row survives. `RecordBatchFromProtoColumns` constructs the
Arrow schema and owns column values in protobuf form. Consequently,
`ArrowBatchToProtoColumnsCopy` measures an owned copy of decoded protobuf columns;
the actual Arrow body conversion is measured by `DecodeRecordBatchMessage`.

Loopback cases reuse an `InMemoryChannelPair` across calls, with a real
`MockGrpcServer` HTTP/2 peer driven on the same caller thread. Each measured call
opens a stream, transfers four batches and checks successful completion and exact
row counts. The DoPut peer decodes every received batch and withholds ACKs until
the configured window fills; counters verify that the writer reaches that
in-flight limit and receives every ACK. DoGet responses are encoded before timing,
so its measurement includes transport, client decoding and `RowView` access, while
excluding server-side IPC encoding. No network sockets or server threads are used.

`items_per_second` counts logical rows, except schema cases which count messages.
`bytes_per_second` counts input payload bytes (tag bytes plus timestamp and price)
for the new row, IPC record-batch and loopback cases. Delete encoding uses the same
input rows but emits only key columns, so this byte counter is not a wire-size
claim. Schema cases count encoded schema bytes.
