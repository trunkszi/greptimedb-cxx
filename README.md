# greptimedb-cxx

greptimedb-cxx is a Linux C++26 GreptimeDB SDK built on grpcxx. It provides
caller-driven unary RPCs, streaming ingestion, Arrow Flight bulk writes and
queries, reflected row APIs, and a built-in Arrow IPC codec. Include
`<greptimedb/greptimedb.h>` for the public SDK, or include individual module
headers and link the corresponding CMake targets below.

Clients, channels, streams, and batches are confined to their creating caller
thread. The SDK creates no internal worker threads or mutexes; applications
scale with thread-local clients and worker-slot pools. It compiles with
`-fno-exceptions`; SDK operations return `Result<T>` (`std::expected<T, Error>`),
while batch and low-level codec APIs retain their declared specialized errors.
It does not link Apache Arrow, grpc++, Folly, or Boost.Asio.

## Capabilities and boundaries

- `Client` supports columnar, row, reflected, and multi-table writes; columnar
  and row deletes; PromQL instant/range queries; and HealthCheck through `Ping`
  and `ProbeHealth`. Its 11 named DDL helpers are `CreateDatabase`,
  `AlterDatabase`, `CreateTable`, `AlterTable`, `DropTable`, `TruncateTable`,
  `CreateView`, `DropView`, `CreateFlow`, `DropFlow`, and `CommentOn`.
  `ExecuteDdl` accepts a raw DDL request.
- `StreamInserter` sends client-streaming ingestion requests with one immutable
  session scope and returns affected rows on `Finish`. Accepted sends do not
  acknowledge server commits; cancellation does not replay uncertain writes.
- Arrow Flight `BulkStreamWriter` implements DoPut with schema acknowledgement,
  a bounded in-flight ACK window, optional hourly partitioning, and none, LZ4
  Frame, or Zstandard IPC body compression. `QueryStream` implements DoGet for
  SQL and query requests, returning decoded record batches and accumulating
  affected-row counts and metrics from metadata frames.
- The built-in Arrow IPC encoder/decoder handles primitive columns, null masks,
  timestamp units, Decimal128, JSON metadata, large strings/binary, and Arrow
  `Utf8View`/`BinaryView` layouts with `variadicBufferCounts`. It validates view
  buffers, lengths, offsets, and compression frames without libarrow. Nested
  and dictionary-encoded fields, big-endian schemas, and timezone-bearing
  timestamps are rejected. PyArrow is optional for independent interoperability
  checks, not required for normal builds or tests.
- C++26 P2996 reflection and P3394 annotations derive schemas and typed access
  for `TypedColumnarBatch<T>`, `BorrowedRow<T>`, and `RowView<T>`.
  `ViewDecodedRow<T>` in `wire_row_view.h` validates decoded Protobuf columns and
  returns a borrowed `RowView<T>`; there is no separate public `WireRowView`
  type. Borrowing avoids payload copies at the view boundary; IPC encoding and
  Protobuf conversion still copy.
- `ColumnarBatch`, `RowBatch`, and `MultiTableBatch` support dynamic schemas and
  ingestion assembly. `BufferPolicy` controls retained row/byte capacity on
  clear. Row appends validate and prepare all columns before committing; errors
  preserve existing logical rows and values, while grown capacity may remain.
- `FailoverPolicy` supplies round-robin, sticky, random, and latency-aware
  endpoint selection, circuit breaking, and safe replay rules. A write already
  placed on the wire can move endpoints only with explicit server retry
  permission. Policy is composed explicitly with clients/factories; `Client`
  does not automatically enable failover.
- `ThreadLocalClient` lazily caches clients per application thread.
  `WorkerClient` adds per-thread slots, endpoint striping, and round-robin,
  least-in-flight, or key-hash leasing. Factories may receive failover
  configuration. Cached clients and leases stay on their owner thread.
- Transports support plain TCP (`grpc://`, `http://`), TLS (`grpcs://`,
  `https://`), and Unix sockets (`unix://`). TLS requires TLS 1.3 and HTTP/2
  ALPN with peer verification, optional custom trust, and client certificates.
  `InMemoryChannelPair` and `MockGrpcServer` provide caller-driven HTTP/2 peers
  for local integration tests.

Borrowed input, decoded payloads, and callback contexts must outlive their uses.
Finish or cancel streams before destroying their channel or client. Buffer
reservation controls SDK storage reuse; standard-container and upstream
allocations remain subject to process memory availability under
`-fno-exceptions`.

## Build, test, and live Docker E2E

Use a GCC 16 or Clang toolchain implementing P2996 reflection and P3394
annotations; a stock compiler with partial C++26 support is insufficient.
CMake requires at least 3.28 (3.30+ provides native C++26 support), Ninja, and
ABI-compatible dependencies for grpcxx, Protobuf/code generation, OpenSSL,
LZ4 Frame, Zstandard, and the enabled test/benchmark/example targets.

`build.sh` is an executable Bash launcher for a uv-managed Python CLI. It
installs uv when absent and uses a cached CMake 3.31.10 when available if the
active CMake is older than 3.30. `setup` configures dependencies;
it does not install compilers or modify the sibling grpcxx checkout.

```sh
./build.sh --help
./build.sh build
./build.sh test
./build.sh test --target test_proto_smoke
./build.sh test --target test_ipc_codec
./build.sh test --cli-only
./build.sh --format json test --no-build --filter test_ipc_codec
./build.sh build -t example_02_reflected_row_and_zero_copy_views -j 2
./build.sh bench --list
./build.sh bench -t bench_arrow_ipc_codec
./build.sh format --check src tests
.github/scripts/check-source-contracts.sh
```

Bare `./build.sh` selects `build`. An unfiltered `test` configures/builds, runs
CTest, and then runs the Python CLI tests. Test target, regex, and label filters
select C++ tests only. Artifacts land in `build/<preset>/bin/`.

| Preset | Configuration |
| --- | --- |
| `gcc-glibc` | Default; GCC 16 reflection compiler under `$HOME/.local/toolchain/gcc-glibc`, ABI-matched dependencies under `$HOME/.local/grpcxx-deps/gcc-glibc` |
| `gcc-glibc-vcpkg` | vcpkg manifest plus host grpcxx code-generator prerequisites; requires a provisioned compiler and consistent Protobuf installation |
| `clang-glibc` | Separately provisioned P2996 Clang with `-freflection-latest` and ABI-compatible dependencies |

Select another preset with `./build.sh --preset <name> build`. Only the default
GCC host-dependency preset is documented as verified in the
[CLI guide](scripts/cli/README.md); the manifest preset is not a standalone
toolchain bootstrap. Upstream grpcxx's code generators currently also require
CapnProto at configure time.

The build uses a sibling grpcxx checkout (`GRPCXX_SOURCE_DIR`, default
`${sourceDir}/../grpcxx`). `GREPTIME_PROTO_DIR`, as a CMake cache variable or
environment variable, may name a greptime-proto checkout or its `proto`
directory. Otherwise configuration populates `$HOME/.cache/greptime-proto`.
Sources must be clean at `acb247415b8f4c2f0750783dab4e1f38c71f5860`; supplied
checkouts are not modified. Messages and reflected RPC contracts are generated
in the build tree, not checked into this repository.

Require live GreptimeDB round trips after building:

```sh
./build.sh test -j 2
GREPTIMEDB_E2E_REQUIRED=1 ctest --test-dir build/gcc-glibc -R e2e -V
GREPTIMEDB_TEST_ENDPOINT=grpc://127.0.0.1:4001 GREPTIMEDB_E2E_REQUIRED=1 \
  ctest --test-dir build/gcc-glibc -R e2e -V
```

Without an endpoint, the suite starts or reuses the `greptimedb-cxx-e2e` Docker
container from `greptime/greptimedb:latest`, publishing an ephemeral loopback
gRPC port. Missing Docker skips ordinary runs; `GREPTIMEDB_E2E_REQUIRED=1`
makes that a failure. Explicit endpoint failures and RPC failures always fail.
Cases create unique databases and clean them up; external endpoints and reused
containers are never removed. A newly created container remains running unless
`GREPTIMEDB_E2E_REMOVE_CONTAINER=1` requests removal.

The six live cases cover the 42-column nullable/type matrix, structured legacy
DATETIME rejection, unary/streaming ingestion and deletes, compressed Flight
DoPut/DoGet, server errors followed by successful reuse, and application-thread
client caches/worker striping. They assert stored/query results against a real
server. See the [live E2E guide](tests/e2e/README.md) for exact coverage,
timeouts, and lifecycle behavior, and the
[fixture guide](tests/units/flight/fixtures/README.md) for optional PyArrow checks.

## Reflected rows and borrowed views

This shortened excerpt preserves the types and operations from
[example 02](examples/02_reflected_row_and_zero_copy_views.cc).
`EXAMPLE_REQUIRE` is the example's assertion helper:

```cpp
#include "example_support.h"

#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>

#include <optional>
#include <string>
#include <vector>

namespace db = greptimedb;
struct[[= db::table{.name = "order_book_ticks"}]] OrderTick {
  [[= db::tag{}]] std::string symbol;
  [[= db::timestamp{.unit = db::TimestampUnit::kNanosecond}]] std::int64_t ts;
  [[= db::field{}]] double best_bid;
  [[= db::field{}]] std::optional<double> size;
  [[= db::field{.json = true}]] std::string payload;
  [[= db::field{.decimal_precision = 18,
                .decimal_scale = 4}]] __int128 notional;
  [[= db::field{}]] std::vector<std::byte> blob;
};

int main() {
  static_assert(db::RowConcept<OrderTick>);
  static_assert(db::TypedColumnarBatch<OrderTick>::kColumnCount == 7);
  OrderTick source{"BTCUSDT",
                   1'700'000'000'000'000'000,
                   100.25,
                   1.5,
                   "{\"bid\":100.25}",
                   123456,
                   {std::byte{0}, std::byte{0xff}}};
  const db::BorrowedRow<OrderTick> borrowed{source};
  const db::RowView<OrderTick> source_view{source};
  EXAMPLE_REQUIRE(borrowed.Get<0>().data() == source.symbol.data());
  EXAMPLE_REQUIRE(source_view.Get<6>().data() == source.blob.data());
  db::TypedColumnarBatch<OrderTick> batch{3, db::BufferPolicy::Unbounded()};
  EXAMPLE_REQUIRE(batch.ReserveRows(3));
  EXAMPLE_REQUIRE(batch.ReserveBytes(256));
  const auto bytes = batch.AllocatedBytes();
  EXAMPLE_REQUIRE(batch.PushRef(borrowed));
  EXAMPLE_REQUIRE(batch.PushRef(db::BorrowedRow<OrderTick>{
      std::string_view{"ETHUSDT"}, source.ts + 1, 2000.5,
      std::optional<double>{}, std::string_view{"{}"},
      static_cast<__int128>(-987654), db::BinaryView{}}));
  EXAMPLE_REQUIRE(batch.PushRef(db::BorrowedRow<OrderTick>{
      std::string_view{"WLDUSDT"}, source.ts + 2, 1.25,
      std::optional<double>{0.0}, std::string_view{"{}"},
      static_cast<__int128>(0), db::BinaryView{}}));
  EXAMPLE_REQUIRE(batch.AllocatedBytes() == bytes);
  EXAMPLE_REQUIRE(batch.RowCount() == 3);
  auto row = batch.GetRow(1);
  EXAMPLE_REQUIRE(row && !row->Get<3>());
  EXAMPLE_REQUIRE(row->Get<"symbol">() == "ETHUSDT");
}
```

The full example also encodes a Protobuf request, projects wire-backed rows with
`ViewDecodedRow<OrderTick>`, reorders columns by name, and round-trips Arrow IPC
with LZ4 Frame. Source views borrow the source row, batch views borrow batch
storage, and decoded views borrow the decoded message. Keep each owner alive
and unmodified while its views are in use.

## Runnable examples

All ten examples are registered by the root CMake build. Binaries are under
`build/<preset>/bin/`; `./build.sh test --filter '^example\.'` runs their
offline integration assertions. Examples use local data or caller-driven mock
HTTP/2 peers. Setting `GREPTIMEDB_ENDPOINT` adds a read-only health probe in
examples 01, 07, 08, and 09; example 05 instead writes to the live
`example_stream_ticks` table. CTest clears this variable for offline runs.

| Source under `examples/` | Binary | Demonstration |
| --- | --- | --- |
| `01_quickstart_unary_ddl_and_crud.cc` | `example_01_quickstart_unary_ddl_and_crud` | Health, DDL, columnar/row CRUD, multi-table ingestion |
| `02_reflected_row_and_zero_copy_views.cc` | `example_02_reflected_row_and_zero_copy_views` | Annotated rows, borrowed input, typed batches, decoded views |
| `03_memory_policy_and_batch_reuse.cc` | `example_03_memory_policy_and_batch_reuse` | BufferPolicy, reservation, retention, and batch reuse |
| `04_full_column_type_matrix.cc` | `example_04_full_column_type_matrix` | Scalar, nullable, JSON, Decimal128, and timestamp types |
| `05_stream_inserter_and_backpressure.cc` | `example_05_stream_inserter_and_backpressure` | Client streaming, backpressure, completion, cancellation |
| `06_arrow_flight_doput_bulk_writer.cc` | `example_06_arrow_flight_doput_bulk_writer` | Flight DoPut, compression, partitioning, ACK windows |
| `07_arrow_flight_doget_and_promql.cc` | `example_07_arrow_flight_doget_and_promql` | SQL DoGet, metadata-only results, PromQL instant/range |
| `08_thread_local_and_worker_striping.cc` | `example_08_thread_local_and_worker_striping` | Application threads, thread-local caches, worker slots |
| `09_tls_and_failover_circuit_breaker.cc` | `example_09_tls_and_failover_circuit_breaker` | TLS configuration, endpoint selection, circuit recovery, replay rules |
| `10_hft_market_data_pipeline.cc` | `example_10_hft_market_data_pipeline` | Market-data ingestion pipeline and batch/stream composition |

## Benchmark suites

Use `./build.sh bench -t <binary>`; Google Benchmark arguments follow `--`.
The CLI enables `GREPTIMEDB_BUILD_BENCHMARKS`; its default target is
`bench_column_buf`. Use `./build.sh bench --list` to inspect available targets.

| CMake target and binary | Workload |
| --- | --- |
| `bench_column_buf` | Column pushes, null masks, warm reuse/cold allocation, Protobuf serialization |
| `bench_reflected_row` | Reflected row ingestion and borrowed/typed access |
| `bench_arrow_ipc_codec` | Arrow IPC schema/batch encoding and decoding |
| `bench_streaming_loopback` | Caller-driven streaming against a loopback peer |

Benchmark results depend on hardware, compiler, build type, and workload. Local
loopback measurements do not establish live GreptimeDB throughput or latency.

## CMake library targets

The root build exports these ten build-tree aliases for `target_link_libraries`:

| Target | Responsibility |
| --- | --- |
| `greptimedb::proto` | Generated Protobuf messages and reflected RPC contracts |
| `greptimedb::core` | Errors, configuration, values, hints, and buffer policy |
| `greptimedb::batch` | Column buffers, dynamic batches, and Protobuf batch encoding |
| `greptimedb::row` | Reflected typed batches, borrowed rows, source/decoded views |
| `greptimedb::transport` | Synchronous channels, TCP/Unix/TLS, in-memory/mock peers |
| `greptimedb::flight_ipc` | Built-in Arrow IPC/FlatBuffers, LZ4 Frame, and Zstandard |
| `greptimedb::streaming` | StreamInserter, Flight BulkStreamWriter, and QueryStream |
| `greptimedb::client` | Unary Client, DDL helpers, PromQL, health, and stream factories |
| `greptimedb::failover` | Endpoint selection, circuit state, and write-replay policy |
| `greptimedb::threading` | ThreadLocalClient and WorkerClient caches/leases |

See the [shared agent guide](AGENTS.md) for engineering invariants and module
ownership, and the [CLI guide](scripts/cli/README.md) for command options.
