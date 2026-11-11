# greptimedb

greptimedb is a Linux C++26 GreptimeDB SDK for SQL, PromQL, DDL, ingestion,
deletes, and Arrow Flight streaming. It provides columnar batches, reflected
row schemas, borrowed row views, and client pools for application-owned threads.
Include `<greptimedb/greptimedb.h>` and link `greptimedb::greptimedb`, or use the
individual domain module headers and CMake targets.

Clients, channels, streams, and batches stay on the thread that created them.
The SDK creates no internal worker threads or mutexes; applications scale with
thread-local clients and worker-slot pools. It compiles with `-fno-exceptions`.
SDK operations return `Result<T>` (`std::expected<T, Error>`). Batch and codec
APIs keep their declared errors. The Flight wire is Protobuf
`arrow.flight.protocol`; the IPC codec is in-tree and does not link libarrow,
grpc++, Folly, or Boost.Asio.

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
- Client factories can apply round-robin, sticky, random, or latency-aware
  endpoint selection and circuit breaking. Writes already placed on the wire
  require explicit server retry permission before replay on another endpoint.
  `Client` does not select endpoints by itself.
- `ThreadLocalClient` lazily caches clients per application thread.
  `WorkerClient` adds per-thread slots, endpoint striping, and round-robin,
  least-in-flight, or key-hash leasing. Factories may receive failover
  configuration. Cached clients and leases stay on their owner thread.
- Connections support plain TCP (`grpc://`, `http://`), TLS (`grpcs://`,
  `https://`), and Unix sockets (`unix://`). TLS 1.3 connections require HTTP/2
  ALPN and support peer verification, custom trust, and client certificates.
  Examples drive both HTTP/2 peers on the calling thread.

Borrowed input, decoded payloads, and callback contexts must outlive their uses.
Finish or cancel streams before destroying their channel or client. Buffer
reservation controls SDK storage reuse; standard-container and upstream
allocations remain subject to process memory availability under
`-fno-exceptions`.

Source: [trunkszi/greptimedb](https://github.com/trunkszi/greptimedb).

## Build, test, and live Docker E2E

Use a GCC 16 or Clang toolchain implementing P2996 reflection and P3394
annotations; a stock compiler with partial C++26 support is insufficient.
CMake 4.0 or newer and Ninja are required.
CMake resolves ABI-compatible installed packages for the selected toolchain.

`build.py` is an executable Bash launcher for a uv-managed Python CLI, not a
Python script. It installs uv when absent and uses CMake from PATH.
`./scripts/install-dependencies.sh` installs the selected reflection compiler
for `gcc-glibc`, `clang-glibc`, `gcc-musl`, or `clang-musl` and provisions missing
overlay ports. `./build.py setup` installs ABI-matched dependencies through vcpkg
and configures the selected preset. The installer preserves CMake 4.4.5 or newer; AddressSanitizer
presets use glibc.

All presets use vcpkg manifest mode. Put overlay ports in `$HOME/.local/vcpkg`
(`VCPKG_OVERLAY_PORTS`); each preset owns its `build/<preset>/vcpkg_installed`
tree and shares the binary cache under `$HOME/.cache/vcpkg/archives`.

```sh
./build.py --help
./build.py build
./build.py test
./build.py test --target test_proto_smoke
./build.py test --target test_ipc_codec
./build.py test --cli-only
./build.py --format json test --no-build --filter test_ipc_codec
./build.py build -t example_02_reflected_row_and_zero_copy_views -j 2
./build.py bench --list
./build.py bench -t bench_arrow_ipc_codec
./build.py format --check src tests
.github/scripts/check-source-contracts.sh
```

Bare `./build.py` selects `build`. An unfiltered `test` configures/builds, runs
CTest, and then runs the Python CLI tests. Test target, regex, and label filters
select C++ tests only. Artifacts land in `build/<preset>/bin/`.

| Preset | Configuration |
| --- | --- |
| `clang-glibc` | Clang P2996 with libc++ and packages under `build/<preset>/vcpkg_installed` |
| `gcc-glibc` | GCC 16 reflection compiler under `$HOME/.local/toolchain/gcc-glibc` and packages under `build/<preset>/vcpkg_installed` |
| `gcc-glibc-vcpkg` | Compatibility alias of `gcc-glibc` |
| `clang-asan` | `clang-glibc` with AddressSanitizer and UndefinedBehaviorSanitizer |
| `gcc-asan` | `gcc-glibc` with AddressSanitizer and UndefinedBehaviorSanitizer |
| `clang-musl` | Clang P2996 against the musl sysroot and `build/<preset>/vcpkg_installed` |
| `gcc-musl` | GCC 16 against the musl sysroot and `build/<preset>/vcpkg_installed` |

The project default is `gcc-glibc`: use `cmake --preset gcc-glibc` to configure
and `ctest --preset gcc-glibc` to test.

Select a preset with `./build.py --preset <name> build`. Installed consumers
use `find_package(greptimedb CONFIG REQUIRED)` and the exported SDK targets.
The overlay port fetches greptime-proto at
`acb247415b8f4c2f0750783dab4e1f38c71f5860` and configures `GREPTIME_PROTO_DIR`
to that tree. Messages and reflected RPC contracts are generated in the build
tree, not checked into this repository.

Require live GreptimeDB round trips after building:

```sh
./build.py test -j 2
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
struct [[= db::table{"metric_samples"}]] MetricSample;
struct MetricSample {
  [[= db::tag{}]] std::string host;
  [[= db::timestamp{.unit = db::TimestampUnit::kNanosecond}]] std::int64_t ts;
  [[= db::field{}]] double cpu_usage;
  [[= db::field{}]] std::optional<double> size;
  [[= db::field{.json = true}]] std::string payload;
  [[= db::field{.decimal_precision = 18,
                .decimal_scale = 4}]] __int128 coefficient;
  [[= db::field{}]] std::vector<std::byte> blob;
};

int main() {
  static_assert(db::RowConcept<MetricSample>);
  static_assert(db::TypedColumnarBatch<MetricSample>::kColumnCount == 7);
  MetricSample source{"host-01",
                   1'700'000'000'000'000'000,
                   100.25,
                   1.5,
                   "{\"cpu_usage\":100.25}",
                   123456,
                   {std::byte{0}, std::byte{0xff}}};
  const db::BorrowedRow<MetricSample> borrowed{source};
  const db::RowView<MetricSample> source_view{source};
  EXAMPLE_REQUIRE(borrowed.Get<0>().data() == source.host.data());
  EXAMPLE_REQUIRE(source_view.Get<6>().data() == source.blob.data());
  db::TypedColumnarBatch<MetricSample> batch{3, db::BufferPolicy::Unbounded()};
  EXAMPLE_REQUIRE(batch.ReserveRows(3));
  EXAMPLE_REQUIRE(batch.ReserveBytes(256));
  const auto bytes = batch.AllocatedBytes();
  EXAMPLE_REQUIRE(batch.PushRef(borrowed));
  EXAMPLE_REQUIRE(batch.PushRef(db::BorrowedRow<MetricSample>{
      std::string_view{"host-02"}, source.ts + 1, 2000.5,
      std::optional<double>{}, std::string_view{"{}"},
      static_cast<__int128>(-987654), db::BinaryView{}}));
  EXAMPLE_REQUIRE(batch.PushRef(db::BorrowedRow<MetricSample>{
      std::string_view{"host-03"}, source.ts + 2, 1.25,
      std::optional<double>{0.0}, std::string_view{"{}"},
      static_cast<__int128>(0), db::BinaryView{}}));
  EXAMPLE_REQUIRE(batch.AllocatedBytes() == bytes);
  EXAMPLE_REQUIRE(batch.RowCount() == 3);
  auto row = batch.GetRow(1);
  EXAMPLE_REQUIRE(row && !row->Get<3>());
  EXAMPLE_REQUIRE(row->Get<"host">() == "host-02");
}
```

The full example also encodes a Protobuf request, projects wire-backed rows with
`ViewDecodedRow<MetricSample>`, reorders columns by name, and round-trips Arrow IPC
with LZ4 Frame. Source views borrow the source row, batch views borrow batch
storage, and decoded views borrow the decoded message. Keep each owner alive
and unmodified while its views are in use.

## Runnable examples

All ten examples are registered by the root CMake build. Binaries are under
`build/<preset>/bin/`; `./build.py test --filter '^example\.'` runs their
offline integration assertions. Examples use local data or caller-driven mock
HTTP/2 peers. Setting `GREPTIMEDB_ENDPOINT` adds a read-only health probe in
examples 01, 07, 08, and 09; example 05 instead writes to the live
`example_stream_samples` table. CTest clears this variable for offline runs.

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
| `10_high_throughput_telemetry_pipeline.cc` | `example_10_high_throughput_telemetry_pipeline` | High-throughput host/network telemetry ingestion pipeline and batch/stream composition |

## Benchmark suites

Use `./build.py bench -t <binary>`; Google Benchmark arguments follow `--`.
The CLI enables `GREPTIMEDB_BUILD_BENCHMARKS`; its default target is
`bench_column_buf`. Use `./build.py bench --list` to inspect available targets.

| CMake target and binary | Workload |
| --- | --- |
| `bench_column_buf` | Column pushes, null masks, warm reuse/cold allocation, Protobuf serialization |
| `bench_reflected_row` | Reflected row ingestion and borrowed/typed access |
| `bench_arrow_ipc_codec` | Arrow IPC schema/batch encoding and decoding |
| `bench_streaming_loopback` | Caller-driven streaming against a loopback peer |

Benchmark results depend on hardware, compiler, build type, and workload. Local
loopback measurements do not establish live GreptimeDB throughput or latency.

## CMake library targets

Installed consumers use `find_package(greptimedb CONFIG REQUIRED)` and
`greptimedb::greptimedb`. The build also exports these domain aliases:

| Target | Responsibility |
| --- | --- |
| `greptimedb::greptimedb` | Umbrella client, rows, and threading |
| `greptimedb::proto` | Generated Protobuf messages and reflected RPC contracts |
| `greptimedb::core` | Errors, configuration, values, hints, and buffer policy |
| `greptimedb::batch` | Column buffers, dynamic batches, and Protobuf batch encoding |
| `greptimedb::row` | Reflected typed batches, borrowed rows, source/decoded views |
| `greptimedb::flight` | Flight DoPut and DoGet surface over the in-tree IPC codec |
| `greptimedb::flight_ipc` | Built-in Arrow IPC/FlatBuffers, LZ4 Frame, and Zstandard |
| `greptimedb::streaming` | StreamInserter, Flight BulkStreamWriter, and QueryStream |
| `greptimedb::client` | Unary Client, DDL helpers, PromQL, health, and stream factories |
| `greptimedb::threading` | ThreadLocalClient and WorkerClient caches/leases |

See the [CLI guide](scripts/cli/README.md) for command options.
