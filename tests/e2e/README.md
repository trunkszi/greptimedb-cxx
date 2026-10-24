# Live GreptimeDB round trips

```sh
./build.sh test -j 2
GREPTIMEDB_E2E_REQUIRED=1 ctest --test-dir build/gcc-glibc -R e2e -V
GREPTIMEDB_TEST_ENDPOINT=grpc://127.0.0.1:4001 GREPTIMEDB_E2E_REQUIRED=1 \
  ctest --test-dir build/gcc-glibc -R e2e -V
```

Without an explicit endpoint, the suite starts or reuses `greptimedb-cxx-e2e`
from `greptime/greptimedb:latest`, pulling the image if it is missing locally.
The container uses `standalone start --grpc-bind-addr 0.0.0.0:4001` and publishes
an ephemeral gRPC port on `127.0.0.1`. Readiness requires both `Ping()` and
`ProbeHealth()` within 90 seconds. Docker commands have a 30-second timeout;
the initial image pull has a 300-second timeout.

A missing Docker daemon skips the ordinary test unless `GREPTIMEDB_E2E_REQUIRED=1`
is set. Explicit endpoint failures, image/container errors, and failed RPCs
always fail. Each case creates a unique database and drops it afterward,
including best-effort cleanup on assertion failures. External endpoints and
reused containers are never stopped or removed. A test-created container remains
running unless `GREPTIMEDB_E2E_REMOVE_CONTAINER=1` requests its removal.

The six cases exercise these real SDK-to-server boundaries:

- A 130-row, 42-column matrix checks exact values for required and nullable
  boolean, signed/unsigned integers, floating point, UTF-8, binary, date,
  decimal128, JSON, and four timestamp units. Nullable masks cross the 64-bit
  and 128-bit boundaries. JSON is checked through `json_to_string` SQL output.
- Legacy `DATETIME` encoding and DDL must return a structured rejection on the
  current server, which requires a timestamp type instead; subsequent health
  calls must succeed. This is negative compatibility coverage, not successful
  `DATETIME` storage.
- Unary dynamic/typed/multi-table/row writes, both delete formats, add-column
  DDL, truncate/drop, and four client-streaming batches verify stored results.
- Flight DoPut sends four batches with a two-batch ACK window for each of none,
  LZ4 Frame, and Zstandard compression. DoGet checks every timestamp/value through
  decoded RecordBatch columns and borrowed RowView projections. The real server
  controls ACK timing; delayed ACK scheduling is covered by mock-server tests.
- Repeated missing-table SQL errors preserve structured server diagnostics and
  leave the same client usable for successful ingestion and queries.
- Three application threads use thread-local clients and two worker stripes per
  thread for concurrent insertion and exact query verification, then close their
  own thread-affine caches before joining.

The SDK creates no worker threads in these cases. Application threads own their
clients throughout each operation. No in-process mock substitutes for the live
GreptimeDB server in this suite.
