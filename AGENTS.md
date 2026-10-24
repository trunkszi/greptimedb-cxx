# greptimedb-cxx — Shared Agent Guide

## Project overview

greptimedb-cxx is a caller-driven C++26 GreptimeDB SDK built on grpcxx for
thread-affine ingestion, queries, and deterministic low-latency applications.
It provides unary clients, streaming ingestion, Arrow Flight, a built-in Arrow
IPC codec, and P2996 reflection/P3394 annotations for typed rows.

This guide applies to Claude Code, Codex, Gemini, and human maintainers.
`src/greptimedb/` is the source of truth for shipped APIs and ownership contracts;
inspect the relevant headers before changing or documenting behavior.

## Non-negotiable engineering invariants

- Keep clients, channels, streams, and batches on their creating owner thread.
  Progress runs on the caller; SDK services create no internal worker threads
  or mutexes. Scale across application threads with `ThreadLocalClient` or
  `WorkerClient`, keeping cached clients and active leases on their owner thread.
- Compile with `-fno-exceptions`. Public SDK failures use
  `std::expected<T, Error>` (`Result<T>`); preserve the specialized error types
  declared by batch and low-level IPC APIs. Use explicit fallible allocation or
  reserved buffers. Batch appends validate and prepare every column before
  committing: failure rolls back logical rows and values atomically, although
  newly grown capacity may remain. Respect `BufferPolicy` retention limits.
- Use `grpcxx::InplaceDelegate<Sig, 48>` for inline callbacks; hot paths must not
  introduce `std::function` or hidden heap-backed callback storage.
- Model protocol and lifecycle transitions with exhaustive `enum class` FSMs.
  State switches have no `default:` branch and compile with `-Wswitch`;
  reject illegal transitions explicitly.
- Keep borrowed data and callback contexts alive through their last use. Finish
  or cancel streams before channel/client teardown. Sending is not a commit
  acknowledgement; replay an uncertain write only with explicit server retry
  permission, following `FailoverPolicy`.
- Use grpcxx and the built-in Arrow IPC codec. Dependencies on `arrow`, `grpc`,
  `folly`, and `boost-asio` are prohibited; PyArrow is an optional fixture oracle,
  not a linked SDK dependency.
- Keep `.h`, `.cc`, `CMakeLists.txt`, and `.py` files at most 800 lines. Run
  `.github/scripts/check-source-contracts.sh`, which enforces source limits,
  banned manifest dependencies, and prohibited production primitives.
- Configure, build, and run routine tests through `./build.sh`. Read its help
  before choosing flags; use the documented CTest command below for required
  live E2E runs against an existing build.

## Quick start

`build.sh` is an executable Bash launcher for a uv-managed Python CLI. The
default preset is `gcc-glibc`; provision its reflection compiler and matching
dependencies before building. `setup` configures dependencies but does not
install a compiler. Run commands from the repository root:

```sh
./build.sh --help
./build.sh build --help
./build.sh build
./build.sh test
./build.sh test --target test_proto_smoke
./build.sh test --cli-only
./build.sh --format json test --no-build --filter test_ipc_codec
./build.sh bench --list
./build.sh bench -t bench_reflected_row
GREPTIMEDB_E2E_REQUIRED=1 ctest --test-dir build/gcc-glibc -R e2e -V
.github/scripts/check-source-contracts.sh
```

Run checks covering the changed API and its callers. For documentation-only
changes, verify commands, tracked links, and `git diff --check`. Report actual
check results; distinguish skipped live tests from successful server round trips.

## Repository architecture

| Path | Responsibility |
| --- | --- |
| `src/greptimedb/core/` | Configuration, errors, scalar/value types, write hints, buffer policy |
| `src/greptimedb/proto/` | GreptimeDB messages, reflected RPC contracts, protocol helpers |
| `src/greptimedb/batch/` | Column buffers, dynamic columnar/row/multi-table batches, Protobuf encoding |
| `src/greptimedb/row/` | Reflected schemas, typed batches, borrowed rows, decoded wire views |
| `src/greptimedb/transport/` | Caller-thread channels, TCP/Unix/TLS transports, in-memory peers |
| `src/greptimedb/client/` | Unary Client, DDL, PromQL, StreamInserter |
| `src/greptimedb/flight/` | DoPut bulk writer, DoGet query stream, Arrow IPC/FlatBuffers/compression |
| `src/greptimedb/failover/` | Endpoint selection, circuit breaking, safe write replay policy |
| `src/greptimedb/threading/` | Thread-local client caches and worker-slot leasing |
| `examples/` | Ten runnable SDK examples, with offline assertions and optional live operations |
| `tests/` | Unit, integration, live Docker E2E tests, and interoperability fixtures |
| `benches/` | Column buffer, reflected row, Arrow IPC, and streaming loopback benchmarks |
| `scripts/cli/` | Build/test/development CLI and its Python tests |

## Context pointers

- Read [README.md](README.md) when selecting SDK APIs, examples, benchmarks, or
  library targets.
- Read [src/greptimedb/greptimedb.h](src/greptimedb/greptimedb.h) before changing
  the public include surface; follow its includes to the owning API contracts.
- Read [scripts/cli/README.md](scripts/cli/README.md) before changing build
  commands, presets, dependency setup, test selection, or structured output.
- Read [tests/e2e/README.md](tests/e2e/README.md) before running or changing live
  server tests, Docker lifecycle, endpoint selection, or required/skip behavior.
- Read [tests/units/flight/fixtures/README.md](tests/units/flight/fixtures/README.md)
  before changing Arrow interoperability fixtures or regenerating PyArrow data.
- Read [.github/scripts/check-source-contracts.sh](.github/scripts/check-source-contracts.sh)
  before changing source limits, prohibited primitives, or dependency checks.
