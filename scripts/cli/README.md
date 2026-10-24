# greptimedb-cxx build CLI

Run the executable `./build.sh` from any working directory. It starts the uv-managed
Python package here, using the checkout containing the launcher as the project root.
If uv is absent, the launcher installs it into `UV_INSTALL_DIR` or `$HOME/.local/bin`.

```sh
./build.sh build
./build.sh test
./build.sh test --cli-only
./build.sh test --target test_proto_smoke
./build.sh bench
./build.sh bench --list
./build.sh --format json test --no-build
./build.sh setup --vcpkg
./build.sh clean --preset gcc-glibc
./build.sh format --check src tests
```

An unfiltered `test` configures and builds the selected CMake preset, runs CTest,
then runs the Python CLI tests. `--cli-only` skips CMake entirely. CTest target,
regex, and label filters select C++ tests only. Child failures preserve their exit
status; JSON output includes both captured stdout and stderr.

The default `gcc-glibc` preset uses the GCC 16 reflection toolchain at
`$HOME/.local/toolchain/gcc-glibc` and the matching grpcxx dependency prefix at
`$HOME/.local/grpcxx-deps/gcc-glibc`. `gcc-glibc-vcpkg` installs the SDK manifest
while retaining that host prefix for upstream code-generator prerequisites.
Upstream grpcxx currently requires CapnProto when configuring its tools, although
CapnProto is outside this SDK's requested manifest dependencies. Protobuf discovery
also follows CMake prefix ordering, so the manifest preset requires checking that
`protoc`, headers, and libraries resolve to the same installation. Only the default
GCC host-dependency preset is verified here; the manifest preset is not a portable
bootstrap for an otherwise unprovisioned host.

The `clang-glibc` preset uses `-freflection-latest` and requires a separately
installed P2996-capable Clang compiler with ABI-compatible dependencies.
`setup` honors global `--preset` / `--compiler`; its explicit `--vcpkg` flag
overrides them with `gcc-glibc-vcpkg`. It only configures a preset; it does not install compilers or edit sibling
repositories.

`bench` enables `GREPTIMEDB_BUILD_BENCHMARKS` and defaults to `bench_column_buf`.
It measures null-mask pushes, typed ingestion, warm buffer reuse versus cold
allocation, and Protobuf column serialization. Forward Google Benchmark options
after `--`, for example `./build.sh bench -- --benchmark_filter=IngestInt64`.
The manifest preset also enables its `benchmarks` dependency feature.

CTest includes `check_source_contracts`, which checks the 800-line source-file
limit, banned manifest dependencies, and prohibited production concurrency,
coroutine, and exception primitives. Run `.github/scripts/check-source-contracts.sh`
directly for a build-independent check.
