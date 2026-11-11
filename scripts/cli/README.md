# greptimedb build CLI

Run the executable `./build.py` from any working directory. It starts the uv-managed
Python package here, using the checkout containing the launcher as the project root.
If uv is absent, the launcher installs it into `UV_INSTALL_DIR` or `$HOME/.local/bin`.

```sh
./build.py build
./build.py test
./build.py test --cli-only
./build.py test --target test_proto_smoke
./build.py bench
./build.py bench --list
./build.py --format json test --no-build
./build.py setup --vcpkg
./build.py clean --preset gcc-glibc
./build.py format --check src tests
```

An unfiltered `test` configures and builds the selected CMake preset, runs CTest,
then runs the Python CLI tests. `--cli-only` skips CMake entirely. CTest target,
regex, and label filters select C++ tests only. Child failures preserve their exit
status; JSON output includes both captured stdout and stderr.

All presets require CMake 4.0 or newer and use vcpkg manifest mode. Overlay
ports come from `$HOME/.local/vcpkg` (`VCPKG_OVERLAY_PORTS`). Target dependencies
live under `build/<preset>/vcpkg_installed`; native code generators use the GCC
glibc host triplet, including when the target uses musl or sanitizers.

`gcc-glibc` is the default. `gcc-glibc-vcpkg` remains a compatibility alias.
`setup` honors global `--preset` / `--compiler`; `--vcpkg` selects the GCC alias.
It configures the manifest without installing compilers. Run the dependency
installer first on a fresh host.

`bench` enables `GREPTIMEDB_BUILD_BENCHMARKS` and defaults to `bench_column_buf`.
It measures null-mask pushes, typed ingestion, warm buffer reuse versus cold
allocation, and Protobuf column serialization. Forward Google Benchmark options
after `--`, for example `./build.py bench -- --benchmark_filter=IngestInt64`.
The manifest preset also enables its `benchmarks` dependency feature.

CTest includes `check_source_contracts`, which checks the 800-line source-file
limit, banned manifest dependencies, and prohibited production concurrency,
coroutine, and exception primitives. Run `.github/scripts/check-source-contracts.sh`
directly for a build-independent check.
