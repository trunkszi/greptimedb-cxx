# greptimedb

greptimedb 是 Linux 上的 C++26 GreptimeDB SDK，提供 SQL、PromQL、DDL、
写入、删除与 Arrow Flight 流式访问，以及列式批次、反射行模式、借用行视图和
面向应用线程的客户端池。包含 `<greptimedb/greptimedb.h>` 并链接
`greptimedb::greptimedb`，也可按需使用领域模块。

客户端、通道、流和批次停留在创建它们的线程上。SDK 不创建内部工作线程或互斥锁；
应用通过线程局部客户端和工作槽池扩展。编译选项包含 `-fno-exceptions`。SDK 操作
返回 `Result<T>`（`std::expected<T, Error>`）。批次与编解码 API 保留各自声明的
错误类型。Flight 线路是 Protobuf `arrow.flight.protocol`；IPC 编解码在库内实现，
不链接 libarrow、grpc++、Folly 或 Boost.Asio。

## 能力与边界

- `Client` 支持列式、行式、反射和多表写入，列式与行式删除，PromQL 即时查询和
  区间查询，以及通过 `Ping` 与 `ProbeHealth` 做的 HealthCheck。11 个具名 DDL
  辅助函数是 `CreateDatabase`、`AlterDatabase`、`CreateTable`、`AlterTable`、
  `DropTable`、`TruncateTable`、`CreateView`、`DropView`、`CreateFlow`、
  `DropFlow` 和 `CommentOn`。`ExecuteDdl` 接受原始 DDL 请求。
- `StreamInserter` 在一个不可变会话范围内发送客户端流式写入，并在 `Finish`
  时返回影响行数。已接受的发送不代表服务端已提交；取消不会重放不确定的写入。
- Arrow Flight `BulkStreamWriter` 实现 DoPut：模式确认、有界在途 ACK 窗口、
  可选的按小时分区，以及 none、LZ4 Frame 或 Zstandard 压缩。`QueryStream`
  实现 SQL 与查询请求的 DoGet，返回解码后的 record batch，并从元数据帧累计
  影响行数与指标。
- 内置 Arrow IPC 编解码器处理原始列、空值掩码、时间戳单位、Decimal128、JSON
  元数据、大字符串/二进制，以及带 `variadicBufferCounts` 的 `Utf8View` /
  `BinaryView`。它校验视图缓冲、长度、偏移和压缩帧。嵌套字段、字典编码、
  大端模式和带时区的时间戳会被拒绝。PyArrow 只用于可选的互操作核对。
- C++26 P2996 反射和 P3394 注解为 `TypedColumnarBatch<T>`、`BorrowedRow<T>`
  和 `RowView<T>` 派生模式与类型化访问。`wire_row_view.h` 中的
  `ViewDecodedRow<T>` 校验解码后的 Protobuf 列并返回借用的 `RowView<T>`。
  借用避免视图边界上的载荷拷贝；IPC 编码和 Protobuf 转换仍然拷贝。
- `ColumnarBatch`、`RowBatch` 和 `MultiTableBatch` 支持动态模式与写入组装。
  `BufferPolicy` 控制 `clear` 之后保留的行数和字节容量。行追加在提交前校验
  并准备全部列；错误保留已有逻辑行，已增长的容量可以留下。
- 客户端工厂可配置轮询、粘滞、随机或延迟感知的端点选择与熔断。已经写入线路的
  请求只有在服务端明确允许重试时才能向其他端点重放。`Client` 自己不选择端点。
- `ThreadLocalClient` 按应用线程惰性缓存客户端。`WorkerClient` 增加每线程槽位、
  端点条带，以及轮询、最少在途或按键哈希租约。缓存的客户端和租约留在所属线程。
- 连接支持明文 TCP（`grpc://`、`http://`）、TLS（`grpcs://`、`https://`）
  和 Unix 套接字（`unix://`）。TLS 1.3 连接要求 HTTP/2 ALPN，支持对端校验、
  自定义信任锚和客户端证书。示例在调用线程上同时驱动两端 HTTP/2。

借用的输入、解码载荷和回调上下文必须比它们的使用活得更久。销毁通道或客户端之前
先结束或取消流。缓冲预留控制 SDK 存储复用；标准容器和上游分配仍受进程可用内存
约束。

源码：[trunkszi/greptimedb](https://github.com/trunkszi/greptimedb)。

## 构建、测试与在线 Docker 端到端

使用实现 P2996 反射和 P3394 注解的 GCC 16 或 Clang。需要 CMake 4.0 以上版本与 Ninja。CMake 为所选工具链解析 ABI 匹配的已安装包。

`build.py` 是 uv 管理的 Python CLI 的 Bash 启动器。它在缺少 uv 时安装 uv，使用 PATH 中的 CMake。
`./scripts/install-dependencies.sh` 为 `gcc-glibc`、`clang-glibc`、`gcc-musl`
或 `clang-musl` 安装对应的反射编译器并创建缺失的 overlay 端口。
`./build.py setup` 通过 vcpkg 安装 ABI 匹配的依赖包并配置所选 CMake 预设。
安装器保留 CMake 4.4.5 以上版本；AddressSanitizer 预设使用 glibc。

所有预设都使用 vcpkg manifest 模式。overlay 端口位于 `$HOME/.local/vcpkg`
（`VCPKG_OVERLAY_PORTS`）。每个预设独立使用 `build/<preset>/vcpkg_installed`，
并共享 `$HOME/.cache/vcpkg/archives` 二进制缓存。

```sh
./build.py --help
./build.py build
./build.py test
./build.py test --target test_proto_smoke
./build.py test --cli-only
./build.py build -t example_02_reflected_row_and_zero_copy_views -j 2
./build.py bench --list
./build.py bench -t bench_arrow_ipc_codec
```

不带参数的 `./build.py` 等价于 `build`。产物位于 `build/<preset>/bin/`。

| 预设 | 配置 |
| --- | --- |
| `clang-glibc` | 使用 libc++ 的 Clang P2996，包位于 `build/<preset>/vcpkg_installed` |
| `gcc-glibc` | `$HOME/.local/toolchain/gcc-glibc` 的 GCC 16，包位于 `build/<preset>/vcpkg_installed` |
| `gcc-glibc-vcpkg` | `gcc-glibc` 的兼容别名 |
| `clang-asan` | 在 `clang-glibc` 上启用 AddressSanitizer 与 UndefinedBehaviorSanitizer |
| `gcc-asan` | 在 `gcc-glibc` 上启用 AddressSanitizer 与 UndefinedBehaviorSanitizer |
| `clang-musl` | 面向 musl sysroot 的 Clang P2996，包位于 `build/<preset>/vcpkg_installed` |
| `gcc-musl` | 面向 musl sysroot 的 GCC 16，包位于 `build/<preset>/vcpkg_installed` |

项目默认预设为 `gcc-glibc`：使用 `cmake --preset gcc-glibc` 配置，
使用 `ctest --preset gcc-glibc` 测试。

用 `./build.py --preset <name> build` 选择预设。安装后的消费方通过
`find_package(greptimedb CONFIG REQUIRED)` 和 SDK 的导出目标链接。
overlay 端口拉取固定在 `acb247415b8f4c2f0750783dab4e1f38c71f5860` 的
greptime-proto，并把它配置为 `GREPTIME_PROTO_DIR`。消息和反射 RPC 契约在构建树中
生成，不入库。

反射行的注解写在前置声明上：

```cpp
struct [[= db::table{"metric_samples"}]] MetricSample;
struct MetricSample {
  [[= db::tag{}]] std::string host;
  [[= db::timestamp{.unit = db::TimestampUnit::kNanosecond}]] std::int64_t ts;
  [[= db::field{}]] double cpu_usage;
};
```

需要真实 GreptimeDB 往返时：

```sh
./build.py test -j 2
GREPTIMEDB_E2E_REQUIRED=1 ctest --test-dir build/gcc-glibc -R e2e -V
GREPTIMEDB_TEST_ENDPOINT=grpc://127.0.0.1:4001 GREPTIMEDB_E2E_REQUIRED=1 \
  ctest --test-dir build/gcc-glibc -R e2e -V
```

没有端点时，套件启动或复用 `greptimedb-cxx-e2e` Docker 容器
（`greptime/greptimedb:latest`），并发布临时环回 gRPC 端口。缺少 Docker 时普通
运行会跳过；`GREPTIMEDB_E2E_REQUIRED=1` 使这种情况失败。显式端点失败和 RPC 失败
始终失败。用例创建唯一数据库并清理；外部端点和复用容器不会被删除。新建容器保持
运行，除非 `GREPTIMEDB_E2E_REMOVE_CONTAINER=1`。

## 可运行示例

十个示例由根 CMake 注册。`./build.py test --filter '^example\.'` 运行离线断言。
示例使用本地数据或调用方驱动的 HTTP/2 对端。设置 `GREPTIMEDB_ENDPOINT` 后，
示例 01、07、08、09 增加只读健康探测；示例 05 则写入在线表
`example_stream_samples`。CTest 在离线运行中清空该变量。

| `examples/` 下的源文件 | 二进制 | 演示 |
| --- | --- | --- |
| `01_quickstart_unary_ddl_and_crud.cc` | `example_01_quickstart_unary_ddl_and_crud` | 健康检查、DDL、列式/行式 CRUD、多表写入 |
| `02_reflected_row_and_zero_copy_views.cc` | `example_02_reflected_row_and_zero_copy_views` | 注解行、借用输入、类型化批次、解码视图 |
| `03_memory_policy_and_batch_reuse.cc` | `example_03_memory_policy_and_batch_reuse` | BufferPolicy、预留、保留与批次复用 |
| `04_full_column_type_matrix.cc` | `example_04_full_column_type_matrix` | 标量、可空、JSON、Decimal128 与时间戳 |
| `05_stream_inserter_and_backpressure.cc` | `example_05_stream_inserter_and_backpressure` | 客户端流、背压、完成与取消 |
| `06_arrow_flight_doput_bulk_writer.cc` | `example_06_arrow_flight_doput_bulk_writer` | Flight DoPut、压缩、分区、ACK 窗口 |
| `07_arrow_flight_doget_and_promql.cc` | `example_07_arrow_flight_doget_and_promql` | SQL DoGet、仅元数据结果、PromQL |
| `08_thread_local_and_worker_striping.cc` | `example_08_thread_local_and_worker_striping` | 应用线程、线程局部缓存、工作槽 |
| `09_tls_and_failover_circuit_breaker.cc` | `example_09_tls_and_failover_circuit_breaker` | TLS、端点选择、熔断恢复、重放规则 |
| `10_high_throughput_telemetry_pipeline.cc` | `example_10_high_throughput_telemetry_pipeline` | 高吞吐主机与网络遥测写入流水线及批次/流组合 |

## 基准

`./build.py bench -t <binary>`。Google Benchmark 参数放在 `--` 之后。CLI 打开
`GREPTIMEDB_BUILD_BENCHMARKS`。默认目标是 `bench_column_buf`。

| CMake 目标与二进制 | 负载 |
| --- | --- |
| `bench_column_buf` | 列推入、空值掩码、热复用/冷分配、Protobuf 序列化 |
| `bench_reflected_row` | 反射行写入与借用/类型化访问 |
| `bench_arrow_ipc_codec` | Arrow IPC 模式与批次编解码 |
| `bench_streaming_loopback` | 对环回对端的调用方驱动流 |

基准结果取决于硬件、编译器、构建类型和负载。环回测量不代表在线 GreptimeDB
吞吐或延迟。

## CMake 库目标

安装后的消费方使用 `find_package(greptimedb CONFIG REQUIRED)` 和
`greptimedb::greptimedb`。构建还导出这些领域别名：

| 目标 | 职责 |
| --- | --- |
| `greptimedb::greptimedb` | 客户端、行与线程模型的伞形目标 |
| `greptimedb::proto` | 生成的 Protobuf 消息与反射 RPC 契约 |
| `greptimedb::core` | 错误、配置、值、提示与缓冲策略 |
| `greptimedb::batch` | 列缓冲、动态批次与 Protobuf 批次编码 |
| `greptimedb::row` | 反射类型化批次、借用行、源/解码视图 |
| `greptimedb::flight` | 基于库内 IPC 编解码的 Flight DoPut 与 DoGet |
| `greptimedb::flight_ipc` | 内置 Arrow IPC/FlatBuffers、LZ4 Frame 与 Zstandard |
| `greptimedb::streaming` | StreamInserter、Flight BulkStreamWriter 与 QueryStream |
| `greptimedb::client` | 一元 Client、DDL、PromQL、健康检查与流工厂 |
| `greptimedb::threading` | ThreadLocalClient 与 WorkerClient 缓存/租约 |

命令选项见 [CLI 指南](scripts/cli/README.md)。
