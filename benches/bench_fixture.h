#pragma once

#include <benchmark/benchmark.h>
#include <greptimedb/flight/ipc_codec.h>
#include <greptimedb/row/reflected_row.h>

#include <array>
#include <cstdint>
#include <string>

namespace greptimedb::bench_fixture {

struct[[= table{.name = "benchmark_ticks"}]] Tick {
  [[= tag{}]] std::string symbol;
  [[= timestamp{}]] TimestampMillisecond ts;
  double price;
};

inline Tick Sample() {
  return {"benchmark-symbol-with-owned-payload", {1'700'000'000'000}, 42.125};
}

/** Deterministic input shared by the codec and real HTTP/2 loopback suites. */
inline flight::FlightResult<flight::RecordBatch> MakeBatch(std::size_t rows) {
  std::array<::greptime::v1::Column, 3> columns;
  columns[0].set_column_name("symbol");
  columns[0].set_semantic_type(::greptime::v1::TAG);
  columns[0].set_datatype(::greptime::v1::STRING);
  columns[1].set_column_name("ts");
  columns[1].set_semantic_type(::greptime::v1::TIMESTAMP);
  columns[1].set_datatype(::greptime::v1::TIMESTAMP_MILLISECOND);
  columns[2].set_column_name("price");
  columns[2].set_semantic_type(::greptime::v1::FIELD);
  columns[2].set_datatype(::greptime::v1::FLOAT64);
  const auto tick = Sample();
  for (std::size_t row = 0; row < rows; ++row) {
    columns[0].mutable_values()->add_string_values(tick.symbol);
    columns[1].mutable_values()->add_timestamp_millisecond_values(
        tick.ts.value + static_cast<std::int64_t>(row) * 60'000);
    columns[2].mutable_values()->add_f64_values(tick.price +
                                                static_cast<double>(row % 17));
  }
  return flight::RecordBatchFromProtoColumns(columns, rows);
}

inline void Throughput(benchmark::State &state, std::int64_t rows,
                       std::int64_t bytes) {
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * bytes);
}

inline std::int64_t LogicalBytes(std::size_t rows) {
  return static_cast<std::int64_t>(
      rows * (Sample().symbol.size() + 2 * sizeof(std::int64_t)));
}

} // namespace greptimedb::bench_fixture
