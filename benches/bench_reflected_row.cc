#include "bench_allocation_probe.h"
#include "bench_fixture.h"

#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/row/wire_row_view.h>

#include <array>
#include <string_view>

namespace greptimedb::bench_fixture {
namespace {

using TypedBatch = TypedColumnarBatch<Tick>;

template <class Batch>
bool Fill(benchmark::State &state, Batch &batch, const Tick &tick) {
  const std::array<ValueView, 3> values{std::string_view(tick.symbol), tick.ts,
                                        tick.price};
  for (std::int64_t row = 0; row < state.range(0); ++row) {
    const bool accepted = [&] {
      if constexpr (std::same_as<Batch, TypedBatch>)
        return batch.Push(tick).has_value();
      else
        return batch.PushRow(values).has_value();
    }();
    if (!accepted) {
      state.SkipWithError("fixture ingestion failed");
      return false;
    }
  }
  return true;
}

template <class Batch> bool AddSchema(benchmark::State &state, Batch &batch) {
  if (!batch.AddTagColumn("symbol", ColumnDataType::kString) ||
      !batch.AddTimestampColumn("ts", TimestampUnit::kMillisecond) ||
      !batch.AddFieldColumn("price", ColumnDataType::kFloat64)) {
    state.SkipWithError("dynamic schema construction failed");
    return false;
  }
  return true;
}

/** Capacity is warmed outside timing; Clear and each row append are measured.
 */
template <bool kBorrowed> void ReflectedPush(benchmark::State &state) {
  TypedBatch batch(static_cast<std::size_t>(state.range(0)));
  const auto tick = Sample();
  if (!batch.ReserveBytes(tick.symbol.size() *
                          static_cast<std::size_t>(state.range(0))) ||
      !Fill(state, batch, tick)) {
    state.SkipWithError("warm reflected fixture failed");
    return;
  }
  const auto retained = batch.AllocatedBytes();
  const BorrowedRow<Tick> borrowed(tick);
  bench_allocation::count = 0;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    bench_allocation::enabled = kBorrowed;
    batch.Clear();
    for (std::int64_t row = 0; row < state.range(0); ++row) {
      const auto pushed = [&] {
        if constexpr (kBorrowed)
          return batch.PushRef(borrowed);
        else
          return batch.Push(tick);
      }();
      if (!pushed) {
        bench_allocation::enabled = false;
        state.SkipWithError("reflected push failed");
        return;
      }
    }
    bench_allocation::enabled = false;
    if (batch.AllocatedBytes() != retained) {
      state.SkipWithError("warmed push grew or released storage");
      return;
    }
    benchmark::DoNotOptimize(batch.RowCount());
    benchmark::ClobberMemory();
  }
  if constexpr (kBorrowed) {
    state.counters["cpp_allocations"] =
        static_cast<double>(bench_allocation::count);
    if (bench_allocation::count != 0)
      state.SkipWithError("warmed PushRef performed a C++ allocation");
  }
  Throughput(state, state.range(0),
             LogicalBytes(static_cast<std::size_t>(state.range(0))));
}

void ReflectedViews(benchmark::State &state) {
  TypedBatch batch;
  if (!Fill(state, batch, Sample()))
    return;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    double sum = 0;
    for (const auto row : batch.Views()) {
      sum += row.Get<2>();
      benchmark::DoNotOptimize(row.Get<0>().data());
    }
    benchmark::DoNotOptimize(sum);
  }
  Throughput(state, state.range(0),
             LogicalBytes(static_cast<std::size_t>(state.range(0))));
}

void WireRowViews(benchmark::State &state) {
  TypedBatch batch;
  if (!Fill(state, batch, Sample()))
    return;
  auto encoded = batch.ToInsertRequest();
  ::greptime::v1::InsertRequest decoded;
  if (!encoded || !decoded.ParseFromString(encoded->SerializeAsString())) {
    state.SkipWithError("protobuf fixture round trip failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    double sum = 0;
    for (std::size_t row = 0; row < decoded.row_count(); ++row) {
      auto view =
          ViewDecodedRow<Tick>(decoded.columns(), row, decoded.row_count());
      if (!view) {
        state.SkipWithError("decoded row projection failed");
        return;
      }
      sum += view->Get<2>();
      benchmark::DoNotOptimize(view->Get<0>().data());
    }
    benchmark::DoNotOptimize(sum);
  }
  Throughput(state, state.range(0),
             LogicalBytes(static_cast<std::size_t>(state.range(0))));
}

/** Identical logical rows and key columns are used for all three encoders. */
template <class Batch, bool kDelete>
void EncodeRequests(benchmark::State &state) {
  Batch batch = [&] {
    if constexpr (std::same_as<Batch, TypedBatch>)
      return Batch{};
    else
      return Batch{"benchmark_ticks"};
  }();
  if constexpr (!std::same_as<Batch, TypedBatch>) {
    if (!AddSchema(state, batch))
      return;
  }
  if (!Fill(state, batch, Sample()))
    return;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto encoded = [&] {
      if constexpr (std::same_as<Batch, RowBatch>) {
        if constexpr (kDelete)
          return batch.EncodeRowDeleteRequest();
        else
          return batch.EncodeRowInsertRequest();
      } else {
        if constexpr (kDelete)
          return batch.EncodeDeleteRequest();
        else
          return batch.EncodeInsertRequest();
      }
    }();
    if (!encoded) {
      state.SkipWithError("request encoding failed");
      return;
    }
    benchmark::DoNotOptimize(*encoded);
    benchmark::ClobberMemory();
  }
  Throughput(state, state.range(0),
             LogicalBytes(static_cast<std::size_t>(state.range(0))));
}

BENCHMARK_TEMPLATE(ReflectedPush, false)
    ->Name("ReflectedPush")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK_TEMPLATE(ReflectedPush, true)
    ->Name("BorrowedPushRefWarmClear")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK(ReflectedViews)->Arg(64)->Arg(1024);
BENCHMARK(WireRowViews)->Arg(64)->Arg(1024);
BENCHMARK_TEMPLATE(EncodeRequests, TypedBatch, false)
    ->Name("TypedToInsertRequest")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK_TEMPLATE(EncodeRequests, TypedBatch, true)
    ->Name("TypedToDeleteRequest")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK_TEMPLATE(EncodeRequests, ColumnarBatch, false)
    ->Name("DynamicToInsertRequest")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK_TEMPLATE(EncodeRequests, ColumnarBatch, true)
    ->Name("DynamicToDeleteRequest")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK_TEMPLATE(EncodeRequests, RowBatch, false)
    ->Name("RowToInsertRequest")
    ->Arg(64)
    ->Arg(1024);
BENCHMARK_TEMPLATE(EncodeRequests, RowBatch, true)
    ->Name("RowToDeleteRequest")
    ->Arg(64)
    ->Arg(1024);

} // namespace
} // namespace greptimedb::bench_fixture
