#include <benchmark/benchmark.h>
#include <greptimedb/batch/column_buf.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace greptimedb {
namespace {

/** Fill one batch while reporting allocation or validation failures to the
 * runner. */
bool Fill(benchmark::State &state, TypedColumnBuf &column,
          const ValueView &value) {
  for (std::int64_t row = 0; row < state.range(0); ++row) {
    if (!column.Push(value)) {
      state.SkipWithError("column push failed");
      return false;
    }
  }
  benchmark::DoNotOptimize(column.NonNullLen());
  benchmark::ClobberMemory();
  return true;
}

void SetThroughput(benchmark::State &state, std::int64_t bytes_per_row) {
  const auto items = state.iterations() * state.range(0);
  state.SetItemsProcessed(items);
  state.SetBytesProcessed(items * bytes_per_row);
}

void NullMaskPush(benchmark::State &state) {
  NullMask mask;
  if (!mask.ReserveRows(static_cast<std::size_t>(state.range(0)))) {
    state.SkipWithError("null mask reserve failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    mask.ClearUnbounded();
    for (std::int64_t row = 0; row < state.range(0); ++row) {
      if (!mask.Push(row % 8 == 0)) {
        state.SkipWithError("null mask push failed");
        return;
      }
    }
    benchmark::DoNotOptimize(mask.RawBytes().data());
    benchmark::DoNotOptimize(mask.NullCount());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

void Ingest(benchmark::State &state, ColumnDataType type,
            const ValueView &value, std::int64_t bytes_per_row) {
  auto column =
      TypedColumnBuf::Create("value", SemanticType::kField, type,
                             static_cast<std::size_t>(state.range(0)));
  if (!column || !Fill(state, *column, value)) {
    if (!column)
      state.SkipWithError("column construction failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    column->ClearUnbounded();
    if (!Fill(state, *column, value))
      return;
  }
  SetThroughput(state, bytes_per_row);
}

void IngestInt64(benchmark::State &state) {
  Ingest(state, ColumnDataType::kInt64, std::int64_t{42}, sizeof(std::int64_t));
}

void IngestFloat64(benchmark::State &state) {
  Ingest(state, ColumnDataType::kFloat64, 42.125, sizeof(double));
}

void IngestString(benchmark::State &state) {
  const std::string payload(128, 's');
  Ingest(state, ColumnDataType::kString, std::string_view(payload),
         payload.size());
}

void IngestDecimal128(benchmark::State &state) {
  const Decimal128 value{static_cast<__int128>(1234567890123456789LL), 38, 8};
  Ingest(state, ColumnDataType::Decimal(value.precision, value.scale), value,
         16);
}

/** Both cases ingest identical string rows; only warm retains payload storage.
 */
template <bool kWarm> void BufferPolicyClear(benchmark::State &state) {
  const std::string payload(256, 'p');
  const ValueView value{std::string_view(payload)};
  if constexpr (kWarm) {
    auto column = TypedColumnBuf::Create(
        "value", SemanticType::kField, ColumnDataType::kString,
        static_cast<std::size_t>(state.range(0)));
    if (!column) {
      state.SkipWithError("column construction failed");
      return;
    }
    if (!Fill(state, *column, value))
      return;
    const auto retained_bytes = column->AllocatedBytes();
    column->Clear();
    if (column->AllocatedBytes() != retained_bytes) {
      state.SkipWithError("warm fixture exceeds default retention budget");
      return;
    }
    for (auto iteration : state) {
      static_cast<void>(iteration);
      if (!Fill(state, *column, value))
        return;
      column->Clear();
      benchmark::ClobberMemory();
    }
  } else {
    for (auto iteration : state) {
      static_cast<void>(iteration);
      auto column = TypedColumnBuf::Create(
          "value", SemanticType::kField, ColumnDataType::kString,
          static_cast<std::size_t>(state.range(0)));
      if (!column) {
        state.SkipWithError("column construction failed");
        return;
      }
      if (!Fill(state, *column, value))
        return;
      column->ClearWithPolicy(BufferPolicy::Zero());
      benchmark::ClobberMemory();
    }
  }
  SetThroughput(state, static_cast<std::int64_t>(payload.size()));
}

void ColumnSerializeToString(benchmark::State &state) {
  auto column = TypedColumnBuf::Create(
      "value", SemanticType::kField, ColumnDataType::kInt64,
      static_cast<std::size_t>(state.range(0)));
  if (!column) {
    state.SkipWithError("column construction failed");
    return;
  }
  for (std::int64_t row = 0; row < state.range(0); ++row) {
    const ValueView value =
        row % 17 == 0 ? ValueView{std::monostate{}} : ValueView{row};
    if (!column->Push(value)) {
      state.SkipWithError("serialization fixture push failed");
      return;
    }
  }
  const auto wire = column->ToProtoColumn();
  std::string serialized;
  if (!wire.SerializeToString(&serialized)) {
    state.SkipWithError("serialization fixture failed");
    return;
  }
  const auto wire_bytes = static_cast<std::int64_t>(serialized.size());
  for (auto iteration : state) {
    static_cast<void>(iteration);
    if (!wire.SerializeToString(&serialized)) {
      state.SkipWithError("column serialization failed");
      return;
    }
    benchmark::DoNotOptimize(serialized.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  state.SetBytesProcessed(state.iterations() * wire_bytes);
}

BENCHMARK(NullMaskPush)->Arg(64)->Arg(1024)->Arg(65536);
BENCHMARK(IngestInt64)->Arg(64)->Arg(1024)->Arg(65536);
BENCHMARK(IngestFloat64)->Arg(64)->Arg(1024)->Arg(65536);
BENCHMARK(IngestString)->Arg(64)->Arg(1024)->Arg(4096);
BENCHMARK(IngestDecimal128)->Arg(64)->Arg(1024)->Arg(65536);
BENCHMARK_TEMPLATE(BufferPolicyClear, true)
    ->Name("BufferPolicyWarmClear")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK_TEMPLATE(BufferPolicyClear, false)
    ->Name("BufferPolicyColdAllocation")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK(ColumnSerializeToString)->Arg(64)->Arg(1024)->Arg(65536);

} // namespace
} // namespace greptimedb
