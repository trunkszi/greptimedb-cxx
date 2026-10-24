#include "bench_fixture.h"

#include <vector>

namespace greptimedb::bench_fixture {
namespace {

using namespace flight;

template <bool kEncode> void SchemaMessage(benchmark::State &state) {
  auto batch = MakeBatch(1);
  if (!batch) {
    state.SkipWithError("schema fixture failed");
    return;
  }
  FlightIpcEncoder encoder;
  auto message = encoder.EncodeSchema(batch->schema);
  if (!message) {
    state.SkipWithError("schema encoding fixture failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    const bool ok = [&] {
      if constexpr (kEncode) {
        auto result = encoder.EncodeSchema(batch->schema);
        if (!result)
          return false;
        benchmark::DoNotOptimize(result->data_header.data());
      } else {
        auto result = DecodeFlightSchema(message->data_header);
        if (!result || result->fields.size() != batch->schema.fields.size())
          return false;
        benchmark::DoNotOptimize(result->fields.data());
      }
      benchmark::ClobberMemory();
      return true;
    }();
    if (!ok) {
      state.SkipWithError("schema codec failed");
      return;
    }
  }
  Throughput(state, 1, static_cast<std::int64_t>(message->data_header.size()));
}

template <FlightCompression kCompression, bool kEncode>
void RecordBatchMessage(benchmark::State &state) {
  auto batch = MakeBatch(static_cast<std::size_t>(state.range(0)));
  if (!batch) {
    state.SkipWithError("record batch fixture failed");
    return;
  }
  FlightIpcEncoder encoder(kCompression);
  auto message = encoder.EncodeBatch(*batch);
  if (!message) {
    state.SkipWithError("encoded record batch fixture failed");
    return;
  }
  auto roundtrip = DecodeFlightRecordBatch(batch->schema, message->data_header,
                                           message->data_body);
  if (!roundtrip || roundtrip->row_count != batch->row_count) {
    state.SkipWithError("record batch fixture round trip failed");
    return;
  }
  state.counters["wire_bytes"] = static_cast<double>(
      message->data_header.size() + message->data_body.size());
  for (auto iteration : state) {
    static_cast<void>(iteration);
    const bool ok = [&] {
      if constexpr (kEncode) {
        auto result = encoder.EncodeBatch(*batch);
        if (!result)
          return false;
        benchmark::DoNotOptimize(result->data_body.data());
      } else {
        auto result = DecodeFlightRecordBatch(
            batch->schema, message->data_header, message->data_body);
        if (!result || result->row_count != batch->row_count)
          return false;
        benchmark::DoNotOptimize(result->columns.data());
      }
      benchmark::ClobberMemory();
      return true;
    }();
    if (!ok) {
      state.SkipWithError("record batch codec failed");
      return;
    }
  }
  Throughput(state, state.range(0), LogicalBytes(batch->row_count));
}

void PartitionBatchByHour(benchmark::State &state) {
  auto batch = MakeBatch(static_cast<std::size_t>(state.range(0)));
  if (!batch) {
    state.SkipWithError("partition fixture failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto partitions = PartitionRecordBatchByHour(*batch);
    if (!partitions) {
      state.SkipWithError("hour partitioning failed");
      return;
    }
    std::size_t total = 0;
    for (const auto &partition : *partitions)
      total += partition.row_count;
    if (total != batch->row_count || partitions->size() < 2) {
      state.SkipWithError("partition fixture did not preserve multi-hour rows");
      return;
    }
    benchmark::DoNotOptimize(partitions->data());
    benchmark::ClobberMemory();
  }
  Throughput(state, state.range(0), LogicalBytes(batch->row_count));
}

void ProtoColumnsToArrowBatch(benchmark::State &state) {
  auto batch = MakeBatch(static_cast<std::size_t>(state.range(0)));
  if (!batch) {
    state.SkipWithError("protobuf conversion fixture failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto converted =
        RecordBatchFromProtoColumns(batch->columns, batch->row_count);
    if (!converted || converted->row_count != batch->row_count) {
      state.SkipWithError("protobuf to Arrow batch conversion failed");
      return;
    }
    benchmark::DoNotOptimize(converted->columns.data());
    benchmark::ClobberMemory();
  }
  Throughput(state, state.range(0), LogicalBytes(batch->row_count));
}

/** RecordBatch stores decoded non-null values in protobuf columns. The reverse
 * conversion is therefore an owned copy, rather than a second Arrow decode. */
void ArrowBatchToProtoColumnsCopy(benchmark::State &state) {
  auto source = MakeBatch(static_cast<std::size_t>(state.range(0)));
  FlightIpcEncoder encoder;
  if (!source) {
    state.SkipWithError("Arrow conversion fixture failed");
    return;
  }
  auto encoded = encoder.EncodeBatch(*source);
  if (!encoded) {
    state.SkipWithError("Arrow conversion fixture encoding failed");
    return;
  }
  auto decoded = DecodeFlightRecordBatch(source->schema, encoded->data_header,
                                         encoded->data_body);
  if (!decoded) {
    state.SkipWithError("Arrow conversion fixture decoding failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    std::vector<::greptime::v1::Column> columns = decoded->columns;
    benchmark::DoNotOptimize(columns.data());
    benchmark::ClobberMemory();
  }
  Throughput(state, state.range(0), LogicalBytes(decoded->row_count));
}

BENCHMARK_TEMPLATE(SchemaMessage, true)->Name("EncodeSchemaMessage");
BENCHMARK_TEMPLATE(SchemaMessage, false)->Name("DecodeSchemaMessage");
BENCHMARK_TEMPLATE(RecordBatchMessage, FlightCompression::kNone, true)
    ->Name("EncodeRecordBatchMessages/None")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK_TEMPLATE(RecordBatchMessage, FlightCompression::kLz4Frame, true)
    ->Name("EncodeRecordBatchMessages/Lz4Frame")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK_TEMPLATE(RecordBatchMessage, FlightCompression::kZstd, true)
    ->Name("EncodeRecordBatchMessages/Zstd")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK_TEMPLATE(RecordBatchMessage, FlightCompression::kNone, false)
    ->Name("DecodeRecordBatchMessage/None")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK_TEMPLATE(RecordBatchMessage, FlightCompression::kLz4Frame, false)
    ->Name("DecodeRecordBatchMessage/Lz4Frame")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK_TEMPLATE(RecordBatchMessage, FlightCompression::kZstd, false)
    ->Name("DecodeRecordBatchMessage/Zstd")
    ->Arg(64)
    ->Arg(4096);
BENCHMARK(PartitionBatchByHour)->Arg(64)->Arg(4096);
BENCHMARK(ProtoColumnsToArrowBatch)->Arg(64)->Arg(4096);
BENCHMARK(ArrowBatchToProtoColumnsCopy)->Arg(64)->Arg(4096);

} // namespace
} // namespace greptimedb::bench_fixture
