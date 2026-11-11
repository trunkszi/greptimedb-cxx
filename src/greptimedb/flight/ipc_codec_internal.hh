#pragma once

#include <greptimedb/flight/ipc_codec.h>

#include "ipc_compression.hh"
#include "ipc_flatbuffers_internal.hh"

#include <google/protobuf/descriptor.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace greptimedb::flight::codec {

using Column = ::greptime::v1::Column;
using Values = ::greptime::v1::Column::Values;
using Bytes = std::vector<uint8_t>;
using ipc::ValidUtf8;

inline constexpr size_t kMaxBody = detail::kMaxBody;
inline constexpr size_t kMaxRows = 16U * 1024U * 1024U;
inline constexpr size_t kMaxFields = 4096;
inline constexpr size_t kMaxCells = 16U * 1024U * 1024U;
inline constexpr size_t kMaxPartitions = 4096;

struct Layout {
  int datatype = 0;
  const char *values = nullptr;
  size_t width = 0;
  bool variable = false;
  bool view = false;
};

[[nodiscard, gnu::cold, gnu::noinline]] std::unexpected<std::string> Fail(const char *why);
std::string_view Meta(const ipc::Metadata &metadata, std::string_view key);
bool NullAt(const Column &column, size_t row);
void Put(Bytes &bytes, uint64_t value, size_t width);
uint64_t Read(std::span<const uint8_t> bytes, size_t pos, size_t width);
[[nodiscard]] FlightResult<Layout> GetLayout(const FlightField &field);
[[nodiscard]] FlightResult<int> Semantic(const FlightField &field);
[[nodiscard]] FlightResult<void> ValidateSchema(const FlightSchema &schema);
[[nodiscard]] FlightResult<FlightField> FieldFromColumn(const Column &column);
const google::protobuf::FieldDescriptor *ValueField(const Layout &layout);
[[nodiscard]] FlightResult<size_t> ValidateColumn(const Column &column, const FlightField &field, size_t rows,
                                                  const Layout &layout);
[[nodiscard]] FlightResult<void> ValidateBatch(const RecordBatch &batch);
[[nodiscard]] FlightResult<void> AppendNative(Bytes &out, const Values &values, const Layout &layout, size_t index);
void AppendDecoded(Values &values, const Layout &layout, std::span<const uint8_t> data, size_t pos);
[[nodiscard]] FlightResult<void> AddBuffer(Bytes &body, ipc::Batch &meta, const Bytes &raw, FlightCompression codec);
[[nodiscard]] FlightResult<void> EncodeColumn(const Column &column, const FlightField &field, size_t rows, Bytes &body,
                                              ipc::Batch &meta, FlightCompression codec);
[[nodiscard]] FlightResult<size_t> TimeColumn(const RecordBatch &batch);

} // namespace greptimedb::flight::codec
