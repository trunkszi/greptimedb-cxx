#pragma once

#include <greptime/v1/column.pb.h>
#include <greptimedb/flight/ipc_flatbuffers.h>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace greptimedb::flight {
inline constexpr const char *kSemanticTypeKey = "greptime:semantic_type";
inline constexpr const char *kTypeExtensionKey = "greptime:type";
inline constexpr const char *kJsonTypeValue = "Json";
inline constexpr const char *kTableMetadataKey = "greptime:table";
inline constexpr const char *kTimeIndexKey = "greptime:time_index";
using FlightField = ipc::Field;
using FlightSchema = ipc::Schema;
using FlightType = ipc::Type;
using FlightTypeId = ipc::TypeId;
using FlightMetadata = ipc::Metadata;
template <class T> using FlightResult = std::expected<T, std::string>;

enum class FlightCompression : std::uint8_t { kNone, kLz4Frame, kZstd };
struct EncodedFlightBody {
  std::vector<uint8_t> data_header;
  std::vector<uint8_t> data_body;
};

// Columns use Protobuf's packed non-null values and one-bit-means-null masks.
struct RecordBatch {
  FlightSchema schema;
  std::vector<::greptime::v1::Column> columns;
  size_t row_count = 0;
};
using FlightTimestampBounds = std::pair<std::optional<int64_t>, std::optional<int64_t>>;

[[nodiscard]] FlightResult<RecordBatch> RecordBatchFromProtoColumns(std::span<const ::greptime::v1::Column> columns,
                                                                    size_t row_count);
[[nodiscard]] FlightResult<FlightTimestampBounds> TimestampBounds(const RecordBatch &batch);
[[nodiscard]] FlightResult<std::vector<RecordBatch>> PartitionRecordBatchByHour(const RecordBatch &batch);

class FlightIpcEncoder {
public:
  explicit FlightIpcEncoder(FlightCompression compression = FlightCompression::kNone) : compression_(compression) {}
  [[nodiscard]] FlightResult<EncodedFlightBody> EncodeSchema(const FlightSchema &schema) const;
  [[nodiscard]] FlightResult<EncodedFlightBody> EncodeBatch(const RecordBatch &batch) const;
  [[nodiscard]] static FlightResult<RecordBatch>
  RecordBatchFromProtoColumns(std::span<const ::greptime::v1::Column> columns, size_t rows) {
    return flight::RecordBatchFromProtoColumns(columns, rows);
  }
  [[nodiscard]] static FlightResult<FlightTimestampBounds> TimestampBounds(const RecordBatch &batch) {
    return flight::TimestampBounds(batch);
  }
  [[nodiscard]] static FlightResult<std::vector<RecordBatch>> PartitionRecordBatchByHour(const RecordBatch &batch) {
    return flight::PartitionRecordBatchByHour(batch);
  }

private:
  FlightCompression compression_;
};

// Metadata-only Flight frames carry a NONE header and no Arrow body.
[[nodiscard]] FlightResult<EncodedFlightBody> EncodeMetadataMessage();
[[nodiscard]] FlightResult<bool> IsMetadataOnlyMessage(std::span<const uint8_t> data_header);

[[nodiscard]] FlightResult<FlightSchema> DecodeFlightSchema(std::span<const uint8_t> data_header);
[[nodiscard]] FlightResult<RecordBatch> DecodeFlightRecordBatch(const FlightSchema &schema,
                                                                std::span<const uint8_t> data_header,
                                                                std::span<const uint8_t> data_body);
} // namespace greptimedb::flight
