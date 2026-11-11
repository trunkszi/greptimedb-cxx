#include "ipc_codec_internal.hh"

namespace greptimedb::flight {
using namespace codec;

FlightResult<RecordBatch> RecordBatchFromProtoColumns(std::span<const Column> columns, size_t rows) {
  if (rows > kMaxRows || columns.size() > kMaxFields || (rows && columns.empty()) ||
      (rows && columns.size() > kMaxCells / rows))
    return Fail("batch exceeds size policy or has no columns");
  RecordBatch result;
  result.row_count = rows;
  size_t proto_bytes = 0;
  for (const auto &c : columns) {
    const size_t bytes = c.ByteSizeLong();
    if (bytes > kMaxBody - proto_bytes) return Fail("proto batch exceeds size policy");
    proto_bytes += bytes;
    auto field = FieldFromColumn(c);
    if (!field) return std::unexpected(field.error());
    auto layout = GetLayout(*field);
    if (!layout) return std::unexpected(layout.error());
    auto count = ValidateColumn(c, *field, rows, *layout);
    if (!count) return std::unexpected(count.error());
    result.schema.fields.push_back(std::move(*field));
    result.columns.push_back(c);
  }
  auto valid = ValidateSchema(result.schema);
  if (!valid) return std::unexpected(valid.error());
  return result;
}
FlightResult<EncodedFlightBody> FlightIpcEncoder::EncodeSchema(const FlightSchema &schema) const {
  auto valid = ValidateSchema(schema);
  if (!valid) return std::unexpected(valid.error());
  auto header = ipc::EncodeSchema(schema);
  if (!header) return std::unexpected(header.error());
  return EncodedFlightBody{std::move(*header), {}};
}
FlightResult<EncodedFlightBody> FlightIpcEncoder::EncodeBatch(const RecordBatch &batch) const {
  if (compression_ != FlightCompression::kNone && compression_ != FlightCompression::kLz4Frame &&
      compression_ != FlightCompression::kZstd)
    return Fail("unsupported Flight compression");
  auto valid = ValidateSchema(batch.schema);
  if (!valid) return std::unexpected(valid.error());
  if (batch.row_count > kMaxRows || batch.columns.size() != batch.schema.fields.size() ||
      (batch.row_count && batch.columns.empty()) ||
      (batch.row_count && batch.columns.size() > kMaxCells / batch.row_count))
    return Fail("invalid batch shape");
  ipc::Batch metadata;
  metadata.length = static_cast<int64_t>(batch.row_count);
  switch (compression_) {
  case FlightCompression::kNone: metadata.compression = ipc::Compression::kNone; break;
  case FlightCompression::kLz4Frame: metadata.compression = ipc::Compression::kLz4Frame; break;
  case FlightCompression::kZstd: metadata.compression = ipc::Compression::kZstd; break;
  }
  Bytes body;
  for (size_t i = 0; i < batch.columns.size(); ++i) {
    auto encoded =
        EncodeColumn(batch.columns[i], batch.schema.fields[i], batch.row_count, body, metadata, compression_);
    if (!encoded) return std::unexpected(encoded.error());
  }
  auto header = ipc::EncodeRecordBatch(metadata, static_cast<int64_t>(body.size()));
  if (!header) return std::unexpected(header.error());
  return EncodedFlightBody{std::move(*header), std::move(body)};
}
FlightResult<EncodedFlightBody> EncodeMetadataMessage() {
  auto header = ipc::EncodeMetadataMessage();
  if (!header) return std::unexpected(header.error());
  return EncodedFlightBody{std::move(*header), {}};
}
FlightResult<bool> IsMetadataOnlyMessage(std::span<const uint8_t> header) { return ipc::IsMetadataOnlyMessage(header); }
} // namespace greptimedb::flight
