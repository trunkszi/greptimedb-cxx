#include <greptimedb/flight/ipc_codec.h>
#include <gtest/gtest.h>
#include <lz4frame.h>
#include <zstd.h>

#include <array>
#include <bit>
#include <limits>

namespace {
namespace flight = greptimedb::flight;
namespace ipc = flight::ipc;
namespace v1 = greptime::v1;
using Bytes = std::vector<uint8_t>;

uint64_t Read(const Bytes &bytes, size_t at, size_t width) {
  uint64_t result = 0;
  for (size_t i = 0; i < width; ++i)
    result |= uint64_t{bytes[at + i]} << (8 * i);
  return result;
}
void Put(Bytes &bytes, size_t at, uint64_t value, size_t width = 8) {
  for (size_t i = 0; i < width; ++i)
    bytes[at + i] = static_cast<uint8_t>(value >> (8 * i));
}
size_t Field(const Bytes &bytes, size_t table, size_t index) {
  const auto vt = static_cast<size_t>(
      static_cast<int64_t>(table) -
      std::bit_cast<int32_t>(static_cast<uint32_t>(Read(bytes, table, 4))));
  return table + Read(bytes, vt + 4 + index * 2, 2);
}
size_t Indirect(const Bytes &bytes, size_t at) {
  return at + Read(bytes, at, 4);
}

flight::RecordBatch BoundaryBatch(size_t rows, bool all_null) {
  constexpr std::array types{
      v1::DATE,       v1::DATETIME, v1::TIMESTAMP_NANOSECOND,
      v1::DECIMAL128, v1::JSON,     v1::BINARY,
      v1::STRING,     v1::BOOLEAN};
  std::vector<v1::Column> columns;
  for (auto type : types) {
    v1::Column column;
    column.set_column_name(std::to_string(type));
    column.set_datatype(type);
    column.set_semantic_type(v1::FIELD);
    if (type == v1::DECIMAL128) {
      column.mutable_datatype_extension()
          ->mutable_decimal_type()
          ->set_precision(38);
      column.mutable_datatype_extension()->mutable_decimal_type()->set_scale(7);
    }
    if (type == v1::JSON)
      column.mutable_datatype_extension()
          ->mutable_json_native_type()
          ->set_datatype(v1::BINARY);
    std::string mask((rows + 7) / 8, '\0');
    for (size_t row = 0; row < rows; ++row) {
      if (all_null || row % 3 == 1) {
        mask[row / 8] |= static_cast<char>(1U << (row % 8));
        continue;
      }
      auto *values = column.mutable_values();
      const auto extreme = row % 2 ? std::numeric_limits<int64_t>::max()
                                   : std::numeric_limits<int64_t>::min();
      if (type == v1::DATE)
        values->add_date_values(static_cast<int32_t>(row) - 20);
      if (type == v1::DATETIME)
        values->add_datetime_values(extreme);
      if (type == v1::TIMESTAMP_NANOSECOND)
        values->add_timestamp_nanosecond_values(extreme);
      if (type == v1::DECIMAL128) {
        auto *decimal = values->add_decimal128_values();
        decimal->set_hi(row % 2 ? -1 : 0);
        decimal->set_lo(extreme);
      }
      if (type == v1::JSON || type == v1::BINARY)
        values->add_binary_values(row % 2 ? std::string("\0\xff", 2) : "");
      if (type == v1::STRING)
        values->add_string_values(row % 2 ? "\xe4\xb8\xad" : "");
      if (type == v1::BOOLEAN)
        values->add_bool_values(row % 2 != 0);
    }
    if (rows)
      column.set_null_mask(mask);
    columns.push_back(std::move(column));
  }
  auto batch = flight::RecordBatchFromProtoColumns(columns, rows);
  EXPECT_TRUE(batch);
  return batch ? std::move(*batch) : flight::RecordBatch{};
}

TEST(FlightIpcAuditUnitScenario,
     EmptyAllNullAndPartialBitmapPreservePackedValues) {
  for (size_t rows : {0U, 65U, 73U}) {
    for (bool all_null : {false, true}) {
      const auto batch = BoundaryBatch(rows, all_null);
      for (auto codec : {flight::FlightCompression::kNone,
                         flight::FlightCompression::kLz4Frame,
                         flight::FlightCompression::kZstd}) {
        auto encoded = flight::FlightIpcEncoder(codec).EncodeBatch(batch);
        ASSERT_TRUE(encoded) << encoded.error();
        auto decoded = flight::DecodeFlightRecordBatch(
            batch.schema, encoded->data_header, encoded->data_body);
        ASSERT_TRUE(decoded) << decoded.error();
        ASSERT_EQ(decoded->columns.size(), batch.columns.size());
        for (size_t i = 0; i < batch.columns.size(); ++i) {
          EXPECT_EQ(decoded->columns[i].values().SerializeAsString(),
                    batch.columns[i].values().SerializeAsString());
          EXPECT_EQ(decoded->columns[i].null_mask(),
                    batch.columns[i].null_mask());
          EXPECT_EQ(
              decoded->columns[i].datatype_extension().SerializeAsString(),
              batch.columns[i].datatype_extension().SerializeAsString());
        }
      }
    }
  }
}

TEST(FlightIpcAuditUnitScenario, NullArrayAcrossPartialTrailingByte) {
  flight::FlightField field;
  field.name = "nulls";
  field.type.id = ipc::TypeId::kNull;
  flight::FlightSchema schema{{field}, {}, true};
  for (int64_t rows : {0, 65, 73}) {
    ipc::Batch batch;
    batch.length = rows;
    batch.nodes.push_back({rows, rows});
    auto header = ipc::EncodeRecordBatch(batch, 0);
    ASSERT_TRUE(header);
    auto decoded = flight::DecodeFlightRecordBatch(schema, *header, {});
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->columns.size(), 1U);
    const auto &mask = decoded->columns[0].null_mask();
    ASSERT_EQ(mask.size(), static_cast<size_t>((rows + 7) / 8));
    if (rows) {
      EXPECT_EQ(static_cast<uint8_t>(mask.back()), 1U);
    }
    auto encoded = flight::FlightIpcEncoder().EncodeBatch(*decoded);
    ASSERT_TRUE(encoded) << encoded.error();
    EXPECT_TRUE(encoded->data_body.empty());
  }
}

TEST(FlightIpcAuditUnitScenario, RejectsLayoutBombBeforeDecompression) {
  v1::Column column;
  column.set_column_name("value");
  column.set_datatype(v1::INT64);
  column.mutable_values()->add_i64_values(7);
  auto batch = flight::RecordBatchFromProtoColumns(std::span{&column, 1}, 1);
  ASSERT_TRUE(batch);
  for (auto codec : {flight::FlightCompression::kLz4Frame,
                     flight::FlightCompression::kZstd}) {
    auto encoded = flight::FlightIpcEncoder(codec).EncodeBatch(*batch);
    ASSERT_TRUE(encoded);
    auto message = ipc::DecodeMessage(encoded->data_header);
    ASSERT_TRUE(message);
    auto &data = message->batch.buffers[1];
    // Fits the aggregate 256MiB cap but cannot belong to this one-row Int64
    // array.
    Put(encoded->data_body, data.offset, 128U * 1024U * 1024U);
    auto decoded = flight::DecodeFlightRecordBatch(
        batch->schema, encoded->data_header, encoded->data_body);
    ASSERT_FALSE(decoded);
    EXPECT_EQ(decoded.error(), "invalid or excessive decompressed size");
  }
}

TEST(FlightIpcAuditUnitScenario, SignedHeaderRangesCannotWrapOrEscapeBody) {
  const auto batch = BoundaryBatch(1, false);
  auto encoded = flight::FlightIpcEncoder().EncodeBatch(batch);
  ASSERT_TRUE(encoded);
  const size_t message = Read(encoded->data_header, 0, 4);
  const size_t record =
      Indirect(encoded->data_header, Field(encoded->data_header, message, 2));
  const size_t nodes =
      Indirect(encoded->data_header, Field(encoded->data_header, record, 1));
  const size_t buffers =
      Indirect(encoded->data_header, Field(encoded->data_header, record, 2));
  for (auto [at, value] : std::array<std::pair<size_t, uint64_t>, 7>{
           {{Field(encoded->data_header, message, 3), uint64_t{1} << 63},
            {Field(encoded->data_header, record, 0), uint64_t{1} << 63},
            {nodes + 4, std::numeric_limits<uint64_t>::max()},
            {nodes + 12, std::numeric_limits<uint64_t>::max()},
            {buffers + 4, uint64_t{1} << 63},
            {buffers + 12, uint64_t{1} << 63},
            {buffers + 4, std::numeric_limits<int64_t>::max()}}}) {
    auto header = encoded->data_header;
    Put(header, at, value);
    EXPECT_FALSE(ipc::DecodeMessage(header));
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch.schema, header,
                                                 encoded->data_body));
  }
  for (auto displacement : {0x80000000U, 0x7fffffffU}) {
    auto header = encoded->data_header;
    Put(header, message, displacement, 4);
    EXPECT_FALSE(ipc::DecodeMessage(header));
  }
  auto header = encoded->data_header;
  Put(header, Field(header, message, 2), 0xfffffff0U, 4);
  EXPECT_FALSE(ipc::DecodeMessage(header));
}

TEST(FlightIpcAuditUnitScenario, UnalignedAndOverlappingBodyBuffersFailClosed) {
  const auto batch = BoundaryBatch(2, false);
  auto encoded = flight::FlightIpcEncoder().EncodeBatch(batch);
  ASSERT_TRUE(encoded);
  auto message = ipc::DecodeMessage(encoded->data_header);
  ASSERT_TRUE(message);
  for (int64_t offset : {1, 0}) {
    auto meta = message->batch;
    meta.buffers[2].offset = offset;
    auto header = ipc::EncodeRecordBatch(meta, encoded->data_body.size());
    ASSERT_TRUE(header);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch.schema, *header,
                                                 encoded->data_body));
  }
}

TEST(FlightIpcAuditUnitScenario,
     InvalidEmptyBatchesDoNotBypassTimestampValidation) {
  auto batch = BoundaryBatch(0, false);
  batch.columns.pop_back();
  EXPECT_FALSE(flight::TimestampBounds(batch));
  EXPECT_FALSE(flight::PartitionRecordBatchByHour(batch));
}

TEST(FlightIpcAuditUnitScenario,
     CompressedZeroLengthFramesDecodeEmptyBooleanArrays) {
  flight::FlightField field;
  field.name = "bools";
  field.type.id = ipc::TypeId::kBool;
  const flight::FlightSchema schema{{field}, {}, true};
  for (auto codec : {ipc::Compression::kLz4Frame, ipc::Compression::kZstd}) {
    Bytes compressed(128);
    size_t length = 0;
    if (codec == ipc::Compression::kLz4Frame) {
      length = LZ4F_compressFrame(compressed.data(), compressed.size(), "", 0,
                                  nullptr);
      ASSERT_FALSE(LZ4F_isError(length));
    } else {
      length = ZSTD_compress(compressed.data(), compressed.size(), "", 0, 1);
      ASSERT_FALSE(ZSTD_isError(length));
    }
    compressed.resize(length);
    Bytes body;
    ipc::Batch batch;
    batch.compression = codec;
    batch.nodes.push_back({0, 0});
    for (unsigned i = 0; i < 2; ++i) {
      batch.buffers.push_back({static_cast<int64_t>(body.size()),
                               static_cast<int64_t>(8 + length)});
      body.resize(body.size() + 8, 0);
      body.insert(body.end(), compressed.begin(), compressed.end());
      body.resize((body.size() + 7) & ~size_t{7});
    }
    auto header = ipc::EncodeRecordBatch(batch, body.size());
    ASSERT_TRUE(header);
    auto decoded = flight::DecodeFlightRecordBatch(schema, *header, body);
    ASSERT_TRUE(decoded) << decoded.error();
    EXPECT_EQ(decoded->row_count, 0U);
    ASSERT_EQ(decoded->columns.size(), 1U);
    EXPECT_EQ(decoded->columns[0].values().bool_values_size(), 0);
  }
}

TEST(FlightIpcAuditUnitScenario,
     ExtremeSecondsPartitionWithoutNegationOrMultiplicationOverflow) {
  v1::Column column;
  column.set_column_name("ts");
  column.set_datatype(v1::TIMESTAMP_SECOND);
  column.set_semantic_type(v1::TIMESTAMP);
  for (auto value : {std::numeric_limits<int64_t>::min(), int64_t{-1},
                     int64_t{0}, std::numeric_limits<int64_t>::max()})
    column.mutable_values()->add_timestamp_second_values(value);
  auto batch = flight::RecordBatchFromProtoColumns(std::span{&column, 1}, 4);
  ASSERT_TRUE(batch);
  auto parts = flight::PartitionRecordBatchByHour(*batch);
  ASSERT_TRUE(parts) << parts.error();
  ASSERT_EQ(parts->size(), 4U);
  for (size_t i = 0; i < parts->size(); ++i)
    EXPECT_EQ((*parts)[i].columns[0].values().timestamp_second_values(0),
              column.values().timestamp_second_values(static_cast<int>(i)));
}
} // namespace
