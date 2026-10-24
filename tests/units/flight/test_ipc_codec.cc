#include <greptimedb/flight/ipc_codec.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <vector>

namespace {
namespace flight = greptimedb::flight;
namespace ipc = greptimedb::flight::ipc;
using Column = greptime::v1::Column;
using flight::FlightCompression;
using flight::FlightIpcEncoder;

constexpr std::array kTypes{greptime::v1::BOOLEAN,
                            greptime::v1::INT8,
                            greptime::v1::INT16,
                            greptime::v1::INT32,
                            greptime::v1::INT64,
                            greptime::v1::UINT8,
                            greptime::v1::UINT16,
                            greptime::v1::UINT32,
                            greptime::v1::UINT64,
                            greptime::v1::FLOAT32,
                            greptime::v1::FLOAT64,
                            greptime::v1::BINARY,
                            greptime::v1::STRING,
                            greptime::v1::DATE,
                            greptime::v1::DATETIME,
                            greptime::v1::TIMESTAMP_SECOND,
                            greptime::v1::TIMESTAMP_MILLISECOND,
                            greptime::v1::TIMESTAMP_MICROSECOND,
                            greptime::v1::TIMESTAMP_NANOSECOND,
                            greptime::v1::DECIMAL128,
                            greptime::v1::JSON};

Column MakeColumn(greptime::v1::ColumnDataType type, std::size_t rows,
                  bool nullable = true) {
  Column column;
  column.set_column_name("c" + std::to_string(static_cast<int>(type)));
  column.set_datatype(type);
  column.set_semantic_type(type == greptime::v1::STRING ? greptime::v1::TAG
                                                        : greptime::v1::FIELD);
  if (type == greptime::v1::DECIMAL128) {
    auto *decimal = column.mutable_datatype_extension()->mutable_decimal_type();
    decimal->set_precision(38);
    decimal->set_scale(7);
  }
  std::string mask((rows + 7) / 8, '\0');
  for (std::size_t row = 0; row < rows; ++row) {
    if (nullable && (row == 1 || row == 7 || row == 8)) {
      mask[row / 8] |= static_cast<char>(1U << (row % 8));
      continue;
    }
    auto *values = column.mutable_values();
    const auto n = static_cast<int64_t>(row);
    if (type == greptime::v1::BOOLEAN)
      values->add_bool_values(row % 2 == 0);
    if (type == greptime::v1::INT8)
      values->add_i8_values(-128 + n);
    if (type == greptime::v1::INT16)
      values->add_i16_values(-32768 + n);
    if (type == greptime::v1::INT32)
      values->add_i32_values(-1234567 + n);
    if (type == greptime::v1::INT64)
      values->add_i64_values(std::numeric_limits<int64_t>::min() + n);
    if (type == greptime::v1::UINT8)
      values->add_u8_values(255 - n);
    if (type == greptime::v1::UINT16)
      values->add_u16_values(65535 - n);
    if (type == greptime::v1::UINT32)
      values->add_u32_values(std::numeric_limits<uint32_t>::max() - n);
    if (type == greptime::v1::UINT64)
      values->add_u64_values(std::numeric_limits<uint64_t>::max() - n);
    if (type == greptime::v1::FLOAT32)
      values->add_f32_values(-0.125F + n);
    if (type == greptime::v1::FLOAT64)
      values->add_f64_values(-0.0625 + n);
    if (type == greptime::v1::BINARY)
      values->add_binary_values(std::string{"\0\xff", 2});
    if (type == greptime::v1::JSON)
      values->add_binary_values("{\"n\":" + std::to_string(n) + "}");
    if (type == greptime::v1::STRING)
      values->add_string_values("symbol-" + std::to_string(n));
    if (type == greptime::v1::DATE)
      values->add_date_values(-1234 + n);
    if (type == greptime::v1::DATETIME)
      values->add_datetime_values(-86400000 + n);
    if (type == greptime::v1::TIMESTAMP_SECOND)
      values->add_timestamp_second_values(-3600 + n);
    if (type == greptime::v1::TIMESTAMP_MILLISECOND)
      values->add_timestamp_millisecond_values(-3600000 + n);
    if (type == greptime::v1::TIMESTAMP_MICROSECOND)
      values->add_timestamp_microsecond_values(-3600000000LL + n);
    if (type == greptime::v1::TIMESTAMP_NANOSECOND)
      values->add_timestamp_nanosecond_values(-3600000000000LL + n);
    if (type == greptime::v1::DECIMAL128) {
      auto *decimal = values->add_decimal128_values();
      decimal->set_hi(-1);
      decimal->set_lo(-123456789 + n);
    }
  }
  if (nullable && rows > 1)
    column.set_null_mask(mask);
  return column;
}

uint64_t ReadLittle(std::span<const uint8_t> bytes, std::size_t at,
                    std::size_t width) {
  uint64_t value = 0;
  for (std::size_t i = 0; i < width; ++i)
    value |= uint64_t{bytes[at + i]} << (8 * i);
  return value;
}

void WriteLittle(std::vector<uint8_t> &bytes, std::size_t at, uint64_t value,
                 std::size_t width) {
  for (std::size_t i = 0; i < width; ++i)
    bytes[at + i] = value >> (8 * i);
}

// Test-side navigation uses the Arrow FlatBuffers field numbers, independent of
// the decoder.
std::size_t FieldAt(const std::vector<uint8_t> &bytes, std::size_t table,
                    unsigned field) {
  const auto distance = static_cast<int32_t>(ReadLittle(bytes, table, 4));
  const auto vtable =
      static_cast<std::size_t>(static_cast<int64_t>(table) - distance);
  return table + ReadLittle(bytes, vtable + 4 + field * 2, 2);
}

std::size_t Indirect(const std::vector<uint8_t> &bytes, std::size_t at) {
  return at + ReadLittle(bytes, at, 4);
}

std::vector<uint8_t> Encapsulate(const std::vector<uint8_t> &raw, bool marker) {
  std::vector<uint8_t> result(marker ? 8 : 4);
  if (marker)
    WriteLittle(result, 0, 0xffffffffU, 4);
  WriteLittle(result, marker ? 4 : 0, raw.size(), 4);
  result.insert(result.end(), raw.begin(), raw.end());
  return result;
}

void ExpectColumns(const flight::RecordBatch &batch,
                   const std::vector<Column> &columns, std::size_t rows) {
  ASSERT_EQ(batch.row_count, rows);
  ASSERT_EQ(batch.columns.size(), columns.size());
  for (std::size_t i = 0; i < columns.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(batch.columns[i].column_name(), columns[i].column_name());
    EXPECT_EQ(batch.columns[i].datatype(), columns[i].datatype());
    EXPECT_EQ(batch.columns[i].semantic_type(), columns[i].semantic_type());
    EXPECT_EQ(batch.columns[i].null_mask(), columns[i].null_mask());
    EXPECT_EQ(batch.columns[i].values().SerializeAsString(),
              columns[i].values().SerializeAsString());
  }
}

TEST(FlightIpcUnitScenario,
     AllPrimitiveTypesAndNullableCompactValuesRoundTrip) {
  for (const auto compression :
       {FlightCompression::kNone, FlightCompression::kLz4Frame,
        FlightCompression::kZstd}) {
    std::vector<Column> columns;
    for (auto type : kTypes)
      columns.push_back(MakeColumn(type, 11));
    auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
    ASSERT_TRUE(batch) << batch.error();
    batch->schema.metadata.emplace_back(flight::kTableMetadataKey, "ticks");
    FlightIpcEncoder encoder(compression);
    auto schema_frame = encoder.EncodeSchema(batch->schema);
    ASSERT_TRUE(schema_frame) << schema_frame.error();
    auto schema = flight::DecodeFlightSchema(schema_frame->data_header);
    ASSERT_TRUE(schema) << schema.error();
    ASSERT_EQ(schema->fields.size(), kTypes.size());
    EXPECT_EQ(schema->metadata, batch->schema.metadata);
    for (std::size_t i = 0; i < columns.size(); ++i) {
      EXPECT_EQ(schema->fields[i].metadata, batch->schema.fields[i].metadata);
      EXPECT_EQ(schema->fields[i].type.id, batch->schema.fields[i].type.id);
    }
    auto frame = encoder.EncodeBatch(*batch);
    ASSERT_TRUE(frame) << frame.error();
    auto received = flight::DecodeFlightRecordBatch(*schema, frame->data_header,
                                                    frame->data_body);
    ASSERT_TRUE(received) << received.error();
    ExpectColumns(*received, columns, 11);
    EXPECT_EQ(
        received->columns[19].datatype_extension().decimal_type().precision(),
        38);
    EXPECT_EQ(received->columns[19].datatype_extension().decimal_type().scale(),
              7);
    auto message = ipc::DecodeMessage(frame->data_header);
    ASSERT_TRUE(message);
    EXPECT_EQ(message->body_length,
              static_cast<int64_t>(frame->data_body.size()));
    for (auto buffer : message->batch.buffers)
      EXPECT_EQ(buffer.offset % 8, 0);
  }
}

TEST(FlightIpcUnitScenario, EmptyAndAllNullBatchesRoundTrip) {
  for (auto compression :
       {FlightCompression::kNone, FlightCompression::kLz4Frame,
        FlightCompression::kZstd}) {
    for (std::size_t rows : {std::size_t{0}, std::size_t{9}}) {
      std::vector<Column> columns;
      for (auto type : kTypes) {
        auto column = MakeColumn(type, 0);
        if (rows)
          column.set_null_mask(std::string{"\xff\x01", 2});
        columns.push_back(std::move(column));
      }
      auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, rows);
      ASSERT_TRUE(batch) << batch.error();
      auto frame = FlightIpcEncoder(compression).EncodeBatch(*batch);
      ASSERT_TRUE(frame) << frame.error();
      auto decoded = flight::DecodeFlightRecordBatch(
          batch->schema, frame->data_header, frame->data_body);
      ASSERT_TRUE(decoded) << decoded.error();
      ExpectColumns(*decoded, columns, rows);
    }
  }
  flight::RecordBatch empty;
  auto frame = FlightIpcEncoder().EncodeBatch(empty);
  ASSERT_TRUE(frame) << frame.error();
  auto decoded = flight::DecodeFlightRecordBatch(
      empty.schema, frame->data_header, frame->data_body);
  ASSERT_TRUE(decoded) << decoded.error();
  EXPECT_EQ(decoded->row_count, 0U);
}

TEST(FlightIpcUnitScenario, UsesArrowValidityAndLittleEndianPhysicalLayouts) {
  auto column = MakeColumn(greptime::v1::INT32, 11);
  std::vector<Column> columns{column};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto message = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(message);
  ASSERT_EQ(message->batch.nodes.size(), 1U);
  EXPECT_EQ(message->batch.nodes[0].null_count, 3);
  ASSERT_EQ(message->batch.buffers.size(), 2U);
  const auto validity = message->batch.buffers[0];
  const auto values = message->batch.buffers[1];
  ASSERT_GE(validity.length, 2);
  EXPECT_EQ(frame->data_body[validity.offset] & 0xff, 0x7d);
  EXPECT_EQ(frame->data_body[validity.offset + 1] & 0x07, 0x06);
  ASSERT_EQ(values.length, 44);
  EXPECT_EQ(
      static_cast<int32_t>(ReadLittle(frame->data_body, values.offset, 4)),
      -1234567);
  EXPECT_EQ(
      static_cast<int32_t>(ReadLittle(frame->data_body, values.offset + 8, 4)),
      -1234565);
  auto decoded = flight::DecodeFlightRecordBatch(
      batch->schema, frame->data_header, frame->data_body);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->columns[0].values().i32_values_size(), 8);
}

TEST(FlightIpcUnitScenario, BareAndEncapsulatedHeadersAreEquivalent) {
  std::vector<Column> columns{MakeColumn(greptime::v1::STRING, 11)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
  ASSERT_TRUE(batch);
  auto schema_frame = FlightIpcEncoder().EncodeSchema(batch->schema);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(schema_frame);
  ASSERT_TRUE(frame);
  for (bool marker : {false, true}) {
    auto schema = flight::DecodeFlightSchema(
        Encapsulate(schema_frame->data_header, marker));
    ASSERT_TRUE(schema) << schema.error();
    auto decoded = flight::DecodeFlightRecordBatch(
        *schema, Encapsulate(frame->data_header, marker), frame->data_body);
    ASSERT_TRUE(decoded) << decoded.error();
    ExpectColumns(*decoded, columns, 11);
    auto bad = Encapsulate(frame->data_header, marker);
    WriteLittle(bad, marker ? 4 : 0, frame->data_header.size() + 100, 4);
    EXPECT_FALSE(
        flight::DecodeFlightRecordBatch(*schema, bad, frame->data_body));
  }
}

TEST(FlightIpcUnitScenario,
     PartitionUsesNativeTimestampUnitsAndNegativeHourFloor) {
  constexpr std::array ticks{int64_t{3600}, int64_t{3600000},
                             int64_t{3600000000}, int64_t{3600000000000}};
  for (std::size_t unit = 0; unit < ticks.size(); ++unit) {
    const auto hour = ticks[unit];
    auto ts = MakeColumn(kTypes[15 + unit], 0, false);
    ts.set_semantic_type(greptime::v1::TIMESTAMP);
    auto label = MakeColumn(greptime::v1::STRING, 0);
    const std::array times{hour, int64_t{-1}, int64_t{0}, -hour, hour - 1};
    for (std::size_t i = 0; i < times.size(); ++i) {
      if (unit == 0)
        ts.mutable_values()->add_timestamp_second_values(times[i]);
      if (unit == 1)
        ts.mutable_values()->add_timestamp_millisecond_values(times[i]);
      if (unit == 2)
        ts.mutable_values()->add_timestamp_microsecond_values(times[i]);
      if (unit == 3)
        ts.mutable_values()->add_timestamp_nanosecond_values(times[i]);
      if (i != 1)
        label.mutable_values()->add_string_values(std::to_string(i));
    }
    label.set_null_mask(std::string(1, '\x02'));
    std::vector<Column> columns{ts, label};
    auto batch =
        FlightIpcEncoder::RecordBatchFromProtoColumns(columns, times.size());
    ASSERT_TRUE(batch) << batch.error();
    auto bounds = FlightIpcEncoder::TimestampBounds(*batch);
    ASSERT_TRUE(bounds) << bounds.error();
    EXPECT_EQ(bounds->first, -hour);
    EXPECT_EQ(bounds->second, hour);
    auto partitions = FlightIpcEncoder::PartitionRecordBatchByHour(*batch);
    ASSERT_TRUE(partitions) << partitions.error();
    ASSERT_EQ(partitions->size(), 3U);
    EXPECT_EQ((*partitions)[0].row_count, 2U);
    EXPECT_EQ((*partitions)[1].row_count, 2U);
    EXPECT_EQ((*partitions)[2].row_count, 1U);
    EXPECT_EQ((*partitions)[0].columns[1].null_mask(), std::string(1, '\x01'));
    EXPECT_EQ((*partitions)[0].columns[1].values().string_values(0), "3");
    EXPECT_EQ((*partitions)[1].columns[1].values().string_values(0), "2");
    EXPECT_EQ((*partitions)[1].columns[1].values().string_values(1), "4");
    EXPECT_EQ((*partitions)[2].columns[1].values().string_values(0), "0");
  }
}

TEST(FlightIpcUnitScenario, MinimumNanosecondTimestampAndMissingTimeIndex) {
  auto ts = MakeColumn(greptime::v1::TIMESTAMP_NANOSECOND, 0, false);
  ts.set_semantic_type(greptime::v1::TIMESTAMP);
  ts.mutable_values()->add_timestamp_nanosecond_values(
      std::numeric_limits<int64_t>::min());
  ts.mutable_values()->add_timestamp_nanosecond_values(0);
  std::vector<Column> columns{ts};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 2);
  ASSERT_TRUE(batch);
  auto parts = FlightIpcEncoder::PartitionRecordBatchByHour(*batch);
  ASSERT_TRUE(parts) << parts.error();
  ASSERT_EQ(parts->size(), 2U);
  EXPECT_EQ((*parts)[0].columns[0].values().timestamp_nanosecond_values(0),
            std::numeric_limits<int64_t>::min());
  columns = {MakeColumn(greptime::v1::INT32, 2)};
  auto projection = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 2);
  ASSERT_TRUE(projection);
  EXPECT_FALSE(FlightIpcEncoder::TimestampBounds(*projection));
  EXPECT_FALSE(FlightIpcEncoder::PartitionRecordBatchByHour(*projection));
  columns = {MakeColumn(greptime::v1::INT32, 0)};
  auto empty = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 0);
  ASSERT_TRUE(empty);
  auto empty_parts = FlightIpcEncoder::PartitionRecordBatchByHour(*empty);
  ASSERT_TRUE(empty_parts);
  ASSERT_EQ(empty_parts->size(), 1U);
  auto bounds = FlightIpcEncoder::TimestampBounds(*empty);
  ASSERT_TRUE(bounds);
  EXPECT_FALSE(bounds->first);
  EXPECT_FALSE(bounds->second);
}

TEST(FlightIpcUnitScenario, RejectsInvalidProtoCountsRangesAndTimestampNulls) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 3)};
  columns[0].mutable_values()->add_i32_values(17);
  EXPECT_FALSE(FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3));
  columns = {MakeColumn(greptime::v1::UINT8, 1, false)};
  columns[0].mutable_values()->set_u8_values(0, 256);
  EXPECT_FALSE(FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1));
  columns = {MakeColumn(greptime::v1::INT8, 1, false)};
  columns[0].mutable_values()->set_i8_values(0, -129);
  EXPECT_FALSE(FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1));
  columns = {MakeColumn(greptime::v1::TIMESTAMP_MILLISECOND, 3)};
  columns[0].set_semantic_type(greptime::v1::TIMESTAMP);
  EXPECT_FALSE(FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3));
  columns = {MakeColumn(greptime::v1::DECIMAL128, 1, false)};
  columns[0].mutable_datatype_extension()->mutable_decimal_type()->set_scale(
      -1);
  EXPECT_FALSE(FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1));
}

TEST(FlightIpcUnitScenario, RejectsTruncatedAndCorruptFlatbufferTables) {
  std::vector<Column> columns{MakeColumn(greptime::v1::STRING, 3)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto schema = FlightIpcEncoder().EncodeSchema(batch->schema);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(schema);
  ASSERT_TRUE(frame);
  for (std::size_t size = 0; size < 20; ++size) {
    EXPECT_FALSE(
        flight::DecodeFlightSchema(std::span(schema->data_header).first(size)));
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(
        batch->schema, std::span(frame->data_header).first(size),
        frame->data_body));
  }
  auto bad = schema->data_header;
  WriteLittle(bad, 0, 0xfffffff0U, 4);
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  bad = schema->data_header;
  auto root = ReadLittle(bad, 0, 4);
  WriteLittle(bad, root, 0x7fffffffU, 4);
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  bad = schema->data_header;
  root = ReadLittle(bad, 0, 4);
  bad[FieldAt(bad, root, 1)] = 4; // Tensor is not a Schema or RecordBatch.
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  EXPECT_FALSE(
      flight::DecodeFlightRecordBatch(batch->schema, bad, frame->data_body));
  bad = schema->data_header;
  root = ReadLittle(bad, 0, 4);
  bad[FieldAt(bad, root, 1)] =
      2; // DictionaryBatch must be explicitly rejected by codec.
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  EXPECT_FALSE(
      flight::DecodeFlightRecordBatch(batch->schema, bad, frame->data_body));
  EXPECT_FALSE(flight::DecodeFlightSchema(frame->data_header));
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(
      batch->schema, schema->data_header, frame->data_body));
  bad = schema->data_header;
  const auto schema_table =
      Indirect(bad, FieldAt(bad, ReadLittle(bad, 0, 4), 2));
  const auto fields = Indirect(bad, FieldAt(bad, schema_table, 1));
  WriteLittle(bad, fields, 0xffffffffU, 4);
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  bad = schema->data_header;
  const auto schema_object =
      Indirect(bad, FieldAt(bad, ReadLittle(bad, 0, 4), 2));
  const auto field_vector = Indirect(bad, FieldAt(bad, schema_object, 1));
  const auto field = Indirect(bad, field_vector + 4);
  bad[FieldAt(bad, field, 2)] = 255;
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  bad = schema->data_header;
  const auto field_vtable = field - ReadLittle(bad, field, 4);
  const auto object_size = ReadLittle(bad, field_vtable + 2, 2);
  WriteLittle(bad, field_vtable + 4, object_size - 1, 2);
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  bad = schema->data_header;
  const auto name = Indirect(bad, FieldAt(bad, field, 0));
  bad[name + 4] = 0xff;
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
}

TEST(FlightIpcUnitScenario, RejectsBodyLengthBitmapOffsetsAndNodeMismatches) {
  std::vector<Column> columns{MakeColumn(greptime::v1::STRING, 3)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto decoded = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(decoded);
  auto bad_body = frame->data_body;
  bad_body.pop_back();
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                               frame->data_header, bad_body));
  bad_body = frame->data_body;
  bad_body.push_back(0);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                               frame->data_header, bad_body));
  auto meta = decoded->batch;
  meta.nodes[0].length = 2;
  auto header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header,
                                               frame->data_body));
  meta = decoded->batch;
  meta.nodes[0].null_count = 0;
  header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header,
                                               frame->data_body));
  meta = decoded->batch;
  meta.buffers[0].length = 0;
  header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header,
                                               frame->data_body));
  bad_body = frame->data_body;
  const auto offsets = decoded->batch.buffers[1];
  WriteLittle(bad_body, offsets.offset + 4, 0x7fffffffU, 4);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                               frame->data_header, bad_body));
  auto bad_header = frame->data_header;
  const auto table = Indirect(
      bad_header, FieldAt(bad_header, ReadLittle(bad_header, 0, 4), 2));
  const auto buffers = Indirect(bad_header, FieldAt(bad_header, table, 2));
  WriteLittle(bad_header, buffers + 4, std::numeric_limits<uint64_t>::max(), 8);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, bad_header,
                                               frame->data_body));
  WriteLittle(bad_header, buffers + 4, frame->data_body.size() + 8, 8);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, bad_header,
                                               frame->data_body));
}

TEST(FlightIpcUnitScenario, CompressionFramesRejectCorruptionAndSizeBombs) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT64, 128, false)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 128);
  ASSERT_TRUE(batch);
  for (auto kind : {FlightCompression::kLz4Frame, FlightCompression::kZstd}) {
    auto frame = FlightIpcEncoder(kind).EncodeBatch(*batch);
    ASSERT_TRUE(frame) << frame.error();
    auto decoded = ipc::DecodeMessage(frame->data_header);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->batch.buffers.size(), 2U);
    const auto buffer = decoded->batch.buffers[1];
    ASSERT_GE(buffer.length, 12);
    auto body = frame->data_body;
    WriteLittle(body, buffer.offset, std::numeric_limits<int64_t>::max(), 8);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                                 frame->data_header, body));
    body = frame->data_body;
    WriteLittle(body, buffer.offset, static_cast<uint64_t>(-2), 8);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                                 frame->data_header, body));
    body = frame->data_body;
    body[buffer.offset + 8] ^= 0xff;
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                                 frame->data_header, body));
    auto meta = decoded->batch;
    meta.buffers[1].length = 7;
    auto header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
    ASSERT_TRUE(header);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header,
                                                 frame->data_body));
    body = frame->data_body;
    WriteLittle(body, buffer.offset, 1023, 8);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema,
                                                 frame->data_header, body));
    body = frame->data_body;
    body.insert(body.begin() + buffer.offset + buffer.length, 0);
    meta = decoded->batch;
    ++meta.buffers[1].length;
    header = ipc::EncodeRecordBatch(meta, body.size());
    ASSERT_TRUE(header);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, body));
  }
}

TEST(FlightIpcUnitScenario, CompressionMinusOnePrefixMeansRawBuffer) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 3)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto decoded = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(decoded);
  for (auto compression :
       {ipc::Compression::kLz4Frame, ipc::Compression::kZstd}) {
    auto meta = decoded->batch;
    std::vector<uint8_t> body;
    for (auto &buffer : meta.buffers) {
      const auto original = buffer;
      while (body.size() % 8)
        body.push_back(0);
      buffer.offset = body.size();
      buffer.length = original.length + 8;
      body.resize(body.size() + 8, 0xff);
      body.insert(body.end(), frame->data_body.begin() + original.offset,
                  frame->data_body.begin() + original.offset + original.length);
    }
    while (body.size() % 8)
      body.push_back(0);
    meta.compression = compression;
    auto header = ipc::EncodeRecordBatch(meta, body.size());
    ASSERT_TRUE(header);
    auto result = flight::DecodeFlightRecordBatch(batch->schema, *header, body);
    ASSERT_TRUE(result) << result.error();
    ExpectColumns(*result, columns, 3);
  }
}

TEST(FlightIpcUnitScenario, DecodesLargeOffsetsAndNullWithoutPrimitiveBuffers) {
  for (auto type : {ipc::TypeId::kLargeUtf8, ipc::TypeId::kLargeBinary}) {
    flight::FlightSchema schema;
    flight::FlightField field;
    field.name = "large";
    field.type.id = type;
    schema.fields.push_back(field);
    ipc::Batch metadata;
    metadata.length = 3;
    metadata.nodes.push_back({3, 1});
    metadata.buffers = {{0, 1}, {8, 32}, {40, 5}};
    std::vector<uint8_t> body(48);
    body[0] = 0x05;
    WriteLittle(body, 8, 0, 8);
    WriteLittle(body, 16, 2, 8);
    WriteLittle(body, 24, 2, 8);
    WriteLittle(body, 32, 5, 8);
    body[40] = 'A';
    body[41] = 0;
    body[42] = 'B';
    body[43] = 'T';
    body[44] = 'C';
    auto header = ipc::EncodeRecordBatch(metadata, body.size());
    ASSERT_TRUE(header);
    auto decoded = flight::DecodeFlightRecordBatch(schema, *header, body);
    ASSERT_TRUE(decoded) << decoded.error();
    ASSERT_EQ(decoded->columns.size(), 1U);
    const auto &c = decoded->columns[0];
    EXPECT_EQ(c.null_mask(), std::string(1, '\x02'));
    if (type == ipc::TypeId::kLargeUtf8) {
      EXPECT_EQ(c.datatype(), greptime::v1::STRING);
      ASSERT_EQ(c.values().string_values_size(), 2);
      EXPECT_EQ(c.values().string_values(0), std::string("A\0", 2));
      EXPECT_EQ(c.values().string_values(1), "BTC");
    } else {
      EXPECT_EQ(c.datatype(), greptime::v1::BINARY);
      ASSERT_EQ(c.values().binary_values_size(), 2);
      EXPECT_EQ(c.values().binary_values(0), std::string("A\0", 2));
      EXPECT_EQ(c.values().binary_values(1), "BTC");
    }
    WriteLittle(body, 24, std::numeric_limits<uint64_t>::max(), 8);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(schema, *header, body));
  }
  flight::FlightSchema schema;
  flight::FlightField field;
  field.name = "nulls";
  field.type.id = ipc::TypeId::kNull;
  schema.fields.push_back(field);
  ipc::Batch metadata;
  metadata.length = 9;
  metadata.nodes.push_back({9, 9});
  auto header = ipc::EncodeRecordBatch(metadata, 0);
  ASSERT_TRUE(header);
  auto decoded = flight::DecodeFlightRecordBatch(schema, *header, {});
  ASSERT_TRUE(decoded) << decoded.error();
  ASSERT_EQ(decoded->columns.size(), 1U);
  EXPECT_EQ(decoded->columns[0].null_mask(), std::string("\xff\x01", 2));
  EXPECT_EQ(decoded->columns[0].values().binary_values_size(), 0);
  metadata.nodes[0].null_count = 8;
  header = ipc::EncodeRecordBatch(metadata, 0);
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(schema, *header, {}));
}

TEST(FlightIpcUnitScenario, EmptyValidityMeansAllValidAndExtraBuffersFail) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 3, false)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto message = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(message);
  ASSERT_EQ(message->batch.buffers.size(), 2U);
  EXPECT_EQ(message->batch.buffers[0].length, 0);
  auto result = flight::DecodeFlightRecordBatch(
      batch->schema, frame->data_header, frame->data_body);
  ASSERT_TRUE(result);
  EXPECT_TRUE(result->columns[0].null_mask().empty());
  auto meta = message->batch;
  meta.buffers.push_back({static_cast<int64_t>(frame->data_body.size()), 0});
  auto header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header,
                                               frame->data_body));
  meta = message->batch;
  meta.buffers.pop_back();
  header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header,
                                               frame->data_body));
}

TEST(FlightIpcUnitScenario, TimeIndexMetadataMustDescribeNonNullableTimestamp) {
  std::vector<Column> columns{
      MakeColumn(greptime::v1::TIMESTAMP_MILLISECOND, 2, false)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 2);
  ASSERT_TRUE(batch);
  auto &field = batch->schema.fields[0];
  field.nullable = false;
  field.metadata = {{flight::kTimeIndexKey, "true"}};
  auto frame = FlightIpcEncoder().EncodeSchema(batch->schema);
  ASSERT_TRUE(frame);
  auto decoded = flight::DecodeFlightSchema(frame->data_header);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->fields[0].metadata, field.metadata);
  field.nullable = true;
  EXPECT_FALSE(FlightIpcEncoder().EncodeSchema(batch->schema));
  field.nullable = false;
  field.type.timezone = "UTC";
  EXPECT_FALSE(FlightIpcEncoder().EncodeSchema(batch->schema));
  field.type.timezone.clear();
  field.metadata = {{flight::kTimeIndexKey, "sometimes"}};
  EXPECT_FALSE(FlightIpcEncoder().EncodeSchema(batch->schema));
  field.metadata = {{flight::kTimeIndexKey, "true"}};
  field.type.id = ipc::TypeId::kInt;
  field.type.bit_width = 32;
  field.type.is_signed = true;
  EXPECT_FALSE(FlightIpcEncoder().EncodeSchema(batch->schema));
  EXPECT_FALSE(FlightIpcEncoder::TimestampBounds(*batch));
  EXPECT_FALSE(FlightIpcEncoder::PartitionRecordBatchByHour(*batch));
}

TEST(FlightIpcUnitScenario, SchemaProtoMismatchFailsBeforeTimestampReflection) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 3, false)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto &field = batch->schema.fields[0];
  field.nullable = false;
  field.metadata = {{flight::kSemanticTypeKey, "timestamp"}};
  field.type.id = ipc::TypeId::kTimestamp;
  field.type.unit = 1;
  EXPECT_FALSE(FlightIpcEncoder().EncodeBatch(*batch));
  EXPECT_FALSE(FlightIpcEncoder::TimestampBounds(*batch));
  EXPECT_FALSE(FlightIpcEncoder::PartitionRecordBatchByHour(*batch));
}

TEST(FlightIpcUnitScenario, RejectsInvalidUtf8StringsAndPreservesOpaqueJson) {
  for (auto type : {greptime::v1::STRING, greptime::v1::JSON}) {
    std::vector<Column> columns{MakeColumn(type, 1, false)};
    if (type == greptime::v1::STRING)
      columns[0].mutable_values()->set_string_values(
          0, std::string("\xc0\x80", 2));
    else
      columns[0].mutable_values()->set_binary_values(
          0, std::string("\xed\xa0\x80", 3));
    auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1);
    if (type == greptime::v1::STRING) {
      EXPECT_FALSE(batch);
      continue;
    }
    ASSERT_TRUE(batch);
    auto frame = FlightIpcEncoder().EncodeBatch(*batch);
    ASSERT_TRUE(frame);
    auto decoded = flight::DecodeFlightRecordBatch(
        batch->schema, frame->data_header, frame->data_body);
    ASSERT_TRUE(decoded);
    ExpectColumns(*decoded, columns, 1);
  }
}

TEST(FlightIpcUnitScenario, BooleanValuesUsePackedBitsRatherThanOneBytePerRow) {
  std::vector<Column> columns{MakeColumn(greptime::v1::BOOLEAN, 11)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
  ASSERT_TRUE(batch);
  EXPECT_FALSE(FlightIpcEncoder(static_cast<FlightCompression>(255))
                   .EncodeBatch(*batch));
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto message = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(message);
  ASSERT_EQ(message->batch.buffers.size(), 2U);
  auto data = message->batch.buffers[1];
  EXPECT_EQ(data.length, 2);
  EXPECT_EQ(frame->data_body[data.offset], 0x55);
  EXPECT_EQ(frame->data_body[data.offset + 1] & 0x07, 0x04);
}
TEST(FlightIpcUnitScenario, MetadataOnlyFramesAreExplicitAndNeverTypedBatches) {
  auto frame = flight::EncodeMetadataMessage();
  ASSERT_TRUE(frame) << frame.error();
  EXPECT_TRUE(frame->data_body.empty());
  for (auto header :
       {frame->data_header, Encapsulate(frame->data_header, true)}) {
    auto metadata = flight::IsMetadataOnlyMessage(header);
    ASSERT_TRUE(metadata) << metadata.error();
    EXPECT_TRUE(*metadata);
    auto message = ipc::DecodeMessage(header);
    ASSERT_TRUE(message);
    EXPECT_EQ(message->header_type, ipc::HeaderType::kNone);
    EXPECT_EQ(message->body_length, 0);
    EXPECT_FALSE(flight::DecodeFlightSchema(header));
    EXPECT_FALSE(flight::DecodeFlightRecordBatch({}, header, {}));
  }
  for (std::size_t size = 0; size < 4; ++size)
    EXPECT_FALSE(flight::IsMetadataOnlyMessage(
        std::span(frame->data_header).first(size)));
  auto malformed = frame->data_header;
  WriteLittle(malformed, 0, 0xfffffff0U, 4);
  EXPECT_FALSE(flight::IsMetadataOnlyMessage(malformed));
  malformed = frame->data_header;
  WriteLittle(malformed, FieldAt(malformed, ReadLittle(malformed, 0, 4), 3), 1,
              8);
  EXPECT_FALSE(flight::IsMetadataOnlyMessage(malformed));
  auto continuation = Encapsulate(frame->data_header, true);
  continuation.resize(7);
  EXPECT_FALSE(flight::IsMetadataOnlyMessage(continuation));
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 1, false)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1);
  ASSERT_TRUE(batch);
  auto schema = FlightIpcEncoder().EncodeSchema(batch->schema);
  auto record = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(schema);
  ASSERT_TRUE(record);
  for (auto header : {schema->data_header, record->data_header}) {
    auto metadata = flight::IsMetadataOnlyMessage(header);
    ASSERT_TRUE(metadata) << metadata.error();
    EXPECT_FALSE(*metadata);
  }
}
} // namespace
