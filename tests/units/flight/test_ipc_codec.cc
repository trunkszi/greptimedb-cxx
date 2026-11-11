#include "test_ipc_codec_support.hh"

namespace {
TEST(FlightIpcUnitScenario, PrimitiveNullableRoundTrip) {
  for (const auto compression : {FlightCompression::kNone, FlightCompression::kLz4Frame, FlightCompression::kZstd}) {
    std::vector<Column> columns;
    for (auto type : kTypes)
      columns.push_back(MakeColumn(type, 11));
    auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
    ASSERT_TRUE(batch) << batch.error();
    batch->schema.metadata.emplace_back(flight::kTableMetadataKey, "samples");
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
    auto received = flight::DecodeFlightRecordBatch(*schema, frame->data_header, frame->data_body);
    ASSERT_TRUE(received) << received.error();
    ExpectColumns(*received, columns, 11);
    EXPECT_EQ(received->columns[19].datatype_extension().decimal_type().precision(), 38);
    EXPECT_EQ(received->columns[19].datatype_extension().decimal_type().scale(), 7);
    auto message = ipc::DecodeMessage(frame->data_header);
    ASSERT_TRUE(message);
    EXPECT_EQ(message->body_length, static_cast<int64_t>(frame->data_body.size()));
    for (auto buffer : message->batch.buffers)
      EXPECT_EQ(buffer.offset % 8, 0);
  }
}

TEST(FlightIpcUnitScenario, EmptyNullBatchesRoundTrip) {
  for (auto compression : {FlightCompression::kNone, FlightCompression::kLz4Frame, FlightCompression::kZstd}) {
    for (std::size_t rows : {std::size_t{0}, std::size_t{9}}) {
      std::vector<Column> columns;
      for (auto type : kTypes) {
        auto column = MakeColumn(type, 0);
        if (rows) column.set_null_mask(std::string{"\xff\x01", 2});
        columns.push_back(std::move(column));
      }
      auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, rows);
      ASSERT_TRUE(batch) << batch.error();
      auto frame = FlightIpcEncoder(compression).EncodeBatch(*batch);
      ASSERT_TRUE(frame) << frame.error();
      auto decoded = flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, frame->data_body);
      ASSERT_TRUE(decoded) << decoded.error();
      ExpectColumns(*decoded, columns, rows);
    }
  }
  flight::RecordBatch empty;
  auto frame = FlightIpcEncoder().EncodeBatch(empty);
  ASSERT_TRUE(frame) << frame.error();
  auto decoded = flight::DecodeFlightRecordBatch(empty.schema, frame->data_header, frame->data_body);
  ASSERT_TRUE(decoded) << decoded.error();
  EXPECT_EQ(decoded->row_count, 0U);
}

TEST(FlightIpcUnitScenario, ArrowValidityLittleEndianLayout) {
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
  EXPECT_EQ(static_cast<int32_t>(ReadLittle(frame->data_body, values.offset, 4)), -1234567);
  EXPECT_EQ(static_cast<int32_t>(ReadLittle(frame->data_body, values.offset + 8, 4)), -1234565);
  auto decoded = flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, frame->data_body);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->columns[0].values().i32_values_size(), 8);
}

TEST(FlightIpcUnitScenario, BareAndFramedHeadersMatch) {
  std::vector<Column> columns{MakeColumn(greptime::v1::STRING, 11)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
  ASSERT_TRUE(batch);
  auto schema_frame = FlightIpcEncoder().EncodeSchema(batch->schema);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(schema_frame);
  ASSERT_TRUE(frame);
  for (bool marker : {false, true}) {
    auto schema = flight::DecodeFlightSchema(Encapsulate(schema_frame->data_header, marker));
    ASSERT_TRUE(schema) << schema.error();
    auto decoded = flight::DecodeFlightRecordBatch(*schema, Encapsulate(frame->data_header, marker), frame->data_body);
    ASSERT_TRUE(decoded) << decoded.error();
    ExpectColumns(*decoded, columns, 11);
    auto bad = Encapsulate(frame->data_header, marker);
    WriteLittle(bad, marker ? 4 : 0, frame->data_header.size() + 100, 4);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(*schema, bad, frame->data_body));
  }
}

TEST(FlightIpcUnitScenario, TimestampUnitsNegativeHourFloor) {
  constexpr std::array kTicks{int64_t{3600}, int64_t{3600000}, int64_t{3600000000}, int64_t{3600000000000}};
  for (std::size_t unit = 0; unit < kTicks.size(); ++unit) {
    const auto hour = kTicks[unit];
    auto ts = MakeColumn(kTypes[15 + unit], 0, false);
    ts.set_semantic_type(greptime::v1::TIMESTAMP);
    auto label = MakeColumn(greptime::v1::STRING, 0);
    const std::array times{hour, int64_t{-1}, int64_t{0}, -hour, hour - 1};
    for (std::size_t i = 0; i < times.size(); ++i) {
      if (unit == 0) ts.mutable_values()->add_timestamp_second_values(times[i]);
      if (unit == 1) ts.mutable_values()->add_timestamp_millisecond_values(times[i]);
      if (unit == 2) ts.mutable_values()->add_timestamp_microsecond_values(times[i]);
      if (unit == 3) ts.mutable_values()->add_timestamp_nanosecond_values(times[i]);
      if (i != 1) label.mutable_values()->add_string_values(std::to_string(i));
    }
    label.set_null_mask(std::string(1, '\x02'));
    std::vector<Column> columns{ts, label};
    auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, times.size());
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

TEST(FlightIpcUnitScenario, MinNanosecondAndMissingIndex) {
  auto ts = MakeColumn(greptime::v1::TIMESTAMP_NANOSECOND, 0, false);
  ts.set_semantic_type(greptime::v1::TIMESTAMP);
  ts.mutable_values()->add_timestamp_nanosecond_values(std::numeric_limits<int64_t>::min());
  ts.mutable_values()->add_timestamp_nanosecond_values(0);
  std::vector<Column> columns{ts};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 2);
  ASSERT_TRUE(batch);
  auto parts = FlightIpcEncoder::PartitionRecordBatchByHour(*batch);
  ASSERT_TRUE(parts) << parts.error();
  ASSERT_EQ(parts->size(), 2U);
  EXPECT_EQ((*parts)[0].columns[0].values().timestamp_nanosecond_values(0), std::numeric_limits<int64_t>::min());
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

TEST(FlightIpcUnitScenario, RejectsInvalidProtoColumnValues) {
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
  columns[0].mutable_datatype_extension()->mutable_decimal_type()->set_scale(-1);
  EXPECT_FALSE(FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1));
}

TEST(FlightIpcUnitScenario, RejectsCorruptFlatbufferTables) {
  std::vector<Column> columns{MakeColumn(greptime::v1::STRING, 3)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto schema = FlightIpcEncoder().EncodeSchema(batch->schema);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(schema);
  ASSERT_TRUE(frame);
  for (std::size_t size = 0; size < 20; ++size) {
    EXPECT_FALSE(flight::DecodeFlightSchema(std::span(schema->data_header).first(size)));
    EXPECT_FALSE(
        flight::DecodeFlightRecordBatch(batch->schema, std::span(frame->data_header).first(size), frame->data_body));
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
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, bad, frame->data_body));
  bad = schema->data_header;
  root = ReadLittle(bad, 0, 4);
  bad[FieldAt(bad, root, 1)] = 2; // DictionaryBatch must be explicitly rejected by codec.
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, bad, frame->data_body));
  EXPECT_FALSE(flight::DecodeFlightSchema(frame->data_header));
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, schema->data_header, frame->data_body));
  bad = schema->data_header;
  const auto schema_table = Indirect(bad, FieldAt(bad, ReadLittle(bad, 0, 4), 2));
  const auto fields = Indirect(bad, FieldAt(bad, schema_table, 1));
  WriteLittle(bad, fields, 0xffffffffU, 4);
  EXPECT_FALSE(flight::DecodeFlightSchema(bad));
  bad = schema->data_header;
  const auto schema_object = Indirect(bad, FieldAt(bad, ReadLittle(bad, 0, 4), 2));
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

TEST(FlightIpcUnitScenario, RejectsMismatchedBodyAndNodes) {
  std::vector<Column> columns{MakeColumn(greptime::v1::STRING, 3)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto decoded = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(decoded);
  auto bad_body = frame->data_body;
  bad_body.pop_back();
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, bad_body));
  bad_body = frame->data_body;
  bad_body.push_back(0);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, bad_body));
  auto meta = decoded->batch;
  meta.nodes[0].length = 2;
  auto header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, frame->data_body));
  meta = decoded->batch;
  meta.nodes[0].null_count = 0;
  header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, frame->data_body));
  meta = decoded->batch;
  meta.buffers[0].length = 0;
  header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, frame->data_body));
  bad_body = frame->data_body;
  const auto offsets = decoded->batch.buffers[1];
  WriteLittle(bad_body, offsets.offset + 4, 0x7fffffffU, 4);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, bad_body));
  auto bad_header = frame->data_header;
  const auto table = Indirect(bad_header, FieldAt(bad_header, ReadLittle(bad_header, 0, 4), 2));
  const auto buffers = Indirect(bad_header, FieldAt(bad_header, table, 2));
  WriteLittle(bad_header, buffers + 4, std::numeric_limits<uint64_t>::max(), 8);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, bad_header, frame->data_body));
  WriteLittle(bad_header, buffers + 4, frame->data_body.size() + 8, 8);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, bad_header, frame->data_body));
}
} // namespace
