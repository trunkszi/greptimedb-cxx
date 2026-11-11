#include "test_ipc_codec_support.hh"

namespace {
TEST(FlightIpcUnitScenario, CompressionRejectsCorruptFrames) {
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
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, body));
    body = frame->data_body;
    WriteLittle(body, buffer.offset, static_cast<uint64_t>(-2), 8);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, body));
    body = frame->data_body;
    body[buffer.offset + 8] ^= 0xff;
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, body));
    auto meta = decoded->batch;
    meta.buffers[1].length = 7;
    auto header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
    ASSERT_TRUE(header);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, frame->data_body));
    body = frame->data_body;
    WriteLittle(body, buffer.offset, 1023, 8);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, body));
    body = frame->data_body;
    body.insert(body.begin() + buffer.offset + buffer.length, 0);
    meta = decoded->batch;
    ++meta.buffers[1].length;
    header = ipc::EncodeRecordBatch(meta, body.size());
    ASSERT_TRUE(header);
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, body));
  }
}

TEST(FlightIpcUnitScenario, RawPrefixBypassesDecompression) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 3)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto decoded = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(decoded);
  for (auto compression : {ipc::Compression::kLz4Frame, ipc::Compression::kZstd}) {
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

TEST(FlightIpcUnitScenario, LargeOffsetsAndNullType) {
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
    body[42] = 's';
    body[43] = 'r';
    body[44] = 'v';
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
      EXPECT_EQ(c.values().string_values(1), "srv");
    } else {
      EXPECT_EQ(c.datatype(), greptime::v1::BINARY);
      ASSERT_EQ(c.values().binary_values_size(), 2);
      EXPECT_EQ(c.values().binary_values(0), std::string("A\0", 2));
      EXPECT_EQ(c.values().binary_values(1), "srv");
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

TEST(FlightIpcUnitScenario, EmptyValidityAndBufferCount) {
  std::vector<Column> columns{MakeColumn(greptime::v1::INT32, 3, false)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 3);
  ASSERT_TRUE(batch);
  auto frame = FlightIpcEncoder().EncodeBatch(*batch);
  ASSERT_TRUE(frame);
  auto message = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(message);
  ASSERT_EQ(message->batch.buffers.size(), 2U);
  EXPECT_EQ(message->batch.buffers[0].length, 0);
  auto result = flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, frame->data_body);
  ASSERT_TRUE(result);
  EXPECT_TRUE(result->columns[0].null_mask().empty());
  auto meta = message->batch;
  meta.buffers.push_back({static_cast<int64_t>(frame->data_body.size()), 0});
  auto header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, frame->data_body));
  meta = message->batch;
  meta.buffers.pop_back();
  header = ipc::EncodeRecordBatch(meta, frame->data_body.size());
  ASSERT_TRUE(header);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch->schema, *header, frame->data_body));
}

TEST(FlightIpcUnitScenario, TimeIndexRequiresTimestamp) {
  std::vector<Column> columns{MakeColumn(greptime::v1::TIMESTAMP_MILLISECOND, 2, false)};
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

TEST(FlightIpcUnitScenario, SchemaMismatchFailsTimeBounds) {
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

TEST(FlightIpcUnitScenario, RejectsBadUtf8PreservesJson) {
  for (auto type : {greptime::v1::STRING, greptime::v1::JSON}) {
    std::vector<Column> columns{MakeColumn(type, 1, false)};
    if (type == greptime::v1::STRING)
      columns[0].mutable_values()->set_string_values(0, std::string("\xc0\x80", 2));
    else
      columns[0].mutable_values()->set_binary_values(0, std::string("\xed\xa0\x80", 3));
    auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 1);
    if (type == greptime::v1::STRING) {
      EXPECT_FALSE(batch);
      continue;
    }
    ASSERT_TRUE(batch);
    auto frame = FlightIpcEncoder().EncodeBatch(*batch);
    ASSERT_TRUE(frame);
    auto decoded = flight::DecodeFlightRecordBatch(batch->schema, frame->data_header, frame->data_body);
    ASSERT_TRUE(decoded);
    ExpectColumns(*decoded, columns, 1);
  }
}

TEST(FlightIpcUnitScenario, BooleanValuesUsePackedBits) {
  std::vector<Column> columns{MakeColumn(greptime::v1::BOOLEAN, 11)};
  auto batch = FlightIpcEncoder::RecordBatchFromProtoColumns(columns, 11);
  ASSERT_TRUE(batch);
  EXPECT_FALSE(FlightIpcEncoder(static_cast<FlightCompression>(255)).EncodeBatch(*batch));
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
TEST(FlightIpcUnitScenario, MetadataFramesStayUntyped) {
  auto frame = flight::EncodeMetadataMessage();
  ASSERT_TRUE(frame) << frame.error();
  EXPECT_TRUE(frame->data_body.empty());
  for (auto header : {frame->data_header, Encapsulate(frame->data_header, true)}) {
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
    EXPECT_FALSE(flight::IsMetadataOnlyMessage(std::span(frame->data_header).first(size)));
  auto malformed = frame->data_header;
  WriteLittle(malformed, 0, 0xfffffff0U, 4);
  EXPECT_FALSE(flight::IsMetadataOnlyMessage(malformed));
  malformed = frame->data_header;
  WriteLittle(malformed, FieldAt(malformed, ReadLittle(malformed, 0, 4), 3), 1, 8);
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
