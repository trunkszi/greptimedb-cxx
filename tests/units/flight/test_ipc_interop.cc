#include <greptimedb/flight/ipc_codec.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

namespace {
namespace flight = greptimedb::flight;
using Bytes = std::vector<uint8_t>;

Bytes ReadFixture(const std::string &file) {
  std::ifstream input(std::string(IPC_REFERENCE_DIR) + "/" + file,
                      std::ios::binary);
  return Bytes(std::istreambuf_iterator<char>(input),
               std::istreambuf_iterator<char>());
}

bool WriteOutput(const std::string &file, const Bytes &bytes) {
  std::ofstream output(std::string(IPC_REFERENCE_OUTPUT_DIR) + "/" + file,
                       std::ios::binary);
  output.write(reinterpret_cast<const char *>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  return output.good();
}

const greptime::v1::Column *Find(const flight::RecordBatch &batch,
                                 const std::string &name) {
  for (const auto &column : batch.columns)
    if (column.column_name() == name)
      return &column;
  return nullptr;
}

TEST(FlightIpcInterop, DecodesIndependentArrowLayoutsAndExportsEncoderFrames) {
  const auto schema_bytes = ReadFixture("schema.bin");
  ASSERT_FALSE(schema_bytes.empty());
  auto schema = flight::DecodeFlightSchema(schema_bytes);
  ASSERT_TRUE(schema) << schema.error();
  ASSERT_EQ(schema->fields.size(), 24U);
  EXPECT_EQ(schema->fields[14].type.unit, 0);
  EXPECT_EQ(schema->fields[15].type.unit, 1);
  EXPECT_EQ(schema->metadata.front().first, flight::kTableMetadataKey);
  EXPECT_EQ(schema->metadata.front().second, "ipc_reference");
  std::error_code error;
  std::filesystem::create_directories(IPC_REFERENCE_OUTPUT_DIR, error);
  ASSERT_FALSE(error) << error.message();
  const std::array<std::pair<const char *, flight::FlightCompression>, 3>
      codecs{{
          {"none", flight::FlightCompression::kNone},
          {"lz4", flight::FlightCompression::kLz4Frame},
          {"zstd", flight::FlightCompression::kZstd},
      }};
  for (const auto &[name, compression] : codecs) {
    SCOPED_TRACE(name);
    const auto header = ReadFixture(std::string(name) + ".header.bin");
    const auto body = ReadFixture(std::string(name) + ".body.bin");
    ASSERT_FALSE(header.empty());
    ASSERT_FALSE(body.empty());
    auto batch = flight::DecodeFlightRecordBatch(*schema, header, body);
    ASSERT_TRUE(batch) << batch.error();
    ASSERT_EQ(batch->row_count, 5U);
    ASSERT_EQ(batch->columns.size(), 24U);
    const auto *time = Find(*batch, "time");
    ASSERT_NE(time, nullptr);
    EXPECT_EQ(time->semantic_type(), greptime::v1::TIMESTAMP);
    ASSERT_EQ(time->values().timestamp_second_values_size(), 5);
    EXPECT_EQ(time->values().timestamp_second_values(0), -3601);
    EXPECT_EQ(time->values().timestamp_second_values(4), 3600);
    const auto *signed_byte = Find(*batch, "i8");
    ASSERT_NE(signed_byte, nullptr);
    EXPECT_EQ(signed_byte->null_mask(), std::string(1, '\x12'));
    ASSERT_EQ(signed_byte->values().i8_values_size(), 3);
    EXPECT_EQ(signed_byte->values().i8_values(0), -128);
    EXPECT_EQ(signed_byte->values().i8_values(2), 127);
    const auto *unsigned_long = Find(*batch, "u64");
    ASSERT_NE(unsigned_long, nullptr);
    EXPECT_EQ(unsigned_long->values().u64_values(1), UINT64_MAX);
    const auto *datetime = Find(*batch, "datetime");
    ASSERT_NE(datetime, nullptr);
    EXPECT_EQ(datetime->values().datetime_values(0), -86400000);
    EXPECT_EQ(datetime->values().datetime_values(2), 86400000);
    const auto *text = Find(*batch, "text");
    ASSERT_NE(text, nullptr);
    EXPECT_EQ(text->semantic_type(), greptime::v1::TAG);
    EXPECT_EQ(text->values().string_values(2), "\xce\xbb\xf0\x9f\x98\x80");
    const auto *json = Find(*batch, "json");
    ASSERT_NE(json, nullptr);
    EXPECT_EQ(json->datatype(), greptime::v1::JSON);
    EXPECT_EQ(json->values().binary_values(0), "{\"x\":1}");
    const auto *nulls = Find(*batch, "nulls");
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->null_mask(), std::string(1, '\x1f'));
    EXPECT_EQ(nulls->values().binary_values_size(), 0);
    auto parts = flight::PartitionRecordBatchByHour(*batch);
    ASSERT_TRUE(parts) << parts.error();
    ASSERT_EQ(parts->size(), 4U);
    EXPECT_EQ((*parts)[0].columns[0].values().timestamp_second_values(0),
              -3601);
    EXPECT_EQ((*parts)[2].row_count, 2U);
    flight::FlightIpcEncoder encoder(compression);
    auto encoded_schema = encoder.EncodeSchema(batch->schema);
    ASSERT_TRUE(encoded_schema) << encoded_schema.error();
    auto encoded = encoder.EncodeBatch(*batch);
    ASSERT_TRUE(encoded) << encoded.error();
    ASSERT_TRUE(WriteOutput("schema.bin", encoded_schema->data_header));
    ASSERT_TRUE(
        WriteOutput(std::string(name) + ".header.bin", encoded->data_header));
    ASSERT_TRUE(
        WriteOutput(std::string(name) + ".body.bin", encoded->data_body));
    auto roundtrip = flight::DecodeFlightRecordBatch(
        *schema, encoded->data_header, encoded->data_body);
    ASSERT_TRUE(roundtrip) << roundtrip.error();
    for (size_t i = 0; i < batch->columns.size(); ++i)
      EXPECT_EQ(roundtrip->columns[i].SerializeAsString(),
                batch->columns[i].SerializeAsString());
  }
}

TEST(FlightIpcInterop, VariableOffsetsMayStartInsideTheDataBuffer) {
  auto schema = flight::DecodeFlightSchema(ReadFixture("schema.bin"));
  ASSERT_TRUE(schema) << schema.error();
  const auto header = ReadFixture("none.header.bin");
  auto message = flight::ipc::DecodeMessage(header);
  ASSERT_TRUE(message) << message.error();
  size_t buffer_index = 0;
  for (const auto &field : schema->fields) {
    const auto type = field.type.id;
    const bool variable = type == flight::FlightTypeId::kUtf8 ||
                          type == flight::FlightTypeId::kBinary ||
                          type == flight::FlightTypeId::kLargeUtf8 ||
                          type == flight::FlightTypeId::kLargeBinary;
    if (field.name == "text" || field.name == "large_text") {
      auto body = ReadFixture("none.body.bin");
      const auto offset =
          static_cast<size_t>(message->batch.buffers[buffer_index + 1].offset);
      ASSERT_LT(offset, body.size());
      body[offset] = 1;
      auto decoded = flight::DecodeFlightRecordBatch(*schema, header, body);
      ASSERT_TRUE(decoded) << decoded.error();
      const auto *value = Find(*decoded, field.name);
      ASSERT_NE(value, nullptr);
      EXPECT_EQ(value->values().string_values(0),
                field.name == "text" ? "lpha" : "arge");
      const size_t width = type == flight::FlightTypeId::kUtf8 ? 4 : 8;
      for (size_t byte = 0; byte < width; ++byte)
        body[offset + byte] = 0xff;
      EXPECT_FALSE(flight::DecodeFlightRecordBatch(*schema, header, body));
    }
    buffer_index += type == flight::FlightTypeId::kNull ? 0 : variable ? 3 : 2;
  }
}

TEST(FlightIpcInterop, DetectsRealDictionaryMessagesAndRejectsDictionarySchemas) {
  const auto header = ReadFixture("dictionary.header.bin");
  ASSERT_FALSE(header.empty());
  auto message = flight::ipc::DecodeMessage(header);
  ASSERT_TRUE(message) << message.error();
  EXPECT_EQ(message->header_type, flight::ipc::HeaderType::kDictionaryBatch);
  EXPECT_GT(message->body_length, 0);
  auto metadata = flight::IsMetadataOnlyMessage(header);
  ASSERT_TRUE(metadata) << metadata.error();
  EXPECT_FALSE(*metadata);
  EXPECT_FALSE(flight::DecodeFlightSchema(ReadFixture("dictionary.schema.bin")));
  EXPECT_FALSE(flight::DecodeFlightRecordBatch({}, header, {}));
}
} // namespace
