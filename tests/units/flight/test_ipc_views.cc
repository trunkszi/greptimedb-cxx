#include "test_ipc_codec_support.hh"

#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/ipc_codec.h>
#include <greptimedb/row/wire_row_view.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <uarch/uarch.hpp>
#include <vector>

namespace {
namespace flight = greptimedb::flight;
namespace ipc = flight::ipc;
namespace v1 = greptime::v1;
using Bytes = std::vector<uint8_t>;
static_assert(uarch::NoPadding<ipc::Node> && uarch::RegisterPassable<ipc::Node> && uarch::GprPair<ipc::Node>);
static_assert(uarch::NoPadding<ipc::Buffer> && uarch::RegisterPassable<ipc::Buffer> && uarch::GprPair<ipc::Buffer>);
static_assert(uarch::NoPadding<flight::BulkWriteStats>);
void Put(Bytes &bytes, size_t at, uint32_t value) {
  for (size_t i = 0; i < 4; ++i)
    bytes[at + i] = static_cast<uint8_t>(value >> (8 * i));
}

struct Fixture {
  flight::FlightSchema schema;
  ipc::Batch batch;
  Bytes body;
  std::array<std::string, 4> values{"", "hello", "123456789012", "a long external payload"};
  Fixture(ipc::TypeId type, bool json = false) {
    flight::FlightField field;
    field.name = "value";
    field.type.id = type;
    if (json) field.metadata.emplace_back(flight::kTypeExtensionKey, flight::kJsonTypeValue);
    schema.fields.push_back(field);
    batch.length = 5;
    batch.nodes.push_back({5, 1});
    batch.variadic_buffer_counts.push_back(2);
    if (type == ipc::TypeId::kBinaryView) values[3].append("\0\xff", 2);
    Add(Bytes{0x0f});
    Bytes views(5 * 16);
    for (size_t row = 0; row < values.size(); ++row) {
      const auto &value = values[row];
      Put(views, row * 16, static_cast<uint32_t>(value.size()));
      std::copy_n(value.begin(), value.size() <= 12 ? value.size() : 4, views.begin() + row * 16 + 4);
      if (value.size() > 12) {
        Put(views, row * 16 + 8, 1);
        Put(views, row * 16 + 12, 3);
      }
    }
    std::fill(views.begin() + 64, views.end(),
              0xff); // Null views are unspecified.
    Add(views);
    Add(Bytes{'u', 'n', 'u', 's', 'e', 'd'});
    Bytes payload{'p', 'a', 'd'};
    payload.insert(payload.end(), values[3].begin(), values[3].end());
    Add(payload);
  }
  void Add(const Bytes &bytes) {
    batch.buffers.push_back({static_cast<int64_t>(body.size()), static_cast<int64_t>(bytes.size())});
    body.insert(body.end(), bytes.begin(), bytes.end());
    body.resize((body.size() + 7) & ~size_t{7});
  }
  [[nodiscard]] flight::FlightResult<flight::RecordBatch> Decode() const {
    auto header = ipc::EncodeRecordBatch(batch, body.size());
    if (!header) return std::unexpected(header.error());
    return flight::DecodeFlightRecordBatch(schema, *header, body);
  }
};

TEST(FlightViewUnitScenario, ViewColumnsRoundTrip) {
  for (auto type : {ipc::TypeId::kUtf8View, ipc::TypeId::kBinaryView}) {
    for (bool json : {false, true}) {
      Fixture fixture(type, json);
      auto decoded = fixture.Decode();
      ASSERT_TRUE(decoded) << decoded.error();
      ASSERT_EQ(decoded->columns.size(), 1U);
      const auto &column = decoded->columns[0];
      const bool text = type == ipc::TypeId::kUtf8View && !json;
      EXPECT_EQ(column.datatype(), json ? v1::JSON : text ? v1::STRING : v1::BINARY);
      EXPECT_EQ(column.null_mask(), std::string(1, '\x10'));
      for (int i = 0; i < 4; ++i)
        EXPECT_EQ(text ? column.values().string_values(i) : column.values().binary_values(i), fixture.values[i]);
      for (auto compression :
           {flight::FlightCompression::kNone, flight::FlightCompression::kLz4Frame, flight::FlightCompression::kZstd}) {
        flight::FlightIpcEncoder encoder(compression);
        auto schema = encoder.EncodeSchema(decoded->schema);
        ASSERT_TRUE(schema) << schema.error();
        auto parsed = flight::DecodeFlightSchema(schema->data_header);
        ASSERT_TRUE(parsed) << parsed.error();
        EXPECT_EQ(parsed->fields[0].type.id, type);
        auto encoded = encoder.EncodeBatch(*decoded);
        ASSERT_TRUE(encoded) << encoded.error();
        auto again = flight::DecodeFlightRecordBatch(*parsed, encoded->data_header, encoded->data_body);
        ASSERT_TRUE(again) << again.error();
        EXPECT_EQ(again->columns[0].SerializeAsString(), column.SerializeAsString());
      }
    }
  }
}

TEST(FlightViewUnitScenario, CorruptViewDescriptorsFailClosed) {
  for (auto type : {ipc::TypeId::kUtf8View, ipc::TypeId::kBinaryView}) {
    for (auto [slot, value] : std::array<std::pair<size_t, uint32_t>, 6>{
             {{0, 0xffffffffU}, {8, 0xffffffffU}, {8, 2}, {12, 0xffffffffU}, {12, 0x7fffffffU}, {0, 0x7fffffffU}}}) {
      Fixture fixture(type);
      Put(fixture.body, static_cast<size_t>(fixture.batch.buffers[1].offset) + 3 * 16 + slot, value);
      EXPECT_FALSE(fixture.Decode());
    }
    Fixture prefix(type);
    prefix.body[static_cast<size_t>(prefix.batch.buffers[1].offset) + 3 * 16 + 4] ^= 1;
    EXPECT_FALSE(prefix.Decode());
    Fixture truncated(type);
    --truncated.batch.buffers[1].length;
    EXPECT_FALSE(truncated.Decode());
  }
}

TEST(FlightViewUnitScenario, VariadicCountsMatchViewFields) {
  Fixture fixture(ipc::TypeId::kUtf8View);
  fixture.batch.variadic_buffer_counts.clear();
  EXPECT_FALSE(fixture.Decode());
  for (int64_t count : {-1, 0, 1, 3, 1000}) {
    fixture.batch.variadic_buffer_counts = {count};
    EXPECT_FALSE(fixture.Decode());
  }
  fixture.batch.variadic_buffer_counts = {2, 0};
  EXPECT_FALSE(fixture.Decode());
  Fixture plain(ipc::TypeId::kUtf8View);
  plain.schema.fields[0].type.id = ipc::TypeId::kInt;
  plain.schema.fields[0].type.bit_width = 64;
  EXPECT_FALSE(plain.Decode());
}

TEST(FlightViewUnitScenario, InlineViewsOmitVariadicBuffers) {
  Fixture fixture(ipc::TypeId::kUtf8View);
  fixture.batch.length = 3;
  fixture.batch.nodes[0] = {3, 0};
  fixture.batch.variadic_buffer_counts = {0};
  fixture.batch.buffers.resize(2);
  fixture.batch.buffers[1].length = 3 * 16;
  fixture.body.resize(static_cast<size_t>(fixture.batch.buffers[1].offset) + 3 * 16);
  auto decoded = fixture.Decode();
  ASSERT_TRUE(decoded) << decoded.error();
  EXPECT_EQ(decoded->columns[0].values().string_values(2), "123456789012");
}

TEST(FlightViewUnitScenario, ValidatesUtf8ViewPayloads) {
  for (bool json : {false, true}) {
    Fixture inline_bad(ipc::TypeId::kUtf8View, json);
    inline_bad.body[static_cast<size_t>(inline_bad.batch.buffers[1].offset) + 16 + 4] = 0xff;
    EXPECT_FALSE(inline_bad.Decode());
    Fixture external_bad(ipc::TypeId::kUtf8View, json);
    external_bad.body[static_cast<size_t>(external_bad.batch.buffers[3].offset) + 8] = 0xff;
    EXPECT_FALSE(external_bad.Decode());
  }
}

TEST(FlightViewInterop, LiveSchemaDecodesJsonViews) {
  std::ifstream input(std::string(IPC_VIEW_REFERENCE_DIR) + "/live-matrix-schema.bin", std::ios::binary);
  const Bytes bytes(std::istreambuf_iterator<char>(input), {});
  ASSERT_FALSE(bytes.empty());
  auto schema = flight::DecodeFlightSchema(bytes);
  ASSERT_TRUE(schema) << schema.error();
  ASSERT_EQ(schema->fields.size(), 42U);
  EXPECT_EQ(schema->fields[32].name, "required_document");
  EXPECT_EQ(schema->fields[33].name, "nullable_document");
  EXPECT_EQ(schema->fields[32].type.id, ipc::TypeId::kUtf8View);
  EXPECT_EQ(schema->fields[33].type.id, ipc::TypeId::kUtf8View);
}

Bytes ReadFixture(const std::string &name) {
  std::ifstream input(std::string(IPC_VIEW_REFERENCE_DIR) + "/" + name, std::ios::binary);
  return Bytes(std::istreambuf_iterator<char>(input), {});
}

TEST(FlightViewInterop, ArrowViewFixturesRoundTrip) {
  auto schema = flight::DecodeFlightSchema(ReadFixture("views.schema.bin"));
  ASSERT_TRUE(schema) << schema.error();
  ASSERT_EQ(schema->fields.size(), 3U);
  std::error_code error;
  std::filesystem::create_directories(IPC_VIEW_OUTPUT_DIR, error);
  ASSERT_FALSE(error);
  for (auto [name, codec] : std::array{std::pair{"none", flight::FlightCompression::kNone},
                                       std::pair{"lz4", flight::FlightCompression::kLz4Frame},
                                       std::pair{"zstd", flight::FlightCompression::kZstd}}) {
    const auto header = ReadFixture(std::string("views-") + name + ".header.bin");
    const auto body = ReadFixture(std::string("views-") + name + ".body.bin");
    auto batch = flight::DecodeFlightRecordBatch(*schema, header, body);
    ASSERT_TRUE(batch) << batch.error();
    ASSERT_EQ(batch->columns.size(), 3U);
    EXPECT_EQ(batch->row_count, 5U);
    EXPECT_EQ(batch->columns[0].null_mask(), std::string(1, '\x02'));
    EXPECT_EQ(batch->columns[0].values().string_values(2), "external view payload 0123456789");
    EXPECT_EQ(batch->columns[1].values().binary_values(3), std::string("\0\xff", 2));
    EXPECT_EQ(batch->columns[2].datatype(), v1::JSON);
    EXPECT_EQ(batch->columns[2].values().binary_values(2), "{\"long_document\":\"external payload\"}");
    auto encoded = flight::FlightIpcEncoder(codec).EncodeBatch(*batch);
    auto encoded_schema = flight::FlightIpcEncoder().EncodeSchema(*schema);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(encoded_schema);
    for (const auto &[file, bytes] :
         std::array{std::pair{std::string("views.schema.bin"), &encoded_schema->data_header},
                    std::pair{std::string("views-") + name + ".header.bin", &encoded->data_header},
                    std::pair{std::string("views-") + name + ".body.bin", &encoded->data_body}}) {
      ASSERT_TRUE(WriteBinaryFixture(IPC_VIEW_OUTPUT_DIR, file, *bytes));
    }
  }
}
std::int64_t FloorHour(std::int64_t value, std::uint64_t hour) {
  if (value >= 0) return static_cast<std::int64_t>(static_cast<std::uint64_t>(value) / hour);
  const auto magnitude = 0ULL - static_cast<std::uint64_t>(value);
  const auto quotient = magnitude / hour;
  const auto remainder = magnitude % hour;
  return -static_cast<std::int64_t>(quotient) - static_cast<std::int64_t>(remainder != 0U);
}

std::int64_t TimestampAt(const v1::Column &column, int unit, int row) {
  switch (unit) {
  case 0: return column.values().timestamp_second_values(row);
  case 1: return column.values().timestamp_millisecond_values(row);
  case 2: return column.values().timestamp_microsecond_values(row);
  default: return column.values().timestamp_nanosecond_values(row);
  }
}

TEST(FlightViewUnitScenario, HourFloorFourUnits) {
  constexpr std::array<std::int64_t, 10> kTimes{-7200, -3601, -3600, -3599, -1,
                                                0,     1,     3599,  3600,  std::numeric_limits<std::int64_t>::min()};
  constexpr std::array<std::uint64_t, 4> kHours{3600ULL, 3'600'000ULL, 3'600'000'000ULL, 3'600'000'000'000ULL};
  constexpr std::array<v1::ColumnDataType, 4> kTypes{v1::TIMESTAMP_SECOND, v1::TIMESTAMP_MILLISECOND,
                                                     v1::TIMESTAMP_MICROSECOND, v1::TIMESTAMP_NANOSECOND};
  for (int unit = 0; unit < 4; ++unit) {
    v1::Column column;
    column.set_column_name("ts");
    column.set_semantic_type(v1::TIMESTAMP);
    column.set_datatype(kTypes[static_cast<std::size_t>(unit)]);
    for (const auto time : kTimes) {
      if (unit == 0) column.mutable_values()->add_timestamp_second_values(time);
      if (unit == 1) column.mutable_values()->add_timestamp_millisecond_values(time);
      if (unit == 2) column.mutable_values()->add_timestamp_microsecond_values(time);
      if (unit == 3) column.mutable_values()->add_timestamp_nanosecond_values(time);
    }
    auto batch = flight::RecordBatchFromProtoColumns(std::span{&column, 1}, kTimes.size());
    ASSERT_TRUE(batch) << batch.error();
    auto parts = flight::PartitionRecordBatchByHour(*batch);
    ASSERT_TRUE(parts) << parts.error();
    struct HourValues {
      std::int64_t hour = 0;
      std::vector<std::int64_t> values;
    };
    std::vector<HourValues> expected;
    for (const auto time : kTimes) {
      const auto hour = FloorHour(time, kHours[static_cast<std::size_t>(unit)]);
      auto slot = std::ranges::lower_bound(expected, hour, {}, &HourValues::hour);
      if (slot == expected.end() || slot->hour != hour) slot = expected.insert(slot, HourValues{hour, {}});
      slot->values.push_back(time);
    }
    ASSERT_EQ(parts->size(), expected.size());
    std::size_t part = 0;
    for (const auto &[hour, values] : expected) {
      const auto &rows = (*parts)[part++];
      ASSERT_EQ(rows.row_count, values.size()) << hour;
      for (std::size_t row = 0; row < values.size(); ++row)
        EXPECT_EQ(TimestampAt(rows.columns[0], unit, static_cast<int>(row)), values[row]);
    }
  }
}
// clang-format off
struct JsonTelemetry {
  [[= greptimedb::timestamp{}]] greptimedb::TimestampMillisecond ts;
  greptimedb::JsonView document;
  [[= greptimedb::field{.json = true}]] std::optional<std::string> details;
};
// clang-format on

TEST(FlightViewUnitScenario, ReflectedJsonRoundTripBorrowsPayload) {
  using namespace greptimedb;
  TypedColumnarBatch<JsonTelemetry> batch;
  constexpr std::string_view kDocument = "{\"cpu\":0.5}";
  ASSERT_TRUE(batch.Push(JsonTelemetry{{1}, JsonView{kDocument}, std::string{"{\"host\":1}"}}));
  ASSERT_TRUE(batch.Push(JsonTelemetry{{2}, JsonView{"{}"}, std::nullopt}));
  auto request = batch.ToInsertRequest();
  ASSERT_TRUE(request);
  auto written = ViewDecodedRow<JsonTelemetry>(request->columns(), 0, 2);
  ASSERT_TRUE(written);
  EXPECT_EQ(written->Get<1>().value, kDocument);
  const std::vector<v1::Column> columns(request->columns().begin(), request->columns().end());
  auto record = flight::RecordBatchFromProtoColumns(columns, request->row_count());
  ASSERT_TRUE(record);
  flight::FlightIpcEncoder encoder;
  auto encoded = encoder.EncodeBatch(*record);
  ASSERT_TRUE(encoded);
  auto decoded = flight::DecodeFlightRecordBatch(record->schema, encoded->data_header, encoded->data_body);
  ASSERT_TRUE(decoded);
  ASSERT_EQ(decoded->columns[1].datatype(), v1::JSON);
  auto row = ViewDecodedRow<JsonTelemetry>(decoded->columns, 0, 2);
  ASSERT_TRUE(row);
  EXPECT_EQ(row->Get<1>().value, kDocument);
  EXPECT_EQ(row->Get<1>().value.data(), decoded->columns[1].values().binary_values(0).data());
  ASSERT_TRUE(row->Get<2>());
  EXPECT_EQ(*row->Get<2>(), "{\"host\":1}");
  EXPECT_EQ(row->Get<2>()->data(), decoded->columns[2].values().binary_values(0).data());
  auto nullable = ViewDecodedRow<JsonTelemetry>(decoded->columns, 1, 2);
  ASSERT_TRUE(nullable);
  EXPECT_FALSE(nullable->Get<2>());
  auto &column = decoded->columns[1];
  column.clear_datatype_extension();
  EXPECT_TRUE(ViewDecodedRow<JsonTelemetry>(decoded->columns, 0, 2));
  for (const auto datatype : {v1::STRING, v1::JSON, v1::INT64}) {
    column.mutable_datatype_extension()->mutable_json_native_type()->set_datatype(datatype);
    auto invalid = ViewDecodedRow<JsonTelemetry>(decoded->columns, 0, 2);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), ColumnBufError::kInvalidSchema);
  }
  column.mutable_datatype_extension()->set_json_type(v1::JSON_BINARY);
  EXPECT_FALSE(ViewDecodedRow<JsonTelemetry>(decoded->columns, 0, 2));
  auto incompatible = *request;
  incompatible.mutable_columns(1)->mutable_datatype_extension()->mutable_json_native_type()->set_datatype(v1::BINARY);
  EXPECT_FALSE(ViewDecodedRow<JsonTelemetry>(incompatible.columns(), 0, 2));
}

} // namespace
