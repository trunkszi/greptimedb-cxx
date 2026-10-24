/** Every supported logical type through dynamic buffers, Protobuf and Arrow
 * IPC. */
#include "example_support.h"

#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/flight/ipc_codec.h>

#include <array>
#include <span>
#include <vector>

namespace db = greptimedb;
namespace flight = db::flight;
struct Definition {
  const char *name;
  db::SemanticType role;
  db::ColumnDataType type;
};
const std::array<Definition, 21> definitions{{
    {"symbol", db::SemanticType::kTag, db::ColumnDataType::kString},
    {"ts", db::SemanticType::kTimestamp,
     db::ColumnDataType::kTimestampMillisecond},
    {"flag", db::SemanticType::kField, db::ColumnDataType::kBoolean},
    {"i8", db::SemanticType::kField, db::ColumnDataType::kInt8},
    {"i16", db::SemanticType::kField, db::ColumnDataType::kInt16},
    {"i32", db::SemanticType::kField, db::ColumnDataType::kInt32},
    {"i64", db::SemanticType::kField, db::ColumnDataType::kInt64},
    {"u8", db::SemanticType::kField, db::ColumnDataType::kUint8},
    {"u16", db::SemanticType::kField, db::ColumnDataType::kUint16},
    {"u32", db::SemanticType::kField, db::ColumnDataType::kUint32},
    {"u64", db::SemanticType::kField, db::ColumnDataType::kUint64},
    {"f32", db::SemanticType::kField, db::ColumnDataType::kFloat32},
    {"f64", db::SemanticType::kField, db::ColumnDataType::kFloat64},
    {"blob", db::SemanticType::kField, db::ColumnDataType::kBinary},
    {"json", db::SemanticType::kField, db::ColumnDataType::kJson},
    {"date", db::SemanticType::kField, db::ColumnDataType::kDate},
    {"datetime", db::SemanticType::kField, db::ColumnDataType::kDateTime},
    {"seconds", db::SemanticType::kField, db::ColumnDataType::kTimestampSecond},
    {"micros", db::SemanticType::kField,
     db::ColumnDataType::kTimestampMicrosecond},
    {"nanos", db::SemanticType::kField,
     db::ColumnDataType::kTimestampNanosecond},
    {"amount", db::SemanticType::kField, db::ColumnDataType::Decimal(38, 9)},
}};

// The two-row fixture has one non-null value per nullable column; only the
// non-null time index has two packed values. Returned payloads borrow c.
db::ValueView ReadValue(const greptime::v1::Column &c, db::ColumnDataType type,
                        std::size_t row) {
  if (!c.null_mask().empty() &&
      (static_cast<unsigned char>(c.null_mask()[0]) & (1U << row)))
    return std::monostate{};
  const int i = c.null_mask().empty() ? static_cast<int>(row) : 0;
  const auto &v = c.values();
  using enum db::ColumnDataType::Kind;
  switch (type.kind) {
  case kBoolean:
    return v.bool_values(i);
  case kInt8:
    return static_cast<std::int8_t>(v.i8_values(i));
  case kInt16:
    return static_cast<std::int16_t>(v.i16_values(i));
  case kInt32:
    return v.i32_values(i);
  case kInt64:
    return v.i64_values(i);
  case kUint8:
    return static_cast<std::uint8_t>(v.u8_values(i));
  case kUint16:
    return static_cast<std::uint16_t>(v.u16_values(i));
  case kUint32:
    return v.u32_values(i);
  case kUint64:
    return v.u64_values(i);
  case kFloat32:
    return v.f32_values(i);
  case kFloat64:
    return v.f64_values(i);
  case kBinary:
    return std::as_bytes(
        std::span{v.binary_values(i).data(), v.binary_values(i).size()});
  case kString:
    return std::string_view{v.string_values(i)};
  case kJson:
    return db::JsonView{v.binary_values(i)};
  case kDate:
    return db::Date{v.date_values(i)};
  case kDateTime:
    return db::DateTime{v.datetime_values(i)};
  case kTimestampSecond:
    return db::TimestampSecond{v.timestamp_second_values(i)};
  case kTimestampMillisecond:
    return db::TimestampMillisecond{v.timestamp_millisecond_values(i)};
  case kTimestampMicrosecond:
    return db::TimestampMicrosecond{v.timestamp_microsecond_values(i)};
  case kTimestampNanosecond:
    return db::TimestampNanosecond{v.timestamp_nanosecond_values(i)};
  case kDecimal128:
    return db::Decimal128::FromProto(v.decimal128_values(i), type.precision,
                                     type.scale);
  }
  return std::monostate{};
}

int main() {
  constexpr std::array blob{std::byte{0xde}, std::byte{0xad}, std::byte{0},
                            std::byte{0xff}};
  const __int128 coefficient =
      static_cast<__int128>(1234567890123456789LL) * 100000000000LL +
      1234567890;
  const std::array<db::ValueView, 21> values{
      std::string_view{"BTCUSDT"},
      db::TimestampMillisecond{1700000000123},
      true,
      std::int8_t{-8},
      std::int16_t{-1600},
      std::int32_t{-320000},
      std::int64_t{-6400000000},
      std::uint8_t{8},
      std::uint16_t{1600},
      std::uint32_t{3200000},
      std::uint64_t{6400000000},
      1.5F,
      -2.25,
      db::BinaryView{blob},
      db::JsonView{"{\"venue\":\"example\"}"},
      db::Date{19700},
      db::DateTime{1700000000123},
      db::TimestampSecond{1700000000},
      db::TimestampMicrosecond{1700000000123456},
      db::TimestampNanosecond{1700000000123456789},
      db::Decimal128{coefficient, 38, 9}};
  db::ColumnarBatch batch("type_matrix", 2);
  for (std::size_t i = 0; i < definitions.size(); ++i) {
    const auto &d = definitions[i];
    EXAMPLE_REQUIRE(batch.AddColumn(d.name, d.role, d.type));
    for (std::size_t j = 0; j < i; ++j)
      EXAMPLE_REQUIRE(d.type != definitions[j].type);
  }
  EXAMPLE_REQUIRE(batch.PushRow(values));
  std::array<db::ValueView, 21> nullable{};
  nullable[1] = db::TimestampMillisecond{1700000000124};
  EXAMPLE_REQUIRE(batch.PushRow(nullable));
  auto request = batch.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request && request->row_count() == 2 &&
                  request->columns_size() == 21);
  EXAMPLE_REQUIRE(request->columns(14).datatype_extension().has_json_type());
  EXAMPLE_REQUIRE(
      request->columns(20).datatype_extension().decimal_type().precision() ==
      38);
  EXAMPLE_REQUIRE(
      request->columns(20).datatype_extension().decimal_type().scale() == 9);
  for (int i = 0; i < 21; ++i)
    EXAMPLE_REQUIRE(request->columns(i).null_mask() ==
                    (i == 1 ? std::string{} : std::string(1, '\x02')));
  std::vector<greptime::v1::Column> columns(request->columns().begin(),
                                            request->columns().end());
  auto record = flight::RecordBatchFromProtoColumns(columns, 2);
  EXAMPLE_REQUIRE(record);
  EXAMPLE_REQUIRE(record->schema.fields.size() == definitions.size());
  for (std::size_t i = 0; i < definitions.size(); ++i) {
    EXAMPLE_REQUIRE(record->schema.fields[i].nullable == (i != 1));
    const auto &metadata = record->schema.fields[i].metadata;
    EXAMPLE_REQUIRE(std::ranges::any_of(metadata, [i](const auto &item) {
      return item.first == flight::kSemanticTypeKey &&
             item.second == db::AsFlightStr(definitions[i].role);
    }));
  }
  for (auto compression :
       {flight::FlightCompression::kNone, flight::FlightCompression::kLz4Frame,
        flight::FlightCompression::kZstd}) {
    flight::FlightIpcEncoder encoder(compression);
    auto schema = encoder.EncodeSchema(record->schema);
    auto encoded = encoder.EncodeBatch(*record);
    EXAMPLE_REQUIRE(schema && encoded);
    auto decoded_schema = flight::DecodeFlightSchema(schema->data_header);
    EXAMPLE_REQUIRE(decoded_schema);
    auto decoded = flight::DecodeFlightRecordBatch(
        *decoded_schema, encoded->data_header, encoded->data_body);
    EXAMPLE_REQUIRE(decoded && decoded->columns.size() == 21 &&
                    decoded->row_count == 2);
    db::ColumnarBatch restored("type_matrix", 2);
    for (const auto &d : definitions)
      EXAMPLE_REQUIRE(restored.AddColumn(d.name, d.role, d.type));
    for (std::size_t row = 0; row < 2; ++row) {
      std::array<db::ValueView, 21> borrowed{};
      for (std::size_t i = 0; i < 21; ++i) {
        EXAMPLE_REQUIRE(decoded->columns[i].column_name() ==
                        definitions[i].name);
        EXAMPLE_REQUIRE(decoded->columns[i].datatype() ==
                        (definitions[i].type == db::ColumnDataType::kJson
                             ? greptime::v1::JSON
                             : definitions[i].type.ToProtoDatatype()));
        EXAMPLE_REQUIRE(decoded->columns[i].semantic_type() ==
                        db::ToProto(definitions[i].role));
        borrowed[i] = ReadValue(decoded->columns[i], definitions[i].type, row);
      }
      EXAMPLE_REQUIRE(restored.PushRow(borrowed));
    }
    // Rebuilding logical buffers normalizes the wire decoder's native-JSON
    // extension back to the same JSON_BINARY form as the original insert.
    auto reencoded = restored.EncodeInsertRequest();
    EXAMPLE_REQUIRE(reencoded && reencoded->SerializeAsString() ==
                                     request->SerializeAsString());
  }
  std::cout
      << "21/21 logical column types: values, null masks, roles, decimal "
         "precision/scale "
         "and JSON extension round-trip through all IPC compression modes\n";
  return 0;
}
