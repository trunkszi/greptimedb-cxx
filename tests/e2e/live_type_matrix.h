#pragma once

#include "docker_e2e_support.h"

#include <limits>
#include <ranges>

namespace greptimedb::e2e {
struct MatrixType {
  const char *name;
  ColumnDataType type;
};
inline const std::array<MatrixType, 20> kMatrixTypes{
    {{"flag", ColumnDataType::kBoolean},
     {"i8", ColumnDataType::kInt8},
     {"i16", ColumnDataType::kInt16},
     {"i32", ColumnDataType::kInt32},
     {"i64", ColumnDataType::kInt64},
     {"u8", ColumnDataType::kUint8},
     {"u16", ColumnDataType::kUint16},
     {"u32", ColumnDataType::kUint32},
     {"u64", ColumnDataType::kUint64},
     {"f32", ColumnDataType::kFloat32},
     {"f64", ColumnDataType::kFloat64},
     {"text", ColumnDataType::kString},
     {"blob", ColumnDataType::kBinary},
     {"day", ColumnDataType::kDate},
     {"amount", ColumnDataType::Decimal(38, 9)},
     {"document", ColumnDataType::kJson},
     {"seconds", ColumnDataType::kTimestampSecond},
     {"millis", ColumnDataType::kTimestampMillisecond},
     {"micros", ColumnDataType::kTimestampMicrosecond},
     {"nanos", ColumnDataType::kTimestampNanosecond}}};
inline bool MatrixNull(std::size_t row, std::size_t column) {
  return (row + column) % 5 == 0 || row == 63 || row == 64 || row == 129;
}
inline auto MatrixValues(std::size_t row) -> std::array<ValueView, 20> {
  static constexpr std::array bytes{std::byte{0}, std::byte{0xff},
                                    std::byte{0x80}, std::byte{0}};
  const auto i = static_cast<std::int64_t>(row);
  const auto coefficient =
      static_cast<__int128>(1234567890123456789LL) * 100000000000LL + i;
  return {row % 2 == 0,
          static_cast<std::int8_t>(-128 + static_cast<int>(row % 128)),
          static_cast<std::int16_t>(-32768 + i),
          std::numeric_limits<std::int32_t>::min() +
              static_cast<std::int32_t>(row),
          std::numeric_limits<std::int64_t>::min() + i,
          static_cast<std::uint8_t>(255 - row),
          static_cast<std::uint16_t>(65535 - row),
          std::numeric_limits<std::uint32_t>::max() -
              static_cast<std::uint32_t>(row),
          std::numeric_limits<std::uint64_t>::max() - row,
          1.25F + static_cast<float>(row) * 0.5F,
          -2.25 - static_cast<double>(row) * 0.125,
          std::string_view{"quoted ' UTF-8 策略"},
          BinaryView{bytes},
          Date{19700 + static_cast<std::int32_t>(row)},
          Decimal128{coefficient, 38, 9},
          JsonView{R"({"n":7})"},
          TimestampSecond{1700000000 + i},
          TimestampMillisecond{kTs + i},
          TimestampMicrosecond{kTs * 1000 + i},
          TimestampNanosecond{kTs * 1000000 + i}};
}
inline auto AddMatrixColumns(ColumnarBatch &batch) -> Result<void> {
  if (auto r = batch.AddTagColumn("host", ColumnDataType::kString); !r)
    return std::unexpected(r.error());
  if (auto r = batch.AddTimestampColumn("ts", TimestampUnit::kMillisecond); !r)
    return std::unexpected(r.error());
  for (const auto &field : kMatrixTypes)
    for (const auto prefix : {"required_", "nullable_"})
      if (auto r = batch.AddFieldColumn(std::string(prefix) + field.name,
                                        field.type);
          !r)
        return std::unexpected(r.error());
  return {};
}
inline auto MatrixTable(std::string_view schema) -> proto::CreateTableExpr {
  proto::CreateTableExpr table;
  table.set_catalog_name("greptime");
  table.set_schema_name(std::string(schema));
  table.set_table_name("type_matrix");
  table.set_time_index("ts");
  table.add_primary_keys("host");
  table.set_engine("mito");
  auto add = [&](std::string name, ColumnDataType type, SemanticType role,
                 bool nullable) {
    auto &column = *table.add_column_defs();
    column.set_name(std::move(name));
    column.set_data_type(type.ToProtoDatatype());
    column.set_semantic_type(ToProto(role));
    column.set_is_nullable(nullable);
    if (auto extension = type.ToProtoExtension())
      *column.mutable_datatype_extension() = std::move(*extension);
  };
  add("host", ColumnDataType::kString, SemanticType::kTag, false);
  add("ts", ColumnDataType::kTimestampMillisecond, SemanticType::kTimestamp,
      false);
  for (const auto &field : kMatrixTypes) {
    add(std::string("required_") + field.name, field.type, SemanticType::kField,
        false);
    add(std::string("nullable_") + field.name, field.type, SemanticType::kField,
        true);
  }
  return table;
}
inline auto MatrixSql() -> std::string {
  std::string sql = "SELECT host, ts";
  for (const auto &field : kMatrixTypes)
    for (const auto prefix : {"required_", "nullable_"}) {
      const auto name = std::string(prefix) + field.name;
      sql += field.type == ColumnDataType::kJson
                 ? ", json_to_string(" + name + ") AS " + name
                 : ", " + name;
    }
  return sql + " FROM type_matrix ORDER BY ts";
}
inline auto MatrixValue(const proto::Column &column, ColumnDataType type,
                        std::size_t row, std::size_t rows)
    -> Result<ValueView> {
  if (type == ColumnDataType::kJson)
    type = ColumnDataType::kString;
  const auto name = std::string_view(column.column_name());
  auto index = row_detail::WireIndex(
      column, ColumnMeta{name, SemanticType::kField, type, true}, row, rows);
  if (!index)
    return std::unexpected(
        Error{ErrorKind::kDecode,
              "invalid live column metadata/mask: " + std::string(name)});
  if (!*index)
    return ValueView{std::monostate{}};
  const auto &v = column.values();
  const auto i = **index;
  using enum ColumnDataType::Kind;
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
  case kString:
    return std::string_view(v.string_values(i));
  case kBinary:
    return std::as_bytes(
        std::span(v.binary_values(i).data(), v.binary_values(i).size()));
  case kDate:
    return Date{v.date_values(i)};
  case kDateTime:
    return DateTime{v.datetime_values(i)};
  case kDecimal128:
    return Decimal128::FromProto(v.decimal128_values(i), type.precision,
                                 type.scale);
  case kTimestampSecond:
    return TimestampSecond{v.timestamp_second_values(i)};
  case kTimestampMillisecond:
    return TimestampMillisecond{v.timestamp_millisecond_values(i)};
  case kTimestampMicrosecond:
    return TimestampMicrosecond{v.timestamp_microsecond_values(i)};
  case kTimestampNanosecond:
    return TimestampNanosecond{v.timestamp_nanosecond_values(i)};
  case kJson:
    break;
  }
  return std::unexpected(Error{ErrorKind::kDecode, "unsupported matrix value"});
}
inline bool EqualMatrixValue(const ValueView &actual,
                             const ValueView &expected) {
  return std::visit(
      [&](const auto &value) {
        using V = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::same_as<V, JsonView>) {
          const auto *text = std::get_if<std::string_view>(&actual);
          return text && *text == value.value;
        } else {
          const auto *found = std::get_if<V>(&actual);
          if (!found)
            return false;
          if constexpr (std::same_as<V, BinaryView>)
            return std::ranges::equal(*found, value);
          else
            return *found == value;
        }
      },
      expected);
}
} // namespace greptimedb::e2e
