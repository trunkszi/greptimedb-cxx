#include <greptimedb/core/types.h>

#include <algorithm>
#include <limits>
#include <type_traits>

namespace greptimedb {

greptime::v1::Decimal128 Decimal128::ToProto() const {
  greptime::v1::Decimal128 result;
  result.set_hi(Hi());
  result.set_lo(std::bit_cast<std::int64_t>(Lo()));
  return result;
}

Decimal128 Decimal128::FromProto(const greptime::v1::Decimal128 &source,
                                 std::uint32_t precision,
                                 std::uint32_t scale) noexcept {
  const auto bits =
      (static_cast<unsigned __int128>(static_cast<std::uint64_t>(source.hi()))
       << 64) |
      static_cast<std::uint64_t>(source.lo());
  return {std::bit_cast<__int128>(bits), precision, scale};
}

greptime::v1::ColumnDataType ColumnDataType::ToProtoDatatype() const noexcept {
  switch (kind) {
  case Kind::kBoolean:
    return greptime::v1::BOOLEAN;
  case Kind::kInt8:
    return greptime::v1::INT8;
  case Kind::kInt16:
    return greptime::v1::INT16;
  case Kind::kInt32:
    return greptime::v1::INT32;
  case Kind::kInt64:
    return greptime::v1::INT64;
  case Kind::kUint8:
    return greptime::v1::UINT8;
  case Kind::kUint16:
    return greptime::v1::UINT16;
  case Kind::kUint32:
    return greptime::v1::UINT32;
  case Kind::kUint64:
    return greptime::v1::UINT64;
  case Kind::kFloat32:
    return greptime::v1::FLOAT32;
  case Kind::kFloat64:
    return greptime::v1::FLOAT64;
  case Kind::kBinary:
    return greptime::v1::BINARY;
  case Kind::kString:
    return greptime::v1::STRING;
  case Kind::kDate:
    return greptime::v1::DATE;
  case Kind::kDateTime:
    return greptime::v1::DATETIME;
  case Kind::kTimestampSecond:
    return greptime::v1::TIMESTAMP_SECOND;
  case Kind::kTimestampMillisecond:
    return greptime::v1::TIMESTAMP_MILLISECOND;
  case Kind::kTimestampMicrosecond:
    return greptime::v1::TIMESTAMP_MICROSECOND;
  case Kind::kTimestampNanosecond:
    return greptime::v1::TIMESTAMP_NANOSECOND;
  case Kind::kDecimal128:
    return greptime::v1::DECIMAL128;
  case Kind::kJson:
    return greptime::v1::BINARY;
  }
  return greptime::v1::BOOLEAN;
}

std::optional<greptime::v1::ColumnDataTypeExtension>
ColumnDataType::ToProtoExtension() const {
  if (kind != Kind::kDecimal128 && kind != Kind::kJson)
    return std::nullopt;
  greptime::v1::ColumnDataTypeExtension result;
  if (kind == Kind::kJson) {
    result.set_json_type(greptime::v1::JSON_BINARY);
  } else {
    constexpr auto limit =
        static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max());
    auto *decimal = result.mutable_decimal_type();
    decimal->set_precision(
        static_cast<std::int32_t>(std::min(precision, limit)));
    decimal->set_scale(static_cast<std::int32_t>(std::min(scale, limit)));
  }
  return result;
}

greptime::v1::Value ToProtoValue(const ValueView &value) {
  greptime::v1::Value result;
  std::visit(
      [&result](const auto &scalar) {
        using T = std::remove_cvref_t<decltype(scalar)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // An unset oneof is SQL NULL.
        } else if constexpr (std::is_same_v<T, bool>) {
          result.set_bool_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int8_t>) {
          result.set_i8_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int16_t>) {
          result.set_i16_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int32_t>) {
          result.set_i32_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int64_t>) {
          result.set_i64_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint8_t>) {
          result.set_u8_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint16_t>) {
          result.set_u16_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint32_t>) {
          result.set_u32_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint64_t>) {
          result.set_u64_value(scalar);
        } else if constexpr (std::is_same_v<T, float>) {
          result.set_f32_value(scalar);
        } else if constexpr (std::is_same_v<T, double>) {
          result.set_f64_value(scalar);
        } else if constexpr (std::is_same_v<T, BinaryView>) {
          if (scalar.empty())
            result.set_binary_value("");
          else
            result.set_binary_value(scalar.data(), scalar.size());
        } else if constexpr (std::is_same_v<T, std::string_view>) {
          if (scalar.empty())
            result.set_string_value("");
          else
            result.set_string_value(scalar.data(), scalar.size());
        } else if constexpr (std::is_same_v<T, Date>) {
          result.set_date_value(scalar.value);
        } else if constexpr (std::is_same_v<T, DateTime>) {
          result.set_datetime_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampSecond>) {
          result.set_timestamp_second_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampMillisecond>) {
          result.set_timestamp_millisecond_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampMicrosecond>) {
          result.set_timestamp_microsecond_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampNanosecond>) {
          result.set_timestamp_nanosecond_value(scalar.value);
        } else if constexpr (std::is_same_v<T, Decimal128>) {
          *result.mutable_decimal128_value() = scalar.ToProto();
        } else if constexpr (std::is_same_v<T, JsonView>) {
          if (scalar.value.empty())
            result.set_binary_value("");
          else
            result.set_binary_value(scalar.value.data(), scalar.value.size());
        }
      },
      value);
  return result;
}

} // namespace greptimedb
