#pragma once

#include <greptime/v1/common.pb.h>
#include <greptime/v1/row.pb.h>

#include <bit>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <variant>

namespace greptimedb {

enum class SemanticType : std::int32_t { kTag = 0, kField = 1, kTimestamp = 2 };

constexpr greptime::v1::SemanticType ToProto(SemanticType type) noexcept {
  return static_cast<greptime::v1::SemanticType>(type);
}

constexpr std::string_view AsFlightStr(SemanticType type) noexcept {
  switch (type) {
  case SemanticType::kTag:
    return "tag";
  case SemanticType::kField:
    return "field";
  case SemanticType::kTimestamp:
    return "timestamp";
  }
  return {};
}

enum class TimestampUnit {
  kSecond = 1,
  kMillisecond = 2,
  kMicrosecond = 3,
  kNanosecond = 0
};

struct TimestampSecond {
  std::int64_t value{};
  constexpr auto operator<=>(const TimestampSecond &) const = default;
};
struct TimestampMillisecond {
  std::int64_t value{};
  constexpr auto operator<=>(const TimestampMillisecond &) const = default;
};
struct TimestampMicrosecond {
  std::int64_t value{};
  constexpr auto operator<=>(const TimestampMicrosecond &) const = default;
};
struct TimestampNanosecond {
  std::int64_t value{};
  constexpr auto operator<=>(const TimestampNanosecond &) const = default;
};
/** Days since the Unix epoch. */
struct Date {
  std::int32_t value{};
  constexpr auto operator<=>(const Date &) const = default;
};
/** GreptimeDB DATETIME wire value. */
struct DateTime {
  std::int64_t value{};
  constexpr auto operator<=>(const DateTime &) const = default;
};

/** Represents value * 10^(-scale); columns validate and enforce matching
 * metadata. */
struct Decimal128 {
  __int128 value{};
  std::uint32_t precision{};
  std::uint32_t scale{};

  constexpr std::int64_t Hi() const noexcept {
    return std::bit_cast<std::int64_t>(static_cast<std::uint64_t>(
        static_cast<unsigned __int128>(value) >> 64));
  }
  constexpr std::uint64_t Lo() const noexcept {
    return static_cast<std::uint64_t>(value);
  }
  greptime::v1::Decimal128 ToProto() const;
  static Decimal128 FromProto(const greptime::v1::Decimal128 &source,
                              std::uint32_t precision,
                              std::uint32_t scale) noexcept;
  constexpr bool operator==(const Decimal128 &) const = default;
};

/** Logical type and the column-level metadata needed by decimal values. */
struct ColumnDataType {
  enum class Kind {
    kBoolean,
    kInt8,
    kInt16,
    kInt32,
    kInt64,
    kUint8,
    kUint16,
    kUint32,
    kUint64,
    kFloat32,
    kFloat64,
    kBinary,
    kString,
    kDate,
    kDateTime,
    kTimestampSecond,
    kTimestampMillisecond,
    kTimestampMicrosecond,
    kTimestampNanosecond,
    kDecimal128,
    kJson
  };

  Kind kind = Kind::kBoolean;
  std::uint32_t precision{};
  std::uint32_t scale{};

  constexpr ColumnDataType() = default;
  constexpr ColumnDataType(Kind value) noexcept : kind(value) {}
  static constexpr ColumnDataType Decimal(std::uint32_t precision,
                                          std::uint32_t scale) noexcept {
    ColumnDataType result{Kind::kDecimal128};
    result.precision = precision;
    result.scale = scale;
    return result;
  }

  greptime::v1::ColumnDataType ToProtoDatatype() const noexcept;
  std::optional<greptime::v1::ColumnDataTypeExtension> ToProtoExtension() const;
  constexpr bool operator==(const ColumnDataType &) const = default;

  static const ColumnDataType kBoolean;
  static const ColumnDataType kInt8;
  static const ColumnDataType kInt16;
  static const ColumnDataType kInt32;
  static const ColumnDataType kInt64;
  static const ColumnDataType kUint8;
  static const ColumnDataType kUint16;
  static const ColumnDataType kUint32;
  static const ColumnDataType kUint64;
  static const ColumnDataType kFloat32;
  static const ColumnDataType kFloat64;
  static const ColumnDataType kBinary;
  static const ColumnDataType kString;
  static const ColumnDataType kDate;
  static const ColumnDataType kDateTime;
  static const ColumnDataType kTimestampSecond;
  static const ColumnDataType kTimestampMillisecond;
  static const ColumnDataType kTimestampMicrosecond;
  static const ColumnDataType kTimestampNanosecond;
  static const ColumnDataType kJson;
};

inline constexpr ColumnDataType ColumnDataType::kBoolean{Kind::kBoolean};
inline constexpr ColumnDataType ColumnDataType::kInt8{Kind::kInt8};
inline constexpr ColumnDataType ColumnDataType::kInt16{Kind::kInt16};
inline constexpr ColumnDataType ColumnDataType::kInt32{Kind::kInt32};
inline constexpr ColumnDataType ColumnDataType::kInt64{Kind::kInt64};
inline constexpr ColumnDataType ColumnDataType::kUint8{Kind::kUint8};
inline constexpr ColumnDataType ColumnDataType::kUint16{Kind::kUint16};
inline constexpr ColumnDataType ColumnDataType::kUint32{Kind::kUint32};
inline constexpr ColumnDataType ColumnDataType::kUint64{Kind::kUint64};
inline constexpr ColumnDataType ColumnDataType::kFloat32{Kind::kFloat32};
inline constexpr ColumnDataType ColumnDataType::kFloat64{Kind::kFloat64};
inline constexpr ColumnDataType ColumnDataType::kBinary{Kind::kBinary};
inline constexpr ColumnDataType ColumnDataType::kString{Kind::kString};
inline constexpr ColumnDataType ColumnDataType::kDate{Kind::kDate};
inline constexpr ColumnDataType ColumnDataType::kDateTime{Kind::kDateTime};
inline constexpr ColumnDataType ColumnDataType::kTimestampSecond{
    Kind::kTimestampSecond};
inline constexpr ColumnDataType ColumnDataType::kTimestampMillisecond{
    Kind::kTimestampMillisecond};
inline constexpr ColumnDataType ColumnDataType::kTimestampMicrosecond{
    Kind::kTimestampMicrosecond};
inline constexpr ColumnDataType ColumnDataType::kTimestampNanosecond{
    Kind::kTimestampNanosecond};
inline constexpr ColumnDataType ColumnDataType::kJson{Kind::kJson};

constexpr ColumnDataType ToColumnDataType(TimestampUnit unit) noexcept {
  switch (unit) {
  case TimestampUnit::kSecond:
    return ColumnDataType::kTimestampSecond;
  case TimestampUnit::kMillisecond:
    return ColumnDataType::kTimestampMillisecond;
  case TimestampUnit::kMicrosecond:
    return ColumnDataType::kTimestampMicrosecond;
  case TimestampUnit::kNanosecond:
    return ColumnDataType::kTimestampNanosecond;
  }
  return ColumnDataType::kTimestampNanosecond;
}

constexpr std::int64_t OneHourTicks(TimestampUnit unit) noexcept {
  switch (unit) {
  case TimestampUnit::kSecond:
    return 3'600;
  case TimestampUnit::kMillisecond:
    return 3'600'000;
  case TimestampUnit::kMicrosecond:
    return 3'600'000'000;
  case TimestampUnit::kNanosecond:
    return 3'600'000'000'000;
  }
  return 0;
}

/** Borrowed UTF-8 JSON, encoded as binary with the JSON type extension. */
struct JsonView {
  std::string_view value;
  constexpr bool operator==(const JsonView &) const = default;
};
using BinaryView = std::span<const std::byte>;
/** Allocation-free scalar storage; string, binary and JSON payloads remain
 * borrowed. */
using ValueView =
    std::variant<std::monostate, bool, std::int8_t, std::int16_t, std::int32_t,
                 std::int64_t, std::uint8_t, std::uint16_t, std::uint32_t,
                 std::uint64_t, float, double, BinaryView, std::string_view,
                 Date, DateTime, TimestampSecond, TimestampMillisecond,
                 TimestampMicrosecond, TimestampNanosecond, Decimal128,
                 JsonView>;

/** Copies borrowed payloads into owned Protobuf storage, which may allocate. */
greptime::v1::Value ToProtoValue(const ValueView &value);

} // namespace greptimedb
