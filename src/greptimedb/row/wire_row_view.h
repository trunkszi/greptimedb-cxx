#pragma once

#include <greptimedb/row/reflected_row.h>

#include <bit>
#include <limits>

namespace greptimedb::row_detail {

inline int WireValueCount(const greptime::v1::Column::Values &v,
                          ColumnDataType type) noexcept {
  using enum ColumnDataType::Kind;
  switch (type.kind) {
  case kBoolean:
    return v.bool_values_size();
  case kInt8:
    return v.i8_values_size();
  case kInt16:
    return v.i16_values_size();
  case kInt32:
    return v.i32_values_size();
  case kInt64:
    return v.i64_values_size();
  case kUint8:
    return v.u8_values_size();
  case kUint16:
    return v.u16_values_size();
  case kUint32:
    return v.u32_values_size();
  case kUint64:
    return v.u64_values_size();
  case kFloat32:
    return v.f32_values_size();
  case kFloat64:
    return v.f64_values_size();
  case kString:
    return v.string_values_size();
  case kBinary:
  case kJson:
    return v.binary_values_size();
  case kDate:
    return v.date_values_size();
  case kDateTime:
    return v.datetime_values_size();
  case kTimestampSecond:
    return v.timestamp_second_values_size();
  case kTimestampMillisecond:
    return v.timestamp_millisecond_values_size();
  case kTimestampMicrosecond:
    return v.timestamp_microsecond_values_size();
  case kTimestampNanosecond:
    return v.timestamp_nanosecond_values_size();
  case kDecimal128:
    return v.decimal128_values_size();
  }
  return -1;
}

inline std::size_t
WireTotalValues(const greptime::v1::Column::Values &v) noexcept {
  return static_cast<std::size_t>(v.bool_values_size()) + v.i8_values_size() +
         v.i16_values_size() + v.i32_values_size() + v.i64_values_size() +
         v.u8_values_size() + v.u16_values_size() + v.u32_values_size() +
         v.u64_values_size() + v.f32_values_size() + v.f64_values_size() +
         v.binary_values_size() + v.string_values_size() +
         v.date_values_size() + v.datetime_values_size() +
         v.timestamp_second_values_size() +
         v.timestamp_millisecond_values_size() +
         v.timestamp_microsecond_values_size() +
         v.timestamp_nanosecond_values_size() + v.decimal128_values_size() +
         v.time_second_values_size() + v.time_millisecond_values_size() +
         v.time_microsecond_values_size() + v.time_nanosecond_values_size() +
         v.interval_year_month_values_size() +
         v.interval_day_time_values_size() +
         v.interval_month_day_nano_values_size() + v.list_values_size() +
         v.struct_values_size() + v.json_values_size();
}

inline bool WireExtensionMatches(const greptime::v1::Column &column,
                                 ColumnDataType type) noexcept {
  const auto &extension = column.datatype_extension();
  if (type.kind == ColumnDataType::Kind::kDecimal128) {
    return extension.has_decimal_type() &&
           extension.decimal_type().precision() ==
               static_cast<std::int32_t>(type.precision) &&
           extension.decimal_type().scale() ==
               static_cast<std::int32_t>(type.scale);
  }
  if (type == ColumnDataType::kJson)
    return extension.has_json_type() &&
           extension.json_type() == greptime::v1::JSON_BINARY;
  return extension.type_ext_case() ==
         greptime::v1::ColumnDataTypeExtension::TYPE_EXT_NOT_SET;
}

/** Validates compact values and mask padding before any repeated-field
 * indexing. */
inline std::expected<std::optional<int>, ColumnBufError>
WireIndex(const greptime::v1::Column &column, const ColumnMeta &meta,
          std::size_t row, std::size_t rows) noexcept {
  if (row >= rows || column.column_name() != meta.name ||
      column.semantic_type() != ToProto(meta.semantic_type) ||
      column.datatype() != meta.data_type.ToProtoDatatype() ||
      !WireExtensionMatches(column, meta.data_type))
    return std::unexpected(ColumnBufError::kInvalidSchema);
  const auto &mask = column.null_mask();
  if (!mask.empty() && mask.size() != rows / 8 + (rows % 8 != 0))
    return std::unexpected(ColumnBufError::kInvalidSchema);
  std::size_t nulls = 0, preceding_nulls = 0;
  for (std::size_t byte = 0; byte < mask.size(); ++byte) {
    const auto bits = static_cast<unsigned char>(mask[byte]);
    if (byte == mask.size() - 1 && rows % 8 != 0 && (bits >> (rows % 8)) != 0)
      return std::unexpected(ColumnBufError::kInvalidSchema);
    nulls += std::popcount(bits);
    if (byte < row / 8)
      preceding_nulls += std::popcount(bits);
    else if (byte == row / 8)
      preceding_nulls +=
          std::popcount(static_cast<unsigned>(bits) & ((1U << (row % 8)) - 1U));
  }
  const auto count = WireValueCount(column.values(), meta.data_type);
  if (count < 0 ||
      WireTotalValues(column.values()) != static_cast<std::size_t>(count) ||
      (!meta.nullable && nulls != 0) || nulls > rows ||
      static_cast<std::size_t>(count) != rows - nulls)
    return std::unexpected(ColumnBufError::kInvalidSchema);
  if (!mask.empty() &&
      (static_cast<unsigned char>(mask[row / 8]) & (1U << (row % 8))))
    return std::optional<int>{};
  if (row - preceding_nulls >
      static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(ColumnBufError::kInvalidSchema);
  return std::optional<int>{static_cast<int>(row - preceding_nulls)};
}

template <class Integer, class Source>
std::expected<Integer, ColumnBufError> WireInteger(Source source) noexcept {
  if (!std::in_range<Integer>(source))
    return std::unexpected(ColumnBufError::kTypeMismatch);
  return static_cast<Integer>(source);
}

template <class Base>
std::expected<Base, ColumnBufError>
ReadWireValue(const greptime::v1::Column::Values &v, ColumnDataType type,
              int i) noexcept {
  if constexpr (std::same_as<Base, bool>)
    return v.bool_values(i);
  else if constexpr (std::same_as<Base, std::int8_t>)
    return WireInteger<Base>(v.i8_values(i));
  else if constexpr (std::same_as<Base, std::int16_t>)
    return WireInteger<Base>(v.i16_values(i));
  else if constexpr (std::same_as<Base, std::int32_t>)
    return v.i32_values(i);
  else if constexpr (std::same_as<Base, std::int64_t>) {
    if (type == ColumnDataType::kTimestampSecond)
      return v.timestamp_second_values(i);
    if (type == ColumnDataType::kTimestampMillisecond)
      return v.timestamp_millisecond_values(i);
    if (type == ColumnDataType::kTimestampMicrosecond)
      return v.timestamp_microsecond_values(i);
    if (type == ColumnDataType::kTimestampNanosecond)
      return v.timestamp_nanosecond_values(i);
    return v.i64_values(i);
  } else if constexpr (std::same_as<Base, std::uint8_t>)
    return WireInteger<Base>(v.u8_values(i));
  else if constexpr (std::same_as<Base, std::uint16_t>)
    return WireInteger<Base>(v.u16_values(i));
  else if constexpr (std::same_as<Base, std::uint32_t>)
    return v.u32_values(i);
  else if constexpr (std::same_as<Base, std::uint64_t>)
    return v.u64_values(i);
  else if constexpr (std::same_as<Base, float>)
    return v.f32_values(i);
  else if constexpr (std::same_as<Base, double>)
    return v.f64_values(i);
  else if constexpr (std::same_as<Base, std::string_view>) {
    return std::string_view(type == ColumnDataType::kJson ? v.binary_values(i)
                                                          : v.string_values(i));
  } else if constexpr (std::same_as<Base, BinaryView>) {
    const auto &bytes = v.binary_values(i);
    return std::as_bytes(std::span(bytes.data(), bytes.size()));
  } else if constexpr (std::same_as<Base, std::span<const std::uint8_t>>) {
    const auto &bytes = v.binary_values(i);
    return std::span(reinterpret_cast<const std::uint8_t *>(bytes.data()),
                     bytes.size());
  } else if constexpr (std::same_as<Base, JsonView>)
    return JsonView{v.binary_values(i)};
  else if constexpr (std::same_as<Base, Date>)
    return Date{v.date_values(i)};
  else if constexpr (std::same_as<Base, DateTime>)
    return DateTime{v.datetime_values(i)};
  else if constexpr (std::same_as<Base, TimestampSecond>)
    return TimestampSecond{v.timestamp_second_values(i)};
  else if constexpr (std::same_as<Base, TimestampMillisecond>)
    return TimestampMillisecond{v.timestamp_millisecond_values(i)};
  else if constexpr (std::same_as<Base, TimestampMicrosecond>)
    return TimestampMicrosecond{v.timestamp_microsecond_values(i)};
  else if constexpr (std::same_as<Base, TimestampNanosecond>)
    return TimestampNanosecond{v.timestamp_nanosecond_values(i)};
  else if constexpr (std::same_as<Base, Decimal128> ||
                     std::same_as<Base, __int128>) {
    const auto decimal = Decimal128::FromProto(v.decimal128_values(i),
                                               type.precision, type.scale);
    if (!ValidCoefficient(decimal.value, type.precision))
      return std::unexpected(ColumnBufError::kInvalidDecimal);
    if constexpr (std::same_as<Base, __int128>)
      return decimal.value;
    else
      return decimal;
  } else
    return std::unexpected(ColumnBufError::kTypeMismatch);
}

template <class Member>
std::expected<Borrowed<Member>, ColumnBufError>
ReadWireColumn(const greptime::v1::Column &column, const ColumnMeta &meta,
               std::size_t row, std::size_t rows) noexcept {
  using View = Borrowed<Member>;
  using Base = typename OptionalTraits<View>::Type;
  const auto index = WireIndex(column, meta, row, rows);
  if (!index)
    return std::unexpected(index.error());
  if (!*index) {
    if constexpr (OptionalTraits<View>::kOptional)
      return View{};
    else
      return std::unexpected(ColumnBufError::kNullTimestamp);
  }
  auto value = ReadWireValue<Base>(column.values(), meta.data_type, **index);
  if (!value)
    return std::unexpected(value.error());
  if constexpr (OptionalTraits<View>::kOptional)
    return View{*value};
  else
    return *value;
}

template <RowConcept T, class Columns, std::size_t... I>
std::expected<RowView<T>, ColumnBufError>
ProjectWireRow(const Columns &columns, std::size_t row, std::size_t rows,
               std::index_sequence<I...>) noexcept {
  typename BorrowedRow<T>::Tuple values{};
  std::expected<void, ColumnBufError> valid;
  (
      [&] {
        if (!valid)
          return;
        auto value = ReadWireColumn<MemberType<T, I>>(
            columns[I], RowSchema<T>::kColumns[I], row, rows);
        if (!value)
          valid = std::unexpected(value.error());
        else
          std::get<I>(values) = *value;
      }(),
      ...);
  if (!valid)
    return std::unexpected(valid.error());
  return RowView<T>{BorrowedRow<T>{std::move(values)}};
}

} // namespace greptimedb::row_detail

namespace greptimedb {

/** Zero-copy typed projection of decoded GreptimeDB columns in schema order.
 * Columns may be a span or a Protobuf repeated column field. Text/binary views
 * remain valid until their decoded source is mutated or destroyed. Validation
 * does not allocate; each nullable mask is checked before compact indexing. */
template <RowConcept T, class Columns>
std::expected<RowView<T>, ColumnBufError>
ViewDecodedRow(const Columns &columns, std::size_t row,
               std::size_t row_count) noexcept {
  if (row >= row_count || static_cast<std::size_t>(columns.size()) !=
                              row_detail::RowSchema<T>::kColumns.size())
    return std::unexpected(ColumnBufError::kInvalidSchema);
  return row_detail::ProjectWireRow<T>(columns, row, row_count,
                                       row_detail::Indices<T>{});
}

/** An owning temporary cannot outlive views into its payload storage. */
template <RowConcept T, class Columns>
  requires(!std::is_lvalue_reference_v<Columns> &&
           !std::ranges::borrowed_range<Columns>)
std::expected<RowView<T>, ColumnBufError>
ViewDecodedRow(Columns &&, std::size_t, std::size_t) noexcept = delete;

} // namespace greptimedb
