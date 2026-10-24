#pragma once

#include <greptimedb/batch/column_buf.h>
#include <greptimedb/row/annotations.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <type_traits>

namespace greptimedb::row_detail {

template <class T> struct BorrowType {
  using Type = T;
};
template <> struct BorrowType<std::string> {
  using Type = std::string_view;
};
template <> struct BorrowType<std::vector<std::byte>> {
  using Type = BinaryView;
};
template <> struct BorrowType<std::vector<std::uint8_t>> {
  using Type = std::span<const std::uint8_t>;
};
template <class T> struct BorrowType<std::optional<T>> {
  using Type = std::optional<typename BorrowType<T>::Type>;
};
template <class T> using Borrowed = typename BorrowType<T>::Type;

template <class T> Borrowed<T> Borrow(const T &value) noexcept {
  if constexpr (OptionalTraits<T>::kOptional) {
    if (!value)
      return std::nullopt;
    return Borrow(*value);
  } else if constexpr (std::same_as<T, std::string>) {
    return std::string_view(value);
  } else if constexpr (std::same_as<T, std::vector<std::byte>>) {
    return BinaryView(value);
  } else if constexpr (std::same_as<T, std::vector<std::uint8_t>>) {
    return std::span<const std::uint8_t>(value);
  } else {
    return value;
  }
}

template <class T>
inline constexpr bool kPayload =
    std::same_as<T, std::string_view> || std::same_as<T, BinaryView> ||
    std::same_as<T, JsonView> || std::same_as<T, std::span<const std::uint8_t>>;

template <class T> std::span<const std::byte> PayloadBytes(T value) noexcept {
  if constexpr (std::same_as<T, BinaryView>)
    return value;
  else if constexpr (std::same_as<T, std::span<const std::uint8_t>>)
    return std::as_bytes(value);
  else if constexpr (std::same_as<T, JsonView>)
    return std::as_bytes(std::span(value.value.data(), value.value.size()));
  else
    return std::as_bytes(std::span(value.data(), value.size()));
}

template <class T> ValueView ToValue(T value, ColumnDataType type) noexcept {
  if constexpr (std::same_as<T, __int128>) {
    return Decimal128{value, type.precision, type.scale};
  } else if constexpr (std::same_as<T, std::span<const std::uint8_t>>) {
    return std::as_bytes(value);
  } else if constexpr (std::same_as<T, std::string_view>) {
    if (type == ColumnDataType::kJson)
      return JsonView{value};
    return value;
  } else if constexpr (std::same_as<T, std::int64_t>) {
    if (type == ColumnDataType::kTimestampSecond)
      return TimestampSecond{value};
    if (type == ColumnDataType::kTimestampMillisecond)
      return TimestampMillisecond{value};
    if (type == ColumnDataType::kTimestampMicrosecond)
      return TimestampMicrosecond{value};
    if (type == ColumnDataType::kTimestampNanosecond)
      return TimestampNanosecond{value};
    return value;
  } else
    return value;
}

inline bool ValidCoefficient(__int128 value, std::uint32_t precision) noexcept {
  if (precision == 0 || precision > 38)
    return false;
  unsigned __int128 limit = 1;
  for (std::uint32_t i = 0; i < precision; ++i)
    limit *= 10;
  const auto bits = static_cast<unsigned __int128>(value);
  const auto magnitude =
      value < 0 ? static_cast<unsigned __int128>(0) - bits : bits;
  return magnitude < limit;
}

/** Non-null values are compact. Nullable columns map row indexes to dense value
 * positions. Variable payloads share a reusable flat arena; offsets remain
 * valid when reservation moves that arena. */
template <class Member> class ReflectedColumnBuf {
public:
  using View = Borrowed<Member>;
  using Base = typename OptionalTraits<View>::Type;
  struct Slice {
    std::size_t offset{}, size{};
  };
  struct NoPositions {};
  using Stored = std::conditional_t<kPayload<Base>, Slice, Base>;
  using Positions =
      std::conditional_t<OptionalTraits<View>::kOptional,
                         detail::ColumnArray<std::size_t>, NoPositions>;

  explicit ReflectedColumnBuf(ColumnMeta meta) noexcept : meta_(meta) {}
  const ColumnMeta &Metadata() const noexcept { return meta_; }
  const NullMask &Nulls() const noexcept { return nulls_; }
  const NullMask &NullMaskValue() const noexcept { return nulls_; }
  std::size_t RowCount() const noexcept { return nulls_.RowCount(); }
  std::size_t NonNullLen() const noexcept { return values_.Size(); }
  std::size_t PayloadCapacity() const noexcept { return arena_.Capacity(); }
  bool OwnsPayload(std::span<const std::byte> bytes) const noexcept {
    return detail::Overlaps(bytes, arena_.Data(), arena_.Capacity()) ||
           detail::Overlaps(bytes, nulls_.RawBytes().data(),
                            nulls_.AllocatedBytes());
  }
  static std::span<const std::byte> InputBytes(const View &value) noexcept {
    if constexpr (kPayload<Base>) {
      if (const auto *base = BasePointer(value))
        return PayloadBytes(*base);
    }
    return {};
  }
  std::size_t RowCapacity() const noexcept {
    auto rows = std::max(values_.Capacity(), nulls_.RowCapacity());
    if constexpr (OptionalTraits<View>::kOptional)
      rows = std::max(rows, positions_.Capacity());
    return rows;
  }
  std::size_t AllocatedBytes() const noexcept {
    auto bytes =
        detail::SaturatingAdd(detail::SaturatingAdd(values_.AllocatedBytes(),
                                                    arena_.AllocatedBytes()),
                              nulls_.AllocatedBytes());
    if constexpr (OptionalTraits<View>::kOptional)
      bytes = detail::SaturatingAdd(bytes, positions_.AllocatedBytes());
    return bytes;
  }
  std::expected<void, ColumnBufError> ReserveRows(std::size_t rows) noexcept {
    if (!values_.Reserve(rows))
      return AllocationFailure();
    if constexpr (OptionalTraits<View>::kOptional) {
      if (!positions_.Reserve(rows))
        return AllocationFailure();
    }
    return nulls_.ReserveRows(rows);
  }
  std::expected<void, ColumnBufError> ReserveBytes(std::size_t bytes) noexcept {
    if constexpr (kPayload<Base>) {
      if (!arena_.Reserve(bytes))
        return AllocationFailure();
    }
    return {};
  }

  /** May grow capacity, but never changes logical rows or existing payloads. */
  std::expected<void, ColumnBufError> Prepare(const View &value) noexcept {
    const Base *base = BasePointer(value);
    if constexpr (std::same_as<Base, Decimal128>) {
      if (base &&
          (base->precision != meta_.data_type.precision ||
           base->scale != meta_.data_type.scale || base->precision == 0 ||
           base->precision > 38 || base->scale > base->precision ||
           !ValidCoefficient(base->value, base->precision)))
        return std::unexpected(ColumnBufError::kInvalidDecimal);
    } else if constexpr (std::same_as<Base, __int128>) {
      if (base && !ValidCoefficient(*base, meta_.data_type.precision))
        return std::unexpected(ColumnBufError::kInvalidDecimal);
    }
    if (RowCount() == std::numeric_limits<std::size_t>::max())
      return AllocationFailure();
    if (auto reserve = nulls_.ReserveRows(RowCount() + 1); !reserve)
      return reserve;
    if constexpr (OptionalTraits<View>::kOptional) {
      if (!positions_.Reserve(RowCount() + 1))
        return AllocationFailure();
    }
    if (!base)
      return {};
    if (!values_.Reserve(values_.Size() + 1))
      return AllocationFailure();
    if constexpr (kPayload<Base>) {
      const auto size = PayloadBytes(*base).size();
      if (size > std::numeric_limits<std::size_t>::max() - arena_.Size())
        return AllocationFailure();
      if (!arena_.Reserve(arena_.Size() + size))
        return AllocationFailure();
    }
    return {};
  }

  /** Requires successful Prepare for this value with no intervening append. */
  void Commit(const View &value) noexcept {
    const Base *base = BasePointer(value);
    if constexpr (OptionalTraits<View>::kOptional)
      positions_.Append(base ? values_.Size() : kNullPosition);
    if (base) {
      if constexpr (kPayload<Base>) {
        const auto bytes = PayloadBytes(*base);
        const auto offset = arena_.Size();
        if (!bytes.empty())
          std::memcpy(arena_.Data() + offset, bytes.data(), bytes.size());
        arena_.SetSize(offset + bytes.size());
        values_.Append(Slice{offset, bytes.size()});
      } else
        values_.Append(*base);
    }
    const auto pushed = nulls_.Push(base == nullptr);
    if (!pushed)
      std::abort();
    assert(values_.Size() + nulls_.NullCount() == RowCount());
    if constexpr (OptionalTraits<View>::kOptional)
      assert(positions_.Size() == RowCount());
  }

  /** The caller checks row < RowCount(). Returned payload views borrow this
   * column. */
  View BorrowAt(std::size_t row) const noexcept {
    assert(row < RowCount());
    if constexpr (OptionalTraits<View>::kOptional) {
      const auto position = positions_[row];
      if (position == kNullPosition)
        return std::nullopt;
      return BorrowNonNull(position);
    } else
      return BorrowNonNull(row);
  }
  ValueView ValueAt(std::size_t row) const noexcept {
    const auto value = BorrowAt(row);
    const Base *base = BasePointer(value);
    return base ? ToValue(*base, meta_.data_type) : ValueView{std::monostate{}};
  }
  Base BorrowNonNull(std::size_t index) const noexcept {
    if constexpr (kPayload<Base>) {
      const auto slice = values_[index];
      const auto *bytes =
          slice.size == 0 ? nullptr : arena_.Data() + slice.offset;
      if constexpr (std::same_as<Base, BinaryView>)
        return {bytes, slice.size};
      else if constexpr (std::same_as<Base, std::span<const std::uint8_t>>)
        return {reinterpret_cast<const std::uint8_t *>(bytes), slice.size};
      else {
        const std::string_view text(
            slice.size == 0 ? "" : reinterpret_cast<const char *>(bytes),
            slice.size);
        if constexpr (std::same_as<Base, JsonView>)
          return JsonView{text};
        else
          return text;
      }
    } else
      return values_[index];
  }
  void Clear() noexcept { ClearWithPolicy(BufferPolicy{}); }
  void ClearUnbounded() noexcept { ClearWithPolicy(BufferPolicy::Unbounded()); }
  void ClearWithPolicy(BufferPolicy policy) noexcept {
    const bool release = policy.max_retained_rows == 0 ||
                         policy.max_retained_bytes == 0 ||
                         policy.ShouldShrink(RowCapacity(), AllocatedBytes());
    values_.Clear();
    arena_.Clear();
    if constexpr (OptionalTraits<View>::kOptional)
      positions_.Clear();
    nulls_.ClearWithPolicy(release ? BufferPolicy::Zero()
                                   : BufferPolicy::Unbounded());
    if (release) {
      values_.Release();
      arena_.Release();
      if constexpr (OptionalTraits<View>::kOptional)
        positions_.Release();
    }
  }
  void ShrinkToFit() noexcept {
    if (!values_.ResizeCapacity(values_.Size()))
      return;
    if (!arena_.ResizeCapacity(arena_.Size()))
      return;
    if constexpr (OptionalTraits<View>::kOptional) {
      if (!positions_.ResizeCapacity(positions_.Size()))
        return;
    }
    nulls_.ShrinkToFit();
  }

private:
  static const Base *BasePointer(const View &value) noexcept {
    if constexpr (OptionalTraits<View>::kOptional)
      return value ? &*value : nullptr;
    else
      return &value;
  }
  static auto AllocationFailure() noexcept {
    return std::unexpected(ColumnBufError::kAllocationFailure);
  }
  ColumnMeta meta_;
  static constexpr auto kNullPosition = std::numeric_limits<std::size_t>::max();
  detail::ColumnArray<Stored> values_;
  detail::ColumnArray<std::byte> arena_;
  [[no_unique_address]] Positions positions_;
  NullMask nulls_;
};

/** Protobuf owns the encoded copy; its allocator is outside ingestion buffers.
 */
inline void EmitValue(greptime::v1::Column::Values &wire,
                      const ValueView &value) {
  std::visit(
      [&wire](const auto &v) {
        using V = std::remove_cvref_t<decltype(v)>;
        if constexpr (std::same_as<V, std::monostate>)
          return;
        else if constexpr (std::same_as<V, bool>)
          wire.add_bool_values(v);
        else if constexpr (std::same_as<V, std::int8_t>)
          wire.add_i8_values(v);
        else if constexpr (std::same_as<V, std::int16_t>)
          wire.add_i16_values(v);
        else if constexpr (std::same_as<V, std::int32_t>)
          wire.add_i32_values(v);
        else if constexpr (std::same_as<V, std::int64_t>)
          wire.add_i64_values(v);
        else if constexpr (std::same_as<V, std::uint8_t>)
          wire.add_u8_values(v);
        else if constexpr (std::same_as<V, std::uint16_t>)
          wire.add_u16_values(v);
        else if constexpr (std::same_as<V, std::uint32_t>)
          wire.add_u32_values(v);
        else if constexpr (std::same_as<V, std::uint64_t>)
          wire.add_u64_values(v);
        else if constexpr (std::same_as<V, float>)
          wire.add_f32_values(v);
        else if constexpr (std::same_as<V, double>)
          wire.add_f64_values(v);
        else if constexpr (std::same_as<V, std::string_view>)
          wire.add_string_values(v.empty() ? "" : v.data(), v.size());
        else if constexpr (std::same_as<V, BinaryView>)
          wire.add_binary_values(
              v.empty() ? "" : reinterpret_cast<const char *>(v.data()),
              v.size());
        else if constexpr (std::same_as<V, JsonView>)
          wire.add_binary_values(v.value.empty() ? "" : v.value.data(),
                                 v.value.size());
        else if constexpr (std::same_as<V, Date>)
          wire.add_date_values(v.value);
        else if constexpr (std::same_as<V, DateTime>)
          wire.add_datetime_values(v.value);
        else if constexpr (std::same_as<V, TimestampSecond>)
          wire.add_timestamp_second_values(v.value);
        else if constexpr (std::same_as<V, TimestampMillisecond>)
          wire.add_timestamp_millisecond_values(v.value);
        else if constexpr (std::same_as<V, TimestampMicrosecond>)
          wire.add_timestamp_microsecond_values(v.value);
        else if constexpr (std::same_as<V, TimestampNanosecond>)
          wire.add_timestamp_nanosecond_values(v.value);
        else if constexpr (std::same_as<V, Decimal128>)
          *wire.add_decimal128_values() = v.ToProto();
      },
      value);
}

template <class Member>
void EncodeColumn(greptime::v1::Column &wire,
                  const ReflectedColumnBuf<Member> &column) {
  const auto &meta = column.Metadata();
  wire.set_column_name(meta.name.data(), meta.name.size());
  wire.set_semantic_type(ToProto(meta.semantic_type));
  wire.set_datatype(meta.data_type.ToProtoDatatype());
  if (auto extension = meta.data_type.ToProtoExtension())
    *wire.mutable_datatype_extension() = std::move(*extension);
  const auto mask = column.Nulls().Bytes();
  if (!mask.empty())
    wire.set_null_mask(mask.data(), mask.size());
  auto &values = *wire.mutable_values();
  for (std::size_t i = 0; i < column.NonNullLen(); ++i)
    EmitValue(values, ToValue(column.BorrowNonNull(i), meta.data_type));
}

} // namespace greptimedb::row_detail
