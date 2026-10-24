/** Typed non-null column values and little-endian GreptimeDB null masks. */
#pragma once

#include <greptime/v1/column.pb.h>
#include <greptimedb/core/buffer_policy.h>
#include <greptimedb/core/types.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace greptimedb {

enum class ColumnBufError {
  kNullTimestamp,
  kInvalidDecimal,
  kTypeMismatch,
  kInvalidSchema,
  kAllocationFailure,
};

namespace detail {

/** Owned capacity with fallible growth and no allocation during a clear. */
template <class T> class ColumnArray {
public:
  ColumnArray() = default;
  ColumnArray(ColumnArray &&other) noexcept { Swap(other); }
  ColumnArray &operator=(ColumnArray &&other) noexcept {
    ColumnArray moved(std::move(other));
    Swap(moved);
    return *this;
  }
  ColumnArray(const ColumnArray &) = delete;
  ColumnArray &operator=(const ColumnArray &) = delete;

  bool Reserve(std::size_t capacity) noexcept {
    if (capacity <= capacity_)
      return true;
    constexpr auto limit = MaxCapacity();
    if (capacity > limit)
      return false;
    const auto grown = capacity_ <= limit / 2 ? capacity_ * 2 : limit;
    return ResizeCapacity(std::max(capacity, grown));
  }
  bool ResizeCapacity(std::size_t capacity) noexcept {
    if (capacity == capacity_)
      return true;
    if (capacity < size_ || capacity > MaxCapacity())
      return false;
    std::unique_ptr<T[]> next;
    if (capacity != 0) {
      next.reset(new (std::nothrow) T[capacity]{});
      if (!next)
        return false;
    }
    // Inactive payload slots own reusable allocations too.
    const auto retained = std::is_trivially_copyable_v<T> ? size_ : capacity_;
    for (std::size_t i = 0; i < std::min(capacity, retained); ++i)
      next[i] = std::move(data_[i]);
    data_ = std::move(next);
    capacity_ = capacity;
    return true;
  }
  void Append(T value) noexcept {
    assert(size_ < capacity_);
    data_[size_++] = std::move(value);
  }
  void SetSize(std::size_t size) noexcept {
    assert(size <= capacity_);
    size_ = size;
  }
  void Clear() noexcept { size_ = 0; }
  void Release() noexcept {
    ColumnArray empty;
    Swap(empty);
  }
  void Swap(ColumnArray &other) noexcept {
    data_.swap(other.data_);
    std::swap(size_, other.size_);
    std::swap(capacity_, other.capacity_);
  }
  T &operator[](std::size_t index) noexcept { return data_[index]; }
  const T &operator[](std::size_t index) const noexcept { return data_[index]; }
  T *Data() noexcept { return data_.get(); }
  const T *Data() const noexcept { return data_.get(); }
  std::size_t Size() const noexcept { return size_; }
  std::size_t Capacity() const noexcept { return capacity_; }
  std::size_t AllocatedBytes() const noexcept { return capacity_ * sizeof(T); }

private:
  static constexpr std::size_t MaxCapacity() noexcept {
    // Leave room for an array allocation cookie inside the object-size limit.
    return (static_cast<std::size_t>(
                std::numeric_limits<std::ptrdiff_t>::max()) -
            sizeof(std::size_t)) /
           sizeof(T);
  }
  std::unique_ptr<T[]> data_;
  std::size_t size_ = 0;
  std::size_t capacity_ = 0;
};

inline std::size_t SaturatingAdd(std::size_t a, std::size_t b) noexcept {
  return b > std::numeric_limits<std::size_t>::max() - a
             ? std::numeric_limits<std::size_t>::max()
             : a + b;
}

inline bool Overlaps(std::span<const std::byte> source, const void *data,
                     std::size_t capacity) noexcept {
  if (source.empty() || capacity == 0)
    return false;
  const auto input = reinterpret_cast<std::uintptr_t>(source.data());
  const auto owned = reinterpret_cast<std::uintptr_t>(data);
  return input >= owned ? input - owned < capacity
                        : owned - input < source.size();
}

} // namespace detail

/** A set bit identifies a null row; Bytes() omits an all-non-null mask. */
class NullMask {
public:
  explicit NullMask(std::size_t rows = 0) noexcept;
  NullMask(NullMask &&other) noexcept;
  NullMask &operator=(NullMask &&other) noexcept;
  std::expected<void, ColumnBufError> Push(bool is_null) noexcept;
  std::expected<void, ColumnBufError> ReserveRows(std::size_t rows) noexcept;
  bool IsNull(std::size_t row) const noexcept;
  std::size_t RowCount() const noexcept { return row_count_; }
  std::size_t NullCount() const noexcept { return null_count_; }
  std::size_t NonNullCount() const noexcept { return row_count_ - null_count_; }
  bool HasNulls() const noexcept { return null_count_ != 0; }
  std::span<const std::uint8_t> Bytes() const noexcept {
    return HasNulls() ? RawBytes() : std::span<const std::uint8_t>{};
  }
  std::span<const std::uint8_t> RawBytes() const noexcept {
    return {bytes_.Data(), bytes_.Size()};
  }
  std::size_t AllocatedBytes() const noexcept {
    return bytes_.AllocatedBytes();
  }
  std::size_t RowCapacity() const noexcept;
  void Clear() noexcept { ClearWithPolicy(BufferPolicy{}); }
  void ClearWithPolicy(BufferPolicy policy) noexcept;
  void ClearUnbounded() noexcept { ClearWithPolicy(BufferPolicy::Unbounded()); }
  void ShrinkToFit() noexcept;

private:
  friend class TypedColumnBuf;
  void CommitPush(bool is_null) noexcept;
  detail::ColumnArray<std::uint8_t> bytes_;
  std::size_t row_count_ = 0;
  std::size_t null_count_ = 0;
};

/** Capacity accounting includes active and spare payload slots, excluding the
 * schema name and allocator bookkeeping. Push validates before changing rows.
 * Cleared payload slots retain their allocation for the next ingestion cycle.
 */
class TypedColumnBuf {
public:
  TypedColumnBuf(std::string_view name, SemanticType semantic_type,
                 ColumnDataType data_type, std::size_t rows = 0) noexcept;
  TypedColumnBuf(TypedColumnBuf &&other) noexcept;
  TypedColumnBuf &operator=(TypedColumnBuf &&other) noexcept;
  TypedColumnBuf(const TypedColumnBuf &) = delete;
  TypedColumnBuf &operator=(const TypedColumnBuf &) = delete;
  static std::expected<TypedColumnBuf, ColumnBufError>
  Create(std::string_view name, SemanticType semantic_type,
         ColumnDataType data_type, std::size_t rows = 0) noexcept;
  std::expected<TypedColumnBuf, ColumnBufError> Clone() const noexcept;
  bool IsValid() const noexcept;
  std::string_view Name() const noexcept {
    return name_.Size() == 0 ? std::string_view{}
                             : std::string_view(name_.Data(), name_.Size());
  }
  SemanticType SemanticTypeValue() const noexcept { return semantic_type_; }
  ColumnDataType DataType() const noexcept { return data_type_; }
  const NullMask &Nulls() const noexcept { return null_mask_; }
  const NullMask &NullMaskValue() const noexcept { return null_mask_; }
  std::size_t RowCount() const noexcept { return null_mask_.RowCount(); }
  std::size_t NonNullLen() const noexcept;
  std::expected<void, ColumnBufError> ReserveRows(std::size_t rows) noexcept;
  std::expected<void, ColumnBufError> Push(const ValueView &value) noexcept;
  /** Protobuf allocation belongs to the upstream codec boundary. Requires
   * IsValid(); use TryToProtoColumn when construction has not been checked. */
  greptime::v1::Column ToProtoColumn() const;
  std::expected<greptime::v1::Column, ColumnBufError> TryToProtoColumn() const;
  std::size_t AllocatedBytes() const noexcept;
  std::size_t RowCapacity() const noexcept;
  void Clear() noexcept { ClearWithPolicy(BufferPolicy{}); }
  void ClearWithPolicy(BufferPolicy policy) noexcept;
  void ClearUnbounded() noexcept { ClearWithPolicy(BufferPolicy::Unbounded()); }
  void ShrinkToFit() noexcept;

private:
  friend class ColumnarBatch;
  using Payload = detail::ColumnArray<std::byte>;
  template <class T> using Array = detail::ColumnArray<T>;
  using Values =
      std::variant<Array<bool>, Array<std::int8_t>, Array<std::int16_t>,
                   Array<std::int32_t>, Array<std::int64_t>,
                   Array<std::uint8_t>, Array<std::uint16_t>,
                   Array<std::uint32_t>, Array<std::uint64_t>, Array<float>,
                   Array<double>, Array<Payload>, Array<Decimal128>>;
  void InitializeValues() noexcept;
  bool Matches(const ValueView &value) const noexcept;
  static std::span<const std::byte> InputBytes(const ValueView &value) noexcept;
  bool OwnsInput(std::span<const std::byte> bytes) const noexcept;
  std::expected<void, ColumnBufError>
  PreparePush(const ValueView &value) noexcept;
  void CommitPush(const ValueView &value) noexcept;
  void ReleaseValues() noexcept;
  Array<char> name_;
  SemanticType semantic_type_;
  ColumnDataType data_type_;
  NullMask null_mask_;
  Values values_;
  bool initialized_ = false;
};

static_assert(MemoryBufferConcept<NullMask>);
static_assert(MemoryBufferConcept<TypedColumnBuf>);

} // namespace greptimedb
