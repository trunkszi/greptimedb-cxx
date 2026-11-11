/** Typed non-null column values and little-endian GreptimeDB null masks. */
#pragma once

#include <greptime/v1/column.pb.h>
#include <greptimedb/batch/leaf_primitives.h>
#include <greptimedb/core/buffer_policy.h>
#include <greptimedb/core/types.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
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

void *AllocateZeroedColumnBytes(std::size_t count, std::size_t elem_size, std::size_t alignment) noexcept;
void FreeColumnBytes(void *ptr) noexcept;

template <class T> struct ColumnBufferDeleter {
  std::size_t capacity = 0;
  void operator()(T *ptr) const noexcept {
    if (!ptr) return;
    if constexpr (!std::is_trivially_destructible_v<T>) std::destroy_n(ptr, capacity);
    FreeColumnBytes(ptr);
  }
};

/** Owned capacity with fallible growth and no allocation during a clear. */
template <class T> class ColumnArray {
public:
  using BufferPtr = std::unique_ptr<T, ColumnBufferDeleter<T>>;

  ColumnArray() = default;
  ColumnArray(ColumnArray &&other) noexcept { Swap(other); }
  ColumnArray &operator=(ColumnArray &&other) noexcept {
    ColumnArray moved(std::move(other));
    Swap(moved);
    return *this;
  }
  ColumnArray(const ColumnArray &) = delete("column array owns one buffer");
  ColumnArray &operator=(const ColumnArray &) = delete("column array owns one buffer");

  bool Reserve(std::size_t capacity) noexcept {
    const auto current = Capacity();
    if (capacity <= current) return true;
    constexpr auto limit = MaxCapacity();
    if (capacity > limit) return false;
    const auto grown = current <= limit / 2 ? current * 2 : limit;
    return ResizeCapacity(std::max(capacity, grown));
  }
  bool ResizeCapacity(std::size_t capacity) noexcept {
    static_assert(std::is_default_constructible_v<T> && std::is_move_assignable_v<T> && std::is_destructible_v<T>);
    const auto current = Capacity();
    if (capacity == current) return true;
    if (capacity < size_ || capacity > MaxCapacity()) return false;
    BufferPtr next;
    if (capacity != 0) {
      void *raw = AllocateZeroedColumnBytes(capacity, sizeof(T), alignof(T));
      if (!raw) return false;
      T *typed = static_cast<T *>(raw);
      if constexpr (!std::is_trivially_default_constructible_v<T>) {
        for (std::size_t i = 0; i < capacity; ++i)
          std::construct_at(typed + i);
      }
      next = BufferPtr(typed, ColumnBufferDeleter<T>{capacity});
    }
    // Inactive payload slots own reusable allocations too.
    const auto retained = std::is_trivially_copyable_v<T> ? size_ : current;
    for (std::size_t i = 0; i < std::min(capacity, retained); ++i)
      next.get()[i] = std::move(data_.get()[i]);
    data_ = std::move(next);
    return true;
  }
  void Append(T value) noexcept {
    assert(size_ < Capacity());
    AppendUnchecked(data_.get(), size_, std::move(value));
  }
  void SetSize(std::size_t size) noexcept {
    assert(size <= Capacity());
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
  }
  T &operator[](std::size_t index) noexcept { return data_.get()[index]; }
  const T &operator[](std::size_t index) const noexcept { return data_.get()[index]; }
  T *Data() noexcept { return data_.get(); }
  const T *Data() const noexcept { return data_.get(); }
  std::size_t Size() const noexcept { return size_; }
  std::size_t Capacity() const noexcept { return data_.get_deleter().capacity; }
  std::size_t AllocatedBytes() const noexcept { return Capacity() * sizeof(T); }

private:
  static constexpr std::size_t MaxCapacity() noexcept {
    // Stay strictly within signed ptrdiff_t object-size bounds.
    return (static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max()) - sizeof(std::size_t)) / sizeof(T);
  }
  BufferPtr data_;
  std::size_t size_ = 0;
};

} // namespace detail

/** A set bit identifies a null row; Bytes() omits an all-non-null mask. */
class NullMask {
public:
  explicit NullMask(std::size_t rows = 0) noexcept;
  NullMask(NullMask &&other) noexcept;
  NullMask &operator=(NullMask &&other) noexcept;
  std::expected<void, ColumnBufError> Push(bool is_null) noexcept;
  std::expected<void, ColumnBufError> ReserveRows(std::size_t rows) noexcept;
  bool IsNull(std::size_t row) const noexcept { return detail::NullMaskTestBit(bytes_.Data(), row_count_, row); }
  std::size_t RowCount() const noexcept { return row_count_; }
  std::size_t NullCount() const noexcept { return null_count_; }
  std::size_t NonNullCount() const noexcept { return row_count_ - null_count_; }
  bool HasNulls() const noexcept { return null_count_ != 0; }
  std::span<const std::uint8_t> Bytes() const noexcept {
    return HasNulls() ? RawBytes() : std::span<const std::uint8_t>{};
  }
  std::span<const std::uint8_t> RawBytes() const noexcept { return {bytes_.Data(), bytes_.Size()}; }
  std::size_t AllocatedBytes() const noexcept { return bytes_.AllocatedBytes(); }
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
  TypedColumnBuf(std::string_view name, SemanticType semantic_type, ColumnDataType data_type,
                 std::size_t rows = 0) noexcept;
  TypedColumnBuf(TypedColumnBuf &&other) noexcept;
  TypedColumnBuf &operator=(TypedColumnBuf &&other) noexcept;
  TypedColumnBuf(const TypedColumnBuf &) = delete("column buffer owns payload slots");
  TypedColumnBuf &operator=(const TypedColumnBuf &) = delete("column buffer owns payload slots");
  static std::expected<TypedColumnBuf, ColumnBufError> Create(std::string_view name, SemanticType semantic_type,
                                                              ColumnDataType data_type, std::size_t rows = 0) noexcept;
  std::expected<TypedColumnBuf, ColumnBufError> Clone() const noexcept;
  bool IsValid() const noexcept;
  std::string_view Name() const noexcept {
    return name_.Size() == 0 ? std::string_view{} : std::string_view(name_.Data(), name_.Size());
  }
  SemanticType SemanticTypeValue() const noexcept { return semantic_type_; }
  ColumnDataType DataType() const noexcept { return data_type_; }
  const NullMask &Nulls() const noexcept { return null_mask_; }
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
  using Values = std::variant<Array<bool>, Array<std::int8_t>, Array<std::int16_t>, Array<std::int32_t>,
                              Array<std::int64_t>, Array<std::uint8_t>, Array<std::uint16_t>, Array<std::uint32_t>,
                              Array<std::uint64_t>, Array<float>, Array<double>, Array<Payload>, Array<Decimal128>>;
  void InitializeValues() noexcept;
  bool Matches(const ValueView &value) const noexcept;
  static std::span<const std::byte> InputBytes(const ValueView &value) noexcept;
  bool OwnsInput(std::span<const std::byte> bytes) const noexcept;
  std::expected<void, ColumnBufError> PreparePush(const ValueView &value) noexcept;
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
