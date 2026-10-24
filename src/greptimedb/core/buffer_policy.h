#pragma once

#include <concepts>
#include <cstddef>
#include <limits>

namespace greptimedb {

inline constexpr std::size_t kDefaultMaxRetainedRows = 65'536;
inline constexpr std::size_t kDefaultMaxRetainedBytes = 8 * 1024 * 1024;

/** Independent watermarks for reusable row and value allocations. */
struct BufferPolicy {
  std::size_t max_retained_rows = kDefaultMaxRetainedRows;
  std::size_t max_retained_bytes = kDefaultMaxRetainedBytes;

  static constexpr BufferPolicy Unbounded() noexcept {
    return {std::numeric_limits<std::size_t>::max(),
            std::numeric_limits<std::size_t>::max()};
  }
  static constexpr BufferPolicy Zero() noexcept { return {0, 0}; }
  constexpr bool ShouldShrink(std::size_t rows,
                              std::size_t bytes) const noexcept {
    return rows > max_retained_rows || bytes > max_retained_bytes;
  }
  constexpr bool operator==(const BufferPolicy &) const = default;
};

using BufferRetentionPolicy = BufferPolicy;

/** Capacity accounting includes nested values, excluding schema and allocator
 * bookkeeping. Clear discards rows; ShrinkToFit preserves rows while releasing
 * unused capacity. */
template <typename T>
concept MemoryBufferConcept =
    requires(T &buffer, const T &read_only, BufferPolicy policy) {
      { read_only.AllocatedBytes() } -> std::same_as<std::size_t>;
      { read_only.RowCapacity() } -> std::same_as<std::size_t>;
      { buffer.Clear() } -> std::same_as<void>;
      { buffer.ClearWithPolicy(policy) } -> std::same_as<void>;
      { buffer.ClearUnbounded() } -> std::same_as<void>;
      { buffer.ShrinkToFit() } -> std::same_as<void>;
    };

} // namespace greptimedb
