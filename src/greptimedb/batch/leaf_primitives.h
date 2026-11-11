/** Protobuf-free data-plane leaf primitives shared by batch, row, and TMA probes. */
#pragma once

#include <uarch/uarch.hpp>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <utility>

namespace greptimedb::detail {

[[nodiscard, gnu::always_inline]] inline constexpr std::size_t SaturatingAdd(std::size_t a, std::size_t b) noexcept {
  return uarch::saturating_add(a, b);
}

[[nodiscard, gnu::always_inline]] inline constexpr bool
NullMaskTestBit(const std::uint8_t *__restrict bytes, std::size_t row_count, std::size_t row) noexcept {
  if (uarch::unlikely(row >= row_count)) return false;
  const auto byte_index = static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(row));
  const auto bit_offset = static_cast<unsigned>(uarch::PowerOfTwo<3>::rem(row));
  return ((bytes[byte_index] >> bit_offset) & 1U) != 0U;
}

[[nodiscard, gnu::always_inline]] inline bool Overlaps(std::span<const std::byte> source, const void *data,
                                                       std::size_t capacity) noexcept {
  const auto non_empty = uarch::flow::predicate(uarch::eager_all(!source.empty(), capacity != 0uz));
  const auto input = std::bit_cast<std::uintptr_t>(source.data());
  const auto owned = std::bit_cast<std::uintptr_t>(data);
  const auto forward = uarch::flow::predicate(input >= owned);
  const auto forward_hit = uarch::flow::predicate(input - owned < capacity);
  const auto reverse_hit = uarch::flow::predicate(owned - input < source.size());
  const auto range_hit = uarch::flow::predicate(
      forward.select_register(std::uint32_t{forward_hit.bit()}, std::uint32_t{reverse_hit.bit()}) != 0U);
  return (non_empty & range_hit).as_bool();
}

template <class T>
[[gnu::always_inline]] inline constexpr void AppendUnchecked(T *__restrict data, std::size_t &size, T value) noexcept {
  data[size++] = std::move(value);
}

} // namespace greptimedb::detail

namespace greptimedb::row_detail {

struct WireMaskIndex {
  std::uint64_t nulls{};
  std::uint64_t value_index{};
};
static_assert(uarch::NoPadding<WireMaskIndex> && uarch::RegisterPassable<WireMaskIndex> &&
              uarch::GprPair<WireMaskIndex>);

inline constexpr std::uint64_t kInvalidWireMask = std::numeric_limits<std::uint64_t>::max();
inline constexpr std::uint64_t kNullWireRow = std::numeric_limits<std::uint64_t>::max() - 1ULL;

template <bool HasTailPadding = true>
[[nodiscard, gnu::always_inline]] inline constexpr WireMaskIndex
ScanWireNullMaskSingleWord(std::uint64_t word, std::size_t row, std::size_t rows) noexcept {
  const auto bit = static_cast<unsigned>(uarch::PowerOfTwo<6>::rem(row));
  const auto prefix = (std::uint64_t{1} << bit) - 1ULL;
  const auto nulls = static_cast<std::uint64_t>(std::popcount(word));
  const auto preceding = static_cast<std::uint64_t>(std::popcount(word & prefix));
  const auto is_null = uarch::flow::predicate(((word >> bit) & 1ULL) != 0ULL);
  const auto valid_idx = is_null.select_register(kNullWireRow, static_cast<std::uint64_t>(row) - preceding);
  if constexpr (!HasTailPadding) {
    return {nulls, valid_idx};
  } else {
    const auto tail_bits = static_cast<unsigned>(uarch::PowerOfTwo<6>::rem(rows));
    const std::uint64_t high_pad_mask = tail_bits == 0U ? 0ULL : ~((std::uint64_t{1} << tail_bits) - 1ULL);
    const auto pad_clean = uarch::flow::predicate((word & high_pad_mask) == 0ULL);
    return {
        pad_clean.select_register(nulls, kInvalidWireMask),
        pad_clean.select_register(valid_idx, kInvalidWireMask),
    };
  }
}

/** Validates little-endian null-mask byte length and trailing padding, then
 * counts total and preceding nulls in 64-bit word popcount steps. Requires
 * row < rows. */
[[nodiscard, gnu::always_inline]] inline WireMaskIndex ScanWireNullMask(std::span<const char> mask, std::size_t row,
                                                                        std::size_t rows) noexcept {
  if (rows == 64uz) {
    alignas(std::uint64_t) static constexpr std::array<char, 8> kZeroWireWord{};
    const auto is_full = uarch::flow::predicate(mask.size() == 8uz);
    const auto src_addr = is_full.select_register(std::bit_cast<std::uintptr_t>(mask.data()),
                                                  std::bit_cast<std::uintptr_t>(kZeroWireWord.data()));
    std::uint64_t word = 0;
    std::memcpy(&word, std::bit_cast<const char *>(src_addr), sizeof(word));
    if constexpr (std::endian::native == std::endian::big) word = std::byteswap(word);
    const auto single = ScanWireNullMaskSingleWord<false>(word, row, 64uz);
    const auto is_empty = uarch::flow::predicate(mask.empty());
    const auto valid_nulls = is_full.select_register(single.nulls, kInvalidWireMask);
    const auto valid_idx = is_full.select_register(single.value_index, kInvalidWireMask);
    return {
        is_empty.select_register(std::uint64_t{0}, valid_nulls),
        is_empty.select_register(static_cast<std::uint64_t>(row), valid_idx),
    };
  }
  if (mask.empty()) return {0ULL, static_cast<std::uint64_t>(row)};
  const auto expected_bytes = static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(rows)) +
                              static_cast<std::size_t>(uarch::PowerOfTwo<3>::rem(rows) != 0ULL);
  if (mask.size() != expected_bytes) return {kInvalidWireMask, kInvalidWireMask};
  if (rows > 56uz && rows <= 64uz) {
    std::uint64_t word = 0;
    std::memcpy(&word, mask.data(), sizeof(word));
    if constexpr (std::endian::native == std::endian::big) word = std::byteswap(word);
    return ScanWireNullMaskSingleWord(word, row, rows);
  }
  const auto tail_bits = static_cast<unsigned>(uarch::PowerOfTwo<3>::rem(rows));
  if (tail_bits != 0U && (static_cast<unsigned char>(mask.back()) >> tail_bits) != 0U)
    return {kInvalidWireMask, kInvalidWireMask};
  std::size_t nulls = 0;
  std::size_t preceding_nulls = 0;
  const auto full_words = static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(mask.size()));
  const auto row_word = static_cast<std::size_t>(uarch::PowerOfTwo<6>::div(row));
  for (std::size_t w = 0; w < full_words; ++w) {
    std::uint64_t word = 0;
    std::memcpy(&word, mask.data() + (w * sizeof(std::uint64_t)), sizeof(word));
    if constexpr (std::endian::native == std::endian::big) word = std::byteswap(word);
    const auto set = static_cast<std::size_t>(std::popcount(word));
    nulls = detail::SaturatingAdd(nulls, set);
    if (w < row_word) {
      preceding_nulls = detail::SaturatingAdd(preceding_nulls, set);
    } else if (w == row_word) {
      const auto bit = static_cast<unsigned>(uarch::PowerOfTwo<6>::rem(row));
      const std::uint64_t prefix_mask = (std::uint64_t{1} << bit) - 1ULL;
      preceding_nulls =
          detail::SaturatingAdd(preceding_nulls, static_cast<std::size_t>(std::popcount(word & prefix_mask)));
    }
  }
  const std::size_t tail_offset = full_words * sizeof(std::uint64_t);
  if (tail_offset < mask.size()) {
    std::uint64_t tail = 0;
    for (std::size_t i = 0; i < mask.size() - tail_offset; ++i)
      tail |= static_cast<std::uint64_t>(static_cast<unsigned char>(mask[tail_offset + i])) << (i * 8U);
    nulls = detail::SaturatingAdd(nulls, static_cast<std::size_t>(std::popcount(tail)));
    if (full_words == row_word) {
      const auto bit = static_cast<unsigned>(uarch::PowerOfTwo<6>::rem(row));
      const std::uint64_t prefix_mask = (std::uint64_t{1} << bit) - 1ULL;
      preceding_nulls =
          detail::SaturatingAdd(preceding_nulls, static_cast<std::size_t>(std::popcount(tail & prefix_mask)));
    }
  }
  const auto row_byte = static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(row));
  const auto row_bit = static_cast<unsigned>(uarch::PowerOfTwo<3>::rem(row));
  if ((static_cast<unsigned char>(mask[row_byte]) & (1U << row_bit)) != 0U)
    return {static_cast<std::uint64_t>(nulls), kNullWireRow};
  return {static_cast<std::uint64_t>(nulls), static_cast<std::uint64_t>(row - preceding_nulls)};
}

} // namespace greptimedb::row_detail
