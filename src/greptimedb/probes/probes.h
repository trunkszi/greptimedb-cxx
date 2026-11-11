/** Steady-state leaf probes over constinit fixture state. */
#pragma once

#include <uarch/uarch.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace greptimedb::probes {

inline constexpr std::uint64_t kProbeRows = 512;
inline constexpr std::size_t kProbeMaskBytes = static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(kProbeRows));
inline constexpr std::size_t kProbeColumnCapacity = 16384;
inline constexpr std::size_t kProbeOwnedBytes = 64;
inline constexpr std::size_t kProbeOwnedOffset = 64;
inline constexpr std::size_t kProbeDisjointAfterOffset = 128;
inline constexpr std::size_t kProbeReverseOverlapOffset = 60;
inline constexpr std::size_t kProbeDisjointBeforeOffset = 53;
inline constexpr std::size_t kProbeOverlapArenaBytes = 192;
inline constexpr std::size_t kProbeSourceBytes = 8;
inline constexpr std::size_t kProbeWireBytes = 8;
inline constexpr std::size_t kProbeWireRows = 64;
inline constexpr std::uint64_t kProbeInvalidWire = std::numeric_limits<std::uint64_t>::max();
inline constexpr std::uint64_t kProbeNullWireRow = kProbeInvalidWire - 1ULL;
inline constexpr std::uint64_t kProbePostColumnSentinel = 0x5A5A5A5A5A5A5A5AULL;

static_assert(std::has_single_bit(kProbeRows));
static_assert(std::has_single_bit(kProbeColumnCapacity));

[[nodiscard]] inline constexpr std::uint64_t ProbeMix(std::uint64_t seed, std::uint64_t index) noexcept {
  auto mixed = seed + index * 0x9E3779B97F4A7C15ULL;
  mixed = (mixed ^ (mixed >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  mixed = (mixed ^ (mixed >> 27U)) * 0x94D049BB133111EBULL;
  return mixed ^ (mixed >> 31U);
}

/** Odd seeds keep a full 64-row word. Zero is an empty mask. Other even seeds truncate. */
[[nodiscard]] inline constexpr std::uint64_t ProbeWireLength(std::uint64_t seed) noexcept {
  if (uarch::PowerOfTwo<1>::rem(seed) != 0ULL) return kProbeWireBytes;
  if (seed == 0ULL) return 0;
  return 3;
}

/** Cycles across forward-overlap, disjoint-after, reverse-overlap, and disjoint-before spans. */
[[nodiscard, gnu::always_inline]] inline std::size_t ProbeOverlapOffset(std::uint64_t index) noexcept {
  const auto mode = uarch::PowerOfTwo<2>::rem(index);
  const auto sub = static_cast<std::size_t>(uarch::PowerOfTwo<2>::rem(uarch::PowerOfTwo<2>::div(index)));
  const auto pair_even =
      uarch::flow::predicate(mode == 0ULL).select_register(kProbeOwnedOffset, kProbeReverseOverlapOffset);
  const auto pair_odd =
      uarch::flow::predicate(mode == 1ULL).select_register(kProbeDisjointAfterOffset, kProbeDisjointBeforeOffset);
  const auto base =
      uarch::flow::predicate(uarch::PowerOfTwo<1>::rem(mode) == 0ULL).select_register(pair_even, pair_odd);
  return base + sub;
}

} // namespace greptimedb::probes

extern "C" {

void greptime_reset_probe_state(std::uint64_t seed) noexcept;

[[nodiscard]] std::uint64_t greptime_probe_null_mask_is_null() noexcept;
[[nodiscard]] std::uint64_t greptime_probe_overlaps() noexcept;
[[nodiscard]] std::uint64_t greptime_probe_column_append_i64() noexcept;
[[nodiscard]] std::uint64_t greptime_probe_wire_row_null_check() noexcept;

[[nodiscard]] std::uint64_t greptime_probe_column_cursor() noexcept;
[[nodiscard]] const std::int64_t *greptime_probe_column_data() noexcept;
[[nodiscard]] std::uint64_t greptime_probe_post_column_sentinel() noexcept;
[[nodiscard]] const std::byte *greptime_probe_owned_data() noexcept;
[[nodiscard]] const std::byte *greptime_probe_separate_data() noexcept;
[[nodiscard]] const std::byte *greptime_probe_arena_data() noexcept;
[[nodiscard]] std::uint64_t greptime_probe_wire_length() noexcept;
[[nodiscard]] const char *greptime_probe_wire_data() noexcept;
}
