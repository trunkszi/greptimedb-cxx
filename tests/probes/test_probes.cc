#include <greptimedb/batch/column_buf.h>
#include <greptimedb/probes/probes.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>

namespace {

using greptimedb::probes::kProbeColumnCapacity;
using greptimedb::probes::kProbeDisjointAfterOffset;
using greptimedb::probes::kProbeDisjointBeforeOffset;
using greptimedb::probes::kProbeInvalidWire;
using greptimedb::probes::kProbeNullWireRow;
using greptimedb::probes::kProbeOwnedBytes;
using greptimedb::probes::kProbeOwnedOffset;
using greptimedb::probes::kProbePostColumnSentinel;
using greptimedb::probes::kProbeReverseOverlapOffset;
using greptimedb::probes::kProbeRows;
using greptimedb::probes::kProbeSourceBytes;
using greptimedb::probes::ProbeMix;
using greptimedb::probes::ProbeOverlapOffset;

[[nodiscard]] std::uint64_t WireExpect(std::span<const char> mask, std::uint64_t row, std::size_t rows) noexcept {
  const auto scan = greptimedb::row_detail::ScanWireNullMask(mask, static_cast<std::size_t>(row), rows);
  if (scan.nulls == greptimedb::row_detail::kInvalidWireMask) return kProbeInvalidWire;
  return scan.value_index;
}

} // namespace

TEST(HotProbes, NullMaskMatchesIsNull) {
  greptime_reset_probe_state(1);
  greptimedb::NullMask mask;
  for (std::size_t row = 0; row < kProbeRows; ++row) {
    const auto byte = static_cast<std::uint8_t>(ProbeMix(1, uarch::PowerOfTwo<3>::div(row)));
    const auto bit = ((byte >> static_cast<unsigned>(uarch::PowerOfTwo<3>::rem(row))) & 1U) != 0U;
    ASSERT_TRUE(mask.Push(bit).has_value());
  }
  greptime_reset_probe_state(1);
  std::uint64_t ones = 0;
  for (std::size_t row = 0; row < kProbeRows; ++row) {
    const auto got = greptime_probe_null_mask_is_null();
    EXPECT_EQ(got, mask.IsNull(row) ? 1ULL : 0ULL);
    ones += got;
  }
  EXPECT_GT(ones, 0ULL);
  EXPECT_LT(ones, kProbeRows);
  EXPECT_FALSE(mask.IsNull(kProbeRows));
}

TEST(HotProbes, OverlapsCoversAllFourQuadrantsAndBoundaries) {
  greptime_reset_probe_state(1);
  const auto *arena = greptime_probe_arena_data();
  const auto *owned = greptime_probe_owned_data();
  ASSERT_EQ(owned, arena + kProbeOwnedOffset);

  // Direct boundary checks on the shared leaf primitive:
  // 1) Forward overlap inside [owned, owned + capacity)
  EXPECT_TRUE(greptimedb::detail::Overlaps({arena + kProbeOwnedOffset, kProbeSourceBytes}, owned, kProbeOwnedBytes));
  // 2) Exact disjoint-after boundary: input - owned == capacity
  EXPECT_FALSE(
      greptimedb::detail::Overlaps({arena + kProbeDisjointAfterOffset, kProbeSourceBytes}, owned, kProbeOwnedBytes));
  // 3) Reverse overlap: input < owned && owned - input < source.size()
  EXPECT_TRUE(
      greptimedb::detail::Overlaps({arena + kProbeReverseOverlapOffset, kProbeSourceBytes}, owned, kProbeOwnedBytes));
  // 4) Exact disjoint-before boundary: owned - input == source.size() (56 + 8 == 64)
  EXPECT_FALSE(greptimedb::detail::Overlaps({arena + kProbeDisjointBeforeOffset + 3uz, kProbeSourceBytes}, owned,
                                            kProbeOwnedBytes));
  // 5) Empty source or zero capacity never overlaps
  EXPECT_FALSE(greptimedb::detail::Overlaps({}, owned, kProbeOwnedBytes));
  EXPECT_FALSE(greptimedb::detail::Overlaps({owned, kProbeSourceBytes}, owned, 0uz));

  std::uint64_t hits = 0;
  for (std::uint64_t index = 0; index < 32; ++index) {
    const auto offset = ProbeOverlapOffset(index);
    const std::span<const std::byte> source{arena + offset, kProbeSourceBytes};
    const auto expect = greptimedb::detail::Overlaps(source, owned, kProbeOwnedBytes);
    const auto got = greptime_probe_overlaps();
    EXPECT_EQ(got, expect ? 1ULL : 0ULL) << "index=" << index << " offset=" << offset;
    const auto mode = uarch::PowerOfTwo<2>::rem(index);
    if (mode == 0ULL || mode == 2ULL) {
      EXPECT_EQ(got, 1ULL);
    } else {
      EXPECT_EQ(got, 0ULL);
    }
    hits += got;
  }
  EXPECT_EQ(hits, 16ULL);
}

TEST(HotProbes, AppendMatchesReservedColumn) {
  greptime_reset_probe_state(7);
  greptimedb::detail::ColumnArray<std::int64_t> column;
  ASSERT_TRUE(column.Reserve(64));
  for (std::uint64_t slot = 0; slot < 64; ++slot) {
    const auto expected_seq = static_cast<std::int64_t>(slot + 1ULL);
    column.Append(expected_seq);
    EXPECT_EQ(greptime_probe_column_append_i64(), static_cast<std::uint64_t>(expected_seq));
  }
  EXPECT_EQ(greptime_probe_column_cursor(), 64ULL);
  for (std::uint64_t slot = 0; slot < 64; ++slot)
    EXPECT_EQ(greptime_probe_column_data()[slot], column[static_cast<std::size_t>(slot)]);
  EXPECT_EQ(greptime_probe_post_column_sentinel(), kProbePostColumnSentinel);
}

TEST(HotProbes, AppendRingStaysInsideCapacity) {
  greptime_reset_probe_state(7);
  ASSERT_EQ(greptime_probe_post_column_sentinel(), kProbePostColumnSentinel);
  for (std::uint64_t n = 0; n < kProbeColumnCapacity + 3; ++n)
    static_cast<void>(greptime_probe_column_append_i64());
  EXPECT_EQ(greptime_probe_column_cursor(), kProbeColumnCapacity + 3);
  // Slots 0..2 were overwritten on the second lap with sequence numbers (Capacity + 1..3),
  // whereas slot 3 still retains its first-lap sequence number (4), and the adjacent
  // post-column sentinel immediately past slot (Capacity - 1) remains untouched.
  EXPECT_EQ(greptime_probe_column_data()[0], static_cast<std::int64_t>(kProbeColumnCapacity + 1));
  EXPECT_EQ(greptime_probe_column_data()[1], static_cast<std::int64_t>(kProbeColumnCapacity + 2));
  EXPECT_EQ(greptime_probe_column_data()[2], static_cast<std::int64_t>(kProbeColumnCapacity + 3));
  EXPECT_EQ(greptime_probe_column_data()[3], std::int64_t{4});
  EXPECT_EQ(greptime_probe_column_data()[kProbeColumnCapacity - 1], static_cast<std::int64_t>(kProbeColumnCapacity));
  EXPECT_EQ(greptime_probe_post_column_sentinel(), kProbePostColumnSentinel);
}

TEST(HotProbes, WireRowNullCheckAcrossMultipleSeeds) {
  static_assert(greptimedb::row_detail::kInvalidWireMask == kProbeInvalidWire);
  static_assert(greptimedb::row_detail::kNullWireRow == kProbeNullWireRow);

  // Seed 0: empty wire mask (0 bytes -> all rows non-null, value_index == row)
  greptime_reset_probe_state(0);
  ASSERT_EQ(greptime_probe_wire_length(), 0ULL);
  for (std::uint64_t row = 0; row < 64; ++row) {
    const auto got = greptime_probe_wire_row_null_check();
    EXPECT_EQ(got, row);
    EXPECT_EQ(got, WireExpect({}, row, 64));
  }

  // Seed 1 & Seed 3: odd seeds produce full 8-byte (64-row) masks with distinct bit patterns
  std::uint64_t seed1_sum = 0;
  greptime_reset_probe_state(1);
  ASSERT_EQ(greptime_probe_wire_length(), 8ULL);
  std::array<char, 8> bytes1{};
  for (std::size_t i = 0; i < bytes1.size(); ++i)
    bytes1[i] = greptime_probe_wire_data()[i];
  for (std::uint64_t row = 0; row < 64; ++row) {
    const auto got = greptime_probe_wire_row_null_check();
    EXPECT_EQ(got, WireExpect(bytes1, row, 64));
    seed1_sum += got;
  }

  std::uint64_t seed3_sum = 0;
  greptime_reset_probe_state(3);
  ASSERT_EQ(greptime_probe_wire_length(), 8ULL);
  std::array<char, 8> bytes3{};
  for (std::size_t i = 0; i < bytes3.size(); ++i)
    bytes3[i] = greptime_probe_wire_data()[i];
  for (std::uint64_t row = 0; row < 64; ++row) {
    const auto got = greptime_probe_wire_row_null_check();
    EXPECT_EQ(got, WireExpect(bytes3, row, 64));
    seed3_sum += got;
  }
  EXPECT_NE(bytes1, bytes3);
  EXPECT_NE(seed1_sum, 0ULL);
  EXPECT_NE(seed3_sum, 0ULL);

  // Seed 2: non-zero even seed produces truncated 3-byte mask -> rejected as kProbeInvalidWire
  greptime_reset_probe_state(2);
  ASSERT_EQ(greptime_probe_wire_length(), 3ULL);
  std::array<char, 3> bytes2{};
  for (std::size_t i = 0; i < bytes2.size(); ++i)
    bytes2[i] = greptime_probe_wire_data()[i];
  for (std::uint64_t row = 0; row < 8; ++row) {
    const auto got = greptime_probe_wire_row_null_check();
    EXPECT_EQ(got, WireExpect(bytes2, row, 64));
    EXPECT_EQ(got, kProbeInvalidWire);
  }
}

TEST(HotProbes, ResetRepeatsNonZeroChecksum) {
  const auto once = [] {
    greptime_reset_probe_state(1);
    std::uint64_t sum = 0;
    for (int i = 0; i < 16; ++i)
      sum += greptime_probe_null_mask_is_null();
    for (int i = 0; i < 16; ++i)
      sum += greptime_probe_overlaps();
    for (int i = 0; i < 16; ++i)
      sum += greptime_probe_column_append_i64();
    for (int i = 0; i < 16; ++i)
      sum += greptime_probe_wire_row_null_check();
    return sum;
  };
  const auto first = once();
  const auto second = once();
  EXPECT_NE(first, 0ULL);
  EXPECT_EQ(first, second);
}
