#include <greptimedb/batch/leaf_primitives.h>
#include <greptimedb/probes/probes.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace greptimedb::probes {
namespace {

struct ProbeState {
  static constexpr std::uint64_t wire_rows = kProbeWireRows;

  std::uint64_t seed = 1;
  std::uint64_t null_cursor = 0;
  std::uint64_t overlap_cursor = 0;
  std::uint64_t wire_cursor = 0;
  std::uint64_t append_cursor = 0;
  std::size_t append_size = 0;
  std::uint64_t sink = 0;
  std::uint64_t wire_len = kProbeWireBytes;
  std::array<std::int64_t, kProbeColumnCapacity> column{};
  std::uint64_t post_column_sentinel = kProbePostColumnSentinel;
  std::array<std::uint8_t, kProbeMaskBytes> mask{};
  std::array<std::byte, kProbeOverlapArenaBytes> overlap_arena{};
  std::array<char, kProbeWireBytes> wire{};
};

static_assert(uarch::NoPadding<ProbeState>);
static_assert(offsetof(ProbeState, post_column_sentinel) == offsetof(ProbeState, column) + sizeof(ProbeState::column));
static_assert((1UZ << 9) == kProbeRows);
static_assert((1UZ << 14) == kProbeColumnCapacity);
static_assert((1UZ << 6) == kProbeWireRows);
static_assert(uarch::PowerOfTwo<3>::div(kProbeRows) == kProbeMaskBytes);

// constinit keeps the fixture out of dynamic initialization before main.
constinit ProbeState g_state{};

} // namespace
} // namespace greptimedb::probes

extern "C" void greptime_reset_probe_state(std::uint64_t seed) noexcept {
  using namespace greptimedb::probes;
  g_state.seed = seed;
  g_state.null_cursor = 0;
  g_state.overlap_cursor = 0;
  g_state.wire_cursor = 0;
  g_state.append_cursor = 0;
  g_state.append_size = 0;
  g_state.sink = 0;
  g_state.wire_len = ProbeWireLength(seed);
  g_state.column.fill(0);
  g_state.post_column_sentinel = kProbePostColumnSentinel;
  for (std::size_t i = 0; i < g_state.mask.size(); ++i)
    g_state.mask[i] = static_cast<std::uint8_t>(ProbeMix(seed, i));
  for (std::size_t i = 0; i < g_state.wire.size(); ++i)
    g_state.wire[i] = static_cast<char>(static_cast<std::uint8_t>(ProbeMix(seed, 1000ULL + i)));
  for (std::size_t i = 0; i < g_state.overlap_arena.size(); ++i)
    g_state.overlap_arena[i] = std::byte{static_cast<std::uint8_t>(ProbeMix(seed, 2000ULL + i))};
}

// The wrapped index models the pre-reserved steady state without dynamic growth inside the leaf.
extern "C" std::uint64_t greptime_probe_null_mask_is_null() noexcept {
  using namespace greptimedb::probes;
  const auto row = static_cast<std::size_t>(uarch::PowerOfTwo<9>::rem(g_state.null_cursor));
  g_state.null_cursor = g_state.null_cursor + 1ULL;
  const auto bit = static_cast<std::uint64_t>(
      greptimedb::detail::NullMaskTestBit(g_state.mask.data(), static_cast<std::size_t>(kProbeRows), row));
  g_state.sink += bit;
  return bit;
}

extern "C" std::uint64_t greptime_probe_overlaps() noexcept {
  using namespace greptimedb::probes;
  const auto index = g_state.overlap_cursor;
  g_state.overlap_cursor = index + 1ULL;
  const auto offset = ProbeOverlapOffset(index);
  const std::span<const std::byte> source{g_state.overlap_arena.data() + offset, kProbeSourceBytes};
  const auto *owned = g_state.overlap_arena.data() + kProbeOwnedOffset;
  const auto hit = static_cast<std::uint64_t>(greptimedb::detail::Overlaps(source, owned, kProbeOwnedBytes));
  g_state.sink += hit;
  return hit;
}

extern "C" std::uint64_t greptime_probe_column_append_i64() noexcept {
  using namespace greptimedb::probes;
  g_state.append_size = static_cast<std::size_t>(uarch::PowerOfTwo<14>::rem(g_state.append_size));
  const auto seq = ++g_state.append_cursor;
  greptimedb::detail::AppendUnchecked(g_state.column.data(), g_state.append_size, static_cast<std::int64_t>(seq));
  g_state.sink += seq;
  return seq;
}

extern "C" std::uint64_t greptime_probe_wire_row_null_check() noexcept {
  using namespace greptimedb::probes;
  const auto row = static_cast<std::size_t>(uarch::PowerOfTwo<6>::rem(g_state.wire_cursor));
  g_state.wire_cursor = g_state.wire_cursor + 1ULL;
  const auto scan =
      greptimedb::row_detail::ScanWireNullMask({g_state.wire.data(), static_cast<std::size_t>(g_state.wire_len)}, row,
                                               static_cast<std::size_t>(g_state.wire_rows));
  const auto value = scan.value_index;
  g_state.sink += value;
  return value;
}

extern "C" std::uint64_t greptime_probe_column_cursor() noexcept { return greptimedb::probes::g_state.append_cursor; }

extern "C" const std::int64_t *greptime_probe_column_data() noexcept {
  return greptimedb::probes::g_state.column.data();
}

extern "C" std::uint64_t greptime_probe_post_column_sentinel() noexcept {
  return greptimedb::probes::g_state.post_column_sentinel;
}

extern "C" const std::byte *greptime_probe_owned_data() noexcept {
  return greptimedb::probes::g_state.overlap_arena.data() + greptimedb::probes::kProbeOwnedOffset;
}

extern "C" const std::byte *greptime_probe_separate_data() noexcept {
  return greptimedb::probes::g_state.overlap_arena.data() + greptimedb::probes::kProbeDisjointAfterOffset;
}

extern "C" const std::byte *greptime_probe_arena_data() noexcept {
  return greptimedb::probes::g_state.overlap_arena.data();
}

extern "C" std::uint64_t greptime_probe_wire_length() noexcept { return greptimedb::probes::g_state.wire_len; }

extern "C" const char *greptime_probe_wire_data() noexcept { return greptimedb::probes::g_state.wire.data(); }
