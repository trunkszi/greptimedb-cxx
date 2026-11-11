#include <greptimedb/probes/gem5_entry.h>
#include <greptimedb/probes/probes.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <print>
#include <string_view>

namespace {

[[nodiscard]] std::optional<std::uint64_t> ParseCount(std::string_view text) noexcept {
  if (text.empty() || text.size() > 20) return std::nullopt;
  constexpr auto kMaxCount = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t value = 0;
  for (const char byte : text) {
    if (byte < '0' || byte > '9') return std::nullopt;
    const auto digit = static_cast<std::uint64_t>(byte - '0');
    if (value > kMaxCount / 10ULL || (value == kMaxCount / 10ULL && digit > kMaxCount % 10ULL)) return std::nullopt;
    value = value * 10ULL + digit;
  }
  return value == 0 ? std::nullopt : std::optional<std::uint64_t>{value};
}

template <auto Probe> [[nodiscard]] std::uint64_t Measure(std::uint64_t iterations) noexcept {
  std::uint64_t warmup = 0;
  for (std::uint64_t i = 0; i < 4097; ++i)
    warmup += Probe();
  greptimedb::probes::Gem5ResetStats();
  std::uint64_t sum = 0;
  for (std::uint64_t i = 0; i < iterations; ++i)
    sum += Probe();
  greptimedb::probes::Gem5DumpStats();
  return sum ^ (warmup << 1U);
}

} // namespace

int main(int argc, char **argv) {
  std::string_view probe;
  std::uint64_t iterations = 0;
  for (int i = 1; i < argc; i += 2) {
    if (i + 1 >= argc) return 2;
    const std::string_view key{argv[i]};
    const std::string_view value{argv[i + 1]};
    if (key == "--probe")
      probe = value;
    else if (key == "--iterations") {
      const auto parsed = ParseCount(value);
      if (!parsed) return 2;
      iterations = *parsed;
    } else
      return 2;
  }
  if (probe.empty() || iterations == 0) return 2;
  greptime_reset_probe_state(1);
  std::uint64_t checksum = 0;
  if (probe == "null_mask")
    checksum = Measure<greptime_probe_null_mask_is_null>(iterations);
  else if (probe == "overlaps")
    checksum = Measure<greptime_probe_overlaps>(iterations);
  else if (probe == "column_append")
    checksum = Measure<greptime_probe_column_append_i64>(iterations);
  else if (probe == "wire_row")
    checksum = Measure<greptime_probe_wire_row_null_check>(iterations);
  else
    return 2;
  std::print("probe={} iterations={:010} checksum={}\n", probe, iterations, checksum);
  return 0;
}
