#include "ipc_views.hh"

#include "ipc_bytes.hh"

#include <algorithm>
#include <bit>
#include <cstring>
#include <uarch/uarch.hpp>

namespace greptimedb::flight::detail {
namespace {
[[nodiscard, gnu::cold, gnu::noinline]] std::unexpected<std::string> Fail(const char *why) {
  uarch::cold_path();
  return std::unexpected(std::string(why));
}
inline uint32_t LoadU32Le(const uint8_t *src) noexcept {
  uint32_t value{};
  std::memcpy(&value, src, sizeof(value));
  if constexpr (std::endian::native != std::endian::little) value = std::byteswap(value);
  return value;
}
inline void StoreU32Le(uint8_t *dst, uint32_t value) noexcept {
  if constexpr (std::endian::native != std::endian::little) value = std::byteswap(value);
  std::memcpy(dst, &value, sizeof(value));
}
int32_t Read(std::span<const uint8_t> bytes, size_t at) { return std::bit_cast<int32_t>(LoadU32Le(bytes.data() + at)); }
void Put(std::vector<uint8_t> &bytes, size_t at, size_t value) {
  StoreU32Le(bytes.data() + at, static_cast<uint32_t>(value));
}
} // namespace

FlightResult<EncodedViews> EncodeViews(const ::greptime::v1::Column &column, size_t rows, bool text) {
  if (uarch::unlikely(rows > kMaxBody / 16)) return Fail("view buffer exceeds size policy");
  EncodedViews output{std::vector<uint8_t>(rows * 16), {}};
  size_t packed = 0;
  for (size_t row = 0; row < rows; ++row) {
    if (row / 8 < column.null_mask().size() && (static_cast<uint8_t>(column.null_mask()[row / 8]) & (1U << (row % 8))))
      continue;
    const auto &value = text ? column.values().string_values(static_cast<int>(packed++))
                             : column.values().binary_values(static_cast<int>(packed++));
    if (uarch::unlikely(value.size() > kMaxBody - output.data.size())) return Fail("view data exceeds size policy");
    const auto at = row * 16;
    Put(output.views, at, value.size());
    const size_t inline_bytes = value.size() <= 12 ? value.size() : 4;
    std::copy_n(value.begin(), inline_bytes, output.views.begin() + at + 4);
    if (value.size() > 12) {
      Put(output.views, at + 12, output.data.size());
      output.data.insert(output.data.end(), value.begin(), value.end());
    }
  }
  return output;
}

FlightResult<std::string_view> DecodeView(std::span<const uint8_t> views, size_t row,
                                          std::span<const DecodedBuffer> buffers) {
  if (uarch::unlikely(row >= views.size() / 16)) return Fail("short view buffer");
  const size_t at = row * 16;
  const auto length = Read(views, at);
  if (uarch::unlikely(length < 0)) return Fail("negative view length");
  const auto size = static_cast<size_t>(length);
  if (size <= 12) return Utf8Bytes(views.subspan(at + 4, size));
  const auto index = Read(views, at + 8), offset = Read(views, at + 12);
  if (uarch::unlikely(index < 0 || offset < 0 || static_cast<size_t>(index) >= buffers.size()))
    return Fail("invalid view buffer index or offset");
  const auto bytes = buffers[static_cast<size_t>(index)].bytes;
  const auto start = static_cast<size_t>(offset);
  if (uarch::unlikely(start > bytes.size() || size > bytes.size() - start))
    return Fail("view payload exceeds data buffer");
  if (uarch::unlikely(LoadU32Le(views.data() + at + 4) != LoadU32Le(bytes.data() + start)))
    return Fail("view prefix mismatch");
  return Utf8Bytes(bytes.subspan(start, size));
}
} // namespace greptimedb::flight::detail
