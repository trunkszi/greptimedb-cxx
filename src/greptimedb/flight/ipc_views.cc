#include "ipc_views.h"

#include <algorithm>
#include <bit>

namespace greptimedb::flight::detail {
namespace {
int32_t Read(std::span<const uint8_t> bytes, size_t at) {
  uint32_t bits = 0;
  for (size_t i = 0; i < 4; ++i)
    bits |= uint32_t{bytes[at + i]} << (8 * i);
  return std::bit_cast<int32_t>(bits);
}
void Put(std::vector<uint8_t> &bytes, size_t at, size_t value) {
  for (size_t i = 0; i < 4; ++i)
    bytes[at + i] = static_cast<uint8_t>(value >> (8 * i));
}
} // namespace

FlightResult<EncodedViews> EncodeViews(const ::greptime::v1::Column &column,
                                       size_t rows, bool text) {
  if (rows > kMaxBody / 16)
    return std::unexpected("view buffer exceeds size policy");
  EncodedViews output{std::vector<uint8_t>(rows * 16), {}};
  size_t packed = 0;
  for (size_t row = 0; row < rows; ++row) {
    if (row / 8 < column.null_mask().size() &&
        (static_cast<uint8_t>(column.null_mask()[row / 8]) & (1U << (row % 8))))
      continue;
    const auto &value =
        text ? column.values().string_values(static_cast<int>(packed++))
             : column.values().binary_values(static_cast<int>(packed++));
    if (value.size() > kMaxBody - output.data.size())
      return std::unexpected("view data exceeds size policy");
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

FlightResult<std::string_view>
DecodeView(std::span<const uint8_t> views, size_t row,
           std::span<const DecodedBuffer> buffers) {
  if (row >= views.size() / 16)
    return std::unexpected("short view buffer");
  const size_t at = row * 16;
  const auto length = Read(views, at);
  if (length < 0)
    return std::unexpected("negative view length");
  const auto size = static_cast<size_t>(length);
  if (size <= 12)
    return std::string_view(
        reinterpret_cast<const char *>(views.data() + at + 4), size);
  const auto index = Read(views, at + 8), offset = Read(views, at + 12);
  if (index < 0 || offset < 0 || static_cast<size_t>(index) >= buffers.size())
    return std::unexpected("invalid view buffer index or offset");
  const auto bytes = buffers[static_cast<size_t>(index)].bytes;
  const auto start = static_cast<size_t>(offset);
  if (start > bytes.size() || size > bytes.size() - start)
    return std::unexpected("view payload exceeds data buffer");
  if (!std::equal(views.begin() + at + 4, views.begin() + at + 8,
                  bytes.begin() + start))
    return std::unexpected("view prefix mismatch");
  return std::string_view(reinterpret_cast<const char *>(bytes.data() + start),
                          size);
}
} // namespace greptimedb::flight::detail
