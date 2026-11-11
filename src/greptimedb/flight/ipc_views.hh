#pragma once

#include "ipc_compression.hh"

#include <string_view>

namespace greptimedb::flight::detail {
struct EncodedViews {
  std::vector<uint8_t> views;
  std::vector<uint8_t> data;
};
[[nodiscard]] FlightResult<EncodedViews> EncodeViews(const ::greptime::v1::Column &column, size_t rows, bool text);
[[nodiscard]] FlightResult<std::string_view> DecodeView(std::span<const uint8_t> views, size_t row,
                                                        std::span<const DecodedBuffer> buffers);
} // namespace greptimedb::flight::detail
