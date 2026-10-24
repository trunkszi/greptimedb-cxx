#pragma once

#include <greptimedb/flight/ipc_codec.h>

#include <memory>

namespace greptimedb::flight::detail {
inline constexpr size_t kMaxBody = 256U * 1024U * 1024U;

// Raw buffers borrow the message body; compressed buffers own a fallible
// allocation.
struct DecodedBuffer {
  std::unique_ptr<uint8_t[]> storage;
  std::span<const uint8_t> bytes;
  size_t size() const noexcept { return bytes.size(); }
  bool empty() const noexcept { return bytes.empty(); }
  uint8_t operator[](size_t i) const noexcept { return bytes[i]; }
  operator std::span<const uint8_t>() const noexcept { return bytes; }
};

FlightResult<std::vector<uint8_t>> Compress(std::span<const uint8_t> input,
                                            FlightCompression codec);
FlightResult<DecodedBuffer> Decompress(std::span<const uint8_t> input,
                                       ipc::Compression codec, size_t &budget,
                                       size_t limit);
} // namespace greptimedb::flight::detail
