#pragma once

#include <greptimedb/flight/ipc_codec.h>

#include <memory>

namespace greptimedb::flight::detail {
inline constexpr size_t kMaxBody = 256U * 1024U * 1024U;

// Raw buffers borrow the message body; compressed buffers own an uninitialized
// std::unique_ptr<std::uint8_t[]> buffer bounded to kMaxBody (256 MiB) prior to allocation.
struct DecodedBuffer {
  std::unique_ptr<std::uint8_t[]> storage;
  std::span<const std::uint8_t> bytes;
  std::uint8_t operator[](size_t i) const noexcept { return bytes[i]; }
  operator std::span<const std::uint8_t>() const noexcept { return bytes; }
};

[[nodiscard]] FlightResult<std::vector<std::uint8_t>> Compress(std::span<const std::uint8_t> input,
                                                               FlightCompression codec);
[[nodiscard]] FlightResult<DecodedBuffer> Decompress(std::span<const std::uint8_t> input, ipc::Compression codec,
                                                     size_t &budget, size_t limit);
} // namespace greptimedb::flight::detail
