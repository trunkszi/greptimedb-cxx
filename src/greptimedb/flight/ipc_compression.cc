#include "ipc_compression.hh"

#include <lz4frame.h>
#include <zstd.h>

#include <bit>
#include <cstring>
#include <memory>
#include <uarch/uarch.hpp>

namespace greptimedb::flight::detail {
namespace {
[[nodiscard, gnu::cold, gnu::noinline]] std::unexpected<std::string> Fail(const char *why) {
  uarch::cold_path();
  return std::unexpected(std::string(why));
}
inline void StoreU64Le(uint8_t *dst, uint64_t value) noexcept {
  if constexpr (std::endian::native != std::endian::little) value = std::byteswap(value);
  std::memcpy(dst, &value, sizeof(value));
}
inline uint64_t LoadU64Le(const uint8_t *src) noexcept {
  uint64_t value{};
  std::memcpy(&value, src, sizeof(value));
  if constexpr (std::endian::native != std::endian::little) value = std::byteswap(value);
  return value;
}
} // namespace

FlightResult<std::vector<uint8_t>> Compress(std::span<const uint8_t> input, FlightCompression codec) {
  if (input.empty()) return std::vector<uint8_t>{};
  if (codec == FlightCompression::kNone) return std::vector<uint8_t>(input.begin(), input.end());
  const size_t bound = codec == FlightCompression::kLz4Frame ? LZ4F_compressFrameBound(input.size(), nullptr)
                                                             : ZSTD_compressBound(input.size());
  if (uarch::unlikely(bound > kMaxBody - 8)) return Fail("compressed buffer exceeds size policy");
  std::vector<uint8_t> out(bound + 8);
  StoreU64Le(out.data(), input.size());
  const size_t written = codec == FlightCompression::kLz4Frame
                             ? LZ4F_compressFrame(out.data() + 8, bound, input.data(), input.size(), nullptr)
                             : ZSTD_compress(out.data() + 8, bound, input.data(), input.size(), 1);
  if ((codec == FlightCompression::kLz4Frame && LZ4F_isError(written)) ||
      (codec == FlightCompression::kZstd && ZSTD_isError(written)))
    return Fail("buffer compression failed");
  out.resize(written + 8);
  return out;
}

FlightResult<DecodedBuffer> Decompress(std::span<const uint8_t> input, ipc::Compression codec, size_t &budget,
                                       size_t limit) {
  const auto raw = [&](std::span<const uint8_t> bytes) -> FlightResult<DecodedBuffer> {
    if (bytes.size() > budget || bytes.size() > limit)
      return Fail("decoded buffer exceeds size policy or logical layout");
    budget -= bytes.size();
    return DecodedBuffer{{}, bytes};
  };
  if (codec == ipc::Compression::kNone || input.empty()) return raw(input);
  if (uarch::unlikely(input.size() < 8)) return Fail("compressed buffer missing size prefix");
  const auto length = std::bit_cast<int64_t>(LoadU64Le(input.data()));
  input = input.subspan(8);
  if (length == -1) return raw(input);
  if (length < 0 || static_cast<uint64_t>(length) > budget || static_cast<uint64_t>(length) > limit)
    return Fail("invalid or excessive decompressed size");
  const auto size = static_cast<size_t>(length);
  if (codec == ipc::Compression::kZstd) {
    const auto frame = ZSTD_findFrameCompressedSize(input.data(), input.size());
    if (ZSTD_isError(frame) || frame != input.size()) return Fail("invalid or trailing ZSTD frame");
    const auto declared = ZSTD_getFrameContentSize(input.data(), input.size());
    if (declared == ZSTD_CONTENTSIZE_ERROR || (declared != ZSTD_CONTENTSIZE_UNKNOWN && declared != size))
      return Fail("invalid ZSTD decoded size");
  }
  // LZ4 and Zstd reject a null destination even when the decoded length is zero.
  const size_t capacity = size == 0 ? 1uz : size;
  DecodedBuffer output{std::make_unique_for_overwrite<std::uint8_t[]>(capacity), {}};
  output.bytes = {output.storage.get(), size};
  if (codec == ipc::Compression::kZstd) {
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context(ZSTD_createDCtx(), ZSTD_freeDCtx);
    if (!context) return Fail("ZSTD context allocation failed");
    const auto actual = ZSTD_decompressDCtx(context.get(), output.storage.get(), size, input.data(), input.size());
    if (ZSTD_isError(actual) || actual != size) return Fail("invalid ZSTD decoded size");
  } else {
    LZ4F_dctx *context = nullptr;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&context, LZ4F_VERSION)))
      return Fail("LZ4 context allocation failed");
    size_t consumed = 0, produced = 0, hint = 1;
    bool failed = false;
    do {
      size_t src = input.size() - consumed, dst = size - produced;
      hint = LZ4F_decompress(context, output.storage.get() + produced, &dst, input.data() + consumed, &src, nullptr);
      if (LZ4F_isError(hint) || (!src && !dst && hint)) {
        failed = true;
        break;
      }
      consumed += src;
      produced += dst;
    } while (hint && consumed < input.size());
    LZ4F_freeDecompressionContext(context);
    if (failed || hint || consumed != input.size() || produced != size) return Fail("invalid or trailing LZ4 frame");
  }
  budget -= size;
  return output;
}
} // namespace greptimedb::flight::detail
