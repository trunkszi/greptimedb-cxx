#pragma once

#include <greptimedb/flight/ipc_flatbuffers.h>

#include <bit>
#include <cstring>
#include <span>
#include <string_view>
#include <type_traits>

namespace greptimedb::flight::ipc {

inline constexpr size_t kMaxHeaderBytes = 64 * 1024 * 1024;
inline constexpr size_t kMaxFields = 65536;

template <class T> inline T Load(std::span<const uint8_t> data, size_t offset) {
  using U = std::make_unsigned_t<T>;
  U bits{};
  std::memcpy(&bits, data.data() + offset, sizeof(bits));
  if constexpr (sizeof(U) > 1 && std::endian::native != std::endian::little) bits = std::byteswap(bits);
  return std::bit_cast<T>(bits);
}

template <class ByteRange> [[nodiscard]] inline bool ValidUtf8Range(const ByteRange &value) noexcept {
  size_t i = 0;
  while (i < value.size()) {
    const auto c = static_cast<uint8_t>(value[i++]);
    if (c < 0x80) continue;
    uint32_t cp = 0;
    size_t n = 0;
    uint32_t minimum = 0;
    if (c >= 0xc2 && c <= 0xdf) {
      cp = c & 31;
      n = 1;
      minimum = 0x80;
    } else if (c >= 0xe0 && c <= 0xef) {
      cp = c & 15;
      n = 2;
      minimum = 0x800;
    } else if (c >= 0xf0 && c <= 0xf4) {
      cp = c & 7;
      n = 3;
      minimum = 0x10000;
    } else
      return false;
    if (n > value.size() - i) return false;
    for (size_t j = 0; j < n; ++j) {
      const auto b = static_cast<uint8_t>(value[i++]);
      if ((b & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (b & 63);
    }
    if (cp < minimum || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return false;
  }
  return true;
}

[[nodiscard]] inline bool ValidUtf8(std::span<const uint8_t> value) noexcept { return ValidUtf8Range(value); }
[[nodiscard]] inline bool ValidUtf8(std::string_view value) noexcept { return ValidUtf8Range(value); }

inline bool ValidType(const Type &type) {
  switch (type.id) {
  case TypeId::kNull:
  case TypeId::kBinary:
  case TypeId::kUtf8:
  case TypeId::kBool:
  case TypeId::kLargeBinary:
  case TypeId::kBinaryView:
  case TypeId::kUtf8View:
  case TypeId::kLargeUtf8: return true;
  case TypeId::kInt: return type.bit_width == 8 || type.bit_width == 16 || type.bit_width == 32 || type.bit_width == 64;
  case TypeId::kFloatingPoint: return type.precision == 1 || type.precision == 2;
  case TypeId::kDecimal:
    return type.bit_width == 128 && type.decimal_precision >= 1 && type.decimal_precision <= 38 && type.scale >= -128 &&
           type.scale <= type.decimal_precision;
  case TypeId::kDate: return type.unit == 0 || type.unit == 1;
  case TypeId::kTimestamp: return type.unit >= 0 && type.unit <= 3 && ValidUtf8(type.timezone);
  }
  return false;
}
} // namespace greptimedb::flight::ipc
