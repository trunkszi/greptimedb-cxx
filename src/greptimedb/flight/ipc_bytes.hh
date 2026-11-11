#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace greptimedb::flight {

// char and uint8_t are not pointer-interconvertible, so string_view needs this one conversion.
[[nodiscard]] inline std::string_view Utf8Bytes(std::span<const uint8_t> bytes) noexcept {
  return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
}

} // namespace greptimedb::flight
