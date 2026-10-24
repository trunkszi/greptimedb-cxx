/** Catalog, schema, and timezone selected for one client request. */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace greptimedb::client {

struct QueryScope {
  std::string_view catalog{};
  std::string_view schema{};
  std::string_view timezone{};
};

namespace detail {
[[nodiscard]] inline bool IsStrictUtf8(std::string_view bytes) noexcept {
  std::size_t pos = 0;
  while (pos < bytes.size()) {
    const auto first = static_cast<unsigned char>(bytes[pos++]);
    if (first < 0x80)
      continue;
    std::uint32_t code = 0;
    int count = 0;
    std::uint32_t minimum = 0;
    if (first >= 0xc2 && first <= 0xdf) {
      code = first & 31U;
      count = 1;
      minimum = 0x80;
    } else if (first >= 0xe0 && first <= 0xef) {
      code = first & 15U;
      count = 2;
      minimum = 0x800;
    } else if (first >= 0xf0 && first <= 0xf4) {
      code = first & 7U;
      count = 3;
      minimum = 0x10000;
    } else {
      return false;
    }
    for (int i = 0; i < count; ++i) {
      if (pos == bytes.size())
        return false;
      const auto byte = static_cast<unsigned char>(bytes[pos++]);
      if ((byte & 0xc0U) != 0x80U)
        return false;
      code = (code << 6U) | (byte & 63U);
    }
    // Overlong sequences, UTF-16 surrogates, and values above U+10FFFF.
    if (code < minimum || code > 0x10ffff ||
        (code >= 0xd800 && code <= 0xdfff))
      return false;
  }
  return true;
}

/** Text identifiers allow Unicode but reject NUL and all-blank input. */
[[nodiscard]] inline bool HasText(std::string_view text) noexcept {
  if (!IsStrictUtf8(text)) return false;
  bool nonblank = false;
  for (unsigned char ch : text) {
    if (ch == 0) return false;
    if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' && ch != '\f' && ch != '\v')
      nonblank = true;
  }
  return nonblank;
}
[[nodiscard]] inline bool ValidScope(QueryScope scope) noexcept {
  return HasText(scope.schema) && (scope.catalog.empty() || HasText(scope.catalog)) &&
         (scope.timezone.empty() || HasText(scope.timezone));
}
} // namespace detail

} // namespace greptimedb::client
