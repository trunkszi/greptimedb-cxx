#include "greptimedb/core/hints.h"

#include <algorithm>
#include <ranges>
#include <utility>

namespace greptimedb {
namespace {

bool ValidComponent(std::string_view value) noexcept {
  if (!value.empty() && (value.front() == ' ' || value.back() == ' '))
    return false;
  return std::ranges::all_of(value, [](unsigned char ch) {
    return ch >= 0x20 && ch <= 0x7e && ch != ',';
  });
}

} // namespace

std::string_view MergeModeName(MergeMode mode) noexcept {
  switch (mode) {
  case MergeMode::kLastRow:
    return "last_row";
  case MergeMode::kLastNonNull:
    return "last_non_null";
  }
  return {};
}

Result<void> WriteHints::SetHint(std::string key, std::string value) {
  if (key.empty() || key.contains('=') || !ValidComponent(key) ||
      !ValidComponent(value)) {
    return std::unexpected(
        Error::InvalidConfig("hint keys must be nonempty printable ASCII "
                             "without commas or equals signs; "
                             "values must be printable ASCII without commas; "
                             "surrounding whitespace is forbidden"));
  }
  Insert(std::move(key), std::move(value));
  return {};
}

Result<void> WriteHints::SetAutoCreateTable(bool enabled) {
  return SetHint("auto_create_table", enabled ? "true" : "false");
}

Result<void> WriteHints::SetTtl(std::string ttl) {
  return SetHint("ttl", std::move(ttl));
}

Result<void> WriteHints::SetAppendMode(bool enabled) {
  return SetHint("append_mode", enabled ? "true" : "false");
}

Result<void> WriteHints::SetMergeMode(MergeMode mode) {
  const auto name = MergeModeName(mode);
  if (name.empty())
    return std::unexpected(Error::InvalidConfig("invalid merge mode"));
  return SetHint("merge_mode", std::string(name));
}

Result<void> WriteHints::SetPhysicalTable(std::string table) {
  return SetHint("physical_table", std::move(table));
}

std::string WriteHints::FormatHeaderValue() const {
  std::string encoded;
  for (const auto &[key, value] : entries_) {
    if (!encoded.empty())
      encoded.push_back(',');
    encoded.append(key).push_back('=');
    encoded.append(value);
  }
  return encoded;
}

WriteHints WriteHints::MergedWith(const WriteHints &extra) const {
  WriteHints merged = *this;
  for (const auto &[key, value] : extra.entries_)
    merged.Insert(key, value);
  return merged;
}

void WriteHints::Insert(std::string key, std::string value) {
  if (auto entry = std::ranges::find(entries_, key, &Entry::first);
      entry != entries_.end()) {
    entry->second = std::move(value);
  } else {
    entries_.emplace_back(std::move(key), std::move(value));
  }
}

} // namespace greptimedb
