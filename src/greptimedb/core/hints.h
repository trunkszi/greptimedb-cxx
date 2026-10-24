#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "greptimedb/core/error.h"

namespace greptimedb {

inline constexpr std::string_view kGreptimeDbHeader = "x-greptime-db-name";
inline constexpr std::string_view kGreptimeAuthHeader = "x-greptime-auth";
inline constexpr std::string_view kGreptimeHintsHeader = "x-greptime-hints";
inline constexpr std::string_view kGreptimeRetryHintHeader =
    "x-greptime-err-retry-hint";
inline constexpr std::string_view kGreptimeInnerErrorHeader = "INNER_ERROR_MSG";
inline constexpr std::string_view kGreptimeErrorCodeHeader =
    "x-greptime-err-code";
inline constexpr std::string_view kGreptimeErrorMessageHeader =
    "x-greptime-err-msg";
inline constexpr std::string_view kDefaultCatalog = "greptime";
inline constexpr std::string_view kDefaultSchema = "public";

/** Merge semantics for rows sharing a primary key and timestamp. */
enum class MergeMode { kLastRow, kLastNonNull };

/** Return the server's spelling, or an empty view for an invalid enum value. */
[[nodiscard]] std::string_view MergeModeName(MergeMode mode) noexcept;

/** Validated, insertion-ordered ingestion directives; repeated assignments
 * replace in place. */
class WriteHints final {
public:
  using Entry = std::pair<std::string, std::string>;

  /** Reject ambiguous delimiters, surrounding whitespace, and non-printable
   * ASCII atomically. */
  [[nodiscard]] Result<void> SetHint(std::string key, std::string value);
  [[nodiscard]] Result<void> SetAutoCreateTable(bool enabled);
  [[nodiscard]] Result<void> SetTtl(std::string ttl);
  [[nodiscard]] Result<void> SetAppendMode(bool enabled);
  [[nodiscard]] Result<void> SetMergeMode(MergeMode mode);
  [[nodiscard]] Result<void> SetPhysicalTable(std::string table);

  /** Empty hints produce an empty string, indicating that no header should be
   * sent. */
  [[nodiscard]] std::string FormatHeaderValue() const;
  [[nodiscard]] bool IsEmpty() const noexcept { return entries_.empty(); }
  [[nodiscard]] const std::vector<Entry> &Entries() const noexcept {
    return entries_;
  }

  /** Request-local assignments override defaults without changing the default
   * object. */
  [[nodiscard]] WriteHints MergedWith(const WriteHints &extra) const;

private:
  void Insert(std::string key, std::string value);
  std::vector<Entry> entries_;
};

} // namespace greptimedb
