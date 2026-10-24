/** PromQL instant and range requests and Prometheus JSON response bodies. */
#pragma once

#include "greptimedb/client/scope.h"

#include <greptime/v1/common.pb.h>
#include <greptime/v1/prom.pb.h>

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>

namespace greptimedb::client {

enum class PromErrorKind : std::uint8_t {
  kEmptySchema,
  kServerStatus,
  kInvalidUtf8,
  kInvalidText,
};

struct PromStatus {
  std::uint32_t code{};
  std::string message;
  bool retry_hint{};
};

struct PromError {
  PromErrorKind kind{PromErrorKind::kServerStatus};
  PromStatus status{};
};

struct PromInstantQuery {
  std::string_view query;
  std::string_view time;
  std::string_view lookback;
};

struct PromRangeQuery {
  std::string_view query;
  std::string_view start;
  std::string_view end;
  std::string_view step;
  std::string_view lookback;
};

struct PromQueryResult {
  std::string body;
  ::greptime::v1::ResponseHeader header;
  /** Borrows `body` when it is strict UTF-8, including empty. Does not parse
   * JSON. */
  [[nodiscard]] std::expected<std::string_view, PromError>
  AsJson() const & noexcept;
  std::expected<std::string_view, PromError> AsJson() const && = delete;
};

[[nodiscard]] std::expected<::greptime::v1::PromqlRequest, PromError>
BuildPromInstantRequest(QueryScope scope, PromInstantQuery query);

[[nodiscard]] std::expected<::greptime::v1::PromqlRequest, PromError>
BuildPromRangeRequest(QueryScope scope, PromRangeQuery query);

[[nodiscard]] std::expected<PromQueryResult, PromError>
DecodePromResponse(const ::greptime::v1::PromqlResponse &response);

} // namespace greptimedb::client
