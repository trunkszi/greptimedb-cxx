/** Build PromQL gateway requests and decode their status and JSON bodies. */
#include "greptimedb/client/prom.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <greptime/v1/common.pb.h>
#include <greptime/v1/prom.pb.h>

namespace greptimedb::client {
namespace {

/** Only the exact tokens "true" and "retryable" grant a retry. */
[[nodiscard]] bool RetryGranted(std::string_view hint) noexcept {
  return hint == "true" || hint == "retryable";
}

/** An empty catalog intentionally yields the schema alone. */
[[nodiscard]] std::string DbName(QueryScope scope) {
  if (scope.catalog.empty())
    return std::string(scope.schema);
  std::string name;
  name.reserve(scope.catalog.size() + 1 + scope.schema.size());
  name.append(scope.catalog);
  name.push_back('-');
  name.append(scope.schema);
  return name;
}

void Assign(std::string *field, std::string_view value) {
  field->assign(value.begin(), value.end());
}

void FillHeader(::greptime::v1::RequestHeader *header, QueryScope scope) {
  Assign(header->mutable_catalog(), scope.catalog);
  Assign(header->mutable_schema(), scope.schema);
  Assign(header->mutable_dbname(), DbName(scope));
  if (!scope.timezone.empty())
    Assign(header->mutable_timezone(), scope.timezone);
}

[[nodiscard]] ::greptime::v1::PromqlRequest
StartRequest(QueryScope scope) {
  ::greptime::v1::PromqlRequest request;
  FillHeader(request.mutable_header(), scope);
  return request;
}

} // namespace

std::expected<std::string_view, PromError>
PromQueryResult::AsJson() const & noexcept {
  if (!detail::IsStrictUtf8(body))
    return std::unexpected(PromError{PromErrorKind::kInvalidUtf8, {}});
  return std::string_view(body);
}

std::expected<::greptime::v1::PromqlRequest, PromError>
BuildPromInstantRequest(QueryScope scope, PromInstantQuery query) {
  if (scope.schema.empty())
    return std::unexpected(PromError{PromErrorKind::kEmptySchema, {}});
  if (!detail::ValidScope(scope))
    return std::unexpected(PromError{PromErrorKind::kInvalidText, {}});
  auto request = StartRequest(scope);
  for (auto text : {query.query, query.time, query.lookback})
    if (!detail::IsStrictUtf8(text) || text.find('\0') != text.npos)
      return std::unexpected(PromError{PromErrorKind::kInvalidText, {}});
  auto *instant = request.mutable_instant_query();
  Assign(instant->mutable_query(), query.query);
  Assign(instant->mutable_time(), query.time);
  Assign(instant->mutable_lookback(), query.lookback);
  return request;
}

std::expected<::greptime::v1::PromqlRequest, PromError>
BuildPromRangeRequest(QueryScope scope, PromRangeQuery query) {
  if (scope.schema.empty())
    return std::unexpected(PromError{PromErrorKind::kEmptySchema, {}});
  if (!detail::ValidScope(scope))
    return std::unexpected(PromError{PromErrorKind::kInvalidText, {}});
  auto request = StartRequest(scope);
  for (auto text : {query.query, query.start, query.end, query.step, query.lookback})
    if (!detail::IsStrictUtf8(text) || text.find('\0') != text.npos)
      return std::unexpected(PromError{PromErrorKind::kInvalidText, {}});
  auto *range = request.mutable_range_query();
  Assign(range->mutable_query(), query.query);
  Assign(range->mutable_start(), query.start);
  Assign(range->mutable_end(), query.end);
  Assign(range->mutable_step(), query.step);
  Assign(range->mutable_lookback(), query.lookback);
  return request;
}

std::expected<PromQueryResult, PromError>
DecodePromResponse(const ::greptime::v1::PromqlResponse &response) {
  // A nonzero application status overrides any JSON body.
  if (response.has_header() && response.header().has_status() &&
      response.header().status().status_code() != 0) {
    const auto &status = response.header().status();
    PromError error{PromErrorKind::kServerStatus, {}};
    error.status.code = status.status_code();
    error.status.message = status.err_msg();
    error.status.retry_hint = RetryGranted(status.retry_hint());
    return std::unexpected(std::move(error));
  }
  PromQueryResult result;
  result.body = response.body();
  result.header = response.header();
  return result;
}

} // namespace greptimedb::client
