/** Builds one GreptimeRequest DDL payload for a query scope.
 * An empty scope schema is rejected before the expression is read. The header
 * copies catalog and schema, sets dbname to the schema or to catalog-schema,
 * and sets timezone only when it is nonempty. Authorization and tracing stay
 * unset. Builders validate the expression fail-closed, then attach it with
 * mutable_ddl(). BuildDdlRequest does not rewrite expression fields.
 */
#pragma once

#include "greptimedb/client/scope.h"

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <utility>

#include <greptime/v1/database.pb.h>
#include <greptime/v1/ddl.pb.h>

namespace greptimedb::client {

enum class DdlError : std::uint8_t {
  kEmptySchema,
  kEmptyName,
  kMissingOperation,
  kEmptyOptionKey,
  kEmptyColumn,
  kEmptyDefinition,
  kEmptySql,
  kMissingColumnName,
  kInvalidText,
};

[[nodiscard]] auto BuildCreateDatabase(
    QueryScope scope, std::string_view schema_name, bool create_if_not_exists,
    std::span<const std::pair<std::string_view, std::string_view>> options)
    noexcept -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildAlterDatabase(QueryScope scope,
                   const ::greptime::v1::AlterDatabaseExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildCreateTable(QueryScope scope,
                 const ::greptime::v1::CreateTableExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildAlterTable(QueryScope scope,
                const ::greptime::v1::AlterTableExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto BuildDropTable(QueryScope scope,
                                  std::string_view table_name,
                                  bool drop_if_exists) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto BuildTruncateTable(QueryScope scope,
                                      std::string_view table_name) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildCreateView(QueryScope scope,
                const ::greptime::v1::CreateViewExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto BuildDropView(QueryScope scope, std::string_view view_name,
                                 bool drop_if_exists) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildCreateFlow(QueryScope scope,
                const ::greptime::v1::CreateFlowExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto BuildDropFlow(QueryScope scope, std::string_view flow_name,
                                 bool drop_if_exists) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildCommentOn(QueryScope scope,
               const ::greptime::v1::CommentOnExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

[[nodiscard]] auto
BuildDdlRequest(QueryScope scope,
                const ::greptime::v1::DdlRequest &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError>;

} // namespace greptimedb::client
