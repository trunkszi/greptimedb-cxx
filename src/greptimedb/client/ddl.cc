/** Fail-closed checks for GreptimeDB DDL expressions and their request header.
 * Constructors fill an expression, then use the same checks as BuildDdlRequest.
 */
#include "greptimedb/client/ddl.h"

#include <string>
#include <vector>
#include <utility>
#include <google/protobuf/message.h>

namespace greptimedb::client {
namespace {

namespace proto = ::greptime::v1;

using Check = std::expected<void, DdlError>;
using Built = std::expected<proto::DdlRequest, DdlError>;

[[nodiscard]] auto Ok() noexcept -> Check { return {}; }

[[nodiscard]] auto Fail(DdlError error) noexcept -> Check {
  return std::unexpected(error);
}

[[nodiscard]] auto ScopeOr(QueryScope scope) noexcept -> Check {
  if (scope.schema.empty())
    return Fail(DdlError::kEmptySchema);
  if (!detail::ValidScope(scope)) return Fail(DdlError::kInvalidText);
  return Ok();
}

template <typename Options>
[[nodiscard]] auto ValidateMapKeys(const Options &options) noexcept -> Check {
  for (const auto &entry : options) {
    if (!detail::HasText(entry.first))
      return Fail(DdlError::kEmptyOptionKey);
  }
  return Ok();
}

template <typename Options>
[[nodiscard]] auto ValidateRequiredOptions(const Options &options) noexcept
    -> Check {
  if (options.empty())
    return Fail(DdlError::kMissingOperation);
  for (const auto &option : options) {
    if (!detail::HasText(option.key()))
      return Fail(DdlError::kEmptyOptionKey);
  }
  return Ok();
}

template <typename Keys>
[[nodiscard]] auto ValidateRequiredKeys(const Keys &keys) noexcept -> Check {
  if (keys.empty())
    return Fail(DdlError::kMissingOperation);
  for (const auto &key : keys) {
    if (!detail::HasText(key))
      return Fail(DdlError::kEmptyOptionKey);
  }
  return Ok();
}

template <typename Columns>
[[nodiscard]] auto ValidateNamedColumns(const Columns &columns) noexcept
    -> Check {
  if (columns.empty())
    return Fail(DdlError::kMissingOperation);
  for (const auto &column : columns) {
    if (!detail::HasText(column.column_name()))
      return Fail(DdlError::kEmptyColumn);
  }
  return Ok();
}

template <typename Names>
[[nodiscard]] auto ValidateTableNames(const Names &names) noexcept -> Check {
  for (const auto &name : names) {
    if (!detail::HasText(name.table_name()))
      return Fail(DdlError::kEmptyName);
  }
  return Ok();
}

/** A named sink must carry a table name; a fully blank sink is absent. */
[[nodiscard]] auto ValidateSink(const proto::TableName &sink) noexcept -> Check {
  const bool named = !sink.catalog_name().empty() ||
                     !sink.schema_name().empty() || !sink.table_name().empty();
  if (named && !detail::HasText(sink.table_name()))
    return Fail(DdlError::kEmptyName);
  return Ok();
}

template <typename Index>
[[nodiscard]] auto ValidateIndex(const Index &index) noexcept -> Check {
  switch (index.options_case()) {
  case Index::kFulltext:
    if (!detail::HasText(index.fulltext().column_name()))
      return Fail(DdlError::kEmptyColumn);
    return Ok();
  case Index::kInverted:
    if (!detail::HasText(index.inverted().column_name()))
      return Fail(DdlError::kEmptyColumn);
    return Ok();
  case Index::kSkipping:
    if (!detail::HasText(index.skipping().column_name()))
      return Fail(DdlError::kEmptyColumn);
    return Ok();
  case Index::OPTIONS_NOT_SET:
    return Fail(DdlError::kMissingOperation);
  }
  return Fail(DdlError::kMissingOperation);
}

template <typename Indexes>
[[nodiscard]] auto ValidateIndexes(const Indexes &indexes) noexcept -> Check {
  if (indexes.empty())
    return Fail(DdlError::kMissingOperation);
  for (const auto &index : indexes) {
    if (auto check = ValidateIndex(index); !check)
      return check;
  }
  return Ok();
}

[[nodiscard]] auto
ValidateAddColumns(const proto::AddColumns &added) noexcept -> Check {
  if (added.add_columns().empty())
    return Fail(DdlError::kMissingOperation);
  for (const auto &column : added.add_columns()) {
    if (!detail::HasText(column.column_def().name()))
      return Fail(DdlError::kEmptyColumn);
  }
  return Ok();
}

[[nodiscard]] auto
ValidateDropColumns(const proto::DropColumns &dropped) noexcept -> Check {
  if (dropped.drop_columns().empty())
    return Fail(DdlError::kMissingOperation);
  for (const auto &column : dropped.drop_columns()) {
    if (!detail::HasText(column.name()))
      return Fail(DdlError::kEmptyColumn);
  }
  return Ok();
}

/** Either into_partition_exprs or a source case makes repartition concrete. */
[[nodiscard]] auto
ValidateRepartition(const proto::Repartition &repartition) noexcept -> Check {
  switch (repartition.source_case()) {
  case proto::Repartition::kPartitionExprs:
  case proto::Repartition::kUnpartitioned:
    return Ok();
  case proto::Repartition::SOURCE_NOT_SET:
    if (repartition.into_partition_exprs_size() == 0)
      return Fail(DdlError::kMissingOperation);
    return Ok();
  }
  return Fail(DdlError::kMissingOperation);
}

[[nodiscard]] auto
ValidateCreateDatabase(const proto::CreateDatabaseExpr &expression) noexcept
    -> Check {
  if (!detail::HasText(expression.schema_name()))
    return Fail(DdlError::kEmptyName);
  return ValidateMapKeys(expression.options());
}

[[nodiscard]] auto
ValidateAlterDatabase(const proto::AlterDatabaseExpr &expression) noexcept
    -> Check {
  if (!detail::HasText(expression.schema_name())) return Fail(DdlError::kEmptyName);
  switch (expression.kind_case()) {
  case proto::AlterDatabaseExpr::kSetDatabaseOptions:
    return ValidateRequiredOptions(
        expression.set_database_options().set_database_options());
  case proto::AlterDatabaseExpr::kUnsetDatabaseOptions:
    return ValidateRequiredKeys(expression.unset_database_options().keys());
  case proto::AlterDatabaseExpr::KIND_NOT_SET:
    return Fail(DdlError::kMissingOperation);
  }
  return Fail(DdlError::kMissingOperation);
}

[[nodiscard]] auto
ValidateCreateTable(const proto::CreateTableExpr &expression) noexcept -> Check {
  // Catalog and schema are taken from the expression, including when either is
  // empty. The client scope is written only onto the request header.
  if (!detail::HasText(expression.table_name()))
    return Fail(DdlError::kEmptyName);
  for (const auto &column : expression.column_defs()) {
    if (!detail::HasText(column.name()))
      return Fail(DdlError::kEmptyColumn);
  }
  for (const auto &key : expression.primary_keys()) {
    if (!detail::HasText(key))
      return Fail(DdlError::kEmptyColumn);
  }
  if (auto check = ValidateMapKeys(expression.table_options()); !check)
    return check;
  /** An unmatched or column-less time index is not a usable column. */
  if (expression.time_index().empty())
    return Ok();
  if (!detail::HasText(expression.time_index())) return Fail(DdlError::kEmptyColumn);
  if (expression.column_defs_size() == 0)
    return Fail(DdlError::kEmptyColumn);
  for (const auto &column : expression.column_defs()) {
    if (column.name() == expression.time_index())
      return Ok();
  }
  return Fail(DdlError::kEmptyColumn);
}

[[nodiscard]] auto
ValidateAlterTable(const proto::AlterTableExpr &expression) noexcept -> Check {
  if (expression.kind_case() == proto::AlterTableExpr::KIND_NOT_SET)
    return Fail(DdlError::kMissingOperation);
  if (!detail::HasText(expression.table_name()))
    return Fail(DdlError::kEmptyName);
  switch (expression.kind_case()) {
  case proto::AlterTableExpr::kAddColumns:
    return ValidateAddColumns(expression.add_columns());
  case proto::AlterTableExpr::kDropColumns:
    return ValidateDropColumns(expression.drop_columns());
  case proto::AlterTableExpr::kRenameTable:
    if (!detail::HasText(expression.rename_table().new_table_name()))
      return Fail(DdlError::kEmptyName);
    return Ok();
  case proto::AlterTableExpr::kModifyColumnTypes:
    return ValidateNamedColumns(
        expression.modify_column_types().modify_column_types());
  case proto::AlterTableExpr::kSetTableOptions:
    return ValidateRequiredOptions(
        expression.set_table_options().table_options());
  case proto::AlterTableExpr::kUnsetTableOptions:
    return ValidateRequiredKeys(expression.unset_table_options().keys());
  case proto::AlterTableExpr::kSetIndex:
    return ValidateIndex(expression.set_index());
  case proto::AlterTableExpr::kUnsetIndex:
    return ValidateIndex(expression.unset_index());
  case proto::AlterTableExpr::kDropDefaults:
    return ValidateNamedColumns(expression.drop_defaults().drop_defaults());
  case proto::AlterTableExpr::kSetIndexes:
    return ValidateIndexes(expression.set_indexes().set_indexes());
  case proto::AlterTableExpr::kUnsetIndexes:
    return ValidateIndexes(expression.unset_indexes().unset_indexes());
  case proto::AlterTableExpr::kSetDefaults:
    return ValidateNamedColumns(expression.set_defaults().set_defaults());
  case proto::AlterTableExpr::kRepartition:
    return ValidateRepartition(expression.repartition());
  case proto::AlterTableExpr::kSetJsonSettings:
    if (!detail::HasText(expression.set_json_settings().column_name()))
      return Fail(DdlError::kEmptyColumn);
    return Ok();
  case proto::AlterTableExpr::KIND_NOT_SET:
    return Fail(DdlError::kMissingOperation);
  }
  return Fail(DdlError::kMissingOperation);
}

[[nodiscard]] auto
ValidateDropTable(const proto::DropTableExpr &expression) noexcept -> Check {
  if (!detail::HasText(expression.table_name()))
    return Fail(DdlError::kEmptyName);
  return Ok();
}

[[nodiscard]] auto
ValidateTruncateTable(const proto::TruncateTableExpr &expression) noexcept
    -> Check {
  if (!detail::HasText(expression.table_name()))
    return Fail(DdlError::kEmptyName);
  return Ok();
}

[[nodiscard]] auto
ValidateCreateView(const proto::CreateViewExpr &expression) noexcept -> Check {
  // Catalog and schema stay on the expression. An unset pair is legal and is
  // not replaced from the client scope.
  if (!detail::HasText(expression.view_name()))
    return Fail(DdlError::kEmptyName);
  if (!detail::HasText(expression.definition()) && expression.logical_plan().empty())
    return Fail(DdlError::kEmptyDefinition);
  return ValidateTableNames(expression.table_names());
}

[[nodiscard]] auto
ValidateDropView(const proto::DropViewExpr &expression) noexcept -> Check {
  if (!detail::HasText(expression.view_name()))
    return Fail(DdlError::kEmptyName);
  return Ok();
}

[[nodiscard]] auto
ValidateCreateFlow(const proto::CreateFlowExpr &expression) noexcept -> Check {
  // Flow expressions may leave catalog_name unset and still round-trip.
  if (!detail::HasText(expression.flow_name()))
    return Fail(DdlError::kEmptyName);
  if (!detail::HasText(expression.sql()))
    return Fail(DdlError::kEmptySql);
  if (auto check = ValidateTableNames(expression.source_table_names()); !check)
    return check;
  if (auto check = ValidateSink(expression.sink_table_name()); !check)
    return check;
  return ValidateMapKeys(expression.flow_options());
}

[[nodiscard]] auto
ValidateDropFlow(const proto::DropFlowExpr &expression) noexcept -> Check {
  // DropFlowExpr has no schema. An empty catalog is the public catalog.
  if (!detail::HasText(expression.flow_name()))
    return Fail(DdlError::kEmptyName);
  return Ok();
}

[[nodiscard]] auto
ValidateCommentOn(const proto::CommentOnExpr &expression) noexcept -> Check {
  if (!proto::CommentObjectType_IsValid(expression.object_type()))
    return Fail(DdlError::kMissingOperation);
  if (!detail::HasText(expression.object_name()))
    return Fail(DdlError::kEmptyName);
  if (expression.object_type() == proto::COLUMN &&
      !detail::HasText(expression.column_name()))
    return Fail(DdlError::kMissingColumnName);
  return Ok();
}

[[nodiscard]] auto ValidateDdl(const proto::DdlRequest &expression) noexcept
    -> Check {
  switch (expression.expr_case()) {
  case proto::DdlRequest::kCreateDatabase:
    return ValidateCreateDatabase(expression.create_database());
  case proto::DdlRequest::kCreateTable:
    return ValidateCreateTable(expression.create_table());
  case proto::DdlRequest::kAlterTable:
    return ValidateAlterTable(expression.alter_table());
  case proto::DdlRequest::kDropTable:
    return ValidateDropTable(expression.drop_table());
  case proto::DdlRequest::kTruncateTable:
    return ValidateTruncateTable(expression.truncate_table());
  case proto::DdlRequest::kCreateFlow:
    return ValidateCreateFlow(expression.create_flow());
  case proto::DdlRequest::kDropFlow:
    return ValidateDropFlow(expression.drop_flow());
  case proto::DdlRequest::kCreateView:
    return ValidateCreateView(expression.create_view());
  case proto::DdlRequest::kDropView:
    return ValidateDropView(expression.drop_view());
  case proto::DdlRequest::kAlterDatabase:
    return ValidateAlterDatabase(expression.alter_database());
  case proto::DdlRequest::kCommentOn:
    return ValidateCommentOn(expression.comment_on());
  case proto::DdlRequest::EXPR_NOT_SET:
    return Fail(DdlError::kMissingOperation);
  }
  return Fail(DdlError::kMissingOperation);
}

[[nodiscard]] auto DatabaseName(QueryScope scope) -> std::string {
  if (scope.catalog.empty())
    return std::string(scope.schema);
  std::string dbname;
  dbname.reserve(scope.catalog.size() + 1 + scope.schema.size());
  dbname.append(scope.catalog);
  dbname.push_back('-');
  dbname.append(scope.schema);
  return dbname;
}

[[nodiscard]] auto Publish(QueryScope scope, proto::DdlRequest ddl)
    -> proto::GreptimeRequest {
  proto::GreptimeRequest request;
  auto *header = request.mutable_header();
  header->set_catalog(std::string(scope.catalog));
  header->set_schema(std::string(scope.schema));
  header->set_dbname(DatabaseName(scope));
  if (!scope.timezone.empty())
    header->set_timezone(std::string(scope.timezone));
  *request.mutable_ddl() = std::move(ddl);
  return request;
}

[[nodiscard]] auto Rejected(const Check &check)
    -> std::expected<proto::GreptimeRequest, DdlError> {
  return std::unexpected(check.error());
}

[[nodiscard]] bool ValidStrings(const google::protobuf::Message &message) {
  const auto *reflection = message.GetReflection();
  std::vector<const google::protobuf::FieldDescriptor *> fields;
  reflection->ListFields(message, &fields);
  for (const auto *field : fields) {
    const int count = field->is_repeated() ? reflection->FieldSize(message, field) : 1;
    for (int i = 0; i < count; ++i) {
      if (field->type() == google::protobuf::FieldDescriptor::TYPE_STRING) {
        const auto text = field->is_repeated() ? reflection->GetRepeatedString(message, field, i)
                                              : reflection->GetString(message, field);
        if (!detail::IsStrictUtf8(text) || text.find('\0') != text.npos) return false;
      } else if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
        const auto &child = field->is_repeated() ? reflection->GetRepeatedMessage(message, field, i)
                                                : reflection->GetMessage(message, field);
        if (!ValidStrings(child)) return false;
      }
    }
  }
  return true;
}

template <typename Fill>
[[nodiscard]] auto Build(QueryScope scope, Fill &&fill)
    -> std::expected<proto::GreptimeRequest, DdlError> {
  if (auto check = ScopeOr(scope); !check)
    return Rejected(check);
  auto ddl = std::forward<Fill>(fill)();
  if (!ddl)
    return std::unexpected(ddl.error());
  if (!ValidStrings(*ddl)) return std::unexpected(DdlError::kInvalidText);
  return Publish(scope, std::move(*ddl));
}

void CopyDatabaseOptions(
    std::span<const std::pair<std::string_view, std::string_view>> options,
    proto::CreateDatabaseExpr *expression) {
  if (options.empty())
    return;
  auto *stored = expression->mutable_options();
  for (const auto &option : options)
    (*stored)[std::string(option.first)] = std::string(option.second);
}

} // namespace

auto BuildCreateDatabase(
    QueryScope scope, std::string_view schema_name, bool create_if_not_exists,
    std::span<const std::pair<std::string_view, std::string_view>> options)
    noexcept -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    proto::CreateDatabaseExpr created;
    created.set_catalog_name(std::string(scope.catalog));
    created.set_schema_name(std::string(schema_name));
    created.set_create_if_not_exists(create_if_not_exists);
    CopyDatabaseOptions(options, &created);
    if (auto check = ValidateCreateDatabase(created); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    *ddl.mutable_create_database() = std::move(created);
    return ddl;
  });
}

auto BuildAlterDatabase(
    QueryScope scope, const ::greptime::v1::AlterDatabaseExpr &expression)
    noexcept -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateAlterDatabase(expression); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    ddl.mutable_alter_database()->CopyFrom(expression);
    return ddl;
  });
}

auto BuildCreateTable(QueryScope scope,
                      const ::greptime::v1::CreateTableExpr &expression)
    noexcept -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateCreateTable(expression); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    ddl.mutable_create_table()->CopyFrom(expression);
    return ddl;
  });
}

auto BuildAlterTable(QueryScope scope,
                     const ::greptime::v1::AlterTableExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateAlterTable(expression); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    ddl.mutable_alter_table()->CopyFrom(expression);
    return ddl;
  });
}

auto BuildDropTable(QueryScope scope, std::string_view table_name,
                    bool drop_if_exists) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    proto::DropTableExpr dropped;
    dropped.set_catalog_name(std::string(scope.catalog));
    dropped.set_schema_name(std::string(scope.schema));
    dropped.set_table_name(std::string(table_name));
    dropped.set_drop_if_exists(drop_if_exists);
    if (auto check = ValidateDropTable(dropped); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    *ddl.mutable_drop_table() = std::move(dropped);
    return ddl;
  });
}

auto BuildTruncateTable(QueryScope scope, std::string_view table_name) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    proto::TruncateTableExpr truncated;
    truncated.set_catalog_name(std::string(scope.catalog));
    truncated.set_schema_name(std::string(scope.schema));
    truncated.set_table_name(std::string(table_name));
    if (auto check = ValidateTruncateTable(truncated); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    *ddl.mutable_truncate_table() = std::move(truncated);
    return ddl;
  });
}

auto BuildCreateView(QueryScope scope,
                     const ::greptime::v1::CreateViewExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateCreateView(expression); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    ddl.mutable_create_view()->CopyFrom(expression);
    return ddl;
  });
}

auto BuildDropView(QueryScope scope, std::string_view view_name,
                   bool drop_if_exists) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    proto::DropViewExpr dropped;
    dropped.set_catalog_name(std::string(scope.catalog));
    dropped.set_schema_name(std::string(scope.schema));
    dropped.set_view_name(std::string(view_name));
    dropped.set_drop_if_exists(drop_if_exists);
    if (auto check = ValidateDropView(dropped); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    *ddl.mutable_drop_view() = std::move(dropped);
    return ddl;
  });
}

auto BuildCreateFlow(QueryScope scope,
                     const ::greptime::v1::CreateFlowExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateCreateFlow(expression); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    ddl.mutable_create_flow()->CopyFrom(expression);
    return ddl;
  });
}

auto BuildDropFlow(QueryScope scope, std::string_view flow_name,
                   bool drop_if_exists) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    proto::DropFlowExpr dropped;
    dropped.set_catalog_name(std::string(scope.catalog));
    dropped.set_flow_name(std::string(flow_name));
    dropped.set_drop_if_exists(drop_if_exists);
    if (auto check = ValidateDropFlow(dropped); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    *ddl.mutable_drop_flow() = std::move(dropped);
    return ddl;
  });
}

auto BuildCommentOn(QueryScope scope,
                    const ::greptime::v1::CommentOnExpr &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateCommentOn(expression); !check)
      return std::unexpected(check.error());
    proto::DdlRequest ddl;
    ddl.mutable_comment_on()->CopyFrom(expression);
    return ddl;
  });
}

auto BuildDdlRequest(QueryScope scope,
                     const ::greptime::v1::DdlRequest &expression) noexcept
    -> std::expected<::greptime::v1::GreptimeRequest, DdlError> {
  return Build(scope, [&]() -> Built {
    if (auto check = ValidateDdl(expression); !check)
      return std::unexpected(check.error());
    return expression;
  });
}

} // namespace greptimedb::client
