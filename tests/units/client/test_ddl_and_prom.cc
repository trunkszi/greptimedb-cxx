/** DDL and Prometheus request builders, including response-body decoding. */
#include <greptime/v1/database.pb.h>
#include <greptime/v1/ddl.pb.h>
#include <greptime/v1/prom.pb.h>
#include <greptimedb/client/ddl.h>
#include <greptimedb/client/prom.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace {
using greptimedb::client::BuildAlterDatabase;
using greptimedb::client::BuildAlterTable;
using greptimedb::client::BuildCommentOn;
using greptimedb::client::BuildCreateDatabase;
using greptimedb::client::BuildCreateFlow;
using greptimedb::client::BuildCreateTable;
using greptimedb::client::BuildCreateView;
using greptimedb::client::BuildDdlRequest;
using greptimedb::client::BuildDropFlow;
using greptimedb::client::BuildDropTable;
using greptimedb::client::BuildDropView;
using greptimedb::client::BuildPromInstantRequest;
using greptimedb::client::BuildPromRangeRequest;
using greptimedb::client::BuildTruncateTable;
using greptimedb::client::DecodePromResponse;
using greptimedb::client::DdlError;
using greptimedb::client::PromError;
using greptimedb::client::PromErrorKind;
using greptimedb::client::PromInstantQuery;
using greptimedb::client::PromQueryResult;
using greptimedb::client::PromRangeQuery;
using greptimedb::client::QueryScope;

using GreptimeRequest = ::greptime::v1::GreptimeRequest;
using DdlRequest = ::greptime::v1::DdlRequest;
using PromqlRequest = ::greptime::v1::PromqlRequest;
using OptionPair = std::pair<std::string_view, std::string_view>;

constexpr std::string_view kSuccessJson =
    R"({"status":"success","data":{"resultType":"vector","result":[]}})";

QueryScope TradingScope() { return {"trading", "ticks", ""}; }
QueryScope PromScope() { return {"trading", "ticks", "UTC"}; }
QueryScope PublicScope() { return {"", "public", ""}; }
QueryScope NoSchema() { return {"trading", "", ""}; }

void ExpectRouted(const GreptimeRequest &request, std::string_view catalog, std::string_view schema,
                  std::string_view dbname) {
  EXPECT_EQ(request.request_case(), GreptimeRequest::kDdl);
  EXPECT_FALSE(request.has_inserts());
  EXPECT_FALSE(request.has_query());
  EXPECT_TRUE(request.has_header());
  EXPECT_EQ(request.header().catalog(), catalog);
  EXPECT_EQ(request.header().schema(), schema);
  EXPECT_EQ(request.header().dbname(), dbname);
}

void ExpectDdlError(const std::expected<GreptimeRequest, DdlError> &result, DdlError error) {
  EXPECT_FALSE(result.has_value());
  if (!result.has_value()) {
    EXPECT_EQ(result.error(), error);
  }
}

template <typename Message>
void ExpectSame(const Message &actual, const Message &expected) {
  EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

template <typename T>
void ExpectPromKind(const std::expected<T, PromError> &result, PromErrorKind kind) {
  EXPECT_FALSE(result.has_value());
  if (!result.has_value()) {
    EXPECT_EQ(result.error().kind, kind);
  }
}

void ExpectServerStatus(const std::expected<PromQueryResult, PromError> &result, std::uint32_t code,
                        std::string_view message, bool retry_hint) {
  EXPECT_FALSE(result.has_value());
  if (result.has_value()) return;
  EXPECT_EQ(result.error().kind, PromErrorKind::kServerStatus);
  EXPECT_EQ(result.error().status.code, code);
  EXPECT_EQ(std::string_view{result.error().status.message}, message);
  EXPECT_EQ(result.error().status.retry_hint, retry_hint);
}

void ExpectPromHeader(const PromqlRequest &request) {
  EXPECT_TRUE(request.has_header());
  EXPECT_EQ(request.header().catalog(), "trading");
  EXPECT_EQ(request.header().schema(), "ticks");
  EXPECT_EQ(request.header().dbname(), "trading-ticks");
  EXPECT_EQ(request.header().timezone(), "UTC");
}

TEST(DdlBuilders, RejectsEmptySchemaWithoutARequest) {
  const std::span<const OptionPair> no_options;
  ExpectDdlError(BuildCreateDatabase(NoSchema(), "archive", true, no_options),
                 DdlError::kEmptySchema);
  ExpectDdlError(BuildDropTable(NoSchema(), "ticks", true), DdlError::kEmptySchema);
  ::greptime::v1::CreateTableExpr table;
  table.set_table_name("ticks");
  table.set_catalog_name("trading");
  table.set_schema_name("ticks");
  table.set_engine("mito");
  DdlRequest request;
  *request.mutable_create_table() = table;
  ExpectDdlError(BuildDdlRequest(NoSchema(), request), DdlError::kEmptySchema);
}

TEST(DdlBuilders, CreateDatabaseKeepsClientHeaderAndOptions) {
  // schema_name is the database being created; the header keeps the client schema.
  const std::array<OptionPair, 1> options{OptionPair{"ttl", "7d"}};
  const auto built = BuildCreateDatabase(TradingScope(), "archive", true, options);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kCreateDatabase);
  const auto &created = built->ddl().create_database();
  EXPECT_EQ(created.catalog_name(), "trading");
  EXPECT_EQ(created.schema_name(), "archive");
  EXPECT_TRUE(created.create_if_not_exists());
  ASSERT_EQ(created.options().size(), 1U);
  const auto ttl = created.options().find("ttl");
  ASSERT_NE(ttl, created.options().end());
  EXPECT_EQ(ttl->second, "7d");

  ExpectDdlError(BuildCreateDatabase(TradingScope(), "", true, std::span<const OptionPair>{}),
                 DdlError::kEmptyName);
  const std::array<OptionPair, 1> blank_key{OptionPair{"", "7d"}};
  ExpectDdlError(BuildCreateDatabase(TradingScope(), "archive", true, blank_key),
                 DdlError::kEmptyOptionKey);
}

TEST(DdlBuilders, AlterDatabaseRoundTripsUnsetAndRejectsMissingKind) {
  ::greptime::v1::AlterDatabaseExpr missing;
  missing.set_catalog_name("foreign_catalog");
  missing.set_schema_name("foreign_schema");
  EXPECT_EQ(missing.kind_case(), ::greptime::v1::AlterDatabaseExpr::KIND_NOT_SET);
  ExpectDdlError(BuildAlterDatabase(TradingScope(), missing), DdlError::kMissingOperation);

  missing.mutable_unset_database_options()->add_keys("");
  ExpectDdlError(BuildAlterDatabase(TradingScope(), missing), DdlError::kEmptyOptionKey);

  ::greptime::v1::AlterDatabaseExpr alter;
  alter.set_catalog_name("foreign_catalog");
  alter.set_schema_name("foreign_schema");
  alter.mutable_unset_database_options()->add_keys("ttl");
  const auto built = BuildAlterDatabase(TradingScope(), alter);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kAlterDatabase);
  ExpectSame(built->ddl().alter_database(), alter);
  EXPECT_NE(built->header().catalog(), "foreign_catalog");
  EXPECT_NE(built->header().schema(), "foreign_schema");
}

TEST(DdlBuilders, CreateTableRoundTripsAndRejectsEmptyNameOrColumn) {
  ::greptime::v1::CreateTableExpr table;
  table.set_table_name("ticks");
  table.set_catalog_name("trading");
  table.set_schema_name("ticks");
  table.set_engine("mito");
  const auto built = BuildCreateTable(TradingScope(), table);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kCreateTable);
  ExpectSame(built->ddl().create_table(), table);

  ::greptime::v1::CreateTableExpr named = table;
  named.add_column_defs()->set_name("price");
  const auto with_column = BuildCreateTable(TradingScope(), named);
  ASSERT_TRUE(with_column.has_value());
  ExpectSame(with_column->ddl().create_table(), named);

  table.set_table_name("");
  ExpectDdlError(BuildCreateTable(TradingScope(), table), DdlError::kEmptyName);

  table.set_table_name("ticks");
  table.add_column_defs()->set_name("");
  ExpectDdlError(BuildCreateTable(TradingScope(), table), DdlError::kEmptyColumn);
}

TEST(DdlBuilders, AlterTableRenameRoundTrips) {
  ::greptime::v1::AlterTableExpr missing;
  missing.set_table_name("ticks");
  missing.set_catalog_name("foreign_catalog");
  missing.set_schema_name("foreign_schema");
  EXPECT_EQ(missing.kind_case(), ::greptime::v1::AlterTableExpr::KIND_NOT_SET);
  ExpectDdlError(BuildAlterTable(TradingScope(), missing), DdlError::kMissingOperation);

  ::greptime::v1::AlterTableExpr alter = missing;
  alter.mutable_rename_table()->set_new_table_name("renamed_ticks");
  const auto built = BuildAlterTable(TradingScope(), alter);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kAlterTable);
  ExpectSame(built->ddl().alter_table(), alter);
}

TEST(DdlBuilders, DropTruncateAndPublicCatalogUseScope) {
  const auto dropped = BuildDropTable(TradingScope(), "ticks", true);
  ASSERT_TRUE(dropped.has_value());
  ExpectRouted(*dropped, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(dropped->ddl().expr_case(), DdlRequest::kDropTable);
  const auto &drop = dropped->ddl().drop_table();
  EXPECT_EQ(drop.catalog_name(), "trading");
  EXPECT_EQ(drop.schema_name(), "ticks");
  EXPECT_EQ(drop.table_name(), "ticks");
  EXPECT_TRUE(drop.drop_if_exists());
  EXPECT_FALSE(drop.has_table_id());

  const auto truncated = BuildTruncateTable(TradingScope(), "ticks");
  ASSERT_TRUE(truncated.has_value());
  ExpectRouted(*truncated, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(truncated->ddl().expr_case(), DdlRequest::kTruncateTable);
  const auto &truncate = truncated->ddl().truncate_table();
  EXPECT_EQ(truncate.catalog_name(), "trading");
  EXPECT_EQ(truncate.schema_name(), "ticks");
  EXPECT_EQ(truncate.table_name(), "ticks");
  EXPECT_FALSE(truncate.has_table_id());
  EXPECT_FALSE(truncate.has_time_ranges());

  // An empty catalog routes as the schema alone, not "-public".
  const auto public_db = BuildTruncateTable(PublicScope(), "ticks");
  ASSERT_TRUE(public_db.has_value());
  ExpectRouted(*public_db, "", "public", "public");
  EXPECT_EQ(public_db->ddl().truncate_table().catalog_name(), "");
  EXPECT_EQ(public_db->ddl().truncate_table().schema_name(), "public");
  EXPECT_EQ(public_db->ddl().truncate_table().table_name(), "ticks");
}

TEST(DdlBuilders, ViewsRoundTripAndRejectEmptyDefinition) {
  ::greptime::v1::CreateViewExpr view;
  view.set_view_name("prices");
  view.set_definition("SELECT price FROM ticks");
  const auto built = BuildCreateView(TradingScope(), view);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kCreateView);
  ExpectSame(built->ddl().create_view(), view);

  ::greptime::v1::CreateViewExpr plan_only;
  plan_only.set_view_name("prices");
  plan_only.set_logical_plan(std::string("\x01", 1));
  const auto from_plan = BuildCreateView(TradingScope(), plan_only);
  ASSERT_TRUE(from_plan.has_value());
  ExpectSame(from_plan->ddl().create_view(), plan_only);

  ::greptime::v1::CreateViewExpr empty;
  empty.set_view_name("prices");
  ExpectDdlError(BuildCreateView(TradingScope(), empty), DdlError::kEmptyDefinition);

  const auto dropped = BuildDropView(TradingScope(), "prices", true);
  ASSERT_TRUE(dropped.has_value());
  ExpectRouted(*dropped, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(dropped->ddl().expr_case(), DdlRequest::kDropView);
  const auto &drop = dropped->ddl().drop_view();
  EXPECT_EQ(drop.catalog_name(), "trading");
  EXPECT_EQ(drop.schema_name(), "ticks");
  EXPECT_EQ(drop.view_name(), "prices");
  EXPECT_TRUE(drop.drop_if_exists());
  EXPECT_FALSE(drop.has_view_id());
}

TEST(DdlBuilders, FlowsRoundTripAndDropHasNoSchema) {
  ::greptime::v1::CreateFlowExpr flow;
  flow.set_flow_name("aggregate");
  flow.set_sql("SELECT avg(price) FROM ticks");
  const auto built = BuildCreateFlow(TradingScope(), flow);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kCreateFlow);
  ExpectSame(built->ddl().create_flow(), flow);

  flow.clear_sql();
  ExpectDdlError(BuildCreateFlow(TradingScope(), flow), DdlError::kEmptySql);

  const auto dropped = BuildDropFlow(TradingScope(), "aggregate", true);
  ASSERT_TRUE(dropped.has_value());
  ExpectRouted(*dropped, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(dropped->ddl().expr_case(), DdlRequest::kDropFlow);
  const auto &drop = dropped->ddl().drop_flow();
  EXPECT_EQ(drop.catalog_name(), "trading");
  EXPECT_EQ(drop.flow_name(), "aggregate");
  EXPECT_TRUE(drop.drop_if_exists());
  EXPECT_FALSE(drop.has_flow_id());
  EXPECT_EQ(drop.GetDescriptor()->FindFieldByName("schema_name"), nullptr);
  ExpectDdlError(BuildDropFlow(TradingScope(), "", true), DdlError::kEmptyName);
}

TEST(DdlBuilders, CommentOnRoundTripsAndRequiresColumnName) {
  ::greptime::v1::CommentOnExpr comment;
  comment.set_catalog_name("foreign_catalog");
  comment.set_schema_name("foreign_schema");
  comment.set_object_name("ticks");
  comment.set_comment("Historical quotes in USD");
  const auto built = BuildCommentOn(TradingScope(), comment);
  ASSERT_TRUE(built.has_value());
  ExpectRouted(*built, "trading", "ticks", "trading-ticks");
  ASSERT_EQ(built->ddl().expr_case(), DdlRequest::kCommentOn);
  ExpectSame(built->ddl().comment_on(), comment);

  ::greptime::v1::CommentOnExpr column = comment;
  column.set_object_type(::greptime::v1::CommentObjectType::COLUMN);
  column.set_column_name("price");
  const auto named = BuildCommentOn(TradingScope(), column);
  ASSERT_TRUE(named.has_value());
  ExpectSame(named->ddl().comment_on(), column);

  column.clear_column_name();
  ExpectDdlError(BuildCommentOn(TradingScope(), column), DdlError::kMissingColumnName);
}

TEST(DdlBuilders, DdlRequestMatchesCreateTable) {
  ::greptime::v1::CreateTableExpr table;
  table.set_table_name("ticks");
  table.set_catalog_name("trading");
  table.set_schema_name("ticks");
  table.set_engine("mito");
  DdlRequest request;
  *request.mutable_create_table() = table;
  const auto via_request = BuildDdlRequest(TradingScope(), request);
  const auto direct = BuildCreateTable(TradingScope(), table);
  ASSERT_TRUE(via_request.has_value());
  ASSERT_TRUE(direct.has_value());
  ExpectSame(*via_request, *direct);
  EXPECT_EQ(via_request->request_case(), GreptimeRequest::kDdl);
  EXPECT_FALSE(via_request->has_inserts());
  EXPECT_FALSE(via_request->has_query());

  ExpectDdlError(BuildDdlRequest(TradingScope(), DdlRequest{}), DdlError::kMissingOperation);
}

TEST(PromBuilders, InstantAndRangeCopyHeaderAndQuery) {
  const PromInstantQuery instant{R"(up{job="quotes"})", "1700000000", "5m"};
  const auto instant_request = BuildPromInstantRequest(PromScope(), instant);
  ASSERT_TRUE(instant_request.has_value());
  ExpectPromHeader(*instant_request);
  ASSERT_EQ(instant_request->promql_case(), PromqlRequest::kInstantQuery);
  EXPECT_TRUE(instant_request->has_instant_query());
  EXPECT_FALSE(instant_request->has_range_query());
  EXPECT_EQ(instant_request->instant_query().query(), R"(up{job="quotes"})");
  EXPECT_EQ(instant_request->instant_query().time(), "1700000000");
  EXPECT_EQ(instant_request->instant_query().lookback(), "5m");

  const PromRangeQuery range{"rate(trades[1m])", "1700000000", "1700000060", "5s", "10m"};
  const auto range_request = BuildPromRangeRequest(PromScope(), range);
  ASSERT_TRUE(range_request.has_value());
  ExpectPromHeader(*range_request);
  ASSERT_EQ(range_request->promql_case(), PromqlRequest::kRangeQuery);
  EXPECT_TRUE(range_request->has_range_query());
  EXPECT_FALSE(range_request->has_instant_query());
  EXPECT_EQ(range_request->range_query().query(), "rate(trades[1m])");
  EXPECT_EQ(range_request->range_query().start(), "1700000000");
  EXPECT_EQ(range_request->range_query().end(), "1700000060");
  EXPECT_EQ(range_request->range_query().step(), "5s");
  EXPECT_EQ(range_request->range_query().lookback(), "10m");
}

TEST(PromBuilders, EmptyQueryFieldsArePreserved) {
  const auto instant = BuildPromInstantRequest(PromScope(), PromInstantQuery{"", "", ""});
  ASSERT_TRUE(instant.has_value());
  ExpectPromHeader(*instant);
  ASSERT_TRUE(instant->has_instant_query());
  EXPECT_FALSE(instant->has_range_query());
  EXPECT_EQ(instant->instant_query().query(), "");
  EXPECT_EQ(instant->instant_query().time(), "");
  EXPECT_EQ(instant->instant_query().lookback(), "");

  const auto range = BuildPromRangeRequest(PromScope(), PromRangeQuery{"", "", "", "", ""});
  ASSERT_TRUE(range.has_value());
  ASSERT_TRUE(range->has_range_query());
  EXPECT_FALSE(range->has_instant_query());
  EXPECT_EQ(range->range_query().query(), "");
  EXPECT_EQ(range->range_query().start(), "");
  EXPECT_EQ(range->range_query().end(), "");
  EXPECT_EQ(range->range_query().step(), "");
  EXPECT_EQ(range->range_query().lookback(), "");
}

TEST(PromBuilders, RejectsEmptySchema) {
  const PromInstantQuery instant{"up", "1700000000", "5m"};
  ExpectPromKind(BuildPromInstantRequest(NoSchema(), instant), PromErrorKind::kEmptySchema);
  const PromRangeQuery range{"up", "1", "2", "5s", "1m"};
  ExpectPromKind(BuildPromRangeRequest(NoSchema(), range), PromErrorKind::kEmptySchema);
}

TEST(PromDecode, SuccessCopiesHeaderAndExposesJson) {
  ::greptime::v1::PromqlResponse response;
  response.set_body(std::string{kSuccessJson});
  response.mutable_header();
  const auto decoded = DecodePromResponse(response);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->body, kSuccessJson);
  EXPECT_EQ(decoded->header.SerializeAsString(), response.header().SerializeAsString());
  EXPECT_FALSE(decoded->header.has_status());
  const auto json = decoded->AsJson();
  ASSERT_TRUE(json.has_value());
  EXPECT_EQ(*json, kSuccessJson);

  auto *status = response.mutable_header()->mutable_status();
  status->set_status_code(0);
  status->set_err_msg("noted");
  status->set_retry_hint("true");
  const auto zero = DecodePromResponse(response);
  ASSERT_TRUE(zero.has_value());
  EXPECT_EQ(zero->header.SerializeAsString(), response.header().SerializeAsString());
  EXPECT_EQ(zero->header.status().status_code(), 0U);
  EXPECT_EQ(zero->header.status().retry_hint(), "true");
  const auto zero_json = zero->AsJson();
  ASSERT_TRUE(zero_json.has_value());
  EXPECT_EQ(*zero_json, kSuccessJson);
}

TEST(PromDecode, ServerStatusOverridesSuccessBody) {
  const std::pair<std::string_view, bool> hints[]{
      {"false", false}, {"true", true}, {"retryable", true},
      {"non_retryable", false}, {"sometimes", false},
  };
  for (const auto &[hint, retryable] : hints) {
    ::greptime::v1::PromqlResponse response;
    response.set_body(std::string{kSuccessJson});
    auto *status = response.mutable_header()->mutable_status();
    status->set_status_code(2001);
    status->set_err_msg("invalid plan");
    status->set_retry_hint(std::string{hint});
    ExpectServerStatus(DecodePromResponse(response), 2001, "invalid plan", retryable);
  }
}

TEST(PromDecode, MissingHeaderAndInvalidUtf8) {
  const auto missing = DecodePromResponse(::greptime::v1::PromqlResponse{});
  ASSERT_TRUE(missing.has_value());
  EXPECT_TRUE(missing->body.empty());
  EXPECT_FALSE(missing->header.has_status());
  const auto empty_json = missing->AsJson();
  ASSERT_TRUE(empty_json.has_value());
  EXPECT_TRUE(empty_json->empty());

  // Invalid UTF-8 stays on the result; only AsJson rejects it.
  ::greptime::v1::PromqlResponse response;
  response.set_body(std::string("\xff", 1));
  const auto decoded = DecodePromResponse(response);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->body, std::string("\xff", 1));
  const auto json = decoded->AsJson();
  EXPECT_FALSE(json.has_value());
  if (!json.has_value()) {
    EXPECT_EQ(json.error().kind, PromErrorKind::kInvalidUtf8);
  }
}

}  // namespace
