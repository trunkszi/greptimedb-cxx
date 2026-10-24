#include <greptimedb/client/client.h>
#include <greptimedb/client/ddl.h>
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/transport/mock_grpc_server.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>

namespace {
using namespace greptimedb;
namespace proto = greptime::v1;

template <class T> concept TemporaryJsonView = requires(T value) { std::move(value).AsJson(); };
static_assert(!TemporaryJsonView<PromQueryResult>);

TEST(UnaryClientUnitScenario, ColumnErrorsPreserveAllocationAndSchemaCategories) {
  EXPECT_EQ(client_detail::ColumnError(ColumnBufError::kAllocationFailure).Kind(), ErrorKind::kEncode);
  for (auto error : {ColumnBufError::kNullTimestamp, ColumnBufError::kInvalidDecimal,
                     ColumnBufError::kTypeMismatch, ColumnBufError::kInvalidSchema}) {
    const auto mapped = client_detail::ColumnError(error);
    EXPECT_EQ(mapped.Kind(), ErrorKind::kSchemaViolation);
    EXPECT_FALSE(mapped.Message().empty());
  }
}

TEST(UnaryClientUnitScenario, HelperRejectsMalformedNamesScopesAndStrings) {
  const client::QueryScope scope{"trading", "ticks", "UTC"};
  for (auto name : {std::string(" \t"), std::string("bad\0name", 8), std::string("\xff", 1)}) {
    EXPECT_FALSE(client::BuildDropTable(scope, name, false));
    EXPECT_FALSE(client::BuildCreateDatabase(scope, name, true, {}));
    EXPECT_FALSE(client::BuildPromInstantRequest({"", name, ""}, {"up", "", ""}));
  }
  proto::AlterDatabaseExpr alter;
  alter.mutable_unset_database_options()->add_keys("ttl");
  EXPECT_FALSE(client::BuildAlterDatabase(scope, alter));
  proto::CreateTableExpr table;
  table.set_table_name("ticks");
  table.set_time_index(" \t");
  EXPECT_FALSE(client::BuildCreateTable(scope, table));
  proto::CommentOnExpr comment;
  comment.set_object_name("ticks");
  comment.set_comment("bad\0comment", 11);
  EXPECT_FALSE(client::BuildCommentOn(scope, comment));
  comment.clear_comment();
  comment.set_object_type(static_cast<proto::CommentObjectType>(99));
  EXPECT_FALSE(client::BuildCommentOn(scope, comment));
  EXPECT_FALSE(client::BuildPromRangeRequest(scope, {"up", "", "", std::string_view("\xff", 1), ""}));
  // Serialized logical plans are binary and may contain NUL and non-UTF-8 bytes.
  proto::CreateViewExpr view;
  view.set_view_name("prices");
  view.set_logical_plan("\0\xff", 2);
  EXPECT_TRUE(client::BuildCreateView(scope, view));
}

TEST(UnaryClientUnitScenario, Utf8ValidationRejectsOverlongSurrogateAndTruncatedSequences) {
  const std::array malformed{std::string("\xc0\xaf", 2), std::string("\xed\xa0\x80", 3),
      std::string("\xf4\x90\x80\x80", 4), std::string("\xe2\x82", 2),
      std::string("\x80", 1)};
  for (const auto& bytes : malformed) {
    PromQueryResult result;
    result.body = bytes;
    auto json = result.AsJson();
    ASSERT_FALSE(json);
    EXPECT_EQ(json.error().kind, client::PromErrorKind::kInvalidUtf8);
  }
  PromQueryResult valid;
  valid.body = std::string("\xf4\x8f\xbf\xbf", 4);
  auto view = valid.AsJson();
  ASSERT_TRUE(view);
  EXPECT_EQ(view->data(), valid.body.data());
  EXPECT_EQ(view->size(), 4U);
}

struct [[= greptimedb::table{"decimals"}]] DecimalRow {
  [[= greptimedb::timestamp{}]] std::int64_t time;
  [[= greptimedb::field{.decimal_precision=3, .decimal_scale=1}]] Decimal128 amount;
};

struct AuditPeer {
  std::unique_ptr<MockGrpcServer> server;
  std::unique_ptr<Client> client;
  std::size_t messages{};
  void Open(MockGrpcServer::Handler handler = nullptr) {
    auto pipe = MemoryPipe::CreatePair(97);
    ASSERT_TRUE(pipe);
    auto peer = MockGrpcServer::Create(std::move(pipe->second),
      handler ? handler : +[](void* context, MockGrpcServer& server, const MockRequest& event) noexcept {
        if (event.event != MockRequest::Event::kMessage) return;
        ++static_cast<AuditPeer*>(context)->messages;
        EXPECT_EQ(event.path, "/greptime.v1.HealthCheck/HealthCheck");
        ASSERT_TRUE(server.Send(event.stream_id, {}));
        ASSERT_TRUE(server.Finish(event.stream_id));
      }, this);
    ASSERT_TRUE(peer);
    server = std::move(*peer);
    auto endpoint = Endpoint::Parse("grpc://127.0.0.1:4001");
    ASSERT_TRUE(endpoint);
    auto opened = Client::FromMemoryPipe(ClientOptions(*endpoint), std::move(pipe->first), server.get(),
      [](void* context) noexcept { return static_cast<MockGrpcServer*>(context)->PollOnce(); });
    ASSERT_TRUE(opened);
    client = std::move(*opened);
  }
};

TEST(UnaryClientIntegrationScenario, EmptyWritesAndRejectedRequestsNeverReachPeer) {
  AuditPeer peer;
  peer.Open();
  ASSERT_TRUE(peer.client);
  auto zero = [](Result<std::uint32_t> result) {
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 0U);
  };
  TypedColumnarBatch<DecimalRow> typed;
  zero(peer.client->Insert<DecimalRow>({}));
  EXPECT_FALSE(peer.client->InsertTo<DecimalRow>("", {}));
  zero(peer.client->InsertBatch(typed));
  zero(peer.client->DeleteBatch(typed));
  ColumnarBatch columns("ticks");
  ASSERT_TRUE(columns.AddTimestampColumn("time", TimestampUnit::kNanosecond));
  RowBatch rows("ticks");
  ASSERT_TRUE(rows.AddTimestampColumn("time", TimestampUnit::kNanosecond));
  zero(peer.client->InsertBatch(columns));
  zero(peer.client->DeleteBatch(columns));
  zero(peer.client->InsertRows(rows));
  zero(peer.client->DeleteRows(rows));
  zero(peer.client->InsertBatches(std::span<const ColumnarBatch* const>{}));
  zero(peer.client->InsertMultiBatch(MultiTableBatch{}));
  ColumnarBatch unnamed("");
  ASSERT_TRUE(unnamed.AddTimestampColumn("time", TimestampUnit::kNanosecond));
  const std::array<const ColumnarBatch*, 2> bad_batch_list{&columns, &unnamed};
  auto bad_list = peer.client->InsertBatches(bad_batch_list);
  ASSERT_FALSE(bad_list);
  EXPECT_EQ(bad_list.error().Kind(), ErrorKind::kSchemaViolation);
  proto::InsertRequests empty;
  auto* valid = empty.add_inserts();
  valid->set_table_name("ticks");
  zero(peer.client->InsertBatches(empty));
  valid->set_table_name("");
  EXPECT_FALSE(peer.client->InsertBatches(empty));
  for (auto name : {std::string_view{}, std::string_view{" \t"}, std::string_view{"bad\0name", 8}}) {
    auto result = peer.client->InsertBatchTo(name, columns);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().Kind(), ErrorKind::kSchemaViolation);
    EXPECT_FALSE(peer.client->DeleteBatchFrom(name, columns));
    EXPECT_FALSE(peer.client->InsertRowsTo(name, rows));
    EXPECT_FALSE(peer.client->DeleteRowsFrom(name, rows));
  }
  const std::array bad_rows{DecimalRow{1, Decimal128{12, 3, 2}}};
  auto decimal = peer.client->Insert<DecimalRow>(bad_rows);
  ASSERT_FALSE(decimal);
  EXPECT_EQ(decimal.error().Kind(), ErrorKind::kSchemaViolation);
  EXPECT_FALSE(peer.client->DropTable("", false));
  proto::AlterTableExpr missing;
  missing.set_table_name("ticks");
  EXPECT_FALSE(peer.client->AlterTable(missing));
  EXPECT_FALSE(peer.client->PromInstantQuery(std::string_view("bad\xff", 4)));
  proto::CreateViewExpr oversized;
  oversized.set_view_name("prices");
  oversized.set_definition(std::string(4U * 1024U * 1024U, 'x'));
  auto large = peer.client->CreateView(std::move(oversized));
  ASSERT_FALSE(large);
  EXPECT_EQ(large.error().Kind(), ErrorKind::kEncode);
  // Pump the channel after rejection so a queued accidental write cannot hide.
  ASSERT_TRUE(peer.client->Ping());
  EXPECT_EQ(peer.messages, 1U);
}

TEST(UnaryClientIntegrationScenario, MetadataRetryDenialDominatesProtobufRetryPermission) {
  AuditPeer peer;
  peer.Open([](void* context, MockGrpcServer& server, const MockRequest& event) noexcept {
    if (event.event != MockRequest::Event::kMessage) return;
    auto& count = static_cast<AuditPeer*>(context)->messages;
    ++count;
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("x-greptime-err-retry-hint", count <= 2 ? "false" : "unknown"));
    std::string bytes;
    if (count % 2 == 1) {
      proto::GreptimeResponse response;
      auto* status = response.mutable_header()->mutable_status();
      status->set_status_code(4000);
      status->set_err_msg("storage unavailable");
      status->set_retry_hint("true");
      ASSERT_TRUE(response.SerializeToString(&bytes));
    } else {
      proto::PromqlResponse response;
      auto* status = response.mutable_header()->mutable_status();
      status->set_status_code(4000);
      status->set_err_msg("storage unavailable");
      status->set_retry_hint("true");
      ASSERT_TRUE(response.SerializeToString(&bytes));
    }
    ASSERT_TRUE(server.Send(event.stream_id, std::as_bytes(std::span{bytes})));
    ASSERT_TRUE(server.Finish(event.stream_id, {}, trailers));
  });
  ASSERT_TRUE(peer.client);
  auto mutation = peer.client->TruncateTable("ticks");
  ASSERT_FALSE(mutation);
  EXPECT_EQ(mutation.error().RawCode(), 4000U);
  EXPECT_EQ(mutation.error().Message(), "storage unavailable");
  EXPECT_FALSE(mutation.error().IsRetryable());
  auto prom = peer.client->PromInstantQuery("up");
  ASSERT_FALSE(prom);
  EXPECT_EQ(prom.error().RawCode(), 4000U);
  EXPECT_FALSE(prom.error().IsRetryable());
  auto unknown_mutation = peer.client->TruncateTable("ticks");
  ASSERT_FALSE(unknown_mutation);
  EXPECT_EQ(unknown_mutation.error().RawCode(), 4000U);
  EXPECT_FALSE(unknown_mutation.error().IsRetryable());
  auto unknown_prom = peer.client->PromInstantQuery("up");
  ASSERT_FALSE(unknown_prom);
  EXPECT_EQ(unknown_prom.error().RawCode(), 4000U);
  EXPECT_FALSE(unknown_prom.error().IsRetryable());
  EXPECT_EQ(peer.messages, 4U);
}

TEST(UnaryClientUnitScenario, ConnectRejectsInvalidOptionsBeforeDialing) {
  auto endpoint = Endpoint::Parse("grpc://127.0.0.1:1");
  ASSERT_TRUE(endpoint);
  for (int invalid = 0; invalid < 7; ++invalid) {
    ClientOptions options(*endpoint);
    if (invalid == 0) options.endpoints.clear();
    if (invalid == 1) options.schema.clear();
    if (invalid == 2) options.connect_timeout = std::chrono::nanoseconds::zero();
    if (invalid == 3) options.request_timeout = std::chrono::nanoseconds(-1);
    if (invalid == 4) options.request_timeout = std::chrono::nanoseconds::max();
    if (invalid == 5) options.schema = " ";
    if (invalid == 6) options.timezone = std::string("bad\0zone", 8);
    auto result = Client::Connect(std::move(options));
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().Kind(), ErrorKind::kInvalidConfig);
  }
}
} // namespace
