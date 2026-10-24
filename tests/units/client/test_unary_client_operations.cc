#include <greptime/v1/health.pb.h>
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/client/client.h>
#include <greptimedb/client/ddl.h>
#include <greptimedb/client/prom.h>
#include <greptimedb/row/reflected_row.h>
#include <greptimedb/transport/mock_grpc_server.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace greptimedb;
namespace proto = greptime::v1;
constexpr std::string_view kHandle = "/greptime.v1.GreptimeDatabase/Handle";
constexpr std::string_view kProm = "/greptime.v1.PrometheusGateway/Handle";
constexpr std::string_view kHealth = "/greptime.v1.HealthCheck/HealthCheck";

static_assert(!std::is_copy_constructible_v<Client>);
static_assert(!std::is_move_constructible_v<Client>);

ClientOptions Options() {
  auto endpoint = Endpoint::Parse("grpc://127.0.0.1:4001");
  EXPECT_TRUE(endpoint);
  ClientOptions options(std::move(*endpoint));
  options.catalog = "trading";
  options.schema = "ticks";
  return options;
}

proto::GreptimeResponse Response(std::uint32_t rows) {
  proto::GreptimeResponse response;
  response.mutable_header()->mutable_status()->set_status_code(0);
  response.mutable_affected_rows()->set_value(rows);
  return response;
}

void CheckHeader(const proto::RequestHeader& header) {
  EXPECT_EQ(header.catalog(), "trading");
  EXPECT_EQ(header.schema(), "ticks");
  EXPECT_EQ(header.dbname(), "trading-ticks");
}

void CheckRows(Result<std::uint32_t> result, std::uint32_t expected) {
  ASSERT_TRUE(result) << result.error().Message();
  EXPECT_EQ(*result, expected);
}

template <typename Batch> void FillTick(Batch& batch) {
  ASSERT_TRUE(batch.AddTagColumn("symbol", ColumnDataType::kString));
  ASSERT_TRUE(batch.AddTimestampColumn("time", TimestampUnit::kMillisecond));
  ASSERT_TRUE(batch.AddFieldColumn("price", ColumnDataType::kFloat64));
  const std::array values{ValueView{std::string_view{"BTC"}},
                          ValueView{TimestampMillisecond{1700000000000}}, ValueView{42.5}};
  ASSERT_TRUE(batch.PushRow(values));
}

// The progress hook advances a real HTTP/2 peer on the SDK caller's thread.
class UnaryPeer {
 public:
  using Handler = void (*)(void*, MockGrpcServer&, const MockRequest&) noexcept;
  std::unique_ptr<MockGrpcServer> server;
  std::unique_ptr<Client> client;
  const std::thread::id owner{std::this_thread::get_id()};
  std::size_t messages{};

  void Open(Handler handler, void* context, ClientOptions options = Options()) {
    auto pipe = MemoryPipe::CreatePair(97);
    ASSERT_TRUE(pipe);
    auto peer = MockGrpcServer::Create(std::move(pipe->second), handler, context);
    ASSERT_TRUE(peer);
    server = std::move(*peer);
    auto opened = Client::FromMemoryPipe(
        std::move(options), std::move(pipe->first), server.get(),
        [](void* pointer) noexcept { return static_cast<MockGrpcServer*>(pointer)->PollOnce(); });
    ASSERT_TRUE(opened);
    client = std::move(*opened);
  }

  bool Message(const MockRequest& request, std::string_view path) {
    if (request.event != MockRequest::Event::kMessage) return false;
    EXPECT_EQ(std::this_thread::get_id(), owner);
    EXPECT_EQ(request.path, path);
    EXPECT_EQ(request.metadata.Get("x-greptime-db-name"), "trading-ticks");
    ++messages;
    return true;
  }

  template <typename Message>
  static void Send(MockGrpcServer& server, const MockRequest& request, const Message& message,
                   grpcxx::Status status = {}, const grpcxx::MetadataMap& trailers = {}) {
    std::string bytes;
    ASSERT_TRUE(message.SerializeToString(&bytes));
    ASSERT_TRUE(server.Send(request.stream_id, std::as_bytes(std::span{bytes})));
    ASSERT_TRUE(server.Finish(request.stream_id, std::move(status), trailers));
  }

  template <typename Message>
  static Message Parse(const MockRequest& request) {
    Message message;
    EXPECT_TRUE(message.ParseFromArray(request.payload.data(),
                                       static_cast<int>(request.payload.size())));
    return message;
  }
};

TEST(UnaryClientIntegrationScenario, IngestionDeletionAndMultiTableUseOneRpc) {
  UnaryPeer peer;
  auto options = Options();
  ASSERT_TRUE(options.write_hints.SetAutoCreateTable(true));
  peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<UnaryPeer*>(opaque);
    if (!self.Message(wire, kHandle)) return;
    const auto index = self.messages - 1;
    const auto request = UnaryPeer::Parse<proto::GreptimeRequest>(wire);
    CheckHeader(request.header());
    EXPECT_EQ(wire.metadata.Get("x-greptime-hints"), index == 1
                  ? "auto_create_table=false,ttl=7d" : "auto_create_table=true");
    std::vector<std::string> tables;
    if (request.has_inserts()) {
      ASSERT_LE(index, 4U);
      ASSERT_EQ(request.inserts().inserts_size(), index >= 3 ? 2 : 1);
      for (const auto& insert : request.inserts().inserts()) {
        tables.push_back(insert.table_name());
        EXPECT_EQ(insert.row_count(), 1U);
        ASSERT_EQ(insert.columns_size(), 3);
        ASSERT_EQ(insert.columns(0).values().string_values_size(), 1);
        EXPECT_EQ(insert.columns(0).values().string_values(0), "BTC");
        ASSERT_EQ(insert.columns(1).values().timestamp_millisecond_values_size(), 1);
        EXPECT_EQ(insert.columns(1).values().timestamp_millisecond_values(0), 1700000000000);
        ASSERT_EQ(insert.columns(2).values().f64_values_size(), 1);
        EXPECT_EQ(insert.columns(2).values().f64_values(0), 42.5);
      }
    } else if (request.has_row_inserts()) {
      ASSERT_TRUE(index == 5 || index == 6);
      ASSERT_EQ(request.row_inserts().inserts_size(), 1);
      const auto& insert = request.row_inserts().inserts(0);
      tables.push_back(insert.table_name());
      ASSERT_TRUE(insert.has_rows());
      EXPECT_EQ(insert.rows().schema_size(), 3);
      ASSERT_EQ(insert.rows().rows_size(), 1);
      EXPECT_EQ(insert.rows().rows(0).values_size(), 3);
    } else if (request.has_deletes()) {
      ASSERT_TRUE(index == 7 || index == 8);
      ASSERT_EQ(request.deletes().deletes_size(), 1);
      const auto& deletion = request.deletes().deletes(0);
      tables.push_back(deletion.table_name());
      ASSERT_EQ(deletion.key_columns_size(), 2);
      EXPECT_EQ(deletion.key_columns(0).column_name(), "symbol");
      EXPECT_EQ(deletion.key_columns(1).column_name(), "time");
    } else {
      ASSERT_EQ(index, 9U);
      ASSERT_TRUE(request.has_row_deletes());
      ASSERT_EQ(request.row_deletes().deletes_size(), 1);
      const auto& deletion = request.row_deletes().deletes(0);
      tables.push_back(deletion.table_name());
      ASSERT_TRUE(deletion.has_rows());
      ASSERT_EQ(deletion.rows().schema_size(), 2);
      EXPECT_EQ(deletion.rows().schema(0).column_name(), "symbol");
      EXPECT_EQ(deletion.rows().schema(1).column_name(), "time");
      ASSERT_EQ(deletion.rows().rows_size(), 1);
      EXPECT_EQ(deletion.rows().rows(0).values_size(), 2);
    }
    const std::vector<std::string> expected = index == 2 || index == 6 || index == 8
        ? std::vector<std::string>{"archive"} : index == 3 || index == 4
        ? std::vector<std::string>{"ticks", "trades"} : std::vector<std::string>{"ticks"};
    EXPECT_EQ(tables, expected);
    UnaryPeer::Send(server, wire, Response(static_cast<std::uint32_t>(expected.size())));
  }, &peer, std::move(options));
  ASSERT_NE(peer.client, nullptr);
  ColumnarBatch first("ticks"), second("trades");
  FillTick(first);
  FillTick(second);
  WriteHints hints;
  ASSERT_TRUE(hints.SetAutoCreateTable(false));
  ASSERT_TRUE(hints.SetTtl("7d"));
  CheckRows(peer.client->InsertBatch(first), 1U);
  CheckRows(peer.client->InsertBatch(first, &hints), 1U);
  CheckRows(peer.client->InsertBatchTo("archive", first), 1U);
  const std::array<const ColumnarBatch*, 2> batches{&first, &second};
  CheckRows(peer.client->InsertBatches(batches), 2U);
  MultiTableBatch multi;
  ASSERT_TRUE(multi.AddBatch(first));
  ASSERT_TRUE(multi.AddBatch(second));
  CheckRows(peer.client->InsertMultiBatch(multi), 2U);
  RowBatch rows("ticks");
  FillTick(rows);
  CheckRows(peer.client->InsertRows(rows), 1U);
  CheckRows(peer.client->InsertRowsTo("archive", rows), 1U);
  CheckRows(peer.client->DeleteBatch(first), 1U);
  CheckRows(peer.client->DeleteBatchFrom("archive", first), 1U);
  CheckRows(peer.client->DeleteRows(rows), 1U);
  EXPECT_EQ(peer.messages, 10U);
}

TEST(UnaryClientIntegrationScenario, AllDdlVariantsPreserveExpressionsAndExplicitScopes) {
  struct State {
    UnaryPeer peer;
    std::vector<proto::DdlRequest> expected;
  } state;
  auto add = [&]() -> proto::DdlRequest& { return state.expected.emplace_back(); };
  auto* database = add().mutable_create_database();
  database->set_catalog_name("trading");
  database->set_schema_name("archive");
  database->set_create_if_not_exists(true);
  (*database->mutable_options())["ttl"] = "7d";
  auto* alter_database = add().mutable_alter_database();
  alter_database->set_catalog_name("foreign_catalog");
  alter_database->set_schema_name("foreign_schema");
  alter_database->mutable_unset_database_options()->add_keys("ttl");
  auto* table = add().mutable_create_table();
  table->set_catalog_name("trading");
  table->set_schema_name("ticks");
  table->set_table_name("ticks");
  table->set_engine("mito");
  auto* alter_table = add().mutable_alter_table();
  alter_table->set_catalog_name("foreign_catalog");
  alter_table->set_schema_name("foreign_schema");
  alter_table->set_table_name("ticks");
  alter_table->mutable_rename_table()->set_new_table_name("renamed_ticks");
  auto* drop_table = add().mutable_drop_table();
  drop_table->set_catalog_name("trading");
  drop_table->set_schema_name("ticks");
  drop_table->set_table_name("ticks");
  drop_table->set_drop_if_exists(true);
  auto* truncate = add().mutable_truncate_table();
  truncate->set_catalog_name("trading");
  truncate->set_schema_name("ticks");
  truncate->set_table_name("ticks");
  auto* view = add().mutable_create_view();
  view->set_view_name("prices");
  view->set_definition("SELECT price FROM ticks");
  auto* drop_view = add().mutable_drop_view();
  drop_view->set_catalog_name("trading");
  drop_view->set_schema_name("ticks");
  drop_view->set_view_name("prices");
  drop_view->set_drop_if_exists(true);
  auto* flow = add().mutable_create_flow();
  flow->set_flow_name("aggregate");
  flow->set_sql("SELECT avg(price) FROM ticks");
  auto* drop_flow = add().mutable_drop_flow();
  drop_flow->set_catalog_name("trading");
  drop_flow->set_flow_name("aggregate");
  drop_flow->set_drop_if_exists(true);
  auto* comment = add().mutable_comment_on();
  comment->set_catalog_name("foreign_catalog");
  comment->set_schema_name("foreign_schema");
  comment->set_object_name("ticks");
  comment->set_comment("Historical quotes in USD");
  const auto create = state.expected[2];
  state.expected.push_back(create);
  state.peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<State*>(opaque);
    if (!self.peer.Message(wire, kHandle)) return;
    const auto request = UnaryPeer::Parse<proto::GreptimeRequest>(wire);
    ASSERT_TRUE(request.has_header());
    CheckHeader(request.header());
    ASSERT_TRUE(request.has_ddl());
    ASSERT_LE(self.peer.messages, self.expected.size());
    EXPECT_EQ(request.ddl().SerializeAsString(),
              self.expected[self.peer.messages - 1].SerializeAsString());
    UnaryPeer::Send(server, wire, Response(static_cast<std::uint32_t>(self.peer.messages)));
  }, &state);
  ASSERT_NE(state.peer.client, nullptr);
  auto& client = *state.peer.client;
  CheckRows(client.CreateDatabase("archive", true, {{"ttl", "7d"}}), 1U);
  CheckRows(client.AlterDatabase(state.expected[1].alter_database()), 2U);
  CheckRows(client.CreateTable(state.expected[2].create_table()), 3U);
  CheckRows(client.AlterTable(state.expected[3].alter_table()), 4U);
  CheckRows(client.DropTable("ticks", true), 5U);
  CheckRows(client.TruncateTable("ticks"), 6U);
  CheckRows(client.CreateView(state.expected[6].create_view()), 7U);
  CheckRows(client.DropView("prices", true), 8U);
  CheckRows(client.CreateFlow(state.expected[8].create_flow()), 9U);
  CheckRows(client.DropFlow("aggregate", true), 10U);
  CheckRows(client.CommentOn(state.expected[10].comment_on()), 11U);
  CheckRows(client.ExecuteDdl(state.expected[11]), 12U);
  EXPECT_EQ(state.peer.messages, 12U);
}

TEST(UnaryClientIntegrationScenario, HealthPromqlAndAuthenticatedMetadata) {
  struct State { UnaryPeer peer; std::size_t health{}, prom{}; } state;
  auto options = Options();
  options.auth = Auth::Basic("user", "pass");
  state.peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<State*>(opaque);
    if (wire.event != MockRequest::Event::kMessage) return;
    EXPECT_EQ(wire.metadata.Get("x-greptime-auth"), "Basic dXNlcjpwYXNz");
    if (wire.path == kHealth) {
      ASSERT_TRUE(self.peer.Message(wire, kHealth));
      ++self.health;
      UnaryPeer::Send(server, wire, proto::HealthCheckResponse{});
      return;
    }
    ASSERT_TRUE(self.peer.Message(wire, kProm));
    const auto request = UnaryPeer::Parse<proto::PromqlRequest>(wire);
    CheckHeader(request.header());
    EXPECT_EQ(request.header().authorization().basic().username(), "user");
    EXPECT_EQ(request.header().authorization().basic().password(), "pass");
    if (self.prom++ == 0) {
      ASSERT_TRUE(request.has_instant_query());
      EXPECT_EQ(request.instant_query().query(), "up{job=\"quotes\"}");
      EXPECT_EQ(request.instant_query().time(), "1700000000");
      EXPECT_EQ(request.instant_query().lookback(), "5m");
    } else {
      ASSERT_TRUE(request.has_range_query());
      EXPECT_EQ(request.range_query().query(), "rate(trades[1m])");
      EXPECT_EQ(request.range_query().start(), "1700000000");
      EXPECT_EQ(request.range_query().end(), "1700000060");
      EXPECT_EQ(request.range_query().step(), "5s");
      EXPECT_EQ(request.range_query().lookback(), "10m");
    }
    proto::PromqlResponse response;
    *response.mutable_header() = Response(0).header();
    response.set_body(R"({"status":"success","data":{"resultType":"vector","result":[]}})");
    UnaryPeer::Send(server, wire, response);
  }, &state, std::move(options));
  ASSERT_NE(state.peer.client, nullptr);
  ASSERT_TRUE(state.peer.client->Ping());
  auto report = state.peer.client->ProbeHealth();
  ASSERT_TRUE(report);
  EXPECT_TRUE(report->healthy);
  EXPECT_EQ(report->endpoint, "grpc://127.0.0.1:4001");
  EXPECT_GE(report->latency, std::chrono::nanoseconds::zero());
  EXPECT_LT(report->latency, std::chrono::seconds(30));
  const auto instant = state.peer.client->PromInstantQuery("up{job=\"quotes\"}", "1700000000", "5m");
  ASSERT_TRUE(instant);
  const auto range = state.peer.client->PromRangeQuery("rate(trades[1m])", "1700000000",
                                                     "1700000060", "5s", "10m");
  ASSERT_TRUE(range);
  EXPECT_EQ(instant->body, range->body);
  const auto json = instant->AsJson();
  ASSERT_TRUE(json);
  EXPECT_EQ(*json,
            R"({"status":"success","data":{"resultType":"vector","result":[]}})");
  EXPECT_EQ(state.health, 2U);
  EXPECT_EQ(state.prom, 2U);
}
TEST(UnaryClientIntegrationScenario, LayeredErrorsAndUnaryCardinalityRejectFalseSuccess) {
  UnaryPeer peer;
  peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<UnaryPeer*>(opaque);
    if (!self.Message(wire, kHandle)) return;
    auto response = Response(0);
    switch (self.messages) {
      case 1: {
        auto* status = response.mutable_header()->mutable_status();
        status->set_status_code(4000);
        status->set_err_msg("storage unavailable");
        status->set_retry_hint("true");
        UnaryPeer::Send(server, wire, response);
        break;
      }
      case 2: {
        grpcxx::MetadataMap trailers;
        ASSERT_TRUE(trailers.Add("inner_error_msg", R"({"code":3001,"err_msg":"ticks missing"})"));
        ASSERT_TRUE(trailers.Add("x-greptime-err-retry-hint", "false"));
        ASSERT_TRUE(server.Finish(wire.stream_id,
                                 grpcxx::Status{grpcxx::StatusCode::kUnavailable, "outer status"},
                                 trailers));
        break;
      }
      case 3: {
        const auto bytes = response.SerializeAsString();
        ASSERT_TRUE(server.Send(wire.stream_id, std::as_bytes(std::span{bytes})));
        UnaryPeer::Send(server, wire, response);
        break;
      }
      case 4:
        ASSERT_TRUE(server.Finish(wire.stream_id));
        break;
      case 5:
        response.clear_response();
        UnaryPeer::Send(server, wire, response);
        break;
      case 6:
        UnaryPeer::Send(server, wire, response);
        break;
      case 7: {
        constexpr std::array malformed{std::byte{0x0a}, std::byte{0xff}};
        ASSERT_TRUE(server.Send(wire.stream_id, malformed));
        ASSERT_TRUE(server.Finish(wire.stream_id));
        break;
      }
      case 8: {
        grpcxx::MetadataMap trailers;
        ASSERT_TRUE(trailers.Add("x-greptime-err-code", "2001"));
        ASSERT_TRUE(trailers.Add("x-greptime-err-msg", "invalid plan"));
        UnaryPeer::Send(server, wire, response, {}, trailers);
        break;
      }
      case 9:
        UnaryPeer::Send(server, wire, Response(17));
        break;
      default:
        FAIL() << "unexpected replay";
    }
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  auto call = [&] { return peer.client->TruncateTable("ticks"); };
  auto error = call();
  ASSERT_FALSE(error);
  EXPECT_EQ(error.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(error.error().RawCode(), 4000U);
  EXPECT_EQ(error.error().Message(), "storage unavailable");
  EXPECT_TRUE(error.error().IsRetryable());
  error = call();
  ASSERT_FALSE(error);
  EXPECT_EQ(error.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(error.error().RawCode(), 3001U);
  EXPECT_EQ(error.error().Message(), "ticks missing");
  EXPECT_FALSE(error.error().IsRetryable());
  for (int index = 0; index != 3; ++index) {
    SCOPED_TRACE(index);
    auto invalid = call();
    ASSERT_FALSE(invalid);
    if (index == 2) {
      EXPECT_EQ(invalid.error().Kind(), ErrorKind::kProtocol);
    } else {
      EXPECT_EQ(invalid.error().Kind(), ErrorKind::kGrpc);
      EXPECT_EQ(invalid.error().GrpcCode(), index == 0
                    ? grpcxx::StatusCode::kFailedPrecondition : grpcxx::StatusCode::kInternal);
    }
  }
  auto zero = call();
  ASSERT_TRUE(zero);
  EXPECT_EQ(*zero, 0U);
  auto malformed = call();
  ASSERT_FALSE(malformed);
  EXPECT_EQ(malformed.error().Kind(), ErrorKind::kDecode);
  auto trailer_error = call();
  ASSERT_FALSE(trailer_error);
  EXPECT_EQ(trailer_error.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(trailer_error.error().RawCode(), 2001U);
  auto recovered = call();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(*recovered, 17U);
  EXPECT_EQ(peer.messages, 9U);
}

TEST(UnaryClientIntegrationScenario, PromqlErrorsOverrideBodyAndEmptyFieldsSurvive) {
  UnaryPeer peer;
  peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<UnaryPeer*>(opaque);
    if (!self.Message(wire, kProm)) return;
    const auto request = UnaryPeer::Parse<proto::PromqlRequest>(wire);
    proto::PromqlResponse response;
    *response.mutable_header() = Response(0).header();
    response.set_body(R"({"status":"success"})");
    if (self.messages == 1) {
      auto* status = response.mutable_header()->mutable_status();
      status->set_status_code(2001);
      status->set_err_msg("invalid plan");
      status->set_retry_hint("false");
      UnaryPeer::Send(server, wire, response);
    } else if (self.messages == 2) {
      grpcxx::MetadataMap trailers;
      ASSERT_TRUE(trailers.Add("inner_error_msg",
                              R"({"code":2002,"err_msg":"query execution failed"})"));
      ASSERT_TRUE(trailers.Add("x-greptime-err-retry-hint", "true"));
      UnaryPeer::Send(server, wire, response,
                      grpcxx::Status{grpcxx::StatusCode::kInternal, "remote failure"}, trailers);
    } else {
      if (self.messages == 3) {
        ASSERT_TRUE(request.has_instant_query());
        EXPECT_EQ(request.instant_query().SerializeAsString(), proto::PromInstantQuery{}.SerializeAsString());
      } else {
        EXPECT_EQ(self.messages, 4U);
        ASSERT_TRUE(request.has_range_query());
        EXPECT_EQ(request.range_query().SerializeAsString(), proto::PromRangeQuery{}.SerializeAsString());
      }
      response.clear_body();
      UnaryPeer::Send(server, wire, response);
    }
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  auto instant_error = peer.client->PromInstantQuery("up", "", "");
  ASSERT_FALSE(instant_error);
  EXPECT_EQ(instant_error.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(instant_error.error().RawCode(), 2001U);
  EXPECT_EQ(instant_error.error().RetryHint(), false);
  auto range_error = peer.client->PromRangeQuery("up", "", "", "", "");
  ASSERT_FALSE(range_error);
  EXPECT_EQ(range_error.error().RawCode(), 2002U);
  EXPECT_EQ(range_error.error().Message(), "query execution failed");
  EXPECT_EQ(range_error.error().RetryHint(), true);
  auto empty_instant = peer.client->PromInstantQuery("", "", "");
  ASSERT_TRUE(empty_instant);
  const auto empty_json = empty_instant->AsJson();
  ASSERT_TRUE(empty_json);
  EXPECT_TRUE(empty_json->empty());
  auto empty_range = peer.client->PromRangeQuery("", "", "", "", "");
  ASSERT_TRUE(empty_range);
  EXPECT_TRUE(empty_range->body.empty());
  PromQueryResult invalid;
  invalid.body = std::string(1, static_cast<char>(0xff));
  auto invalid_json = invalid.AsJson();
  ASSERT_FALSE(invalid_json);
  EXPECT_EQ(invalid_json.error().kind, greptimedb::client::PromErrorKind::kInvalidUtf8);
  EXPECT_EQ(peer.messages, 4U);
}

struct [[= greptimedb::table{"typed_ticks"}]] TypedTick {
  [[= greptimedb::tag{}]] std::string symbol;
  [[= greptimedb::timestamp{.unit = TimestampUnit::kMillisecond}]] std::int64_t time;
  [[= greptimedb::field{}]] double price;
};

TEST(UnaryClientIntegrationScenario, TypedRowsUseColumnarWireAndExplicitDestination) {
  UnaryPeer peer;
  peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<UnaryPeer*>(opaque);
    if (!self.Message(wire, kHandle)) return;
    const auto request = UnaryPeer::Parse<proto::GreptimeRequest>(wire);
    ASSERT_TRUE(request.has_inserts());
    ASSERT_EQ(request.inserts().inserts_size(), 1);
    const auto& insert = request.inserts().inserts(0);
    EXPECT_EQ(insert.table_name(), self.messages == 1 ? "typed_ticks" : "explicit_ticks");
    EXPECT_EQ(insert.row_count(), 2U);
    ASSERT_EQ(insert.columns_size(), 3);
    ASSERT_EQ(insert.columns(0).values().string_values_size(), 2);
    EXPECT_EQ(insert.columns(0).values().string_values(0), "BTC");
    EXPECT_EQ(insert.columns(0).values().string_values(1), "ETH");
    ASSERT_EQ(insert.columns(1).values().timestamp_millisecond_values_size(), 2);
    EXPECT_EQ(insert.columns(1).values().timestamp_millisecond_values(0), 1700000000000);
    EXPECT_EQ(insert.columns(1).values().timestamp_millisecond_values(1), 1700000000001);
    ASSERT_EQ(insert.columns(2).values().f64_values_size(), 2);
    EXPECT_EQ(insert.columns(2).values().f64_values(0), 42.5);
    EXPECT_EQ(insert.columns(2).values().f64_values(1), 18.25);
    UnaryPeer::Send(server, wire, Response(2));
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  const std::array rows{TypedTick{"BTC", 1700000000000, 42.5},
                        TypedTick{"ETH", 1700000000001, 18.25}};
  const std::array invalid_tables{std::string_view{}, std::string_view{" \t\n"},
                                   std::string_view{"bad\0table", 9}};
  for (const auto table : invalid_tables) {
    auto rejected = peer.client->InsertTo<TypedTick>(table, std::span<const TypedTick>{rows});
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().Kind(), ErrorKind::kSchemaViolation);
  }
  CheckRows(peer.client->Insert<TypedTick>(std::span<const TypedTick>{rows}), 2U);
  CheckRows(peer.client->InsertTo<TypedTick>("explicit_ticks", std::span<const TypedTick>{rows}), 2U);
  EXPECT_EQ(peer.messages, 2U);
}

TEST(UnaryClientIntegrationScenario, HealthFailureAndDeadlineLeaveTheClientUsable) {
  UnaryPeer peer;
  auto options = Options();
  options.request_timeout = std::chrono::milliseconds(10);
  peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<UnaryPeer*>(opaque);
    if (!self.Message(wire, kHealth)) return;
    if (self.messages == 1) {
      ASSERT_TRUE(server.Finish(wire.stream_id,
                               grpcxx::Status{grpcxx::StatusCode::kUnavailable, "unhealthy"}));
    } else if (self.messages != 2) {
      UnaryPeer::Send(server, wire, proto::HealthCheckResponse{});
    }
  }, &peer, std::move(options));
  ASSERT_NE(peer.client, nullptr);
  auto unavailable = peer.client->Ping();
  ASSERT_FALSE(unavailable);
  EXPECT_EQ(unavailable.error().GrpcCode(), grpcxx::StatusCode::kUnavailable);
  auto deadline = peer.client->ProbeHealth();
  ASSERT_FALSE(deadline);
  EXPECT_EQ(deadline.error().GrpcCode(), grpcxx::StatusCode::kDeadlineExceeded);
  EXPECT_TRUE(peer.client->Ping());
  auto healthy = peer.client->ProbeHealth();
  ASSERT_TRUE(healthy);
  EXPECT_TRUE(healthy->healthy);
  EXPECT_EQ(peer.messages, 4U);
}

TEST(UnaryClientIntegrationScenario, SecondBatchEncodingFailureSendsNoPartialRpc) {
  struct State { UnaryPeer peer; std::size_t writes{}, health{}; } state;
  state.peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<State*>(opaque);
    if (wire.event != MockRequest::Event::kMessage) return;
    if (wire.path == kHandle) {
      ASSERT_TRUE(self.peer.Message(wire, kHandle));
      ++self.writes;
      UnaryPeer::Send(server, wire, Response(1));
    } else {
      ASSERT_TRUE(self.peer.Message(wire, kHealth));
      ++self.health;
      UnaryPeer::Send(server, wire, proto::HealthCheckResponse{});
    }
  }, &state);
  ASSERT_NE(state.peer.client, nullptr);
  ColumnarBatch valid("ticks"), invalid("rejected");
  FillTick(valid);
  ASSERT_TRUE(invalid.AddFieldColumn("price", ColumnDataType::kFloat64));
  const std::array values{ValueView{42.5}};
  ASSERT_TRUE(invalid.PushResultRow(values));
  ASSERT_FALSE(invalid.ValidateSchema());
  const std::array<const ColumnarBatch*, 2> batches{&valid, &invalid};
  auto rejected = state.peer.client->InsertBatches(batches);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Kind(), ErrorKind::kSchemaViolation);
  ASSERT_TRUE(state.peer.client->Ping());
  EXPECT_EQ(state.writes, 0U);
  EXPECT_EQ(state.health, 1U);
}

TEST(UnaryClientIntegrationScenario, InitialAndTrailingDiagnosticsCombineWithoutGrantingRetry) {
  UnaryPeer peer;
  peer.Open([](void* opaque, MockGrpcServer& server, const MockRequest& wire) noexcept {
    auto& self = *static_cast<UnaryPeer*>(opaque);
    if (!self.Message(wire, kHandle)) return;
    grpcxx::MetadataMap initial;
    grpcxx::MetadataMap trailers;
    if (self.messages <= 2) {
      ASSERT_TRUE(initial.Add("x-greptime-err-code", "4000"));
      ASSERT_TRUE(initial.Add("x-greptime-err-msg", "storage unavailable"));
      if (self.messages == 2) {
        ASSERT_TRUE(initial.Add("x-greptime-err-retry-hint", "false"));
      }
      ASSERT_TRUE(trailers.Add("x-greptime-err-retry-hint", "true"));
      ASSERT_TRUE(server.SetInitialMetadata(wire.stream_id, initial));
      ASSERT_TRUE(server.Finish(wire.stream_id,
                               grpcxx::Status{grpcxx::StatusCode::kUnavailable},
                               trailers));
    } else if (self.messages == 3) {
      ASSERT_TRUE(initial.Add("inner_error_msg",
                              R"({"code":3001,"err_msg":"initial metadata table missing"})"));
      ASSERT_TRUE(server.SetInitialMetadata(wire.stream_id, initial));
      UnaryPeer::Send(server, wire, Response(99));
    } else {
      EXPECT_EQ(self.messages, 4U);
      UnaryPeer::Send(server, wire, Response(3));
    }
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  auto split = peer.client->TruncateTable("ticks");
  ASSERT_FALSE(split);
  EXPECT_EQ(split.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(split.error().RawCode(), 4000U);
  EXPECT_EQ(split.error().Message(), "storage unavailable");
  EXPECT_TRUE(split.error().IsRetryable());
  auto conflicting_retry = peer.client->TruncateTable("ticks");
  ASSERT_FALSE(conflicting_retry);
  EXPECT_EQ(conflicting_retry.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(conflicting_retry.error().RawCode(), 4000U);
  EXPECT_EQ(conflicting_retry.error().Message(), "storage unavailable");
  EXPECT_FALSE(conflicting_retry.error().IsRetryable());
  auto initial_error = peer.client->TruncateTable("ticks");
  ASSERT_FALSE(initial_error);
  EXPECT_EQ(initial_error.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(initial_error.error().RawCode(), 3001U);
  EXPECT_EQ(initial_error.error().Message(), "initial metadata table missing");
  EXPECT_FALSE(initial_error.error().IsRetryable());
  CheckRows(peer.client->TruncateTable("ticks"), 3U);
  EXPECT_EQ(peer.messages, 4U);
}

TEST(UnaryClientUnitScenario, FactoriesRejectInvalidOptionsMissingChannelAndClosedPipe) {
  auto missing_channel = Client::FromChannel(Options(), nullptr);
  ASSERT_FALSE(missing_channel);
  EXPECT_EQ(missing_channel.error().Kind(), ErrorKind::kInvalidConfig);

  auto pipe = MemoryPipe::CreatePair();
  ASSERT_TRUE(pipe);
  auto channel = SynchronousChannel::Create(std::move(pipe->first));
  ASSERT_TRUE(channel);
  auto invalid_options = Client::FromChannel(ClientOptions{}, std::move(*channel));
  ASSERT_FALSE(invalid_options);
  EXPECT_EQ(invalid_options.error().Kind(), ErrorKind::kInvalidConfig);

  auto closed = MemoryPipe::CreatePair();
  ASSERT_TRUE(closed);
  closed->first->Close();
  auto closed_pipe = Client::FromMemoryPipe(Options(), std::move(closed->first));
  ASSERT_FALSE(closed_pipe);
  EXPECT_EQ(closed_pipe.error().Kind(), ErrorKind::kGrpc);
  EXPECT_EQ(closed_pipe.error().GrpcCode(), grpcxx::StatusCode::kInvalidArgument);

  auto unconfigured = MemoryPipe::CreatePair();
  ASSERT_TRUE(unconfigured);
  auto missing_endpoint = Client::FromMemoryPipe(ClientOptions{}, std::move(unconfigured->first));
  ASSERT_FALSE(missing_endpoint);
  EXPECT_EQ(missing_endpoint.error().Kind(), ErrorKind::kInvalidConfig);
}
} // namespace
