#include "test_unary_client_operations_support.h"

namespace {
TEST(UnaryClientIntegrationScenario, IngestionDeletionAndMultiTableUseOneRpc) {
  UnaryPeer peer;
  auto options = Options();
  ASSERT_TRUE(options.write_hints.SetAutoCreateTable(true));
  peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<UnaryPeer *>(opaque);
        if (!self.Message(wire, kHandle)) return;
        const auto index = self.messages - 1;
        const auto request = UnaryPeer::Parse<proto::GreptimeRequest>(wire);
        CheckHeader(request.header());
        EXPECT_EQ(wire.metadata.Get("x-greptime-hints"),
                  index == 1 ? "auto_create_table=false,ttl=7d" : "auto_create_table=true");
        std::vector<std::string> tables;
        if (request.has_inserts()) {
          ASSERT_LE(index, 4U);
          ASSERT_EQ(request.inserts().inserts_size(), index >= 3 ? 2 : 1);
          for (const auto &insert : request.inserts().inserts()) {
            tables.push_back(insert.table_name());
            EXPECT_EQ(insert.row_count(), 1U);
            ASSERT_EQ(insert.columns_size(), 3);
            ASSERT_EQ(insert.columns(0).values().string_values_size(), 1);
            EXPECT_EQ(insert.columns(0).values().string_values(0), "srv");
            ASSERT_EQ(insert.columns(1).values().timestamp_millisecond_values_size(), 1);
            EXPECT_EQ(insert.columns(1).values().timestamp_millisecond_values(0), 1700000000000);
            ASSERT_EQ(insert.columns(2).values().f64_values_size(), 1);
            EXPECT_EQ(insert.columns(2).values().f64_values(0), 42.5);
          }
        } else if (request.has_row_inserts()) {
          ASSERT_TRUE(index == 5 || index == 6);
          ASSERT_EQ(request.row_inserts().inserts_size(), 1);
          const auto &insert = request.row_inserts().inserts(0);
          tables.push_back(insert.table_name());
          ASSERT_TRUE(insert.has_rows());
          EXPECT_EQ(insert.rows().schema_size(), 3);
          ASSERT_EQ(insert.rows().rows_size(), 1);
          EXPECT_EQ(insert.rows().rows(0).values_size(), 3);
        } else if (request.has_deletes()) {
          ASSERT_TRUE(index == 7 || index == 8);
          ASSERT_EQ(request.deletes().deletes_size(), 1);
          const auto &deletion = request.deletes().deletes(0);
          tables.push_back(deletion.table_name());
          ASSERT_EQ(deletion.key_columns_size(), 2);
          EXPECT_EQ(deletion.key_columns(0).column_name(), "host");
          EXPECT_EQ(deletion.key_columns(1).column_name(), "time");
        } else {
          ASSERT_EQ(index, 9U);
          ASSERT_TRUE(request.has_row_deletes());
          ASSERT_EQ(request.row_deletes().deletes_size(), 1);
          const auto &deletion = request.row_deletes().deletes(0);
          tables.push_back(deletion.table_name());
          ASSERT_TRUE(deletion.has_rows());
          ASSERT_EQ(deletion.rows().schema_size(), 2);
          EXPECT_EQ(deletion.rows().schema(0).column_name(), "host");
          EXPECT_EQ(deletion.rows().schema(1).column_name(), "time");
          ASSERT_EQ(deletion.rows().rows_size(), 1);
          EXPECT_EQ(deletion.rows().rows(0).values_size(), 2);
        }
        const std::vector<std::string> expected =
            index == 2 || index == 6 || index == 8 ? std::vector<std::string>{"archive"}
            : index == 3 || index == 4             ? std::vector<std::string>{"samples", "cpu_metrics"}
                                                   : std::vector<std::string>{"samples"};
        EXPECT_EQ(tables, expected);
        UnaryPeer::Send(server, wire, Response(static_cast<std::uint32_t>(expected.size())));
      },
      &peer, std::move(options));
  ASSERT_NE(peer.client, nullptr);
  ColumnarBatch first("samples"), second("cpu_metrics");
  FillMetric(first);
  FillMetric(second);
  WriteHints hints;
  ASSERT_TRUE(hints.SetAutoCreateTable(false));
  ASSERT_TRUE(hints.SetTtl("7d"));
  CheckRows(peer.client->InsertBatch(first), 1U);
  CheckRows(peer.client->InsertBatch(first, &hints), 1U);
  CheckRows(peer.client->InsertBatchTo("archive", first), 1U);
  const std::array<const ColumnarBatch *, 2> batches{&first, &second};
  CheckRows(peer.client->InsertBatches(batches), 2U);
  MultiTableBatch multi;
  ASSERT_TRUE(multi.AddBatch(first));
  ASSERT_TRUE(multi.AddBatch(second));
  CheckRows(peer.client->InsertMultiBatch(multi), 2U);
  RowBatch rows("samples");
  FillMetric(rows);
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
  auto add = [&]() -> proto::DdlRequest & { return state.expected.emplace_back(); };
  auto *database = add().mutable_create_database();
  database->set_catalog_name("observability");
  database->set_schema_name("archive");
  database->set_create_if_not_exists(true);
  (*database->mutable_options())["ttl"] = "7d";
  auto *alter_database = add().mutable_alter_database();
  alter_database->set_catalog_name("foreign_catalog");
  alter_database->set_schema_name("foreign_schema");
  alter_database->mutable_unset_database_options()->add_keys("ttl");
  auto *table = add().mutable_create_table();
  table->set_catalog_name("observability");
  table->set_schema_name("samples");
  table->set_table_name("samples");
  table->set_engine("mito");
  auto *alter_table = add().mutable_alter_table();
  alter_table->set_catalog_name("foreign_catalog");
  alter_table->set_schema_name("foreign_schema");
  alter_table->set_table_name("samples");
  alter_table->mutable_rename_table()->set_new_table_name("renamed_samples");
  auto *drop_table = add().mutable_drop_table();
  drop_table->set_catalog_name("observability");
  drop_table->set_schema_name("samples");
  drop_table->set_table_name("samples");
  drop_table->set_drop_if_exists(true);
  auto *truncate = add().mutable_truncate_table();
  truncate->set_catalog_name("observability");
  truncate->set_schema_name("samples");
  truncate->set_table_name("samples");
  auto *view = add().mutable_create_view();
  view->set_view_name("metric_values");
  view->set_definition("SELECT value FROM samples");
  auto *drop_view = add().mutable_drop_view();
  drop_view->set_catalog_name("observability");
  drop_view->set_schema_name("samples");
  drop_view->set_view_name("metric_values");
  drop_view->set_drop_if_exists(true);
  auto *flow = add().mutable_create_flow();
  flow->set_flow_name("aggregate");
  flow->set_sql("SELECT avg(value) FROM samples");
  auto *drop_flow = add().mutable_drop_flow();
  drop_flow->set_catalog_name("observability");
  drop_flow->set_flow_name("aggregate");
  drop_flow->set_drop_if_exists(true);
  auto *comment = add().mutable_comment_on();
  comment->set_catalog_name("foreign_catalog");
  comment->set_schema_name("foreign_schema");
  comment->set_object_name("samples");
  comment->set_comment("System CPU utilization metrics");
  const auto create = state.expected[2];
  state.expected.push_back(create);
  state.peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<State *>(opaque);
        if (!self.peer.Message(wire, kHandle)) return;
        const auto request = UnaryPeer::Parse<proto::GreptimeRequest>(wire);
        ASSERT_TRUE(request.has_header());
        CheckHeader(request.header());
        ASSERT_TRUE(request.has_ddl());
        ASSERT_LE(self.peer.messages, self.expected.size());
        EXPECT_EQ(request.ddl().SerializeAsString(), self.expected[self.peer.messages - 1].SerializeAsString());
        UnaryPeer::Send(server, wire, Response(static_cast<std::uint32_t>(self.peer.messages)));
      },
      &state);
  ASSERT_NE(state.peer.client, nullptr);
  auto &client = *state.peer.client;
  CheckRows(client.CreateDatabase("archive", true, {{"ttl", "7d"}}), 1U);
  CheckRows(client.AlterDatabase(state.expected[1].alter_database()), 2U);
  CheckRows(client.CreateTable(state.expected[2].create_table()), 3U);
  CheckRows(client.AlterTable(state.expected[3].alter_table()), 4U);
  CheckRows(client.DropTable("samples", true), 5U);
  CheckRows(client.TruncateTable("samples"), 6U);
  CheckRows(client.CreateView(state.expected[6].create_view()), 7U);
  CheckRows(client.DropView("metric_values", true), 8U);
  CheckRows(client.CreateFlow(state.expected[8].create_flow()), 9U);
  CheckRows(client.DropFlow("aggregate", true), 10U);
  CheckRows(client.CommentOn(state.expected[10].comment_on()), 11U);
  CheckRows(client.ExecuteDdl(state.expected[11]), 12U);
  EXPECT_EQ(state.peer.messages, 12U);
}

TEST(UnaryClientIntegrationScenario, HealthPromqlAndAuthenticatedMetadata) {
  struct State {
    UnaryPeer peer;
    std::size_t health{}, prom{};
  } state;
  auto options = Options();
  options.auth = Auth::Basic("user", "pass");
  state.peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<State *>(opaque);
        if (wire.event != grpcxx::ServerRequest::Event::kMessage) return;
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
          EXPECT_EQ(request.instant_query().query(), "up{job=\"metrics\"}");
          EXPECT_EQ(request.instant_query().time(), "1700000000");
          EXPECT_EQ(request.instant_query().lookback(), "5m");
        } else {
          ASSERT_TRUE(request.has_range_query());
          EXPECT_EQ(request.range_query().query(), "rate(cpu_metrics[1m])");
          EXPECT_EQ(request.range_query().start(), "1700000000");
          EXPECT_EQ(request.range_query().end(), "1700000060");
          EXPECT_EQ(request.range_query().step(), "5s");
          EXPECT_EQ(request.range_query().lookback(), "10m");
        }
        proto::PromqlResponse response;
        *response.mutable_header() = Response(0).header();
        response.set_body(R"({"status":"success","data":{"resultType":"vector","result":[]}})");
        UnaryPeer::Send(server, wire, response);
      },
      &state, std::move(options));
  ASSERT_NE(state.peer.client, nullptr);
  ASSERT_TRUE(state.peer.client->Ping());
  auto report = state.peer.client->ProbeHealth();
  ASSERT_TRUE(report);
  EXPECT_TRUE(report->healthy);
  EXPECT_EQ(report->endpoint, "grpc://127.0.0.1:4001");
  EXPECT_GE(report->latency, std::chrono::nanoseconds::zero());
  EXPECT_LT(report->latency, std::chrono::seconds(30));
  const auto instant = state.peer.client->PromInstantQuery("up{job=\"metrics\"}", "1700000000", "5m");
  ASSERT_TRUE(instant);
  const auto range =
      state.peer.client->PromRangeQuery("rate(cpu_metrics[1m])", "1700000000", "1700000060", "5s", "10m");
  ASSERT_TRUE(range);
  EXPECT_EQ(instant->body, range->body);
  const auto json = instant->AsJson();
  ASSERT_TRUE(json);
  EXPECT_EQ(*json, R"({"status":"success","data":{"resultType":"vector","result":[]}})");
  EXPECT_EQ(state.health, 2U);
  EXPECT_EQ(state.prom, 2U);
}
TEST(UnaryClientIntegrationScenario, LayeredErrorsAndUnaryCardinalityRejectFalseSuccess) {
  UnaryPeer peer;
  peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<UnaryPeer *>(opaque);
        if (!self.Message(wire, kHandle)) return;
        auto response = Response(0);
        switch (self.messages) {
        case 1: {
          auto *status = response.mutable_header()->mutable_status();
          status->set_status_code(4000);
          status->set_err_msg("storage unavailable");
          status->set_retry_hint("true");
          UnaryPeer::Send(server, wire, response);
          break;
        }
        case 2: {
          grpcxx::MetadataMap trailers;
          ASSERT_TRUE(trailers.Add("inner_error_msg", R"({"code":3001,"err_msg":"samples missing"})"));
          ASSERT_TRUE(trailers.Add("x-greptime-err-retry-hint", "false"));
          ASSERT_TRUE(server.Finish(wire.stream_id, grpcxx::Status{grpcxx::StatusCode::kUnavailable, "outer status"},
                                    trailers));
          break;
        }
        case 3: {
          const auto bytes = response.SerializeAsString();
          ASSERT_TRUE(server.Send(wire.stream_id, std::as_bytes(std::span{bytes})));
          UnaryPeer::Send(server, wire, response);
          break;
        }
        case 4: ASSERT_TRUE(server.Finish(wire.stream_id, {})); break;
        case 5:
          response.clear_response();
          UnaryPeer::Send(server, wire, response);
          break;
        case 6: UnaryPeer::Send(server, wire, response); break;
        case 7: {
          constexpr std::array malformed{std::byte{0x0a}, std::byte{0xff}};
          ASSERT_TRUE(server.Send(wire.stream_id, malformed));
          ASSERT_TRUE(server.Finish(wire.stream_id, {}));
          break;
        }
        case 8: {
          grpcxx::MetadataMap trailers;
          ASSERT_TRUE(trailers.Add("x-greptime-err-code", "2001"));
          ASSERT_TRUE(trailers.Add("x-greptime-err-msg", "invalid plan"));
          UnaryPeer::Send(server, wire, response, {}, trailers);
          break;
        }
        case 9: UnaryPeer::Send(server, wire, Response(17)); break;
        default: FAIL() << "unexpected replay";
        }
      },
      &peer);
  ASSERT_NE(peer.client, nullptr);
  auto call = [&] { return peer.client->TruncateTable("samples"); };
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
  EXPECT_EQ(error.error().Message(), "samples missing");
  EXPECT_FALSE(error.error().IsRetryable());
  for (int index = 0; index != 3; ++index) {
    SCOPED_TRACE(index);
    auto invalid = call();
    ASSERT_FALSE(invalid);
    if (index == 2) {
      EXPECT_EQ(invalid.error().Kind(), ErrorKind::kProtocol);
    } else {
      EXPECT_EQ(invalid.error().Kind(), ErrorKind::kProtocol) << invalid.error().Message();
      EXPECT_EQ(invalid.error().GrpcCode(), grpcxx::StatusCode::kInternal);
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
  peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<UnaryPeer *>(opaque);
        if (!self.Message(wire, kProm)) return;
        const auto request = UnaryPeer::Parse<proto::PromqlRequest>(wire);
        proto::PromqlResponse response;
        *response.mutable_header() = Response(0).header();
        response.set_body(R"({"status":"success"})");
        if (self.messages == 1) {
          auto *status = response.mutable_header()->mutable_status();
          status->set_status_code(2001);
          status->set_err_msg("invalid plan");
          status->set_retry_hint("false");
          UnaryPeer::Send(server, wire, response);
        } else if (self.messages == 2) {
          grpcxx::MetadataMap trailers;
          ASSERT_TRUE(trailers.Add("inner_error_msg", R"({"code":2002,"err_msg":"query execution failed"})"));
          ASSERT_TRUE(trailers.Add("x-greptime-err-retry-hint", "true"));
          UnaryPeer::Send(server, wire, response, grpcxx::Status{grpcxx::StatusCode::kInternal, "remote failure"},
                          trailers);
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
      },
      &peer);
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

// clang-format off
struct [[= greptimedb::table{"typed_samples"}]] TypedMetric {
  [[= greptimedb::tag{}]] std::string host;
  [[= greptimedb::timestamp{.unit = TimestampUnit::kMillisecond}]] std::int64_t time;
  [[= greptimedb::field{}]] double value;
  // clang-format on
};

TEST(UnaryClientIntegrationScenario, TypedRowsUseColumnarWireAndExplicitDestination) {
  UnaryPeer peer;
  peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<UnaryPeer *>(opaque);
        if (!self.Message(wire, kHandle)) return;
        const auto request = UnaryPeer::Parse<proto::GreptimeRequest>(wire);
        ASSERT_TRUE(request.has_inserts());
        ASSERT_EQ(request.inserts().inserts_size(), 1);
        const auto &insert = request.inserts().inserts(0);
        EXPECT_EQ(insert.table_name(), self.messages == 1 ? "typed_samples" : "explicit_samples");
        EXPECT_EQ(insert.row_count(), 2U);
        ASSERT_EQ(insert.columns_size(), 3);
        ASSERT_EQ(insert.columns(0).values().string_values_size(), 2);
        EXPECT_EQ(insert.columns(0).values().string_values(0), "srv");
        EXPECT_EQ(insert.columns(0).values().string_values(1), "mem");
        ASSERT_EQ(insert.columns(1).values().timestamp_millisecond_values_size(), 2);
        EXPECT_EQ(insert.columns(1).values().timestamp_millisecond_values(0), 1700000000000);
        EXPECT_EQ(insert.columns(1).values().timestamp_millisecond_values(1), 1700000000001);
        ASSERT_EQ(insert.columns(2).values().f64_values_size(), 2);
        EXPECT_EQ(insert.columns(2).values().f64_values(0), 42.5);
        EXPECT_EQ(insert.columns(2).values().f64_values(1), 18.25);
        UnaryPeer::Send(server, wire, Response(2));
      },
      &peer);
  ASSERT_NE(peer.client, nullptr);
  const std::array rows{TypedMetric{"srv", 1700000000000, 42.5}, TypedMetric{"mem", 1700000000001, 18.25}};
  const std::array invalid_tables{std::string_view{}, std::string_view{" \t\n"}, std::string_view{"bad\0table", 9}};
  for (const auto table : invalid_tables) {
    auto rejected = peer.client->InsertTo<TypedMetric>(table, std::span<const TypedMetric>{rows});
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().Kind(), ErrorKind::kSchemaViolation);
  }
  CheckRows(peer.client->Insert<TypedMetric>(std::span<const TypedMetric>{rows}), 2U);
  CheckRows(peer.client->InsertTo<TypedMetric>("explicit_samples", std::span<const TypedMetric>{rows}), 2U);
  EXPECT_EQ(peer.messages, 2U);
}

} // namespace
