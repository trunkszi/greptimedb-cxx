#include "test_unary_client_operations_support.h"

namespace {
TEST(UnaryClientIntegrationScenario, HealthFailureAndDeadlineLeaveTheClientUsable) {
  UnaryPeer peer;
  auto options = Options();
  options.request_timeout = std::chrono::milliseconds(10);
  peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<UnaryPeer *>(opaque);
        if (!self.Message(wire, kHealth)) return;
        if (self.messages == 1) {
          ASSERT_TRUE(server.Finish(wire.stream_id, grpcxx::Status{grpcxx::StatusCode::kUnavailable, "unhealthy"}));
        } else if (self.messages != 2) {
          UnaryPeer::Send(server, wire, proto::HealthCheckResponse{});
        }
      },
      &peer, std::move(options));
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
  struct State {
    UnaryPeer peer;
    std::size_t writes{}, health{};
  } state;
  state.peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<State *>(opaque);
        if (wire.event != grpcxx::ServerRequest::Event::kMessage) return;
        if (wire.path == kHandle) {
          ASSERT_TRUE(self.peer.Message(wire, kHandle));
          ++self.writes;
          UnaryPeer::Send(server, wire, Response(1));
        } else {
          ASSERT_TRUE(self.peer.Message(wire, kHealth));
          ++self.health;
          UnaryPeer::Send(server, wire, proto::HealthCheckResponse{});
        }
      },
      &state);
  ASSERT_NE(state.peer.client, nullptr);
  ColumnarBatch valid("samples"), invalid("rejected");
  FillMetric(valid);
  ASSERT_TRUE(invalid.AddFieldColumn("value", ColumnDataType::kFloat64));
  const std::array values{ValueView{42.5}};
  ASSERT_TRUE(invalid.PushResultRow(values));
  ASSERT_FALSE(invalid.ValidateSchema());
  const std::array<const ColumnarBatch *, 2> batches{&valid, &invalid};
  auto rejected = state.peer.client->InsertBatches(batches);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Kind(), ErrorKind::kSchemaViolation);
  ASSERT_TRUE(state.peer.client->Ping());
  EXPECT_EQ(state.writes, 0U);
  EXPECT_EQ(state.health, 1U);
}

TEST(UnaryClientIntegrationScenario, InitialAndTrailingDiagnosticsCombineWithoutGrantingRetry) {
  UnaryPeer peer;
  peer.Open(
      [](void *opaque, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &wire) noexcept {
        auto &self = *static_cast<UnaryPeer *>(opaque);
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
          ASSERT_TRUE(server.Finish(wire.stream_id, grpcxx::Status{grpcxx::StatusCode::kUnavailable}, trailers));
        } else if (self.messages == 3) {
          ASSERT_TRUE(initial.Add("inner_error_msg", R"({"code":3001,"err_msg":"initial metadata table missing"})"));
          ASSERT_TRUE(server.SetInitialMetadata(wire.stream_id, initial));
          UnaryPeer::Send(server, wire, Response(99));
        } else {
          EXPECT_EQ(self.messages, 4U);
          UnaryPeer::Send(server, wire, Response(3));
        }
      },
      &peer);
  ASSERT_NE(peer.client, nullptr);
  auto split = peer.client->TruncateTable("samples");
  ASSERT_FALSE(split);
  EXPECT_EQ(split.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(split.error().RawCode(), 4000U);
  EXPECT_EQ(split.error().Message(), "storage unavailable");
  EXPECT_TRUE(split.error().IsRetryable());
  auto conflicting_retry = peer.client->TruncateTable("samples");
  ASSERT_FALSE(conflicting_retry);
  EXPECT_EQ(conflicting_retry.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(conflicting_retry.error().RawCode(), 4000U);
  EXPECT_EQ(conflicting_retry.error().Message(), "storage unavailable");
  EXPECT_FALSE(conflicting_retry.error().IsRetryable());
  auto initial_error = peer.client->TruncateTable("samples");
  ASSERT_FALSE(initial_error);
  EXPECT_EQ(initial_error.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(initial_error.error().RawCode(), 3001U);
  EXPECT_EQ(initial_error.error().Message(), "initial metadata table missing");
  EXPECT_FALSE(initial_error.error().IsRetryable());
  CheckRows(peer.client->TruncateTable("samples"), 3U);
  EXPECT_EQ(peer.messages, 4U);
}

TEST(UnaryClientUnitScenario, FactoriesRejectInvalidOptionsMissingChannelAndClosedPipe) {
  auto missing_channel = Client::FromChannel(Options(), nullptr);
  ASSERT_FALSE(missing_channel);
  EXPECT_EQ(missing_channel.error().Kind(), ErrorKind::kInvalidConfig);

  auto pipe = nexora::MemoryPipe::CreatePair();
  ASSERT_TRUE(pipe);
  auto channel = grpcxx::SynchronousChannel::FromMemoryPipe(std::move(pipe->client));
  ASSERT_TRUE(channel);
  auto invalid_options = Client::FromChannel(ClientOptions{}, std::move(*channel));
  ASSERT_FALSE(invalid_options);
  EXPECT_EQ(invalid_options.error().Kind(), ErrorKind::kInvalidConfig);

  auto closed = nexora::MemoryPipe::CreatePair();
  ASSERT_TRUE(closed);
  closed->client.Close();
  auto closed_pipe = Client::FromMemoryPipe(Options(), std::move(closed->client));
  ASSERT_FALSE(closed_pipe);
  EXPECT_EQ(closed_pipe.error().Kind(), ErrorKind::kGrpc);
  EXPECT_EQ(closed_pipe.error().GrpcCode(), grpcxx::StatusCode::kInvalidArgument);

  auto unconfigured = nexora::MemoryPipe::CreatePair();
  ASSERT_TRUE(unconfigured);
  auto missing_endpoint = Client::FromMemoryPipe(ClientOptions{}, std::move(unconfigured->client));
  ASSERT_FALSE(missing_endpoint);
  EXPECT_EQ(missing_endpoint.error().Kind(), ErrorKind::kInvalidConfig);
}
} // namespace
