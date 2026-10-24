#include <greptimedb/client/client.h>
#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/query_stream.h>
#include <greptimedb/transport/mock_grpc_server.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace {
using namespace greptimedb;
namespace proto = greptime::v1;
namespace af = arrow::flight::protocol;

struct SessionPeer {
  std::unique_ptr<MockGrpcServer> server;
  // Sessions are test locals, so they finish and die before this client.
  std::unique_ptr<Client> client;
  std::size_t messages{}, half_closes{};

  void Open(MockGrpcServer::Handler handler, void* context) {
    auto pipe = MemoryPipe::CreatePair(97);
    ASSERT_TRUE(pipe);
    auto created_server = MockGrpcServer::Create(std::move(pipe->second), handler, context);
    ASSERT_TRUE(created_server);
    server = std::move(*created_server);
    auto endpoint = Endpoint::Parse("grpc://127.0.0.1:4001");
    ASSERT_TRUE(endpoint);
    ClientOptions options(std::move(*endpoint));
    options.catalog = "trading";
    options.schema = "ticks";
    options.timezone = "UTC";
    options.auth = Auth::Basic("user", "pass");
    options.max_in_flight = 1;
    ASSERT_TRUE(options.write_hints.SetAutoCreateTable(true));
    auto created = Client::FromMemoryPipe(
        std::move(options), std::move(pipe->first), server.get(),
        [](void* context) noexcept { return static_cast<MockGrpcServer*>(context)->PollOnce(); });
    ASSERT_TRUE(created);
    client = std::move(*created);
  }

  static void CheckMetadata(const MockRequest& request, std::string_view hints) {
    EXPECT_EQ(request.metadata.Get("x-greptime-db-name"), "trading-ticks");
    EXPECT_EQ(request.metadata.Get("x-greptime-auth"), "Basic dXNlcjpwYXNz");
    EXPECT_EQ(request.metadata.Get("x-greptime-hints"), hints);
  }

  static void CheckHeader(const proto::RequestHeader& header) {
    EXPECT_EQ(header.catalog(), "trading");
    EXPECT_EQ(header.schema(), "ticks");
    EXPECT_EQ(header.dbname(), "trading-ticks");
    EXPECT_EQ(header.timezone(), "UTC");
    EXPECT_EQ(header.authorization().basic().username(), "user");
    EXPECT_EQ(header.authorization().basic().password(), "pass");
  }

  template <typename Message> static Message Parse(const MockRequest& request) {
    Message message;
    EXPECT_TRUE(message.ParseFromArray(request.payload.data(),
                                       static_cast<int>(request.payload.size())));
    return message;
  }

  template <typename Message>
  static void Send(MockGrpcServer& server, std::int32_t stream, const Message& message) {
    std::string wire;
    ASSERT_TRUE(message.SerializeToString(&wire));
    ASSERT_TRUE(server.Send(stream, std::as_bytes(std::span{wire})));
  }
};

flight::RecordBatch FactoryBatch() {
  std::array<proto::Column, 2> columns;
  columns[0].set_column_name("time");
  columns[0].set_datatype(proto::TIMESTAMP_MILLISECOND);
  columns[0].set_semantic_type(proto::TIMESTAMP);
  columns[0].mutable_values()->add_timestamp_millisecond_values(1700000000000);
  columns[1].set_column_name("price");
  columns[1].set_datatype(proto::FLOAT64);
  columns[1].set_semantic_type(proto::FIELD);
  columns[1].mutable_values()->add_f64_values(42.5);
  auto batch = flight::RecordBatchFromProtoColumns(columns, 1);
  EXPECT_TRUE(batch);
  return batch ? std::move(*batch) : flight::RecordBatch{};
}

TEST(UnaryClientIntegrationScenario, StreamInserterFactoryCarriesScopeAuthAndHints) {
  SessionPeer peer;
  peer.Open([](void* context, MockGrpcServer& server, const MockRequest& event) noexcept {
    auto& self = *static_cast<SessionPeer*>(context);
    if (event.event == MockRequest::Event::kCancel) return;
    EXPECT_EQ(event.path, StreamInserter::kPath);
    if (event.event == MockRequest::Event::kMessage) {
      SessionPeer::CheckMetadata(event, self.messages == 0
          ? "auto_create_table=false,ttl=7d" : "auto_create_table=true");
      const auto request = SessionPeer::Parse<proto::GreptimeRequest>(event);
      SessionPeer::CheckHeader(request.header());
      ASSERT_TRUE(request.has_inserts());
      ASSERT_EQ(request.inserts().inserts_size(), 1);
      EXPECT_EQ(request.inserts().inserts(0).table_name(), "trades");
      EXPECT_EQ(request.inserts().inserts(0).row_count(), 2U);
      ++self.messages;
      return;
    }
    ++self.half_closes;
    proto::GreptimeResponse response;
    response.mutable_affected_rows()->set_value(17);
    SessionPeer::Send(server, event.stream_id, response);
    ASSERT_TRUE(server.Finish(event.stream_id));
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  WriteHints hints;
  ASSERT_TRUE(hints.SetAutoCreateTable(false));
  ASSERT_TRUE(hints.SetTtl("7d"));
  for (const auto* override_hints : std::array<const WriteHints*, 2>{&hints, nullptr}) {
    auto stream = peer.client->StreamInserter(override_hints);
    ASSERT_TRUE(stream);
    proto::GreptimeRequest request;
    request.mutable_header()->set_catalog("ignored_catalog");
    request.mutable_header()->set_schema("ignored_schema");
    auto* insert = request.mutable_inserts()->add_inserts();
    insert->set_table_name("trades");
    insert->set_row_count(2);
    ASSERT_TRUE((*stream)->SendRequest(std::move(request)));
    auto result = (*stream)->Finish();
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 17U);
    EXPECT_EQ((*stream)->BatchesSent(), 1U);
    EXPECT_EQ((*stream)->RowsSent(), 2U);
  }
  EXPECT_EQ(peer.messages, 2U);
  EXPECT_EQ(peer.half_closes, 2U);
}

TEST(UnaryClientIntegrationScenario, QueryFactoriesEncodeScopedFlightTicketsAndCleanEof) {
  SessionPeer peer;
  peer.Open([](void* context, MockGrpcServer& server, const MockRequest& event) noexcept {
    auto& self = *static_cast<SessionPeer*>(context);
    if (event.event == MockRequest::Event::kCancel) return;
    EXPECT_EQ(event.path, "/arrow.flight.protocol.FlightService/DoGet");
    if (event.event == MockRequest::Event::kMessage) {
      SessionPeer::CheckMetadata(event, "auto_create_table=true");
      const auto ticket = SessionPeer::Parse<af::Ticket>(event);
      proto::GreptimeRequest request;
      ASSERT_TRUE(request.ParseFromString(ticket.ticket()));
      SessionPeer::CheckHeader(request.header());
      ASSERT_TRUE(request.has_query());
      if (self.messages == 0) {
        EXPECT_EQ(request.query().sql(), "SELECT price FROM trades");
      } else {
        EXPECT_EQ(request.query().logical_plan(), std::string("\0\1plan", 6));
      }
      ++self.messages;
      return;
    }
    ++self.half_closes;
    ASSERT_TRUE(server.Finish(event.stream_id));
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  {
    auto stream = peer.client->Query("SELECT price FROM trades");
    ASSERT_TRUE(stream);
    auto eof = (*stream)->NextBatch();
    ASSERT_TRUE(eof);
    EXPECT_FALSE(eof->has_value());
    auto result = (*stream)->Finish();
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->affected_rows);
    EXPECT_TRUE(result->metrics.empty());
  }
  {
    proto::QueryRequest request;
    request.set_logical_plan("\0\1plan", 6);
    auto stream = peer.client->Query(request);
    ASSERT_TRUE(stream);
    auto result = (*stream)->Finish();
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->affected_rows);
    EXPECT_FALSE((*stream)->Schema());
  }
  EXPECT_EQ(peer.messages, 2U);
  EXPECT_EQ(peer.half_closes, 2U);
}

TEST(UnaryClientIntegrationScenario, BulkWriterFactoryCarriesMetadataAndAcknowledgesSchemaAndBatch) {
  SessionPeer peer;
  peer.Open([](void* context, MockGrpcServer& server, const MockRequest& event) noexcept {
    auto& self = *static_cast<SessionPeer*>(context);
    if (event.event == MockRequest::Event::kCancel) return;
    EXPECT_EQ(event.path, "/arrow.flight.protocol.FlightService/DoPut");
    if (event.event == MockRequest::Event::kHalfClose) {
      ++self.half_closes;
      ASSERT_TRUE(server.Finish(event.stream_id));
      return;
    }
    SessionPeer::CheckMetadata(event, "auto_create_table=false,ttl=7d");
    const auto frame = SessionPeer::Parse<af::FlightData>(event);
    auto metadata = flight::DoPutMetadata::Decode(frame.app_metadata());
    ASSERT_TRUE(metadata);
    EXPECT_EQ(metadata->request_id, static_cast<std::int64_t>(self.messages));
    if (self.messages == 0) {
      ASSERT_TRUE(frame.has_flight_descriptor());
      EXPECT_EQ(frame.flight_descriptor().type(), af::FlightDescriptor::PATH);
      ASSERT_EQ(frame.flight_descriptor().path_size(), 1);
      EXPECT_EQ(frame.flight_descriptor().path(0), "trades");
      auto schema = flight::DecodeFlightSchema(std::span{
          reinterpret_cast<const std::uint8_t*>(frame.data_header().data()),
          frame.data_header().size()});
      ASSERT_TRUE(schema);
      ASSERT_EQ(schema->fields.size(), 2U);
      EXPECT_EQ(schema->fields[0].name, "time");
      EXPECT_EQ(schema->fields[1].name, "price");
    } else {
      EXPECT_EQ(metadata->min_timestamp, 1700000000000);
      EXPECT_EQ(metadata->max_timestamp, 1700000000000);
    }
    ++self.messages;
    af::PutResult result;
    result.set_app_metadata("{\"request_id\":" + std::to_string(metadata->request_id) +
                            ",\"affected_rows\":" + (metadata->request_id == 0 ? "0" : "1") +
                            ",\"elapsed_secs\":0.125}");
    SessionPeer::Send(server, event.stream_id, result);
  }, &peer);
  ASSERT_NE(peer.client, nullptr);
  const auto batch = FactoryBatch();
  const std::array invalid_tables{std::string_view{}, std::string_view{" \t"},
                                   std::string_view{"bad\0table", 9}};
  for (auto table : invalid_tables) {
    auto invalid = peer.client->BulkWriter(table, batch.schema);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().Kind(), ErrorKind::kSchemaViolation);
  }
  flight::BulkWriteOptions invalid_options;
  invalid_options.max_in_flight = 0;
  auto invalid = peer.client->BulkWriter("trades", batch.schema, invalid_options);
  ASSERT_FALSE(invalid);
  EXPECT_EQ(invalid.error().Kind(), ErrorKind::kInvalidConfig);
  WriteHints hints;
  ASSERT_TRUE(hints.SetAutoCreateTable(false));
  ASSERT_TRUE(hints.SetTtl("7d"));
  auto writer = peer.client->BulkWriter("trades", batch.schema, std::nullopt, &hints);
  ASSERT_TRUE(writer);
  EXPECT_EQ(peer.messages, 1U);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  auto result = (*writer)->Finish();
  ASSERT_TRUE(result);
  EXPECT_EQ(result->batches_sent, 1U);
  EXPECT_EQ(result->total_affected_rows, 1U);
  EXPECT_DOUBLE_EQ(result->elapsed_secs, 0.125);
  EXPECT_EQ(peer.messages, 2U);
  EXPECT_EQ(peer.half_closes, 1U);
}
} // namespace
