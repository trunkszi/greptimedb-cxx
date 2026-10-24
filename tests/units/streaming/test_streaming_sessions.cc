/** HTTP/2 streaming sessions against a deterministic caller-driven MemoryPipe
 * peer. */
#include <gtest/gtest.h>

#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/query_stream.h>
#include <greptimedb/transport/in_memory_channel_pair.h>

#include <array>
#include <chrono>
#include <limits>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace {
using namespace greptimedb;
using namespace greptimedb::flight;
namespace v1 = ::greptime::v1;
namespace af = ::arrow::flight::protocol;

static_assert(!std::is_move_constructible_v<StreamInserter>);
static_assert(!std::is_move_constructible_v<BulkStreamWriter>);
static_assert(!std::is_move_constructible_v<QueryStream>);

enum class Scenario {
  kNormal,
  kHeaderError,
  kMissingRows,
  kExtraResponse,
  kTrailerError,
  kEarlyStatus,
  kBadHandshake,
  kOutOfOrderAck,
  kUnknownAck,
  kMissingAck,
  kDelayedAck,
  kQueryError,
  kQueryHold,
  kBatchBeforeSchema,
  kDuplicateSchema,
};

RecordBatch Batch() {
  std::array<v1::Column, 2> columns;
  columns[0].set_column_name("ts");
  columns[0].set_datatype(v1::TIMESTAMP_NANOSECOND);
  columns[0].set_semantic_type(v1::TIMESTAMP);
  columns[0].mutable_values()->add_timestamp_nanosecond_values(100);
  columns[0].mutable_values()->add_timestamp_nanosecond_values(200);
  columns[1].set_column_name("price");
  columns[1].set_datatype(v1::FLOAT64);
  columns[1].set_semantic_type(v1::FIELD);
  columns[1].mutable_values()->add_f64_values(4.0);
  columns[1].mutable_values()->add_f64_values(5.0);
  auto batch = RecordBatchFromProtoColumns(columns, 2);
  EXPECT_TRUE(batch.has_value());
  return batch ? std::move(*batch) : RecordBatch{};
}

v1::RequestHeader Header() {
  v1::RequestHeader header;
  header.set_catalog("research");
  header.set_schema("ticks");
  header.set_dbname("research-ticks");
  header.mutable_authorization()->mutable_token()->set_token("test-secret");
  return header;
}

template <class Message>
void SendProto(MockGrpcServer &server, std::int32_t id,
               const Message &message) noexcept {
  std::string encoded;
  ASSERT_TRUE(message.SerializeToString(&encoded));
  EXPECT_TRUE(server.Send(id, std::as_bytes(std::span{encoded})).has_value());
}

void SendAck(MockGrpcServer &server, std::int32_t id, std::int64_t request_id,
             std::uint64_t rows = 2) noexcept {
  af::PutResult response;
  response.set_app_metadata("{\"request_id\":" + std::to_string(request_id) +
                            ",\"affected_rows\":" + std::to_string(rows) +
                            ",\"elapsed_secs\":0.125}");
  SendProto(server, id, response);
}

void SendBody(MockGrpcServer &server, std::int32_t id,
              const EncodedFlightBody &body) noexcept {
  af::FlightData frame;
  frame.set_data_header(body.data_header.data(), body.data_header.size());
  if (!body.data_body.empty())
    frame.set_data_body(body.data_body.data(), body.data_body.size());
  SendProto(server, id, frame);
}

void SendMetadata(MockGrpcServer &server, std::int32_t id, std::uint32_t rows,
                  std::string_view metrics) noexcept {
  // Arrow Message FlatBuffer with MetadataVersion V5 and HeaderType NONE.
  constexpr std::array<std::uint8_t, 20> header{12, 0, 0, 0, 6, 0, 8, 0, 6, 0,
                                                0,  0, 8, 0, 0, 0, 0, 0, 4, 0};
  af::FlightData frame;
  frame.set_data_header(header.data(), header.size());
  v1::FlightMetadata metadata;
  metadata.mutable_affected_rows()->set_value(rows);
  metadata.mutable_metrics()->set_metrics(metrics.data(), metrics.size());
  ASSERT_TRUE(metadata.SerializeToString(frame.mutable_app_metadata()));
  SendProto(server, id, frame);
}

struct Peer {
  Scenario scenario{Scenario::kNormal};
  std::vector<v1::GreptimeRequest> writes;
  std::vector<DoPutMetadata> bulk_metadata;
  std::vector<af::FlightData> bulk_frames;
  std::optional<v1::GreptimeRequest> ticket;
  std::unique_ptr<InMemoryChannelPair> pair;
  std::int32_t delayed_stream{};
  std::int64_t delayed_id{};
  unsigned delay_ticks{};
  unsigned delayed_polls{};
  bool delayed_finish{};
  bool failed{};

  static void Handle(void *context, MockGrpcServer &server,
                     const MockRequest &event) noexcept {
    auto &self = *static_cast<Peer *>(context);
    switch (event.event) {
    case MockRequest::Event::kCancel:
      return;
    case MockRequest::Event::kMessage:
      if (event.path == StreamInserter::kPath)
        self.InsertMessage(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoPut")
        self.BulkMessage(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoGet")
        self.QueryMessage(server, event);
      else
        self.failed = true;
      return;
    case MockRequest::Event::kHalfClose:
      if (event.path == StreamInserter::kPath)
        self.InsertFinish(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoPut" &&
               self.scenario == Scenario::kDelayedAck &&
               self.delayed_stream != 0)
        self.delayed_finish = true;
      else if (event.path == "/arrow.flight.protocol.FlightService/DoGet" &&
               self.scenario == Scenario::kQueryHold)
        return;
      else {
        auto status = self.scenario == Scenario::kQueryError
                          ? grpcxx::Status{grpcxx::StatusCode::kUnavailable,
                                           "query interrupted"}
                          : grpcxx::Status{};
        if (!server.Finish(event.stream_id, std::move(status)))
          self.failed = true;
      }
      return;
    }
  }

  bool Open() noexcept {
    auto created = InMemoryChannelPair::Create(&Handle, this);
    if (!created)
      return false;
    pair = std::move(*created);
    return true;
  }

  void Pump() noexcept {
    for (unsigned i = 0; i != 16; ++i)
      EXPECT_TRUE(pair->channel().PollOnce().has_value());
  }

  void InsertMessage(MockGrpcServer &server,
                     const MockRequest &event) noexcept {
    v1::GreptimeRequest request;
    if (!request.ParseFromArray(event.payload.data(),
                                static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    writes.push_back(std::move(request));
    if (scenario == Scenario::kEarlyStatus) {
      v1::GreptimeResponse response;
      response.mutable_header()->mutable_status()->set_status_code(3001);
      response.mutable_header()->mutable_status()->set_err_msg(
          "early rejection");
      response.mutable_affected_rows()->set_value(0);
      SendProto(server, event.stream_id, response);
      EXPECT_TRUE(server.Finish(event.stream_id).has_value());
    }
  }

  void InsertFinish(MockGrpcServer &server, const MockRequest &event) noexcept {
    if (scenario == Scenario::kTrailerError) {
      grpcxx::MetadataMap trailers;
      EXPECT_TRUE(trailers
                      .Add("inner_error_msg",
                           "{\"code\":4003,\"err_msg\":\"stream busy\"}")
                      .has_value());
      EXPECT_TRUE(
          trailers.Add("x-greptime-err-retry-hint", "true").has_value());
      EXPECT_TRUE(server.Finish(event.stream_id, {}, trailers).has_value());
      return;
    }
    v1::GreptimeResponse response;
    if (scenario == Scenario::kHeaderError) {
      auto *status = response.mutable_header()->mutable_status();
      status->set_status_code(3001);
      status->set_err_msg("destination absent");
      status->set_retry_hint("false");
    }
    if (scenario != Scenario::kMissingRows)
      response.mutable_affected_rows()->set_value(17);
    SendProto(server, event.stream_id, response);
    if (scenario == Scenario::kExtraResponse)
      SendProto(server, event.stream_id, response);
    EXPECT_TRUE(server.Finish(event.stream_id).has_value());
  }

  void BulkMessage(MockGrpcServer &server, const MockRequest &event) noexcept {
    af::FlightData frame;
    if (!frame.ParseFromArray(event.payload.data(),
                              static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    auto metadata = DoPutMetadata::Decode(frame.app_metadata());
    if (!metadata) {
      failed = true;
      return;
    }
    bulk_metadata.push_back(*metadata);
    bulk_frames.push_back(frame);
    if (metadata->request_id == 0) {
      SendAck(server, event.stream_id,
              scenario == Scenario::kBadHandshake ? 9 : 0, 0);
      return;
    }
    if (scenario == Scenario::kMissingAck)
      return;
    if (scenario == Scenario::kDelayedAck) {
      delayed_stream = event.stream_id;
      delayed_id = metadata->request_id;
      delay_ticks = 12;
      return;
    }
    auto id = metadata->request_id;
    if (scenario == Scenario::kOutOfOrderAck)
      ++id;
    if (scenario == Scenario::kUnknownAck)
      id = 999;
    SendAck(server, event.stream_id, id);
  }

  static auto DelayedProgress(void *context) noexcept
      -> std::expected<void, grpcxx::Status> {
    auto &self = *static_cast<Peer *>(context);
    auto polled = self.pair->server().PollOnce();
    if (!polled)
      return polled;
    if (self.delayed_stream != 0) {
      ++self.delayed_polls;
      if (--self.delay_ticks == 0) {
        SendAck(self.pair->server(), self.delayed_stream, self.delayed_id);
        if (self.delayed_finish) {
          EXPECT_TRUE(
              self.pair->server().Finish(self.delayed_stream).has_value());
        }
        self.delayed_stream = 0;
      }
    }
    return {};
  }

  void QueryMessage(MockGrpcServer &server, const MockRequest &event) noexcept {
    af::Ticket encoded;
    v1::GreptimeRequest request;
    if (!encoded.ParseFromArray(event.payload.data(),
                                static_cast<int>(event.payload.size())) ||
        !request.ParseFromString(encoded.ticket())) {
      failed = true;
      return;
    }
    ticket = std::move(request);
    const auto batch = Batch();
    FlightIpcEncoder encoder;
    auto schema = encoder.EncodeSchema(batch.schema);
    auto body = encoder.EncodeBatch(batch);
    ASSERT_TRUE(schema.has_value());
    ASSERT_TRUE(body.has_value());
    if (scenario == Scenario::kBatchBeforeSchema) {
      SendBody(server, event.stream_id, *body);
      return;
    }
    SendMetadata(server, event.stream_id, 7, "before-schema");
    SendBody(server, event.stream_id, *schema);
    if (scenario == Scenario::kDuplicateSchema)
      SendBody(server, event.stream_id, *schema);
    SendBody(server, event.stream_id, *body);
    if (scenario == Scenario::kQueryError || scenario == Scenario::kQueryHold)
      return;
    SendMetadata(server, event.stream_id, 5, "between-batches");
    SendBody(server, event.stream_id, *body);
  }
};

TEST(StreamingInsert, FourFrameKindsRoutingCountersAndFinish) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto stream = StreamInserter::Open(peer.pair->channel(), Header());
  ASSERT_TRUE(stream.has_value());
  v1::InsertRequest insert;
  insert.set_table_name("trades");
  insert.set_row_count(2);
  ASSERT_TRUE((*stream)->SendBatch(insert));
  ASSERT_TRUE((*stream)->SendBatchTo("alternate", insert));
  ASSERT_TRUE((*stream)->SendBatches(std::array{insert, insert}));
  v1::RowInsertRequest rows;
  rows.set_table_name("trades");
  rows.mutable_rows()->add_rows();
  ASSERT_TRUE((*stream)->SendRows(rows));
  v1::DeleteRequest deletion;
  deletion.set_table_name("trades");
  deletion.set_row_count(3);
  ASSERT_TRUE((*stream)->SendDeleteBatch(deletion));
  v1::RowDeleteRequest row_delete;
  row_delete.set_table_name("trades");
  row_delete.mutable_rows()->add_rows();
  ASSERT_TRUE((*stream)->SendDeleteRows(row_delete));
  EXPECT_EQ((*stream)->BatchesSent(), 6u);
  EXPECT_EQ((*stream)->RowsSent(), 13u);
  auto affected = (*stream)->Finish();
  ASSERT_TRUE(affected.has_value());
  EXPECT_EQ(*affected, 17u);
  ASSERT_EQ(peer.writes.size(), 6u);
  EXPECT_EQ(peer.writes[0].request_case(), v1::GreptimeRequest::kInserts);
  EXPECT_EQ(peer.writes[1].inserts().inserts(0).table_name(), "alternate");
  EXPECT_EQ(peer.writes[2].inserts().inserts_size(), 2);
  EXPECT_EQ(peer.writes[3].request_case(), v1::GreptimeRequest::kRowInserts);
  EXPECT_EQ(peer.writes[4].request_case(), v1::GreptimeRequest::kDeletes);
  EXPECT_EQ(peer.writes[5].request_case(), v1::GreptimeRequest::kRowDeletes);
  for (const auto &request : peer.writes)
    EXPECT_EQ(request.header().SerializeAsString(),
              Header().SerializeAsString());
  EXPECT_FALSE((*stream)->SendBatch(insert));
  EXPECT_FALSE((*stream)->Finish());
  EXPECT_FALSE(peer.failed);
}

TEST(StreamingInsert, ServerStatusMissingRowsAndResponseCardinalityFailClosed) {
  for (const auto scenario :
       {Scenario::kHeaderError, Scenario::kMissingRows,
        Scenario::kExtraResponse, Scenario::kTrailerError}) {
    Peer peer;
    peer.scenario = scenario;
    ASSERT_TRUE(peer.Open());
    auto stream = StreamInserter::Open(peer.pair->channel());
    ASSERT_TRUE(stream.has_value());
    auto result = (*stream)->Finish();
    ASSERT_FALSE(result.has_value());
    if (scenario == Scenario::kHeaderError) {
      EXPECT_EQ(result.error().Kind(), ErrorKind::kServerStatus);
      EXPECT_EQ(result.error().RawCode(), 3001u);
      EXPECT_EQ(result.error().RetryHint(), false);
    }
    if (scenario == Scenario::kTrailerError) {
      EXPECT_EQ(result.error().Kind(), ErrorKind::kServerStatus);
      EXPECT_EQ(result.error().RawCode(), 4003u);
      EXPECT_EQ(result.error().RetryHint(), true);
    }
    auto cancelled = (*stream)->Cancel();
    ASSERT_FALSE(cancelled);
    EXPECT_EQ(cancelled.error().Message(), result.error().Message());
    auto repeated = (*stream)->Finish();
    ASSERT_FALSE(repeated);
    EXPECT_EQ(repeated.error().Message(), result.error().Message());
    EXPECT_FALSE(peer.failed);
  }
}

TEST(StreamingInsert, NonIngestionRejectedBeforeSend) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto stream = StreamInserter::Open(peer.pair->channel());
  ASSERT_TRUE(stream.has_value());
  v1::GreptimeRequest request;
  request.mutable_query()->set_sql("SELECT 1");
  EXPECT_FALSE((*stream)->SendRequest(request));
  EXPECT_EQ((*stream)->BatchesSent(), 0u);
  EXPECT_TRUE((*stream)->Finish());
  EXPECT_TRUE(peer.writes.empty());
}

TEST(StreamingInsert, QueuedEarlyResponseRetainsApplicationError) {
  Peer peer;
  peer.scenario = Scenario::kEarlyStatus;
  ASSERT_TRUE(peer.Open());
  auto stream = StreamInserter::Open(peer.pair->channel());
  ASSERT_TRUE(stream.has_value());
  v1::InsertRequest insert;
  insert.set_table_name("missing");
  insert.set_row_count(1);
  ASSERT_TRUE((*stream)->SendBatch(insert));
  peer.Pump();
  auto result = (*stream)->Finish();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(result.error().RawCode(), 3001u);
  EXPECT_EQ(result.error().Message(), "early rejection");
}

TEST(StreamingInsert, WriteAfterEarlyErrorRetainsApplicationFailure) {
  Peer peer;
  peer.scenario = Scenario::kEarlyStatus;
  ASSERT_TRUE(peer.Open());
  auto stream = StreamInserter::Open(peer.pair->channel());
  ASSERT_TRUE(stream);
  v1::InsertRequest insert;
  insert.set_table_name("missing");
  insert.set_row_count(1);
  ASSERT_TRUE((*stream)->SendBatch(insert));
  peer.Pump();
  auto second = (*stream)->SendBatch(insert);
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(second.error().Message(), "early rejection");
  EXPECT_EQ((*stream)->BatchesSent(), 1U);
  EXPECT_EQ((*stream)->RowsSent(), 1U);
  auto finished = (*stream)->Finish();
  ASSERT_FALSE(finished);
  EXPECT_EQ(finished.error().Message(), second.error().Message());
}

TEST(StreamingInsert, EarlyDropResetsAndNextRpcWorks) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  {
    auto stream = StreamInserter::Open(peer.pair->channel());
    ASSERT_TRUE(stream.has_value());
    v1::InsertRequest insert;
    insert.set_table_name("trades");
    insert.set_row_count(1);
    ASSERT_TRUE((*stream)->SendBatch(insert));
    peer.Pump();
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->server().CancelledStreams(), 1u);
  auto recovery = StreamInserter::Open(peer.pair->channel());
  ASSERT_TRUE(recovery.has_value());
  EXPECT_TRUE((*recovery)->Finish());
}

TEST(StreamingMetadata, JsonPreservesIntegerPrecisionAndRejectsInvalidFields) {
  const DoPutMetadata original{9007199254740993LL,
                               std::numeric_limits<std::int64_t>::min(),
                               std::numeric_limits<std::int64_t>::max()};
  auto json = original.Encode();
  ASSERT_TRUE(json.has_value());
  auto roundtrip = DoPutMetadata::Decode(*json);
  ASSERT_TRUE(roundtrip.has_value());
  EXPECT_EQ(roundtrip->request_id, original.request_id);
  EXPECT_EQ(roundtrip->min_timestamp, original.min_timestamp);
  EXPECT_EQ(roundtrip->max_timestamp, original.max_timestamp);
  auto response = DoPutResponse::Decode(
      "{\"request_id\":9007199254740993,\"affected_rows\":18446744073709551615,"
      "\"elapsed_secs\":0.25}");
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->request_id, original.request_id);
  EXPECT_EQ(response->affected_rows, std::numeric_limits<std::uint64_t>::max());
  for (const auto invalid :
       {"{}", "{\"request_id\":1.5}", "{\"request_id\":1,\"request_id\":2}",
        "{\"request_id\":9223372036854775808}"})
    EXPECT_FALSE(DoPutMetadata::Decode(invalid));
  EXPECT_FALSE(DoPutResponse::Decode(
      "{\"request_id\":1,\"affected_rows\":2,\"elapsed_secs\":-1}"));
}

TEST(StreamingBulk, SchemaHandshakeAckAndRemainingDrain) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto batch = Batch();
  auto writer =
      BulkStreamWriter::Open(peer.pair->channel(), "trades", batch.schema);
  ASSERT_TRUE(writer.has_value());
  ASSERT_EQ(peer.bulk_frames.size(), 1u);
  const auto &schema = peer.bulk_frames[0];
  EXPECT_EQ(schema.flight_descriptor().type(), af::FlightDescriptor::PATH);
  ASSERT_EQ(schema.flight_descriptor().path_size(), 1);
  EXPECT_EQ(schema.flight_descriptor().path(0), "trades");
  EXPECT_EQ(peer.bulk_metadata[0].request_id, 0);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  auto stats = (*writer)->Finish();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->batches_sent, 2u);
  EXPECT_EQ(stats->total_affected_rows, 4u);
  EXPECT_DOUBLE_EQ(stats->elapsed_secs, 0.25);
  ASSERT_EQ(peer.bulk_metadata.size(), 3u);
  EXPECT_EQ(peer.bulk_metadata[1].request_id, 1);
  EXPECT_EQ(peer.bulk_metadata[2].request_id, 2);
  EXPECT_EQ(peer.bulk_metadata[1].min_timestamp, 100);
  EXPECT_EQ(peer.bulk_metadata[1].max_timestamp, 200);
  EXPECT_FALSE(peer.failed);
}

TEST(StreamingBulk, MaxInFlightBlocksUntilDelayedAck) {
  Peer peer;
  peer.scenario = Scenario::kDelayedAck;
  ASSERT_TRUE(peer.Open());
  peer.pair->channel().SetProgressHook(&peer, &Peer::DelayedProgress);
  auto batch = Batch();
  BulkWriteOptions options;
  options.max_in_flight = 1;
  auto writer = BulkStreamWriter::Open(peer.pair->channel(), "trades",
                                       batch.schema, options);
  ASSERT_TRUE(writer.has_value());
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  EXPECT_EQ((*writer)->InFlight(), 1u);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  EXPECT_GE(peer.delayed_polls, 12u);
  EXPECT_EQ((*writer)->InFlight(), 1u);
  auto stats = (*writer)->Finish();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->total_affected_rows, 4u);
}

TEST(StreamingBulk, InvalidHandshakeOutOfOrderUnknownAndMissingAckReject) {
  for (const auto scenario : {Scenario::kBadHandshake, Scenario::kOutOfOrderAck,
                              Scenario::kUnknownAck, Scenario::kMissingAck}) {
    Peer peer;
    peer.scenario = scenario;
    ASSERT_TRUE(peer.Open());
    auto batch = Batch();
    auto writer =
        BulkStreamWriter::Open(peer.pair->channel(), "trades", batch.schema);
    if (scenario == Scenario::kBadHandshake) {
      EXPECT_FALSE(writer);
      continue;
    }
    ASSERT_TRUE(writer.has_value());
    ASSERT_TRUE((*writer)->WriteBatch(batch));
    EXPECT_FALSE((*writer)->Finish());
  }
}

TEST(StreamingBulk, DropCancelsAndChannelCanReopen) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto batch = Batch();
  {
    auto writer =
        BulkStreamWriter::Open(peer.pair->channel(), "trades", batch.schema);
    ASSERT_TRUE(writer.has_value());
    ASSERT_TRUE((*writer)->WriteBatch(batch));
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->server().CancelledStreams(), 1u);
  auto next =
      BulkStreamWriter::Open(peer.pair->channel(), "trades", batch.schema);
  ASSERT_TRUE(next.has_value());
  EXPECT_TRUE((*next)->Finish());
}

TEST(StreamingQuery, AllQueryTicketVariantsRetainHeaderAndPayload) {
  std::array<v1::QueryRequest, 4> queries;
  queries[0].set_sql("SELECT * FROM trades");
  queries[1].set_logical_plan("\0\1binary", 8);
  queries[2].mutable_prom_range_query()->set_query("rate(trades[5m])");
  auto *plan = queries[3].mutable_insert_into_plan();
  plan->mutable_table_name()->set_table_name("trades");
  plan->set_logical_plan("insert-plan");
  for (const auto &query : queries) {
    Peer peer;
    ASSERT_TRUE(peer.Open());
    auto stream = QueryStream::Open(peer.pair->channel(), Header(), query);
    ASSERT_TRUE(stream.has_value());
    ASSERT_TRUE((*stream)->Finish());
    ASSERT_TRUE(peer.ticket.has_value());
    EXPECT_EQ(peer.ticket->header().SerializeAsString(),
              Header().SerializeAsString());
    EXPECT_EQ(peer.ticket->query().SerializeAsString(),
              query.SerializeAsString());
  }
}

TEST(StreamingQuery, MetadataBeforeAndBetweenBatchesNeverTruncatesStream) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto stream =
      QueryStream::Sql(peer.pair->channel(), Header(), "SELECT * FROM trades");
  ASSERT_TRUE(stream.has_value());
  auto first = (*stream)->NextBatch();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  EXPECT_EQ((**first).row_count, 2u);
  EXPECT_EQ((**first).columns[1].values().f64_values(0), 4.0);
  EXPECT_EQ((*stream)->AffectedRows(), 7u);
  auto second = (*stream)->NextBatch();
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->has_value());
  EXPECT_EQ((**second).row_count, 2u);
  auto eof = (*stream)->NextBatch();
  ASSERT_TRUE(eof.has_value());
  EXPECT_FALSE(eof->has_value());
  auto result = (*stream)->Finish();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->affected_rows, 12u);
  ASSERT_EQ(result->metrics.size(), 2u);
  EXPECT_EQ(result->metrics[0], "before-schema");
  EXPECT_EQ(result->metrics[1], "between-batches");
}

TEST(StreamingQuery, MidStreamGrpcErrorIsNotCleanEof) {
  Peer peer;
  peer.scenario = Scenario::kQueryError;
  ASSERT_TRUE(peer.Open());
  auto stream =
      QueryStream::Sql(peer.pair->channel(), Header(), "SELECT * FROM trades");
  ASSERT_TRUE(stream.has_value());
  auto first = (*stream)->NextBatch();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto error = (*stream)->NextBatch();
  ASSERT_FALSE(error.has_value());
  EXPECT_EQ(error.error().GrpcCode(), grpcxx::StatusCode::kUnavailable);
  auto cancelled = (*stream)->Cancel();
  ASSERT_FALSE(cancelled);
  EXPECT_EQ(cancelled.error().Message(), error.error().Message());
  auto finished = (*stream)->Finish();
  ASSERT_FALSE(finished);
  EXPECT_EQ(finished.error().GrpcCode(), grpcxx::StatusCode::kUnavailable);
}

TEST(StreamingQuery, BatchBeforeSchemaAndDuplicateSchemaReject) {
  for (const auto scenario :
       {Scenario::kBatchBeforeSchema, Scenario::kDuplicateSchema}) {
    Peer peer;
    peer.scenario = scenario;
    ASSERT_TRUE(peer.Open());
    auto stream = QueryStream::Sql(peer.pair->channel(), Header(),
                                   "SELECT * FROM trades");
    ASSERT_TRUE(stream.has_value());
    EXPECT_FALSE((*stream)->NextBatch());
  }
}

TEST(StreamingQuery, EarlyDropCancelsAndConnectionRecovers) {
  Peer peer;
  peer.scenario = Scenario::kQueryHold;
  ASSERT_TRUE(peer.Open());
  {
    auto stream = QueryStream::Sql(peer.pair->channel(), Header(),
                                   "SELECT * FROM trades");
    ASSERT_TRUE(stream.has_value());
    auto first = (*stream)->NextBatch();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(first->has_value());
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->server().CancelledStreams(), 1u);
  peer.scenario = Scenario::kNormal;
  auto recovery = QueryStream::Sql(peer.pair->channel(), Header(), "SELECT 1");
  ASSERT_TRUE(recovery.has_value());
  EXPECT_TRUE((*recovery)->Finish());
}

TEST(StreamingSessions, WrongThreadCallsRejectWithoutMutatingSession) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto insertion = StreamInserter::Open(peer.pair->channel());
  auto batch = Batch();
  auto writer =
      BulkStreamWriter::Open(peer.pair->channel(), "trades", batch.schema);
  auto query = QueryStream::Sql(peer.pair->channel(), Header(), "SELECT 1");
  ASSERT_TRUE(insertion.has_value());
  ASSERT_TRUE(writer.has_value());
  ASSERT_TRUE(query.has_value());
  std::thread wrong_thread([&] {
    EXPECT_FALSE((*insertion)->Finish());
    EXPECT_FALSE((*writer)->WriteBatch(batch));
    EXPECT_FALSE((*query)->NextBatch());
  });
  wrong_thread.join();
  EXPECT_TRUE((*insertion)->Finish());
  EXPECT_TRUE((*writer)->Finish());
  EXPECT_TRUE((*query)->Finish());
}

} // namespace
