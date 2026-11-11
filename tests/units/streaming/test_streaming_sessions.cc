#include "test_streaming_sessions_support.h"

namespace {
TEST(StreamingInsert, FourFrameKindsRoutingCountersAndFinish) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto stream = StreamInserter::Open(peer.pair->Channel(), Header());
  ASSERT_TRUE(stream.has_value());
  v1::InsertRequest insert;
  insert.set_table_name("cpu_metrics");
  insert.set_row_count(2);
  ASSERT_TRUE((*stream)->SendBatch(insert));
  ASSERT_TRUE((*stream)->SendBatchTo("alternate", insert));
  ASSERT_TRUE((*stream)->SendBatches(std::array{insert, insert}));
  v1::RowInsertRequest rows;
  rows.set_table_name("cpu_metrics");
  rows.mutable_rows()->add_rows();
  ASSERT_TRUE((*stream)->SendRows(rows));
  v1::DeleteRequest deletion;
  deletion.set_table_name("cpu_metrics");
  deletion.set_row_count(3);
  ASSERT_TRUE((*stream)->SendDeleteBatch(deletion));
  v1::RowDeleteRequest row_delete;
  row_delete.set_table_name("cpu_metrics");
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
    EXPECT_EQ(request.header().SerializeAsString(), Header().SerializeAsString());
  EXPECT_FALSE((*stream)->SendBatch(insert));
  EXPECT_FALSE((*stream)->Finish());
  EXPECT_FALSE(peer.failed);
}

TEST(StreamingInsert, ServerStatusMissingRowsAndResponseCardinalityFailClosed) {
  for (const auto scenario :
       {Scenario::kHeaderError, Scenario::kMissingRows, Scenario::kExtraResponse, Scenario::kTrailerError}) {
    Peer peer;
    peer.scenario = scenario;
    ASSERT_TRUE(peer.Open());
    auto stream = StreamInserter::Open(peer.pair->Channel());
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
  auto stream = StreamInserter::Open(peer.pair->Channel());
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
  auto stream = StreamInserter::Open(peer.pair->Channel());
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
  auto stream = StreamInserter::Open(peer.pair->Channel());
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
    auto stream = StreamInserter::Open(peer.pair->Channel());
    ASSERT_TRUE(stream.has_value());
    v1::InsertRequest insert;
    insert.set_table_name("cpu_metrics");
    insert.set_row_count(1);
    ASSERT_TRUE((*stream)->SendBatch(insert));
    peer.Pump();
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->Server().CancelledStreams(), 1u);
  auto recovery = StreamInserter::Open(peer.pair->Channel());
  ASSERT_TRUE(recovery.has_value());
  EXPECT_TRUE((*recovery)->Finish());
}

TEST(StreamingMetadata, JsonPreservesIntegerPrecisionAndRejectsInvalidFields) {
  const DoPutMetadata original{9007199254740993LL, std::numeric_limits<std::int64_t>::min(),
                               std::numeric_limits<std::int64_t>::max()};
  auto json = original.Encode();
  ASSERT_TRUE(json.has_value());
  auto roundtrip = DoPutMetadata::Decode(*json);
  ASSERT_TRUE(roundtrip.has_value());
  EXPECT_EQ(roundtrip->request_id, original.request_id);
  EXPECT_EQ(roundtrip->min_timestamp, original.min_timestamp);
  EXPECT_EQ(roundtrip->max_timestamp, original.max_timestamp);
  auto response = DoPutResponse::Decode("{\"request_id\":9007199254740993,\"affected_rows\":18446744073709551615,"
                                        "\"elapsed_secs\":0.25}");
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->request_id, original.request_id);
  EXPECT_EQ(response->affected_rows, std::numeric_limits<std::uint64_t>::max());
  for (const auto invalid :
       {"{}", "{\"request_id\":1.5}", "{\"request_id\":1,\"request_id\":2}", "{\"request_id\":9223372036854775808}"})
    EXPECT_FALSE(DoPutMetadata::Decode(invalid));
  EXPECT_FALSE(DoPutResponse::Decode("{\"request_id\":1,\"affected_rows\":2,\"elapsed_secs\":-1}"));
}

TEST(StreamingBulk, SchemaHandshakeAckAndRemainingDrain) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto batch = Batch();
  auto writer = BulkStreamWriter::Open(peer.pair->Channel(), "cpu_metrics", batch.schema);
  ASSERT_TRUE(writer.has_value());
  ASSERT_EQ(peer.bulk_frames.size(), 1u);
  const auto &schema = peer.bulk_frames[0];
  EXPECT_EQ(schema.flight_descriptor().type(), af::FlightDescriptor::PATH);
  ASSERT_EQ(schema.flight_descriptor().path_size(), 1);
  EXPECT_EQ(schema.flight_descriptor().path(0), "cpu_metrics");
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
  peer.pair->Channel().SetProgressHook(&peer, &Peer::DelayedProgress);
  auto batch = Batch();
  BulkWriteOptions options;
  options.max_in_flight = 1;
  auto writer = BulkStreamWriter::Open(peer.pair->Channel(), "cpu_metrics", batch.schema, options);
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
  for (const auto scenario :
       {Scenario::kBadHandshake, Scenario::kOutOfOrderAck, Scenario::kUnknownAck, Scenario::kMissingAck}) {
    Peer peer;
    peer.scenario = scenario;
    ASSERT_TRUE(peer.Open());
    auto batch = Batch();
    auto writer = BulkStreamWriter::Open(peer.pair->Channel(), "cpu_metrics", batch.schema);
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
  peer.scenario = Scenario::kMissingAck;
  ASSERT_TRUE(peer.Open());
  auto batch = Batch();
  {
    auto writer = BulkStreamWriter::Open(peer.pair->Channel(), "cpu_metrics", batch.schema);
    ASSERT_TRUE(writer.has_value());
    ASSERT_TRUE((*writer)->WriteBatch(batch));
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->Server().CancelledStreams(), 1u);
  peer.scenario = Scenario::kNormal;
  auto next = BulkStreamWriter::Open(peer.pair->Channel(), "cpu_metrics", batch.schema);
  ASSERT_TRUE(next.has_value());
  EXPECT_TRUE((*next)->Finish());
}

TEST(StreamingQuery, AllQueryTicketVariantsRetainHeaderAndPayload) {
  std::array<v1::QueryRequest, 4> queries;
  queries[0].set_sql("SELECT * FROM cpu_metrics");
  queries[1].set_logical_plan("\0\1binary", 8);
  queries[2].mutable_prom_range_query()->set_query("rate(cpu_metrics[5m])");
  auto *plan = queries[3].mutable_insert_into_plan();
  plan->mutable_table_name()->set_table_name("cpu_metrics");
  plan->set_logical_plan("insert-plan");
  for (const auto &query : queries) {
    Peer peer;
    ASSERT_TRUE(peer.Open());
    auto stream = QueryStream::Open(peer.pair->Channel(), Header(), query);
    ASSERT_TRUE(stream.has_value());
    ASSERT_TRUE((*stream)->Finish());
    ASSERT_TRUE(peer.ticket.has_value());
    EXPECT_EQ(peer.ticket->header().SerializeAsString(), Header().SerializeAsString());
    EXPECT_EQ(peer.ticket->query().SerializeAsString(), query.SerializeAsString());
  }
}

TEST(StreamingQuery, MetadataBeforeAndBetweenBatchesNeverTruncatesStream) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto stream = QueryStream::Sql(peer.pair->Channel(), Header(), "SELECT * FROM cpu_metrics");
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
  auto stream = QueryStream::Sql(peer.pair->Channel(), Header(), "SELECT * FROM cpu_metrics");
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
  for (const auto scenario : {Scenario::kBatchBeforeSchema, Scenario::kDuplicateSchema}) {
    Peer peer;
    peer.scenario = scenario;
    ASSERT_TRUE(peer.Open());
    auto stream = QueryStream::Sql(peer.pair->Channel(), Header(), "SELECT * FROM cpu_metrics");
    ASSERT_TRUE(stream.has_value());
    EXPECT_FALSE((*stream)->NextBatch());
  }
}

TEST(StreamingQuery, EarlyDropCancelsAndConnectionRecovers) {
  Peer peer;
  peer.scenario = Scenario::kQueryHold;
  ASSERT_TRUE(peer.Open());
  {
    auto stream = QueryStream::Sql(peer.pair->Channel(), Header(), "SELECT * FROM cpu_metrics");
    ASSERT_TRUE(stream.has_value());
    auto first = (*stream)->NextBatch();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(first->has_value());
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->Server().CancelledStreams(), 1u);
  peer.scenario = Scenario::kNormal;
  auto recovery = QueryStream::Sql(peer.pair->Channel(), Header(), "SELECT 1");
  ASSERT_TRUE(recovery.has_value());
  EXPECT_TRUE((*recovery)->Finish());
}

TEST(StreamingSessions, WrongThreadCallsRejectWithoutMutatingSession) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto insertion = StreamInserter::Open(peer.pair->Channel());
  auto batch = Batch();
  auto writer = BulkStreamWriter::Open(peer.pair->Channel(), "cpu_metrics", batch.schema);
  auto query = QueryStream::Sql(peer.pair->Channel(), Header(), "SELECT 1");
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

TEST(StreamingSessions, LocalDeadlinesRetainTimeoutCategory) {
  auto pair =
      grpcxx::InMemoryChannelPair::Create([](grpcxx::SynchronousServer &, const grpcxx::ServerRequest &) noexcept {});
  ASSERT_TRUE(pair);
  grpcxx::CallOptions budget;
  budget.timeout = std::chrono::milliseconds(1);
  auto insert = StreamInserter::Open((*pair)->Channel(), {}, budget);
  ASSERT_TRUE(insert);
  auto inserted = (*insert)->Finish();
  ASSERT_FALSE(inserted);
  EXPECT_EQ(inserted.error().Kind(), ErrorKind::kTimeout);
  EXPECT_EQ(inserted.error().GrpcCode(), grpcxx::StatusCode::kDeadlineExceeded);
  auto query = QueryStream::Sql((*pair)->Channel(), {}, "SELECT 1", budget);
  ASSERT_TRUE(query);
  auto read = (*query)->NextBatch();
  ASSERT_FALSE(read);
  EXPECT_EQ(read.error().Kind(), ErrorKind::kTimeout);
  EXPECT_EQ(read.error().GrpcCode(), grpcxx::StatusCode::kDeadlineExceeded);
  BulkWriteOptions options;
  options.timeout = std::chrono::milliseconds(1);
  auto bulk = BulkStreamWriter::Open((*pair)->Channel(), "cpu_metrics", Batch().schema, options);
  ASSERT_FALSE(bulk);
  EXPECT_EQ(bulk.error().Kind(), ErrorKind::kTimeout);
  EXPECT_EQ(bulk.error().GrpcCode(), grpcxx::StatusCode::kDeadlineExceeded);
}

} // namespace
