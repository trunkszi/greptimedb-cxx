#include "streaming_edges_support.h"

namespace greptimedb::flight::testing {
namespace {
TEST(StreamingJsonEdges, PreservesIntegerAndTimestampExtremesExactly) {
  const DoPutMetadata original{9007199254740993LL, std::numeric_limits<std::int64_t>::min(),
                               std::numeric_limits<std::int64_t>::max()};
  auto encoded = original.Encode();
  ASSERT_TRUE(encoded);
  auto decoded = DoPutMetadata::Decode(*encoded);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->request_id, original.request_id);
  EXPECT_EQ(decoded->min_timestamp, original.min_timestamp);
  EXPECT_EQ(decoded->max_timestamp, original.max_timestamp);
  auto response = DoPutResponse::Decode(
      R"({"request_id":9007199254740993,"affected_rows":18446744073709551615,"elapsed_secs":1e-3})");
  ASSERT_TRUE(response);
  EXPECT_EQ(response->request_id, original.request_id);
  EXPECT_EQ(response->affected_rows, std::numeric_limits<std::uint64_t>::max());
  EXPECT_DOUBLE_EQ(response->elapsed_secs, 0.001);
}
TEST(StreamingJsonEdges, DecodesEscapedKeysAndSkipsStructuredExtensions) {
  auto metadata = DoPutMetadata::Decode(
      R"({"request\u005fid":7,"min_timestamp":null,"max_timestamp":-9,"future":{"nested":[true,null,"\ud83d\ude00"]}})");
  ASSERT_TRUE(metadata);
  EXPECT_EQ(metadata->request_id, 7);
  EXPECT_FALSE(metadata->min_timestamp);
  EXPECT_EQ(metadata->max_timestamp, -9);
  auto response = DoPutResponse::Decode(R"({"request_id":7,"affected\u005frows":19,"extra":[1,{"x":false}]})");
  ASSERT_TRUE(response);
  EXPECT_EQ(response->affected_rows, 19U);
}
TEST(StreamingJsonEdges, RejectsMalformedDuplicateAndOutOfRangeValues) {
  for (std::string_view json :
       {R"({})", R"({"request_id":1,"request_id":2})", R"({"request_id":1,"request\u005fid":2})",
        R"({"request_id":9223372036854775808})", R"({"request_id":1.0})", R"({"request_id":01})",
        R"({"request_id":1,})", R"({"request_id":1}false)",
        R"({"request_id":1,"min_timestamp":0,"min_timestamp":null})", R"({"request_id":1,"unknown":"\ud800"})"}) {
    EXPECT_FALSE(DoPutMetadata::Decode(json)) << json;
  }
  for (std::string_view json : {R"({"request_id":1})", R"({"request_id":1,"affected_rows":-1})",
                                R"({"request_id":1,"affected_rows":18446744073709551616})",
                                R"({"request_id":1,"affected_rows":0,"elapsed_secs":-0.01})",
                                R"({"request_id":1,"affected_rows":0,"elapsed_secs":1e999})",
                                R"({"request_id":1,"affected_rows":0,"elapsed_secs":NaN})",
                                R"({"request_id":1,"affected_rows":0,"affected_rows":1})",
                                R"({"request_id":1,"affected_rows":0,"elapsed_secs":0,"elapsed_secs":0})"}) {
    EXPECT_FALSE(DoPutResponse::Decode(json)) << json;
  }
}
TEST(BulkStreamingEdges, UnknownAndDuplicateAcksFailPermanently) {
  for (auto behavior : {BulkPeer::Behavior::kUnknown, BulkPeer::Behavior::kDuplicate}) {
    BulkPeer peer{behavior};
    auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
    ASSERT_TRUE(pair);
    auto batch = SampleBatch();
    auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
    ASSERT_TRUE(writer);
    ASSERT_TRUE((*writer)->WriteBatch(batch));
    auto finished = (*writer)->Finish();
    ASSERT_FALSE(finished);
    EXPECT_EQ(finished.error().Kind(), ErrorKind::kProtocol);
    const std::string diagnostic{finished.error().Message()};
    auto repeated = (*writer)->Finish();
    ASSERT_FALSE(repeated);
    EXPECT_EQ(repeated.error().Message(), diagnostic);
    auto later = (*writer)->WriteBatch(batch);
    ASSERT_FALSE(later);
    EXPECT_EQ(later.error().Message(), diagnostic);
    auto cancelled = (*writer)->Cancel();
    ASSERT_FALSE(cancelled);
    EXPECT_EQ(cancelled.error().Message(), diagnostic);
    auto after_cancel = (*writer)->Finish();
    ASSERT_FALSE(after_cancel);
    EXPECT_EQ(after_cancel.error().Message(), diagnostic);
    EXPECT_EQ(peer.batches, 1U);
    EXPECT_TRUE(peer.ok);
  }
}
TEST(BulkStreamingEdges, MissingAckIsNotSuccessfulEof) {
  BulkPeer peer{BulkPeer::Behavior::kMissing};
  auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto batch = SampleBatch();
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
  ASSERT_TRUE(writer);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  auto finished = (*writer)->Finish();
  ASSERT_FALSE(finished);
  EXPECT_EQ(finished.error().Kind(), ErrorKind::kProtocol);
  EXPECT_EQ((*writer)->InFlight(), 1U);
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, PrematureEofCannotAcceptAnotherBatch) {
  BulkPeer peer{BulkPeer::Behavior::kEarlyEof};
  auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto batch = SampleBatch();
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
  ASSERT_TRUE(writer);
  auto written = (*writer)->WriteBatch(batch);
  ASSERT_FALSE(written);
  EXPECT_EQ(written.error().Kind(), ErrorKind::kProtocol);
  EXPECT_EQ(peer.batches, 0U);
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, TrailerFailureSurvivesSuccessfulAcknowledgements) {
  BulkPeer peer{BulkPeer::Behavior::kDeniedTrailer};
  auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto batch = SampleBatch();
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
  ASSERT_TRUE(writer);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  auto finished = (*writer)->Finish();
  ASSERT_FALSE(finished);
  EXPECT_EQ(finished.error().GrpcCode(), grpcxx::StatusCode::kPermissionDenied);
  auto repeated = (*writer)->Finish();
  ASSERT_FALSE(repeated);
  EXPECT_EQ(repeated.error().GrpcCode(), grpcxx::StatusCode::kPermissionDenied);
  EXPECT_EQ(peer.batches, 1U);
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, TerminalTrailersDoNotHideQueuedAcknowledgements) {
  for (auto behavior : {BulkPeer::Behavior::kEarlySuccess, BulkPeer::Behavior::kEarlyDenied}) {
    BulkPeer peer{behavior};
    auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
    ASSERT_TRUE(pair);
    auto batch = SampleBatch();
    auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
    ASSERT_TRUE(writer);
    ASSERT_TRUE((*writer)->WriteBatch(batch));
    // Drive the final trailers into the RPC without consuming the queued ACK.
    for (unsigned tick = 0; tick < 16; ++tick)
      ASSERT_TRUE((*pair)->Channel().PollOnce());
    EXPECT_EQ((*writer)->InFlight(), 1U);
    auto finished = (*writer)->Finish();
    if (behavior == BulkPeer::Behavior::kEarlySuccess) {
      ASSERT_TRUE(finished);
      EXPECT_EQ(finished->total_affected_rows, 1U);
      EXPECT_EQ(finished->batches_sent, 1U);
      EXPECT_EQ((*writer)->InFlight(), 0U);
    } else {
      ASSERT_FALSE(finished);
      EXPECT_EQ(finished.error().GrpcCode(), grpcxx::StatusCode::kPermissionDenied);
      EXPECT_EQ(finished.error().Message(), "early denial");
    }
    EXPECT_EQ(peer.batches, 1U);
    EXPECT_TRUE(peer.ok);
  }
}
TEST(BulkStreamingEdges, LocalInvalidBatchLeavesRequestSequenceUsable) {
  BulkPeer peer;
  auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto batch = SampleBatch();
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
  ASSERT_TRUE(writer);
  auto malformed = batch;
  malformed.row_count = 2;
  auto invalid = (*writer)->WriteBatch(malformed);
  ASSERT_FALSE(invalid);
  EXPECT_EQ(invalid.error().Kind(), ErrorKind::kEncode);
  EXPECT_EQ((*writer)->InFlight(), 0U);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  auto finished = (*writer)->Finish();
  ASSERT_TRUE(finished);
  EXPECT_EQ(finished->batches_sent, 1U);
  EXPECT_EQ(finished->total_affected_rows, 1U);
  EXPECT_DOUBLE_EQ(finished->elapsed_secs, 0.25);
  EXPECT_EQ(peer.batches, 1U);
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, HourPartitionsPreserveNativeTimestampsAndCompression) {
  const std::vector<std::vector<std::int64_t>> expected_times{
      {-3600000000001LL}, {-1LL, -3600000000000LL}, {0LL}, {3600000000000LL}, {9007199254740993LL, 9007199254740994LL}};
  const std::vector<std::vector<std::int64_t>> expected_values{{3}, {1, 4}, {2}, {0}, {5, 6}};
  for (auto compression : {FlightCompression::kNone, FlightCompression::kLz4Frame, FlightCompression::kZstd}) {
    PartitionPeer peer;
    auto pair = grpcxx::InMemoryChannelPair::Create(PartitionPeer::Handle, &peer, {}, {}, 97);
    ASSERT_TRUE(pair);
    auto batch = HourBoundaryBatch();
    BulkWriteOptions options;
    options.partition_by_hour = true;
    options.max_in_flight = 1;
    options.compression = compression;
    auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema, options);
    ASSERT_TRUE(writer);
    ASSERT_TRUE((*writer)->WriteBatch(batch));
    auto finished = (*writer)->Finish();
    ASSERT_TRUE(finished);
    EXPECT_EQ(finished->total_affected_rows, 7U);
    EXPECT_EQ(finished->batches_sent, 5U);
    EXPECT_DOUBLE_EQ(finished->elapsed_secs, 0.625);
    ASSERT_EQ(peer.batches.size(), expected_times.size());
    ASSERT_EQ(peer.metadata.size(), expected_times.size());
    ASSERT_EQ(peer.compression.size(), expected_times.size());
    for (std::size_t group = 0; group < expected_times.size(); ++group) {
      const auto &decoded = peer.batches[group];
      ASSERT_EQ(decoded.columns.size(), 2U);
      EXPECT_EQ(decoded.row_count, expected_times[group].size());
      const auto &times = decoded.columns[0].values().timestamp_nanosecond_values();
      const auto &values = decoded.columns[1].values().i64_values();
      EXPECT_EQ(std::vector<std::int64_t>(times.begin(), times.end()), expected_times[group]);
      EXPECT_EQ(std::vector<std::int64_t>(values.begin(), values.end()), expected_values[group]);
      const auto bounds = std::minmax_element(expected_times[group].begin(), expected_times[group].end());
      EXPECT_EQ(peer.metadata[group].request_id, static_cast<std::int64_t>(group + 1));
      EXPECT_EQ(peer.metadata[group].min_timestamp, *bounds.first);
      EXPECT_EQ(peer.metadata[group].max_timestamp, *bounds.second);
      const auto expected_compression = compression == FlightCompression::kNone       ? ipc::Compression::kNone
                                        : compression == FlightCompression::kLz4Frame ? ipc::Compression::kLz4Frame
                                                                                      : ipc::Compression::kZstd;
      EXPECT_EQ(peer.compression[group], expected_compression);
    }
    EXPECT_EQ(peer.messages, 6U);
    EXPECT_TRUE(peer.ok);
  }
}
TEST(BulkStreamingEdges, InvalidOptionsFailBeforeOpeningAnyRpc) {
  PartitionPeer peer;
  auto pair = grpcxx::InMemoryChannelPair::Create(PartitionPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  const auto schema = SampleBatch().schema;
  BulkWriteOptions options;
  options.max_in_flight = 0;
  auto zero_capacity = BulkStreamWriter::Open((*pair)->Channel(), "samples", schema, options);
  ASSERT_FALSE(zero_capacity);
  EXPECT_EQ(zero_capacity.error().Kind(), ErrorKind::kInvalidConfig);
  options.max_in_flight = 1;
  options.compression = static_cast<FlightCompression>(255);
  EXPECT_FALSE(BulkStreamWriter::Open((*pair)->Channel(), "samples", schema, options));
  options.compression = FlightCompression::kNone;
  options.max_in_flight = 65537;
  EXPECT_FALSE(BulkStreamWriter::Open((*pair)->Channel(), "samples", schema, options));
  options.max_in_flight = 1;
  for (auto timeout : {std::chrono::nanoseconds{0}, std::chrono::nanoseconds{-1}}) {
    options.timeout = timeout;
    auto invalid = BulkStreamWriter::Open((*pair)->Channel(), "samples", schema, options);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().Kind(), ErrorKind::kInvalidConfig);
  }
  for (unsigned tick = 0; tick < 16; ++tick)
    ASSERT_TRUE((*pair)->Channel().PollOnce());
  EXPECT_EQ(peer.messages, 0U);
  EXPECT_EQ((*pair)->Server().ActiveStreams(), 0U);
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, TimeIndexMarkerAloneIncludesNativeBounds) {
  ::greptime::v1::Column column;
  column.set_column_name("ts");
  column.set_datatype(::greptime::v1::TIMESTAMP_SECOND);
  column.set_semantic_type(::greptime::v1::TIMESTAMP);
  column.mutable_values()->add_timestamp_second_values(std::numeric_limits<int64_t>::min());
  column.mutable_values()->add_timestamp_second_values(std::numeric_limits<int64_t>::max());
  auto batch = RecordBatchFromProtoColumns(std::span{&column, 1}, 2);
  ASSERT_TRUE(batch);
  batch->schema.fields[0].metadata = {{kTimeIndexKey, "true"}};
  PartitionPeer peer;
  auto pair = grpcxx::InMemoryChannelPair::Create(PartitionPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch->schema);
  ASSERT_TRUE(writer);
  ASSERT_TRUE((*writer)->WriteBatch(*batch));
  ASSERT_TRUE((*writer)->Finish());
  ASSERT_EQ(peer.metadata.size(), 1U);
  EXPECT_EQ(peer.metadata[0].min_timestamp, std::numeric_limits<int64_t>::min());
  EXPECT_EQ(peer.metadata[0].max_timestamp, std::numeric_limits<int64_t>::max());
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, FailedPartitionPrefixCannotResumeOrReplay) {
  PartitionPeer peer;
  peer.reject_second_batch = true;
  auto pair = grpcxx::InMemoryChannelPair::Create(PartitionPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto batch = HourBoundaryBatch();
  BulkWriteOptions options;
  options.partition_by_hour = true;
  options.max_in_flight = 1;
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema, options);
  ASSERT_TRUE(writer);
  auto rejected = (*writer)->WriteBatch(batch);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Kind(), ErrorKind::kProtocol);
  EXPECT_EQ(peer.batches.size(), 2U);
  auto repeated = (*writer)->WriteBatch(batch);
  ASSERT_FALSE(repeated);
  EXPECT_EQ(repeated.error().Message(), rejected.error().Message());
  auto finished = (*writer)->Finish();
  ASSERT_FALSE(finished);
  EXPECT_EQ(finished.error().Message(), rejected.error().Message());
  EXPECT_EQ(peer.batches.size(), 2U);
  EXPECT_TRUE(peer.ok);
}
TEST(BulkStreamingEdges, EarlyFailurePrecedesInvalidLocalBatch) {
  BulkPeer peer{BulkPeer::Behavior::kEarlyDenied};
  auto pair = grpcxx::InMemoryChannelPair::Create(BulkPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto batch = SampleBatch();
  auto writer = BulkStreamWriter::Open((*pair)->Channel(), "samples", batch.schema);
  ASSERT_TRUE(writer);
  ASSERT_TRUE((*writer)->WriteBatch(batch));
  for (unsigned tick = 0; tick < 16; ++tick)
    ASSERT_TRUE((*pair)->Channel().PollOnce());
  batch.schema.fields[0].name.clear();
  auto rejected = (*writer)->WriteBatch(batch);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().GrpcCode(), grpcxx::StatusCode::kPermissionDenied);
  EXPECT_EQ(peer.batches, 1U);
  EXPECT_TRUE(peer.ok);
}
TEST(QueryStreamingEdges, RejectsMalformedAndEmptyMetadata) {
  auto empty = MetadataFrame({});
  ExpectQueryFailure({empty}, ErrorKind::kProtocol, "empty Flight metadata");
  auto malformed = empty;
  malformed.set_app_metadata(std::string{char(0x0a), char(0xff)});
  ExpectQueryFailure({malformed}, ErrorKind::kDecode, "Flight metadata");
}
TEST(QueryStreamingEdges, RejectsBodyLengthMismatch) {
  auto metadata = MetadataFrame({});
  metadata.set_data_body("unexpected");
  ExpectQueryFailure({metadata}, ErrorKind::kProtocol, "body length");
}
TEST(QueryStreamingEdges, RejectsDuplicateSchemaAndBatchBeforeSchema) {
  auto batch = SampleBatch();
  auto schema = FlightIpcEncoder{}.EncodeSchema(batch.schema);
  auto encoded = FlightIpcEncoder{}.EncodeBatch(batch);
  ASSERT_TRUE(schema);
  ASSERT_TRUE(encoded);
  ExpectQueryFailure({Frame(*schema), Frame(*schema)}, ErrorKind::kProtocol, "duplicate Flight schema");
  ExpectQueryFailure({Frame(*encoded)}, ErrorKind::kProtocol, "preceded Flight schema");
}
TEST(QueryStreamingEdges, SchemaOnlyResponseCannotBypassSemanticValidation) {
  auto schema = SampleBatch().schema;
  schema.fields.push_back(schema.fields[0]);
  auto header = ipc::EncodeSchema(schema);
  ASSERT_TRUE(header);
  FlightData frame;
  frame.set_data_header(header->data(), header->size());
  ExpectQueryFailure({frame}, ErrorKind::kDecode, "duplicate column name");
}
} // namespace
} // namespace greptimedb::flight::testing
