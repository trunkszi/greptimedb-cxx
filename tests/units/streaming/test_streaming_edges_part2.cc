#include "streaming_edges_support.h"

namespace greptimedb::flight::testing {
namespace {
TEST(QueryStreamingEdges, BatchMetadataIsCollectedAndMalformedMetadataFails) {
  const auto batch = SampleBatch();
  auto schema = FlightIpcEncoder().EncodeSchema(batch.schema);
  auto body = FlightIpcEncoder().EncodeBatch(batch);
  ASSERT_TRUE(schema);
  ASSERT_TRUE(body);
  auto data = Frame(*body);
  ::greptime::v1::FlightMetadata metadata;
  metadata.mutable_affected_rows()->set_value(9);
  metadata.mutable_metrics()->set_metrics("batch metrics");
  ASSERT_TRUE(metadata.SerializeToString(data.mutable_app_metadata()));
  QueryPeer peer{{Frame(*schema), data}};
  auto pair = grpcxx::InMemoryChannelPair::Create(QueryPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto stream = QueryStream::Sql((*pair)->Channel(), {}, "SELECT value FROM samples");
  ASSERT_TRUE(stream);
  auto first = (*stream)->NextBatch();
  ASSERT_TRUE(first);
  ASSERT_TRUE(*first);
  EXPECT_EQ((*stream)->AffectedRows(), 9U);
  ASSERT_EQ((*stream)->Metrics().size(), 1U);
  EXPECT_EQ((*stream)->Metrics()[0], "batch metrics");
  ASSERT_TRUE((*stream)->Finish());
  data.set_app_metadata(std::string(1, '\xff'));
  ExpectQueryFailure({Frame(*schema), data}, ErrorKind::kDecode, "Flight metadata");
}
TEST(QueryStreamingEdges, MetadataOnlyDmlCollectsAllRowsAndMetricsUntilEof) {
  ::greptime::v1::FlightMetadata first;
  first.mutable_affected_rows()->set_value(12);
  ::greptime::v1::FlightMetadata second;
  second.mutable_metrics()->set_metrics("{\"elapsed\":1}");
  ::greptime::v1::FlightMetadata third;
  third.mutable_affected_rows()->set_value(7);
  QueryPeer peer{{MetadataFrame(first), MetadataFrame(second), MetadataFrame(third)}};
  auto pair = grpcxx::InMemoryChannelPair::Create(QueryPeer::Handle, &peer, {}, {}, 97);
  ASSERT_TRUE(pair);
  auto stream = QueryStream::Sql((*pair)->Channel(), {}, "DELETE FROM samples");
  ASSERT_TRUE(stream);
  auto next = (*stream)->NextBatch();
  ASSERT_TRUE(next);
  EXPECT_FALSE(*next);
  EXPECT_FALSE((*stream)->Schema());
  EXPECT_EQ((*stream)->AffectedRows(), 19U);
  ASSERT_EQ((*stream)->Metrics().size(), 1U);
  EXPECT_EQ((*stream)->Metrics().front(), "{\"elapsed\":1}");
  auto finished = (*stream)->Finish();
  ASSERT_TRUE(finished);
  EXPECT_EQ(finished->affected_rows, 19U);
  EXPECT_EQ(finished->metrics, (*stream)->Metrics());
  EXPECT_TRUE(peer.ok);
}
} // namespace
} // namespace greptimedb::flight::testing
