/** Client streaming with bounded buffers, finish acknowledgements and recovery.
 * Set GREPTIMEDB_ENDPOINT to also insert into example_stream_ticks on a live
 * DB. */
#include "example_support.h"
#include "offline_peer.h"

#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/core/hints.h>

#include <array>
#include <cstdlib>
#include <string>

namespace db = greptimedb;
int Configure(db::ColumnarBatch &batch,
              std::string_view label = "offline-host") {
  EXAMPLE_REQUIRE(
      batch.AddTimestampColumn("ts", db::TimestampUnit::kMillisecond));
  EXAMPLE_REQUIRE(batch.AddTagColumn("host", db::ColumnDataType::kString));
  EXAMPLE_REQUIRE(batch.AddFieldColumn("value", db::ColumnDataType::kFloat64));
  const std::array<db::ValueView, 3> row{
      db::TimestampMillisecond{1700000000000}, label, 42.0};
  EXAMPLE_REQUIRE(batch.PushRow(row));
  return 0;
}
int Ingestion(db::SynchronousChannel &channel) {
  db::ColumnarBatch first("example_stream_ticks", 8),
      second("example_stream_other", 8);
  EXAMPLE_REQUIRE(Configure(first) == 0 && Configure(second) == 0);
  auto one = first.EncodeInsertRequest(), two = second.EncodeInsertRequest();
  EXAMPLE_REQUIRE(one && two);
  db::RowBatch rows("example_stream_ticks", 8);
  EXAMPLE_REQUIRE(
      rows.AddTimestampColumn("ts", db::TimestampUnit::kMillisecond));
  EXAMPLE_REQUIRE(rows.AddTagColumn("host", db::ColumnDataType::kString));
  EXAMPLE_REQUIRE(rows.AddFieldColumn("value", db::ColumnDataType::kFloat64));
  const std::array<db::ValueView, 3> row{
      db::TimestampMillisecond{1700000000001}, std::string_view{"offline-host"},
      43.0};
  EXAMPLE_REQUIRE(rows.PushRow(row));
  auto row_request = rows.EncodeRowInsertRequest();
  auto deletion = first.EncodeDeleteRequest();
  EXAMPLE_REQUIRE(row_request && deletion);
  auto opened = db::StreamInserter::Open(channel);
  EXAMPLE_REQUIRE(opened);
  auto &writer = **opened;
  EXAMPLE_REQUIRE(writer.SendBatch(*one));
  EXAMPLE_REQUIRE(writer.SendBatchTo("example_stream_other", *one));
  const std::array requests{*one, *two};
  EXAMPLE_REQUIRE(writer.SendBatches(requests));
  db::MultiTableBatch multi;
  EXAMPLE_REQUIRE(multi.AddBatch(first) && multi.AddBatch(second));
  EXAMPLE_REQUIRE(writer.SendBatch(multi.IntoInsertRequests()));
  EXAMPLE_REQUIRE(writer.SendRows(*row_request));
  EXAMPLE_REQUIRE(writer.SendDeleteBatch(*deletion));
  EXAMPLE_REQUIRE(writer.SendBatch(*one));
  first.ClearUnbounded();
  EXAMPLE_REQUIRE(first.IsEmpty() && first.RowCapacity() > 0);
  EXAMPLE_REQUIRE(writer.SendBatch(*two));
  second.ClearWithPolicy(db::BufferPolicy::Zero());
  EXAMPLE_REQUIRE(second.IsEmpty() && second.RowCapacity() == 0);
  EXAMPLE_REQUIRE(writer.BatchesSent() == 8 && writer.RowsSent() == 10);
  auto affected = writer.Finish();
  EXAMPLE_REQUIRE(affected && *affected == 10);
  std::cout << "Finish acknowledged 8 messages / 10 rows; send alone was not a "
               "commit ACK\n";
  return 0;
}
int BoundedProducer(db::SynchronousChannel &channel) {
  const std::string label(128, 'x');
  db::ColumnarBatch batch("example_stream_ticks", 512,
                          db::BufferPolicy::Unbounded());
  EXAMPLE_REQUIRE(Configure(batch, label) == 0);
  for (std::int64_t i = 1; i < 512; ++i) {
    const std::array<db::ValueView, 3> row{
        db::TimestampMillisecond{1700000000000 + i}, std::string_view{label},
        42.0};
    EXAMPLE_REQUIRE(batch.PushRow(row));
  }
  const auto bytes = batch.AllocatedBytes();
  auto request = batch.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request && request->ByteSizeLong() > 65535);
  auto opened = db::StreamInserter::Open(channel);
  EXAMPLE_REQUIRE(opened);
  for (unsigned i = 0; i < 12; ++i)
    EXAMPLE_REQUIRE((*opened)->SendBatch(*request));
  auto affected = (*opened)->Finish();
  EXAMPLE_REQUIRE(affected && *affected == 6144);
  EXAMPLE_REQUIRE(batch.RowCount() == 512 && batch.AllocatedBytes() == bytes);
  std::cout
      << "HTTP/2 flow-controlled 6144 rows using one reusable 512-row source\n";
  return 0;
}
int Recovery(example::OfflinePeer &peer) {
  db::ColumnarBatch retained("_reject", 8);
  EXAMPLE_REQUIRE(Configure(retained) == 0);
  auto snapshot = retained.EncodeInsertRequest();
  EXAMPLE_REQUIRE(snapshot);
  {
    auto stream = db::StreamInserter::Open(peer.pair->channel());
    EXAMPLE_REQUIRE(stream);
    auto sent = (*stream)->SendBatch(*snapshot);
    auto finished = (*stream)->Finish();
    EXAMPLE_REQUIRE(!finished);
    std::cout << "Rejected stream: send accepted=" << bool(sent)
              << "; caller snapshot retained\n";
  }
  auto unchanged = retained.EncodeInsertRequest();
  EXAMPLE_REQUIRE(unchanged && unchanged->SerializeAsString() ==
                                   snapshot->SerializeAsString());
  EXAMPLE_REQUIRE(peer.rejected == 1);
  {
    auto stream = db::StreamInserter::Open(peer.pair->channel());
    EXAMPLE_REQUIRE(stream);
    const auto received = peer.insert_messages;
    EXAMPLE_REQUIRE((*stream)->SendBatchTo("cancelled", *snapshot));
    for (unsigned i = 0; i < 64 && peer.insert_messages == received; ++i)
      peer.Pump();
    EXAMPLE_REQUIRE(peer.insert_messages == received + 1);
    EXAMPLE_REQUIRE((*stream)->Cancel());
  }
  peer.Pump();
  EXAMPLE_REQUIRE(peer.pair->server().CancelledStreams() >= 1);
  auto next = db::StreamInserter::Open(peer.pair->channel());
  EXAMPLE_REQUIRE(next && (*next)->SendBatchTo("after_cancel", *snapshot));
  auto recovered = (*next)->Finish();
  EXAMPLE_REQUIRE(recovered && *recovered == 1);
  std::cout << "Cancellation released the channel; uncertain writes were not "
               "replayed\n";
  return 0;
}
int Live(std::string endpoint) {
  if (endpoint.find("://") == std::string::npos)
    endpoint.insert(0, "grpc://");
  auto channel = db::SynchronousChannel::Connect(endpoint);
  EXAMPLE_REQUIRE(channel);
  db::ColumnarBatch batch("example_stream_ticks", 1);
  EXAMPLE_REQUIRE(Configure(batch, "cpp-example") == 0);
  auto request = batch.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request);
  greptime::v1::RequestHeader header;
  header.set_catalog("greptime");
  header.set_schema("public");
  header.set_dbname("greptime-public");
  db::CallOptions options;
  EXAMPLE_REQUIRE(
      options.metadata.Add(db::kGreptimeHintsHeader, "auto_create_table=true"));
  auto stream = db::StreamInserter::Open(**channel, header, std::move(options));
  EXAMPLE_REQUIRE(stream && (*stream)->SendBatch(*request));
  auto affected = (*stream)->Finish();
  EXAMPLE_REQUIRE(affected);
  std::cout << "Live example_stream_ticks affected rows=" << *affected << '\n';
  return 0;
}
int main() {
  example::OfflinePeer peer;
  EXAMPLE_REQUIRE(peer.Open());
  EXAMPLE_REQUIRE(Ingestion(peer.pair->channel()) == 0);
  EXAMPLE_REQUIRE(BoundedProducer(peer.pair->channel()) == 0);
  EXAMPLE_REQUIRE(Recovery(peer) == 0);
  EXAMPLE_REQUIRE(!peer.failed);
  if (const char *endpoint = std::getenv("GREPTIMEDB_ENDPOINT");
      endpoint && *endpoint)
    return Live(endpoint);
  std::cout << "Live ingestion skipped (GREPTIMEDB_ENDPOINT unset)\n";
  return 0;
}
