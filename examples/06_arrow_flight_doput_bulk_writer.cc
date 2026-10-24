/** Compressed DoPut with schema ACK, a bounded ACK window and hourly batches.
 */
#include "example_support.h"
#include "offline_peer.h"

#include <greptimedb/row/reflected_row.h>

namespace db = greptimedb;
namespace flight = db::flight;
struct[[= db::table{.name = "bulk_readings"}]] Reading {
  [[= db::timestamp{.unit = db::TimestampUnit::kMillisecond}]] std::int64_t ts;
  [[= db::tag{}]] std::string sensor;
  [[= db::field{}]] double value;
};
int Run(flight::FlightCompression compression) {
  db::TypedColumnarBatch<Reading> rows{3};
  EXAMPLE_REQUIRE(rows.ReserveRows(3) && rows.ReserveBytes(64));
  for (std::int64_t hour = 0; hour < 3; ++hour)
    EXAMPLE_REQUIRE(rows.PushRef(db::BorrowedRow<Reading>{
        hour * 3600000, std::string_view{"sensor-a"}, 10.0 + hour}));
  auto request = rows.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request);
  std::vector<greptime::v1::Column> columns(request->columns().begin(),
                                            request->columns().end());
  auto record =
      flight::RecordBatchFromProtoColumns(columns, request->row_count());
  EXAMPLE_REQUIRE(record);
  auto partitions = flight::PartitionRecordBatchByHour(*record);
  EXAMPLE_REQUIRE(partitions && partitions->size() == 3);
  example::OfflinePeer peer;
  peer.table = "bulk_readings";
  peer.defer_first_bulk_ack = true;
  peer.expected_compression =
      compression == flight::FlightCompression::kLz4Frame
          ? flight::ipc::Compression::kLz4Frame
          : flight::ipc::Compression::kZstd;
  EXAMPLE_REQUIRE(peer.Open());
  flight::BulkWriteOptions options;
  options.compression = compression;
  options.max_in_flight = 2;
  options.partition_by_hour = true;
  auto opened = flight::BulkStreamWriter::Open(peer.pair->channel(), peer.table,
                                               record->schema, options);
  EXAMPLE_REQUIRE(opened);
  EXAMPLE_REQUIRE(peer.schema_handshakes == 1);
  auto &writer = **opened;
  for (unsigned i = 0; i < 3; ++i) {
    EXAMPLE_REQUIRE(writer.WriteRecordBatch(*record));
    EXAMPLE_REQUIRE(writer.InFlight() <= options.max_in_flight);
  }
  auto stats = writer.Finish();
  EXAMPLE_REQUIRE(stats && stats->batches_sent == 9 &&
                  stats->total_affected_rows == 9);
  EXAMPLE_REQUIRE(writer.InFlight() == 0);
  EXAMPLE_REQUIRE(peer.bulk_batches == 9 && peer.bulk_rows == 9 &&
                  !peer.failed);
  EXAMPLE_REQUIRE(peer.deferred_ack_pairs == 1);
  for (const auto &batch : peer.stored)
    EXAMPLE_REQUIRE(batch.row_count == 1);
  std::cout << (compression == flight::FlightCompression::kLz4Frame
                    ? "LZ4_FRAME"
                    : "ZSTD")
            << ": schema request 0 ACKed, nine hourly batches ACKed, "
               "max_in_flight=2\n";
  return 0;
}
int main() {
  EXAMPLE_REQUIRE(Run(flight::FlightCompression::kLz4Frame) == 0);
  EXAMPLE_REQUIRE(Run(flight::FlightCompression::kZstd) == 0);
  return 0;
}
