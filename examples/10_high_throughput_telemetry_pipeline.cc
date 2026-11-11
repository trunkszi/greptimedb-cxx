/** Borrowed host/network telemetry capture into warmed SoA buffers, DoPut ACKs, and typed DoGet
 * replay. IPC, Protobuf and transport may allocate; only the warmed capture
 * storage is checked for stable capacity. The mock is driven on the same caller
 * thread. */
#include "example_support.h"
#include "offline_peer.h"

#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <print>

namespace db = greptimedb;
namespace flight = db::flight;
// clang-format off
struct HostTelemetry; struct[[= db::table{.name = "host_telemetry"}]] HostTelemetry {
  [[= db::tag{}]] std::string host;
  [[= db::tag{}]] std::string region;
  [[= db::timestamp{.unit =
                        db::TimestampUnit::kNanosecond}]] std::int64_t ts_ns;
  [[= db::field{}]] double cpu_usage;
  [[= db::field{}]] double mem_usage;
  [[= db::field{}]] std::uint64_t rx_bytes;
  [[= db::field{}]] std::uint64_t tx_bytes;
  [[= db::field{}]] std::uint64_t sequence;
  [[= db::field{}]] std::vector<std::byte> payload;
  // clang-format on
};
constexpr std::size_t kRows = 256;
constexpr std::int64_t kHour = 3600000000000;
constexpr std::array kPayload{std::byte{'R'}, std::byte{'X'}, std::byte{0}, std::byte{0xff}};
int Fill(db::TypedColumnarBatch<HostTelemetry> &batch) {
  for (std::size_t i = 0; i < kRows; ++i) {
    const db::BorrowedRow<HostTelemetry> captured{std::string_view{"host-01"},
                                                  std::string_view{"us-east"},
                                                  kHour - 128 + static_cast<std::int64_t>(i),
                                                  0.25,
                                                  0.5,
                                                  4096U,
                                                  8192U,
                                                  static_cast<std::uint64_t>(i),
                                                  db::BinaryView{kPayload}};
    EXAMPLE_REQUIRE(captured.Get<8>().data() == kPayload.data());
    EXAMPLE_REQUIRE(batch.PushRef(captured));
  }
  return 0;
}
int main() {
  db::TypedColumnarBatch<HostTelemetry> capture{kRows, db::BufferPolicy::Unbounded()};
  EXAMPLE_REQUIRE(capture.ReserveRows(kRows) && capture.ReserveBytes(kRows * 16));
  EXAMPLE_REQUIRE(Fill(capture) == 0);
  capture.ClearUnbounded();
  const auto bytes = capture.AllocatedBytes(), capacity = capture.RowCapacity();
  const auto started = std::chrono::steady_clock::now();
  EXAMPLE_REQUIRE(Fill(capture) == 0);
  const auto elapsed = std::chrono::steady_clock::now() - started;
  EXAMPLE_REQUIRE(capture.AllocatedBytes() == bytes && capture.RowCapacity() == capacity);
  double cpu_total = 0;
  for (const auto view : capture.Views())
    cpu_total += view.Get<3>();
  EXAMPLE_REQUIRE(cpu_total / kRows == 0.25);
  auto request = capture.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request);
  std::vector<greptime::v1::Column> columns(request->columns().begin(), request->columns().end());
  auto record = flight::RecordBatchFromProtoColumns(columns, request->row_count());
  EXAMPLE_REQUIRE(record);
  example::OfflinePeer peer;
  peer.table = "host_telemetry";
  peer.expected_compression = flight::ipc::Compression::kLz4Frame;
  peer.sql = "SELECT host, region, ts_ns, cpu_usage, mem_usage, rx_bytes, tx_bytes, "
             "sequence, payload FROM host_telemetry ORDER BY ts_ns";
  EXAMPLE_REQUIRE(peer.Open());
  flight::BulkWriteOptions options;
  options.compression = flight::FlightCompression::kLz4Frame;
  options.max_in_flight = 2;
  options.partition_by_hour = true;
  auto writer = flight::BulkStreamWriter::Open(peer.pair->Channel(), peer.table, record->schema, options);
  EXAMPLE_REQUIRE(writer && (*writer)->WriteRecordBatch(*record));
  auto committed = (*writer)->Finish();
  EXAMPLE_REQUIRE(committed && committed->batches_sent == 2 && committed->total_affected_rows == kRows);
  // The peer has decoded and acknowledged its independent copies before reuse.
  capture.ClearUnbounded();
  EXAMPLE_REQUIRE(capture.RowCount() == 0 && capture.AllocatedBytes() == bytes);
  auto query = flight::QueryStream::Sql(peer.pair->Channel(), {}, peer.sql);
  EXAMPLE_REQUIRE(query);
  std::size_t replayed = 0;
  while (true) {
    auto next = (*query)->NextBatch();
    EXAMPLE_REQUIRE(next);
    if (!next->has_value()) break;
    const auto &batch = **next;
    for (std::size_t row = 0; row < batch.row_count; ++row) {
      auto view = db::ViewDecodedRow<HostTelemetry>(batch.columns, row, batch.row_count);
      EXAMPLE_REQUIRE(view);
      EXAMPLE_REQUIRE(view->Get<0>() == "host-01" && view->Get<1>() == "us-east");
      EXAMPLE_REQUIRE(view->Get<2>() == kHour - 128 + static_cast<std::int64_t>(replayed));
      EXAMPLE_REQUIRE(view->Get<3>() == 0.25 && view->Get<4>() == 0.5);
      EXAMPLE_REQUIRE(view->Get<5>() == 4096U && view->Get<6>() == 8192U);
      EXAMPLE_REQUIRE(view->Get<7>() == replayed);
      EXAMPLE_REQUIRE(std::ranges::equal(view->Get<8>(), kPayload));
      EXAMPLE_REQUIRE(view->Get<0>().data() == batch.columns[0].values().string_values(row).data());
      ++replayed;
    }
  }
  EXAMPLE_REQUIRE((*query)->Finish());
  EXAMPLE_REQUIRE(replayed == kRows && !peer.failed);
  std::println("Host telemetry capture ns={}; 256 ACKed rows in two hourly batches, typed replay cpu_usage=0.25; "
               "retained bytes={}",
               std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count(), bytes);
  return 0;
}
