/** Borrowed L1 capture into warmed SoA buffers, DoPut ACKs, and typed DoGet
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

namespace db = greptimedb;
namespace flight = db::flight;
struct[[= db::table{.name = "l1_book_ticks"}]] L1BookTick {
  [[= db::tag{}]] std::string symbol;
  [[= db::tag{}]] std::string venue;
  [[= db::timestamp{.unit =
                        db::TimestampUnit::kNanosecond}]] std::int64_t ts_ns;
  [[= db::field{}]] double bid_px;
  [[= db::field{}]] double ask_px;
  [[= db::field{}]] double bid_sz;
  [[= db::field{}]] double ask_sz;
  [[= db::field{}]] std::uint64_t sequence;
  [[= db::field{}]] std::vector<std::byte> payload;
};
constexpr std::size_t kRows = 256;
constexpr std::int64_t kHour = 3600000000000;
constexpr std::array kPayload{std::byte{'L'}, std::byte{'1'}, std::byte{0},
                              std::byte{0xff}};
int Fill(db::TypedColumnarBatch<L1BookTick> &batch) {
  for (std::size_t i = 0; i < kRows; ++i) {
    const db::BorrowedRow<L1BookTick> captured{std::string_view{"BTCUSDT"},
                                               std::string_view{"example"},
                                               kHour - 128 +
                                                   static_cast<std::int64_t>(i),
                                               100.0,
                                               100.5,
                                               2.0,
                                               3.0,
                                               static_cast<std::uint64_t>(i),
                                               db::BinaryView{kPayload}};
    EXAMPLE_REQUIRE(captured.Get<8>().data() == kPayload.data());
    EXAMPLE_REQUIRE(batch.PushRef(captured));
  }
  return 0;
}
int main() {
  db::TypedColumnarBatch<L1BookTick> capture{kRows,
                                             db::BufferPolicy::Unbounded()};
  EXAMPLE_REQUIRE(capture.ReserveRows(kRows) &&
                  capture.ReserveBytes(kRows * 16));
  EXAMPLE_REQUIRE(Fill(capture) == 0);
  capture.ClearUnbounded();
  const auto bytes = capture.AllocatedBytes(), capacity = capture.RowCapacity();
  const auto started = std::chrono::steady_clock::now();
  EXAMPLE_REQUIRE(Fill(capture) == 0);
  const auto elapsed = std::chrono::steady_clock::now() - started;
  EXAMPLE_REQUIRE(capture.AllocatedBytes() == bytes &&
                  capture.RowCapacity() == capacity);
  double spread = 0;
  for (const auto view : capture.Views())
    spread += view.Get<4>() - view.Get<3>();
  EXAMPLE_REQUIRE(spread / kRows == 0.5);
  auto request = capture.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request);
  std::vector<greptime::v1::Column> columns(request->columns().begin(),
                                            request->columns().end());
  auto record =
      flight::RecordBatchFromProtoColumns(columns, request->row_count());
  EXAMPLE_REQUIRE(record);
  example::OfflinePeer peer;
  peer.table = "l1_book_ticks";
  peer.expected_compression = flight::ipc::Compression::kLz4Frame;
  peer.sql = "SELECT symbol, venue, ts_ns, bid_px, ask_px, bid_sz, ask_sz, "
             "sequence, payload FROM l1_book_ticks ORDER BY ts_ns";
  EXAMPLE_REQUIRE(peer.Open());
  flight::BulkWriteOptions options;
  options.compression = flight::FlightCompression::kLz4Frame;
  options.max_in_flight = 2;
  options.partition_by_hour = true;
  auto writer = flight::BulkStreamWriter::Open(peer.pair->channel(), peer.table,
                                               record->schema, options);
  EXAMPLE_REQUIRE(writer && (*writer)->WriteRecordBatch(*record));
  auto committed = (*writer)->Finish();
  EXAMPLE_REQUIRE(committed && committed->batches_sent == 2 &&
                  committed->total_affected_rows == kRows);
  // The peer has decoded and acknowledged its independent copies before reuse.
  capture.ClearUnbounded();
  EXAMPLE_REQUIRE(capture.RowCount() == 0 && capture.AllocatedBytes() == bytes);
  auto query = flight::QueryStream::Sql(peer.pair->channel(), {}, peer.sql);
  EXAMPLE_REQUIRE(query);
  std::size_t replayed = 0;
  while (true) {
    auto next = (*query)->NextBatch();
    EXAMPLE_REQUIRE(next);
    if (!next->has_value())
      break;
    const auto &batch = **next;
    for (std::size_t row = 0; row < batch.row_count; ++row) {
      auto view =
          db::ViewDecodedRow<L1BookTick>(batch.columns, row, batch.row_count);
      EXAMPLE_REQUIRE(view);
      EXAMPLE_REQUIRE(view->Get<0>() == "BTCUSDT" &&
                      view->Get<1>() == "example");
      EXAMPLE_REQUIRE(view->Get<2>() ==
                      kHour - 128 + static_cast<std::int64_t>(replayed));
      EXAMPLE_REQUIRE(view->Get<3>() == 100.0 && view->Get<4>() == 100.5);
      EXAMPLE_REQUIRE(view->Get<5>() == 2.0 && view->Get<6>() == 3.0);
      EXAMPLE_REQUIRE(view->Get<7>() == replayed);
      EXAMPLE_REQUIRE(std::ranges::equal(view->Get<8>(), kPayload));
      EXAMPLE_REQUIRE(view->Get<0>().data() ==
                      batch.columns[0].values().string_values(row).data());
      ++replayed;
    }
  }
  EXAMPLE_REQUIRE((*query)->Finish());
  EXAMPLE_REQUIRE(replayed == kRows && !peer.failed);
  std::cout
      << "L1 capture ns="
      << std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()
      << "; 256 ACKed rows in two hourly batches, typed replay spread=0.5; "
         "retained bytes="
      << bytes << '\n';
  return 0;
}
