/** Annotated rows, borrowed input, owned SoA storage, and wire-backed views.
 * Source views borrow their owner; IPC encoding and Protobuf conversion copy.
 */
#include "example_support.h"

#include <greptimedb/flight/ipc_codec.h>
#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>

#include <algorithm>
#include <array>
#include <optional>
#include <print>
#include <string>
#include <vector>

namespace db = greptimedb;
namespace flight = db::flight;
// clang-format off
struct MetricSample; struct[[= db::table{.name = "metric_samples"}]] MetricSample {
  [[= db::tag{}]] std::string host;
  [[= db::timestamp{.unit = db::TimestampUnit::kNanosecond}]] std::int64_t ts;
  [[= db::field{}]] double cpu_usage;
  [[= db::field{}]] std::optional<double> size;
  [[= db::field{.json = true}]] std::string payload;
  [[= db::field{.decimal_precision = 18,
                .decimal_scale = 4}]] __int128 coefficient;
  [[= db::field{}]] std::vector<std::byte> blob;
  // clang-format on
};

// ViewDecodedRow validates schema order. A fixed pointer index adapts reordered
// wire columns by name without copying any Protobuf payloads.
struct ColumnOrderView {
  std::array<const greptime::v1::Column *, 7> columns{};
  std::size_t size() const { return columns.size(); }
  const greptime::v1::Column &operator[](std::size_t i) const { return *columns[i]; }
};

int main() {
  static_assert(db::RowConcept<MetricSample>);
  static_assert(db::TypedColumnarBatch<MetricSample>::kColumnCount == 7);
  MetricSample source{"host-01", 1'700'000'000'000'000'000,      100.25, 1.5, "{\"cpu_usage\":100.25}",
                      123456,    {std::byte{0}, std::byte{0xff}}};
  const db::BorrowedRow<MetricSample> borrowed{source};
  const db::RowView<MetricSample> source_view{source};
  EXAMPLE_REQUIRE(borrowed.Get<0>().data() == source.host.data());
  EXAMPLE_REQUIRE(source_view.Get<6>().data() == source.blob.data());
  db::TypedColumnarBatch<MetricSample> batch{3, db::BufferPolicy::Unbounded()};
  EXAMPLE_REQUIRE(batch.ReserveRows(3));
  EXAMPLE_REQUIRE(batch.ReserveBytes(256));
  const auto bytes = batch.AllocatedBytes();
  EXAMPLE_REQUIRE(batch.PushRef(borrowed));
  EXAMPLE_REQUIRE(batch.PushRef(db::BorrowedRow<MetricSample>{std::string_view{"host-02"}, source.ts + 1, 2000.5,
                                                              std::optional<double>{}, std::string_view{"{}"},
                                                              static_cast<__int128>(-987654), db::BinaryView{}}));
  EXAMPLE_REQUIRE(batch.PushRef(db::BorrowedRow<MetricSample>{std::string_view{"host-03"}, source.ts + 2, 1.25,
                                                              std::optional<double>{0.0}, std::string_view{"{}"},
                                                              static_cast<__int128>(0), db::BinaryView{}}));
  EXAMPLE_REQUIRE(batch.AllocatedBytes() == bytes);
  EXAMPLE_REQUIRE(batch.RowCount() == 3);
  auto row = batch.GetRow(1);
  EXAMPLE_REQUIRE(row && !row->Get<3>());
  EXAMPLE_REQUIRE(row->Get<"host">() == "host-02");
  EXAMPLE_REQUIRE(!batch.GetRow(3));
  std::size_t seen = 0;
  for (const auto view : batch.Views()) {
    EXAMPLE_REQUIRE(view.Get<1>() == source.ts + static_cast<std::int64_t>(seen));
    ++seen;
  }
  EXAMPLE_REQUIRE(seen == 3);
  auto request = batch.EncodeInsertRequest();
  EXAMPLE_REQUIRE(request && request->table_name() == "metric_samples");
  EXAMPLE_REQUIRE(request->columns(3).null_mask() == std::string(1, '\x02'));
  greptime::v1::InsertRequest wire;
  EXAMPLE_REQUIRE(wire.ParseFromString(request->SerializeAsString()));
  // ViewDecodedRow is this SDK's wire-backed RowView<T> constructor.
  auto wire_view = db::ViewDecodedRow<MetricSample>(wire.columns(), 0, wire.row_count());
  EXAMPLE_REQUIRE(wire_view && wire_view->Get<0>() == "host-01");
  EXAMPLE_REQUIRE(wire_view->Get<0>().data() == wire.columns(0).values().string_values(0).data());
  EXAMPLE_REQUIRE(wire_view->Get<6>().size() == 2);
  std::reverse(wire.mutable_columns()->begin(), wire.mutable_columns()->end());
  EXAMPLE_REQUIRE(!db::ViewDecodedRow<MetricSample>(wire.columns(), 1, wire.row_count()));
  ColumnOrderView ordered;
  for (std::size_t i = 0; i < ordered.size(); ++i) {
    for (const auto &column : wire.columns())
      if (column.column_name() == db::TypedColumnarBatch<MetricSample>::kColumns[i].name) ordered.columns[i] = &column;
    EXAMPLE_REQUIRE(ordered.columns[i] != nullptr);
  }
  auto by_name = db::ViewDecodedRow<MetricSample>(ordered, 1, wire.row_count());
  EXAMPLE_REQUIRE(by_name && by_name->Get<0>() == "host-02" && !by_name->Get<3>());

  std::vector<greptime::v1::Column> columns(request->columns().begin(), request->columns().end());
  // RecordBatchFromProtoColumns and DecodeFlightRecordBatch are the actual
  // public Protobuf-to-Arrow and Arrow-to-Protobuf conversion entry points.
  auto arrow = flight::RecordBatchFromProtoColumns(columns, request->row_count());
  EXAMPLE_REQUIRE(arrow);
  flight::FlightIpcEncoder encoder(flight::FlightCompression::kLz4Frame);
  auto schema = encoder.EncodeSchema(arrow->schema);
  auto frame = encoder.EncodeBatch(*arrow);
  EXAMPLE_REQUIRE(schema && frame);
  auto decoded_schema = flight::DecodeFlightSchema(schema->data_header);
  EXAMPLE_REQUIRE(decoded_schema);
  auto restored = flight::DecodeFlightRecordBatch(*decoded_schema, frame->data_header, frame->data_body);
  EXAMPLE_REQUIRE(restored && restored->row_count == 3 && restored->columns.size() == 7);
  EXAMPLE_REQUIRE(restored->columns[3].null_mask() == std::string(1, '\x02'));
  EXAMPLE_REQUIRE(restored->columns[4].values().binary_values(0) == source.payload);
  EXAMPLE_REQUIRE(restored->columns[4].datatype() == greptime::v1::JSON);
  EXAMPLE_REQUIRE(restored->schema.fields[4].type.id == flight::FlightTypeId::kBinary);
  EXAMPLE_REQUIRE(restored->schema.fields[5].type.decimal_precision == 18);
  EXAMPLE_REQUIRE(restored->schema.fields[5].type.scale == 4);
  batch.ClearUnbounded();
  EXAMPLE_REQUIRE(batch.RowCount() == 0 && batch.AllocatedBytes() == bytes);
  std::println("3 annotated rows: borrowed capture, retained SoA buffers, "
               "name-bound wire views, and compressed Arrow round-trip verified");
  return 0;
}
