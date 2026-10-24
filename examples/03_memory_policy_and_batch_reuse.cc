/** Measure capacity retention and release across every reusable batch family.
 */
#include "example_support.h"

#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/row/reflected_row.h>

#include <array>
#include <string>

namespace db = greptimedb;
struct[[= db::table{.name = "sensors"}]] Reading {
  [[= db::tag{}]] std::uint32_t rack;
  [[= db::timestamp{}]] db::TimestampNanosecond ts;
  [[= db::field{}]] double value;
};
template <db::MemoryBufferConcept T>
void Snapshot(const char *label, const T &buffer) {
  std::cout << label << ": capacity=" << buffer.RowCapacity()
            << " bytes=" << buffer.AllocatedBytes() << '\n';
}
int main() {
  constexpr db::BufferPolicy tight{16, 1024};
  static_assert(!tight.ShouldShrink(16, 1024));
  static_assert(tight.ShouldShrink(17, 1024));
  static_assert(tight.ShouldShrink(16, 1025));
  db::TypedColumnBuf scalar("value", db::SemanticType::kField,
                            db::ColumnDataType::kInt64);
  EXAMPLE_REQUIRE(scalar.ReserveRows(256));
  for (std::int64_t i = 0; i < 64; ++i)
    EXAMPLE_REQUIRE(scalar.Push(db::ValueView{i}));
  const auto capacity = scalar.RowCapacity(), bytes = scalar.AllocatedBytes();
  scalar.ClearUnbounded();
  EXAMPLE_REQUIRE(scalar.RowCount() == 0 && scalar.RowCapacity() == capacity);
  EXAMPLE_REQUIRE(scalar.AllocatedBytes() == bytes);
  Snapshot("typed scalar retained", scalar);
  scalar.ClearWithPolicy(db::BufferPolicy::Zero());
  EXAMPLE_REQUIRE(scalar.RowCapacity() == 0 && scalar.AllocatedBytes() == 0);

  db::ColumnarBatch columns("ticks", 256, db::BufferPolicy::Unbounded());
  EXAMPLE_REQUIRE(columns.AddTagColumn("symbol", db::ColumnDataType::kString));
  EXAMPLE_REQUIRE(
      columns.AddTimestampColumn("ts", db::TimestampUnit::kMillisecond));
  EXAMPLE_REQUIRE(
      columns.AddFieldColumn("price", db::ColumnDataType::kFloat64));
  const std::string payload(128, 'x');
  const std::array<db::ValueView, 3> values{std::string_view{payload},
                                            db::TimestampMillisecond{1}, 1.5};
  for (unsigned i = 0; i < 64; ++i)
    EXAMPLE_REQUIRE(columns.PushRow(values));
  const auto pooled = columns.AllocatedBytes();
  columns.ClearUnbounded();
  EXAMPLE_REQUIRE(columns.IsEmpty() && columns.AllocatedBytes() >= pooled);
  for (unsigned i = 0; i < 64; ++i)
    EXAMPLE_REQUIRE(columns.PushRow(values));
  EXAMPLE_REQUIRE(columns.AllocatedBytes() == pooled);
  Snapshot("columnar warmed payloads", columns);
  columns.ShrinkToFit();
  EXAMPLE_REQUIRE(columns.RowCount() == 64 && columns.RowCapacity() == 64);

  db::RowBatch rows("ticks", 256, db::BufferPolicy::Unbounded());
  EXAMPLE_REQUIRE(rows.AddTagColumn("symbol", db::ColumnDataType::kString));
  EXAMPLE_REQUIRE(
      rows.AddTimestampColumn("ts", db::TimestampUnit::kMillisecond));
  EXAMPLE_REQUIRE(rows.AddFieldColumn("price", db::ColumnDataType::kFloat64));
  for (unsigned i = 0; i < 64; ++i)
    EXAMPLE_REQUIRE(rows.PushRow(values));
  const auto row_capacity = rows.RowCapacity(),
             row_bytes = rows.AllocatedBytes();
  rows.ClearUnbounded();
  EXAMPLE_REQUIRE(rows.IsEmpty() && rows.RowCapacity() == row_capacity);
  EXAMPLE_REQUIRE(rows.AllocatedBytes() == row_bytes);
  Snapshot("row batch retained", rows);
  EXAMPLE_REQUIRE(rows.PushRow(values));
  rows.ClearWithPolicy(tight);
  EXAMPLE_REQUIRE(rows.RowCapacity() == 0 && rows.AllocatedBytes() == 0);

  db::MultiTableBatch multi;
  EXAMPLE_REQUIRE(multi.AddBatch(columns));
  EXAMPLE_REQUIRE(multi.AddBatchTo("archive", columns));
  EXAMPLE_REQUIRE(multi.TableCount() == 2 && multi.TotalRows() == 128);
  multi.Clear();
  EXAMPLE_REQUIRE(multi.TableCount() == 0 && multi.TotalRows() == 0);
  EXAMPLE_REQUIRE(multi.AddBatch(columns));
  auto snapshots = multi.IntoInsertRequests();
  EXAMPLE_REQUIRE(snapshots.inserts_size() == 1 && multi.TableCount() == 0);

  db::TypedColumnarBatch<Reading> typed{256, db::BufferPolicy::Unbounded()};
  EXAMPLE_REQUIRE(typed.ReserveRows(256));
  for (std::int64_t i = 0; i < 64; ++i)
    EXAMPLE_REQUIRE(typed.Push(Reading{7, {i}, 2.0}));
  const auto typed_bytes = typed.AllocatedBytes();
  typed.ClearUnbounded();
  EXAMPLE_REQUIRE(typed.RowCount() == 0 &&
                  typed.AllocatedBytes() == typed_bytes);
  EXAMPLE_REQUIRE(typed.Push(Reading{7, {1}, 2.0}));
  Snapshot("reflected SoA retained", typed);
  typed.ClearWithPolicy(db::BufferPolicy::Zero());
  columns.ClearWithPolicy(db::BufferPolicy::Zero());
  EXAMPLE_REQUIRE(typed.AllocatedBytes() == 0 && typed.RowCapacity() == 0);
  EXAMPLE_REQUIRE(columns.AllocatedBytes() == 0 && columns.RowCapacity() == 0);
  std::cout << "Inclusive policy watermarks, warm reuse, snapshots, shrink, "
               "and zero-policy release verified\n";
  return 0;
}
