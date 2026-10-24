#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <limits>
#include <new>
#include <string>

namespace allocation_failure {
thread_local bool enabled = false;
thread_local std::size_t remaining = 0;
bool Reject() noexcept {
  if (!enabled)
    return false;
  if (remaining == 0)
    return true;
  --remaining;
  return false;
}
struct Scope {
  explicit Scope(std::size_t budget) {
    remaining = budget;
    enabled = true;
  }
  ~Scope() { enabled = false; }
};
} // namespace allocation_failure

void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  if (allocation_failure::Reject())
    return nullptr;
  return std::malloc(size == 0 ? 1 : size);
}
void *operator new[](std::size_t size, const std::nothrow_t &tag) noexcept {
  return ::operator new(size, tag);
}
void operator delete(void *memory, const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, const std::nothrow_t &) noexcept {
  std::free(memory);
}

namespace {
using namespace greptimedb;

template <class Batch> void Schema(Batch &batch) {
  ASSERT_TRUE(batch.AddTimestampColumn("ts", TimestampUnit::kMillisecond));
  ASSERT_TRUE(batch.AddTagColumn("symbol", ColumnDataType::kString));
  ASSERT_TRUE(batch.AddFieldColumn("binary", ColumnDataType::kBinary));
  ASSERT_TRUE(batch.AddFieldColumn("json", ColumnDataType::kJson));
}

template <class Batch> auto Snapshot(const Batch &batch) {
  if constexpr (std::same_as<Batch, RowBatch>)
    return batch.EncodeRowInsertRequest();
  else
    return batch.EncodeInsertRequest();
}

template <class Batch> void AllocationSweep() {
  std::size_t failures = 0;
  std::size_t successes = 0;
  const std::string text(1024, 'x');
  const std::array bytes{std::byte{1}, std::byte{2}, std::byte{3}};
  const std::array<ValueView, 4> first{TimestampMillisecond{1},
                                       std::string_view("old"), BinaryView{},
                                       JsonView{}};
  const std::array<ValueView, 4> second{
      TimestampMillisecond{2}, std::string_view(text), BinaryView(bytes),
      JsonView{"{\"x\":1}"}};
  for (std::size_t budget = 0; budget < 12; ++budget) {
    Batch batch("ticks", 16);
    Schema(batch);
    ASSERT_TRUE(batch.PushRow(first));
    auto original = Snapshot(batch);
    ASSERT_TRUE(original);
    const auto original_bytes = original->SerializeAsString();
    const auto pushed = [&] {
      allocation_failure::Scope scope(budget);
      return batch.PushRow(second);
    }();
    if (pushed) {
      ++successes;
      EXPECT_EQ(batch.RowCount(), 2U);
      continue;
    }
    ++failures;
    EXPECT_EQ(batch.RowCount(), 1U) << "budget=" << budget;
    if constexpr (std::same_as<Batch, ColumnarBatch>) {
      for (const auto &column : batch.Columns())
        EXPECT_EQ(column.RowCount(), 1U);
    }
    auto retained = Snapshot(batch);
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->SerializeAsString(), original_bytes)
        << "budget=" << budget;
    ASSERT_TRUE(batch.PushRow(second)) << "budget=" << budget;
    EXPECT_EQ(batch.RowCount(), 2U);
    auto retry = Snapshot(batch);
    ASSERT_TRUE(retry);
    if constexpr (std::same_as<Batch, RowBatch>) {
      EXPECT_EQ(retry->rows().rows(1).values(1).string_value(), text);
      EXPECT_EQ(retry->rows().rows(1).values(2).binary_value().size(),
                bytes.size());
    } else {
      EXPECT_EQ(retry->columns(1).values().string_values(1), text);
      EXPECT_EQ(retry->columns(2).values().binary_values(1).size(),
                bytes.size());
    }
  }
  EXPECT_GE(failures, 2U);
  EXPECT_GT(successes, 0U);
}

TEST(AtomicAppend, DynamicColumnsSurviveEveryAllocationFailure) {
  AllocationSweep<ColumnarBatch>();
}

TEST(AtomicAppend, RowSchemaInsertionPreservesExistingSchemaAndReusableRows) {
  for (std::size_t budget = 0; budget < 2; ++budget) {
    RowBatch batch("ticks", 16);
    ASSERT_TRUE(batch.AddTimestampColumn("ts", TimestampUnit::kSecond));
    ASSERT_TRUE(batch.PushRow(std::array<ValueView, 1>{TimestampSecond{1}}));
    batch.ClearUnbounded();
    auto before = batch.EncodeRowInsertRequest();
    ASSERT_TRUE(before);
    const auto added = [&] {
      allocation_failure::Scope scope(budget);
      return batch.AddFieldColumn("text", ColumnDataType::kString);
    }();
    ASSERT_FALSE(added);
    EXPECT_EQ(batch.ColumnCount(), 1U);
    EXPECT_EQ(batch.RowCount(), 0U);
    auto after = batch.EncodeRowInsertRequest();
    ASSERT_TRUE(after);
    EXPECT_EQ(before->SerializeAsString(), after->SerializeAsString());
    ASSERT_TRUE(batch.AddFieldColumn("text", ColumnDataType::kString));
    const std::array<ValueView, 2> row{TimestampSecond{2},
                                       std::string_view("new")};
    ASSERT_TRUE(batch.PushRow(row));
    auto result = batch.EncodeRowInsertRequest();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->rows().schema_size(), 2);
    EXPECT_EQ(result->rows().rows(0).values_size(), 2);
  }
}

TEST(AtomicAppend,
     MultiTableInsertionPreservesExistingRequestsOnAllocationFailure) {
  ColumnarBatch source("ticks");
  ASSERT_TRUE(source.AddTimestampColumn("ts", TimestampUnit::kSecond));
  ASSERT_TRUE(source.PushRow(std::array<ValueView, 1>{TimestampSecond{1}}));
  MultiTableBatch tables;
  ASSERT_TRUE(tables.AddBatch(source));
  const auto before = tables.ToInsertRequests().SerializeAsString();
  const auto failed = [&] {
    allocation_failure::Scope scope(0);
    return tables.AddBatchTo("other", source);
  }();
  ASSERT_FALSE(failed);
  EXPECT_EQ(tables.TableCount(), 1U);
  EXPECT_EQ(tables.TotalRows(), 1U);
  EXPECT_EQ(tables.ToInsertRequests().SerializeAsString(), before);
  ASSERT_TRUE(tables.AddBatchTo("other", source));
  EXPECT_EQ(tables.TableCount(), 2U);
  EXPECT_EQ(tables.TotalRows(), 2U);
  MultiTableBatch copy(tables);
  MultiTableBatch assigned;
  assigned = tables;
  tables.Clear();
  EXPECT_EQ(copy.ToInsertRequests().SerializeAsString(),
            assigned.ToInsertRequests().SerializeAsString());
  auto consumed = copy.IntoInsertRequests();
  EXPECT_EQ(consumed.inserts_size(), 2);
  EXPECT_EQ(copy.TableCount(), 0U);
  EXPECT_EQ(assigned.TableCount(), 2U);
}
TEST(AtomicAppend, OwnedRowsSurviveEveryAllocationFailure) {
  AllocationSweep<RowBatch>();
}

TEST(AtomicAppend, DynamicResultRowsAlsoPrepareAllPayloads) {
  ColumnarBatch empty("query");
  EXPECT_FALSE(empty.PushResultRow({}));
  ColumnarBatch batch("query", 16);
  ASSERT_TRUE(batch.AddFieldColumn("number", ColumnDataType::kInt64));
  ASSERT_TRUE(batch.AddFieldColumn("text", ColumnDataType::kString));
  const std::array<ValueView, 2> row{std::int64_t{7}, std::string_view("text")};
  const auto failed = [&] {
    allocation_failure::Scope scope(0);
    return batch.PushResultRow(row);
  }();
  ASSERT_FALSE(failed);
  EXPECT_EQ(batch.Column(0)->RowCount(), 0U);
  EXPECT_EQ(batch.Column(1)->RowCount(), 0U);
  ASSERT_TRUE(batch.PushResultRow(row));
  EXPECT_EQ(batch.RowCount(), 1U);
}

TEST(AtomicAppend, RejectCrossColumnMaskAliasingBeforeAnyReservation) {
  ColumnarBatch batch("ticks");
  ASSERT_TRUE(batch.AddTimestampColumn("ts", TimestampUnit::kSecond));
  ASSERT_TRUE(batch.AddFieldColumn("data", ColumnDataType::kBinary));
  const std::array<ValueView, 2> row{TimestampSecond{1}, BinaryView{}};
  for (std::size_t i = 0; i < 64; ++i)
    ASSERT_TRUE(batch.PushRow(row));
  const auto mask = std::as_bytes(batch.Column(0)->Nulls().RawBytes());
  const std::array<ValueView, 2> alias{TimestampSecond{2}, mask};
  EXPECT_FALSE(batch.PushRow(alias));
  EXPECT_EQ(batch.Column(0)->RowCount(), 64U);
  EXPECT_EQ(batch.Column(1)->RowCount(), 64U);
  ASSERT_TRUE(batch.PushRow(row));
  EXPECT_EQ(batch.RowCount(), 65U);
}

TEST(AtomicAppend,
     ReusableOwnedRowSlotsPreserveValuesAcrossCompactionAndSchemaChanges) {
  RowBatch batch("ticks", 16);
  Schema(batch);
  const std::array<ValueView, 4> row{TimestampMillisecond{8},
                                     std::string_view("owned"), BinaryView{},
                                     JsonView{"{}"}};
  ASSERT_TRUE(batch.PushRow(row));
  auto snapshot = Snapshot(batch);
  ASSERT_TRUE(snapshot);
  batch.ShrinkToFit();
  auto shrunk = Snapshot(batch);
  ASSERT_TRUE(shrunk);
  EXPECT_EQ(snapshot->SerializeAsString(), shrunk->SerializeAsString());
  const auto capacity = batch.AllocatedBytes();
  batch.ClearUnbounded();
  ASSERT_TRUE(batch.PushRow(row));
  EXPECT_EQ(batch.AllocatedBytes(), capacity);
  batch.Clear();
  ASSERT_TRUE(batch.AddFieldColumn("extra", ColumnDataType::kInt64));
  const std::array<ValueView, 5> wider{row[0], row[1], row[2], row[3],
                                       std::int64_t{9}};
  ASSERT_TRUE(batch.PushRow(wider));
  auto encoded = Snapshot(batch);
  ASSERT_TRUE(encoded);
  ASSERT_EQ(encoded->rows().rows(0).values_size(), 5);
  EXPECT_EQ(encoded->rows().rows(0).values(4).i64_value(), 9);
}

TEST(AtomicAppend, NullMaskBoundariesAndOverflowReservations) {
  for (const std::size_t rows : {0U, 7U, 8U, 63U, 64U, 65U}) {
    NullMask mask;
    for (std::size_t i = 0; i < rows; ++i)
      ASSERT_TRUE(mask.Push(i % 3 == 0));
    EXPECT_EQ(mask.RowCount(), rows);
    EXPECT_EQ(mask.NonNullCount() + mask.NullCount(), rows);
    EXPECT_EQ(mask.RawBytes().size(), rows / 8 + (rows % 8 != 0));
    for (std::size_t i = 0; i < rows; ++i)
      EXPECT_EQ(mask.IsNull(i), i % 3 == 0);
    EXPECT_FALSE(mask.IsNull(rows));
    mask.ClearUnbounded();
    for (std::size_t i = 0; i < rows; ++i)
      ASSERT_TRUE(mask.Push(false));
    EXPECT_TRUE(mask.Bytes().empty());
  }
  constexpr auto max = std::numeric_limits<std::size_t>::max();
  EXPECT_EQ(detail::SaturatingAdd(max, 1), max);
  EXPECT_EQ(detail::SaturatingAdd(max - 1, 1), max);
  detail::ColumnArray<std::int64_t> array;
  EXPECT_FALSE(array.Reserve(max));
  EXPECT_EQ(array.Capacity(), 0U);
  auto column = TypedColumnBuf::Create("number", SemanticType::kField,
                                       ColumnDataType::kInt64);
  ASSERT_TRUE(column);
  EXPECT_FALSE(column->ReserveRows(max));
  ASSERT_TRUE(column->Push(std::int64_t{3}));
  EXPECT_EQ(column->RowCount(), 1U);
}

TEST(AtomicAppend, EmptyPayloadsAndDirectDecimalBoundsAreSafe) {
  auto text = TypedColumnBuf::Create("text", SemanticType::kField,
                                     ColumnDataType::kString);
  auto binary = TypedColumnBuf::Create("bytes", SemanticType::kField,
                                       ColumnDataType::kBinary);
  auto json = TypedColumnBuf::Create("json", SemanticType::kField,
                                     ColumnDataType::kJson);
  ASSERT_TRUE(text && binary && json);
  ASSERT_TRUE(text->Push(std::string_view{}));
  ASSERT_TRUE(binary->Push(BinaryView{}));
  ASSERT_TRUE(json->Push(JsonView{}));
  EXPECT_TRUE(text->ToProtoColumn().values().string_values(0).empty());
  EXPECT_TRUE(binary->ToProtoColumn().values().binary_values(0).empty());
  EXPECT_TRUE(json->ToProtoColumn().values().binary_values(0).empty());
  ASSERT_TRUE(text->Clone());
  auto decimal = TypedColumnBuf::Create("decimal", SemanticType::kField,
                                        ColumnDataType::Decimal(2, 0));
  ASSERT_TRUE(decimal);
  EXPECT_FALSE(decimal->Push(Decimal128{100, 2, 0}));
  EXPECT_FALSE(decimal->Push(Decimal128{-100, 2, 0}));
  EXPECT_EQ(decimal->RowCount(), 0U);
  ASSERT_TRUE(decimal->Push(Decimal128{-99, 2, 0}));
  EXPECT_EQ(decimal->RowCount(), 1U);
}
} // namespace
