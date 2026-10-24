#include <array>
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/row/reflected_row.h>
#include <gtest/gtest.h>

namespace {
using namespace greptimedb;
struct[[= table{.name = "measurements"}]] Sample {
  [[= timestamp{}]] std::int64_t time;
  double value;
};
static_assert(BatchEncodeConcept<TypedColumnarBatch<Sample>>);

TEST(MultiTableReflected, DynamicAndTypedBatchesShareSnapshotsAndOverrides) {
  TypedColumnarBatch<Sample> typed;
  ASSERT_TRUE(typed.Push(Sample{7, 3.5}));
  ColumnarBatch dynamic("dynamic");
  ASSERT_TRUE(dynamic.AddTimestampColumn("time", TimestampUnit::kMillisecond));
  ASSERT_TRUE(
      dynamic.PushRow(std::array<ValueView, 1>{TimestampMillisecond{8}}));
  MultiTableBatch batches;
  ASSERT_TRUE(batches.AddBatch(typed));
  ASSERT_TRUE(batches.AddBatch(dynamic));
  ASSERT_TRUE(batches.AddBatchTo("typed_override", typed));
  ASSERT_TRUE(batches.AddBatchTo("dynamic_override", dynamic));
  EXPECT_EQ(typed.TableName(), "measurements");
  EXPECT_EQ(dynamic.TableName(), "dynamic");
  typed.Clear();
  dynamic.Clear();
  const auto requests = batches.ToInsertRequests();
  ASSERT_EQ(requests.inserts_size(), 4);
  EXPECT_EQ(requests.inserts(0).table_name(), "measurements");
  EXPECT_EQ(requests.inserts(1).table_name(), "dynamic");
  EXPECT_EQ(requests.inserts(2).table_name(), "typed_override");
  EXPECT_EQ(requests.inserts(3).table_name(), "dynamic_override");
  EXPECT_EQ(batches.TotalRows(), 4U);
  EXPECT_FALSE(batches.AddBatchTo("", typed));
  EXPECT_FALSE(batches.AddBatchTo(" \t", typed));
  EXPECT_EQ(batches.TableCount(), 4U);
}

struct FailedBatch {
  ColumnBufError error;
  std::string_view TableName() const { return "failed"; }
  std::size_t RowCount() const { return 1; }
  std::expected<greptime::v1::InsertRequest, ColumnBufError>
  EncodeInsertRequest() const {
    return std::unexpected(error);
  }
  std::expected<greptime::v1::DeleteRequest, ColumnBufError>
  EncodeDeleteRequest() const {
    return std::unexpected(error);
  }
};
static_assert(BatchEncodeConcept<FailedBatch>);
TEST(MultiTableReflected, ColumnBufferErrorsConvertWithoutChangingTheGroup) {
  MultiTableBatch batches;
  auto schema = batches.AddBatch(FailedBatch{ColumnBufError::kInvalidSchema});
  ASSERT_FALSE(schema);
  EXPECT_EQ(schema.error().Kind(), ErrorKind::kSchemaViolation);
  auto allocation = batches.AddBatchTo(
      "renamed", FailedBatch{ColumnBufError::kAllocationFailure});
  ASSERT_FALSE(allocation);
  EXPECT_EQ(allocation.error().Kind(), ErrorKind::kEncode);
  EXPECT_EQ(batches.TableCount(), 0U);
}
} // namespace
