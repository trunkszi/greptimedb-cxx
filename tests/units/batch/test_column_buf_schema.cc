#include "column_buf_test_support.h"

namespace {
using namespace greptimedb;
using namespace column_buf_test;

TEST(ColumnBufferUnitScenario, CheckedConstructionAndSerializationRejectInvalidSchemas) {
  auto valid = TypedColumnBuf::Create("value", SemanticType::kField, Type::kInt64, 8);
  ASSERT_TRUE(valid);
  EXPECT_TRUE(valid->IsValid());
  ASSERT_TRUE(valid->Push(std::int64_t{42}));
  const auto checked = valid->TryToProtoColumn();
  ASSERT_TRUE(checked);
  EXPECT_EQ(Wire(*checked), Wire(valid->ToProtoColumn()));
  auto invalid = TypedColumnBuf::Create("value", SemanticType::kField, Type::Decimal(0, 0));
  ASSERT_FALSE(invalid);
  EXPECT_EQ(invalid.error(), ColumnBufError::kInvalidDecimal);
  TypedColumnBuf direct("value", SemanticType::kField, Type::Decimal(38, 39));
  EXPECT_FALSE(direct.IsValid());
  const auto rejected = direct.TryToProtoColumn();
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), ColumnBufError::kInvalidDecimal);
}

TEST(ColumnBufferUnitLifecycle, MovesLeaveSourcesSafeAndPermitExplicitReinitialization) {
  NullMask source_mask;
  ASSERT_TRUE(source_mask.Push(true));
  ASSERT_TRUE(source_mask.Push(false));
  NullMask moved_mask(std::move(source_mask));
  EXPECT_EQ(moved_mask.RowCount(), 2U);
  EXPECT_TRUE(moved_mask.IsNull(0));
  EXPECT_EQ(source_mask.RowCount(), 0U);
  EXPECT_EQ(source_mask.NullCount(), 0U);
  ASSERT_TRUE(source_mask.Push(false));
  EXPECT_EQ(source_mask.NonNullCount(), 1U);

  auto source = Column(Type::kString);
  ASSERT_TRUE(source.Push(std::string_view{"owned"}));
  ASSERT_TRUE(source.Push(std::monostate{}));
  const auto before = Wire(source.ToProtoColumn());
  TypedColumnBuf moved(std::move(source));
  EXPECT_EQ(Wire(moved.ToProtoColumn()), before);
  EXPECT_FALSE(source.IsValid());
  EXPECT_FALSE(source.Push(std::string_view{"rejected"}));
  EXPECT_FALSE(source.TryToProtoColumn());
  source = Column(Type::kInt64);
  ASSERT_TRUE(source.Push(std::int64_t{7}));
  EXPECT_EQ(source.ToProtoColumn().values().i64_values(0), 7);
  auto &alias = moved;
  moved = std::move(alias);
  EXPECT_EQ(Wire(moved.ToProtoColumn()), before);
}

TEST(ColumnBufferUnitScenario, ImpossibleReservationsFailWithoutChangingRowsOrPayload) {
  NullMask mask;
  ASSERT_TRUE(mask.Push(true));
  const auto mask_bytes = std::vector<std::uint8_t>(mask.Bytes().begin(), mask.Bytes().end());
  const auto mask_capacity = mask.RowCapacity();
  const auto mask_rejected = mask.ReserveRows(kMax);
  ASSERT_FALSE(mask_rejected);
  EXPECT_EQ(mask_rejected.error(), ColumnBufError::kAllocationFailure);
  EXPECT_EQ(mask.RowCount(), 1U);
  EXPECT_EQ(mask.RowCapacity(), mask_capacity);
  EXPECT_TRUE(std::ranges::equal(mask.Bytes(), mask_bytes));
  auto column = Column(Type::kInt64);
  ASSERT_TRUE(column.Push(std::int64_t{42}));
  const auto before = Wire(column.ToProtoColumn());
  const auto rejected = column.ReserveRows(kMax);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), ColumnBufError::kAllocationFailure);
  EXPECT_EQ(Wire(column.ToProtoColumn()), before);
  const auto oom_rejected = column.ReserveRows(std::size_t{1} << 45U);
  ASSERT_FALSE(oom_rejected);
  EXPECT_EQ(oom_rejected.error(), ColumnBufError::kAllocationFailure);
  EXPECT_EQ(Wire(column.ToProtoColumn()), before);
  ASSERT_TRUE(column.Push(std::int64_t{43}));
  EXPECT_EQ(column.RowCount(), 2U);

  struct alignas(64) OverAlignedSlot {
    std::uint64_t value{};
  };
  detail::ColumnArray<OverAlignedSlot> aligned;
  ASSERT_TRUE(aligned.Reserve(4));
  EXPECT_EQ(std::bit_cast<std::uintptr_t>(aligned.Data()) % 64U, 0U);
  aligned.Append(OverAlignedSlot{77});
  ASSERT_TRUE(aligned.ResizeCapacity(8));
  EXPECT_EQ(aligned[0].value, 77U);
  EXPECT_FALSE(aligned.ResizeCapacity(std::size_t{1} << 42U));
  EXPECT_EQ(aligned[0].value, 77U);
  aligned.Release();
  EXPECT_EQ(aligned.Capacity(), 0U);
}

TEST(ColumnBufferUnitLifecycle, MemoryAccountingAndRetentionCoverEveryStorageFamily) {
  for (const auto &[type, value] :
       std::array{std::pair{Type::kString, ValueView{std::string_view{"payload"}}},
                  std::pair{Type::kBinary, ValueView{Bytes("payload")}},
                  std::pair{Type::kJson, ValueView{JsonView{"{\"k\":1}"}}},
                  std::pair{Type::kInt64, ValueView{std::int64_t{42}}}, std::pair{Type::kFloat64, ValueView{1.5}},
                  std::pair{Type::kTimestampMillisecond, ValueView{TimestampMillisecond{42}}},
                  std::pair{Type::Decimal(38, 9), ValueView{Decimal128{42, 38, 9}}}}) {
    auto buf = Column(type);
    for (int cycle = 0; cycle < 3; ++cycle) {
      auto previous = buf.AllocatedBytes();
      for (int row = 0; row < 17; ++row) {
        ASSERT_TRUE(buf.Push(value));
        EXPECT_GE(buf.AllocatedBytes(), previous);
        previous = buf.AllocatedBytes();
      }
      buf.ClearUnbounded();
      const auto bytes = buf.AllocatedBytes();
      const auto rows = buf.RowCapacity();
      EXPECT_GT(bytes, 0U);
      buf.ClearWithPolicy({rows, bytes});
      EXPECT_EQ(buf.AllocatedBytes(), bytes);
      EXPECT_EQ(buf.RowCapacity(), rows);
    }
    buf.ClearWithPolicy({kMax, buf.AllocatedBytes() - 1});
    EXPECT_EQ(buf.AllocatedBytes(), 0U);
    EXPECT_EQ(buf.RowCapacity(), 0U);
    ASSERT_TRUE(buf.Push(value));
    buf.ClearWithPolicy({buf.RowCapacity() - 1, kMax});
    EXPECT_EQ(buf.AllocatedBytes(), 0U);
    EXPECT_EQ(buf.RowCapacity(), 0U);
    ASSERT_TRUE(buf.Push(value));
    buf.ClearWithPolicy(BufferPolicy::Zero());
    EXPECT_EQ(buf.AllocatedBytes(), 0U);
    EXPECT_EQ(buf.RowCapacity(), 0U);
  }
}
} // namespace
