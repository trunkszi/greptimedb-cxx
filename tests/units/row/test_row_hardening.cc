#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace allocation_probe {
extern thread_local bool fail_nothrow;
extern thread_local std::size_t nothrow_remaining;
} // namespace allocation_probe

namespace {
using namespace greptimedb;
struct[[= table{.name = "safe_rows"}]] SafeRow {
  [[= timestamp{}]] TimestampMillisecond ts;
  std::optional<std::string> text;
  std::optional<std::vector<std::byte>> bytes;
  std::optional<std::int8_t> number;
};

using Borrowed = BorrowedRow<SafeRow>;
using Batch = TypedColumnarBatch<SafeRow>;
static_assert(!std::constructible_from<Borrowed, SafeRow &&>);
static_assert(!std::constructible_from<RowView<SafeRow>, SafeRow &&>);
static_assert(!std::constructible_from<
              Borrowed, TimestampMillisecond, std::optional<std::string>,
              std::optional<BinaryView>, std::optional<std::int8_t>>);
static_assert(std::constructible_from<
              Borrowed, TimestampMillisecond, std::optional<std::string> &,
              std::optional<BinaryView>, std::optional<std::int8_t>>);
static_assert(!std::constructible_from<
              Borrowed, TimestampMillisecond, std::optional<std::string_view>,
              std::array<std::byte, 2>, std::optional<std::int8_t>>);
static_assert(std::constructible_from<
              Borrowed, TimestampMillisecond, std::optional<std::string_view>,
              std::array<std::byte, 2> &, std::optional<std::int8_t>>);
struct TemporaryText {
  std::string text;
  operator std::string_view() const noexcept { return text; }
};
static_assert(!std::constructible_from<Borrowed, TimestampMillisecond,
                                       TemporaryText, std::optional<BinaryView>,
                                       std::optional<std::int8_t>>);
static_assert(
    !std::is_reference_v<decltype(std::declval<const Borrowed &&>().Get<0>())>);
static_assert(!std::is_reference_v<
              decltype(std::declval<const RowView<SafeRow> &&>().Get<0>())>);
static_assert(std::is_reference_v<
              decltype(std::declval<const RowView<SafeRow> &>().Get<0>())>);

template <class T>
concept TemporaryViews = requires { T{}.Views(); };
template <class T>
concept TemporaryRow = requires { T{}.GetRow(0); };
static_assert(!TemporaryViews<Batch>);
static_assert(!TemporaryRow<Batch>);
template <class Columns>
concept WireProjection = requires(Columns &&columns) {
  ViewDecodedRow<SafeRow>(std::forward<Columns>(columns), 0, 1);
};
static_assert(!WireProjection<std::vector<::greptime::v1::Column>>);
static_assert(WireProjection<std::vector<::greptime::v1::Column> &>);
static_assert(WireProjection<std::span<const ::greptime::v1::Column>>);

struct[[= table{.rename_all = static_cast<RenameRule>(999)}]] InvalidRename {
  [[= timestamp{}]] std::int64_t ts;
};
static_assert(!RowConcept<InvalidRename>);

TEST(RowHardening, PrepareFailureSweepPreservesDenseValuesAndNulls) {
  const SafeRow first{{1}, "old", std::vector<std::byte>{std::byte{2}}, 3};
  const SafeRow second{{2},
                       std::string(1024, 'x'),
                       std::vector<std::byte>(1024, std::byte{7}),
                       std::nullopt};
  std::size_t failures = 0;
  std::size_t successes = 0;
  for (std::size_t budget = 0; budget < 10; ++budget) {
    Batch batch{16};
    ASSERT_TRUE(batch.Push(first));
    auto before = batch.ToInsertRequest();
    ASSERT_TRUE(before);
    allocation_probe::nothrow_remaining = budget;
    allocation_probe::fail_nothrow = true;
    const auto pushed = batch.Push(second);
    allocation_probe::fail_nothrow = false;
    if (pushed) {
      ++successes;
      continue;
    }
    ++failures;
    EXPECT_EQ(batch.RowCount(), 1U);
    EXPECT_EQ(batch.Column<0>().RowCount(), 1U);
    EXPECT_EQ(batch.Column<1>().RowCount(), 1U);
    EXPECT_EQ(batch.Column<2>().RowCount(), 1U);
    EXPECT_EQ(batch.Column<3>().RowCount(), 1U);
    auto after = batch.ToInsertRequest();
    ASSERT_TRUE(after);
    EXPECT_EQ(before->SerializeAsString(), after->SerializeAsString());
    ASSERT_TRUE(batch.Push(second));
    EXPECT_EQ(batch.RowCount(), 2U);
    auto recovered = batch.GetRow(1);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(recovered->Get<1>(), second.text);
    ASSERT_TRUE(recovered->Get<2>());
    EXPECT_EQ(recovered->Get<2>()->size(), second.bytes->size());
    EXPECT_FALSE(recovered->Get<3>());
  }
  EXPECT_GE(failures, 2U);
  EXPECT_GT(successes, 0U);
}

TEST(RowHardening, BorrowingANullMaskIsRejectedBeforeReallocation) {
  Batch batch;
  const SafeRow row{{1}, std::nullopt, std::nullopt, std::nullopt};
  for (std::size_t i = 0; i < 64; ++i)
    ASSERT_TRUE(batch.Push(row));
  const auto bytes = std::as_bytes(batch.Column<0>().Nulls().RawBytes());
  const Borrowed alias{
      TimestampMillisecond{2}, std::optional<std::string_view>{},
      std::optional<BinaryView>{bytes}, std::optional<std::int8_t>{}};
  EXPECT_FALSE(batch.PushRef(alias));
  EXPECT_EQ(batch.RowCount(), 64U);
  ASSERT_TRUE(batch.Push(row));
  EXPECT_EQ(batch.RowCount(), 65U);
}

TEST(RowHardening, WireMasksCountsPaddingAndForeignValuesAreValidated) {
  for (const std::size_t rows : {0U, 7U, 8U, 63U, 64U, 65U}) {
    Batch batch;
    for (std::size_t row = 0; row < rows; ++row) {
      SafeRow value{{static_cast<std::int64_t>(row)},
                    std::nullopt,
                    std::nullopt,
                    std::nullopt};
      if (row % 2 != 0)
        value.text = "owned";
      if (row % 3 != 0)
        value.bytes = std::vector<std::byte>{std::byte{4}};
      if (row % 5 != 0)
        value.number = 7;
      ASSERT_TRUE(batch.Push(value));
    }
    auto request = batch.ToInsertRequest();
    ASSERT_TRUE(request);
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), rows, rows));
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(
        request->columns(), 0, std::numeric_limits<std::size_t>::max()));
    if (rows == 0)
      continue;
    for (std::size_t row = 0; row < rows; ++row) {
      auto view = ViewDecodedRow<SafeRow>(request->columns(), row, rows);
      ASSERT_TRUE(view);
      EXPECT_EQ(view->Get<0>().value, static_cast<std::int64_t>(row));
      EXPECT_EQ(view->Get<1>().has_value(), row % 2 != 0);
      EXPECT_EQ(view->Get<2>().has_value(), row % 3 != 0);
      EXPECT_EQ(view->Get<3>().has_value(), row % 5 != 0);
    }
    const auto original = request->columns(1);
    auto &column = *request->mutable_columns(1);
    column.mutable_null_mask()->push_back('\0');
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_null_mask()->pop_back();
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_values()->mutable_string_values()->RemoveLast();
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_values()->add_string_values("extra");
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_values()->add_i64_values(99);
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_values()->add_time_second_values(99);
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_values()->add_json_values();
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    column.mutable_datatype_extension()->mutable_vector_type();
    EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
    column = original;
    if (rows % 8 != 0) {
      column.mutable_null_mask()->back() |= static_cast<char>(0x80);
      EXPECT_FALSE(ViewDecodedRow<SafeRow>(request->columns(), 0, rows));
      column = original;
    }
    const auto meta = Batch::kColumns[1];
    EXPECT_FALSE(row_detail::WireIndex(column, meta, rows, rows));
    EXPECT_FALSE(row_detail::WireIndex(column, meta, 0, 0));
    auto valid = ViewDecodedRow<SafeRow>(request->columns(), 1, rows);
    ASSERT_TRUE(valid);
    ASSERT_TRUE(valid->Get<1>());
    EXPECT_EQ(valid->Get<1>()->data(),
              request->columns(1).values().string_values(0).data());
  }
}

TEST(RowHardening, EmptyPayloadsReservationOverflowAndCoefficientLimits) {
  Batch batch;
  EXPECT_FALSE(batch.ReserveRows(std::numeric_limits<std::size_t>::max()));
  EXPECT_FALSE(batch.ReserveBytes(std::numeric_limits<std::size_t>::max()));
  const SafeRow empty{
      {1}, std::string{}, std::vector<std::byte>{}, std::int8_t{0}};
  ASSERT_TRUE(batch.Push(empty));
  batch.ShrinkToFit();
  auto view = batch.GetRow(0);
  ASSERT_TRUE(view);
  ASSERT_TRUE(view->Get<1>());
  EXPECT_TRUE(view->Get<1>()->empty());
  ASSERT_TRUE(view->Get<2>());
  EXPECT_TRUE(view->Get<2>()->empty());
  auto wire = batch.ToInsertRequest();
  ASSERT_TRUE(wire);
  EXPECT_TRUE(ViewDecodedRow<SafeRow>(wire->columns(), 0, 1));
  EXPECT_FALSE(row_detail::ValidCoefficient(0, 0));
  EXPECT_FALSE(row_detail::ValidCoefficient(
      1, std::numeric_limits<std::uint32_t>::max()));
}
} // namespace
