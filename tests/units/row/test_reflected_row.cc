#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "allocation_probe.h"

#include "reflected_row_fixtures.h"

namespace {
using namespace greptimedb;
using namespace reflected_row_test;

TEST(ReflectedRowUnit, ReflectionPreservesSemanticTypesAndLogicalMetadata) {
  constexpr auto columns = row_detail::RowSchema<MetricRow>::kColumns;
  static_assert(columns.size() == 6);
  static_assert(columns[0].semantic_type == SemanticType::kTag);
  static_assert(columns[1].semantic_type == SemanticType::kTimestamp);
  static_assert(columns[1].data_type == ColumnDataType::kTimestampNanosecond);
  static_assert(columns[2].nullable && columns[3].nullable);
  static_assert(columns[4].data_type == ColumnDataType::kJson);
  static_assert(columns[5].data_type == ColumnDataType::Decimal(38, 10));
  EXPECT_EQ(columns[0].name, "host");
  EXPECT_EQ(columns[1].name, "eventTime");
}

TEST(ReflectedRowUnit, SourceProjectionsBorrowPayloadsWithoutAllocating) {
  MetricRow source{std::string(80, 's'), 7, 1.5, std::vector<std::byte>(80, std::byte{0x7f}), "{}", 123};
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  const BorrowedRow<MetricRow> borrowed{source};
  const RowView<MetricRow> view{source};
  const bool same_text =
      borrowed.Get<0>().data() == source.host_name.data() && view.Get<0>().data() == source.host_name.data();
  const bool same_bytes =
      borrowed.Get<3>()->data() == source.payload->data() && view.Get<3>()->data() == source.payload->data();
  allocation_probe::enabled = false;
  EXPECT_EQ(allocation_probe::count, 0U);
  EXPECT_TRUE(same_text);
  EXPECT_TRUE(same_bytes);
  EXPECT_EQ(borrowed.Get<1>(), 7);
}

TEST(ReflectedRowUnitScenario, BorrowedRowsProduceDenseValuesAndLittleEndianNullMasks) {
  TypedColumnarBatch<MetricRow> batch{16};
  const std::array payload{std::byte{0}, std::byte{0xff}};
  for (std::int64_t i = 0; i < 16; ++i) {
    const bool missing = i == 0 || i == 7 || i == 8 || i == 15;
    const BorrowedRow<MetricRow> row{std::string_view{"node-01"},
                                     i,
                                     missing ? std::nullopt : std::optional<double>{12.5},
                                     i % 2 == 0 ? std::optional<BinaryView>{payload} : std::nullopt,
                                     std::string_view{"{\"x\":1}"},
                                     static_cast<__int128>(-123)};
    ASSERT_TRUE(batch.PushRef(row));
  }
  ASSERT_EQ(batch.RowCount(), 16U);
  EXPECT_FALSE(batch.GetRow(16));
  auto first = batch.GetRow(0);
  ASSERT_TRUE(first);
  EXPECT_EQ(first->Get<0>(), "node-01");
  EXPECT_EQ(first->Get<"host">(), "node-01");
  EXPECT_EQ(first->Get<1>(), 0);
  EXPECT_FALSE(first->Get<2>());
  ASSERT_TRUE(first->Get<3>());
  EXPECT_TRUE(std::ranges::equal(*first->Get<3>(), payload));
  std::int64_t sum = 0;
  for (std::size_t i = 0; i < batch.RowCount(); ++i) {
    auto view = batch.GetRow(i);
    ASSERT_TRUE(view);
    sum += view->Get<1>();
  }
  EXPECT_EQ(sum, 120);
  auto encoded = batch.EncodeInsertRequest();
  ASSERT_TRUE(encoded);
  EXPECT_EQ(encoded->table_name(), "samples");
  EXPECT_EQ(encoded->row_count(), 16U);
  ASSERT_EQ(encoded->columns_size(), 6);
  EXPECT_EQ(encoded->columns(0).column_name(), "host");
  EXPECT_EQ(encoded->columns(1).column_name(), "eventTime");
  EXPECT_EQ(encoded->columns(2).null_mask(), std::string(2, '\x81'));
  ASSERT_EQ(encoded->columns(2).values().f64_values_size(), 12);
  for (auto value : encoded->columns(2).values().f64_values())
    EXPECT_EQ(value, 12.5);
  EXPECT_EQ(encoded->columns(3).null_mask(), std::string(2, '\xaa'));
  EXPECT_EQ(encoded->columns(3).values().binary_values(0), std::string("\0\xff", 2));
  EXPECT_EQ(encoded->columns(4).datatype(), ColumnDataType::kJson.ToProtoDatatype());
  EXPECT_TRUE(encoded->columns(4).has_datatype_extension());
  EXPECT_EQ(encoded->columns(4).values().binary_values(0), "{\"x\":1}");
  const auto &decimal = encoded->columns(5).values().decimal128_values(0);
  EXPECT_EQ(decimal.hi(), -1);
  EXPECT_EQ(decimal.lo(), static_cast<std::uint64_t>(-123));
  auto deleted = batch.EncodeDeleteRequest();
  ASSERT_TRUE(deleted);
  EXPECT_EQ(deleted->table_name(), "samples");
  ASSERT_EQ(deleted->key_columns_size(), 2);
  EXPECT_EQ(deleted->key_columns(0).column_name(), "host");
  EXPECT_EQ(deleted->key_columns(1).column_name(), "eventTime");
  EXPECT_EQ(deleted->row_count(), 16U);
}

TEST(ReflectedRowUnit, ScalarAndTemporalTypesPreserveTheirWireValues) {
  constexpr auto columns = row_detail::RowSchema<ScalarRow>::kColumns;
  constexpr std::array expected{ColumnDataType::kTimestampMicrosecond,
                                ColumnDataType::kBoolean,
                                ColumnDataType::kInt8,
                                ColumnDataType::kInt16,
                                ColumnDataType::kInt32,
                                ColumnDataType::kInt64,
                                ColumnDataType::kUint8,
                                ColumnDataType::kUint16,
                                ColumnDataType::kUint32,
                                ColumnDataType::kUint64,
                                ColumnDataType::kFloat32,
                                ColumnDataType::kFloat64,
                                ColumnDataType::kDate,
                                ColumnDataType::kDateTime,
                                ColumnDataType::kTimestampSecond,
                                ColumnDataType::kTimestampMillisecond,
                                ColumnDataType::kTimestampMicrosecond,
                                ColumnDataType::kTimestampNanosecond};
  for (std::size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(columns[i].data_type, expected[i]);
    EXPECT_FALSE(columns[i].nullable);
    EXPECT_EQ(columns[i].semantic_type, i == 0 ? SemanticType::kTimestamp : SemanticType::kField);
  }
  const ScalarRow source{-5,
                         true,
                         -8,
                         -16,
                         -32,
                         -64,
                         255,
                         65535,
                         std::numeric_limits<std::uint32_t>::max(),
                         std::numeric_limits<std::uint64_t>::max(),
                         1.25F,
                         -2.5,
                         Date{-1},
                         DateTime{-2},
                         TimestampSecond{-3},
                         TimestampMillisecond{-4},
                         TimestampMicrosecond{-5},
                         TimestampNanosecond{-6}};
  TypedColumnarBatch<ScalarRow> batch;
  ASSERT_TRUE(batch.Push(source));
  auto view = batch.GetRow(0);
  ASSERT_TRUE(view);
  EXPECT_EQ(view->Get<0>(), -5);
  EXPECT_EQ(view->Get<12>(), Date{-1});
  EXPECT_EQ(view->Get<17>(), TimestampNanosecond{-6});
  auto encoded = batch.EncodeInsertRequest();
  ASSERT_TRUE(encoded);
  EXPECT_EQ(encoded->table_name(), "scalar_row");
  EXPECT_EQ(encoded->columns(0).values().timestamp_microsecond_values(0), -5);
  EXPECT_EQ(encoded->columns(1).values().bool_values(0), true);
  EXPECT_EQ(encoded->columns(2).values().i8_values(0), -8);
  EXPECT_EQ(encoded->columns(3).values().i16_values(0), -16);
  EXPECT_EQ(encoded->columns(4).values().i32_values(0), -32);
  EXPECT_EQ(encoded->columns(5).values().i64_values(0), -64);
  EXPECT_EQ(encoded->columns(6).values().u8_values(0), 255);
  EXPECT_EQ(encoded->columns(7).values().u16_values(0), 65535);
  EXPECT_EQ(encoded->columns(8).values().u32_values(0), std::numeric_limits<std::uint32_t>::max());
  EXPECT_EQ(encoded->columns(9).values().u64_values(0), std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(encoded->columns(10).values().f32_values(0), 1.25F);
  EXPECT_EQ(encoded->columns(11).values().f64_values(0), -2.5);
  EXPECT_EQ(encoded->columns(12).values().date_values(0), -1);
  EXPECT_EQ(encoded->columns(13).values().datetime_values(0), -2);
  EXPECT_EQ(encoded->columns(14).values().timestamp_second_values(0), -3);
  EXPECT_EQ(encoded->columns(15).values().timestamp_millisecond_values(0), -4);
  EXPECT_EQ(encoded->columns(16).values().timestamp_microsecond_values(0), -5);
  EXPECT_EQ(encoded->columns(17).values().timestamp_nanosecond_values(0), -6);
}

struct BorrowedDocument {
  // clang-format off
  [[= timestamp{}]] TimestampNanosecond time;
  std::string_view text;
  BinaryView bytes;
  [[= field{.json = true}]] std::string_view document;
  // clang-format on
};

TEST(ReflectedRowUnitLifecycle, BorrowedSourcesAreCopiedAndRowViewsBorrowBatchStorage) {
  TypedColumnarBatch<BorrowedDocument> batch{4};
  std::string source = "borrowed UTF-8 text: é文";
  std::array bytes{std::byte{0}, std::byte{0xff}};
  ASSERT_TRUE(batch.Push(BorrowedDocument{TimestampNanosecond{9}, source, bytes, "{}"}));
  auto first = batch.GetRow(0);
  ASSERT_TRUE(first);
  EXPECT_NE(first->Get<1>().data(), source.data());
  EXPECT_NE(first->Get<2>().data(), bytes.data());
  const auto *stored_text = first->Get<1>().data();
  const auto *stored_bytes = first->Get<2>().data();
  auto again = batch.GetRow(0);
  ASSERT_TRUE(again);
  EXPECT_EQ(again->Get<1>().data(), stored_text);
  EXPECT_EQ(again->Get<2>().data(), stored_bytes);
  source.assign(source.size(), 'x');
  bytes.fill(std::byte{1});
  EXPECT_EQ(first->Get<1>(), "borrowed UTF-8 text: é文");
  EXPECT_EQ(first->Get<2>()[1], std::byte{0xff});
  auto allocated = batch.AllocatedBytes();
  batch.ClearUnbounded();
  EXPECT_EQ(batch.RowCount(), 0U);
  EXPECT_FALSE(batch.GetRow(0));
  EXPECT_EQ(batch.AllocatedBytes(), allocated);
  ASSERT_TRUE(batch.PushRef(
      BorrowedRow<BorrowedDocument>{TimestampNanosecond{10}, "replacement", std::span<const std::byte>{bytes}, "[]"}));
  auto reused = batch.GetRow(0);
  ASSERT_TRUE(reused);
  EXPECT_EQ(reused->Get<1>(), "replacement");
  EXPECT_EQ(reused->Get<1>().data(), stored_text);
  EXPECT_EQ(reused->Get<2>().data(), stored_bytes);
  EXPECT_EQ(batch.AllocatedBytes(), allocated);
  batch.ClearWithPolicy(BufferPolicy::Zero());
  EXPECT_EQ(batch.AllocatedBytes(), 0U);
  EXPECT_EQ(batch.RowCapacity(), 0U);
  EXPECT_FALSE(batch.GetRow(0));
}

struct DecimalRow {
  // clang-format off
  [[= timestamp{}]] TimestampSecond time;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] std::optional<Decimal128> value;
  // clang-format on
};

TEST(ReflectedRowUnitScenario, DecimalMetadataFailureDoesNotPartiallyAppendColumns) {
  TypedColumnarBatch<DecimalRow> batch;
  ASSERT_TRUE(batch.Push(DecimalRow{TimestampSecond{1}, Decimal128{123, 5, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{123, 6, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{100000, 5, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{-100000, 5, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{std::numeric_limits<__int128>::min(), 5, 2}}));
  EXPECT_EQ(batch.RowCount(), 1U);
  EXPECT_EQ(batch.Column<0>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<1>().RowCount(), 1U);
  auto existing = batch.GetRow(0);
  ASSERT_TRUE(existing);
  ASSERT_TRUE(existing->Get<1>());
  EXPECT_EQ(existing->Get<1>()->value, 123);
  ASSERT_TRUE(batch.Push(DecimalRow{TimestampSecond{3}, std::nullopt}));
  auto encoded = batch.EncodeInsertRequest();
  ASSERT_TRUE(encoded);
  EXPECT_EQ(encoded->row_count(), 2U);
  EXPECT_EQ(encoded->columns(1).null_mask(), std::string(1, '\x02'));
  EXPECT_EQ(encoded->columns(1).values().decimal128_values_size(), 1);
}

struct RawDecimalRow {
  // clang-format off
  [[= timestamp{}]] std::int64_t time;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] __int128 value;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] std::optional<__int128> nullable;
  // clang-format on
};

TEST(ReflectedRowUnitScenario, RawDecimalOverflowPreservesAllColumnsAtomically) {
  TypedColumnarBatch<RawDecimalRow> batch;
  ASSERT_TRUE(batch.Push(RawDecimalRow{1, 99999, -99999}));
  for (auto invalid : std::array<__int128, 3>{100000, -100000, std::numeric_limits<__int128>::min()}) {
    EXPECT_FALSE(batch.Push(RawDecimalRow{2, invalid, 1}));
    EXPECT_FALSE(batch.Push(RawDecimalRow{2, 1, invalid}));
    EXPECT_EQ(batch.RowCount(), 1U);
    EXPECT_EQ(batch.Column<0>().RowCount(), 1U);
    EXPECT_EQ(batch.Column<1>().RowCount(), 1U);
    EXPECT_EQ(batch.Column<2>().RowCount(), 1U);
  }
  ASSERT_TRUE(batch.Push(RawDecimalRow{3, 0, std::nullopt}));
  auto view = batch.GetRow(0);
  ASSERT_TRUE(view);
  EXPECT_EQ(view->Get<1>(), 99999);
  EXPECT_EQ(view->Get<2>(), std::optional<__int128>{-99999});
  auto wire = batch.EncodeInsertRequest();
  ASSERT_TRUE(wire);
  EXPECT_EQ(wire->columns(1).values().decimal128_values_size(), 2);
  EXPECT_EQ(wire->columns(2).values().decimal128_values_size(), 1);
  EXPECT_EQ(wire->columns(2).null_mask(), std::string(1, '\x02'));
}

TEST(ReflectedRowUnitLifecycle, WarmedBorrowedPushAndViewsAllocateNothing) {
  TypedColumnarBatch<MetricRow> batch{32, BufferPolicy::Unbounded()};
  const std::array bytes{std::byte{0}, std::byte{0xff}, std::byte{7}};
  const BorrowedRow<MetricRow> row{std::string_view{"a host longer than the small-string buffer"},
                                   std::int64_t{1},
                                   std::optional<double>{1.5},
                                   std::optional<BinaryView>{bytes},
                                   std::string_view{"{\"description\":\"larger than small-string storage\"}"},
                                   static_cast<__int128>(123)};
  for (std::size_t i = 0; i < 32; ++i)
    ASSERT_TRUE(batch.PushRef(row));
  batch.ClearUnbounded();
  const auto allocated = batch.AllocatedBytes();
  bool succeeded = true;
  std::int64_t sum = 0;
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  for (std::size_t i = 0; i < 32; ++i)
    succeeded &= batch.PushRef(row).has_value();
  for (std::size_t i = 0; i < 32; ++i) {
    auto view = batch.GetRow(i);
    succeeded &= view.has_value();
    if (view) sum += view->Get<1>();
  }
  std::int64_t range_sum = 0;
  for (const auto &view : batch.Views())
    range_sum += view.Get<1>();
  allocation_probe::enabled = false;
  const auto allocations = allocation_probe::count;
  EXPECT_TRUE(succeeded);
  EXPECT_EQ(sum, 32);
  EXPECT_EQ(range_sum, 32);
  EXPECT_EQ(allocations, 0U);
  EXPECT_EQ(batch.AllocatedBytes(), allocated);
}

TEST(ReflectedRowUnitScenario, ExplicitReservationEnablesAllocationFreeBorrowedPush) {
  TypedColumnarBatch<BorrowedDocument> batch;
  ASSERT_TRUE(batch.ReserveRows(4));
  ASSERT_TRUE(batch.ReserveBytes(4096));
  const std::array bytes{std::byte{3}, std::byte{4}};
  const BorrowedRow<BorrowedDocument> row{TimestampNanosecond{1}, "a document text longer than a small string",
                                          BinaryView{bytes},
                                          "{\"message\":\"a JSON document longer than a small string\"}"};
  bool succeeded = true;
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  for (std::size_t i = 0; i < 4; ++i)
    succeeded &= batch.PushRef(row).has_value();
  allocation_probe::enabled = false;
  EXPECT_TRUE(succeeded);
  EXPECT_EQ(allocation_probe::count, 0U);
  EXPECT_EQ(batch.RowCount(), 4U);
}

TEST(ReflectedRowUnitScenario, LaterColumnAllocationFailurePreservesLogicalRows) {
  TypedColumnarBatch<BorrowedDocument> batch{4};
  const std::array initial_bytes{std::byte{3}, std::byte{4}};
  ASSERT_TRUE(batch.Push(BorrowedDocument{TimestampNanosecond{1}, "first", initial_bytes, "{}"}));
  const std::string text(1024, 'x');
  const std::vector<std::byte> bytes(1024, std::byte{7});
  const BorrowedDocument next{TimestampNanosecond{2}, text, bytes, "[]"};
  allocation_probe::nothrow_remaining = 1;
  allocation_probe::fail_nothrow = true;
  auto failed = batch.Push(next);
  allocation_probe::fail_nothrow = false;
  ASSERT_FALSE(failed);
  EXPECT_EQ(allocation_probe::nothrow_remaining, 0U);
  EXPECT_EQ(failed.error(), ColumnBufError::kAllocationFailure);
  EXPECT_EQ(batch.RowCount(), 1U);
  EXPECT_EQ(batch.Column<0>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<1>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<2>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<3>().RowCount(), 1U);
  auto original = batch.GetRow(0);
  ASSERT_TRUE(original);
  EXPECT_EQ(original->Get<1>(), "first");
  EXPECT_TRUE(std::ranges::equal(original->Get<2>(), initial_bytes));
  ASSERT_TRUE(batch.Push(next));
  EXPECT_EQ(batch.RowCount(), 2U);
  auto retried = batch.GetRow(1);
  ASSERT_TRUE(retried);
  EXPECT_EQ(retried->Get<1>(), text);
  EXPECT_TRUE(std::ranges::equal(retried->Get<2>(), bytes));
}

TEST(ReflectedRowUnitScenario, SelfAliasingPayloadIsRejectedBeforeMutation) {
  TypedColumnarBatch<BorrowedDocument> batch;
  const std::array bytes{std::byte{3}, std::byte{4}};
  ASSERT_TRUE(batch.Push(BorrowedDocument{TimestampNanosecond{1}, "original", bytes, "{}"}));
  auto view = batch.GetRow(0);
  ASSERT_TRUE(view);
  const BorrowedRow<BorrowedDocument> alias{TimestampNanosecond{2}, view->Get<1>(), view->Get<2>(), view->Get<3>()};
  auto pushed = batch.PushRef(alias);
  ASSERT_FALSE(pushed);
  EXPECT_EQ(pushed.error(), ColumnBufError::kInvalidSchema);
  EXPECT_EQ(batch.RowCount(), 1U);
  auto retained = batch.GetRow(0);
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->Get<0>(), TimestampNanosecond{1});
  EXPECT_EQ(retained->Get<1>(), "original");
  EXPECT_TRUE(std::ranges::equal(retained->Get<2>(), bytes));
}

struct NullableRow {
  // clang-format off
  [[= timestamp{}]] TimestampMillisecond time;
  std::optional<std::string> text;
  std::optional<bool> flag;
  [[= field{.json = true}]] std::optional<std::string> json;
  // clang-format on
};

TEST(ReflectedRowUnitScenario, NullableTextDistinguishesEmptyValuesFromNull) {
  TypedColumnarBatch<NullableRow> batch;
  ASSERT_TRUE(batch.Push(NullableRow{TimestampMillisecond{1}, "", false, "{}"}));
  ASSERT_TRUE(batch.Push(NullableRow{TimestampMillisecond{2}, std::nullopt, std::nullopt, std::nullopt}));
  ASSERT_TRUE(batch.Push(NullableRow{TimestampMillisecond{3}, "é文", true, "[]"}));
  auto first = batch.GetRow(0);
  auto second = batch.GetRow(1);
  auto third = batch.GetRow(2);
  ASSERT_TRUE(first && second && third);
  ASSERT_TRUE(first->Get<1>());
  EXPECT_TRUE(first->Get<1>()->empty());
  EXPECT_EQ(first->Get<2>(), std::optional<bool>{false});
  EXPECT_FALSE(second->Get<1>());
  EXPECT_FALSE(second->Get<2>());
  EXPECT_FALSE(second->Get<3>());
  EXPECT_EQ(third->Get<1>(), std::optional<std::string_view>{"é文"});
  auto wire = batch.EncodeInsertRequest();
  ASSERT_TRUE(wire);
  for (int i = 1; i < wire->columns_size(); ++i)
    EXPECT_EQ(wire->columns(i).null_mask(), std::string(1, '\x02'));
  ASSERT_EQ(wire->columns(1).values().string_values_size(), 2);
  EXPECT_EQ(wire->columns(1).values().string_values(0), "");
  EXPECT_EQ(wire->columns(1).values().string_values(1), "é文");
  EXPECT_FALSE(wire->columns(2).values().bool_values(0));
  EXPECT_TRUE(wire->columns(2).values().bool_values(1));
  EXPECT_EQ(wire->columns(3).values().binary_values(1), "[]");
}

TEST(ReflectedRowUnitLifecycle, RetentionWatermarksBoundTheAggregateBatchStorage) {
  TypedColumnarBatch<MetricRow> batch{1024, BufferPolicy{2, 512}};
  MetricRow row{std::string(1000, 'x'), 1, std::nullopt, std::vector<std::byte>(1000, std::byte{7}), "{}", 0};
  ASSERT_TRUE(batch.Push(row));
  ASSERT_TRUE(batch.Push(row));
  EXPECT_GT(batch.AllocatedBytes(), 512U);
  batch.Clear();
  EXPECT_LE(batch.AllocatedBytes(), 512U);
  EXPECT_LE(batch.RowCapacity(), 2U);
  EXPECT_FALSE(batch.GetRow(0));
  ASSERT_TRUE(batch.Push(row));
  auto view = batch.GetRow(0);
  ASSERT_TRUE(view);
  EXPECT_EQ(view->Get<0>(), row.host_name);
  ASSERT_TRUE(view->Get<3>());
  EXPECT_TRUE(std::ranges::equal(*view->Get<3>(), *row.payload));
  batch.ShrinkToFit();
  EXPECT_EQ(batch.RowCount(), 1U);
  auto retained = batch.GetRow(0);
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->Get<0>(), row.host_name);
}

struct UnsignedBytes {
  // clang-format off
  [[= timestamp{}]] std::int64_t time;
  // clang-format on
  std::vector<std::uint8_t> bytes;
  std::optional<std::vector<std::uint8_t>> optional_bytes;
};

TEST(ReflectedRowUnitScenario, UnsignedByteVectorsRoundTripBinaryValuesAndNulls) {
  TypedColumnarBatch<UnsignedBytes> batch;
  ASSERT_TRUE(batch.Push(UnsignedBytes{1, {0, 127, 255}, std::vector<std::uint8_t>{}}));
  ASSERT_TRUE(batch.Push(UnsignedBytes{2, {}, std::nullopt}));
  auto first = batch.GetRow(0);
  auto second = batch.GetRow(1);
  ASSERT_TRUE(first && second);
  EXPECT_EQ(first->Get<1>().size(), 3U);
  EXPECT_EQ(static_cast<unsigned>(first->Get<1>()[2]), 255U);
  ASSERT_TRUE(first->Get<2>());
  EXPECT_TRUE(first->Get<2>()->empty());
  EXPECT_TRUE(second->Get<1>().empty());
  EXPECT_FALSE(second->Get<2>());
  auto wire = batch.EncodeInsertRequest();
  ASSERT_TRUE(wire);
  EXPECT_EQ(wire->columns(1).values().binary_values(0), std::string("\0\x7f\xff", 3));
  EXPECT_EQ(wire->columns(1).values().binary_values(1), "");
  EXPECT_EQ(wire->columns(2).null_mask(), std::string(1, '\x02'));
  EXPECT_EQ(wire->columns(2).values().binary_values_size(), 1);
}

struct WireRow {
  // clang-format off
  [[= timestamp{}]] std::int64_t time;
  [[= tag{}]] std::string host;
  std::optional<std::vector<std::byte>> bytes;
  std::optional<std::int8_t> small;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] std::optional<Decimal128> amount;
  [[= field{.json = true}]] std::string json;
  // clang-format on
};

template <class Columns>
concept DecodableColumns =
    requires(Columns &&columns) { ViewDecodedRow<WireRow>(std::forward<Columns>(columns), 0, 1); };
static_assert(!DecodableColumns<std::vector<greptime::v1::Column>>);
static_assert(DecodableColumns<std::vector<greptime::v1::Column> &>);
static_assert(DecodableColumns<std::span<const greptime::v1::Column>>);

TEST(ReflectedRowUnitScenario, DecodedWireProjectionBorrowsPayloadsAndRejectsCorruption) {
  TypedColumnarBatch<WireRow> batch;
  ASSERT_TRUE(batch.Push(WireRow{7, std::string(80, 's'), std::vector<std::byte>{std::byte{0}, std::byte{0xff}}, -8,
                                 Decimal128{-123, 5, 2}, "{\"event\":1}"}));
  ASSERT_TRUE(batch.Push(WireRow{8, "other", std::nullopt, std::nullopt, std::nullopt, "[]"}));
  auto encoded = batch.EncodeInsertRequest();
  ASSERT_TRUE(encoded);
  std::string serialized;
  ASSERT_TRUE(encoded->SerializeToString(&serialized));
  greptime::v1::InsertRequest decoded;
  ASSERT_TRUE(decoded.ParseFromString(serialized));
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  auto first = ViewDecodedRow<WireRow>(decoded.columns(), 0, decoded.row_count());
  auto second = ViewDecodedRow<WireRow>(decoded.columns(), 1, decoded.row_count());
  allocation_probe::enabled = false;
  const auto allocations = allocation_probe::count;
  ASSERT_TRUE(first && second);
  EXPECT_EQ(allocations, 0U);
  EXPECT_EQ(first->Get<0>(), 7);
  EXPECT_EQ(first->Get<1>().data(), decoded.columns(1).values().string_values(0).data());
  EXPECT_EQ(first->Get<1>().size(), 80U);
  EXPECT_EQ(first->Get<1>().front(), 's');
  ASSERT_TRUE(first->Get<2>());
  EXPECT_EQ(first->Get<2>()->data(), static_cast<const std::byte *>(static_cast<const void *>(
                                         decoded.columns(2).values().binary_values(0).data())));
  EXPECT_EQ((*first->Get<2>())[1], std::byte{0xff});
  EXPECT_EQ(first->Get<3>(), std::optional<std::int8_t>{-8});
  ASSERT_TRUE(first->Get<4>());
  EXPECT_EQ(*first->Get<4>(), (Decimal128{-123, 5, 2}));
  EXPECT_EQ(first->Get<5>().data(), decoded.columns(5).values().binary_values(0).data());
  EXPECT_FALSE(second->Get<2>());
  EXPECT_FALSE(second->Get<3>());
  EXPECT_FALSE(second->Get<4>());
  std::array<greptime::v1::Column, 6> contiguous;
  std::ranges::copy(decoded.columns(), contiguous.begin());
  auto spanned = ViewDecodedRow<WireRow>(std::span<const greptime::v1::Column>{contiguous}, 0, 2);
  ASSERT_TRUE(spanned);
  EXPECT_EQ(spanned->Get<1>().data(), contiguous[1].values().string_values(0).data());
  EXPECT_FALSE(ViewDecodedRow<WireRow>(decoded.columns(), 2, 2));
  EXPECT_FALSE(ViewDecodedRow<WireRow>(decoded.columns(), 0, 3));
  auto rejects = [&decoded](auto mutate) {
    auto damaged = decoded;
    mutate(damaged);
    EXPECT_FALSE(ViewDecodedRow<WireRow>(damaged.columns(), 0, damaged.row_count()));
  };
  rejects([](auto &r) { r.mutable_columns()->RemoveLast(); });
  rejects([](auto &r) { r.mutable_columns(2)->set_null_mask(std::string(1, '\x82')); });
  rejects([](auto &r) { r.mutable_columns(2)->set_null_mask(std::string(2, '\x02')); });
  rejects([](auto &r) { r.mutable_columns(3)->mutable_values()->clear_i8_values(); });
  rejects([](auto &r) { r.mutable_columns(1)->set_column_name("wrong"); });
  rejects([](auto &r) { r.mutable_columns(1)->set_semantic_type(greptime::v1::FIELD); });
  rejects([](auto &r) { r.mutable_columns(1)->set_datatype(greptime::v1::BINARY); });
  rejects([](auto &r) { r.mutable_columns(4)->clear_datatype_extension(); });
  rejects([](auto &r) { r.mutable_columns(5)->clear_datatype_extension(); });
  rejects([](auto &r) {
    auto *coefficient = r.mutable_columns(4)->mutable_values()->mutable_decimal128_values(0);
    coefficient->set_hi(0);
    coefficient->set_lo(100000);
  });
  rejects([](auto &r) { r.mutable_columns(3)->mutable_values()->set_i8_values(0, 128); });
  rejects([](auto &r) { r.mutable_columns(3)->mutable_values()->set_i8_values(0, -129); });
  rejects([](auto &r) { r.mutable_columns(0)->set_null_mask(std::string(1, '\x01')); });
}

} // namespace
