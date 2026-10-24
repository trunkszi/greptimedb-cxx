/** Columnar, multi-table, and row batch builders over their GreptimeDB wire
 * encodings. */
#include <greptime/v1/database.pb.h>
#include <greptime/v1/row.pb.h>
#include <greptimedb/batch/column_buf.h>
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/core/buffer_policy.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/types.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

using namespace greptimedb;

/** Wire bytes as a std::string, including embedded NUL and 0xff octets. */
std::string ByteString(std::initializer_list<unsigned> values) {
  std::string out;
  out.reserve(values.size());
  for (unsigned value : values) out.push_back(static_cast<char>(value));
  return out;
}

template <std::size_t N>
std::span<const std::byte> ByteSpan(const std::array<std::byte, N> &bytes) {
  return bytes;
}

ValueView Null() { return ValueView{std::monostate{}}; }
ValueView Text(std::string_view value) { return ValueView{value}; }

template <typename Message>
void ExpectSameMessage(const Message &actual, const Message &expected) {
  EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

/** The default tag/time/field schema shared by the columnar scenarios. */
void AddTickColumns(ColumnarBatch &batch) {
  ASSERT_TRUE(batch.AddTagColumn("symbol", ColumnDataType::kString).has_value());
  ASSERT_TRUE(batch.AddTimestampColumn("time", TimestampUnit::kMillisecond).has_value());
  ASSERT_TRUE(batch.AddFieldColumn("price", ColumnDataType::kFloat64).has_value());
}

void AddTickRowSchema(RowBatch &batch) {
  ASSERT_TRUE(
      batch.AddColumnSchema("symbol", SemanticType::kTag, ColumnDataType::kString).has_value());
  ASSERT_TRUE(batch
                  .AddColumnSchema("time", SemanticType::kTimestamp,
                                   ColumnDataType::kTimestampMillisecond)
                  .has_value());
  ASSERT_TRUE(
      batch.AddColumnSchema("price", SemanticType::kField, ColumnDataType::kFloat64).has_value());
}

template <typename Buffer> void ExerciseBufferReuse(Buffer &buffer) {
  const std::size_t bytes = buffer.AllocatedBytes();
  const std::size_t capacity = buffer.RowCapacity();
  EXPECT_GT(bytes, 0U);
  EXPECT_GE(capacity, 16U);
  buffer.ClearUnbounded();
  EXPECT_EQ(buffer.AllocatedBytes(), bytes);
  EXPECT_EQ(buffer.RowCapacity(), capacity);
  buffer.ClearWithPolicy(BufferPolicy{std::numeric_limits<std::size_t>::max(), 0});
  EXPECT_EQ(buffer.AllocatedBytes(), 0U);
  EXPECT_EQ(buffer.RowCapacity(), 0U);
}

TEST(BatchEncoding, InvalidTableOverridesDoNotChangeValidEncoding) {
  ColumnarBatch batch("ticks");
  AddTickColumns(batch);
  EXPECT_FALSE(batch.EncodeInsertRequestFor("").has_value());
  EXPECT_FALSE(batch.EncodeDeleteRequestFor("").has_value());
  auto archived = batch.EncodeInsertRequestFor("archive");
  ASSERT_TRUE(archived.has_value());
  EXPECT_EQ(archived->table_name(), "archive");
  auto insert = batch.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->table_name(), "ticks");
  ColumnarBatch unnamed("");
  AddTickColumns(unnamed);
  EXPECT_FALSE(unnamed.EncodeInsertRequest().has_value());

  RowBatch rows("ticks");
  AddTickRowSchema(rows);
  EXPECT_FALSE(rows.EncodeRowInsertRequestFor("").has_value());
  EXPECT_FALSE(rows.EncodeRowDeleteRequestFor("").has_value());
  auto row_archived = rows.EncodeRowInsertRequestFor("archive");
  ASSERT_TRUE(row_archived.has_value());
  EXPECT_EQ(row_archived->table_name(), "archive");
  auto row_insert = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_insert.has_value());
  EXPECT_EQ(row_insert->table_name(), "ticks");
  RowBatch unnamed_rows("");
  AddTickRowSchema(unnamed_rows);
  EXPECT_FALSE(unnamed_rows.EncodeRowInsertRequest().has_value());
}

TEST(BatchEncoding, TimestampSchemaIsMandatoryAndUnique) {
  ColumnarBatch columns("ticks");
  ASSERT_TRUE(columns.AddFieldColumn("price", ColumnDataType::kFloat64).has_value());
  EXPECT_FALSE(columns.ValidateSchema().has_value());
  ASSERT_TRUE(columns.AddTimestampColumn("time", TimestampUnit::kMillisecond).has_value());
  EXPECT_TRUE(columns.ValidateSchema().has_value());
  EXPECT_FALSE(columns.AddTimestampColumn("other_time", TimestampUnit::kSecond).has_value());
  EXPECT_TRUE(columns.ValidateSchema().has_value());

  RowBatch rows("ticks");
  ASSERT_TRUE(
      rows.AddColumnSchema("price", SemanticType::kField, ColumnDataType::kFloat64).has_value());
  EXPECT_FALSE(rows.ValidateSchema().has_value());
  ASSERT_TRUE(rows
                  .AddColumnSchema("time", SemanticType::kTimestamp,
                                   ColumnDataType::kTimestampMillisecond)
                  .has_value());
  EXPECT_TRUE(rows.ValidateSchema().has_value());
  EXPECT_FALSE(rows
                   .AddColumnSchema("other_time", SemanticType::kTimestamp,
                                    ColumnDataType::kTimestampSecond)
                   .has_value());
  EXPECT_TRUE(rows.ValidateSchema().has_value());
}

TEST(BatchEncoding, DuplicateAndEmptyColumnNamesLeaveSchemaUnchanged) {
  ColumnarBatch columns("ticks");
  AddTickColumns(columns);
  auto encoded = columns.EncodeInsertRequest();
  ASSERT_TRUE(encoded.has_value());
  const std::string before = encoded->SerializeAsString();
  EXPECT_FALSE(columns.AddFieldColumn("symbol", ColumnDataType::kFloat64).has_value());
  EXPECT_FALSE(columns.AddFieldColumn("", ColumnDataType::kFloat64).has_value());
  auto after = columns.EncodeInsertRequest();
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(after->SerializeAsString(), before);

  RowBatch rows("ticks");
  AddTickRowSchema(rows);
  auto row_encoded = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_encoded.has_value());
  const std::string row_before = row_encoded->SerializeAsString();
  EXPECT_FALSE(
      rows.AddColumnSchema("symbol", SemanticType::kField, ColumnDataType::kFloat64).has_value());
  EXPECT_FALSE(
      rows.AddColumnSchema("", SemanticType::kField, ColumnDataType::kFloat64).has_value());
  auto row_after = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_after.has_value());
  EXPECT_EQ(row_after->SerializeAsString(), row_before);
}

TEST(BatchEncoding, ColumnarNullMaskCrossesByteBoundaryAndDeleteFiltersFields) {
  ColumnarBatch batch("ticks");
  AddTickColumns(batch);
  ASSERT_TRUE(batch.AddFieldColumn("payload", ColumnDataType::kBinary).has_value());
  const std::array<std::byte, 2> binary{std::byte{0}, std::byte{0xff}};
  for (int row = 0; row < 10; ++row) {
    const std::array values{
        row == 1 || row == 8 ? Null() : Text("BTC"),
        ValueView{TimestampMillisecond{1'700'000'000'000LL + row}},
        row == 0 || row == 7 || row == 9 ? Null() : ValueView{42.5},
        row == 2 ? ValueView{ByteSpan(binary)} : Null(),
    };
    ASSERT_TRUE(batch.PushRow(values).has_value());
  }
  auto insert = batch.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->table_name(), "ticks");
  EXPECT_EQ(insert->row_count(), 10U);
  ASSERT_EQ(insert->columns_size(), 4);
  EXPECT_EQ(insert->columns(0).column_name(), "symbol");
  EXPECT_EQ(insert->columns(1).column_name(), "time");
  EXPECT_EQ(insert->columns(2).column_name(), "price");
  EXPECT_EQ(insert->columns(3).column_name(), "payload");
  EXPECT_EQ(insert->columns(0).semantic_type(), greptime::v1::SemanticType::TAG);
  EXPECT_EQ(insert->columns(1).semantic_type(), greptime::v1::SemanticType::TIMESTAMP);
  EXPECT_EQ(insert->columns(2).semantic_type(), greptime::v1::SemanticType::FIELD);
  EXPECT_EQ(insert->columns(0).null_mask(), ByteString({0x02, 0x01}));
  ASSERT_EQ(insert->columns(0).values().string_values_size(), 8);
  for (const std::string &value : insert->columns(0).values().string_values()) {
    EXPECT_EQ(value, "BTC");
  }
  EXPECT_TRUE(insert->columns(1).null_mask().empty());
  ASSERT_EQ(insert->columns(1).values().timestamp_millisecond_values_size(), 10);
  for (int row = 0; row < 10; ++row) {
    EXPECT_EQ(insert->columns(1).values().timestamp_millisecond_values(row),
              1'700'000'000'000LL + row);
  }
  EXPECT_EQ(insert->columns(2).null_mask(), ByteString({0x81, 0x02}));
  ASSERT_EQ(insert->columns(2).values().f64_values_size(), 7);
  for (double value : insert->columns(2).values().f64_values()) EXPECT_EQ(value, 42.5);
  EXPECT_EQ(insert->columns(3).null_mask(), ByteString({0xfb, 0x03}));
  ASSERT_EQ(insert->columns(3).values().binary_values_size(), 1);
  EXPECT_EQ(insert->columns(3).values().binary_values(0), ByteString({0, 0xff}));

  auto remove = batch.EncodeDeleteRequest();
  ASSERT_TRUE(remove.has_value());
  EXPECT_EQ(remove->table_name(), "ticks");
  EXPECT_EQ(remove->row_count(), 10U);
  ASSERT_EQ(remove->key_columns_size(), 2);
  for (int index = 0; index < 2; ++index) {
    ExpectSameMessage(remove->key_columns(index), insert->columns(index));
  }
}

TEST(BatchEncoding, RowWireValuesAndDeleteKeysPreserveSchemaOrder) {
  RowBatch batch("ticks");
  ASSERT_TRUE(
      batch.AddColumnSchema("price", SemanticType::kField, ColumnDataType::kFloat64).has_value());
  ASSERT_TRUE(batch
                  .AddColumnSchema("time", SemanticType::kTimestamp,
                                   ColumnDataType::kTimestampMillisecond)
                  .has_value());
  ASSERT_TRUE(
      batch.AddColumnSchema("symbol", SemanticType::kTag, ColumnDataType::kString).has_value());
  const std::array first{ValueView{42.5}, ValueView{TimestampMillisecond{1234}}, Text("BTC")};
  ASSERT_TRUE(batch.PushRow(first).has_value());
  const std::array second{Null(), ValueView{TimestampMillisecond{1235}}, Null()};
  ASSERT_TRUE(batch.PushRow(second).has_value());

  auto insert = batch.EncodeRowInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->table_name(), "ticks");
  const greptime::v1::Rows &rows = insert->rows();
  ASSERT_EQ(rows.schema_size(), 3);
  EXPECT_EQ(rows.schema(0).column_name(), "price");
  EXPECT_EQ(rows.schema(1).column_name(), "time");
  EXPECT_EQ(rows.schema(2).column_name(), "symbol");
  EXPECT_EQ(rows.schema(0).semantic_type(), greptime::v1::SemanticType::FIELD);
  EXPECT_EQ(rows.schema(1).semantic_type(), greptime::v1::SemanticType::TIMESTAMP);
  EXPECT_EQ(rows.schema(2).semantic_type(), greptime::v1::SemanticType::TAG);
  ASSERT_EQ(rows.rows_size(), 2);
  const greptime::v1::Row &first_row = rows.rows(0);
  ASSERT_EQ(first_row.values_size(), 3);
  EXPECT_EQ(first_row.values(0).value_data_case(), greptime::v1::Value::kF64Value);
  EXPECT_EQ(first_row.values(0).f64_value(), 42.5);
  EXPECT_EQ(first_row.values(1).value_data_case(),
            greptime::v1::Value::kTimestampMillisecondValue);
  EXPECT_EQ(first_row.values(1).timestamp_millisecond_value(), 1234);
  EXPECT_EQ(first_row.values(2).value_data_case(), greptime::v1::Value::kStringValue);
  EXPECT_EQ(first_row.values(2).string_value(), "BTC");
  const greptime::v1::Row &second_row = rows.rows(1);
  ASSERT_EQ(second_row.values_size(), 3);
  EXPECT_EQ(second_row.values(0).value_data_case(), greptime::v1::Value::VALUE_DATA_NOT_SET);
  EXPECT_EQ(second_row.values(1).value_data_case(),
            greptime::v1::Value::kTimestampMillisecondValue);
  EXPECT_EQ(second_row.values(1).timestamp_millisecond_value(), 1235);
  EXPECT_EQ(second_row.values(2).value_data_case(), greptime::v1::Value::VALUE_DATA_NOT_SET);

  auto remove = batch.EncodeRowDeleteRequest();
  ASSERT_TRUE(remove.has_value());
  EXPECT_EQ(remove->table_name(), "ticks");
  const greptime::v1::Rows &keys = remove->rows();
  ASSERT_EQ(keys.schema_size(), 2);
  ExpectSameMessage(keys.schema(0), rows.schema(1));
  ExpectSameMessage(keys.schema(1), rows.schema(2));
  ASSERT_EQ(keys.rows_size(), 2);
  ASSERT_EQ(keys.rows(0).values_size(), 2);
  ExpectSameMessage(keys.rows(0).values(0), rows.rows(0).values(1));
  ExpectSameMessage(keys.rows(0).values(1), rows.rows(0).values(2));
  ExpectSameMessage(keys.rows(1).values(0), rows.rows(1).values(1));
  ExpectSameMessage(keys.rows(1).values(1), rows.rows(1).values(2));
}

TEST(BatchEncoding, FailedRowsAreAtomicForBothBuilders) {
  ColumnarBatch columns("ticks");
  AddTickColumns(columns);
  RowBatch rows("ticks");
  AddTickRowSchema(rows);
  const std::array good{Text("BTC"), ValueView{TimestampMillisecond{1234}}, ValueView{42.5}};
  ASSERT_TRUE(columns.PushRow(good).has_value());
  ASSERT_TRUE(rows.PushRow(good).has_value());
  auto column_encoded = columns.EncodeInsertRequest();
  ASSERT_TRUE(column_encoded.has_value());
  const std::string column_before = column_encoded->SerializeAsString();
  auto row_encoded = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_encoded.has_value());
  const std::string row_before = row_encoded->SerializeAsString();

  const auto expect_rejected = [&](std::span<const ValueView> invalid) {
    EXPECT_FALSE(columns.PushRow(invalid).has_value());
    EXPECT_FALSE(rows.PushRow(invalid).has_value());
    auto column_now = columns.EncodeInsertRequest();
    ASSERT_TRUE(column_now.has_value());
    EXPECT_EQ(column_now->SerializeAsString(), column_before);
    auto row_now = rows.EncodeRowInsertRequest();
    ASSERT_TRUE(row_now.has_value());
    EXPECT_EQ(row_now->SerializeAsString(), row_before);
  };

  const std::array too_narrow{Text("ETH"), ValueView{TimestampMillisecond{1235}}};
  const std::array wrong_last_type{Text("ETH"), ValueView{TimestampMillisecond{1235}},
                                   Text("wrong last column")};
  const std::array null_time{Text("ETH"), Null(), ValueView{43.0}};
  const std::array wrong_unit{Text("ETH"), ValueView{TimestampSecond{1235}}, ValueView{43.0}};
  const std::array too_wide{Text("ETH"), ValueView{TimestampMillisecond{1235}}, ValueView{43.0},
                            Null()};
  expect_rejected(too_narrow);
  expect_rejected(wrong_last_type);
  expect_rejected(null_time);
  expect_rejected(wrong_unit);
  expect_rejected(too_wide);

  EXPECT_FALSE(columns.AddFieldColumn("late", ColumnDataType::kInt64).has_value());
  EXPECT_FALSE(
      rows.AddColumnSchema("late", SemanticType::kField, ColumnDataType::kInt64).has_value());
  auto column_after_schema = columns.EncodeInsertRequest();
  ASSERT_TRUE(column_after_schema.has_value());
  EXPECT_EQ(column_after_schema->SerializeAsString(), column_before);
  auto row_after_schema = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_after_schema.has_value());
  EXPECT_EQ(row_after_schema->SerializeAsString(), row_before);

  ASSERT_TRUE(columns.PushRow(good).has_value());
  ASSERT_TRUE(rows.PushRow(good).has_value());
  EXPECT_EQ(columns.RowCount(), 2U);
  EXPECT_EQ(rows.RowCount(), 2U);
}

TEST(BatchEncoding, UnequalColumnLengthsFailBeforeEncodingAndCanBeCompleted) {
  ColumnarBatch batch("ticks");
  AddTickColumns(batch);
  ASSERT_TRUE(batch.ColumnMut(0)->Push(Text("BTC")).has_value());
  EXPECT_FALSE(batch.EncodeInsertRequest().has_value());
  EXPECT_FALSE(batch.EncodeDeleteRequest().has_value());
  const std::array row{Text("ETH"), ValueView{TimestampMillisecond{2}}, Null()};
  EXPECT_FALSE(batch.PushRow(row).has_value());
  ASSERT_TRUE(batch.ColumnMut(1)->Push(ValueView{TimestampMillisecond{1}}).has_value());
  ASSERT_TRUE(batch.ColumnMut(2)->Push(ValueView{42.5}).has_value());
  auto insert = batch.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->row_count(), 1U);
  ASSERT_EQ(insert->columns(0).values().string_values_size(), 1);
  EXPECT_EQ(insert->columns(0).values().string_values(0), "BTC");
}

TEST(BatchEncoding, TimestampSemanticRejectsNonTemporalTypes) {
  ColumnarBatch columns("ticks");
  EXPECT_FALSE(
      columns.AddColumn("time", SemanticType::kTimestamp, ColumnDataType::kInt64).has_value());
  ASSERT_TRUE(columns.AddTimestampColumn("time", TimestampUnit::kNanosecond).has_value());
  const std::array row{ValueView{TimestampNanosecond{-1}}};
  ASSERT_TRUE(columns.PushRow(row).has_value());
  auto insert = columns.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  ASSERT_EQ(insert->columns_size(), 1);
  ASSERT_EQ(insert->columns(0).values().timestamp_nanosecond_values_size(), 1);
  EXPECT_EQ(insert->columns(0).values().timestamp_nanosecond_values(0), -1);

  RowBatch rows("ticks");
  EXPECT_FALSE(
      rows.AddColumnSchema("time", SemanticType::kTimestamp, ColumnDataType::kString).has_value());
  ASSERT_TRUE(
      rows.AddColumnSchema("time", SemanticType::kTimestamp, ColumnDataType::kTimestampSecond)
          .has_value());
  const std::array second_row{ValueView{TimestampSecond{-1}}};
  ASSERT_TRUE(rows.PushRow(second_row).has_value());
  EXPECT_EQ(rows.RowCount(), 1U);
}

TEST(BatchEncoding, MultiTableEncodingKeepsOrderAndFailedAddIsAtomic) {
  ColumnarBatch ticks("ticks");
  AddTickColumns(ticks);
  const std::array tick_row{Text("BTC"), ValueView{TimestampMillisecond{1}}, ValueView{42.5}};
  ASSERT_TRUE(ticks.PushRow(tick_row).has_value());
  ColumnarBatch trades("trades");
  AddTickColumns(trades);
  const std::array trade_row{Text("ETH"), ValueView{TimestampMillisecond{2}}, ValueView{43.0}};
  ASSERT_TRUE(trades.PushRow(trade_row).has_value());

  MultiTableBatch multi;
  ASSERT_TRUE(multi.AddBatch(ticks).has_value());
  ASSERT_TRUE(multi.AddBatchTo("archive", trades).has_value());
  const greptime::v1::InsertRequests before = multi.ToInsertRequests();
  EXPECT_EQ(multi.TotalRows(), 2U);
  EXPECT_EQ(multi.TableCount(), 2U);
  EXPECT_FALSE(multi.AddBatchTo("", ticks).has_value());
  ColumnarBatch broken("broken");
  EXPECT_FALSE(multi.AddBatch(broken).has_value());
  const greptime::v1::InsertRequests after = multi.ToInsertRequests();
  ExpectSameMessage(after, before);

  ASSERT_EQ(before.inserts_size(), 2);
  EXPECT_EQ(before.inserts(0).table_name(), "ticks");
  EXPECT_EQ(before.inserts(1).table_name(), "archive");
  auto ticks_encoded = ticks.EncodeInsertRequest();
  ASSERT_TRUE(ticks_encoded.has_value());
  ExpectSameMessage(before.inserts(0), *ticks_encoded);
  auto trades_encoded = trades.EncodeInsertRequestFor("archive");
  ASSERT_TRUE(trades_encoded.has_value());
  ExpectSameMessage(before.inserts(1), *trades_encoded);
  const greptime::v1::InsertRequests consumed = multi.IntoInsertRequests();
  ExpectSameMessage(consumed, before);
}

TEST(BatchLifecycle, ReuseRetainsSchemaAndEnforcesAggregateByteLimit) {
  ColumnarBatch columns("ticks", 16);
  ASSERT_TRUE(columns.AddTimestampColumn("time", TimestampUnit::kMillisecond).has_value());
  RowBatch rows("ticks", 16);
  ASSERT_TRUE(rows
                  .AddColumnSchema("time", SemanticType::kTimestamp,
                                   ColumnDataType::kTimestampMillisecond)
                  .has_value());
  const std::array first{ValueView{TimestampMillisecond{1}}};
  ASSERT_TRUE(columns.PushRow(first).has_value());
  ASSERT_TRUE(rows.PushRow(first).has_value());
  ExerciseBufferReuse(columns);
  ExerciseBufferReuse(rows);
  EXPECT_EQ(columns.RowCount(), 0U);
  EXPECT_EQ(rows.RowCount(), 0U);
  const std::array second{ValueView{TimestampMillisecond{2}}};
  ASSERT_TRUE(columns.PushRow(second).has_value());
  ASSERT_TRUE(rows.PushRow(second).has_value());
  auto insert = columns.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->row_count(), 1U);
  auto row_insert = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_insert.has_value());
  EXPECT_EQ(row_insert->rows().rows_size(), 1);
}

TEST(BatchEncoding, DecimalJsonAndBinaryKeepWireMetadataAndOwnedValues) {
  struct SchemaEntry {
    const char *name;
    SemanticType role;
    ColumnDataType type;
  };
  const std::array schema{
      SchemaEntry{"time", SemanticType::kTimestamp, ColumnDataType::kTimestampMicrosecond},
      SchemaEntry{"amount", SemanticType::kField, ColumnDataType::Decimal(12, 4)},
      SchemaEntry{"meta", SemanticType::kField, ColumnDataType::kJson},
      SchemaEntry{"raw", SemanticType::kField, ColumnDataType::kBinary},
      SchemaEntry{"day", SemanticType::kField, ColumnDataType::kDate},
  };
  ColumnarBatch columns("trades");
  RowBatch rows("trades");
  for (const SchemaEntry &entry : schema) {
    ASSERT_TRUE(columns.AddColumn(entry.name, entry.role, entry.type).has_value());
    ASSERT_TRUE(rows.AddColumnSchema(entry.name, entry.role, entry.type).has_value());
  }
  const std::array<std::byte, 3> binary{std::byte{0}, std::byte{0xff}, std::byte{3}};
  const std::array values{ValueView{TimestampMicrosecond{5}},
                          ValueView{Decimal128{-12345, 12, 4}},
                          ValueView{JsonView{"{\"venue\":\"X\"}"}},
                          ValueView{ByteSpan(binary)},
                          ValueView{Date{20'000}}};
  ASSERT_TRUE(columns.PushRow(values).has_value());
  ASSERT_TRUE(rows.PushRow(values).has_value());
  auto column_encoded = columns.EncodeInsertRequest();
  ASSERT_TRUE(column_encoded.has_value());
  const greptime::v1::InsertRequest &column_before = *column_encoded;
  auto row_encoded = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_encoded.has_value());
  const greptime::v1::RowInsertRequest &row_before = *row_encoded;

  const std::array invalid{ValueView{TimestampMicrosecond{6}},
                           ValueView{Decimal128{1, 12, 3}},
                           Null(),
                           Null(),
                           Null()};
  EXPECT_FALSE(columns.PushRow(invalid).has_value());
  EXPECT_FALSE(rows.PushRow(invalid).has_value());
  auto column_after = columns.EncodeInsertRequest();
  ASSERT_TRUE(column_after.has_value());
  ExpectSameMessage(*column_after, column_before);
  auto row_after = rows.EncodeRowInsertRequest();
  ASSERT_TRUE(row_after.has_value());
  ExpectSameMessage(*row_after, row_before);

  ASSERT_EQ(column_before.columns_size(), 5);
  const greptime::v1::Column &amount_column = column_before.columns(1);
  EXPECT_EQ(amount_column.datatype(), greptime::v1::ColumnDataType::DECIMAL128);
  ASSERT_TRUE(amount_column.has_datatype_extension());
  EXPECT_EQ(amount_column.datatype_extension().type_ext_case(),
            greptime::v1::ColumnDataTypeExtension::kDecimalType);
  EXPECT_EQ(amount_column.datatype_extension().decimal_type().precision(), 12);
  EXPECT_EQ(amount_column.datatype_extension().decimal_type().scale(), 4);
  ASSERT_EQ(amount_column.values().decimal128_values_size(), 1);
  EXPECT_EQ(amount_column.values().decimal128_values(0).hi(), -1);
  EXPECT_EQ(amount_column.values().decimal128_values(0).lo(), -12345);
  ASSERT_EQ(column_before.columns(2).values().binary_values_size(), 1);
  EXPECT_EQ(column_before.columns(2).values().binary_values(0), "{\"venue\":\"X\"}");
  ASSERT_EQ(column_before.columns(3).values().binary_values_size(), 1);
  EXPECT_EQ(column_before.columns(3).values().binary_values(0), ByteString({0, 0xff, 3}));
  ASSERT_EQ(column_before.columns(4).values().date_values_size(), 1);
  EXPECT_EQ(column_before.columns(4).values().date_values(0), 20'000);
  EXPECT_EQ(column_before.columns(2).datatype(), greptime::v1::ColumnDataType::BINARY);
  ASSERT_TRUE(column_before.columns(2).has_datatype_extension());
  EXPECT_EQ(column_before.columns(2).datatype_extension().type_ext_case(),
            greptime::v1::ColumnDataTypeExtension::kJsonType);

  const greptime::v1::Rows &encoded_rows = row_before.rows();
  ASSERT_EQ(encoded_rows.schema_size(), 5);
  ExpectSameMessage(encoded_rows.schema(1).datatype_extension(),
                    column_before.columns(1).datatype_extension());
  ExpectSameMessage(encoded_rows.schema(2).datatype_extension(),
                    column_before.columns(2).datatype_extension());
  ASSERT_EQ(encoded_rows.rows_size(), 1);
  const greptime::v1::Row &row = encoded_rows.rows(0);
  ASSERT_EQ(row.values_size(), 5);
  EXPECT_EQ(row.values(2).value_data_case(), greptime::v1::Value::kBinaryValue);
  EXPECT_EQ(row.values(2).binary_value(), "{\"venue\":\"X\"}");
  EXPECT_EQ(row.values(3).value_data_case(), greptime::v1::Value::kBinaryValue);
  EXPECT_EQ(row.values(3).binary_value(), ByteString({0, 0xff, 3}));
  EXPECT_EQ(row.values(4).value_data_case(), greptime::v1::Value::kDateValue);
  EXPECT_EQ(row.values(4).date_value(), 20'000);
}

TEST(BatchLifecycle, ConfiguredClearPolicyAndExplicitOverride) {
  ColumnarBatch columns("ticks", 16, BufferPolicy::Zero());
  ASSERT_TRUE(columns.AddTimestampColumn("time", TimestampUnit::kSecond).has_value());
  const std::array first{ValueView{TimestampSecond{1}}};
  ASSERT_TRUE(columns.PushRow(first).has_value());
  columns.ClearUnbounded();
  EXPECT_GT(columns.AllocatedBytes(), 0U);
  const std::array second{ValueView{TimestampSecond{2}}};
  ASSERT_TRUE(columns.PushRow(second).has_value());
  columns.Clear();
  EXPECT_EQ(columns.AllocatedBytes(), 0U);
  const std::array third{ValueView{TimestampSecond{3}}};
  ASSERT_TRUE(columns.PushRow(third).has_value());
  auto insert = columns.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->row_count(), 1U);
}

TEST(BatchLifecycle, RetainedByteBudgetAppliesToWholeBatch) {
  ColumnarBatch columns("ticks", 16);
  ASSERT_TRUE(columns.AddTimestampColumn("time", TimestampUnit::kSecond).has_value());
  ASSERT_TRUE(columns.AddFieldColumn("price", ColumnDataType::kFloat64).has_value());
  ASSERT_NE(columns.Column(0), nullptr);
  const std::size_t one_column_bytes = columns.Column(0)->AllocatedBytes();
  EXPECT_LT(one_column_bytes, columns.AllocatedBytes());
  const std::array first{ValueView{TimestampSecond{1}}, ValueView{42.5}};
  ASSERT_TRUE(columns.PushRow(first).has_value());
  columns.ClearWithPolicy(BufferPolicy{std::numeric_limits<std::size_t>::max(), one_column_bytes});
  EXPECT_LE(columns.AllocatedBytes(), one_column_bytes);
  const std::array second{ValueView{TimestampSecond{2}}, ValueView{43.0}};
  ASSERT_TRUE(columns.PushRow(second).has_value());
  auto insert = columns.EncodeInsertRequest();
  ASSERT_TRUE(insert.has_value());
  EXPECT_EQ(insert->row_count(), 1U);
  columns.ClearWithPolicy(BufferPolicy{0, std::numeric_limits<std::size_t>::max()});
  EXPECT_EQ(columns.RowCapacity(), 0U);

  RowBatch rows("ticks", 16);
  ASSERT_TRUE(
      rows.AddColumnSchema("time", SemanticType::kTimestamp, ColumnDataType::kTimestampSecond)
          .has_value());
  const std::array row{ValueView{TimestampSecond{1}}};
  ASSERT_TRUE(rows.PushRow(row).has_value());
  rows.ClearWithPolicy(BufferPolicy{0, std::numeric_limits<std::size_t>::max()});
  EXPECT_EQ(rows.AllocatedBytes(), 0U);
  EXPECT_EQ(rows.RowCapacity(), 0U);
}

TEST(BatchEncoding, MutableColumnReplacementCannotBypassSchemaChecks) {
  ColumnarBatch batch("ticks");
  AddTickColumns(batch);
  struct Replacement {
    const char *name;
    SemanticType role;
    ColumnDataType type;
  };
  const std::array replacements{
      Replacement{"symbol", SemanticType::kField, ColumnDataType::kFloat64},
      Replacement{" ", SemanticType::kField, ColumnDataType::kFloat64},
      Replacement{"other_time", SemanticType::kTimestamp, ColumnDataType::kTimestampSecond},
      Replacement{"other_time", SemanticType::kTimestamp, ColumnDataType::kInt64},
      Replacement{"amount", SemanticType::kField, ColumnDataType::Decimal(0, 0)},
  };
  const std::array row{Text("BTC"), ValueView{TimestampMillisecond{1}}, Null()};
  for (const Replacement &replacement : replacements) {
    auto original = batch.ReplaceColumn(
        2, TypedColumnBuf(replacement.name, replacement.role, replacement.type));
    ASSERT_TRUE(original.has_value());
    EXPECT_FALSE(batch.EncodeInsertRequest().has_value());
    EXPECT_FALSE(batch.EncodeDeleteRequest().has_value());
    EXPECT_FALSE(batch.PushRow(row).has_value());
    EXPECT_EQ(batch.RowCount(), 0U);
    for (const TypedColumnBuf &column : batch.Columns()) EXPECT_EQ(column.RowCount(), 0U);
    auto restored = batch.ReplaceColumn(2, std::move(*original));
    ASSERT_TRUE(restored.has_value());
    EXPECT_TRUE(batch.ValidateSchema().has_value());
  }
}

TEST(BatchEncoding, DirectDecimalAppendsEnforceCoefficientPrecision) {
  ColumnarBatch batch("trades");
  ASSERT_TRUE(batch.AddTimestampColumn("time", TimestampUnit::kSecond).has_value());
  ASSERT_TRUE(batch.AddColumn("amount", SemanticType::kField, ColumnDataType::Decimal(2, 0))
                  .has_value());
  ASSERT_TRUE(batch.ColumnMut(0)->Push(ValueView{TimestampSecond{1}}).has_value());
  auto append = batch.ColumnMut(1)->Push(ValueView{Decimal128{100, 2, 0}});
  EXPECT_TRUE(!append.has_value() || !batch.EncodeInsertRequest().has_value());
  EXPECT_FALSE(batch.EncodeDeleteRequest().has_value());
}

} // namespace
