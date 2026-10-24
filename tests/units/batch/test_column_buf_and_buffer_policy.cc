#include <greptimedb/batch/column_buf.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace {
using namespace greptimedb;
using Type = ColumnDataType;
using Values = greptime::v1::Column::Values;
constexpr auto kMax = std::numeric_limits<std::size_t>::max();

TypedColumnBuf Column(Type type) {
  return {"value", SemanticType::kField, type};
}
std::span<const std::byte> Bytes(std::string_view text) {
  return std::as_bytes(std::span{text.data(), text.size()});
}
std::string Mask(std::initializer_list<unsigned char> bytes) {
  return {bytes.begin(), bytes.end()};
}
template <typename Message> std::string Wire(const Message &message) {
  return message.SerializeAsString();
}

template <typename Writer>
void AssertWire(Type type, ValueView value, int datatype, Writer writer) {
  auto buf = Column(type);
  ASSERT_TRUE(buf.Push(std::monostate{}));
  ASSERT_TRUE(buf.Push(value));
  ASSERT_TRUE(buf.Push(std::monostate{}));
  EXPECT_EQ(buf.RowCount(), 3U);
  EXPECT_EQ(buf.NonNullLen(), 1U);
  EXPECT_EQ(buf.Nulls().NullCount(), 2U);
  EXPECT_EQ(buf.Name(), "value");
  EXPECT_EQ(buf.SemanticTypeValue(), SemanticType::kField);
  EXPECT_EQ(buf.DataType(), type);
  greptime::v1::Column expected;
  expected.set_column_name("value");
  expected.set_semantic_type(greptime::v1::FIELD);
  expected.set_datatype(static_cast<greptime::v1::ColumnDataType>(datatype));
  expected.set_null_mask(Mask({5}));
  writer(*expected.mutable_values());
  if (type == Type::kJson) {
    expected.mutable_datatype_extension()->set_json_type(
        greptime::v1::JSON_BINARY);
  } else if (type.kind == Type::Kind::kDecimal128) {
    auto *extension =
        expected.mutable_datatype_extension()->mutable_decimal_type();
    extension->set_precision(type.precision);
    extension->set_scale(type.scale);
  }
  EXPECT_EQ(Wire(buf.ToProtoColumn()), Wire(expected));
}

TEST(ColumnBufferUnitScenario,
     PolicyWatermarksAreIndependentInclusiveAndOverflowFree) {
  const BufferPolicy defaults;
  EXPECT_EQ(defaults.max_retained_rows, kDefaultMaxRetainedRows);
  EXPECT_EQ(defaults.max_retained_bytes, kDefaultMaxRetainedBytes);
  EXPECT_EQ(defaults.max_retained_rows, 65'536U);
  EXPECT_EQ(defaults.max_retained_bytes, 8U * 1024U * 1024U);
  const BufferPolicy policy{8, 64};
  EXPECT_FALSE(policy.ShouldShrink(8, 64));
  EXPECT_TRUE(policy.ShouldShrink(9, 0));
  EXPECT_TRUE(policy.ShouldShrink(0, 65));
  EXPECT_FALSE(BufferPolicy::Unbounded().ShouldShrink(kMax, kMax));
  EXPECT_FALSE(BufferPolicy::Zero().ShouldShrink(0, 0));
  EXPECT_TRUE(BufferPolicy::Zero().ShouldShrink(1, 0));
  EXPECT_TRUE(BufferPolicy::Zero().ShouldShrink(0, 1));
}

TEST(ColumnBufferUnitScenario,
     NullMaskUsesLittleEndianBitsAcrossByteBoundaries) {
  NullMask mask(24);
  for (std::size_t row = 0; row < 18; ++row)
    ASSERT_TRUE(mask.Push(row == 0 || row == 7 || row == 8 || row == 17));
  EXPECT_EQ(mask.RowCount(), 18U);
  EXPECT_EQ(mask.NullCount(), 4U);
  EXPECT_EQ(mask.NonNullCount(), 14U);
  EXPECT_TRUE(mask.HasNulls());
  EXPECT_TRUE(std::ranges::equal(mask.Bytes(), std::array{0x81, 1, 2}));
  EXPECT_TRUE(std::ranges::equal(mask.RawBytes(), std::array{0x81, 1, 2}));
  for (std::size_t row = 0; row < 18; ++row)
    EXPECT_EQ(mask.IsNull(row), row == 0 || row == 7 || row == 8 || row == 17);
  EXPECT_FALSE(mask.IsNull(18));
  EXPECT_FALSE(mask.IsNull(kMax));
}

TEST(ColumnBufferUnitLifecycle, NoNullsOmitWireMaskAndClearResetsStaleBits) {
  NullMask mask(64);
  EXPECT_TRUE(mask.Bytes().empty());
  for (int row = 0; row < 17; ++row)
    ASSERT_TRUE(mask.Push(true));
  EXPECT_TRUE(std::ranges::equal(mask.Bytes(), std::array{255, 255, 1}));
  const auto rows = mask.RowCapacity();
  const auto bytes = mask.AllocatedBytes();
  mask.ClearUnbounded();
  EXPECT_EQ(mask.RowCount(), 0U);
  EXPECT_EQ(mask.NullCount(), 0U);
  EXPECT_EQ(mask.RowCapacity(), rows);
  EXPECT_EQ(mask.AllocatedBytes(), bytes);
  for (int row = 0; row < 17; ++row)
    ASSERT_TRUE(mask.Push(false));
  EXPECT_TRUE(std::ranges::equal(mask.RawBytes(), std::array{0, 0, 0}));
  EXPECT_EQ(mask.NonNullCount(), 17U);
  EXPECT_FALSE(mask.HasNulls());
  EXPECT_TRUE(mask.Bytes().empty());
  mask.ClearWithPolicy(BufferPolicy::Zero());
  EXPECT_EQ(mask.RowCapacity(), 0U);
  EXPECT_EQ(mask.AllocatedBytes(), 0U);
}

TEST(ColumnBufferUnitLifecycle,
     MaskShrinkPreservesRowsAndOnlyClearDiscardsThem) {
  NullMask mask(4096);
  for (int row = 0; row < 9; ++row)
    ASSERT_TRUE(mask.Push(row == 8));
  mask.ShrinkToFit();
  EXPECT_EQ(mask.RowCount(), 9U);
  EXPECT_TRUE(std::ranges::equal(mask.Bytes(), std::array{0, 1}));
  EXPECT_GE(mask.RowCapacity(), 9U);
  EXPECT_LT(mask.RowCapacity(), 4096U);
  mask.ClearWithPolicy({0, kMax});
  EXPECT_EQ(mask.RowCapacity(), 0U);
}

TEST(ColumnBufferUnitScenario,
     FixedWidthScalarsEncodeIntoTheirDistinctWireArrays) {
#define CHECK_SCALAR(kind, cpp_type, value, datatype, field)                   \
  AssertWire(Type::k##kind, cpp_type{value}, datatype,                         \
             [](Values &wire) { wire.add_##field(value); })
  CHECK_SCALAR(Boolean, bool, true, 0, bool_values);
  CHECK_SCALAR(Int8, std::int8_t, -128, 1, i8_values);
  CHECK_SCALAR(Int16, std::int16_t, -32768, 2, i16_values);
  CHECK_SCALAR(Int32, std::int32_t, INT32_MIN, 3, i32_values);
  CHECK_SCALAR(Int64, std::int64_t, INT64_MIN, 4, i64_values);
  CHECK_SCALAR(Uint8, std::uint8_t, 255, 5, u8_values);
  CHECK_SCALAR(Uint16, std::uint16_t, 65535, 6, u16_values);
  CHECK_SCALAR(Uint32, std::uint32_t, UINT32_MAX, 7, u32_values);
  CHECK_SCALAR(Uint64, std::uint64_t, UINT64_MAX, 8, u64_values);
  CHECK_SCALAR(Float32, float, -1.5F, 9, f32_values);
  CHECK_SCALAR(Float64, double, 0.125, 10, f64_values);
#undef CHECK_SCALAR
}

TEST(ColumnBufferUnitScenario, StringsAndBinaryPreserveEmptyAndBorrowedValues) {
  const std::string text{"東京\0", 7};
  AssertWire(Type::kString, std::string_view{text}, 12,
             [&](Values &wire) { wire.add_string_values(text); });
  const std::string binary = Mask({0, 255, 128});
  AssertWire(Type::kBinary, Bytes(binary), 11,
             [&](Values &wire) { wire.add_binary_values(binary); });
  auto buf = Column(Type::kString);
  {
    const std::string input = "original";
    ASSERT_TRUE(buf.Push(std::string_view{input}));
  }
  ASSERT_TRUE(buf.Push(std::string_view{}));
  const auto wire = buf.ToProtoColumn();
  ASSERT_EQ(wire.values().string_values_size(), 2);
  EXPECT_EQ(wire.values().string_values(0), "original");
  EXPECT_TRUE(wire.values().string_values(1).empty());
  EXPECT_TRUE(wire.null_mask().empty());
}

TEST(ColumnBufferUnitScenario,
     MismatchAndTimestampNullRejectionLeaveBuffersUnchanged) {
  TypedColumnBuf timestamp("ts", SemanticType::kTimestamp,
                           Type::kTimestampNanosecond);
  ASSERT_TRUE(timestamp.Push(TimestampNanosecond{42}));
  const auto before = Wire(timestamp.ToProtoColumn());
  EXPECT_FALSE(timestamp.Push(std::monostate{}));
  EXPECT_FALSE(timestamp.Push(std::int64_t{42}));
  EXPECT_EQ(Wire(timestamp.ToProtoColumn()), before);
  EXPECT_EQ(timestamp.RowCount(), 1U);
  auto integer = Column(Type::kInt8);
  ASSERT_TRUE(integer.Push(std::monostate{}));
  const auto before_integer = Wire(integer.ToProtoColumn());
  EXPECT_FALSE(integer.Push(std::int16_t{127}));
  EXPECT_FALSE(integer.Push(std::string_view{"127"}));
  EXPECT_EQ(Wire(integer.ToProtoColumn()), before_integer);
  EXPECT_EQ(integer.NonNullLen(), 0U);
}

TEST(ColumnBufferUnitScenario, NullOnlyColumnsDoNotCreatePlaceholderValues) {
  auto buf = Column(Type::kInt64);
  for (int row = 0; row < 9; ++row)
    ASSERT_TRUE(buf.Push(std::monostate{}));
  const auto wire = buf.ToProtoColumn();
  EXPECT_EQ(wire.null_mask(), Mask({255, 1}));
  EXPECT_TRUE(wire.has_values());
  EXPECT_TRUE(Wire(wire.values()).empty());
  EXPECT_EQ(buf.NonNullLen(), 0U);
  EXPECT_EQ(buf.RowCount(), 9U);
}

TEST(ColumnBufferUnitScenario,
     DecimalSplitRoundTripsSignedExtremesAndEnforcesSchema) {
  constexpr __int128 minimum = std::numeric_limits<__int128>::min();
  constexpr __int128 maximum = std::numeric_limits<__int128>::max();
  for (const auto &[value, hi, lo] :
       std::array{std::tuple{minimum, INT64_MIN, std::uint64_t{0}},
                  std::tuple{__int128{-1}, std::int64_t{-1}, UINT64_MAX},
                  std::tuple{__int128{0}, std::int64_t{0}, std::uint64_t{0}},
                  std::tuple{__int128{1}, std::int64_t{0}, std::uint64_t{1}},
                  std::tuple{maximum, INT64_MAX, UINT64_MAX}}) {
    const Decimal128 decimal{value, 38, 9};
    const auto proto = decimal.ToProto();
    EXPECT_EQ(decimal.Hi(), hi);
    EXPECT_EQ(decimal.Lo(), lo);
    EXPECT_EQ(proto.hi(), hi);
    EXPECT_EQ(static_cast<std::uint64_t>(proto.lo()), lo);
    EXPECT_TRUE(Decimal128::FromProto(proto, 38, 9) == decimal);
  }
  auto buf = Column(Type::Decimal(38, 9));
  ASSERT_TRUE(buf.Push(Decimal128{-42, 38, 9}));
  const auto before = Wire(buf.ToProtoColumn());
  EXPECT_FALSE(buf.Push(Decimal128{-42, 37, 9}));
  EXPECT_FALSE(buf.Push(Decimal128{-42, 38, 8}));
  EXPECT_EQ(Wire(buf.ToProtoColumn()), before);
}

TEST(ColumnBufferUnitScenario, TemporalTypesKeepUnitsAndSignedTicksOnWire) {
  EXPECT_EQ(TimestampUnit{}, TimestampUnit::kNanosecond);
#define CHECK_TIME(kind, wrapper, tick, datatype, field)                       \
  AssertWire(Type::k##kind, wrapper{tick}, datatype,                           \
             [](Values &wire) { wire.add_##field(tick); })
  CHECK_TIME(Date, Date, -1, 13, date_values);
  CHECK_TIME(DateTime, DateTime, -2, 14, datetime_values);
  CHECK_TIME(TimestampSecond, TimestampSecond, -3, 15, timestamp_second_values);
  CHECK_TIME(TimestampMillisecond, TimestampMillisecond, -4, 16,
             timestamp_millisecond_values);
  CHECK_TIME(TimestampMicrosecond, TimestampMicrosecond, -5, 17,
             timestamp_microsecond_values);
  CHECK_TIME(TimestampNanosecond, TimestampNanosecond, -6, 18,
             timestamp_nanosecond_values);
#undef CHECK_TIME
  for (const auto &[unit, type, hour] : std::array{
           std::tuple{TimestampUnit::kSecond, Type::kTimestampSecond,
                      std::int64_t{3600}},
           std::tuple{TimestampUnit::kMillisecond, Type::kTimestampMillisecond,
                      std::int64_t{3600000}},
           std::tuple{TimestampUnit::kMicrosecond, Type::kTimestampMicrosecond,
                      std::int64_t{3600000000}},
           std::tuple{TimestampUnit::kNanosecond, Type::kTimestampNanosecond,
                      std::int64_t{3600000000000}}}) {
    EXPECT_EQ(ToColumnDataType(unit), type);
    EXPECT_EQ(OneHourTicks(unit), hour);
  }
}

TEST(ColumnBufferUnitScenario,
     JsonAndDecimalExtensionsAreIndependentWireFixtures) {
  AssertWire(Type::kJson, JsonView{"{}"}, 11,
             [](Values &wire) { wire.add_binary_values("{}"); });
  AssertWire(Type::Decimal(38, 9), Decimal128{-1, 38, 9}, 30, [](Values &wire) {
    auto *decimal = wire.add_decimal128_values();
    decimal->set_hi(-1);
    decimal->set_lo(-1);
  });
  auto json = Column(Type::kJson);
  ASSERT_TRUE(json.Push(JsonView{"{\"city\":\"東京\"}"}));
  const auto json_wire = json.ToProtoColumn();
  EXPECT_EQ(static_cast<int>(json_wire.datatype()), 11);
  ASSERT_EQ(json_wire.values().binary_values_size(), 1);
  EXPECT_EQ(json_wire.values().binary_values(0), "{\"city\":\"東京\"}");
  ASSERT_TRUE(json_wire.has_datatype_extension());
  EXPECT_TRUE(json_wire.datatype_extension().has_json_type());
  EXPECT_EQ(static_cast<int>(json_wire.datatype_extension().json_type()), 0);
  auto decimal = Column(Type::Decimal(38, 9));
  ASSERT_TRUE(decimal.Push(Decimal128{-1, 38, 9}));
  const auto decimal_wire = decimal.ToProtoColumn();
  EXPECT_EQ(static_cast<int>(decimal_wire.datatype()), 30);
  ASSERT_EQ(decimal_wire.values().decimal128_values_size(), 1);
  EXPECT_EQ(decimal_wire.values().decimal128_values(0).hi(), -1);
  EXPECT_EQ(decimal_wire.values().decimal128_values(0).lo(), -1);
  ASSERT_TRUE(decimal_wire.has_datatype_extension());
  ASSERT_TRUE(decimal_wire.datatype_extension().has_decimal_type());
  EXPECT_EQ(decimal_wire.datatype_extension().decimal_type().precision(), 38);
  EXPECT_EQ(decimal_wire.datatype_extension().decimal_type().scale(), 9);
}

TEST(ColumnBufferUnitScenario, SemanticsAndRowValuesMatchProtoVariants) {
  for (const auto &[semantic, number, flight] :
       std::array{std::tuple{SemanticType::kTag, 0, "tag"},
                  std::tuple{SemanticType::kField, 1, "field"},
                  std::tuple{SemanticType::kTimestamp, 2, "timestamp"}}) {
    EXPECT_EQ(static_cast<int>(ToProto(semantic)), number);
    EXPECT_EQ(AsFlightStr(semantic), flight);
  }
  EXPECT_EQ(ToProtoValue(ValueView{std::monostate{}}).value_data_case(),
            greptime::v1::Value::VALUE_DATA_NOT_SET);
#define CHECK_ROW(value, field, expected)                                      \
  {                                                                            \
    greptime::v1::Value wire;                                                  \
    wire.set_##field(expected);                                                \
    EXPECT_EQ(Wire(ToProtoValue(ValueView{value})), Wire(wire));               \
  }
  CHECK_ROW(true, bool_value, true);
  CHECK_ROW(std::int8_t{-128}, i8_value, -128);
  CHECK_ROW(std::int16_t{-32768}, i16_value, -32768);
  CHECK_ROW(std::int32_t{INT32_MIN}, i32_value, INT32_MIN);
  CHECK_ROW(std::int64_t{INT64_MIN}, i64_value, INT64_MIN);
  CHECK_ROW(std::uint8_t{255}, u8_value, 255);
  CHECK_ROW(std::uint16_t{65535}, u16_value, 65535);
  CHECK_ROW(std::uint32_t{UINT32_MAX}, u32_value, UINT32_MAX);
  CHECK_ROW(std::uint64_t{UINT64_MAX}, u64_value, UINT64_MAX);
  CHECK_ROW(float{1.5F}, f32_value, 1.5F);
  CHECK_ROW(double{-0.5}, f64_value, -0.5);
  CHECK_ROW(std::string_view{"a"}, string_value, "a");
  CHECK_ROW(Bytes(Mask({0, 255})), binary_value, Mask({0, 255}));
  CHECK_ROW(Date{-1}, date_value, -1);
  CHECK_ROW(DateTime{-2}, datetime_value, -2);
  CHECK_ROW(TimestampSecond{-3}, timestamp_second_value, -3);
  CHECK_ROW(TimestampMillisecond{-4}, timestamp_millisecond_value, -4);
  CHECK_ROW(TimestampMicrosecond{-5}, timestamp_microsecond_value, -5);
  CHECK_ROW(TimestampNanosecond{-6}, timestamp_nanosecond_value, -6);
  CHECK_ROW(JsonView{"{}"}, binary_value, "{}");
#undef CHECK_ROW
  greptime::v1::Value decimal;
  decimal.mutable_decimal128_value()->set_hi(-1);
  decimal.mutable_decimal128_value()->set_lo(-1);
  EXPECT_EQ(Wire(ToProtoValue(ValueView{Decimal128{-1, 38, 9}})),
            Wire(decimal));
}

TEST(ColumnBufferUnitLifecycle,
     ClearedFixedWidthColumnsRetainOrDropCapacityByWatermark) {
  TypedColumnBuf buf("n", SemanticType::kField, Type::kInt64, 1024);
  for (std::int64_t value = 0; value < 10; ++value)
    ASSERT_TRUE(buf.Push(value));
  const auto before = Wire(buf.ToProtoColumn());
  const auto large = buf.AllocatedBytes();
  buf.ShrinkToFit();
  EXPECT_EQ(Wire(buf.ToProtoColumn()), before);
  EXPECT_LT(buf.AllocatedBytes(), large);
  const auto bytes = buf.AllocatedBytes();
  const auto rows = buf.RowCapacity();
  buf.ClearWithPolicy({rows, bytes});
  EXPECT_EQ(buf.RowCount(), 0U);
  EXPECT_EQ(buf.NonNullLen(), 0U);
  EXPECT_EQ(buf.AllocatedBytes(), bytes);
  EXPECT_EQ(buf.RowCapacity(), rows);
  ASSERT_TRUE(buf.Push(std::int64_t{99}));
  ASSERT_EQ(buf.ToProtoColumn().values().i64_values_size(), 1);
  EXPECT_EQ(buf.ToProtoColumn().values().i64_values(0), 99);
  buf.ClearWithPolicy({kMax, bytes - 1});
  EXPECT_EQ(buf.AllocatedBytes(), 0U);
  EXPECT_EQ(buf.RowCapacity(), 0U);
  ASSERT_TRUE(buf.Push(std::monostate{}));
  buf.ClearWithPolicy({0, kMax});
  EXPECT_EQ(buf.AllocatedBytes(), 0U);
}

ValueView VariableValue(Type type, std::string_view value) {
  if (type == Type::kString)
    return value;
  if (type == Type::kBinary)
    return Bytes(value);
  return JsonView{value};
}
TEST(ColumnBufferUnitLifecycle,
     NestedStringBinaryAndJsonCapacityIsBudgetedAndReused) {
  const std::string large(16 * 1024, 'x');
  for (auto type : {Type::kString, Type::kBinary, Type::kJson}) {
    auto buf = Column(type);
    for (int row = 0; row < 3; ++row)
      ASSERT_TRUE(buf.Push(VariableValue(type, large)));
    EXPECT_GE(buf.AllocatedBytes(), 3 * large.size());
    buf.ClearUnbounded();
    EXPECT_GE(buf.AllocatedBytes(), 3 * large.size());
    ASSERT_TRUE(buf.ReserveRows(64));
    const auto warm = buf.AllocatedBytes();
    EXPECT_GE(warm, 3 * large.size());
    EXPECT_EQ(buf.RowCount(), 0U);
    EXPECT_EQ(buf.NonNullLen(), 0U);
    for (int row = 0; row < 3; ++row)
      ASSERT_TRUE(buf.Push(VariableValue(type, "small")));
    EXPECT_EQ(buf.AllocatedBytes(), warm);
    const auto wire = Wire(buf.ToProtoColumn());
    buf.ShrinkToFit();
    EXPECT_EQ(Wire(buf.ToProtoColumn()), wire);
    EXPECT_LT(buf.AllocatedBytes(), warm);
    buf.ClearWithPolicy(BufferPolicy::Zero());
    EXPECT_EQ(buf.AllocatedBytes(), 0U);
    EXPECT_EQ(buf.RowCapacity(), 0U);
  }
}

TEST(ColumnBufferUnitLifecycle,
     CloneAndClearAreIndependentAndDefaultReleasesBurstCapacity) {
  auto buf = Column(Type::kString);
  ASSERT_TRUE(buf.Push(std::string_view{"retained"}));
  auto cloned = buf.Clone();
  ASSERT_TRUE(cloned);
  buf.Clear();
  EXPECT_EQ(buf.RowCount(), 0U);
  EXPECT_EQ(cloned->RowCount(), 1U);
  EXPECT_EQ(cloned->ToProtoColumn().values().string_values(0), "retained");
  TypedColumnBuf burst("burst", SemanticType::kField, Type::kInt8, 65'537);
  EXPECT_GT(burst.RowCapacity(), 65'536U);
  burst.Clear();
  EXPECT_EQ(burst.RowCapacity(), 0U);
  EXPECT_EQ(burst.AllocatedBytes(), 0U);
}

template <typename T> void AssertWrapper() {
  static_assert(std::is_trivially_copyable_v<T>);
  EXPECT_TRUE(T{} == T{0});
  EXPECT_TRUE(T{-1} < T{1});
  std::set<T> values;
  values.insert(T{-1});
  values.insert(T{1});
  values.insert(T{-1});
  EXPECT_EQ(values.size(), 2U);
}
TEST(ColumnBufferUnitScenario,
     TypedTemporalWrappersHaveValueSemanticsAndZeroDefaults) {
  AssertWrapper<TimestampSecond>();
  AssertWrapper<TimestampMillisecond>();
  AssertWrapper<TimestampMicrosecond>();
  AssertWrapper<TimestampNanosecond>();
  AssertWrapper<Date>();
  AssertWrapper<DateTime>();
}

TEST(ColumnBufferUnitScenario,
     EveryTimestampUnitRejectsNullBeforeAndAfterValues) {
  for (const auto &[type, value] : std::array{
           std::pair{Type::kTimestampSecond, ValueView{TimestampSecond{1}}},
           std::pair{Type::kTimestampMillisecond,
                     ValueView{TimestampMillisecond{1}}},
           std::pair{Type::kTimestampMicrosecond,
                     ValueView{TimestampMicrosecond{1}}},
           std::pair{Type::kTimestampNanosecond,
                     ValueView{TimestampNanosecond{1}}}}) {
    TypedColumnBuf buf("ts", SemanticType::kTimestamp, type);
    EXPECT_FALSE(buf.Push(std::monostate{}));
    EXPECT_EQ(buf.RowCount(), 0U);
    ASSERT_TRUE(buf.Push(value));
    EXPECT_FALSE(buf.Push(std::monostate{}));
    EXPECT_EQ(buf.RowCount(), 1U);
    EXPECT_EQ(buf.NonNullLen(), 1U);
    EXPECT_TRUE(buf.ToProtoColumn().null_mask().empty());
  }
}

TEST(ColumnBufferUnitScenario, FloatsPreserveNanInfinityAndNegativeZeroBits) {
  auto f32 = Column(Type::kFloat32);
  const std::array<std::uint32_t, 3> bits32{0x7fc01234, 0x7f800000, 0x80000000};
  for (auto bits : bits32)
    ASSERT_TRUE(f32.Push(std::bit_cast<float>(bits)));
  const auto wire32 = f32.ToProtoColumn();
  ASSERT_EQ(wire32.values().f32_values_size(), 3);
  for (int index = 0; index < 3; ++index)
    EXPECT_EQ(std::bit_cast<std::uint32_t>(wire32.values().f32_values(index)),
              bits32[static_cast<std::size_t>(index)]);
  auto f64 = Column(Type::kFloat64);
  const std::array<std::uint64_t, 3> bits64{
      0x7ff8000000001234, 0xfff0000000000000, 0x8000000000000000};
  for (auto bits : bits64)
    ASSERT_TRUE(f64.Push(std::bit_cast<double>(bits)));
  const auto wire64 = f64.ToProtoColumn();
  ASSERT_EQ(wire64.values().f64_values_size(), 3);
  for (int index = 0; index < 3; ++index)
    EXPECT_EQ(std::bit_cast<std::uint64_t>(wire64.values().f64_values(index)),
              bits64[static_cast<std::size_t>(index)]);
}

TEST(ColumnBufferUnitScenario, BoundaryMasksAndNonNullValuesStayInLockstep) {
  struct Fixture {
    std::size_t rows;
    bool sparse;
    std::string mask;
  };
  for (const auto &[rows, sparse, mask] : std::array{
           Fixture{8, false, Mask({0x55})}, Fixture{9, false, Mask({0x55, 1})},
           Fixture{16, false, Mask({0x55, 0x55})},
           Fixture{17, false, Mask({0x55, 0x55, 1})},
           Fixture{64, false, std::string(8, '\x55')},
           Fixture{65, false, std::string(8, '\x55') + Mask({1})},
           Fixture{8, true, Mask({0x80})}, Fixture{9, true, Mask({0x80, 1})},
           Fixture{16, true, Mask({0x80, 0x80})},
           Fixture{17, true, Mask({0x80, 0, 1})},
           Fixture{64, true, Mask({0x80, 0, 0, 0, 0, 0, 0, 0x80})},
           Fixture{65, true, Mask({0x80, 0, 0, 0, 0, 0, 0, 0, 1})}}) {
    auto buf = Column(Type::kInt64);
    Values expected;
    for (std::size_t row = 0; row < rows; ++row) {
      const bool null = sparse ? row == 7 || row == rows - 1 : row % 2 == 0;
      if (null) {
        ASSERT_TRUE(buf.Push(std::monostate{}));
      } else {
        expected.add_i64_values(static_cast<std::int64_t>(row));
        ASSERT_TRUE(buf.Push(static_cast<std::int64_t>(row)));
      }
      EXPECT_EQ(buf.Nulls().IsNull(row), null);
    }
    const auto wire = buf.ToProtoColumn();
    EXPECT_EQ(buf.RowCount(), rows);
    EXPECT_EQ(buf.NonNullLen(),
              static_cast<std::size_t>(expected.i64_values_size()));
    EXPECT_EQ(wire.null_mask(), mask);
    EXPECT_EQ(Wire(wire.values()), Wire(expected));
    std::size_t nulls = 0;
    for (unsigned char byte : wire.null_mask())
      nulls += std::popcount(byte);
    EXPECT_EQ(buf.NonNullLen() + nulls, rows);
  }
}

TEST(ColumnBufferUnitScenario,
     RejectedValuesThenValidAppendsPreserveExactWireState) {
  auto integer = Column(Type::kInt8);
  ASSERT_TRUE(integer.Push(std::monostate{}));
  EXPECT_FALSE(integer.Push(std::int16_t{42}));
  ASSERT_TRUE(integer.Push(std::int8_t{42}));
  EXPECT_EQ(integer.RowCount(), 2U);
  EXPECT_EQ(integer.ToProtoColumn().null_mask(), Mask({1}));
  Values expected_integer;
  expected_integer.add_i8_values(42);
  EXPECT_EQ(Wire(integer.ToProtoColumn().values()), Wire(expected_integer));
  TypedColumnBuf timestamp("ts", SemanticType::kTimestamp,
                           Type::kTimestampMillisecond);
  EXPECT_FALSE(timestamp.Push(std::monostate{}));
  ASSERT_TRUE(timestamp.Push(TimestampMillisecond{42}));
  EXPECT_EQ(timestamp.RowCount(), 1U);
  EXPECT_TRUE(timestamp.ToProtoColumn().null_mask().empty());
  Values expected_timestamp;
  expected_timestamp.add_timestamp_millisecond_values(42);
  EXPECT_EQ(Wire(timestamp.ToProtoColumn().values()), Wire(expected_timestamp));
  auto decimal = Column(Type::Decimal(38, 9));
  ASSERT_TRUE(decimal.Push(std::monostate{}));
  for (auto [precision, scale] :
       std::array<std::pair<std::uint32_t, std::uint32_t>, 6>{
           {{0, 0}, {39, 0}, {38, 39}, {38, UINT32_MAX}, {37, 9}, {38, 8}}}) {
    const auto before = Wire(decimal.ToProtoColumn());
    EXPECT_FALSE(decimal.Push(Decimal128{1, precision, scale}));
    EXPECT_EQ(Wire(decimal.ToProtoColumn()), before);
    ASSERT_TRUE(decimal.Push(Decimal128{-1, 38, 9}));
  }
  EXPECT_EQ(decimal.RowCount(), 7U);
  const auto wire = decimal.ToProtoColumn();
  EXPECT_EQ(wire.null_mask(), Mask({1}));
  ASSERT_EQ(wire.values().decimal128_values_size(), 6);
  for (const auto &value : wire.values().decimal128_values()) {
    EXPECT_EQ(value.hi(), -1);
    EXPECT_EQ(value.lo(), -1);
  }
  for (auto [precision, scale] :
       std::array<std::pair<std::uint32_t, std::uint32_t>, 5>{
           {{0, 0}, {39, 0}, {UINT32_MAX, 0}, {1, 2}, {38, UINT32_MAX}}}) {
    auto invalid = Column(Type::Decimal(precision, scale));
    for (const auto &value : {ValueView{std::monostate{}},
                              ValueView{Decimal128{1, precision, scale}}}) {
      EXPECT_FALSE(invalid.Push(value));
      EXPECT_EQ(invalid.RowCount(), 0U);
      EXPECT_EQ(invalid.NonNullLen(), 0U);
      EXPECT_EQ(invalid.Nulls().NullCount(), 0U);
      const auto rejected = invalid.TryToProtoColumn();
      ASSERT_FALSE(rejected);
      EXPECT_EQ(rejected.error(), ColumnBufError::kInvalidDecimal);
    }
  }
}

TEST(ColumnBufferUnitScenario,
     CheckedConstructionAndSerializationRejectInvalidSchemas) {
  auto valid =
      TypedColumnBuf::Create("value", SemanticType::kField, Type::kInt64, 8);
  ASSERT_TRUE(valid);
  EXPECT_TRUE(valid->IsValid());
  ASSERT_TRUE(valid->Push(std::int64_t{42}));
  const auto checked = valid->TryToProtoColumn();
  ASSERT_TRUE(checked);
  EXPECT_EQ(Wire(*checked), Wire(valid->ToProtoColumn()));
  auto invalid = TypedColumnBuf::Create("value", SemanticType::kField,
                                        Type::Decimal(0, 0));
  ASSERT_FALSE(invalid);
  EXPECT_EQ(invalid.error(), ColumnBufError::kInvalidDecimal);
  TypedColumnBuf direct("value", SemanticType::kField, Type::Decimal(38, 39));
  EXPECT_FALSE(direct.IsValid());
  const auto rejected = direct.TryToProtoColumn();
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), ColumnBufError::kInvalidDecimal);
}

TEST(ColumnBufferUnitLifecycle,
     MovesLeaveSourcesSafeAndPermitExplicitReinitialization) {
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

TEST(ColumnBufferUnitScenario,
     ImpossibleReservationsFailWithoutChangingRowsOrPayload) {
  NullMask mask;
  ASSERT_TRUE(mask.Push(true));
  const auto mask_bytes =
      std::vector<std::uint8_t>(mask.Bytes().begin(), mask.Bytes().end());
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
  ASSERT_TRUE(column.Push(std::int64_t{43}));
  EXPECT_EQ(column.RowCount(), 2U);
}

TEST(ColumnBufferUnitLifecycle,
     MemoryAccountingAndRetentionCoverEveryStorageFamily) {
  for (const auto &[type, value] : std::array{
           std::pair{Type::kString, ValueView{std::string_view{"payload"}}},
           std::pair{Type::kBinary, ValueView{Bytes("payload")}},
           std::pair{Type::kJson, ValueView{JsonView{"{\"k\":1}"}}},
           std::pair{Type::kInt64, ValueView{std::int64_t{42}}},
           std::pair{Type::kFloat64, ValueView{1.5}},
           std::pair{Type::kTimestampMillisecond,
                     ValueView{TimestampMillisecond{42}}},
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
