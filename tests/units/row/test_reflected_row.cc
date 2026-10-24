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

namespace allocation_probe {
thread_local bool enabled = false;
thread_local std::size_t count = 0;
thread_local bool fail_nothrow = false;
thread_local std::size_t nothrow_remaining = 0;
bool RejectNothrow() noexcept {
  if (!fail_nothrow) return false;
  if (nothrow_remaining == 0) return true;
  --nothrow_remaining;
  return false;
}

void *Allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t)) noexcept {
  if (enabled) ++count;
  size = std::max(size, std::size_t{1});
  if (alignment <= alignof(std::max_align_t)) return std::malloc(size);
  void *result = nullptr;
  if (posix_memalign(&result, alignment, size) != 0) return nullptr;
  return result;
}
void *Require(void *memory) noexcept {
  if (!memory) std::abort();
  return memory;
}
} // namespace allocation_probe

void *operator new(std::size_t size) {
  return allocation_probe::Require(allocation_probe::Allocate(size));
}
void *operator new[](std::size_t size) { return ::operator new(size); }
void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  if (allocation_probe::RejectNothrow()) return nullptr;
  return allocation_probe::Allocate(size);
}
void *operator new[](std::size_t size, const std::nothrow_t &tag) noexcept {
  return ::operator new(size, tag);
}
void *operator new(std::size_t size, std::align_val_t alignment) {
  return allocation_probe::Require(
      allocation_probe::Allocate(size, static_cast<std::size_t>(alignment)));
}
void *operator new[](std::size_t size, std::align_val_t alignment) {
  return ::operator new(size, alignment);
}
void *operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t &) noexcept {
  if (allocation_probe::RejectNothrow()) return nullptr;
  return allocation_probe::Allocate(size, static_cast<std::size_t>(alignment));
}
void *operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t &tag) noexcept {
  return ::operator new(size, alignment, tag);
}
void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete(void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete[](void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t, const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, std::align_val_t, const std::nothrow_t &) noexcept {
  std::free(memory);
}

namespace {
using namespace greptimedb;

struct [[= table{.name = "ticks", .rename_all = RenameRule::kCamelCase}]] TradeTick {
  [[= tag{.column = "symbol"}]] std::string instrument_name;
  [[= timestamp{}]] std::int64_t event_time;
  std::optional<double> price;
  std::optional<std::vector<std::byte>> payload;
  [[= field{.json = true}]] std::string document;
  [[= field{.decimal_precision = 38, .decimal_scale = 10}]] __int128 coefficient;
};

static_assert(BatchEncodeConcept<TypedColumnarBatch<TradeTick>>);

template <RenameRule Rule>
struct [[= table{.rename_all = Rule}]] Naming {
  [[= timestamp{}]] std::int64_t event_time;
  std::int32_t HTTPServerID;
  [[= field{.column = "Exact Name"}]] double custom_name;
};

template <RenameRule Rule>
consteval bool Names(std::string_view time, std::string_view acronym) {
  constexpr auto columns = row_detail::RowSchema<Naming<Rule>>::kColumns;
  return columns[0].name == time && columns[1].name == acronym &&
         columns[2].name == "Exact Name";
}

static_assert(Names<RenameRule::kNone>("event_time", "HTTPServerID"));
static_assert(Names<RenameRule::kLowercase>("event_time", "httpserverid"));
static_assert(Names<RenameRule::kUppercase>("EVENT_TIME", "HTTPSERVERID"));
static_assert(Names<RenameRule::kSnakeCase>("event_time", "http_server_id"));
static_assert(Names<RenameRule::kCamelCase>("eventTime", "httpServerId"));
static_assert(Names<RenameRule::kPascalCase>("EventTime", "HttpServerId"));
static_assert(Names<RenameRule::kScreamingSnakeCase>("EVENT_TIME", "HTTP_SERVER_ID"));
static_assert(Names<RenameRule::kKebabCase>("event-time", "http-server-id"));

struct HTTPTradeTick {
  [[= timestamp{}]] TimestampMillisecond time;
};
static_assert(row_detail::RowSchema<HTTPTradeTick>::kTableName == "http_trade_tick");
static_assert(RowConcept<TradeTick>);
static_assert(BatchEncodeConcept<TypedColumnarBatch<TradeTick>>);
static_assert(MemoryBufferConcept<TypedColumnarBatch<TradeTick>>);
static_assert(std::same_as<std::remove_cvref_t<decltype(
    std::declval<const BorrowedRow<TradeTick> &>().Get<0>())>, std::string_view>);
static_assert(std::same_as<std::remove_cvref_t<decltype(
    std::declval<const BorrowedRow<TradeTick> &>().Get<3>())>, std::optional<BinaryView>>);
static_assert(!std::constructible_from<BorrowedRow<TradeTick>, TradeTick &&>);
static_assert(!std::constructible_from<RowView<TradeTick>, TradeTick &&>);
static_assert(!std::is_move_constructible_v<TypedColumnarBatch<TradeTick>>);

struct MissingTime { std::int64_t ts; };
struct TwiceTime {
  [[= timestamp{}]] std::int64_t first;
  [[= timestamp{}]] TimestampSecond second;
};
template <class T> struct TimeType { [[= timestamp{}]] T time; };
template <class T> struct FieldType {
  [[= timestamp{}]] std::int64_t time;
  T value;
};
struct DuplicateName {
  [[= timestamp{.column = "value"}]] std::int64_t time;
  double value;
};
struct [[= table{.rename_all = RenameRule::kCamelCase}]] DuplicateAfterRename {
  [[= timestamp{}]] std::int64_t event_time;
  double eventTime;
};
struct [[= table{.name = " \t\n"}]] BlankTable { [[= timestamp{}]] std::int64_t time; };
struct [[= table{.name = "bad\0name"}]] NulTable {
  [[= timestamp{}]] std::int64_t time;
};
struct BlankColumn { [[= timestamp{.column = " \t"}]] std::int64_t time; };
struct [[= table{.name = ""}]] EmptyTable { [[= timestamp{}]] std::int64_t time; };
struct EmptyColumn { [[= timestamp{.column = ""}]] std::int64_t time; };
struct ConflictingSemantic {
  [[= timestamp{}]] std::int64_t time;
  [[= tag{}, = field{}]] std::string label;
};
struct DuplicateAnnotation {
  [[= timestamp{}]] std::int64_t time;
  [[= tag{}, = tag{}]] std::string label;
};
struct BadJson {
  [[= timestamp{}]] std::int64_t time;
  [[= field{.json = true}]] std::vector<std::byte> value;
};
template <std::uint32_t Precision, std::uint32_t Scale>
struct BadDecimal {
  [[= timestamp{}]] std::int64_t time;
  [[= field{.decimal_precision = Precision, .decimal_scale = Scale}]] __int128 value;
};
struct DecimalOnFloat {
  [[= timestamp{}]] std::int64_t time;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] double value;
};
struct [[= tag{}]] WrongTableScope { [[= timestamp{}]] std::int64_t time; };
struct WrongMemberScope { [[= timestamp{}, = table{}]] std::int64_t time; };
struct [[= table{}, = table{}]] DuplicateTable { [[= timestamp{}]] std::int64_t time; };
struct BadUnit {
  [[= timestamp{.unit = static_cast<TimestampUnit>(-1)}]] std::int64_t time;
};
struct ConflictingUnit {
  [[= timestamp{.unit = TimestampUnit::kMillisecond}]] TimestampSecond time;
};
struct ExplicitNsOnSecond {
  [[= timestamp{.unit = TimestampUnit::kNanosecond}]] TimestampSecond time;
};
struct ExplicitSecond {
  [[= timestamp{.unit = TimestampUnit::kSecond}]] TimestampSecond time;
};
struct IncompleteRow;
struct BaseFields { double value; };
struct InheritedRow : BaseFields { [[= timestamp{}]] std::int64_t time; };
struct PrivateBaseRow : private BaseFields { [[= timestamp{}]] std::int64_t time; };
struct AnonymousUnionRow {
  [[= timestamp{}]] std::int64_t time;
  union { double floating; std::int64_t integer; };
};

static_assert(!RowConcept<int>);
static_assert(!RowConcept<MissingTime>);
static_assert(!RowConcept<TwiceTime>);
static_assert(!RowConcept<TimeType<std::optional<std::int64_t>>>);
static_assert(!RowConcept<TimeType<std::optional<TimestampNanosecond>>>);
static_assert(!RowConcept<TimeType<std::uint64_t>>);
static_assert(!RowConcept<TimeType<std::string>>);
static_assert(!RowConcept<FieldType<std::optional<std::optional<double>>>>);
static_assert(!RowConcept<FieldType<std::vector<std::int32_t>>>);
static_assert(!RowConcept<FieldType<std::array<std::byte, 8>>>);
static_assert(!RowConcept<FieldType<const double *>>);
static_assert(!RowConcept<FieldType<Decimal128>>);
static_assert(!RowConcept<DuplicateName>);
static_assert(!RowConcept<DuplicateAfterRename>);
static_assert(!RowConcept<BlankTable>);
static_assert(!RowConcept<NulTable>);
static_assert(!RowConcept<BlankColumn>);
static_assert(!RowConcept<EmptyTable>);
static_assert(!RowConcept<EmptyColumn>);
static_assert(!RowConcept<ConflictingSemantic>);
static_assert(!RowConcept<DuplicateAnnotation>);
static_assert(!RowConcept<BadJson>);
static_assert(!RowConcept<BadDecimal<0, 0>>);
static_assert(!RowConcept<BadDecimal<39, 0>>);
static_assert(!RowConcept<BadDecimal<4, 5>>);
static_assert(!RowConcept<DecimalOnFloat>);
static_assert(!RowConcept<WrongTableScope>);
static_assert(!RowConcept<WrongMemberScope>);
static_assert(!RowConcept<DuplicateTable>);
static_assert(!RowConcept<BadUnit>);
static_assert(!RowConcept<ConflictingUnit>);
static_assert(!RowConcept<ExplicitNsOnSecond>);
static_assert(RowConcept<ExplicitSecond>);
static_assert(!RowConcept<IncompleteRow>);
static_assert(!RowConcept<InheritedRow>);
static_assert(!RowConcept<PrivateBaseRow>);
static_assert(!RowConcept<AnonymousUnionRow>);
static_assert(!RowConcept<const TradeTick>);

struct ScalarRow {
  [[= timestamp{.unit = TimestampUnit::kMicrosecond}]] std::int64_t time;
  bool flag;
  std::int8_t i8;
  std::int16_t i16;
  std::int32_t i32;
  std::int64_t i64;
  std::uint8_t u8;
  std::uint16_t u16;
  std::uint32_t u32;
  std::uint64_t u64;
  float f32;
  double f64;
  Date day;
  DateTime datetime;
  TimestampSecond seconds;
  TimestampMillisecond millis;
  TimestampMicrosecond micros;
  TimestampNanosecond nanos;
};
static_assert(RowConcept<ScalarRow>);

TEST(ReflectedRowUnit, ReflectionPreservesSemanticTypesAndLogicalMetadata) {
  constexpr auto columns = row_detail::RowSchema<TradeTick>::kColumns;
  static_assert(columns.size() == 6);
  static_assert(columns[0].semantic_type == SemanticType::kTag);
  static_assert(columns[1].semantic_type == SemanticType::kTimestamp);
  static_assert(columns[1].data_type == ColumnDataType::kTimestampNanosecond);
  static_assert(columns[2].nullable && columns[3].nullable);
  static_assert(columns[4].data_type == ColumnDataType::kJson);
  static_assert(columns[5].data_type == ColumnDataType::Decimal(38, 10));
  EXPECT_EQ(columns[0].name, "symbol");
  EXPECT_EQ(columns[1].name, "eventTime");
}

TEST(ReflectedRowUnit, SourceProjectionsBorrowPayloadsWithoutAllocating) {
  TradeTick source{std::string(80, 's'), 7, 1.5,
                   std::vector<std::byte>(80, std::byte{0x7f}), "{}", 123};
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  const BorrowedRow<TradeTick> borrowed{source};
  const RowView<TradeTick> view{source};
  const bool same_text = borrowed.Get<0>().data() == source.instrument_name.data() &&
                         view.Get<0>().data() == source.instrument_name.data();
  const bool same_bytes = borrowed.Get<3>()->data() == source.payload->data() &&
                          view.Get<3>()->data() == source.payload->data();
  allocation_probe::enabled = false;
  EXPECT_EQ(allocation_probe::count, 0U);
  EXPECT_TRUE(same_text);
  EXPECT_TRUE(same_bytes);
  EXPECT_EQ(borrowed.Get<1>(), 7);
}

TEST(ReflectedRowUnitScenario, BorrowedRowsProduceDenseValuesAndLittleEndianNullMasks) {
  TypedColumnarBatch<TradeTick> batch{16};
  const std::array payload{std::byte{0}, std::byte{0xff}};
  for (std::int64_t i = 0; i < 16; ++i) {
    const bool missing = i == 0 || i == 7 || i == 8 || i == 15;
    const BorrowedRow<TradeTick> row{
        std::string_view{"BTC-USDT"}, i,
        missing ? std::nullopt : std::optional<double>{12.5},
        i % 2 == 0 ? std::optional<BinaryView>{payload} : std::nullopt,
        std::string_view{"{\"x\":1}"}, static_cast<__int128>(-123)};
    ASSERT_TRUE(batch.PushRef(row));
  }
  ASSERT_EQ(batch.RowCount(), 16U);
  EXPECT_FALSE(batch.GetRow(16));
  auto first = batch.GetRow(0);
  ASSERT_TRUE(first);
  EXPECT_EQ(first->Get<0>(), "BTC-USDT");
  EXPECT_EQ(first->Get<"symbol">(), "BTC-USDT");
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
  EXPECT_EQ(encoded->table_name(), "ticks");
  EXPECT_EQ(encoded->row_count(), 16U);
  ASSERT_EQ(encoded->columns_size(), 6);
  EXPECT_EQ(encoded->columns(0).column_name(), "symbol");
  EXPECT_EQ(encoded->columns(1).column_name(), "eventTime");
  EXPECT_EQ(encoded->columns(2).null_mask(), std::string(2, '\x81'));
  ASSERT_EQ(encoded->columns(2).values().f64_values_size(), 12);
  for (auto price : encoded->columns(2).values().f64_values()) EXPECT_EQ(price, 12.5);
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
  EXPECT_EQ(deleted->table_name(), "ticks");
  ASSERT_EQ(deleted->key_columns_size(), 2);
  EXPECT_EQ(deleted->key_columns(0).column_name(), "symbol");
  EXPECT_EQ(deleted->key_columns(1).column_name(), "eventTime");
  EXPECT_EQ(deleted->row_count(), 16U);
}

TEST(ReflectedRowUnit, ScalarAndTemporalTypesPreserveTheirWireValues) {
  constexpr auto columns = row_detail::RowSchema<ScalarRow>::kColumns;
  constexpr std::array expected{
      ColumnDataType::kTimestampMicrosecond, ColumnDataType::kBoolean,
      ColumnDataType::kInt8, ColumnDataType::kInt16, ColumnDataType::kInt32,
      ColumnDataType::kInt64, ColumnDataType::kUint8, ColumnDataType::kUint16,
      ColumnDataType::kUint32, ColumnDataType::kUint64, ColumnDataType::kFloat32,
      ColumnDataType::kFloat64, ColumnDataType::kDate, ColumnDataType::kDateTime,
      ColumnDataType::kTimestampSecond, ColumnDataType::kTimestampMillisecond,
      ColumnDataType::kTimestampMicrosecond, ColumnDataType::kTimestampNanosecond};
  for (std::size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(columns[i].data_type, expected[i]);
    EXPECT_FALSE(columns[i].nullable);
    EXPECT_EQ(columns[i].semantic_type, i == 0 ? SemanticType::kTimestamp
                                             : SemanticType::kField);
  }
  const ScalarRow source{
      -5, true, -8, -16, -32, -64, 255, 65535,
      std::numeric_limits<std::uint32_t>::max(),
      std::numeric_limits<std::uint64_t>::max(), 1.25F, -2.5, Date{-1}, DateTime{-2},
      TimestampSecond{-3}, TimestampMillisecond{-4}, TimestampMicrosecond{-5},
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
  EXPECT_EQ(encoded->columns(8).values().u32_values(0),
            std::numeric_limits<std::uint32_t>::max());
  EXPECT_EQ(encoded->columns(9).values().u64_values(0),
            std::numeric_limits<std::uint64_t>::max());
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
  [[= timestamp{}]] TimestampNanosecond time;
  std::string_view text;
  BinaryView bytes;
  [[= field{.json = true}]] std::string_view document;
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
  ASSERT_TRUE(batch.PushRef(BorrowedRow<BorrowedDocument>{
      TimestampNanosecond{10}, "replacement", std::span<const std::byte>{bytes}, "[]"}));
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
  [[= timestamp{}]] TimestampSecond time;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] std::optional<Decimal128> value;
};

TEST(ReflectedRowUnitScenario, DecimalMetadataFailureDoesNotPartiallyAppendColumns) {
  TypedColumnarBatch<DecimalRow> batch;
  ASSERT_TRUE(batch.Push(DecimalRow{TimestampSecond{1}, Decimal128{123, 5, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{123, 6, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{100000, 5, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{TimestampSecond{2}, Decimal128{-100000, 5, 2}}));
  EXPECT_FALSE(batch.Push(DecimalRow{
      TimestampSecond{2}, Decimal128{std::numeric_limits<__int128>::min(), 5, 2}}));
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
  [[= timestamp{}]] std::int64_t time;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] __int128 value;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] std::optional<__int128> nullable;
};

TEST(ReflectedRowUnitScenario, RawDecimalOverflowPreservesAllColumnsAtomically) {
  TypedColumnarBatch<RawDecimalRow> batch;
  ASSERT_TRUE(batch.Push(RawDecimalRow{1, 99999, -99999}));
  for (auto invalid : std::array<__int128, 3>{100000, -100000,
                                            std::numeric_limits<__int128>::min()}) {
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
  TypedColumnarBatch<TradeTick> batch{32, BufferPolicy::Unbounded()};
  const std::array bytes{std::byte{0}, std::byte{0xff}, std::byte{7}};
  const BorrowedRow<TradeTick> row{
      std::string_view{"a symbol longer than the small-string buffer"}, std::int64_t{1},
      std::optional<double>{1.5}, std::optional<BinaryView>{bytes},
      std::string_view{"{\"description\":\"larger than small-string storage\"}"},
      static_cast<__int128>(123)};
  for (std::size_t i = 0; i < 32; ++i) ASSERT_TRUE(batch.PushRef(row));
  batch.ClearUnbounded();
  const auto allocated = batch.AllocatedBytes();
  bool succeeded = true;
  std::int64_t sum = 0;
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  for (std::size_t i = 0; i < 32; ++i) succeeded &= batch.PushRef(row).has_value();
  for (std::size_t i = 0; i < 32; ++i) {
    auto view = batch.GetRow(i);
    succeeded &= view.has_value();
    if (view) sum += view->Get<1>();
  }
  std::int64_t range_sum = 0;
  for (const auto &view : batch.Views()) range_sum += view.Get<1>();
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
  const BorrowedRow<BorrowedDocument> row{
      TimestampNanosecond{1}, "a document text longer than a small string", BinaryView{bytes},
      "{\"message\":\"a JSON document longer than a small string\"}"};
  bool succeeded = true;
  allocation_probe::count = 0;
  allocation_probe::enabled = true;
  for (std::size_t i = 0; i < 4; ++i) succeeded &= batch.PushRef(row).has_value();
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
  const BorrowedRow<BorrowedDocument> alias{
      TimestampNanosecond{2}, view->Get<1>(), view->Get<2>(), view->Get<3>()};
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
  [[= timestamp{}]] TimestampMillisecond time;
  std::optional<std::string> text;
  std::optional<bool> flag;
  [[= field{.json = true}]] std::optional<std::string> json;
};

TEST(ReflectedRowUnitScenario, NullableTextDistinguishesEmptyValuesFromNull) {
  TypedColumnarBatch<NullableRow> batch;
  ASSERT_TRUE(batch.Push(NullableRow{TimestampMillisecond{1}, "", false, "{}"}));
  ASSERT_TRUE(batch.Push(NullableRow{TimestampMillisecond{2}, std::nullopt,
                                    std::nullopt, std::nullopt}));
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
  TypedColumnarBatch<TradeTick> batch{1024, BufferPolicy{2, 512}};
  TradeTick row{std::string(1000, 'x'), 1, std::nullopt,
                std::vector<std::byte>(1000, std::byte{7}), "{}", 0};
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
  EXPECT_EQ(view->Get<0>(), row.instrument_name);
  ASSERT_TRUE(view->Get<3>());
  EXPECT_TRUE(std::ranges::equal(*view->Get<3>(), *row.payload));
  batch.ShrinkToFit();
  EXPECT_EQ(batch.RowCount(), 1U);
  auto retained = batch.GetRow(0);
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->Get<0>(), row.instrument_name);
}

struct UnsignedBytes {
  [[= timestamp{}]] std::int64_t time;
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
  [[= timestamp{}]] std::int64_t time;
  [[= tag{}]] std::string symbol;
  std::optional<std::vector<std::byte>> bytes;
  std::optional<std::int8_t> small;
  [[= field{.decimal_precision = 5, .decimal_scale = 2}]] std::optional<Decimal128> amount;
  [[= field{.json = true}]] std::string json;
};

template <class Columns>
concept DecodableColumns = requires(Columns &&columns) {
  ViewDecodedRow<WireRow>(std::forward<Columns>(columns), 0, 1);
};
static_assert(!DecodableColumns<std::vector<greptime::v1::Column>>);
static_assert(DecodableColumns<std::vector<greptime::v1::Column> &>);
static_assert(DecodableColumns<std::span<const greptime::v1::Column>>);

TEST(ReflectedRowUnitScenario, DecodedWireProjectionBorrowsPayloadsAndRejectsCorruption) {
  TypedColumnarBatch<WireRow> batch;
  ASSERT_TRUE(batch.Push(WireRow{7, std::string(80, 's'),
      std::vector<std::byte>{std::byte{0}, std::byte{0xff}}, -8,
      Decimal128{-123, 5, 2}, "{\"event\":1}"}));
  ASSERT_TRUE(batch.Push(WireRow{8, "other", std::nullopt, std::nullopt,
                                std::nullopt, "[]"}));
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
  EXPECT_EQ(first->Get<2>()->data(), reinterpret_cast<const std::byte *>(
      decoded.columns(2).values().binary_values(0).data()));
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
