#pragma once

#include <greptimedb/row/reflected_row.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace reflected_row_test {
using namespace greptimedb;

// clang-format off
struct MetricRow; struct [[= table{.name = "samples", .rename_all = RenameRule::kCamelCase}]] MetricRow {
  [[= tag{.column = "host"}]] std::string host_name;
  [[= timestamp{}]] std::int64_t event_time;
  std::optional<double> value;
  std::optional<std::vector<std::byte>> payload;
  [[= field{.json = true}]] std::string document;
  [[= field{.decimal_precision = 38, .decimal_scale = 10}]] __int128 coefficient;
  // clang-format on
};
static_assert(BatchEncodeConcept<TypedColumnarBatch<MetricRow>>);

template <RenameRule Rule> struct Naming;
template <RenameRule Rule>
// clang-format off
struct [[= table{.rename_all = Rule}]] Naming {
  [[= timestamp{}]] std::int64_t event_time;
  std::int32_t HTTPServerID;
  [[= field{.column = "Exact Name"}]] double custom_name;
  // clang-format on
};

template <RenameRule Rule> consteval bool Names(std::string_view time, std::string_view acronym) {
  constexpr auto columns = row_detail::RowSchema<Naming<Rule>>::kColumns;
  return columns[0].name == time && columns[1].name == acronym && columns[2].name == "Exact Name";
}
static_assert(Names<RenameRule::kNone>("event_time", "HTTPServerID"));
static_assert(Names<RenameRule::kLowercase>("event_time", "httpserverid"));
static_assert(Names<RenameRule::kUppercase>("EVENT_TIME", "HTTPSERVERID"));
static_assert(Names<RenameRule::kSnakeCase>("event_time", "http_server_id"));
static_assert(Names<RenameRule::kCamelCase>("eventTime", "httpServerId"));
static_assert(Names<RenameRule::kPascalCase>("EventTime", "HttpServerId"));
static_assert(Names<RenameRule::kScreamingSnakeCase>("EVENT_TIME", "HTTP_SERVER_ID"));
static_assert(Names<RenameRule::kKebabCase>("event-time", "http-server-id"));

struct HTTPMetricRow {
  // clang-format off
  [[= timestamp{}]] TimestampMillisecond time;
  // clang-format on
};
static_assert(row_detail::RowSchema<HTTPMetricRow>::kTableName == "http_metric_row");
static_assert(RowConcept<MetricRow>);
static_assert(BatchEncodeConcept<TypedColumnarBatch<MetricRow>>);
static_assert(MemoryBufferConcept<TypedColumnarBatch<MetricRow>>);
static_assert(std::same_as<std::remove_cvref_t<decltype(std::declval<const BorrowedRow<MetricRow> &>().Get<0>())>,
                           std::string_view>);
static_assert(std::same_as<std::remove_cvref_t<decltype(std::declval<const BorrowedRow<MetricRow> &>().Get<3>())>,
                           std::optional<BinaryView>>);
static_assert(!std::constructible_from<BorrowedRow<MetricRow>, MetricRow &&>);
static_assert(!std::constructible_from<RowView<MetricRow>, MetricRow &&>);
static_assert(!std::is_move_constructible_v<TypedColumnarBatch<MetricRow>>);

struct MissingTime {
  std::int64_t ts;
};
struct TwiceTime {
  // clang-format off
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
struct DuplicateAfterRename; struct [[= table{.rename_all = RenameRule::kCamelCase}]] DuplicateAfterRename {
  [[= timestamp{}]] std::int64_t event_time;
  double eventTime;
};
struct BlankTable; struct [[= table{.name = " \t\n"}]] BlankTable { [[= timestamp{}]] std::int64_t time; };
struct NulTable; struct [[= table{.name = "bad\0name"}]] NulTable {
  [[= timestamp{}]] std::int64_t time;
};
struct BlankColumn { [[= timestamp{.column = " \t"}]] std::int64_t time; };
struct EmptyTable; struct [[= table{.name = ""}]] EmptyTable { [[= timestamp{}]] std::int64_t time; };
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
struct WrongTableScope; struct [[= tag{}]] WrongTableScope { [[= timestamp{}]] std::int64_t time; };
struct WrongMemberScope { [[= timestamp{}, = table{}]] std::int64_t time; };
struct DuplicateTable; struct [[= table{}, = table{}]] DuplicateTable { [[= timestamp{}]] std::int64_t time; };
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
  // clang-format on
  union {
    double floating;
    std::int64_t integer;
  };
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
static_assert(!RowConcept<const MetricRow>);

struct ScalarRow {
  // clang-format off
  [[= timestamp{.unit = TimestampUnit::kMicrosecond}]] std::int64_t time;
  // clang-format on
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
} // namespace reflected_row_test
