#pragma once

#include <greptimedb/core/types.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <meta>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace greptimedb {

enum class RenameRule {
  kIdentity,
  kLowerCase,
  kUpperCase,
  kSnakeCase,
  kCamelCase,
  kPascalCase,
  kScreamingSnakeCase,
  kKebabCase,
  kNone = kIdentity,
  kLowercase = kLowerCase,
  kUppercase = kUpperCase
};

/** Structural text for annotation values; names are limited to 511 bytes. */
struct AnnotationText {
  std::array<char, 512> text{};
  std::size_t length{};
  bool specified{};

  constexpr AnnotationText() = default;
  template <std::size_t N>
  consteval AnnotationText(const char (&source)[N])
      : length(N - 1), specified(true) {
    static_assert(N <= 512, "GreptimeDB annotation name exceeds 511 bytes");
    for (std::size_t i = 0; i < N; ++i)
      text[i] = source[i];
  }
  constexpr std::string_view View() const noexcept {
    return length < text.size() ? std::string_view{text.data(), length}
                                : std::string_view{};
  }
};

struct table {
  AnnotationText name{};
  RenameRule rename_all{RenameRule::kNone};
};
struct tag {
  AnnotationText column{};
};
/** An omitted unit preserves a timestamp wrapper's intrinsic unit. */
struct AnnotationTimestampUnit {
  TimestampUnit value{TimestampUnit::kNanosecond};
  bool specified{};
  constexpr AnnotationTimestampUnit() = default;
  consteval AnnotationTimestampUnit(TimestampUnit unit)
      : value(unit), specified(true) {}
  constexpr operator TimestampUnit() const noexcept { return value; }
};
struct timestamp {
  AnnotationText column{};
  AnnotationTimestampUnit unit{};
};
struct field {
  AnnotationText column{};
  std::uint32_t decimal_precision{38};
  std::uint32_t decimal_scale{0};
  bool json{false};
};

/** Logical column metadata in source member declaration order. */
struct ColumnMeta {
  std::string_view name;
  SemanticType semantic_type;
  ColumnDataType data_type;
  bool nullable;
};

namespace row_detail {

template <class T> struct OptionalTraits {
  using Type = T;
  static constexpr bool kOptional = false;
};
template <class T> struct OptionalTraits<std::optional<T>> {
  using Type = T;
  static constexpr bool kOptional = true;
};

constexpr bool Lower(char c) noexcept { return c >= 'a' && c <= 'z'; }
constexpr bool Upper(char c) noexcept { return c >= 'A' && c <= 'Z'; }
constexpr bool Digit(char c) noexcept { return c >= '0' && c <= '9'; }
constexpr char ToLower(char c) noexcept { return Upper(c) ? c - 'A' + 'a' : c; }
constexpr char ToUpper(char c) noexcept { return Lower(c) ? c - 'a' + 'A' : c; }
constexpr bool Space(char c) noexcept {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' ||
         c == '\v';
}
constexpr bool ValidName(std::string_view name) noexcept {
  bool nonspace = false;
  for (char c : name) {
    if (c == '\0')
      return false;
    if (!Space(c))
      nonspace = true;
  }
  return nonspace;
}

consteval AnnotationText Rename(std::string_view source, RenameRule rule) {
  AnnotationText result;
  result.specified = true;
  auto append = [&](char c) {
    if (result.length < result.text.size() - 1)
      result.text[result.length++] = c;
    else
      result.length = result.text.size();
  };
  if (source.size() >= result.text.size()) {
    result.length = result.text.size();
    return result;
  }
  if (rule == RenameRule::kNone || rule == RenameRule::kLowercase ||
      rule == RenameRule::kUppercase) {
    for (char c : source)
      append(rule == RenameRule::kLowercase   ? ToLower(c)
             : rule == RenameRule::kUppercase ? ToUpper(c)
                                              : c);
    return result;
  }
  bool in_word = false;
  bool first_word = true;
  char previous = '\0';
  for (std::size_t i = 0; i < source.size(); ++i) {
    const char current = source[i];
    if (current == '_' || current == '-' || Space(current)) {
      in_word = false;
      previous = '\0';
      continue;
    }
    const bool next_lower = i + 1 < source.size() && Lower(source[i + 1]);
    const bool boundary =
        in_word && Upper(current) &&
        (Lower(previous) || Digit(previous) || (Upper(previous) && next_lower));
    if (!in_word || boundary) {
      if (!first_word && (rule == RenameRule::kSnakeCase ||
                          rule == RenameRule::kScreamingSnakeCase))
        append('_');
      if (!first_word && rule == RenameRule::kKebabCase)
        append('-');
      const bool capital = rule == RenameRule::kPascalCase ||
                           (rule == RenameRule::kCamelCase && !first_word) ||
                           rule == RenameRule::kScreamingSnakeCase;
      append(capital ? ToUpper(current) : ToLower(current));
      first_word = false;
      in_word = true;
    } else {
      append(rule == RenameRule::kScreamingSnakeCase ? ToUpper(current)
                                                     : ToLower(current));
    }
    previous = current;
  }
  return result;
}

template <AnnotationText Text> consteval std::string_view StaticName() {
  if constexpr (Text.length < Text.text.size())
    return {std::define_static_array(Text.text).data(), Text.length};
  else
    return {};
}

enum class SchemaError {
  kNone,
  kInvalidRow,
  kInvalidName,
  kDuplicateName,
  kConflictingAnnotation,
  kWrongAnnotationScope,
  kUnsupportedType,
  kInvalidTimestamp,
  kTimestampCount,
  kInvalidDecimal,
  kInvalidJson
};

struct TableOptions {
  table value{};
  SchemaError error{};
};
template <class T> consteval TableOptions TableAnnotation() {
  TableOptions result;
  std::size_t count = 0;
  template for (constexpr auto annotation :
                std::define_static_array(std::meta::annotations_of(^^T))) {
    constexpr auto type = std::meta::remove_cv(std::meta::type_of(annotation));
    if constexpr (type == ^^table) {
      result.value = std::meta::extract<table>(annotation);
      if (++count > 1)
        result.error = SchemaError::kConflictingAnnotation;
    } else if constexpr (type == ^^tag || type == ^^timestamp ||
                         type == ^^field) {
      result.error = SchemaError::kWrongAnnotationScope;
    }
  }
  if (result.value.rename_all != RenameRule::kIdentity &&
      result.value.rename_all != RenameRule::kLowerCase &&
      result.value.rename_all != RenameRule::kUpperCase &&
      result.value.rename_all != RenameRule::kSnakeCase &&
      result.value.rename_all != RenameRule::kCamelCase &&
      result.value.rename_all != RenameRule::kPascalCase &&
      result.value.rename_all != RenameRule::kScreamingSnakeCase &&
      result.value.rename_all != RenameRule::kKebabCase)
    result.error = SchemaError::kConflictingAnnotation;
  return result;
}

struct MemberOptions {
  AnnotationText column{};
  SemanticType semantic{SemanticType::kField};
  TimestampUnit unit{TimestampUnit::kNanosecond};
  bool unit_specified = false;
  field field_options{};
  bool explicit_field = false;
  SchemaError error{};
};
template <std::meta::info Member> consteval MemberOptions MemberAnnotation() {
  MemberOptions result;
  std::size_t count = 0;
  template for (constexpr auto annotation :
                std::define_static_array(std::meta::annotations_of(Member))) {
    constexpr auto type = std::meta::remove_cv(std::meta::type_of(annotation));
    if constexpr (type == ^^tag) {
      result.column = std::meta::extract<tag>(annotation).column;
      result.semantic = SemanticType::kTag;
      ++count;
    } else if constexpr (type == ^^timestamp) {
      constexpr auto value = std::meta::extract<timestamp>(annotation);
      result.column = value.column;
      result.unit = value.unit.value;
      result.unit_specified = value.unit.specified;
      result.semantic = SemanticType::kTimestamp;
      ++count;
    } else if constexpr (type == ^^field) {
      result.field_options = std::meta::extract<field>(annotation);
      result.column = result.field_options.column;
      result.explicit_field = true;
      ++count;
    } else if constexpr (type == ^^table) {
      result.error = SchemaError::kWrongAnnotationScope;
    }
  }
  if (count > 1)
    result.error = SchemaError::kConflictingAnnotation;
  return result;
}

template <class T> consteval std::optional<ColumnDataType> ScalarDataType() {
  if constexpr (std::same_as<T, bool>)
    return ColumnDataType::kBoolean;
  else if constexpr (std::same_as<T, std::int8_t>)
    return ColumnDataType::kInt8;
  else if constexpr (std::same_as<T, std::int16_t>)
    return ColumnDataType::kInt16;
  else if constexpr (std::same_as<T, std::int32_t>)
    return ColumnDataType::kInt32;
  else if constexpr (std::same_as<T, std::int64_t>)
    return ColumnDataType::kInt64;
  else if constexpr (std::same_as<T, std::uint8_t>)
    return ColumnDataType::kUint8;
  else if constexpr (std::same_as<T, std::uint16_t>)
    return ColumnDataType::kUint16;
  else if constexpr (std::same_as<T, std::uint32_t>)
    return ColumnDataType::kUint32;
  else if constexpr (std::same_as<T, std::uint64_t>)
    return ColumnDataType::kUint64;
  else if constexpr (std::same_as<T, float>)
    return ColumnDataType::kFloat32;
  else if constexpr (std::same_as<T, double>)
    return ColumnDataType::kFloat64;
  else if constexpr (std::same_as<T, std::string> ||
                     std::same_as<T, std::string_view>)
    return ColumnDataType::kString;
  else if constexpr (std::same_as<T, std::vector<std::byte>> ||
                     std::same_as<T, BinaryView> ||
                     std::same_as<T, std::vector<std::uint8_t>> ||
                     std::same_as<T, std::span<const std::uint8_t>>)
    return ColumnDataType::kBinary;
  else if constexpr (std::same_as<T, Date>)
    return ColumnDataType::kDate;
  else if constexpr (std::same_as<T, DateTime>)
    return ColumnDataType::kDateTime;
  else if constexpr (std::same_as<T, TimestampSecond>)
    return ColumnDataType::kTimestampSecond;
  else if constexpr (std::same_as<T, TimestampMillisecond>)
    return ColumnDataType::kTimestampMillisecond;
  else if constexpr (std::same_as<T, TimestampMicrosecond>)
    return ColumnDataType::kTimestampMicrosecond;
  else if constexpr (std::same_as<T, TimestampNanosecond>)
    return ColumnDataType::kTimestampNanosecond;
  else if constexpr (std::same_as<T, Decimal128> || std::same_as<T, __int128>)
    return ColumnDataType::Decimal(38, 0);
  else if constexpr (std::same_as<T, JsonView>)
    return ColumnDataType::kJson;
  else
    return std::nullopt;
}

constexpr bool IsTimestamp(ColumnDataType type) noexcept {
  return type == ColumnDataType::kTimestampSecond ||
         type == ColumnDataType::kTimestampMillisecond ||
         type == ColumnDataType::kTimestampMicrosecond ||
         type == ColumnDataType::kTimestampNanosecond;
}
constexpr bool ValidUnit(TimestampUnit unit) noexcept {
  return unit == TimestampUnit::kSecond ||
         unit == TimestampUnit::kMillisecond ||
         unit == TimestampUnit::kMicrosecond ||
         unit == TimestampUnit::kNanosecond;
}

struct MemberSchema {
  ColumnMeta column{};
  SchemaError error{};
};
template <std::meta::info Member, RenameRule Rule>
consteval MemberSchema ExtractMember() {
  using MemberType = [:std::meta::type_of(Member):];
  using Base = typename OptionalTraits<MemberType>::Type;
  constexpr auto options = MemberAnnotation<Member>();
  constexpr auto name = options.column.specified
                            ? options.column
                            : Rename(std::meta::has_identifier(Member)
                                         ? std::meta::identifier_of(Member)
                                         : std::string_view{},
                                     Rule);
  MemberSchema result{{StaticName<name>(),
                       options.semantic,
                       {},
                       OptionalTraits<MemberType>::kOptional},
                      options.error};
  if (!std::meta::is_public(Member) || std::meta::is_bit_field(Member) ||
      std::is_const_v<MemberType> || std::is_volatile_v<MemberType>)
    result.error = SchemaError::kUnsupportedType;
  if (!ValidName(result.column.name))
    result.error = SchemaError::kInvalidName;
  constexpr auto datatype = ScalarDataType<Base>();
  if constexpr (!datatype.has_value()) {
    result.error = SchemaError::kUnsupportedType;
    return result;
  } else {
    result.column.data_type = *datatype;
    if (options.semantic == SemanticType::kTimestamp) {
      if (result.column.nullable ||
          (!IsTimestamp(*datatype) && *datatype != ColumnDataType::kInt64) ||
          !ValidUnit(options.unit))
        result.error = SchemaError::kInvalidTimestamp;
      else if (IsTimestamp(*datatype)) {
        if (options.unit_specified &&
            ToColumnDataType(options.unit) != *datatype)
          result.error = SchemaError::kInvalidTimestamp;
      } else
        result.column.data_type = ToColumnDataType(options.unit);
    }
    if (options.field_options.json) {
      if (*datatype != ColumnDataType::kString)
        result.error = SchemaError::kInvalidJson;
      else
        result.column.data_type = ColumnDataType::kJson;
    }
    if (*datatype == ColumnDataType::Decimal(38, 0)) {
      const auto precision = options.field_options.decimal_precision;
      const auto scale = options.field_options.decimal_scale;
      if (!options.explicit_field || precision == 0 || precision > 38 ||
          scale > precision)
        result.error = SchemaError::kInvalidDecimal;
      result.column.data_type = ColumnDataType::Decimal(precision, scale);
    } else if (options.field_options.decimal_precision != 38 ||
               options.field_options.decimal_scale != 0) {
      result.error = SchemaError::kInvalidDecimal;
    }
    return result;
  }
}

template <class T> consteval bool ValidRowShape() {
  if constexpr (!std::is_class_v<T> || !std::same_as<T, std::remove_cvref_t<T>>)
    return false;
  else if constexpr (!std::meta::is_complete_type(^^T))
    return false;
  else
    return std::is_aggregate_v<T> &&
           std::meta::bases_of(^^T, std::meta::access_context::unchecked())
               .empty();
}

template <class T> consteval auto Members() {
  if constexpr (ValidRowShape<T>())
    return std::define_static_array(std::meta::nonstatic_data_members_of(
        ^^T, std::meta::access_context::unchecked()));
  else
    return std::define_static_array(std::array<std::meta::info, 0>{});
}

template <class T> consteval AnnotationText DefaultTableName() {
  if constexpr (std::meta::has_identifier(^^T))
    return Rename(std::meta::identifier_of(^^T), RenameRule::kSnakeCase);
  else if constexpr (std::meta::has_template_arguments(^^T))
    return Rename(std::meta::identifier_of(std::meta::template_of(^^T)),
                  RenameRule::kSnakeCase);
  else
    return {};
}

template <std::size_t N> struct SchemaResult {
  std::array<ColumnMeta, N> columns{};
  std::string_view table_name;
  SchemaError error{};
};

template <class T> consteval auto ExtractSchema() {
  static constexpr auto members = Members<T>();
  SchemaResult<members.size()> result;
  if constexpr (!ValidRowShape<T>() || members.empty()) {
    result.error = SchemaError::kInvalidRow;
    return result;
  } else {
    constexpr auto options = TableAnnotation<T>();
    constexpr auto name = options.value.name.specified ? options.value.name
                                                       : DefaultTableName<T>();
    result.table_name = StaticName<name>();
    result.error = options.error;
    if (static_cast<int>(options.value.rename_all) < 0 ||
        static_cast<int>(options.value.rename_all) >
            static_cast<int>(RenameRule::kKebabCase))
      result.error = SchemaError::kInvalidName;
    if (!ValidName(result.table_name))
      result.error = SchemaError::kInvalidName;
    std::size_t index = 0;
    std::size_t timestamps = 0;
    template for (constexpr auto member : members) {
      constexpr auto schema = ExtractMember<member, options.value.rename_all>();
      result.columns[index++] = schema.column;
      if (schema.error != SchemaError::kNone)
        result.error = schema.error;
      if (schema.column.semantic_type == SemanticType::kTimestamp)
        ++timestamps;
    }
    if (timestamps != 1)
      result.error = SchemaError::kTimestampCount;
    for (std::size_t i = 0; i < result.columns.size(); ++i)
      for (std::size_t j = 0; j < i; ++j)
        if (result.columns[i].name == result.columns[j].name)
          result.error = SchemaError::kDuplicateName;
    return result;
  }
}

template <class T> inline constexpr auto kSchema = ExtractSchema<T>();

template <class T> struct RowSchema {
  static_assert(kSchema<T>.error == SchemaError::kNone,
                "invalid GreptimeDB row schema: require unique names, "
                "supported field types, "
                "and exactly one nonoptional timestamp");
  static constexpr auto kMembers = Members<T>();
  static constexpr auto kColumns = kSchema<T>.columns;
  static constexpr auto kTableName = kSchema<T>.table_name;
};

} // namespace row_detail

template <class T>
concept RowConcept =
    row_detail::kSchema<T>.error == row_detail::SchemaError::kNone;

} // namespace greptimedb
