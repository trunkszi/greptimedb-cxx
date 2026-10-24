#include <greptimedb/batch/row_batch.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>
#include <variant>

namespace greptimedb {
namespace {

using Kind = ColumnDataType::Kind;

std::unexpected<Error> SchemaFailure(std::string message) {
  return std::unexpected(
      Error(ErrorKind::kSchemaViolation, std::move(message)));
}

bool IsBlank(std::string_view text) noexcept {
  for (char ch : text)
    if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\v' && ch != '\f' &&
        ch != '\r')
      return false;
  return true;
}

bool ValidDecimalType(const ColumnDataType &type) noexcept {
  if (type.kind != Kind::kDecimal128)
    return true;
  return type.precision >= 1 && type.precision <= 38 &&
         type.scale <= type.precision;
}

bool IsTemporal(Kind kind) noexcept {
  return kind == Kind::kTimestampSecond ||
         kind == Kind::kTimestampMillisecond ||
         kind == Kind::kTimestampMicrosecond ||
         kind == Kind::kTimestampNanosecond;
}

bool MatchesKind(Kind kind, const ValueView &value) noexcept {
  switch (kind) {
  case Kind::kBoolean:
    return std::holds_alternative<bool>(value);
  case Kind::kInt8:
    return std::holds_alternative<std::int8_t>(value);
  case Kind::kInt16:
    return std::holds_alternative<std::int16_t>(value);
  case Kind::kInt32:
    return std::holds_alternative<std::int32_t>(value);
  case Kind::kInt64:
    return std::holds_alternative<std::int64_t>(value);
  case Kind::kUint8:
    return std::holds_alternative<std::uint8_t>(value);
  case Kind::kUint16:
    return std::holds_alternative<std::uint16_t>(value);
  case Kind::kUint32:
    return std::holds_alternative<std::uint32_t>(value);
  case Kind::kUint64:
    return std::holds_alternative<std::uint64_t>(value);
  case Kind::kFloat32:
    return std::holds_alternative<float>(value);
  case Kind::kFloat64:
    return std::holds_alternative<double>(value);
  case Kind::kBinary:
    return std::holds_alternative<BinaryView>(value);
  case Kind::kString:
    return std::holds_alternative<std::string_view>(value);
  case Kind::kDate:
    return std::holds_alternative<Date>(value);
  case Kind::kDateTime:
    return std::holds_alternative<DateTime>(value);
  case Kind::kTimestampSecond:
    return std::holds_alternative<TimestampSecond>(value);
  case Kind::kTimestampMillisecond:
    return std::holds_alternative<TimestampMillisecond>(value);
  case Kind::kTimestampMicrosecond:
    return std::holds_alternative<TimestampMicrosecond>(value);
  case Kind::kTimestampNanosecond:
    return std::holds_alternative<TimestampNanosecond>(value);
  case Kind::kDecimal128:
    return std::holds_alternative<Decimal128>(value);
  case Kind::kJson:
    return std::holds_alternative<JsonView>(value);
  }
  return false;
}

bool DecimalFits(const Decimal128 &value) noexcept {
  if (value.precision < 1 || value.precision > 38 ||
      value.scale > value.precision)
    return false;
  unsigned __int128 magnitude = 0;
  if (value.value < 0) {
    // Negating INT128_MIN directly would overflow; shift it into range first.
    magnitude = static_cast<unsigned __int128>(-(value.value + 1));
    magnitude += 1;
  } else {
    magnitude = static_cast<unsigned __int128>(value.value);
  }
  unsigned __int128 limit = 1;
  for (std::uint32_t i = 0; i < value.precision; ++i)
    limit *= 10;
  return magnitude < limit;
}

int ClampToInt(std::size_t value) noexcept {
  constexpr auto limit =
      static_cast<std::size_t>(std::numeric_limits<int>::max());
  return static_cast<int>(std::min(value, limit));
}

void FillProtoSchema(::greptime::v1::ColumnSchema *schema,
                     std::string_view name, SemanticType semantic_type,
                     const ColumnDataType &data_type) {
  schema->set_column_name(name.empty() ? "" : name.data(), name.size());
  schema->set_semantic_type(ToProto(semantic_type));
  schema->set_datatype(data_type.ToProtoDatatype());
  if (auto extension = data_type.ToProtoExtension())
    *schema->mutable_datatype_extension() = std::move(*extension);
}

/** Overwrites a reusable slot so no earlier oneof alternative survives, while
 * letting string and binary payloads keep their allocation when the same
 * alternative is written again. */
void AssignString(::greptime::v1::Value *slot, std::string_view text) {
  auto *target = slot->mutable_string_value();
  if (text.empty())
    target->clear();
  else
    target->assign(text.data(), text.size());
}

void AssignBinary(::greptime::v1::Value *slot, BinaryView bytes) {
  auto *target = slot->mutable_binary_value();
  if (bytes.empty()) {
    target->clear();
    return;
  }
  target->assign(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void AssignBinaryText(::greptime::v1::Value *slot, std::string_view text) {
  auto *target = slot->mutable_binary_value();
  if (text.empty()) {
    target->clear();
    return;
  }
  target->assign(text.data(), text.size());
}

void AssignValue(::greptime::v1::Value *slot, const ValueView &value) {
  std::visit(
      [slot](const auto &scalar) {
        using T = std::remove_cvref_t<decltype(scalar)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          slot->Clear();
        } else if constexpr (std::is_same_v<T, bool>) {
          slot->set_bool_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int8_t>) {
          slot->set_i8_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int16_t>) {
          slot->set_i16_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int32_t>) {
          slot->set_i32_value(scalar);
        } else if constexpr (std::is_same_v<T, std::int64_t>) {
          slot->set_i64_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint8_t>) {
          slot->set_u8_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint16_t>) {
          slot->set_u16_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint32_t>) {
          slot->set_u32_value(scalar);
        } else if constexpr (std::is_same_v<T, std::uint64_t>) {
          slot->set_u64_value(scalar);
        } else if constexpr (std::is_same_v<T, float>) {
          slot->set_f32_value(scalar);
        } else if constexpr (std::is_same_v<T, double>) {
          slot->set_f64_value(scalar);
        } else if constexpr (std::is_same_v<T, BinaryView>) {
          AssignBinary(slot, scalar);
        } else if constexpr (std::is_same_v<T, std::string_view>) {
          AssignString(slot, scalar);
        } else if constexpr (std::is_same_v<T, JsonView>) {
          AssignBinaryText(slot, scalar.value);
        } else if constexpr (std::is_same_v<T, Date>) {
          slot->set_date_value(scalar.value);
        } else if constexpr (std::is_same_v<T, DateTime>) {
          slot->set_datetime_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampSecond>) {
          slot->set_timestamp_second_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampMillisecond>) {
          slot->set_timestamp_millisecond_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampMicrosecond>) {
          slot->set_timestamp_microsecond_value(scalar.value);
        } else if constexpr (std::is_same_v<T, TimestampNanosecond>) {
          slot->set_timestamp_nanosecond_value(scalar.value);
        } else if constexpr (std::is_same_v<T, Decimal128>) {
          *slot->mutable_decimal128_value() = scalar.ToProto();
        }
      },
      value);
}

} // namespace

RowBatch::RowBatch(std::string_view table_name)
    : RowBatch(table_name, 0, BufferPolicy{}) {}

RowBatch::RowBatch(std::string_view table_name, std::size_t row_capacity)
    : RowBatch(table_name, row_capacity, BufferPolicy{}) {}

RowBatch::RowBatch(std::string_view table_name, std::size_t row_capacity,
                   BufferPolicy policy)
    : table_name_(table_name), policy_(policy) {
  // Construction is a capacity hint; PushRow reports fallible growth.
  if (row_capacity <= static_cast<std::size_t>(std::numeric_limits<int>::max()))
    static_cast<void>(rows_.Reserve(row_capacity));
}

RowBatch::RowBatch(RowBatch &&other) noexcept
    : table_name_(std::move(other.table_name_)),
      columns_(std::move(other.columns_)), rows_(std::move(other.rows_)),
      row_count_(std::exchange(other.row_count_, 0)), policy_(other.policy_) {}

RowBatch &RowBatch::operator=(RowBatch &&other) noexcept {
  if (this == &other)
    return *this;
  table_name_ = std::move(other.table_name_);
  columns_ = std::move(other.columns_);
  rows_ = std::move(other.rows_);
  row_count_ = std::exchange(other.row_count_, 0);
  policy_ = other.policy_;
  return *this;
}

Result<std::size_t> RowBatch::AddColumnSchema(std::string_view name,
                                              SemanticType semantic_type,
                                              ColumnDataType data_type) {
  if (row_count_ != 0)
    return SchemaFailure("cannot change a populated row schema");
  if (IsBlank(name))
    return SchemaFailure("column names must be nonempty and unique");
  for (const auto &column : Columns())
    if (column.Name() == name)
      return SchemaFailure("column names must be nonempty and unique");
  if (!ValidDecimalType(data_type))
    return SchemaFailure(
        "decimal precision must be 1..=38 and scale must not exceed precision");
  // Reuse the typed buffer's exhaustive datatype and semantic validation.
  if (!TypedColumnBuf::Create({}, semantic_type, data_type))
    return SchemaFailure("invalid row column type");
  if (columns_.Size() >=
      static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return SchemaFailure("row schema exceeds protobuf column limit");
  if (semantic_type == SemanticType::kTimestamp) {
    if (!IsTemporal(data_type.kind))
      return SchemaFailure("the time index requires a timestamp datatype");
    for (const auto &column : Columns())
      if (column.semantic_type == SemanticType::kTimestamp)
        return SchemaFailure("a row schema requires exactly one time index");
  }
  const std::size_t index = columns_.Size();
  Column staged;
  if (!staged.name.Reserve(name.size()) || !columns_.Reserve(index + 1))
    return std::unexpected(Error(ErrorKind::kEncode, "allocate schema"));
  if (!name.empty())
    std::memcpy(staged.name.Data(), name.data(), name.size());
  staged.name.SetSize(name.size());
  staged.semantic_type = semantic_type;
  staged.data_type = data_type;
  columns_.Append(std::move(staged));
  return index;
}

Result<std::size_t> RowBatch::AddTagColumn(std::string_view name,
                                           ColumnDataType data_type) {
  return AddColumnSchema(name, SemanticType::kTag, data_type);
}

Result<std::size_t> RowBatch::AddTimestampColumn(std::string_view name,
                                                 TimestampUnit unit) {
  return AddColumnSchema(name, SemanticType::kTimestamp,
                         ToColumnDataType(unit));
}

Result<std::size_t> RowBatch::AddFieldColumn(std::string_view name,
                                             ColumnDataType data_type) {
  return AddColumnSchema(name, SemanticType::kField, data_type);
}

Result<void> RowBatch::ValidateValue(const Column &column,
                                     const ValueView &value) {
  if (std::holds_alternative<std::monostate>(value)) {
    if (column.semantic_type == SemanticType::kTimestamp)
      return SchemaFailure("timestamp columns cannot contain nulls");
    return {};
  }
  if (!MatchesKind(column.data_type.kind, value))
    return SchemaFailure("value does not match the declared column datatype");
  if (column.data_type.kind == Kind::kDecimal128) {
    const auto &decimal = std::get<Decimal128>(value);
    if (decimal.precision != column.data_type.precision ||
        decimal.scale != column.data_type.scale)
      return SchemaFailure(
          "decimal metadata must match the declared column type");
    if (!DecimalFits(decimal))
      return SchemaFailure("decimal coefficient exceeds declared precision");
  }
  return {};
}

Result<void> RowBatch::ValidateSchema() const {
  std::size_t timestamp_index = 0;
  std::size_t timestamp_count = 0;
  for (std::size_t i = 0; i < columns_.Size(); ++i) {
    const auto &column = columns_[i];
    if (IsBlank(column.Name()))
      return SchemaFailure("column names must be nonempty and unique");
    for (std::size_t j = 0; j < i; ++j)
      if (columns_[j].Name() == column.Name())
        return SchemaFailure("column names must be nonempty and unique");
    if (!ValidDecimalType(column.data_type))
      return SchemaFailure(
          "decimal precision must be 1..=38 and scale must not exceed "
          "precision");
    if (column.semantic_type != SemanticType::kTimestamp)
      continue;
    if (!IsTemporal(column.data_type.kind))
      return SchemaFailure("the time index requires a timestamp datatype");
    ++timestamp_count;
    timestamp_index = i;
  }
  if (timestamp_count != 1)
    return SchemaFailure("a row schema requires exactly one time index");
  for (std::size_t i = 0; i < row_count_; ++i) {
    if (rows_[i].Size() != columns_.Size())
      return SchemaFailure("row width must equal the schema width");
    if (std::holds_alternative<std::monostate>(rows_[i][timestamp_index].value))
      return SchemaFailure("timestamp values cannot be null");
  }
  return {};
}

Result<void> RowBatch::PushRow(std::span<const ValueView> values) {
  if (auto valid = ValidateSchema(); !valid)
    return std::unexpected(std::move(valid.error()));
  if (values.size() != columns_.Size())
    return SchemaFailure("row width must equal the schema width");
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (auto valid = ValidateValue(columns_[i], values[i]); !valid)
      return std::unexpected(std::move(valid.error()));
  }

  if (row_count_ >= static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
      !rows_.Reserve(row_count_ + 1))
    return std::unexpected(Error(ErrorKind::kEncode, "allocate rows"));
  auto &row = rows_[row_count_];
  if (!row.Reserve(values.size()))
    return std::unexpected(Error(ErrorKind::kEncode, "allocate values"));
  for (std::size_t i = 0; i < values.size(); ++i)
    if (!row[i].Prepare(values[i]))
      return std::unexpected(Error(ErrorKind::kEncode, "allocate bytes"));
  // Nothing below can allocate: existing rows and values remain intact on
  // error.
  for (std::size_t i = 0; i < values.size(); ++i)
    row[i].Commit(values[i]);
  row.SetSize(values.size());
  rows_.SetSize(std::max(rows_.Size(), row_count_ + 1));
  ++row_count_;
  return {};
}

namespace {
std::span<const std::byte> InputBytes(const ValueView &input) noexcept {
  return std::visit(
      [](const auto &v) -> std::span<const std::byte> {
        using T = std::remove_cvref_t<decltype(v)>;
        if constexpr (std::same_as<T, BinaryView>)
          return v;
        else if constexpr (std::same_as<T, std::string_view>)
          return std::as_bytes(std::span(v.data(), v.size()));
        else if constexpr (std::same_as<T, JsonView>)
          return std::as_bytes(std::span(v.value.data(), v.value.size()));
        else
          return {};
      },
      input);
}
} // namespace

bool RowBatch::StoredValue::Prepare(const ValueView &input) noexcept {
  return payload.Reserve(InputBytes(input).size());
}

void RowBatch::StoredValue::Commit(const ValueView &input) noexcept {
  const auto bytes = InputBytes(input);
  if (!bytes.empty())
    std::memcpy(payload.Data(), bytes.data(), bytes.size());
  payload.SetSize(bytes.size());
  value = input;
  const auto *text =
      bytes.empty() ? "" : reinterpret_cast<const char *>(payload.Data());
  if (std::holds_alternative<std::string_view>(input))
    value = std::string_view(text, bytes.size());
  else if (std::holds_alternative<JsonView>(input))
    value = JsonView{{text, bytes.size()}};
  else if (std::holds_alternative<BinaryView>(input))
    value = BinaryView(payload.Data(), bytes.size());
}

Result<void> RowBatch::ValidateDestination(std::string_view table_name) const {
  if (IsBlank(table_name))
    return SchemaFailure("table name must be nonempty");
  return ValidateSchema();
}

Result<::greptime::v1::RowInsertRequest>
RowBatch::EncodeRowInsertRequest() const {
  return EncodeRowInsertRequestFor(table_name_);
}

Result<::greptime::v1::RowInsertRequest>
RowBatch::EncodeRowInsertRequestFor(std::string_view table_name) const {
  if (auto valid = ValidateDestination(table_name); !valid)
    return std::unexpected(std::move(valid.error()));
  ::greptime::v1::RowInsertRequest request;
  request.set_table_name(std::string(table_name));
  auto *rows = request.mutable_rows();
  for (const auto &column : Columns())
    FillProtoSchema(rows->add_schema(), column.Name(), column.semantic_type,
                    column.data_type);
  auto *encoded = rows->mutable_rows();
  encoded->Reserve(ClampToInt(row_count_));
  for (std::size_t i = 0; i < row_count_; ++i) {
    auto *row = encoded->Add();
    for (std::size_t j = 0; j < columns_.Size(); ++j)
      AssignValue(row->add_values(), rows_[i][j].value);
  }
  return request;
}

Result<::greptime::v1::RowDeleteRequest>
RowBatch::EncodeRowDeleteRequest() const {
  return EncodeRowDeleteRequestFor(table_name_);
}

Result<::greptime::v1::RowDeleteRequest>
RowBatch::EncodeRowDeleteRequestFor(std::string_view table_name) const {
  if (auto valid = ValidateDestination(table_name); !valid)
    return std::unexpected(std::move(valid.error()));
  ::greptime::v1::RowDeleteRequest request;
  request.set_table_name(std::string(table_name));
  auto *rows = request.mutable_rows();
  auto *schema = rows->mutable_schema();
  std::size_t key_columns = 0;
  for (const auto &column : Columns()) {
    if (column.semantic_type == SemanticType::kField)
      continue;
    FillProtoSchema(schema->Add(), column.Name(), column.semantic_type,
                    column.data_type);
    ++key_columns;
  }
  auto *encoded = rows->mutable_rows();
  encoded->Reserve(ClampToInt(row_count_));
  for (std::size_t i = 0; i < row_count_; ++i) {
    const auto &source = rows_[i];
    auto *values = encoded->Add()->mutable_values();
    values->Reserve(ClampToInt(key_columns));
    for (std::size_t j = 0; j < columns_.Size(); ++j) {
      if (columns_[j].semantic_type == SemanticType::kField)
        continue;
      AssignValue(values->Add(), source[j].value);
    }
  }
  return request;
}

std::size_t RowBatch::AllocatedBytes() const noexcept {
  auto bytes = rows_.AllocatedBytes();
  for (std::size_t i = 0; i < rows_.Capacity(); ++i) {
    bytes = detail::SaturatingAdd(bytes, rows_[i].AllocatedBytes());
    for (std::size_t j = 0; j < rows_[i].Capacity(); ++j)
      bytes =
          detail::SaturatingAdd(bytes, rows_[i][j].payload.AllocatedBytes());
  }
  return bytes;
}

std::size_t RowBatch::RowCapacity() const noexcept { return rows_.Capacity(); }
void RowBatch::Clear() noexcept { ClearWithPolicy(policy_); }
void RowBatch::ClearWithPolicy(BufferPolicy policy) noexcept {
  row_count_ = 0;
  if (policy.max_retained_rows == 0 || policy.max_retained_bytes == 0 ||
      policy.ShouldShrink(RowCapacity(), AllocatedBytes()))
    rows_.Release();
}
void RowBatch::ClearUnbounded() noexcept {
  ClearWithPolicy(BufferPolicy::Unbounded());
}
void RowBatch::ShrinkToFit() noexcept {
  // Optional compaction preserves logical values if any allocation fails.
  for (std::size_t i = 0; i < row_count_; ++i) {
    for (std::size_t j = 0; j < rows_[i].Size(); ++j) {
      auto &slot = rows_[i][j];
      const auto input = slot.value;
      if (slot.payload.ResizeCapacity(slot.payload.Size())) {
        // Refresh views after moving their backing allocation.
        const auto size = slot.payload.Size();
        const auto *text =
            size == 0 ? ""
                      : reinterpret_cast<const char *>(slot.payload.Data());
        if (std::holds_alternative<std::string_view>(input))
          slot.value = std::string_view(text, size);
        else if (std::holds_alternative<JsonView>(input))
          slot.value = JsonView{{text, size}};
        else if (std::holds_alternative<BinaryView>(input))
          slot.value = BinaryView(slot.payload.Data(), size);
      }
    }
    static_cast<void>(rows_[i].ResizeCapacity(rows_[i].Size()));
  }
  rows_.SetSize(row_count_);
  static_cast<void>(rows_.ResizeCapacity(row_count_));
}

} // namespace greptimedb
