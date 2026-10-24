#include <greptimedb/batch/columnar_batch.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <string>
#include <utility>
#include <variant>

namespace greptimedb {
namespace {

using Kind = ColumnDataType::Kind;

constexpr std::size_t kMaxWireRows = std::numeric_limits<std::uint32_t>::max();

bool IsBlank(std::string_view text) noexcept {
  return text.find_first_not_of(" \t\n\r\f\v") == std::string_view::npos;
}

Error SchemaError(std::string message) {
  return Error(ErrorKind::kSchemaViolation, std::move(message));
}

Error ColumnBufferError(ColumnBufError error) {
  switch (error) {
  case ColumnBufError::kNullTimestamp:
    return SchemaError("timestamp columns cannot contain nulls");
  case ColumnBufError::kInvalidDecimal:
    return SchemaError("decimal precision must be 1..=38 and scale must not "
                       "exceed precision");
  case ColumnBufError::kTypeMismatch:
    return SchemaError("value does not match the declared column datatype");
  case ColumnBufError::kInvalidSchema:
    return SchemaError("invalid column schema");
  case ColumnBufError::kAllocationFailure:
    return Error(ErrorKind::kEncode, "allocate column");
  }
  return SchemaError("invalid column buffer");
}

bool ValidDecimal(std::uint32_t precision, std::uint32_t scale) noexcept {
  return precision >= 1 && precision <= 38 && scale <= precision;
}

/** The time index accepts only timestamp datatypes, not DATE or DATETIME. */
bool IsTemporal(const ColumnDataType &type) noexcept {
  switch (type.kind) {
  case Kind::kTimestampSecond:
  case Kind::kTimestampMillisecond:
  case Kind::kTimestampMicrosecond:
  case Kind::kTimestampNanosecond:
    return true;
  case Kind::kBoolean:
  case Kind::kInt8:
  case Kind::kInt16:
  case Kind::kInt32:
  case Kind::kInt64:
  case Kind::kUint8:
  case Kind::kUint16:
  case Kind::kUint32:
  case Kind::kUint64:
  case Kind::kFloat32:
  case Kind::kFloat64:
  case Kind::kBinary:
  case Kind::kString:
  case Kind::kDate:
  case Kind::kDateTime:
  case Kind::kDecimal128:
  case Kind::kJson:
    return false;
  }
  return false;
}

/** A decimal coefficient must fit the column precision; TypedColumnBuf only
 * checks the value metadata. */
bool DecimalFits(const Decimal128 &decimal) noexcept {
  if (!ValidDecimal(decimal.precision, decimal.scale))
    return false;
  const unsigned __int128 magnitude =
      decimal.value < 0
          ? static_cast<unsigned __int128>(-(decimal.value + 1)) + 1
          : static_cast<unsigned __int128>(decimal.value);
  unsigned __int128 limit = 1;
  for (std::uint32_t digit = 0; digit < decimal.precision; ++digit)
    limit *= 10;
  return magnitude < limit;
}

bool DecimalValuesFit(const greptime::v1::Column &column,
                      const ColumnDataType &type) {
  if (type.kind != Kind::kDecimal128)
    return true;
  for (const greptime::v1::Decimal128 &value :
       column.values().decimal128_values()) {
    if (!DecimalFits(Decimal128::FromProto(value, type.precision, type.scale)))
      return false;
  }
  return true;
}

bool MatchesValue(const ValueView &value, Kind kind) {
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
  case Kind::kJson:
    return std::holds_alternative<JsonView>(value);
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
  }
  return false;
}

/** Validates one value without touching the column, so a rejected row leaves
 * every buffer unchanged. */
Result<void> ValidateValue(const TypedColumnBuf &column,
                           const ValueView &value) {
  const ColumnDataType type = column.DataType();
  if (std::holds_alternative<std::monostate>(value)) {
    if (column.SemanticTypeValue() == SemanticType::kTimestamp)
      return std::unexpected(
          SchemaError("timestamp columns cannot contain nulls"));
    return {};
  }
  if (type.kind == Kind::kDecimal128) {
    const auto *decimal = std::get_if<Decimal128>(&value);
    if (decimal == nullptr || decimal->precision != type.precision ||
        decimal->scale != type.scale)
      return std::unexpected(
          SchemaError("value does not match the declared column datatype"));
    if (!DecimalFits(*decimal))
      return std::unexpected(
          SchemaError("decimal coefficient exceeds declared precision"));
    return {};
  }
  if (!MatchesValue(value, type.kind))
    return std::unexpected(
        SchemaError("value does not match the declared column datatype"));
  return {};
}

} // namespace

ColumnarBatch::ColumnarBatch(std::string_view table) : table_(table) {}

ColumnarBatch::ColumnarBatch(std::string_view table, std::size_t row_capacity)
    : table_(table), initial_row_capacity_(row_capacity) {}

ColumnarBatch::ColumnarBatch(std::string_view table, std::size_t row_capacity,
                             BufferPolicy policy)
    : table_(table), policy_(policy), initial_row_capacity_(row_capacity) {}

std::size_t ColumnarBatch::RowCount() const noexcept {
  return columns_.empty() ? 0 : columns_.front().RowCount();
}

const TypedColumnBuf *ColumnarBatch::Column(std::size_t index) const noexcept {
  return index < columns_.size() ? &columns_[index] : nullptr;
}

const TypedColumnBuf *
ColumnarBatch::ColumnByName(std::string_view name) const noexcept {
  for (const TypedColumnBuf &column : columns_)
    if (column.Name() == name)
      return &column;
  return nullptr;
}

TypedColumnBuf *ColumnarBatch::ColumnMut(std::size_t index) noexcept {
  return index < columns_.size() ? &columns_[index] : nullptr;
}

TypedColumnBuf *ColumnarBatch::ColumnByNameMut(std::string_view name) noexcept {
  for (TypedColumnBuf &column : columns_)
    if (column.Name() == name)
      return &column;
  return nullptr;
}

Result<std::size_t> ColumnarBatch::AddColumn(std::string_view name,
                                             SemanticType semantic_type,
                                             ColumnDataType data_type) {
  if (IsBlank(name))
    return std::unexpected(SchemaError("column names must be nonempty"));
  if (ColumnByName(name) != nullptr)
    return std::unexpected(SchemaError("column names must be unique"));
  for (const TypedColumnBuf &column : columns_)
    if (column.RowCount() != 0)
      return std::unexpected(
          SchemaError("cannot change schema while rows are buffered"));
  if (semantic_type == SemanticType::kTimestamp) {
    if (!IsTemporal(data_type))
      return std::unexpected(
          SchemaError("the time index requires a timestamp datatype"));
    for (const TypedColumnBuf &column : columns_)
      if (column.SemanticTypeValue() == SemanticType::kTimestamp)
        return std::unexpected(
            SchemaError("a table must have exactly one timestamp column"));
  }
  auto column = TypedColumnBuf::Create(name, semantic_type, data_type,
                                       initial_row_capacity_);
  if (!column)
    return std::unexpected(ColumnBufferError(column.error()));
  columns_.push_back(std::move(*column));
  return columns_.size() - 1;
}

Result<std::size_t> ColumnarBatch::AddTagColumn(std::string_view name,
                                                ColumnDataType data_type) {
  return AddColumn(name, SemanticType::kTag, data_type);
}

Result<std::size_t> ColumnarBatch::AddTimestampColumn(std::string_view name,
                                                      TimestampUnit unit) {
  return AddColumn(name, SemanticType::kTimestamp, ToColumnDataType(unit));
}

Result<std::size_t> ColumnarBatch::AddFieldColumn(std::string_view name,
                                                  ColumnDataType data_type) {
  return AddColumn(name, SemanticType::kField, data_type);
}

Result<TypedColumnBuf>
ColumnarBatch::ReplaceColumn(std::size_t index, TypedColumnBuf replacement) {
  if (index >= columns_.size())
    return std::unexpected(SchemaError("column index is out of range"));
  TypedColumnBuf previous = std::move(columns_[index]);
  columns_[index] = std::move(replacement);
  return previous;
}

Result<void> ColumnarBatch::PushRow(std::span<const ValueView> values) {
  if (auto schema = ValidateSchema(); !schema)
    return schema;
  return AppendRow(values);
}

Result<void> ColumnarBatch::PushResultRow(std::span<const ValueView> values) {
  if (auto shape = ValidateResultShape(); !shape)
    return shape;
  return AppendRow(values);
}

Result<void> ColumnarBatch::AppendRow(std::span<const ValueView> values) {
  if (columns_.empty() || values.size() != columns_.size())
    return std::unexpected(
        SchemaError("row width must equal the schema width"));
  if (RowCount() >= kMaxWireRows)
    return std::unexpected(
        SchemaError("row count exceeds the protobuf u32 limit"));
  for (std::size_t index = 0; index < columns_.size(); ++index) {
    if (auto valid = ValidateValue(columns_[index], values[index]); !valid)
      return valid;
  }
  // Reject inputs borrowing any column before reservation can invalidate them.
  for (const auto &value : values)
    for (const auto &column : columns_)
      if (column.OwnsInput(TypedColumnBuf::InputBytes(value)))
        return std::unexpected(
            SchemaError("row payload aliases batch storage"));
  // All payload allocation is fallible preparation; commit never allocates.
  for (std::size_t index = 0; index < columns_.size(); ++index) {
    if (auto prepared = columns_[index].PreparePush(values[index]); !prepared)
      return std::unexpected(ColumnBufferError(prepared.error()));
  }
  for (std::size_t index = 0; index < columns_.size(); ++index)
    columns_[index].CommitPush(values[index]);
  return {};
}

Result<void> ColumnarBatch::ValidateResultShape() const {
  std::size_t timestamp_count = 0;
  for (std::size_t index = 0; index < columns_.size(); ++index) {
    const TypedColumnBuf &column = columns_[index];
    if (!column.IsValid())
      return std::unexpected(SchemaError("column buffer is not initialized"));
    if (IsBlank(column.Name()))
      return std::unexpected(SchemaError("column names must be nonempty"));
    for (std::size_t previous = 0; previous < index; ++previous)
      if (columns_[previous].Name() == column.Name())
        return std::unexpected(SchemaError("column names must be unique"));
    const ColumnDataType type = column.DataType();
    if (type.kind == Kind::kDecimal128 &&
        !ValidDecimal(type.precision, type.scale))
      return std::unexpected(
          SchemaError("decimal precision must be 1..=38 and scale must not "
                      "exceed precision"));
    if (column.RowCount() != RowCount())
      return std::unexpected(
          SchemaError("all columns must have identical row counts"));
    if (column.SemanticTypeValue() == SemanticType::kTimestamp) {
      ++timestamp_count;
      if (!IsTemporal(type) || column.Nulls().HasNulls())
        return std::unexpected(
            SchemaError("the time index must contain non-null timestamps"));
    }
  }
  if (timestamp_count > 1)
    return std::unexpected(
        SchemaError("a table may have at most one timestamp column"));
  if (RowCount() > kMaxWireRows)
    return std::unexpected(
        SchemaError("row count exceeds the protobuf u32 limit"));
  return {};
}

Result<void> ColumnarBatch::ValidateSchema() const {
  if (auto shape = ValidateResultShape(); !shape)
    return shape;
  for (const TypedColumnBuf &column : columns_)
    if (column.SemanticTypeValue() == SemanticType::kTimestamp)
      return {};
  return std::unexpected(
      SchemaError("a table must have exactly one timestamp column"));
}

Result<::greptime::v1::InsertRequest>
ColumnarBatch::EncodeInsertRequest() const {
  return EncodeInsertRequestFor(table_);
}

Result<::greptime::v1::InsertRequest>
ColumnarBatch::EncodeInsertRequestFor(std::string_view table_name) const {
  if (auto schema = ValidateSchema(); !schema)
    return std::unexpected(std::move(schema.error()));
  if (IsBlank(table_name))
    return std::unexpected(
        Error(ErrorKind::kInvalidConfig, "table name must not be empty"));
  const std::size_t rows = RowCount();
  if (rows > kMaxWireRows)
    return std::unexpected(
        SchemaError("row count exceeds the protobuf u32 limit"));
  ::greptime::v1::InsertRequest request;
  request.set_table_name(std::string(table_name));
  request.set_row_count(static_cast<std::uint32_t>(rows));
  for (const TypedColumnBuf &column : columns_) {
    auto encoded = column.TryToProtoColumn();
    if (!encoded)
      return std::unexpected(ColumnBufferError(encoded.error()));
    if (!DecimalValuesFit(*encoded, column.DataType()))
      return std::unexpected(
          SchemaError("decimal coefficient exceeds declared precision"));
    *request.add_columns() = std::move(*encoded);
  }
  return request;
}

Result<::greptime::v1::DeleteRequest>
ColumnarBatch::EncodeDeleteRequest() const {
  return EncodeDeleteRequestFor(table_);
}

Result<::greptime::v1::DeleteRequest>
ColumnarBatch::EncodeDeleteRequestFor(std::string_view table_name) const {
  if (auto schema = ValidateSchema(); !schema)
    return std::unexpected(std::move(schema.error()));
  if (IsBlank(table_name))
    return std::unexpected(
        Error(ErrorKind::kInvalidConfig, "table name must not be empty"));
  const std::size_t rows = RowCount();
  if (rows > kMaxWireRows)
    return std::unexpected(
        SchemaError("row count exceeds the protobuf u32 limit"));
  ::greptime::v1::DeleteRequest request;
  request.set_table_name(std::string(table_name));
  request.set_row_count(static_cast<std::uint32_t>(rows));
  for (const TypedColumnBuf &column : columns_) {
    auto encoded = column.TryToProtoColumn();
    if (!encoded)
      return std::unexpected(ColumnBufferError(encoded.error()));
    // Field decimals are checked even though the key projection drops them.
    if (!DecimalValuesFit(*encoded, column.DataType()))
      return std::unexpected(
          SchemaError("decimal coefficient exceeds declared precision"));
    if (column.SemanticTypeValue() == SemanticType::kField)
      continue;
    *request.add_key_columns() = std::move(*encoded);
  }
  return request;
}

std::size_t ColumnarBatch::AllocatedBytes() const noexcept {
  std::size_t bytes = 0;
  for (const TypedColumnBuf &column : columns_)
    bytes = detail::SaturatingAdd(bytes, column.AllocatedBytes());
  return bytes;
}

std::size_t ColumnarBatch::RowCapacity() const noexcept {
  std::size_t capacity = 0;
  for (const TypedColumnBuf &column : columns_)
    capacity = std::max(capacity, column.RowCapacity());
  return capacity;
}

void ColumnarBatch::ClearWithPolicy(BufferPolicy policy) noexcept {
  std::size_t remaining = policy.max_retained_bytes;
  for (TypedColumnBuf &column : columns_) {
    column.ClearWithPolicy({policy.max_retained_rows, remaining});
    const std::size_t allocated = column.AllocatedBytes();
    remaining = remaining > allocated ? remaining - allocated : 0;
  }
}

void ColumnarBatch::ShrinkToFit() noexcept {
  for (TypedColumnBuf &column : columns_)
    column.ShrinkToFit();
}

Error MultiTableBatch::EncodeError(ColumnBufError error) {
  return ColumnBufferError(error);
}

MultiTableBatch::MultiTableBatch(const MultiTableBatch &other) {
  // Copying is an allocating value operation, like protobuf snapshot encoding.
  if (!requests_.Reserve(other.requests_.Size()))
    std::abort();
  for (std::size_t i = 0; i < other.requests_.Size(); ++i)
    requests_.Append(other.requests_[i]);
}

MultiTableBatch &MultiTableBatch::operator=(const MultiTableBatch &other) {
  if (this != &other) {
    MultiTableBatch copy(other);
    requests_.Swap(copy.requests_);
  }
  return *this;
}

Result<void>
MultiTableBatch::PushRequest(::greptime::v1::InsertRequest request) {
  const std::size_t total = TotalRows();
  if (request.row_count() > std::numeric_limits<std::size_t>::max() - total)
    return std::unexpected(SchemaError("multi-table row count exceeds size_t"));
  if (requests_.Size() >=
          static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
      !requests_.Reserve(requests_.Size() + 1))
    return std::unexpected(Error(ErrorKind::kEncode, "allocate batch"));
  requests_.Append(std::move(request));
  return {};
}

std::size_t MultiTableBatch::TotalRows() const noexcept {
  std::size_t total = 0;
  for (std::size_t i = 0; i < requests_.Size(); ++i)
    total += requests_[i].row_count();
  return total;
}

std::size_t MultiTableBatch::TableCount() const noexcept {
  return requests_.Size();
}

::greptime::v1::InsertRequests MultiTableBatch::IntoInsertRequests() {
  auto result = ToInsertRequests();
  Clear();
  return result;
}

::greptime::v1::InsertRequests MultiTableBatch::ToInsertRequests() const {
  ::greptime::v1::InsertRequests result;
  result.mutable_inserts()->Reserve(static_cast<int>(requests_.Size()));
  for (std::size_t i = 0; i < requests_.Size(); ++i)
    *result.add_inserts() = requests_[i];
  return result;
}

void MultiTableBatch::Clear() noexcept {
  for (std::size_t i = 0; i < requests_.Size(); ++i)
    requests_[i].Clear();
  requests_.Clear();
}

} // namespace greptimedb
