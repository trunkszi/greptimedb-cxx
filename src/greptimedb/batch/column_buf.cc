#include <greptimedb/batch/column_buf.h>

#include <cstdlib>
#include <cstring>
#include <type_traits>

namespace greptimedb {
namespace {
using Kind = ColumnDataType::Kind;
bool ValidDecimal(std::uint32_t precision, std::uint32_t scale) noexcept {
  return precision >= 1 && precision <= 38 && scale <= precision;
}
constexpr auto AllocationFailure() noexcept {
  return std::unexpected(ColumnBufError::kAllocationFailure);
}
std::optional<ColumnBufError> SchemaError(SemanticType semantic,
                                          ColumnDataType type) noexcept {
  bool valid_semantic = false;
  switch (semantic) {
  case SemanticType::kTag:
  case SemanticType::kField:
  case SemanticType::kTimestamp:
    valid_semantic = true;
    break;
  }
  if (!valid_semantic)
    return ColumnBufError::kInvalidSchema;
  switch (type.kind) {
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
  case Kind::kJson:
  case Kind::kDate:
  case Kind::kDateTime:
  case Kind::kTimestampSecond:
  case Kind::kTimestampMillisecond:
  case Kind::kTimestampMicrosecond:
  case Kind::kTimestampNanosecond:
    return std::nullopt;
  case Kind::kDecimal128:
    if (!ValidDecimal(type.precision, type.scale))
      return ColumnBufError::kInvalidDecimal;
    return std::nullopt;
  }
  return ColumnBufError::kInvalidSchema;
}
} // namespace

NullMask::NullMask(std::size_t rows) noexcept {
  // Direct construction treats capacity as a hint; ReserveRows reports failure.
  const auto reserved = ReserveRows(rows);
  if (!reserved)
    bytes_.Release();
}

NullMask::NullMask(NullMask &&other) noexcept
    : bytes_(std::move(other.bytes_)),
      row_count_(std::exchange(other.row_count_, 0)),
      null_count_(std::exchange(other.null_count_, 0)) {}

NullMask &NullMask::operator=(NullMask &&other) noexcept {
  if (this == &other)
    return *this;
  bytes_ = std::move(other.bytes_);
  row_count_ = std::exchange(other.row_count_, 0);
  null_count_ = std::exchange(other.null_count_, 0);
  return *this;
}

std::expected<void, ColumnBufError>
NullMask::ReserveRows(std::size_t rows) noexcept {
  const auto bytes = rows / 8 + static_cast<std::size_t>(rows % 8 != 0);
  if (!bytes_.Reserve(bytes))
    return AllocationFailure();
  return {};
}

std::expected<void, ColumnBufError> NullMask::Push(bool is_null) noexcept {
  if (row_count_ == std::numeric_limits<std::size_t>::max())
    return AllocationFailure();
  if (auto reserved = ReserveRows(row_count_ + 1); !reserved)
    return reserved;
  CommitPush(is_null);
  return {};
}

void NullMask::CommitPush(bool is_null) noexcept {
  if (row_count_ % 8 == 0)
    bytes_.Append(0);
  if (is_null) {
    bytes_[row_count_ / 8] |= static_cast<std::uint8_t>(1U << (row_count_ % 8));
    ++null_count_;
  }
  ++row_count_;
}

bool NullMask::IsNull(std::size_t row) const noexcept {
  return row < row_count_ && (bytes_[row / 8] & (1U << (row % 8))) != 0;
}

std::size_t NullMask::RowCapacity() const noexcept {
  const auto cap = bytes_.Capacity();
  return cap > std::numeric_limits<std::size_t>::max() / 8
             ? std::numeric_limits<std::size_t>::max()
             : cap * 8;
}

void NullMask::ClearWithPolicy(BufferPolicy policy) noexcept {
  const bool release = policy.ShouldShrink(RowCapacity(), AllocatedBytes());
  bytes_.Clear();
  row_count_ = 0;
  null_count_ = 0;
  if (release)
    bytes_.Release();
}

void NullMask::ShrinkToFit() noexcept {
  // A failed optional capacity reduction leaves all rows and capacity intact.
  if (!bytes_.ResizeCapacity(bytes_.Size()))
    return;
}

TypedColumnBuf::TypedColumnBuf(std::string_view name,
                               SemanticType semantic_type,
                               ColumnDataType data_type,
                               std::size_t rows) noexcept
    : semantic_type_(semantic_type), data_type_(data_type) {
  if (SchemaError(semantic_type_, data_type_))
    return;
  InitializeValues();
  if (!name_.Reserve(name.size()))
    return;
  if (!name.empty())
    std::memcpy(name_.Data(), name.data(), name.size());
  name_.SetSize(name.size());
  initialized_ = true;
  if (auto reserved = ReserveRows(rows); !reserved)
    initialized_ = false;
}

TypedColumnBuf::TypedColumnBuf(TypedColumnBuf &&other) noexcept
    : name_(std::move(other.name_)), semantic_type_(other.semantic_type_),
      data_type_(other.data_type_), null_mask_(std::move(other.null_mask_)),
      values_(std::move(other.values_)),
      initialized_(std::exchange(other.initialized_, false)) {}

TypedColumnBuf &TypedColumnBuf::operator=(TypedColumnBuf &&other) noexcept {
  if (this == &other)
    return *this;
  name_ = std::move(other.name_);
  semantic_type_ = other.semantic_type_;
  data_type_ = other.data_type_;
  null_mask_ = std::move(other.null_mask_);
  values_ = std::move(other.values_);
  initialized_ = std::exchange(other.initialized_, false);
  return *this;
}

std::expected<TypedColumnBuf, ColumnBufError>
TypedColumnBuf::Create(std::string_view name, SemanticType semantic_type,
                       ColumnDataType data_type, std::size_t rows) noexcept {
  if (auto error = SchemaError(semantic_type, data_type))
    return std::unexpected(*error);
  TypedColumnBuf result(name, semantic_type, data_type, rows);
  if (!result.IsValid())
    return AllocationFailure();
  return result;
}

bool TypedColumnBuf::IsValid() const noexcept {
  return initialized_ && !SchemaError(semantic_type_, data_type_);
}

void TypedColumnBuf::InitializeValues() noexcept {
  switch (data_type_.kind) {
  case Kind::kBoolean:
    values_.emplace<Array<bool>>();
    break;
  case Kind::kInt8:
    values_.emplace<Array<std::int8_t>>();
    break;
  case Kind::kInt16:
    values_.emplace<Array<std::int16_t>>();
    break;
  case Kind::kInt32:
  case Kind::kDate:
    values_.emplace<Array<std::int32_t>>();
    break;
  case Kind::kInt64:
  case Kind::kDateTime:
  case Kind::kTimestampSecond:
  case Kind::kTimestampMillisecond:
  case Kind::kTimestampMicrosecond:
  case Kind::kTimestampNanosecond:
    values_.emplace<Array<std::int64_t>>();
    break;
  case Kind::kUint8:
    values_.emplace<Array<std::uint8_t>>();
    break;
  case Kind::kUint16:
    values_.emplace<Array<std::uint16_t>>();
    break;
  case Kind::kUint32:
    values_.emplace<Array<std::uint32_t>>();
    break;
  case Kind::kUint64:
    values_.emplace<Array<std::uint64_t>>();
    break;
  case Kind::kFloat32:
    values_.emplace<Array<float>>();
    break;
  case Kind::kFloat64:
    values_.emplace<Array<double>>();
    break;
  case Kind::kBinary:
  case Kind::kString:
  case Kind::kJson:
    values_.emplace<Array<Payload>>();
    break;
  case Kind::kDecimal128:
    values_.emplace<Array<Decimal128>>();
    break;
  }
}

std::size_t TypedColumnBuf::NonNullLen() const noexcept {
  return std::visit([](const auto &v) { return v.Size(); }, values_);
}

std::expected<void, ColumnBufError>
TypedColumnBuf::ReserveRows(std::size_t rows) noexcept {
  if (auto error = SchemaError(semantic_type_, data_type_))
    return std::unexpected(*error);
  if (!initialized_)
    return AllocationFailure();
  if (!std::visit([rows](auto &v) { return v.Reserve(rows); }, values_))
    return AllocationFailure();
  return null_mask_.ReserveRows(rows);
}

bool TypedColumnBuf::Matches(const ValueView &value) const noexcept {
  switch (data_type_.kind) {
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
  case Kind::kDecimal128: {
    const auto *decimal = std::get_if<Decimal128>(&value);
    return decimal && decimal->precision == data_type_.precision &&
           decimal->scale == data_type_.scale;
  }
  }
  return false;
}

std::expected<void, ColumnBufError>
TypedColumnBuf::PreparePush(const ValueView &value) noexcept {
  if (auto error = SchemaError(semantic_type_, data_type_))
    return std::unexpected(*error);
  if (!initialized_)
    return AllocationFailure();
  if (const auto *decimal = std::get_if<Decimal128>(&value);
      decimal && !ValidDecimal(decimal->precision, decimal->scale))
    return std::unexpected(ColumnBufError::kInvalidDecimal);
  if (const auto *decimal = std::get_if<Decimal128>(&value)) {
    unsigned __int128 limit = 1;
    for (std::uint32_t i = 0; i < decimal->precision; ++i)
      limit *= 10;
    const auto bits = static_cast<unsigned __int128>(decimal->value);
    const auto magnitude =
        decimal->value < 0 ? static_cast<unsigned __int128>(0) - bits : bits;
    if (magnitude >= limit)
      return std::unexpected(ColumnBufError::kInvalidDecimal);
  }
  const bool is_null = std::holds_alternative<std::monostate>(value);
  if (is_null && semantic_type_ == SemanticType::kTimestamp)
    return std::unexpected(ColumnBufError::kNullTimestamp);
  if (!is_null && !Matches(value))
    return std::unexpected(ColumnBufError::kTypeMismatch);
  if (RowCount() == std::numeric_limits<std::size_t>::max())
    return AllocationFailure();
  const auto bytes = InputBytes(value);
  if (OwnsInput(bytes))
    return std::unexpected(ColumnBufError::kInvalidSchema);
  if (auto reserved = null_mask_.ReserveRows(RowCount() + 1); !reserved)
    return reserved;
  if (is_null)
    return {};
  if (!std::visit([](auto &v) { return v.Reserve(v.Size() + 1); }, values_))
    return AllocationFailure();
  if (data_type_.kind == Kind::kString || data_type_.kind == Kind::kBinary ||
      data_type_.kind == Kind::kJson) {
    auto &values = std::get<Array<Payload>>(values_);
    if (!values[values.Size()].Reserve(bytes.size()))
      return AllocationFailure();
  }
  return {};
}

std::span<const std::byte>
TypedColumnBuf::InputBytes(const ValueView &value) noexcept {
  return std::visit(
      [](const auto &x) -> std::span<const std::byte> {
        using T = std::remove_cvref_t<decltype(x)>;
        if constexpr (std::is_same_v<T, BinaryView>)
          return x;
        else if constexpr (std::is_same_v<T, std::string_view>)
          return std::as_bytes(std::span(x.data(), x.size()));
        else if constexpr (std::is_same_v<T, JsonView>)
          return std::as_bytes(std::span(x.value.data(), x.value.size()));
        else
          return {};
      },
      value);
}

bool TypedColumnBuf::OwnsInput(
    std::span<const std::byte> bytes) const noexcept {
  // Values are exposed only as owned protobuf copies; masks expose a borrow.
  return detail::Overlaps(bytes, null_mask_.RawBytes().data(),
                          null_mask_.AllocatedBytes());
}

std::expected<void, ColumnBufError>
TypedColumnBuf::Push(const ValueView &value) noexcept {
  if (auto prepared = PreparePush(value); !prepared)
    return prepared;
  CommitPush(value);
  return {};
}

void TypedColumnBuf::CommitPush(const ValueView &value) noexcept {
  if (std::holds_alternative<std::monostate>(value)) {
    null_mask_.CommitPush(true);
    return;
  }
  const bool appended = std::visit(
      [this](const auto &x) {
        using T = std::remove_cvref_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return false;
        } else if constexpr (std::is_same_v<T, BinaryView> ||
                             std::is_same_v<T, std::string_view> ||
                             std::is_same_v<T, JsonView>) {
          std::span<const std::byte> bytes;
          if constexpr (std::is_same_v<T, BinaryView>)
            bytes = x;
          else if constexpr (std::is_same_v<T, JsonView>)
            bytes = std::as_bytes(std::span(x.value.data(), x.value.size()));
          else
            bytes = std::as_bytes(std::span(x.data(), x.size()));
          auto &values = std::get<Array<Payload>>(values_);
          auto &payload = values[values.Size()];
          if (!bytes.empty())
            std::memcpy(payload.Data(), bytes.data(), bytes.size());
          payload.SetSize(bytes.size());
          values.SetSize(values.Size() + 1);
          return true;
        } else if constexpr (std::is_same_v<T, Date>) {
          std::get<Array<std::int32_t>>(values_).Append(x.value);
          return true;
        } else if constexpr (std::is_same_v<T, DateTime> ||
                             std::is_same_v<T, TimestampSecond> ||
                             std::is_same_v<T, TimestampMillisecond> ||
                             std::is_same_v<T, TimestampMicrosecond> ||
                             std::is_same_v<T, TimestampNanosecond>) {
          std::get<Array<std::int64_t>>(values_).Append(x.value);
          return true;
        } else {
          std::get<Array<T>>(values_).Append(x);
          return true;
        }
      },
      value);
  if (!appended)
    std::abort();
  null_mask_.CommitPush(false);
  assert(NonNullLen() + null_mask_.NullCount() == RowCount());
}

std::size_t TypedColumnBuf::AllocatedBytes() const noexcept {
  const auto values = std::visit(
      [](const auto &v) {
        auto bytes = v.AllocatedBytes();
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(v)>,
                                     Array<Payload>>)
          for (std::size_t i = 0; i < v.Capacity(); ++i)
            bytes = detail::SaturatingAdd(bytes, v[i].AllocatedBytes());
        return bytes;
      },
      values_);
  return detail::SaturatingAdd(values, null_mask_.AllocatedBytes());
}

std::size_t TypedColumnBuf::RowCapacity() const noexcept {
  return std::max(
      null_mask_.RowCapacity(),
      std::visit([](const auto &v) { return v.Capacity(); }, values_));
}

void TypedColumnBuf::ReleaseValues() noexcept {
  std::visit([](auto &v) { v.Release(); }, values_);
  null_mask_.ClearWithPolicy(BufferPolicy::Zero());
}

void TypedColumnBuf::ClearWithPolicy(BufferPolicy policy) noexcept {
  if (policy.max_retained_rows == 0 || policy.max_retained_bytes == 0 ||
      policy.ShouldShrink(RowCapacity(), AllocatedBytes())) {
    ReleaseValues();
    return;
  }
  std::visit(
      [](auto &v) {
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(v)>,
                                     Array<Payload>>)
          for (std::size_t i = 0; i < v.Size(); ++i)
            v[i].Clear();
        v.Clear();
      },
      values_);
  null_mask_.ClearUnbounded();
}

void TypedColumnBuf::ShrinkToFit() noexcept {
  std::visit(
      [](auto &v) {
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(v)>,
                                     Array<Payload>>) {
          for (std::size_t i = v.Size(); i < v.Capacity(); ++i)
            v[i].Release();
          for (std::size_t i = 0; i < v.Size(); ++i)
            if (!v[i].ResizeCapacity(v[i].Size()))
              continue;
        }
        if (!v.ResizeCapacity(v.Size()))
          return;
      },
      values_);
  null_mask_.ShrinkToFit();
}

std::expected<TypedColumnBuf, ColumnBufError>
TypedColumnBuf::Clone() const noexcept {
  if (auto error = SchemaError(semantic_type_, data_type_))
    return std::unexpected(*error);
  if (!initialized_)
    return AllocationFailure();
  TypedColumnBuf clone(Name(), semantic_type_, data_type_);
  if (!clone.IsValid())
    return AllocationFailure();
  const bool copied = std::visit(
      [&clone](const auto &source) {
        using A = std::remove_cvref_t<decltype(source)>;
        auto &target = std::get<A>(clone.values_);
        if (!target.ResizeCapacity(source.Capacity()))
          return false;
        const auto slots = std::is_same_v<A, Array<Payload>> ? source.Capacity()
                                                             : source.Size();
        for (std::size_t i = 0; i < slots; ++i) {
          if constexpr (std::is_same_v<A, Array<Payload>>) {
            if (!target[i].ResizeCapacity(source[i].Capacity()))
              return false;
            if (source[i].Size() != 0)
              std::memcpy(target[i].Data(), source[i].Data(), source[i].Size());
            target[i].SetSize(source[i].Size());
          } else
            target[i] = source[i];
        }
        target.SetSize(source.Size());
        return true;
      },
      values_);
  if (!copied ||
      !clone.null_mask_.bytes_.ResizeCapacity(null_mask_.bytes_.Capacity()))
    return AllocationFailure();
  for (auto byte : null_mask_.RawBytes())
    clone.null_mask_.bytes_.Append(byte);
  clone.null_mask_.row_count_ = null_mask_.row_count_;
  clone.null_mask_.null_count_ = null_mask_.null_count_;
  return clone;
}

std::expected<greptime::v1::Column, ColumnBufError>
TypedColumnBuf::TryToProtoColumn() const {
  if (auto error = SchemaError(semantic_type_, data_type_))
    return std::unexpected(*error);
  if (!initialized_)
    return AllocationFailure();
  return ToProtoColumn();
}

greptime::v1::Column TypedColumnBuf::ToProtoColumn() const {
  // Invalid allocation state cannot be encoded, including release builds.
  if (!IsValid())
    std::abort();
  greptime::v1::Column column;
  const auto name = Name();
  column.set_column_name(name.empty() ? "" : name.data(), name.size());
  column.set_semantic_type(ToProto(semantic_type_));
  column.set_datatype(data_type_.ToProtoDatatype());
  if (auto extension = data_type_.ToProtoExtension())
    *column.mutable_datatype_extension() = std::move(*extension);
  const auto mask = null_mask_.Bytes();
  if (!mask.empty())
    column.set_null_mask(mask.data(), mask.size());
  auto &wire = *column.mutable_values();
#define GREPTIMEDB_EMIT(kind, type, field)                                     \
  case Kind::kind: {                                                           \
    const auto &v = std::get<Array<type>>(values_);                            \
    for (std::size_t i = 0; i < v.Size(); ++i)                                 \
      wire.add_##field(v[i]);                                                  \
    break;                                                                     \
  }
  switch (data_type_.kind) {
    GREPTIMEDB_EMIT(kBoolean, bool, bool_values)
    GREPTIMEDB_EMIT(kInt8, std::int8_t, i8_values)
    GREPTIMEDB_EMIT(kInt16, std::int16_t, i16_values)
    GREPTIMEDB_EMIT(kInt32, std::int32_t, i32_values)
    GREPTIMEDB_EMIT(kInt64, std::int64_t, i64_values)
    GREPTIMEDB_EMIT(kUint8, std::uint8_t, u8_values)
    GREPTIMEDB_EMIT(kUint16, std::uint16_t, u16_values)
    GREPTIMEDB_EMIT(kUint32, std::uint32_t, u32_values)
    GREPTIMEDB_EMIT(kUint64, std::uint64_t, u64_values)
    GREPTIMEDB_EMIT(kFloat32, float, f32_values)
    GREPTIMEDB_EMIT(kFloat64, double, f64_values)
    GREPTIMEDB_EMIT(kDate, std::int32_t, date_values)
    GREPTIMEDB_EMIT(kDateTime, std::int64_t, datetime_values)
    GREPTIMEDB_EMIT(kTimestampSecond, std::int64_t, timestamp_second_values)
    GREPTIMEDB_EMIT(kTimestampMillisecond, std::int64_t,
                    timestamp_millisecond_values)
    GREPTIMEDB_EMIT(kTimestampMicrosecond, std::int64_t,
                    timestamp_microsecond_values)
    GREPTIMEDB_EMIT(kTimestampNanosecond, std::int64_t,
                    timestamp_nanosecond_values)
  case Kind::kString:
  case Kind::kBinary:
  case Kind::kJson: {
    const auto &v = std::get<Array<Payload>>(values_);
    for (std::size_t i = 0; i < v.Size(); ++i) {
      const auto *data =
          v[i].Size() == 0 ? "" : reinterpret_cast<const char *>(v[i].Data());
      if (data_type_.kind == Kind::kString)
        wire.add_string_values(data, v[i].Size());
      else
        wire.add_binary_values(data, v[i].Size());
    }
    break;
  }
  case Kind::kDecimal128: {
    const auto &v = std::get<Array<Decimal128>>(values_);
    for (std::size_t i = 0; i < v.Size(); ++i)
      *wire.add_decimal128_values() = v[i].ToProto();
    break;
  }
  }
#undef GREPTIMEDB_EMIT
  return column;
}

} // namespace greptimedb
