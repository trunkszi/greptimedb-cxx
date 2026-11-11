#include <greptimedb/batch/column_buf.h>

#include <uarch/uarch.hpp>

#include <cstdlib>
#include <cstring>
#include <type_traits>

namespace allocation_probe {
[[gnu::weak]] bool RejectNothrow() noexcept;
[[gnu::weak]] void *Allocate(std::size_t size, std::size_t alignment) noexcept;
} // namespace allocation_probe

namespace greptimedb {
namespace detail {

[[gnu::weak]] void *AllocateZeroedColumnBytes(std::size_t count, std::size_t elem_size,
                                              std::size_t alignment) noexcept {
  if (count == 0 || elem_size == 0) return nullptr;
  if (count > std::numeric_limits<std::size_t>::max() / elem_size) return nullptr;
  if (&allocation_probe::RejectNothrow != nullptr && allocation_probe::RejectNothrow()) return nullptr;
  if (&allocation_probe::Allocate != nullptr) {
    const std::size_t bytes = count * elem_size;
    void *ptr = allocation_probe::Allocate(bytes, alignment);
    if (ptr) std::memset(ptr, 0, bytes);
    return ptr;
  }
  if (alignment <= alignof(std::max_align_t)) return std::calloc(count, elem_size);
  const std::size_t bytes = count * elem_size;
  void *ptr = nullptr;
  if (::posix_memalign(&ptr, alignment, bytes) != 0) return nullptr;
  std::memset(ptr, 0, bytes);
  return ptr;
}

[[gnu::weak]] void FreeColumnBytes(void *ptr) noexcept { std::free(ptr); }

} // namespace detail
namespace {
using Kind = ColumnDataType::Kind;
bool ValidDecimal(std::uint32_t precision, std::uint32_t scale) noexcept {
  return precision >= 1 && precision <= 38 && scale <= precision;
}
constexpr auto AllocationFailure() noexcept { return std::unexpected(ColumnBufError::kAllocationFailure); }
const char *BytesAsChar(const std::byte *data) noexcept {
  return static_cast<const char *>(static_cast<const void *>(data));
}
template <class Values, class Emit> void EmitEach(const Values &values, Emit emit) {
  for (std::size_t i = 0; i < values.Size(); ++i)
    emit(values[i]);
}
std::optional<ColumnBufError> SchemaError(SemanticType semantic, ColumnDataType type) noexcept {
  bool valid_semantic = false;
  switch (semantic) {
  case SemanticType::kTag:
  case SemanticType::kField:
  case SemanticType::kTimestamp: valid_semantic = true; break;
  }
  if (!valid_semantic) return ColumnBufError::kInvalidSchema;
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
  case Kind::kTimestampNanosecond: return std::nullopt;
  case Kind::kDecimal128:
    if (!ValidDecimal(type.precision, type.scale)) return ColumnBufError::kInvalidDecimal;
    return std::nullopt;
  }
  return ColumnBufError::kInvalidSchema;
}
} // namespace

NullMask::NullMask(std::size_t rows) noexcept {
  // Direct construction treats capacity as a hint; ReserveRows reports failure.
  const auto reserved = ReserveRows(rows);
  if (!reserved) bytes_.Release();
}

NullMask::NullMask(NullMask &&other) noexcept
    : bytes_(std::move(other.bytes_)), row_count_(std::exchange(other.row_count_, 0)),
      null_count_(std::exchange(other.null_count_, 0)) {}

NullMask &NullMask::operator=(NullMask &&other) noexcept {
  if (this == &other) return *this;
  bytes_ = std::move(other.bytes_);
  row_count_ = std::exchange(other.row_count_, 0);
  null_count_ = std::exchange(other.null_count_, 0);
  return *this;
}

std::expected<void, ColumnBufError> NullMask::ReserveRows(std::size_t rows) noexcept {
  const auto bytes = static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(rows)) +
                     static_cast<std::size_t>(uarch::PowerOfTwo<3>::rem(rows) != 0U);
  if (!bytes_.Reserve(bytes)) return AllocationFailure();
  return {};
}

std::expected<void, ColumnBufError> NullMask::Push(bool is_null) noexcept {
  if (uarch::unlikely(row_count_ == std::numeric_limits<std::size_t>::max())) return AllocationFailure();
  if (auto reserved = ReserveRows(row_count_ + 1); uarch::unlikely(!reserved)) return reserved;
  CommitPush(is_null);
  return {};
}

void NullMask::CommitPush(bool is_null) noexcept {
  if (uarch::PowerOfTwo<3>::rem(row_count_) == 0U) bytes_.Append(0);
  const auto bit_flag = uarch::predicate(is_null) * std::uint8_t{1};
  bytes_[static_cast<std::size_t>(uarch::PowerOfTwo<3>::div(row_count_))] |=
      static_cast<std::uint8_t>(bit_flag << uarch::PowerOfTwo<3>::rem(row_count_));
  null_count_ += bit_flag;
  ++row_count_;
}

std::size_t NullMask::RowCapacity() const noexcept {
  const auto cap = bytes_.Capacity();
  return cap > std::numeric_limits<std::size_t>::max() / 8 ? std::numeric_limits<std::size_t>::max() : cap * 8;
}

void NullMask::ClearWithPolicy(BufferPolicy policy) noexcept {
  const bool release = policy.ShouldShrink(RowCapacity(), AllocatedBytes());
  bytes_.Clear();
  row_count_ = 0;
  null_count_ = 0;
  if (release) bytes_.Release();
}

void NullMask::ShrinkToFit() noexcept {
  // A failed optional capacity reduction leaves all rows and capacity intact.
  if (!bytes_.ResizeCapacity(bytes_.Size())) return;
}

TypedColumnBuf::TypedColumnBuf(std::string_view name, SemanticType semantic_type, ColumnDataType data_type,
                               std::size_t rows) noexcept
    : semantic_type_(semantic_type), data_type_(data_type) {
  if (SchemaError(semantic_type_, data_type_)) return;
  InitializeValues();
  if (!name_.Reserve(name.size())) return;
  if (!name.empty()) std::memcpy(name_.Data(), name.data(), name.size());
  name_.SetSize(name.size());
  initialized_ = true;
  if (auto reserved = ReserveRows(rows); !reserved) initialized_ = false;
}

TypedColumnBuf::TypedColumnBuf(TypedColumnBuf &&other) noexcept
    : name_(std::move(other.name_)), semantic_type_(other.semantic_type_), data_type_(other.data_type_),
      null_mask_(std::move(other.null_mask_)), values_(std::move(other.values_)),
      initialized_(std::exchange(other.initialized_, false)) {}

TypedColumnBuf &TypedColumnBuf::operator=(TypedColumnBuf &&other) noexcept {
  if (this == &other) return *this;
  name_ = std::move(other.name_);
  semantic_type_ = other.semantic_type_;
  data_type_ = other.data_type_;
  null_mask_ = std::move(other.null_mask_);
  values_ = std::move(other.values_);
  initialized_ = std::exchange(other.initialized_, false);
  return *this;
}

std::expected<TypedColumnBuf, ColumnBufError> TypedColumnBuf::Create(std::string_view name, SemanticType semantic_type,
                                                                     ColumnDataType data_type,
                                                                     std::size_t rows) noexcept {
  if (auto error = SchemaError(semantic_type, data_type)) return std::unexpected(*error);
  TypedColumnBuf result(name, semantic_type, data_type, rows);
  if (!result.IsValid()) return AllocationFailure();
  return result;
}

bool TypedColumnBuf::IsValid() const noexcept { return initialized_ && !SchemaError(semantic_type_, data_type_); }

void TypedColumnBuf::InitializeValues() noexcept {
  switch (data_type_.kind) {
  case Kind::kBoolean: values_.emplace<Array<bool>>(); break;
  case Kind::kInt8: values_.emplace<Array<std::int8_t>>(); break;
  case Kind::kInt16: values_.emplace<Array<std::int16_t>>(); break;
  case Kind::kInt32:
  case Kind::kDate: values_.emplace<Array<std::int32_t>>(); break;
  case Kind::kInt64:
  case Kind::kDateTime:
  case Kind::kTimestampSecond:
  case Kind::kTimestampMillisecond:
  case Kind::kTimestampMicrosecond:
  case Kind::kTimestampNanosecond: values_.emplace<Array<std::int64_t>>(); break;
  case Kind::kUint8: values_.emplace<Array<std::uint8_t>>(); break;
  case Kind::kUint16: values_.emplace<Array<std::uint16_t>>(); break;
  case Kind::kUint32: values_.emplace<Array<std::uint32_t>>(); break;
  case Kind::kUint64: values_.emplace<Array<std::uint64_t>>(); break;
  case Kind::kFloat32: values_.emplace<Array<float>>(); break;
  case Kind::kFloat64: values_.emplace<Array<double>>(); break;
  case Kind::kBinary:
  case Kind::kString:
  case Kind::kJson: values_.emplace<Array<Payload>>(); break;
  case Kind::kDecimal128: values_.emplace<Array<Decimal128>>(); break;
  }
}

std::size_t TypedColumnBuf::NonNullLen() const noexcept {
  return std::visit([](const auto &v) { return v.Size(); }, values_);
}

std::expected<void, ColumnBufError> TypedColumnBuf::ReserveRows(std::size_t rows) noexcept {
  if (auto error = SchemaError(semantic_type_, data_type_)) return std::unexpected(*error);
  if (!initialized_) return AllocationFailure();
  if (!std::visit([rows](auto &v) { return v.Reserve(rows); }, values_)) return AllocationFailure();
  return null_mask_.ReserveRows(rows);
}

bool TypedColumnBuf::Matches(const ValueView &value) const noexcept {
  switch (data_type_.kind) {
  case Kind::kBoolean: return std::holds_alternative<bool>(value);
  case Kind::kInt8: return std::holds_alternative<std::int8_t>(value);
  case Kind::kInt16: return std::holds_alternative<std::int16_t>(value);
  case Kind::kInt32: return std::holds_alternative<std::int32_t>(value);
  case Kind::kInt64: return std::holds_alternative<std::int64_t>(value);
  case Kind::kUint8: return std::holds_alternative<std::uint8_t>(value);
  case Kind::kUint16: return std::holds_alternative<std::uint16_t>(value);
  case Kind::kUint32: return std::holds_alternative<std::uint32_t>(value);
  case Kind::kUint64: return std::holds_alternative<std::uint64_t>(value);
  case Kind::kFloat32: return std::holds_alternative<float>(value);
  case Kind::kFloat64: return std::holds_alternative<double>(value);
  case Kind::kBinary: return std::holds_alternative<BinaryView>(value);
  case Kind::kString: return std::holds_alternative<std::string_view>(value);
  case Kind::kJson: return std::holds_alternative<JsonView>(value);
  case Kind::kDate: return std::holds_alternative<Date>(value);
  case Kind::kDateTime: return std::holds_alternative<DateTime>(value);
  case Kind::kTimestampSecond: return std::holds_alternative<TimestampSecond>(value);
  case Kind::kTimestampMillisecond: return std::holds_alternative<TimestampMillisecond>(value);
  case Kind::kTimestampMicrosecond: return std::holds_alternative<TimestampMicrosecond>(value);
  case Kind::kTimestampNanosecond: return std::holds_alternative<TimestampNanosecond>(value);
  case Kind::kDecimal128: {
    const auto *decimal = std::get_if<Decimal128>(&value);
    return decimal && decimal->precision == data_type_.precision && decimal->scale == data_type_.scale;
  }
  }
  return false;
}

std::expected<void, ColumnBufError> TypedColumnBuf::PreparePush(const ValueView &value) noexcept {
  if (auto error = SchemaError(semantic_type_, data_type_); uarch::unlikely(error.has_value()))
    return std::unexpected(*error);
  if (uarch::unlikely(!initialized_)) return AllocationFailure();
  if (const auto *decimal = std::get_if<Decimal128>(&value)) {
    if (uarch::unlikely(!ValidDecimal(decimal->precision, decimal->scale) ||
                        !ValidDecimalCoefficient(decimal->value, decimal->precision)))
      return std::unexpected(ColumnBufError::kInvalidDecimal);
  }
  const bool is_null = std::holds_alternative<std::monostate>(value);
  if (uarch::unlikely(is_null && semantic_type_ == SemanticType::kTimestamp))
    return std::unexpected(ColumnBufError::kNullTimestamp);
  if (uarch::unlikely(!is_null && !Matches(value))) return std::unexpected(ColumnBufError::kTypeMismatch);
  if (uarch::unlikely(RowCount() == std::numeric_limits<std::size_t>::max())) return AllocationFailure();
  const auto bytes = InputBytes(value);
  if (uarch::unlikely(OwnsInput(bytes))) return std::unexpected(ColumnBufError::kInvalidSchema);
  if (auto reserved = null_mask_.ReserveRows(RowCount() + 1); uarch::unlikely(!reserved)) return reserved;
  if (is_null) return {};
  if (uarch::unlikely(!std::visit([](auto &v) { return v.Reserve(v.Size() + 1); }, values_)))
    return AllocationFailure();
  if (data_type_.kind == Kind::kString || data_type_.kind == Kind::kBinary || data_type_.kind == Kind::kJson) {
    auto &values = std::get<Array<Payload>>(values_);
    if (uarch::unlikely(!values[values.Size()].Reserve(bytes.size()))) return AllocationFailure();
  }
  return {};
}

std::span<const std::byte> TypedColumnBuf::InputBytes(const ValueView &value) noexcept {
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

bool TypedColumnBuf::OwnsInput(std::span<const std::byte> bytes) const noexcept {
  // Values are exposed only as owned protobuf copies; masks expose a borrow.
  return detail::Overlaps(bytes, null_mask_.RawBytes().data(), null_mask_.AllocatedBytes());
}

std::expected<void, ColumnBufError> TypedColumnBuf::Push(const ValueView &value) noexcept {
  if (auto prepared = PreparePush(value); !prepared) return prepared;
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
        } else if constexpr (std::is_same_v<T, BinaryView> || std::is_same_v<T, std::string_view> ||
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
          if (!bytes.empty()) std::memcpy(payload.Data(), bytes.data(), bytes.size());
          payload.SetSize(bytes.size());
          values.SetSize(values.Size() + 1);
          return true;
        } else if constexpr (std::is_same_v<T, Date>) {
          std::get<Array<std::int32_t>>(values_).Append(x.value);
          return true;
        } else if constexpr (std::is_same_v<T, DateTime> || std::is_same_v<T, TimestampSecond> ||
                             std::is_same_v<T, TimestampMillisecond> || std::is_same_v<T, TimestampMicrosecond> ||
                             std::is_same_v<T, TimestampNanosecond>) {
          std::get<Array<std::int64_t>>(values_).Append(x.value);
          return true;
        } else {
          std::get<Array<T>>(values_).Append(x);
          return true;
        }
      },
      value);
  if (!appended) std::abort();
  null_mask_.CommitPush(false);
  assert(NonNullLen() + null_mask_.NullCount() == RowCount());
}

std::size_t TypedColumnBuf::AllocatedBytes() const noexcept {
  const auto values = std::visit(
      [](const auto &v) {
        auto bytes = v.AllocatedBytes();
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(v)>, Array<Payload>>)
          for (std::size_t i = 0; i < v.Capacity(); ++i)
            bytes = detail::SaturatingAdd(bytes, v[i].AllocatedBytes());
        return bytes;
      },
      values_);
  return detail::SaturatingAdd(values, null_mask_.AllocatedBytes());
}

std::size_t TypedColumnBuf::RowCapacity() const noexcept {
  return std::max(null_mask_.RowCapacity(), std::visit([](const auto &v) { return v.Capacity(); }, values_));
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
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(v)>, Array<Payload>>)
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
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(v)>, Array<Payload>>) {
          for (std::size_t i = v.Size(); i < v.Capacity(); ++i)
            v[i].Release();
          for (std::size_t i = 0; i < v.Size(); ++i)
            if (!v[i].ResizeCapacity(v[i].Size())) continue;
        }
        if (!v.ResizeCapacity(v.Size())) return;
      },
      values_);
  null_mask_.ShrinkToFit();
}

std::expected<TypedColumnBuf, ColumnBufError> TypedColumnBuf::Clone() const noexcept {
  if (auto error = SchemaError(semantic_type_, data_type_)) return std::unexpected(*error);
  if (!initialized_) return AllocationFailure();
  TypedColumnBuf clone(Name(), semantic_type_, data_type_);
  if (!clone.IsValid()) return AllocationFailure();
  const bool copied = std::visit(
      [&clone](const auto &source) {
        using A = std::remove_cvref_t<decltype(source)>;
        auto &target = std::get<A>(clone.values_);
        if (!target.ResizeCapacity(source.Capacity())) return false;
        const auto slots = std::is_same_v<A, Array<Payload>> ? source.Capacity() : source.Size();
        for (std::size_t i = 0; i < slots; ++i) {
          if constexpr (std::is_same_v<A, Array<Payload>>) {
            if (!target[i].ResizeCapacity(source[i].Capacity())) return false;
            if (source[i].Size() != 0) std::memcpy(target[i].Data(), source[i].Data(), source[i].Size());
            target[i].SetSize(source[i].Size());
          } else
            target[i] = source[i];
        }
        target.SetSize(source.Size());
        return true;
      },
      values_);
  if (!copied || !clone.null_mask_.bytes_.ResizeCapacity(null_mask_.bytes_.Capacity())) return AllocationFailure();
  for (auto byte : null_mask_.RawBytes())
    clone.null_mask_.bytes_.Append(byte);
  clone.null_mask_.row_count_ = null_mask_.row_count_;
  clone.null_mask_.null_count_ = null_mask_.null_count_;
  return clone;
}

std::expected<greptime::v1::Column, ColumnBufError> TypedColumnBuf::TryToProtoColumn() const {
  if (auto error = SchemaError(semantic_type_, data_type_)) return std::unexpected(*error);
  if (!initialized_) return AllocationFailure();
  return ToProtoColumn();
}

greptime::v1::Column TypedColumnBuf::ToProtoColumn() const {
  // Invalid allocation state cannot be encoded, including release builds.
  if (!IsValid()) std::abort();
  greptime::v1::Column column;
  const auto name = Name();
  column.set_column_name(name.empty() ? "" : name.data(), name.size());
  column.set_semantic_type(ToProto(semantic_type_));
  column.set_datatype(data_type_.ToProtoDatatype());
  if (auto extension = data_type_.ToProtoExtension()) *column.mutable_datatype_extension() = std::move(*extension);
  const auto mask = null_mask_.Bytes();
  if (!mask.empty()) column.set_null_mask(mask.data(), mask.size());
  auto &wire = *column.mutable_values();
  switch (data_type_.kind) {
  case Kind::kBoolean:
    EmitEach(std::get<Array<bool>>(values_), [&](bool value) { wire.add_bool_values(value); });
    break;
  case Kind::kInt8:
    EmitEach(std::get<Array<std::int8_t>>(values_), [&](std::int8_t value) { wire.add_i8_values(value); });
    break;
  case Kind::kInt16:
    EmitEach(std::get<Array<std::int16_t>>(values_), [&](std::int16_t value) { wire.add_i16_values(value); });
    break;
  case Kind::kInt32:
    EmitEach(std::get<Array<std::int32_t>>(values_), [&](std::int32_t value) { wire.add_i32_values(value); });
    break;
  case Kind::kInt64:
    EmitEach(std::get<Array<std::int64_t>>(values_), [&](std::int64_t value) { wire.add_i64_values(value); });
    break;
  case Kind::kUint8:
    EmitEach(std::get<Array<std::uint8_t>>(values_), [&](std::uint8_t value) { wire.add_u8_values(value); });
    break;
  case Kind::kUint16:
    EmitEach(std::get<Array<std::uint16_t>>(values_), [&](std::uint16_t value) { wire.add_u16_values(value); });
    break;
  case Kind::kUint32:
    EmitEach(std::get<Array<std::uint32_t>>(values_), [&](std::uint32_t value) { wire.add_u32_values(value); });
    break;
  case Kind::kUint64:
    EmitEach(std::get<Array<std::uint64_t>>(values_), [&](std::uint64_t value) { wire.add_u64_values(value); });
    break;
  case Kind::kFloat32:
    EmitEach(std::get<Array<float>>(values_), [&](float value) { wire.add_f32_values(value); });
    break;
  case Kind::kFloat64:
    EmitEach(std::get<Array<double>>(values_), [&](double value) { wire.add_f64_values(value); });
    break;
  case Kind::kDate:
    EmitEach(std::get<Array<std::int32_t>>(values_), [&](std::int32_t value) { wire.add_date_values(value); });
    break;
  case Kind::kDateTime:
    EmitEach(std::get<Array<std::int64_t>>(values_), [&](std::int64_t value) { wire.add_datetime_values(value); });
    break;
  case Kind::kTimestampSecond:
    EmitEach(std::get<Array<std::int64_t>>(values_),
             [&](std::int64_t value) { wire.add_timestamp_second_values(value); });
    break;
  case Kind::kTimestampMillisecond:
    EmitEach(std::get<Array<std::int64_t>>(values_),
             [&](std::int64_t value) { wire.add_timestamp_millisecond_values(value); });
    break;
  case Kind::kTimestampMicrosecond:
    EmitEach(std::get<Array<std::int64_t>>(values_),
             [&](std::int64_t value) { wire.add_timestamp_microsecond_values(value); });
    break;
  case Kind::kTimestampNanosecond:
    EmitEach(std::get<Array<std::int64_t>>(values_),
             [&](std::int64_t value) { wire.add_timestamp_nanosecond_values(value); });
    break;
  case Kind::kString:
  case Kind::kBinary:
  case Kind::kJson: {
    const auto &v = std::get<Array<Payload>>(values_);
    for (std::size_t i = 0; i < v.Size(); ++i) {
      const auto *data = v[i].Size() == 0 ? "" : BytesAsChar(v[i].Data());
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
  return column;
}

} // namespace greptimedb
