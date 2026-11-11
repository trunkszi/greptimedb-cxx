#include "ipc_codec_internal.hh"

#include "ipc_views.hh"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <string_view>
#include <uarch/uarch.hpp>

namespace greptimedb::flight::codec {
using detail::Compress;
using FD = google::protobuf::FieldDescriptor;

[[nodiscard, gnu::cold, gnu::noinline]] std::unexpected<std::string> Fail(const char *why) {
  uarch::cold_path();
  return std::unexpected(std::string(why));
}
std::string_view Meta(const ipc::Metadata &m, std::string_view key) {
  for (const auto &[k, v] : m)
    if (k == key) return v;
  return {};
}
bool NullAt(const Column &c, size_t row) {
  return row / 8 < c.null_mask().size() && (static_cast<uint8_t>(c.null_mask()[row / 8]) & (1U << (row % 8)));
}
template <class U> inline void StoreLe(uint8_t *dst, U value) noexcept {
  if constexpr (sizeof(U) > 1 && std::endian::native != std::endian::little) value = std::byteswap(value);
  std::memcpy(dst, &value, sizeof(value));
}
template <class U> inline U LoadLe(const uint8_t *src) noexcept {
  U value{};
  std::memcpy(&value, src, sizeof(value));
  if constexpr (sizeof(U) > 1 && std::endian::native != std::endian::little) value = std::byteswap(value);
  return value;
}
void Put(Bytes &b, uint64_t value, size_t width) {
  const size_t at = b.size();
  b.resize(at + width);
  if (width == 0) return;
  auto *dst = b.data() + at;
  switch (width) {
  case 2: StoreLe(dst, static_cast<uint16_t>(value)); return;
  case 4: StoreLe(dst, static_cast<uint32_t>(value)); return;
  case 8: StoreLe(dst, value); return;
  default: dst[0] = static_cast<uint8_t>(value); return;
  }
}
uint64_t Read(std::span<const uint8_t> b, size_t pos, size_t width) {
  switch (width) {
  case 2: return LoadLe<uint16_t>(b.data() + pos);
  case 4: return LoadLe<uint32_t>(b.data() + pos);
  case 8: return LoadLe<uint64_t>(b.data() + pos);
  default: return width == 0 ? 0 : b[pos];
  }
}

FlightResult<Layout> GetLayout(const FlightField &f) {
  using enum ipc::TypeId;
  const auto &t = f.type;
  switch (t.id) {
  case kNull: return Layout{11, "binary_values", 0};
  case kBool: return Layout{0, "bool_values", 0};
  case kInt: {
    if (t.bit_width != 8 && t.bit_width != 16 && t.bit_width != 32 && t.bit_width != 64)
      return Fail("invalid integer bit width");
    static constexpr std::array<const char *, 8> kIntegerValueNames{
        "i8_values", "i16_values", "i32_values", "i64_values", "u8_values", "u16_values", "u32_values", "u64_values"};
    const int index = t.bit_width == 8 ? 0 : t.bit_width == 16 ? 1 : t.bit_width == 32 ? 2 : 3;
    return Layout{1 + index + (t.is_signed ? 0 : 4), kIntegerValueNames[index + (t.is_signed ? 0 : 4)],
                  static_cast<size_t>(t.bit_width / 8)};
  }
  case kFloatingPoint:
    if (t.precision == 1) return Layout{9, "f32_values", 4};
    if (t.precision == 2) return Layout{10, "f64_values", 8};
    return Fail("unsupported floating precision");
  case kBinary:
  case kLargeBinary:
    return Layout{Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? 31 : 11, "binary_values",
                  t.id == kBinary ? 4U : 8U, true};
  case kBinaryView:
    return Layout{Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? 31 : 11, "binary_values", 16, false, true};
  case kUtf8View:
    return Layout{Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? 31 : 12,
                  Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? "binary_values" : "string_values", 16, false,
                  true};
  case kUtf8:
  case kLargeUtf8: return Layout{12, "string_values", t.id == kUtf8 ? 4U : 8U, true};
  case kDate:
    if (t.unit == 0) return Layout{13, "date_values", 4};
    if (t.unit == 1) return Layout{14, "datetime_values", 8};
    return Fail("unsupported date unit");
  case kTimestamp: {
    if (t.unit < 0 || t.unit > 3 || !t.timezone.empty()) return Fail("invalid timestamp unit/timezone");
    static constexpr std::array<const char *, 4> kTimestampValueNames{
        "timestamp_second_values", "timestamp_millisecond_values", "timestamp_microsecond_values",
        "timestamp_nanosecond_values"};
    return Layout{15 + t.unit, kTimestampValueNames[static_cast<size_t>(t.unit)], 8};
  }
  case kDecimal:
    if ((t.bit_width != 0 && t.bit_width != 128) || t.decimal_precision < 1 || t.decimal_precision > 38 ||
        t.scale < 0 || t.scale > t.decimal_precision)
      return Fail("invalid decimal128 precision/scale");
    return Layout{30, "decimal128_values", 16};
  }
  return Fail("unsupported Arrow type");
}
FlightResult<int> Semantic(const FlightField &f) {
  const auto role = Meta(f.metadata, kSemanticTypeKey);
  int semantic = 1;
  if (role == "tag")
    semantic = 0;
  else if (role == "timestamp")
    semantic = 2;
  else if (!role.empty() && role != "field")
    return Fail("invalid semantic type metadata");
  if (role.empty() && Meta(f.metadata, kTimeIndexKey) == "true") semantic = 2;
  const auto time_index = Meta(f.metadata, kTimeIndexKey);
  if (!time_index.empty() && time_index != "true" && time_index != "false") return Fail("invalid time index metadata");
  if (semantic == 2 && (f.nullable || f.type.id != ipc::TypeId::kTimestamp || !f.type.timezone.empty()))
    return Fail("time index must be a non-null timezone-free timestamp");
  return semantic;
}
FlightResult<void> ValidateSchema(const FlightSchema &schema) {
  if (!schema.little_endian || schema.fields.size() > kMaxFields)
    return Fail("schema exceeds policy or endian unsupported");
  for (size_t i = 0; i < schema.fields.size(); ++i) {
    const auto &f = schema.fields[i];
    if (f.name.empty()) return Fail("column name must be nonempty");
    for (size_t j = 0; j < i; ++j)
      if (schema.fields[j].name == f.name) return Fail("duplicate column name");
    auto layout = GetLayout(f);
    if (!layout) return std::unexpected(layout.error());
    auto semantic = Semantic(f);
    if (!semantic) return std::unexpected(semantic.error());
  }
  return {};
}
FlightResult<FlightField> FieldFromColumn(const Column &c) {
  FlightField f;
  f.name = c.column_name();
  if (c.semantic_type() < 0 || c.semantic_type() > 2) return Fail("invalid proto semantic type");
  f.nullable = c.semantic_type() != 2;
  f.metadata.emplace_back(kSemanticTypeKey, c.semantic_type() == 2   ? "timestamp"
                                            : c.semantic_type() == 0 ? "tag"
                                                                     : "field");
  auto &t = f.type;
  const int d = c.datatype();
  if (d >= 1 && d <= 8) {
    t.id = ipc::TypeId::kInt;
    t.is_signed = d <= 4;
    t.bit_width = 8 << ((d - 1) % 4);
  } else if (d == 0)
    t.id = ipc::TypeId::kBool;
  else if (d == 9 || d == 10) {
    t.id = ipc::TypeId::kFloatingPoint;
    t.precision = d == 9 ? 1 : 2;
  } else if (d == 11 || d == 31)
    t.id = ipc::TypeId::kBinary;
  else if (d == 12)
    t.id = ipc::TypeId::kUtf8;
  else if (d == 13 || d == 14) {
    t.id = ipc::TypeId::kDate;
    t.unit = d - 13;
  } else if (d >= 15 && d <= 18) {
    t.id = ipc::TypeId::kTimestamp;
    t.unit = d - 15;
  } else if (d == 30 && c.datatype_extension().has_decimal_type()) {
    t.id = ipc::TypeId::kDecimal;
    t.bit_width = 128;
    t.decimal_precision = c.datatype_extension().decimal_type().precision();
    t.scale = c.datatype_extension().decimal_type().scale();
  } else
    return Fail("unsupported proto datatype or missing decimal extension");
  const auto &ext = c.datatype_extension();
  const bool json = d == 31 || ext.has_json_type() || ext.has_json_native_type();
  if (json && d != 11 && d != 31) return Fail("JSON requires binary physical type");
  if (json) f.metadata.emplace_back(kTypeExtensionKey, kJsonTypeValue);
  return f;
}
const FD *ValueField(const Layout &l) { return Values::descriptor()->FindFieldByName(l.values); }
FlightResult<size_t> ValidateColumn(const Column &c, const FlightField &f, size_t rows, const Layout &l) {
  auto inferred = FieldFromColumn(c);
  if (!inferred) return std::unexpected(inferred.error());
  auto inferred_layout = GetLayout(*inferred);
  auto semantic = Semantic(f);
  if (!inferred_layout || !semantic || c.column_name() != f.name ||
      (f.type.id != ipc::TypeId::kNull && inferred_layout->datatype != l.datatype) || c.semantic_type() != *semantic)
    return Fail("column/schema mismatch");
  if (l.datatype == 30 &&
      (inferred->type.decimal_precision != f.type.decimal_precision || inferred->type.scale != f.type.scale))
    return Fail("decimal column/schema mismatch");
  if (c.null_mask().size() > (rows + 7) / 8) return Fail("excessive null mask length");
  size_t nulls = 0;
  for (size_t i = 0; i < rows; ++i)
    nulls += NullAt(c, i);
  for (size_t i = rows; i < c.null_mask().size() * 8; ++i)
    if (NullAt(c, i)) return Fail("null mask contains bits beyond row count");
  if (!f.nullable && nulls) return Fail("null in non-nullable field");
  const auto *field = ValueField(l);
  const auto *r = c.values().GetReflection();
  const size_t count = r->FieldSize(c.values(), field);
  std::vector<const FD *> populated;
  r->ListFields(c.values(), &populated);
  for (const auto *other : populated)
    if (other != field) return Fail("unexpected proto value array");
  if (count != rows - nulls || (f.type.id == ipc::TypeId::kNull && nulls != rows))
    return Fail("packed value count differs from non-null row count");
  if (l.datatype == 1 || l.datatype == 2) {
    const int64_t limit = int64_t{1} << (l.width * 8 - 1);
    for (size_t i = 0; i < count; ++i) {
      const int64_t v = r->GetRepeatedInt32(c.values(), field, static_cast<int>(i));
      if (v < -limit || v >= limit) return Fail("narrow signed value overflow");
    }
  }
  if (l.datatype == 5 || l.datatype == 6) {
    const uint64_t limit = uint64_t{1} << (l.width * 8);
    for (size_t i = 0; i < count; ++i)
      if (r->GetRepeatedUInt32(c.values(), field, static_cast<int>(i)) >= limit)
        return Fail("narrow unsigned value overflow");
  }
  if (l.variable || l.view) {
    size_t bytes = 0;
    for (size_t i = 0; i < count; ++i) {
      const auto &value = l.datatype == 12 ? c.values().string_values(static_cast<int>(i))
                                           : c.values().binary_values(static_cast<int>(i));
      if (value.size() > kMaxBody - bytes) return Fail("column data exceeds size policy");
      bytes += value.size();
      if ((l.datatype == 12 || f.type.id == ipc::TypeId::kUtf8View) && !ValidUtf8(value))
        return Fail("invalid UTF-8 value");
    }
  }
  return nulls;
}
FlightResult<void> ValidateBatch(const RecordBatch &batch) {
  auto schema = ValidateSchema(batch.schema);
  if (!schema) return schema;
  if (batch.row_count > kMaxRows || batch.columns.size() != batch.schema.fields.size() ||
      (batch.row_count && batch.columns.empty()) ||
      (batch.row_count && batch.columns.size() > kMaxCells / batch.row_count))
    return Fail("invalid batch shape");
  for (size_t i = 0; i < batch.columns.size(); ++i) {
    auto layout = GetLayout(batch.schema.fields[i]);
    if (!layout) return std::unexpected(layout.error());
    auto count = ValidateColumn(batch.columns[i], batch.schema.fields[i], batch.row_count, *layout);
    if (!count) return std::unexpected(count.error());
  }
  return {};
}
FlightResult<void> AppendNative(Bytes &out, const Values &values, const Layout &l, size_t index) {
  const auto *field = ValueField(l);
  const auto *r = values.GetReflection();
  const int i = static_cast<int>(index);
  uint64_t bits = 0;
  switch (field->cpp_type()) {
  case FD::CPPTYPE_INT32: {
    const int64_t v = r->GetRepeatedInt32(values, field, i);
    if (l.width < 4 && (v < -(int64_t{1} << (l.width * 8 - 1)) || v >= (int64_t{1} << (l.width * 8 - 1))))
      return Fail("narrow signed value overflow");
    bits = static_cast<uint64_t>(v);
    break;
  }
  case FD::CPPTYPE_INT64: bits = static_cast<uint64_t>(r->GetRepeatedInt64(values, field, i)); break;
  case FD::CPPTYPE_UINT32:
    bits = r->GetRepeatedUInt32(values, field, i);
    if (l.width < 4 && bits >= (uint64_t{1} << (l.width * 8))) return Fail("narrow unsigned value overflow");
    break;
  case FD::CPPTYPE_UINT64: bits = r->GetRepeatedUInt64(values, field, i); break;
  case FD::CPPTYPE_FLOAT: bits = std::bit_cast<uint32_t>(r->GetRepeatedFloat(values, field, i)); break;
  case FD::CPPTYPE_DOUBLE: bits = std::bit_cast<uint64_t>(r->GetRepeatedDouble(values, field, i)); break;
  case FD::CPPTYPE_MESSAGE: {
    const auto &v = values.decimal128_values(i);
    Put(out, static_cast<uint64_t>(v.lo()), 8);
    Put(out, static_cast<uint64_t>(v.hi()), 8);
    return {};
  }
  case FD::CPPTYPE_BOOL:
  case FD::CPPTYPE_ENUM:
  case FD::CPPTYPE_STRING: return Fail("unexpected primitive value type");
  }
  Put(out, bits, l.width);
  return {};
}
void AppendDecoded(Values &values, const Layout &l, std::span<const uint8_t> data, size_t pos) {
  const auto *f = ValueField(l);
  const auto *r = values.GetReflection();
  const auto bits = Read(data, pos, std::min(l.width, size_t{8}));
  int64_t signed_bits = std::bit_cast<int64_t>(bits);
  // Zero-width values have no sign bit to extend.
  if (l.width > 0 && l.width < 8) {
    const auto sign_shift = l.width * 8 - 1;
    if ((bits & (uint64_t{1} << sign_shift)) != 0)
      signed_bits = std::bit_cast<int64_t>(bits | (~uint64_t{0} << (sign_shift + 1)));
  }
  switch (f->cpp_type()) {
  case FD::CPPTYPE_INT32: r->AddInt32(&values, f, static_cast<int32_t>(signed_bits)); break;
  case FD::CPPTYPE_INT64: r->AddInt64(&values, f, signed_bits); break;
  case FD::CPPTYPE_UINT32: r->AddUInt32(&values, f, static_cast<uint32_t>(bits)); break;
  case FD::CPPTYPE_UINT64: r->AddUInt64(&values, f, bits); break;
  case FD::CPPTYPE_FLOAT: r->AddFloat(&values, f, std::bit_cast<float>(static_cast<uint32_t>(bits))); break;
  case FD::CPPTYPE_DOUBLE: r->AddDouble(&values, f, std::bit_cast<double>(bits)); break;
  case FD::CPPTYPE_MESSAGE: {
    auto *d = values.add_decimal128_values();
    d->set_lo(std::bit_cast<int64_t>(bits));
    d->set_hi(std::bit_cast<int64_t>(Read(data, pos + 8, 8)));
    break;
  }
  case FD::CPPTYPE_BOOL:
  case FD::CPPTYPE_ENUM:
  case FD::CPPTYPE_STRING: break;
  }
}
FlightResult<void> AddBuffer(Bytes &body, ipc::Batch &meta, const Bytes &raw, FlightCompression codec) {
  auto encoded = Compress(raw, codec);
  if (!encoded) return std::unexpected(encoded.error());
  if (encoded->size() > kMaxBody - body.size() || body.size() + encoded->size() > kMaxBody - 7)
    return Fail("IPC body exceeds size policy");
  meta.buffers.push_back({static_cast<int64_t>(body.size()), static_cast<int64_t>(encoded->size())});
  body.insert(body.end(), encoded->begin(), encoded->end());
  body.resize((body.size() + 7) & ~size_t{7});
  return {};
}
FlightResult<void> EncodeColumn(const Column &c, const FlightField &f, size_t rows, Bytes &body, ipc::Batch &meta,
                                FlightCompression codec) {
  auto layout = GetLayout(f);
  if (!layout) return std::unexpected(layout.error());
  const auto &l = *layout;
  auto nulls = ValidateColumn(c, f, rows, l);
  if (!nulls) return std::unexpected(nulls.error());
  meta.nodes.push_back({static_cast<int64_t>(rows), static_cast<int64_t>(*nulls)});
  if (f.type.id == ipc::TypeId::kNull) return {};
  Bytes valid(*nulls ? (rows + 7) / 8 : 0);
  if (*nulls)
    for (size_t i = 0; i < rows; ++i)
      if (!NullAt(c, i)) valid[i / 8] |= 1U << (i % 8);
  auto added = AddBuffer(body, meta, valid, codec);
  if (!added) return added;
  if (l.view) {
    auto views = detail::EncodeViews(c, rows, l.datatype == 12);
    if (!views) return std::unexpected(views.error());
    meta.variadic_buffer_counts.push_back(1);
    auto stored = AddBuffer(body, meta, views->views, codec);
    if (!stored) return stored;
    return AddBuffer(body, meta, views->data, codec);
  }
  Bytes data, offsets;
  if (l.variable)
    Put(offsets, 0, l.width);
  else if (l.datatype == 0)
    data.resize((rows + 7) / 8);
  else if (rows > kMaxBody / l.width)
    return Fail("column exceeds size policy");
  else
    data.reserve(rows * l.width);
  size_t packed = 0;
  for (size_t row = 0; row < rows; ++row) {
    const bool is_null = NullAt(c, row);
    if (l.variable) {
      if (!is_null) {
        const auto &str = l.datatype == 12 ? c.values().string_values(static_cast<int>(packed))
                                           : c.values().binary_values(static_cast<int>(packed));
        if (uarch::unlikely(l.datatype == 12 && !ValidUtf8(str))) return Fail("invalid UTF-8 value");
        if (uarch::unlikely(str.size() > kMaxBody - data.size())) return Fail("binary data exceeds size policy");
        data.insert(data.end(), str.begin(), str.end());
      }
      Put(offsets, data.size(), l.width);
    } else if (l.datatype == 0) {
      if (!is_null && c.values().bool_values(static_cast<int>(packed))) data[row / 8] |= 1U << (row % 8);
    } else if (is_null)
      data.resize(data.size() + l.width, 0);
    else {
      auto written = AppendNative(data, c.values(), l, packed);
      if (!written) return written;
    }
    packed += !is_null;
  }
  if (l.variable) {
    added = AddBuffer(body, meta, offsets, codec);
    if (!added) return added;
  }
  return AddBuffer(body, meta, data, codec);
}
FlightResult<size_t> TimeColumn(const RecordBatch &batch) {
  size_t found = batch.schema.fields.size();
  for (size_t i = 0; i < batch.schema.fields.size(); ++i) {
    auto role = Semantic(batch.schema.fields[i]);
    if (!role) return std::unexpected(role.error());
    if (*role == 2) {
      if (found != batch.schema.fields.size()) return Fail("multiple timestamp columns");
      found = i;
    }
  }
  if (found == batch.schema.fields.size()) return Fail("missing timestamp column");
  return found;
}
} // namespace greptimedb::flight::codec
