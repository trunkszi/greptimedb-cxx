#include <greptimedb/flight/ipc_codec.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include "ipc_compression.h"
#include "ipc_views.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <map>
#include <string_view>

namespace greptimedb::flight {
namespace {
using Column = ::greptime::v1::Column;
using Values = ::greptime::v1::Column::Values;
using FD = google::protobuf::FieldDescriptor;
using Bytes = std::vector<uint8_t>;
constexpr size_t kMaxBody = detail::kMaxBody;
using detail::Compress;
using detail::DecodedBuffer;
using detail::Decompress;
constexpr size_t kMaxRows = 16U * 1024U * 1024U;
constexpr size_t kMaxFields = 4096;
constexpr size_t kMaxCells = 16U * 1024U * 1024U;
constexpr size_t kMaxPartitions = 4096;
std::unexpected<std::string> Fail(const char *why) { return std::unexpected(std::string(why)); }
std::string_view Meta(const ipc::Metadata &m, std::string_view key) {
  for (const auto &[k, v] : m)
    if (k == key) return v;
  return {};
}
bool NullAt(const Column &c, size_t row) {
  return row / 8 < c.null_mask().size() && (static_cast<uint8_t>(c.null_mask()[row / 8]) & (1U << (row % 8)));
}
void Put(Bytes &b, uint64_t value, size_t width) {
  for (size_t j = 0; j < width; ++j) b.push_back(static_cast<uint8_t>(value >> (8 * j)));
}
uint64_t Read(std::span<const uint8_t> b, size_t pos, size_t width) {
  uint64_t v = 0;
  for (size_t j = 0; j < width; ++j) v |= uint64_t{b[pos + j]} << (8 * j);
  return v;
}
struct Layout {
  int datatype;
  const char *values;
  size_t width;
  bool variable = false;
  bool view = false;
};
FlightResult<Layout> GetLayout(const FlightField &f) {
  using enum ipc::TypeId;
  const auto &t = f.type;
  switch (t.id) {
  case kNull:
    return Layout{11, "binary_values", 0};
  case kBool:
    return Layout{0, "bool_values", 0};
  case kInt: {
    if (t.bit_width != 8 && t.bit_width != 16 && t.bit_width != 32 && t.bit_width != 64)
      return Fail("invalid integer bit width");
    static constexpr const char *names[] = {"i8_values", "i16_values", "i32_values", "i64_values",
                                            "u8_values", "u16_values", "u32_values", "u64_values"};
    const int index = t.bit_width == 8 ? 0 : t.bit_width == 16 ? 1 : t.bit_width == 32 ? 2 : 3;
    return Layout{1 + index + (t.is_signed ? 0 : 4), names[index + (t.is_signed ? 0 : 4)],
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
    return Layout{Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? 31 : 11,
                  "binary_values", 16, false, true};
  case kUtf8View:
    return Layout{Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? 31 : 12,
                  Meta(f.metadata, kTypeExtensionKey) == kJsonTypeValue ? "binary_values" : "string_values",
                  16, false, true};
  case kUtf8:
  case kLargeUtf8:
    return Layout{12, "string_values", t.id == kUtf8 ? 4U : 8U, true};
  case kDate:
    if (t.unit == 0) return Layout{13, "date_values", 4};
    if (t.unit == 1) return Layout{14, "datetime_values", 8};
    return Fail("unsupported date unit");
  case kTimestamp: {
    if (t.unit < 0 || t.unit > 3 || !t.timezone.empty()) return Fail("invalid timestamp unit/timezone");
    static constexpr const char *names[] = {"timestamp_second_values", "timestamp_millisecond_values",
                                            "timestamp_microsecond_values", "timestamp_nanosecond_values"};
    return Layout{15 + t.unit, names[t.unit], 8};
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
  if (!time_index.empty() && time_index != "true" && time_index != "false")
    return Fail("invalid time index metadata");
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
bool ValidUtf8(std::string_view s);
FlightResult<size_t> ValidateColumn(const Column &c, const FlightField &f, size_t rows, const Layout &l) {
  auto inferred = FieldFromColumn(c);
  if (!inferred) return std::unexpected(inferred.error());
  auto inferred_layout = GetLayout(*inferred);
  auto semantic = Semantic(f);
  if (!inferred_layout || !semantic || c.column_name() != f.name ||
      (f.type.id != ipc::TypeId::kNull && inferred_layout->datatype != l.datatype) ||
      c.semantic_type() != *semantic)
    return Fail("column/schema mismatch");
  if (l.datatype == 30 &&
      (inferred->type.decimal_precision != f.type.decimal_precision || inferred->type.scale != f.type.scale))
    return Fail("decimal column/schema mismatch");
  if (c.null_mask().size() > (rows + 7) / 8) return Fail("excessive null mask length");
  size_t nulls = 0;
  for (size_t i = 0; i < rows; ++i) nulls += NullAt(c, i);
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
bool ValidUtf8(std::string_view s) {
  size_t i = 0;
  while (i < s.size()) {
    const uint8_t a = static_cast<uint8_t>(s[i++]);
    if (a < 0x80) continue;
    unsigned n;
    uint32_t cp;
    if (a >= 0xc2 && a <= 0xdf) {
      n = 1;
      cp = a & 0x1f;
    } else if (a >= 0xe0 && a <= 0xef) {
      n = 2;
      cp = a & 0x0f;
    } else if (a >= 0xf0 && a <= 0xf4) {
      n = 3;
      cp = a & 7;
    } else
      return false;
    if (s.size() - i < n) return false;
    for (unsigned j = 0; j < n; ++j) {
      const auto b = static_cast<uint8_t>(s[i++]);
      if ((b & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (b & 0x3f);
    }
    if ((n == 1 && cp < 0x80) || (n == 2 && cp < 0x800) || (n == 3 && cp < 0x10000) || cp > 0x10ffff ||
        (cp >= 0xd800 && cp <= 0xdfff))
      return false;
  }
  return true;
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
  case FD::CPPTYPE_INT64:
    bits = static_cast<uint64_t>(r->GetRepeatedInt64(values, field, i));
    break;
  case FD::CPPTYPE_UINT32:
    bits = r->GetRepeatedUInt32(values, field, i);
    if (l.width < 4 && bits >= (uint64_t{1} << (l.width * 8))) return Fail("narrow unsigned value overflow");
    break;
  case FD::CPPTYPE_UINT64:
    bits = r->GetRepeatedUInt64(values, field, i);
    break;
  case FD::CPPTYPE_FLOAT:
    bits = std::bit_cast<uint32_t>(r->GetRepeatedFloat(values, field, i));
    break;
  case FD::CPPTYPE_DOUBLE:
    bits = std::bit_cast<uint64_t>(r->GetRepeatedDouble(values, field, i));
    break;
  case FD::CPPTYPE_MESSAGE: {
    const auto &v = values.decimal128_values(i);
    Put(out, static_cast<uint64_t>(v.lo()), 8);
    Put(out, static_cast<uint64_t>(v.hi()), 8);
    return {};
  }
  case FD::CPPTYPE_BOOL:
  case FD::CPPTYPE_ENUM:
  case FD::CPPTYPE_STRING:
    return Fail("unexpected primitive value type");
  }
  Put(out, bits, l.width);
  return {};
}
void AppendDecoded(Values &values, const Layout &l, std::span<const uint8_t> data, size_t pos) {
  const auto *f = ValueField(l);
  const auto *r = values.GetReflection();
  const auto bits = Read(data, pos, std::min(l.width, size_t{8}));
  int64_t signed_bits = std::bit_cast<int64_t>(bits);
  if (l.width < 8 && (bits & (uint64_t{1} << (l.width * 8 - 1))))
    signed_bits = std::bit_cast<int64_t>(bits | (~uint64_t{0} << (l.width * 8)));
  switch (f->cpp_type()) {
  case FD::CPPTYPE_INT32:
    r->AddInt32(&values, f, static_cast<int32_t>(signed_bits));
    break;
  case FD::CPPTYPE_INT64:
    r->AddInt64(&values, f, signed_bits);
    break;
  case FD::CPPTYPE_UINT32:
    r->AddUInt32(&values, f, static_cast<uint32_t>(bits));
    break;
  case FD::CPPTYPE_UINT64:
    r->AddUInt64(&values, f, bits);
    break;
  case FD::CPPTYPE_FLOAT:
    r->AddFloat(&values, f, std::bit_cast<float>(static_cast<uint32_t>(bits)));
    break;
  case FD::CPPTYPE_DOUBLE:
    r->AddDouble(&values, f, std::bit_cast<double>(bits));
    break;
  case FD::CPPTYPE_MESSAGE: {
    auto *d = values.add_decimal128_values();
    d->set_lo(std::bit_cast<int64_t>(bits));
    d->set_hi(std::bit_cast<int64_t>(Read(data, pos + 8, 8)));
    break;
  }
  case FD::CPPTYPE_BOOL:
  case FD::CPPTYPE_ENUM:
  case FD::CPPTYPE_STRING:
    break;
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
FlightResult<void> EncodeColumn(const Column &c, const FlightField &f, size_t rows, Bytes &body,
                                ipc::Batch &meta, FlightCompression codec) {
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
        if (l.datatype == 12 && !ValidUtf8(str)) return Fail("invalid UTF-8 value");
        if (str.size() > kMaxBody - data.size()) return Fail("binary data exceeds size policy");
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
} // namespace

FlightResult<RecordBatch> RecordBatchFromProtoColumns(std::span<const Column> columns, size_t rows) {
  if (rows > kMaxRows || columns.size() > kMaxFields || (rows && columns.empty()) ||
      (rows && columns.size() > kMaxCells / rows))
    return Fail("batch exceeds size policy or has no columns");
  RecordBatch result;
  result.row_count = rows;
  size_t proto_bytes = 0;
  for (const auto &c : columns) {
    const size_t bytes = c.ByteSizeLong();
    if (bytes > kMaxBody - proto_bytes) return Fail("proto batch exceeds size policy");
    proto_bytes += bytes;
    auto field = FieldFromColumn(c);
    if (!field) return std::unexpected(field.error());
    auto layout = GetLayout(*field);
    if (!layout) return std::unexpected(layout.error());
    auto count = ValidateColumn(c, *field, rows, *layout);
    if (!count) return std::unexpected(count.error());
    result.schema.fields.push_back(std::move(*field));
    result.columns.push_back(c);
  }
  auto valid = ValidateSchema(result.schema);
  if (!valid) return std::unexpected(valid.error());
  return result;
}
FlightResult<EncodedFlightBody> FlightIpcEncoder::EncodeSchema(const FlightSchema &schema) const {
  auto valid = ValidateSchema(schema);
  if (!valid) return std::unexpected(valid.error());
  auto header = ipc::EncodeSchema(schema);
  if (!header) return std::unexpected(header.error());
  return EncodedFlightBody{std::move(*header), {}};
}
FlightResult<EncodedFlightBody> FlightIpcEncoder::EncodeBatch(const RecordBatch &batch) const {
  if (compression_ != FlightCompression::kNone && compression_ != FlightCompression::kLz4Frame &&
      compression_ != FlightCompression::kZstd)
    return Fail("unsupported Flight compression");
  auto valid = ValidateSchema(batch.schema);
  if (!valid) return std::unexpected(valid.error());
  if (batch.row_count > kMaxRows || batch.columns.size() != batch.schema.fields.size() ||
      (batch.row_count && batch.columns.empty()) ||
      (batch.row_count && batch.columns.size() > kMaxCells / batch.row_count))
    return Fail("invalid batch shape");
  ipc::Batch metadata;
  metadata.length = static_cast<int64_t>(batch.row_count);
  switch (compression_) {
  case FlightCompression::kNone:
    metadata.compression = ipc::Compression::kNone;
    break;
  case FlightCompression::kLz4Frame:
    metadata.compression = ipc::Compression::kLz4Frame;
    break;
  case FlightCompression::kZstd:
    metadata.compression = ipc::Compression::kZstd;
    break;
  }
  Bytes body;
  for (size_t i = 0; i < batch.columns.size(); ++i) {
    auto encoded =
        EncodeColumn(batch.columns[i], batch.schema.fields[i], batch.row_count, body, metadata, compression_);
    if (!encoded) return std::unexpected(encoded.error());
  }
  auto header = ipc::EncodeRecordBatch(metadata, static_cast<int64_t>(body.size()));
  if (!header) return std::unexpected(header.error());
  return EncodedFlightBody{std::move(*header), std::move(body)};
}
FlightResult<EncodedFlightBody> EncodeMetadataMessage() {
  auto header = ipc::EncodeMetadataMessage();
  if (!header) return std::unexpected(header.error());
  return EncodedFlightBody{std::move(*header), {}};
}
FlightResult<bool> IsMetadataOnlyMessage(std::span<const uint8_t> header) {
  return ipc::IsMetadataOnlyMessage(header);
}
FlightResult<FlightSchema> DecodeFlightSchema(std::span<const uint8_t> header) {
  auto message = ipc::DecodeMessage(header);
  if (!message) return std::unexpected(message.error());
  if (message->header_type != ipc::HeaderType::kSchema || message->body_length != 0)
    return Fail("expected schema frame");
  auto valid = ValidateSchema(message->schema);
  if (!valid) return std::unexpected(valid.error());
  return std::move(message->schema);
}
FlightResult<RecordBatch> DecodeFlightRecordBatch(const FlightSchema &schema, std::span<const uint8_t> header,
                                                  std::span<const uint8_t> body) {
  auto valid = ValidateSchema(schema);
  if (!valid) return std::unexpected(valid.error());
  auto message = ipc::DecodeMessage(header);
  if (!message) return std::unexpected(message.error());
  if (message->header_type != ipc::HeaderType::kRecordBatch)
    return Fail("expected record batch, dictionaries unsupported");
  const auto &b = message->batch;
  if (body.size() > kMaxBody || message->body_length < 0 ||
      static_cast<uint64_t>(message->body_length) != body.size() || b.length < 0 ||
      static_cast<uint64_t>(b.length) > kMaxRows || b.nodes.size() != schema.fields.size())
    return Fail("body length, row count or node count mismatch");
  RecordBatch result{schema, {}, static_cast<size_t>(b.length)};
  if (result.row_count && (schema.fields.empty() || schema.fields.size() > kMaxCells / result.row_count))
    return Fail("batch logical cell count exceeds policy");
  size_t next = 0, budget = kMaxBody, previous_end = 0, next_variadic = 0, materialized = 0;
  auto buffer = [&](size_t limit = kMaxBody) -> FlightResult<DecodedBuffer> {
    if (next >= b.buffers.size()) return Fail("missing body buffer");
    const auto item = b.buffers[next++];
    if (item.offset < 0 || item.length < 0 || static_cast<uint64_t>(item.offset) > body.size() ||
        static_cast<uint64_t>(item.length) > body.size() - static_cast<size_t>(item.offset) ||
        static_cast<uint64_t>(item.offset) < previous_end || item.offset % 8 != 0)
      return Fail("invalid body buffer range");
    previous_end = static_cast<size_t>(item.offset + item.length);
    return Decompress(body.subspan(static_cast<size_t>(item.offset), static_cast<size_t>(item.length)),
                      b.compression, budget, limit);
  };
  for (size_t i = 0; i < schema.fields.size(); ++i) {
    const auto &f = schema.fields[i];
    auto layout = GetLayout(f);
    if (!layout) return std::unexpected(layout.error());
    const auto &l = *layout;
    const auto node = b.nodes[i];
    if (node.length != b.length || node.null_count < 0 || node.null_count > b.length)
      return Fail("invalid field node");
    auto semantic = Semantic(f);
    if (!semantic) return std::unexpected(semantic.error());
    Column c;
    c.set_column_name(f.name);
    c.set_datatype(static_cast<::greptime::v1::ColumnDataType>(l.datatype));
    c.set_semantic_type(static_cast<::greptime::v1::SemanticType>(*semantic));
    if (l.datatype == 30) {
      auto *ext = c.mutable_datatype_extension()->mutable_decimal_type();
      ext->set_precision(f.type.decimal_precision);
      ext->set_scale(f.type.scale);
    }
    if (l.datatype == 31)
      c.mutable_datatype_extension()->mutable_json_native_type()->set_datatype(::greptime::v1::BINARY);
    auto *values = c.mutable_values();
    size_t nulls = 0;
    if (f.type.id == ipc::TypeId::kNull) {
      if (node.null_count != b.length || !f.nullable) return Fail("invalid null field node");
      std::string mask((result.row_count + 7) / 8, '\0');
      for (size_t row = 0; row < result.row_count; ++row) mask[row / 8] |= static_cast<char>(1U << (row % 8));
      c.set_null_mask(std::move(mask));
      result.columns.push_back(std::move(c));
      continue;
    }
    auto validity = buffer(((result.row_count + 7) / 8 + 7) & ~size_t{7});
    if (!validity) return std::unexpected(validity.error());
    if (!validity->empty() && validity->size() < (result.row_count + 7) / 8)
      return Fail("short validity bitmap");
    if (node.null_count && validity->empty()) return Fail("missing validity bitmap for nulls");
    DecodedBuffer offsets;
    if (l.variable) {
      auto read = buffer(((result.row_count + 1) * l.width + 7) & ~size_t{7});
      if (!read) return std::unexpected(read.error());
      offsets = std::move(*read);
    }
    const size_t fixed_size = l.datatype == 0 ? (result.row_count + 7) / 8 : result.row_count * l.width;
    auto data = buffer(l.variable ? kMaxBody : (fixed_size + 7) & ~size_t{7});
    if (!data) return std::unexpected(data.error());
    std::vector<DecodedBuffer> variadic;
    if (l.view) {
      if (next_variadic >= b.variadic_buffer_counts.size()) return Fail("missing variadic buffer count");
      const auto count = b.variadic_buffer_counts[next_variadic++];
      if (count < 0 || static_cast<uint64_t>(count) > b.buffers.size() - next)
        return Fail("invalid variadic buffer count");
      for (int64_t j = 0; j < count; ++j) {
        auto payload = buffer();
        if (!payload) return std::unexpected(payload.error());
        variadic.push_back(std::move(*payload));
      }
    }
    if (l.variable) {
      if (offsets.size() < (result.row_count + 1) * l.width) return Fail("invalid offset buffer");
      uint64_t prior = 0;
      for (size_t row = 0; row <= result.row_count; ++row) {
        auto end = Read(offsets, row * l.width, l.width);
        if (end < prior || end > data->size()) return Fail("non-monotone/out-of-range offset");
        prior = end;
      }
    } else if (data->size() < (l.datatype == 0 ? (result.row_count + 7) / 8 : result.row_count * l.width))
      return Fail("short fixed-width data buffer");
    std::string mask((result.row_count + 7) / 8, '\0');
    for (size_t row = 0; row < result.row_count; ++row) {
      const bool is_null = !validity->empty() && !((*validity)[row / 8] & (1U << (row % 8)));
      if (is_null) {
        ++nulls;
        mask[row / 8] |= static_cast<char>(1U << (row % 8));
        continue;
      }
      if (l.view) {
        auto value = detail::DecodeView(*data, row, variadic);
        if (!value) return std::unexpected(value.error());
        if (value->size() > kMaxBody - materialized) return Fail("view values exceed decoded size policy");
        materialized += value->size();
        if (f.type.id == ipc::TypeId::kUtf8View && !ValidUtf8(*value)) return Fail("invalid UTF-8 value");
        if (l.datatype == 12) values->add_string_values(std::string(*value));
        else values->add_binary_values(std::string(*value));
      } else if (l.variable) {
        const auto start = Read(offsets, row * l.width, l.width),
                   end = Read(offsets, (row + 1) * l.width, l.width);
        std::string str;
        if (end != start)
          str.assign(reinterpret_cast<const char *>(data->bytes.data() + start), end - start);
        if (l.datatype == 12 && !ValidUtf8(str)) return Fail("invalid UTF-8 value");
        if (l.datatype == 12)
          values->add_string_values(str);
        else
          values->add_binary_values(str);
      } else if (l.datatype == 0)
        values->add_bool_values(((*data)[row / 8] & (1U << (row % 8))) != 0);
      else
        AppendDecoded(*values, l, *data, row * l.width);
    }
    if (nulls != static_cast<uint64_t>(node.null_count) || (!f.nullable && nulls))
      return Fail("null count/nullable mismatch");
    if (nulls) c.set_null_mask(std::move(mask));
    result.columns.push_back(std::move(c));
  }
  if (next != b.buffers.size()) return Fail("unexpected extra body buffers");
  if (next_variadic != b.variadic_buffer_counts.size()) return Fail("unexpected variadic buffer counts");
  return result;
}

FlightResult<FlightTimestampBounds> TimestampBounds(const RecordBatch &batch) {
  auto validated = ValidateBatch(batch);
  if (!validated) return std::unexpected(validated.error());
  if (!batch.row_count) return FlightTimestampBounds{};
  auto index = TimeColumn(batch);
  if (!index) return std::unexpected(index.error());
  if (*index >= batch.columns.size()) return Fail("timestamp column missing");
  auto layout = GetLayout(batch.schema.fields[*index]);
  if (!layout) return std::unexpected(layout.error());
  const auto &values = batch.columns[*index].values();
  const auto *r = values.GetReflection();
  int64_t low = std::numeric_limits<int64_t>::max(), high = std::numeric_limits<int64_t>::min();
  for (size_t row = 0; row < batch.row_count; ++row) {
    if (NullAt(batch.columns[*index], row)) return Fail("null timestamp");
    auto value = r->GetRepeatedInt64(values, ValueField(*layout), static_cast<int>(row));
    low = std::min(low, value);
    high = std::max(high, value);
  }
  return FlightTimestampBounds{low, high};
}
FlightResult<std::vector<RecordBatch>> PartitionRecordBatchByHour(const RecordBatch &batch) {
  auto bounds = TimestampBounds(batch);
  if (!bounds) return std::unexpected(bounds.error());
  if (!batch.row_count) return std::vector<RecordBatch>{batch};
  auto index = TimeColumn(batch);
  if (!index) return std::unexpected(index.error());
  const auto &f = batch.schema.fields[*index];
  auto layout = GetLayout(f);
  static constexpr int64_t ticks[] = {3600LL, 3600000LL, 3600000000LL, 3600000000000LL};
  const auto &tv = batch.columns[*index].values();
  const auto *tr = tv.GetReflection();
  std::map<int64_t, std::vector<size_t>> groups;
  for (size_t row = 0; row < batch.row_count; ++row) {
    const int64_t value = tr->GetRepeatedInt64(tv, ValueField(*layout), static_cast<int>(row));
    const int64_t tick = ticks[f.type.unit];
    const int64_t hour = value / tick - (value % tick < 0 ? 1 : 0);
    if (!groups.contains(hour) && groups.size() >= kMaxPartitions)
      return Fail("hour partitions exceed policy");
    groups[hour].push_back(row);
  }
  auto schema_wire = ipc::EncodeSchema(batch.schema);
  if (!schema_wire) return std::unexpected(schema_wire.error());
  if (schema_wire->size() > kMaxBody / groups.size()) return Fail("partition schemas exceed size policy");
  std::vector<RecordBatch> result;
  for (const auto &[hour, rows] : groups) {
    static_cast<void>(hour);
    RecordBatch part{batch.schema, {}, rows.size()};
    for (size_t col = 0; col < batch.columns.size(); ++col) {
      const auto &source = batch.columns[col];
      auto l = GetLayout(batch.schema.fields[col]);
      if (!l) return std::unexpected(l.error());
      Column target = source;
      target.clear_values();
      target.clear_null_mask();
      std::string mask((rows.size() + 7) / 8, '\0');
      size_t packed = 0, selected = 0;
      for (size_t row = 0; row < batch.row_count && selected < rows.size(); ++row) {
        const bool is_null = NullAt(source, row);
        if (row == rows[selected]) {
          if (is_null)
            mask[selected / 8] |= static_cast<char>(1U << (selected % 8));
          else if (l->variable || l->view) {
            const auto &str = l->datatype == 12 ? source.values().string_values(static_cast<int>(packed))
                                                : source.values().binary_values(static_cast<int>(packed));
            if (l->datatype == 12)
              target.mutable_values()->add_string_values(str);
            else
              target.mutable_values()->add_binary_values(str);
          } else if (l->datatype == 0)
            target.mutable_values()->add_bool_values(source.values().bool_values(static_cast<int>(packed)));
          else {
            Bytes native;
            auto appended = AppendNative(native, source.values(), *l, packed);
            if (!appended) return std::unexpected(appended.error());
            AppendDecoded(*target.mutable_values(), *l, native, 0);
          }
          ++selected;
        }
        packed += !is_null;
      }
      if (std::any_of(mask.begin(), mask.end(), [](char c) { return c != 0; }))
        target.set_null_mask(std::move(mask));
      part.columns.push_back(std::move(target));
    }
    result.push_back(std::move(part));
  }
  return result;
}
} // namespace greptimedb::flight
