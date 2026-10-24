#include "greptimedb/flight/ipc_flatbuffers.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <string_view>
#include <type_traits>

namespace greptimedb::flight::ipc {
namespace {
constexpr size_t kMaxHeaderBytes = 64 * 1024 * 1024;
constexpr size_t kMaxFields = 65536;

template <class T>
T Load(std::span<const uint8_t> data, size_t offset) {
  using U = std::make_unsigned_t<T>;
  U result = 0;
  for (size_t i = 0; i < sizeof(T); ++i)
    result |= static_cast<U>(data[offset + i]) << (8 * i);
  return std::bit_cast<T>(result);
}

bool ValidUtf8(std::string_view value) {
  size_t i = 0;
  while (i < value.size()) {
    const auto c = static_cast<uint8_t>(value[i++]);
    if (c < 0x80) continue;
    uint32_t cp = 0;
    size_t n = 0;
    uint32_t minimum = 0;
    if (c >= 0xc2 && c <= 0xdf) { cp = c & 31; n = 1; minimum = 0x80; }
    else if (c >= 0xe0 && c <= 0xef) { cp = c & 15; n = 2; minimum = 0x800; }
    else if (c >= 0xf0 && c <= 0xf4) { cp = c & 7; n = 3; minimum = 0x10000; }
    else return false;
    if (n > value.size() - i) return false;
    for (size_t j = 0; j < n; ++j) {
      const auto b = static_cast<uint8_t>(value[i++]);
      if ((b & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (b & 63);
    }
    if (cp < minimum || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff))
      return false;
  }
  return true;
}

bool ValidType(const Type& type) {
  switch (type.id) {
    case TypeId::kNull:
    case TypeId::kBinary:
    case TypeId::kUtf8:
    case TypeId::kBool:
    case TypeId::kLargeBinary:
    case TypeId::kBinaryView:
    case TypeId::kUtf8View:
    case TypeId::kLargeUtf8: return true;
    case TypeId::kInt:
      return type.bit_width == 8 || type.bit_width == 16 ||
             type.bit_width == 32 || type.bit_width == 64;
    case TypeId::kFloatingPoint: return type.precision == 1 || type.precision == 2;
    case TypeId::kDecimal:
      return type.bit_width == 128 && type.decimal_precision >= 1 &&
             type.decimal_precision <= 38 && type.scale >= -128 &&
             type.scale <= type.decimal_precision;
    case TypeId::kDate: return type.unit == 0 || type.unit == 1;
    case TypeId::kTimestamp:
      return type.unit >= 0 && type.unit <= 3 && ValidUtf8(type.timezone);
  }
  return false;
}

class Writer {
 public:
  Writer() : bytes_(4, 0) {}
  template <class T> void Put(size_t at, T value) {
    using U = std::make_unsigned_t<T>;
    U bits = std::bit_cast<U>(value);
    for (size_t i = 0; i < sizeof(T); ++i)
      bytes_[at + i] = static_cast<uint8_t>(bits >> (8 * i));
  }
  void Align(size_t alignment, size_t prefix = 0) {
    while ((bytes_.size() + prefix) % alignment) bytes_.push_back(0);
  }
  size_t Table(size_t fields) {
    Align(2);
    const size_t vt = bytes_.size();
    bytes_.resize(vt + 4 + fields * 2, 0);
    Put<uint16_t>(vt, static_cast<uint16_t>(4 + fields * 2));
    Put<uint16_t>(vt + 2, static_cast<uint16_t>(8 + fields * 8));
    Align(8);
    const size_t at = bytes_.size();
    bytes_.resize(at + 8 + fields * 8, 0);
    Put<int32_t>(at, static_cast<int32_t>(at - vt));
    for (size_t i = 0; i < fields; ++i)
      Put<uint16_t>(vt + 4 + i * 2, static_cast<uint16_t>(8 + 8 * i));
    return at;
  }
  size_t Slot(size_t table, size_t index) const { return table + 8 + index * 8; }
  void Omit(size_t table, size_t index) {
    const size_t vt = table - Load<int32_t>(bytes_, table);
    Put<uint16_t>(vt + 4 + 2 * index, 0);
  }
  void Ref(size_t slot, size_t target) {
    Put<uint32_t>(slot, static_cast<uint32_t>(target - slot));
  }
  size_t String(const std::string& value) {
    Align(4);
    const size_t at = bytes_.size();
    bytes_.resize(at + 4, 0);
    Put<uint32_t>(at, static_cast<uint32_t>(value.size()));
    bytes_.insert(bytes_.end(), value.begin(), value.end());
    bytes_.push_back(0);
    return at;
  }
  size_t Offsets(size_t count) {
    Align(4);
    const size_t at = bytes_.size();
    bytes_.resize(at + 4 + count * 4, 0);
    Put<uint32_t>(at, static_cast<uint32_t>(count));
    return at;
  }
  size_t MetadataVector(const Metadata& metadata) {
    const size_t at = Offsets(metadata.size());
    for (size_t i = 0; i < metadata.size(); ++i) {
      const size_t kv = Table(2);
      Ref(at + 4 + 4 * i, kv);
      Ref(Slot(kv, 0), String(metadata[i].first));
      Ref(Slot(kv, 1), String(metadata[i].second));
    }
    return at;
  }
  size_t TypeTable(const Type& type) {
    size_t at = 0;
    switch (type.id) {
      case TypeId::kInt:
        at = Table(2);
        Put<int32_t>(Slot(at, 0), type.bit_width);
        Put<uint8_t>(Slot(at, 1), type.is_signed ? 1 : 0);
        return at;
      case TypeId::kFloatingPoint:
        at = Table(1); Put<int16_t>(Slot(at, 0), type.precision); return at;
      case TypeId::kDecimal:
        at = Table(3);
        Put<int32_t>(Slot(at, 0), type.decimal_precision);
        Put<int32_t>(Slot(at, 1), type.scale);
        Put<int32_t>(Slot(at, 2), type.bit_width);
        return at;
      case TypeId::kDate:
        at = Table(1); Put<int16_t>(Slot(at, 0), type.unit); return at;
      case TypeId::kTimestamp:
        at = Table(type.timezone.empty() ? 1 : 2);
        Put<int16_t>(Slot(at, 0), type.unit);
        if (!type.timezone.empty()) Ref(Slot(at, 1), String(type.timezone));
        return at;
      case TypeId::kNull:
      case TypeId::kBinary:
      case TypeId::kUtf8:
      case TypeId::kBool:
      case TypeId::kLargeBinary:
      case TypeId::kBinaryView:
      case TypeId::kUtf8View:
      case TypeId::kLargeUtf8: return Table(0);
    }
    return 0;
  }
  size_t SchemaTable(const Schema& schema) {
    const size_t at = Table(3);
    Put<int16_t>(Slot(at, 0), schema.little_endian ? 0 : 1);
    const size_t fields = Offsets(schema.fields.size());
    Ref(Slot(at, 1), fields);
    for (size_t i = 0; i < schema.fields.size(); ++i) {
      const auto& field = schema.fields[i];
      const size_t f = Table(7);
      Ref(fields + 4 + 4 * i, f);
      Ref(Slot(f, 0), String(field.name));
      Put<uint8_t>(Slot(f, 1), field.nullable ? 1 : 0);
      Put<uint8_t>(Slot(f, 2), static_cast<uint8_t>(field.type.id));
      Ref(Slot(f, 3), TypeTable(field.type));
      // Absent dictionary and children offsets must have absent vtable entries.
      Omit(f, 4);
      Omit(f, 5);
      Ref(Slot(f, 6), MetadataVector(field.metadata));
    }
    Ref(Slot(at, 2), MetadataVector(schema.metadata));
    return at;
  }
  template <class T> size_t StructVector(const std::vector<T>& values) {
    Align(8, 4);
    const size_t at = bytes_.size();
    bytes_.resize(at + 4 + values.size() * 16, 0);
    Put<uint32_t>(at, static_cast<uint32_t>(values.size()));
    for (size_t i = 0; i < values.size(); ++i) {
      if constexpr (std::is_same_v<T, Node>) {
        Put<int64_t>(at + 4 + i * 16, values[i].length);
        Put<int64_t>(at + 12 + i * 16, values[i].null_count);
      } else {
        Put<int64_t>(at + 4 + i * 16, values[i].offset);
        Put<int64_t>(at + 12 + i * 16, values[i].length);
      }
    }
    return at;
  }
  size_t BatchTable(const Batch& batch) {
    const size_t at = Table(batch.variadic_buffer_counts.empty() ?
        (batch.compression == Compression::kNone ? 3 : 4) : 5);
    Put<int64_t>(Slot(at, 0), batch.length);
    Ref(Slot(at, 1), StructVector(batch.nodes));
    Ref(Slot(at, 2), StructVector(batch.buffers));
    if (batch.compression != Compression::kNone) {
      const size_t compression = Table(2);
      Put<uint8_t>(Slot(compression, 0), batch.compression == Compression::kLz4Frame ? 0 : 1);
      Put<uint8_t>(Slot(compression, 1), 0);
      Ref(Slot(at, 3), compression);
    } else if (!batch.variadic_buffer_counts.empty()) {
      Omit(at, 3);
    }
    if (!batch.variadic_buffer_counts.empty()) {
      Align(8, 4);
      const size_t counts = bytes_.size();
      bytes_.resize(counts + 4 + batch.variadic_buffer_counts.size() * 8, 0);
      Put<uint32_t>(counts, static_cast<uint32_t>(batch.variadic_buffer_counts.size()));
      for (size_t i = 0; i < batch.variadic_buffer_counts.size(); ++i)
        Put<int64_t>(counts + 4 + i * 8, batch.variadic_buffer_counts[i]);
      Ref(Slot(at, 4), counts);
    }
    return at;
  }
  size_t MessageTable(HeaderType kind, int64_t body_length) {
    const size_t at = Table(4);
    Ref(0, at);
    Put<int16_t>(Slot(at, 0), 4);
    Put<uint8_t>(Slot(at, 1), static_cast<uint8_t>(kind));
    Put<int64_t>(Slot(at, 3), body_length);
    return at;
  }
  std::vector<uint8_t> Finish() { Align(8); return std::move(bytes_); }
 private:
  std::vector<uint8_t> bytes_;
};

class Reader {
 public:
  explicit Reader(std::span<const uint8_t> bytes) : bytes_(bytes) {}
  bool Fail(const char* message) { if (error_.empty()) error_ = message; return false; }
  bool Charge(size_t size) {
    if (size > remaining_) return Fail("decoded Arrow metadata exceeds resource limit");
    remaining_ -= size;
    return true;
  }
  bool Range(size_t at, size_t count) {
    return (at <= bytes_.size() && count <= bytes_.size() - at) ||
           Fail("truncated Arrow FlatBuffer");
  }
  template <class T> T Read(size_t at) {
    if (!Range(at, sizeof(T))) return 0;
    return Load<T>(bytes_, at);
  }
  bool Table(size_t at) {
    if (!Range(at, 4) || at % 4 != 0) return Fail("invalid table alignment");
    const int64_t vt = static_cast<int64_t>(at) - Read<int32_t>(at);
    if (vt < 0 || vt % 2 || !Range(static_cast<size_t>(vt), 4))
      return Fail("invalid vtable offset");
    const auto size = Read<uint16_t>(static_cast<size_t>(vt));
    if (size < 4 || size % 2 || !Range(static_cast<size_t>(vt), size))
      return Fail("invalid vtable length");
    const auto object_size = Read<uint16_t>(static_cast<size_t>(vt) + 2);
    if (object_size < 4 || !Range(at, object_size)) return Fail("invalid table size");
    if (static_cast<size_t>(vt) < at + object_size &&
        static_cast<size_t>(vt) + size > at)
      return Fail("vtable overlaps its Arrow table");
    return true;
  }
  size_t FieldAt(size_t table, size_t field, size_t width) {
    if (!Table(table)) return 0;
    const size_t vt = static_cast<size_t>(static_cast<int64_t>(table) - Read<int32_t>(table));
    const auto size = Read<uint16_t>(vt);
    if (field >= (static_cast<size_t>(size) - 4) / 2) return 0;
    const auto offset = Read<uint16_t>(vt + 4 + 2 * field);
    if (offset == 0) return 0;
    const auto object_size = Read<uint16_t>(vt + 2);
    if (offset < 4 || offset > object_size || width > static_cast<size_t>(object_size - offset)) {
      Fail("field exceeds FlatBuffer table"); return 0;
    }
    if ((table + offset) % std::min(width, size_t{8}) != 0) {
      Fail("invalid scalar alignment"); return 0;
    }
    return table + offset;
  }
  template <class T> T Scalar(size_t table, size_t field, T fallback = 0) {
    const size_t at = FieldAt(table, field, sizeof(T));
    return at ? Read<T>(at) : fallback;
  }
  size_t Offset(size_t at) {
    if (!at || !Range(at, 4)) return 0;
    const auto offset = Read<uint32_t>(at);
    if (offset < 4 || offset > bytes_.size() - at) {
      Fail("invalid forward FlatBuffer offset"); return 0;
    }
    const size_t target = at + offset;
    if (target >= bytes_.size()) { Fail("offset points past payload"); return 0; }
    return target;
  }
  size_t Object(size_t table, size_t field, bool required = false) {
    const size_t at = FieldAt(table, field, 4);
    if (!at) { if (required) Fail("required Arrow field is absent"); return 0; }
    return Offset(at);
  }
  size_t Vector(size_t at, size_t width, size_t& count) {
    count = 0;
    if (!at) return 0;
    if (at % 4 || !Range(at, 4)) { Fail("invalid vector alignment"); return 0; }
    const auto n = Read<uint32_t>(at);
    if (n > (bytes_.size() - at - 4) / width || (at + 4) % std::min(width, size_t{8})) {
      Fail("invalid FlatBuffer vector length"); return 0;
    }
    count = n;
    return at + 4;
  }
  std::string String(size_t at) {
    if (!at) return {};
    if (at % 4 || !Range(at, 4)) { Fail("invalid string alignment"); return {}; }
    const auto size = Read<uint32_t>(at);
    if (!Range(at + 4, static_cast<size_t>(size) + 1)) return {};
    if (!Charge(static_cast<size_t>(size) + 1)) return {};
    if (bytes_[at + 4 + size] != 0) { Fail("unterminated Arrow string"); return {}; }
    const std::string_view value(reinterpret_cast<const char*>(bytes_.data() + at + 4), size);
    if (!ValidUtf8(value)) { Fail("invalid UTF-8 in Arrow metadata"); return {}; }
    return std::string(value);
  }
  bool MetadataAt(size_t table, size_t field, Metadata& metadata) {
    size_t count = 0;
    const size_t entries = Vector(Object(table, field), 4, count);
    if (count > kMaxFields) return Fail("too many Arrow metadata entries");
    for (size_t i = 0; i < count && Ok(); ++i) {
      if (!Charge(sizeof(Metadata::value_type))) return false;
      const size_t kv = Offset(entries + i * 4);
      if (!kv || !Table(kv)) return false;
      const size_t key = Object(kv, 0, true);
      const size_t value = Object(kv, 1);
      metadata.emplace_back(String(key), String(value));
    }
    return Ok();
  }
  bool TypeAt(size_t at, Type& type) {
    if (!at || !Table(at)) return false;
    switch (type.id) {
      case TypeId::kInt:
        type.bit_width = Scalar<int32_t>(at, 0);
        if (Scalar<uint8_t>(at, 1) > 1) return Fail("invalid Int signedness");
        type.is_signed = Scalar<uint8_t>(at, 1) != 0;
        break;
      case TypeId::kFloatingPoint: type.precision = Scalar<int16_t>(at, 0); break;
      case TypeId::kDecimal:
        type.decimal_precision = Scalar<int32_t>(at, 0);
        type.scale = Scalar<int32_t>(at, 1);
        type.bit_width = Scalar<int32_t>(at, 2, 128);
        break;
      case TypeId::kDate: type.unit = Scalar<int16_t>(at, 0, 1); break;
      case TypeId::kTimestamp:
        type.unit = Scalar<int16_t>(at, 0);
        type.timezone = String(Object(at, 1));
        break;
      case TypeId::kNull:
      case TypeId::kBinary:
      case TypeId::kUtf8:
      case TypeId::kBool:
      case TypeId::kLargeBinary:
      case TypeId::kBinaryView:
      case TypeId::kUtf8View:
      case TypeId::kLargeUtf8: break;
    }
    return (Ok() && ValidType(type)) || Fail("unsupported or invalid Arrow type");
  }
  bool SchemaAt(size_t at, Schema& schema) {
    if (!at || !Table(at)) return false;
    const auto endian = Scalar<int16_t>(at, 0);
    if (endian != 0 && endian != 1) return Fail("unknown Arrow endianness");
    schema.little_endian = endian == 0;
    size_t count = 0;
    const size_t entries = Vector(Object(at, 1), 4, count);
    if (count > kMaxFields) return Fail("too many Arrow fields");
    for (size_t i = 0; i < count && Ok(); ++i) {
      if (!Charge(sizeof(Field))) return false;
      const size_t f = Offset(entries + 4 * i);
      if (!f || !Table(f)) return false;
      Field field;
      field.name = String(Object(f, 0, true));
      if (Scalar<uint8_t>(f, 1) > 1) return Fail("invalid field nullability");
      field.nullable = Scalar<uint8_t>(f, 1) != 0;
      field.type.id = static_cast<TypeId>(Scalar<uint8_t>(f, 2));
      if (!TypeAt(Object(f, 3, true), field.type)) return false;
      if (Object(f, 4)) return Fail("dictionary-encoded Arrow fields are unsupported");
      size_t children = 0;
      Vector(Object(f, 5), 4, children);
      if (children) return Fail("nested Arrow fields are unsupported");
      if (!MetadataAt(f, 6, field.metadata)) return false;
      schema.fields.push_back(std::move(field));
    }
    size_t features = 0;
    Vector(Object(at, 3), 8, features);
    return MetadataAt(at, 2, schema.metadata);
  }
  bool BatchAt(size_t at, Batch& batch) {
    if (!at || !Table(at)) return false;
    batch.length = Scalar<int64_t>(at, 0);
    if (batch.length < 0) return Fail("negative record-batch length");
    size_t count = 0;
    size_t entries = Vector(Object(at, 1), 16, count);
    for (size_t i = 0; i < count && Ok(); ++i) {
      if (!Charge(sizeof(Node))) return false;
      Node node{Read<int64_t>(entries + 16 * i), Read<int64_t>(entries + 16 * i + 8)};
      if (node.length < 0 || node.null_count < 0 || node.null_count > node.length)
        return Fail("invalid Arrow field node");
      batch.nodes.push_back(node);
    }
    entries = Vector(Object(at, 2), 16, count);
    for (size_t i = 0; i < count && Ok(); ++i) {
      if (!Charge(sizeof(Buffer))) return false;
      Buffer buffer{Read<int64_t>(entries + 16 * i), Read<int64_t>(entries + 16 * i + 8)};
      if (buffer.offset < 0 || buffer.length < 0 ||
          buffer.offset > std::numeric_limits<int64_t>::max() - buffer.length)
        return Fail("invalid Arrow buffer range");
      batch.buffers.push_back(buffer);
    }
    const size_t compression = Object(at, 3);
    if (compression) {
      if (!Table(compression)) return false;
      const auto codec = Scalar<uint8_t>(compression, 0);
      if (codec > 1 || Scalar<uint8_t>(compression, 1) != 0)
        return Fail("unsupported Arrow body compression");
      batch.compression = codec == 0 ? Compression::kLz4Frame : Compression::kZstd;
    }
    size_t variadic = 0;
    const size_t counts = Vector(Object(at, 4), 8, variadic);
    if (variadic > batch.nodes.size()) return Fail("too many variadic buffer counts");
    size_t total = 0;
    for (size_t i = 0; i < variadic; ++i) {
      if (!Charge(sizeof(int64_t))) return false;
      const auto n = Read<int64_t>(counts + 8 * i);
      if (n < 0 || static_cast<uint64_t>(n) > batch.buffers.size() - total)
        return Fail("invalid variadic buffer count");
      total += static_cast<size_t>(n);
      batch.variadic_buffer_counts.push_back(n);
    }
    return Ok();
  }
  bool Ok() const { return error_.empty(); }
  const std::string& Error() const { return error_; }
 private:
  std::span<const uint8_t> bytes_;
  std::string error_;
  size_t remaining_ = kMaxHeaderBytes;
};

bool MetadataValid(const Metadata& metadata, size_t& size) {
  if (metadata.size() > kMaxFields) return false;
  for (const auto& [key, value] : metadata) {
    if (!ValidUtf8(key) || !ValidUtf8(value)) return false;
    if (key.size() > kMaxHeaderBytes || value.size() > kMaxHeaderBytes) return false;
    size += key.size() + value.size() + 96;
    if (size > kMaxHeaderBytes) return false;
  }
  return true;
}
}  // namespace

std::expected<std::vector<uint8_t>, std::string> EncodeSchema(const Schema& schema) {
  size_t size = 128;
  if (schema.fields.size() > kMaxFields || !MetadataValid(schema.metadata, size))
    return std::unexpected("Arrow schema metadata is invalid or too large");
  for (const auto& field : schema.fields) {
    if (!ValidType(field.type) || !ValidUtf8(field.name) ||
        field.name.size() > kMaxHeaderBytes || field.type.timezone.size() > kMaxHeaderBytes ||
        !MetadataValid(field.metadata, size))
      return std::unexpected("Arrow field is invalid or too large");
    size += field.name.size() + field.type.timezone.size() + 384;
    if (size > kMaxHeaderBytes) return std::unexpected("Arrow schema exceeds metadata limit");
  }
  Writer writer;
  const size_t message = writer.MessageTable(HeaderType::kSchema, 0);
  writer.Ref(writer.Slot(message, 2), writer.SchemaTable(schema));
  auto encoded = writer.Finish();
  if (encoded.size() > kMaxHeaderBytes)
    return std::unexpected("Arrow schema exceeds metadata limit");
  return encoded;
}

std::expected<std::vector<uint8_t>, std::string> EncodeRecordBatch(
    const Batch& batch, int64_t body_length) {
  if (batch.length < 0 || body_length < 0 || batch.nodes.size() > kMaxHeaderBytes / 32 ||
      batch.buffers.size() > kMaxHeaderBytes / 32 ||
      batch.variadic_buffer_counts.size() > batch.nodes.size() ||
      batch.nodes.size() + batch.buffers.size() + batch.variadic_buffer_counts.size() > (kMaxHeaderBytes - 256) / 16)
    return std::unexpected("Arrow record batch exceeds metadata limits");
  if (batch.compression != Compression::kNone &&
      batch.compression != Compression::kLz4Frame &&
      batch.compression != Compression::kZstd)
    return std::unexpected("unsupported Arrow compression");
  for (const auto& node : batch.nodes)
    if (node.length < 0 || node.null_count < 0 || node.null_count > node.length)
      return std::unexpected("invalid Arrow field node");
  for (const auto& buffer : batch.buffers)
    if (buffer.offset < 0 || buffer.length < 0 || buffer.offset > body_length ||
        buffer.length > body_length - buffer.offset)
      return std::unexpected("Arrow buffer exceeds body");
  size_t total = 0;
  for (const auto count : batch.variadic_buffer_counts) {
    if (count < 0 || static_cast<uint64_t>(count) > batch.buffers.size() - total)
      return std::unexpected("invalid variadic buffer count");
    total += static_cast<size_t>(count);
  }
  Writer writer;
  const size_t message = writer.MessageTable(HeaderType::kRecordBatch, body_length);
  writer.Ref(writer.Slot(message, 2), writer.BatchTable(batch));
  auto encoded = writer.Finish();
  if (encoded.size() > kMaxHeaderBytes)
    return std::unexpected("Arrow record batch exceeds metadata limit");
  return encoded;
}

std::expected<std::vector<uint8_t>, std::string> EncodeMetadataMessage() {
  Writer writer;
  const size_t message = writer.MessageTable(HeaderType::kNone, 0);
  writer.Omit(message, 2);
  return writer.Finish();
}

std::expected<Message, std::string> DecodeMessage(std::span<const uint8_t> header) {
  if (header.size() < 4 || header.size() > kMaxHeaderBytes)
    return std::unexpected("invalid Arrow IPC metadata size");
  const auto first = Load<uint32_t>(header, 0);
  if (first == 0xffffffffu) {
    if (header.size() < 8) return std::unexpected("missing Arrow IPC metadata length");
    const auto length = Load<uint32_t>(header, 4);
    if (length == 0 || length > header.size() - 8)
      return std::unexpected("invalid encapsulated Arrow IPC metadata length");
    header = header.subspan(8, length);
  } else if (first == header.size() - 4) {
    header = header.subspan(4);
  }
  if (header.size() < 4) return std::unexpected("truncated Arrow Message root");
  Reader reader(header);
  const size_t message_at = Load<uint32_t>(header, 0);
  if (message_at < 4 || !reader.Table(message_at))
    return std::unexpected(reader.Ok() ? "invalid Arrow Message root" : reader.Error());
  Message message;
  message.version = reader.Scalar<int16_t>(message_at, 0);
  if (message.version < 0 || message.version > 4)
    return std::unexpected("unsupported Arrow IPC metadata version");
  message.body_length = reader.Scalar<int64_t>(message_at, 3);
  if (message.body_length < 0) return std::unexpected("negative Arrow body length");
  const auto kind = reader.Scalar<uint8_t>(message_at, 1);
  const size_t at = kind == 0 ? 0 : reader.Object(message_at, 2, true);
  switch (kind) {
    case 0:
      message.header_type = HeaderType::kNone;
      if (message.body_length != 0 || reader.FieldAt(message_at, 2, 4) != 0)
        return std::unexpected("metadata-only Arrow message has a header or body");
      break;
    case 1:
      message.header_type = HeaderType::kSchema;
      if (!reader.SchemaAt(at, message.schema))
        return std::unexpected(reader.Ok() ? "invalid Arrow schema" : reader.Error());
      if (message.body_length != 0) return std::unexpected("schema message carries a body");
      break;
    case 2:
      message.header_type = HeaderType::kDictionaryBatch;
      if (!at || !reader.Table(at)) return std::unexpected("invalid DictionaryBatch header");
      reader.Scalar<int64_t>(at, 0);
      if (!reader.BatchAt(reader.Object(at, 1, true), message.batch))
        return std::unexpected(reader.Ok() ? "invalid DictionaryBatch data" : reader.Error());
      if (reader.Scalar<uint8_t>(at, 2) > 1)
        return std::unexpected("invalid DictionaryBatch delta flag");
      break;
    case 3:
      message.header_type = HeaderType::kRecordBatch;
      if (!reader.BatchAt(at, message.batch))
        return std::unexpected(reader.Ok() ? "invalid RecordBatch header" : reader.Error());
      break;
    default: return std::unexpected("unsupported Arrow IPC message header");
  }
  if (!reader.MetadataAt(message_at, 4, message.metadata))
    return std::unexpected(reader.Error());
  for (const auto& buffer : message.batch.buffers)
    if (buffer.offset > message.body_length ||
        buffer.length > message.body_length - buffer.offset)
      return std::unexpected("Arrow buffer exceeds declared message body");
  if (!reader.Ok()) return std::unexpected(reader.Error());
  return message;
}

std::expected<bool, std::string> IsMetadataOnlyMessage(std::span<const uint8_t> header) {
  auto message = DecodeMessage(header);
  if (!message) return std::unexpected(message.error());
  return message->header_type == HeaderType::kNone;
}
}  // namespace greptimedb::flight::ipc
