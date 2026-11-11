#include "ipc_flatbuffers_internal.hh"

#include <algorithm>
#include <limits>
#include <uarch/uarch.hpp>

namespace greptimedb::flight::ipc {
namespace {

class Reader {
public:
  explicit Reader(std::span<const uint8_t> bytes) : bytes_(bytes) {}
  [[gnu::cold, gnu::noinline]] bool Fail(const char *message);
  bool Charge(size_t size) {
    if (uarch::unlikely(size > remaining_)) return Fail("decoded Arrow metadata exceeds resource limit");
    remaining_ -= size;
    return true;
  }
  bool Range(size_t at, size_t count) {
    if (uarch::likely(at <= bytes_.size() && count <= bytes_.size() - at)) return true;
    return Fail("truncated Arrow FlatBuffer");
  }
  template <class T> T Read(size_t at) {
    if (!Range(at, sizeof(T))) return 0;
    return Load<T>(bytes_, at);
  }
  bool Table(size_t at) {
    if (!Range(at, 4) || at % 4 != 0) return Fail("invalid table alignment");
    const int64_t vt = static_cast<int64_t>(at) - Read<int32_t>(at);
    if (vt < 0 || vt % 2 || !Range(static_cast<size_t>(vt), 4)) return Fail("invalid vtable offset");
    const auto size = Read<uint16_t>(static_cast<size_t>(vt));
    if (size < 4 || size % 2 || !Range(static_cast<size_t>(vt), size)) return Fail("invalid vtable length");
    const auto object_size = Read<uint16_t>(static_cast<size_t>(vt) + 2);
    if (object_size < 4 || !Range(at, object_size)) return Fail("invalid table size");
    if (static_cast<size_t>(vt) < at + object_size && static_cast<size_t>(vt) + size > at)
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
      Fail("field exceeds FlatBuffer table");
      return 0;
    }
    if ((table + offset) % std::min(width, size_t{8}) != 0) {
      Fail("invalid scalar alignment");
      return 0;
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
      Fail("invalid forward FlatBuffer offset");
      return 0;
    }
    const size_t target = at + offset;
    if (target >= bytes_.size()) {
      Fail("offset points past payload");
      return 0;
    }
    return target;
  }
  size_t Object(size_t table, size_t field, bool required = false) {
    const size_t at = FieldAt(table, field, 4);
    if (!at) {
      if (required) Fail("required Arrow field is absent");
      return 0;
    }
    return Offset(at);
  }
  size_t Vector(size_t at, size_t width, size_t &count) {
    count = 0;
    if (!at) return 0;
    if (at % 4 || !Range(at, 4)) {
      Fail("invalid vector alignment");
      return 0;
    }
    const auto n = Read<uint32_t>(at);
    if (n > (bytes_.size() - at - 4) / width || (at + 4) % std::min(width, size_t{8})) {
      Fail("invalid FlatBuffer vector length");
      return 0;
    }
    count = n;
    return at + 4;
  }
  std::string String(size_t at) {
    if (!at) return {};
    if (at % 4 || !Range(at, 4)) {
      Fail("invalid string alignment");
      return {};
    }
    const auto size = Read<uint32_t>(at);
    if (!Range(at + 4, static_cast<size_t>(size) + 1)) return {};
    if (!Charge(static_cast<size_t>(size) + 1)) return {};
    if (bytes_[at + 4 + size] != 0) {
      Fail("unterminated Arrow string");
      return {};
    }
    const auto raw = bytes_.subspan(at + 4, size);
    if (!ValidUtf8(raw)) {
      Fail("invalid UTF-8 in Arrow metadata");
      return {};
    }
    return std::string(raw.begin(), raw.end());
  }
  bool MetadataAt(size_t table, size_t field, Metadata &metadata) {
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
  bool TypeAt(size_t at, Type &type) {
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
  bool SchemaAt(size_t at, Schema &schema) {
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
  bool BatchAt(size_t at, Batch &batch) {
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
      if (buffer.offset < 0 || buffer.length < 0 || buffer.offset > std::numeric_limits<int64_t>::max() - buffer.length)
        return Fail("invalid Arrow buffer range");
      batch.buffers.push_back(buffer);
    }
    const size_t compression = Object(at, 3);
    if (compression) {
      if (!Table(compression)) return false;
      const auto codec = Scalar<uint8_t>(compression, 0);
      if (codec > 1 || Scalar<uint8_t>(compression, 1) != 0) return Fail("unsupported Arrow body compression");
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
  const std::string &Error() const { return error_; }

private:
  std::span<const uint8_t> bytes_;
  std::string error_;
  size_t remaining_ = kMaxHeaderBytes;
};

[[gnu::cold, gnu::noinline]] bool Reader::Fail(const char *message) {
  uarch::cold_path();
  if (error_.empty()) error_ = message;
  return false;
}
} // namespace

std::expected<Message, std::string> DecodeMessage(std::span<const uint8_t> header) {
  if (header.size() < 4 || header.size() > kMaxHeaderBytes) return std::unexpected("invalid Arrow IPC metadata size");
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
  if (message.version < 0 || message.version > 4) return std::unexpected("unsupported Arrow IPC metadata version");
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
    if (reader.Scalar<uint8_t>(at, 2) > 1) return std::unexpected("invalid DictionaryBatch delta flag");
    break;
  case 3:
    message.header_type = HeaderType::kRecordBatch;
    if (!reader.BatchAt(at, message.batch))
      return std::unexpected(reader.Ok() ? "invalid RecordBatch header" : reader.Error());
    break;
  default: return std::unexpected("unsupported Arrow IPC message header");
  }
  if (!reader.MetadataAt(message_at, 4, message.metadata)) return std::unexpected(reader.Error());
  for (const auto &buffer : message.batch.buffers)
    if (buffer.offset > message.body_length || buffer.length > message.body_length - buffer.offset)
      return std::unexpected("Arrow buffer exceeds declared message body");
  if (!reader.Ok()) return std::unexpected(reader.Error());
  return message;
}

std::expected<bool, std::string> IsMetadataOnlyMessage(std::span<const uint8_t> header) {
  auto message = DecodeMessage(header);
  if (!message) return std::unexpected(message.error());
  return message->header_type == HeaderType::kNone;
}
} // namespace greptimedb::flight::ipc
