#include "ipc_flatbuffers_internal.hh"

#include <algorithm>
#include <cstring>
#include <type_traits>
#include <utility>
#include <vector>

namespace greptimedb::flight::ipc {
namespace {

class Writer {
public:
  Writer() : bytes_(4, 0) {}
  template <class T> void Put(size_t at, T value) {
    using U = std::make_unsigned_t<T>;
    auto bits = std::bit_cast<U>(value);
    if constexpr (sizeof(U) > 1 && std::endian::native != std::endian::little) bits = std::byteswap(bits);
    std::memcpy(bytes_.data() + at, &bits, sizeof(bits));
  }
  void Align(size_t alignment, size_t prefix = 0) {
    while ((bytes_.size() + prefix) % alignment)
      bytes_.push_back(0);
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
  void Ref(size_t slot, size_t target) { Put<uint32_t>(slot, static_cast<uint32_t>(target - slot)); }
  size_t String(const std::string &value) {
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
  size_t MetadataVector(const Metadata &metadata) {
    const size_t at = Offsets(metadata.size());
    for (size_t i = 0; i < metadata.size(); ++i) {
      const size_t kv = Table(2);
      Ref(at + 4 + 4 * i, kv);
      Ref(Slot(kv, 0), String(metadata[i].first));
      Ref(Slot(kv, 1), String(metadata[i].second));
    }
    return at;
  }
  size_t TypeTable(const Type &type) {
    size_t at = 0;
    switch (type.id) {
    case TypeId::kInt:
      at = Table(2);
      Put<int32_t>(Slot(at, 0), type.bit_width);
      Put<uint8_t>(Slot(at, 1), type.is_signed ? 1 : 0);
      return at;
    case TypeId::kFloatingPoint:
      at = Table(1);
      Put<int16_t>(Slot(at, 0), type.precision);
      return at;
    case TypeId::kDecimal:
      at = Table(3);
      Put<int32_t>(Slot(at, 0), type.decimal_precision);
      Put<int32_t>(Slot(at, 1), type.scale);
      Put<int32_t>(Slot(at, 2), type.bit_width);
      return at;
    case TypeId::kDate:
      at = Table(1);
      Put<int16_t>(Slot(at, 0), type.unit);
      return at;
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
  size_t SchemaTable(const Schema &schema) {
    const size_t at = Table(3);
    Put<int16_t>(Slot(at, 0), schema.little_endian ? 0 : 1);
    const size_t fields = Offsets(schema.fields.size());
    Ref(Slot(at, 1), fields);
    for (size_t i = 0; i < schema.fields.size(); ++i) {
      const auto &field = schema.fields[i];
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
  template <class T> size_t StructVector(const std::vector<T> &values) {
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
  size_t BatchTable(const Batch &batch) {
    const size_t at =
        Table(batch.variadic_buffer_counts.empty() ? (batch.compression == Compression::kNone ? 3 : 4) : 5);
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
  std::vector<uint8_t> Finish() {
    Align(8);
    return std::move(bytes_);
  }

private:
  std::vector<uint8_t> bytes_;
};

bool MetadataValid(const Metadata &metadata, size_t &size) {
  if (metadata.size() > kMaxFields) return false;
  for (const auto &[key, value] : metadata) {
    if (!ValidUtf8(key) || !ValidUtf8(value)) return false;
    if (key.size() > kMaxHeaderBytes || value.size() > kMaxHeaderBytes) return false;
    size += key.size() + value.size() + 96;
    if (size > kMaxHeaderBytes) return false;
  }
  return true;
}
} // namespace

std::expected<std::vector<uint8_t>, std::string> EncodeSchema(const Schema &schema) {
  size_t size = 128;
  if (schema.fields.size() > kMaxFields || !MetadataValid(schema.metadata, size))
    return std::unexpected("Arrow schema metadata is invalid or too large");
  for (const auto &field : schema.fields) {
    if (!ValidType(field.type) || !ValidUtf8(field.name) || field.name.size() > kMaxHeaderBytes ||
        field.type.timezone.size() > kMaxHeaderBytes || !MetadataValid(field.metadata, size))
      return std::unexpected("Arrow field is invalid or too large");
    size += field.name.size() + field.type.timezone.size() + 384;
    if (size > kMaxHeaderBytes) return std::unexpected("Arrow schema exceeds metadata limit");
  }
  Writer writer;
  const size_t message = writer.MessageTable(HeaderType::kSchema, 0);
  writer.Ref(writer.Slot(message, 2), writer.SchemaTable(schema));
  auto encoded = writer.Finish();
  if (encoded.size() > kMaxHeaderBytes) return std::unexpected("Arrow schema exceeds metadata limit");
  return encoded;
}

std::expected<std::vector<uint8_t>, std::string> EncodeRecordBatch(const Batch &batch, int64_t body_length) {
  if (batch.length < 0 || body_length < 0 || batch.nodes.size() > kMaxHeaderBytes / 32 ||
      batch.buffers.size() > kMaxHeaderBytes / 32 || batch.variadic_buffer_counts.size() > batch.nodes.size() ||
      batch.nodes.size() + batch.buffers.size() + batch.variadic_buffer_counts.size() > (kMaxHeaderBytes - 256) / 16)
    return std::unexpected("Arrow record batch exceeds metadata limits");
  if (batch.compression != Compression::kNone && batch.compression != Compression::kLz4Frame &&
      batch.compression != Compression::kZstd)
    return std::unexpected("unsupported Arrow compression");
  for (const auto &node : batch.nodes)
    if (node.length < 0 || node.null_count < 0 || node.null_count > node.length)
      return std::unexpected("invalid Arrow field node");
  for (const auto &buffer : batch.buffers)
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
  if (encoded.size() > kMaxHeaderBytes) return std::unexpected("Arrow record batch exceeds metadata limit");
  return encoded;
}

std::expected<std::vector<uint8_t>, std::string> EncodeMetadataMessage() {
  Writer writer;
  const size_t message = writer.MessageTable(HeaderType::kNone, 0);
  writer.Omit(message, 2);
  return writer.Finish();
}
} // namespace greptimedb::flight::ipc
