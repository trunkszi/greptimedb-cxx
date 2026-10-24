#ifndef GREPTIMEDB_FLIGHT_IPC_FLATBUFFERS_H_
#define GREPTIMEDB_FLIGHT_IPC_FLATBUFFERS_H_

#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace greptimedb::flight::ipc {

using Metadata = std::vector<std::pair<std::string, std::string>>;

enum class TypeId : uint8_t {
  kNull = 1, kInt = 2, kFloatingPoint = 3, kBinary = 4, kUtf8 = 5,
  kBool = 6, kDecimal = 7, kDate = 8, kTimestamp = 10,
  kLargeBinary = 19, kLargeUtf8 = 20,
  kBinaryView = 23, kUtf8View = 24,
};

struct Type {
  TypeId id = TypeId::kNull;
  int32_t bit_width = 0;
  bool is_signed = false;
  int16_t precision = 0;
  int32_t decimal_precision = 38;
  int32_t scale = 0;
  int16_t unit = 0;
  std::string timezone;
};

struct Field {
  std::string name;
  Type type;
  bool nullable = true;
  Metadata metadata;
};

struct Schema {
  std::vector<Field> fields;
  Metadata metadata;
  bool little_endian = true;
};

struct Node { int64_t length = 0; int64_t null_count = 0; };
struct Buffer { int64_t offset = 0; int64_t length = 0; };
enum class Compression : uint8_t { kNone, kLz4Frame, kZstd };
struct Batch {
  int64_t length = 0;
  std::vector<Node> nodes;
  std::vector<Buffer> buffers;
  Compression compression = Compression::kNone;
  std::vector<int64_t> variadic_buffer_counts;
};

enum class HeaderType : uint8_t {
  kNone = 0, kSchema = 1, kDictionaryBatch = 2, kRecordBatch = 3,
};
struct Message {
  HeaderType header_type = HeaderType::kSchema;
  int16_t version = 4;
  int64_t body_length = 0;
  Schema schema;
  Batch batch;
  Metadata metadata;
};

// IPC metadata is emitted as a bare little-endian Arrow Message FlatBuffer.
std::expected<std::vector<uint8_t>, std::string> EncodeSchema(const Schema& schema);
std::expected<std::vector<uint8_t>, std::string> EncodeRecordBatch(
    const Batch& batch, int64_t body_length);
std::expected<std::vector<uint8_t>, std::string> EncodeMetadataMessage();

// Accepts bare messages and encapsulated IPC metadata, including continuation markers.
std::expected<Message, std::string> DecodeMessage(std::span<const uint8_t> header);
std::expected<bool, std::string> IsMetadataOnlyMessage(std::span<const uint8_t> header);

}  // namespace greptimedb::flight::ipc
#endif
