#include <greptimedb/flight/bulk_writer.h>

#include <Flight.pb.h>

#include <charconv>
#include <cmath>
#include <limits>
#include <new>
#include <utility>
#include <vector>

namespace greptimedb::flight {
namespace {
constexpr std::string_view kDoPutPath =
    "/arrow.flight.protocol.FlightService/DoPut";

Error Protocol(std::string message) noexcept {
  return Error(ErrorKind::kProtocol, std::move(message));
}
Error DecodeError() noexcept {
  return Error(ErrorKind::kDecode, "invalid DoPut JSON metadata");
}

// Parse integer tokens directly: JSON's floating-point representation loses ids
// > 2^53.
class JsonReader {
public:
  explicit JsonReader(std::string_view input) noexcept : input_(input) {}
  bool Take(char c) noexcept {
    Space();
    if (pos_ == input_.size() || input_[pos_] != c)
      return false;
    ++pos_;
    return true;
  }
  bool End() noexcept {
    Space();
    return pos_ == input_.size();
  }
  bool Null() noexcept {
    Space();
    if (input_.substr(pos_, 4) != "null")
      return false;
    pos_ += 4;
    return true;
  }
  bool String(std::string *out) noexcept {
    if (!Take('"'))
      return false;
    while (pos_ < input_.size()) {
      auto c = static_cast<unsigned char>(input_[pos_++]);
      if (c == '"')
        return true;
      if (c < 0x20)
        return false;
      if (c >= 0x80) {
        const auto start = pos_ - 1;
        const unsigned count = c >= 0xf0 ? 3 : c >= 0xe0 ? 2 : 1;
        if (c < 0xc2 || c > 0xf4 || input_.size() - pos_ < count)
          return false;
        std::uint32_t code = c & (count == 3 ? 7 : count == 2 ? 15 : 31);
        for (unsigned index = 0; index < count; ++index) {
          const auto next = static_cast<unsigned char>(input_[pos_++]);
          if ((next & 0xc0) != 0x80)
            return false;
          code = (code << 6) | (next & 0x3f);
        }
        if ((count == 2 && code < 0x800) || (count == 3 && code < 0x10000) ||
            code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff))
          return false;
        if (out)
          out->append(input_.substr(start, count + 1));
        continue;
      }
      if (c != '\\') {
        if (out)
          out->push_back(static_cast<char>(c));
        continue;
      }
      if (pos_ == input_.size())
        return false;
      const char escape = input_[pos_++];
      switch (escape) {
      case '"':
      case '\\':
      case '/':
        c = escape;
        break;
      case 'b':
        c = '\b';
        break;
      case 'f':
        c = '\f';
        break;
      case 'n':
        c = '\n';
        break;
      case 'r':
        c = '\r';
        break;
      case 't':
        c = '\t';
        break;
      case 'u': {
        std::uint32_t code{};
        if (!Hex(code))
          return false;
        if (code >= 0xd800 && code <= 0xdbff) {
          if (input_.substr(pos_, 2) != "\\u")
            return false;
          pos_ += 2;
          std::uint32_t low{};
          if (!Hex(low) || low < 0xdc00 || low > 0xdfff)
            return false;
          code = 0x10000 + ((code - 0xd800) << 10) + (low - 0xdc00);
        } else if (code >= 0xdc00 && code <= 0xdfff)
          return false;
        if (out) {
          if (code <= 0x7f)
            out->push_back(static_cast<char>(code));
          else {
            if (code > 0xffff)
              out->push_back(static_cast<char>(0xf0 | (code >> 18)));
            if (code > 0x7ff)
              out->push_back(static_cast<char>((code > 0xffff ? 0x80 : 0xe0) |
                                               ((code >> 12) & 0x3f)));
            out->push_back(static_cast<char>((code > 0x7ff ? 0x80 : 0xc0) |
                                             ((code >> 6) & 0x3f)));
            out->push_back(static_cast<char>(0x80 | (code & 0x3f)));
          }
        }
        continue;
      }
      default:
        return false;
      }
      if (out)
        out->push_back(static_cast<char>(c));
    }
    return false;
  }
  bool Number(std::string_view &value) noexcept {
    Space();
    const auto start = pos_;
    if (pos_ < input_.size() && input_[pos_] == '-')
      ++pos_;
    if (pos_ == input_.size())
      return false;
    if (input_[pos_] == '0')
      ++pos_;
    else {
      if (input_[pos_] < '1' || input_[pos_] > '9')
        return false;
      while (pos_ < input_.size() && Digit(input_[pos_]))
        ++pos_;
    }
    if (pos_ < input_.size() && input_[pos_] == '.') {
      ++pos_;
      const auto fraction = pos_;
      while (pos_ < input_.size() && Digit(input_[pos_]))
        ++pos_;
      if (pos_ == fraction)
        return false;
    }
    if (pos_ < input_.size() && (input_[pos_] == 'e' || input_[pos_] == 'E')) {
      ++pos_;
      if (pos_ < input_.size() && (input_[pos_] == '+' || input_[pos_] == '-'))
        ++pos_;
      const auto exponent = pos_;
      while (pos_ < input_.size() && Digit(input_[pos_]))
        ++pos_;
      if (pos_ == exponent)
        return false;
    }
    value = input_.substr(start, pos_ - start);
    return true;
  }
  template <class T> bool Integer(T &value) noexcept {
    std::string_view token;
    if (!Number(token))
      return false;
    const auto parsed =
        std::from_chars(token.data(), token.data() + token.size(), value);
    return parsed.ec == std::errc{} &&
           parsed.ptr == token.data() + token.size();
  }
  bool Real(double &value) noexcept {
    std::string_view token;
    if (!Number(token))
      return false;
    const auto parsed =
        std::from_chars(token.data(), token.data() + token.size(), value);
    return parsed.ec == std::errc{} &&
           parsed.ptr == token.data() + token.size() && std::isfinite(value) &&
           value >= 0;
  }
  bool Skip(unsigned depth = 0) noexcept {
    if (depth > 16)
      return false;
    Space();
    if (pos_ == input_.size())
      return false;
    if (input_[pos_] == '"')
      return String(nullptr);
    if (Take('{')) {
      if (Take('}'))
        return true;
      do {
        if (!String(nullptr) || !Take(':') || !Skip(depth + 1))
          return false;
        if (Take('}'))
          return true;
      } while (Take(','));
      return false;
    }
    if (Take('[')) {
      if (Take(']'))
        return true;
      do {
        if (!Skip(depth + 1))
          return false;
        if (Take(']'))
          return true;
      } while (Take(','));
      return false;
    }
    for (const auto literal :
         {std::string_view("true"), std::string_view("false"),
          std::string_view("null")}) {
      if (input_.substr(pos_, literal.size()) == literal) {
        pos_ += literal.size();
        return true;
      }
    }
    std::string_view ignored;
    return Number(ignored);
  }

private:
  static bool Digit(char c) noexcept { return c >= '0' && c <= '9'; }
  void Space() noexcept {
    while (pos_ < input_.size() &&
           (input_[pos_] == ' ' || input_[pos_] == '\n' ||
            input_[pos_] == '\r' || input_[pos_] == '\t'))
      ++pos_;
  }
  bool Hex(std::uint32_t &result) noexcept {
    if (input_.size() - pos_ < 4)
      return false;
    for (int index = 0; index != 4; ++index) {
      const char c = input_[pos_++];
      const auto value = c >= '0' && c <= '9'   ? c - '0'
                         : c >= 'a' && c <= 'f' ? c - 'a' + 10
                         : c >= 'A' && c <= 'F' ? c - 'A' + 10
                                                : -1;
      if (value < 0)
        return false;
      result = (result << 4) | static_cast<std::uint32_t>(value);
    }
    return true;
  }
  std::string_view input_;
  std::size_t pos_{};
};

template <class F> bool Object(std::string_view input, F field) noexcept {
  if (input.size() > 65536)
    return false;
  JsonReader reader(input);
  if (!reader.Take('{'))
    return false;
  if (reader.Take('}'))
    return reader.End();
  do {
    std::string key;
    if (!reader.String(&key) || !reader.Take(':') || !field(reader, key))
      return false;
    if (reader.Take('}'))
      return reader.End();
  } while (reader.Take(','));
  return false;
}

template <class T>
bool Once(bool &seen, JsonReader &reader, T &value) noexcept {
  if (seen)
    return false;
  seen = true;
  return reader.Integer(value);
}

template <class Message>
Result<std::vector<std::byte>> Serialize(const Message &message) noexcept {
  const auto size = message.ByteSizeLong();
  if (size > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(
        Error(ErrorKind::kEncode, "Flight frame exceeds Protobuf size limit"));
  std::vector<std::byte> bytes(size);
  if (size && !message.SerializeToArray(bytes.data(), static_cast<int>(size)))
    return std::unexpected(
        Error(ErrorKind::kEncode, "cannot encode Flight frame"));
  return bytes;
}

void SetBody(::arrow::flight::protocol::FlightData &frame,
             const EncodedFlightBody &encoded) {
  if (!encoded.data_header.empty())
    frame.set_data_header(encoded.data_header.data(), encoded.data_header.size());
  if (!encoded.data_body.empty())
    frame.set_data_body(encoded.data_body.data(), encoded.data_body.size());
}
} // namespace

Result<std::string> DoPutMetadata::Encode() const noexcept {
  std::string json = "{\"request_id\":" + std::to_string(request_id);
  if (min_timestamp)
    json += ",\"min_timestamp\":" + std::to_string(*min_timestamp);
  if (max_timestamp)
    json += ",\"max_timestamp\":" + std::to_string(*max_timestamp);
  json += '}';
  return json;
}

Result<DoPutMetadata> DoPutMetadata::Decode(std::string_view json) noexcept {
  DoPutMetadata result;
  bool id{}, minimum{}, maximum{};
  const bool valid =
      Object(json, [&](JsonReader &reader, std::string_view key) {
        if (key == "request_id")
          return Once(id, reader, result.request_id);
        auto optional = [&](bool &seen, std::optional<std::int64_t> &value) {
          if (seen)
            return false;
          seen = true;
          if (reader.Null())
            return true;
          std::int64_t number{};
          if (!reader.Integer(number))
            return false;
          value = number;
          return true;
        };
        if (key == "min_timestamp")
          return optional(minimum, result.min_timestamp);
        if (key == "max_timestamp")
          return optional(maximum, result.max_timestamp);
        return reader.Skip();
      });
  if (!valid || !id)
    return std::unexpected(DecodeError());
  return result;
}

Result<DoPutResponse> DoPutResponse::Decode(std::string_view json) noexcept {
  DoPutResponse result;
  bool id{}, rows{}, elapsed{};
  const bool valid =
      Object(json, [&](JsonReader &reader, std::string_view key) {
        if (key == "request_id")
          return Once(id, reader, result.request_id);
        if (key == "affected_rows")
          return Once(rows, reader, result.affected_rows);
        if (key == "elapsed_secs") {
          if (elapsed)
            return false;
          elapsed = true;
          return reader.Real(result.elapsed_secs);
        }
        return reader.Skip();
      });
  if (!valid || !id || !rows)
    return std::unexpected(DecodeError());
  return result;
}

Result<DoPutResponse> DoPutResponse::Decode(
    const ::arrow::flight::protocol::PutResult &response) noexcept {
  return Decode(response.app_metadata());
}

BulkStreamWriter::BulkStreamWriter(std::unique_ptr<RpcStream> call,
                                   FlightSchema schema,
                                   BulkWriteOptions options) noexcept
    : call_(std::move(call)), schema_(std::move(schema)),
      encoder_(options.compression), options_(options),
      owner_(std::this_thread::get_id()),
      pending_(new (std::nothrow) std::int64_t[options.max_in_flight]) {}

Result<std::unique_ptr<BulkStreamWriter>>
BulkStreamWriter::Open(SynchronousChannel &channel, std::string_view table_name,
                       const FlightSchema &schema, BulkWriteOptions options,
                       CallOptions call_options) noexcept {
  if (table_name.empty() || options.max_in_flight == 0 ||
      options.timeout.count() <= 0 ||
      options.max_in_flight > 65536 ||
      (options.compression != FlightCompression::kNone &&
       options.compression != FlightCompression::kLz4Frame &&
       options.compression != FlightCompression::kZstd))
    return std::unexpected(Error::InvalidConfig(
        "table, max_in_flight in [1, 65536], positive timeout and valid compression are required"));
  // Validate schema locally before opening a remote operation.
  auto encoded = FlightIpcEncoder(options.compression).EncodeSchema(schema);
  if (!encoded)
    return std::unexpected(Error(ErrorKind::kEncode, encoded.error()));
  call_options.timeout = options.timeout;
  auto call = channel.BidiStreamCall(kDoPutPath, std::move(call_options));
  if (!call)
    return std::unexpected(Error::Grpc(call.error()));
  auto writer = std::unique_ptr<BulkStreamWriter>(
      new (std::nothrow) BulkStreamWriter(std::move(*call), schema, options));
  if (!writer || !writer->pending_)
    return std::unexpected(
        Error(ErrorKind::kStream, "cannot allocate bulk writer"));
  auto handshake = writer->SendSchema(table_name);
  if (!handshake)
    return std::unexpected(handshake.error());
  writer->state_ = State::kActive;
  return writer;
}

BulkStreamWriter::~BulkStreamWriter() {
  if (state_ != State::kFinished && state_ != State::kCancelled && call_)
    static_cast<void>(call_->Cancel());
}

Result<void> BulkStreamWriter::EnsureActive() const noexcept {
  if (std::this_thread::get_id() != owner_)
    return std::unexpected(
        Protocol("bulk writer used from a different thread"));
  switch (state_) {
  case State::kActive:
    return {};
  case State::kFailed:
    return std::unexpected(*failure_);
  case State::kOpening:
    return std::unexpected(Protocol("bulk schema handshake is incomplete"));
  case State::kDraining:
    return std::unexpected(Protocol("bulk writer is draining"));
  case State::kFinished:
    return std::unexpected(Protocol("bulk writer is finished"));
  case State::kCancelled:
    return std::unexpected(Protocol("bulk writer is cancelled"));
  }
  return std::unexpected(Protocol("invalid bulk writer state"));
}

Error BulkStreamWriter::Fail(Error error) noexcept {
  if (!failure_)
    failure_ = std::move(error);
  state_ = State::kFailed;
  // A terminal acknowledgement failure must release the RPC immediately.
  if (call_)
    static_cast<void>(call_->Cancel());
  return *failure_;
}

Error BulkStreamWriter::TransportError(grpcxx::Status status) noexcept {
  if (!call_->FinalStatus().Ok())
    status = call_->FinalStatus();
  return Fail(Error::FromGrpcStatusAndTrailers(std::move(status),
                                               call_->TrailingMetadata()));
}

Result<void> BulkStreamWriter::CheckTrailers() noexcept {
  if (!call_->Done())
    return std::unexpected(
        Fail(Protocol("DoPut ended without final trailers")));
  const auto &status = call_->FinalStatus();
  if (!status.Ok())
    return std::unexpected(TransportError(status));
  auto application = Error::CheckOkTrailers(call_->TrailingMetadata());
  if (!application)
    return std::unexpected(Fail(application.error()));
  return {};
}

Result<void>
BulkStreamWriter::SendSchema(std::string_view table_name) noexcept {
  auto body = encoder_.EncodeSchema(schema_);
  if (!body)
    return std::unexpected(Fail(Error(ErrorKind::kEncode, body.error())));
  auto metadata = DoPutMetadata{}.Encode();
  if (!metadata)
    return std::unexpected(Fail(metadata.error()));
  ::arrow::flight::protocol::FlightData frame;
  auto *descriptor = frame.mutable_flight_descriptor();
  descriptor->set_type(::arrow::flight::protocol::FlightDescriptor::PATH);
  descriptor->add_path(std::string(table_name));
  SetBody(frame, *body);
  frame.set_app_metadata(*metadata);
  auto bytes = Serialize(frame);
  if (!bytes)
    return std::unexpected(Fail(bytes.error()));
  auto sent = call_->Send(*bytes);
  if (!sent)
    return std::unexpected(TransportError(sent.error()));
  auto response = call_->RecvNext();
  if (!response)
    return std::unexpected(TransportError(response.error()));
  if (!*response) {
    auto trailers = CheckTrailers();
    if (!trailers)
      return trailers;
    return std::unexpected(
        Fail(Protocol("DoPut closed before schema acknowledgement")));
  }
  return AcceptResult(**response, true);
}

Result<void> BulkStreamWriter::AcceptResult(std::span<const std::byte> payload,
                                            bool schema_ack) noexcept {
  ::arrow::flight::protocol::PutResult message;
  if (payload.size() >
          static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
      !message.ParseFromArray(payload.data(), static_cast<int>(payload.size())))
    return std::unexpected(
        Fail(Error(ErrorKind::kDecode, "invalid DoPut PutResult")));
  auto response = DoPutResponse::Decode(message);
  if (!response)
    return std::unexpected(Fail(response.error()));
  if (schema_ack) {
    if (response->request_id != 0)
      return std::unexpected(
          Fail(Protocol("schema acknowledgement must use request_id 0")));
    return {};
  }
  if (pending_count_ == 0 || response->request_id != pending_[pending_head_])
    return std::unexpected(
        Fail(Protocol("out-of-order or unknown DoPut request_id")));
  if (response->affected_rows > std::numeric_limits<std::uint64_t>::max() -
                                    stats_.total_affected_rows ||
      !std::isfinite(stats_.elapsed_secs + response->elapsed_secs))
    return std::unexpected(
        Fail(Protocol("DoPut acknowledgement counters overflowed")));
  stats_.total_affected_rows += response->affected_rows;
  stats_.elapsed_secs += response->elapsed_secs;
  pending_head_ = (pending_head_ + 1) % options_.max_in_flight;
  --pending_count_;
  return {};
}

Result<void> BulkStreamWriter::DrainReady() noexcept {
  for (;;) {
    auto response = call_->TryRecvNext();
    if (!response)
      return std::unexpected(TransportError(response.error()));
    if (!*response) {
      if (!call_->Done())
        return {};
      auto trailers = CheckTrailers();
      if (!trailers)
        return trailers;
      return std::unexpected(
          Fail(Protocol("DoPut closed before requests were half-closed")));
    }
    auto accepted = AcceptResult(**response);
    if (!accepted)
      return accepted;
  }
}

Result<void> BulkStreamWriter::WaitForCapacity() noexcept {
  while (pending_count_ >= options_.max_in_flight) {
    auto response = call_->RecvNext();
    if (!response)
      return std::unexpected(TransportError(response.error()));
    if (!*response) {
      auto trailers = CheckTrailers();
      if (!trailers)
        return trailers;
      return std::unexpected(
          Fail(Protocol("DoPut closed with batches in flight")));
    }
    auto accepted = AcceptResult(**response);
    if (!accepted)
      return accepted;
  }
  return {};
}

Result<void>
BulkStreamWriter::WriteRecordBatch(const RecordBatch &batch) noexcept {
  auto active = EnsureActive();
  if (!active)
    return active;
  auto ready = DrainReady();
  if (!ready)
    return ready;
  auto expected_schema = encoder_.EncodeSchema(schema_);
  auto actual_schema = encoder_.EncodeSchema(batch.schema);
  if (!actual_schema)
    return std::unexpected(
        Error(ErrorKind::kSchemaViolation, actual_schema.error()));
  if (!expected_schema ||
      expected_schema->data_header != actual_schema->data_header)
    return std::unexpected(Error(ErrorKind::kSchemaViolation,
                                 "record batch schema differs from stream"));
  if (!options_.partition_by_hour)
    return SendBatch(batch);
  auto partitions = PartitionRecordBatchByHour(batch);
  if (!partitions)
    return std::unexpected(Error(ErrorKind::kEncode, partitions.error()));
  bool sent_partition = false;
  for (const auto &partition : *partitions) {
    auto sent = SendBatch(partition);
    if (!sent) {
      if (sent_partition)
        return std::unexpected(Fail(sent.error()));
      return sent;
    }
    sent_partition = true;
  }
  return {};
}

Result<void> BulkStreamWriter::SendBatch(const RecordBatch &batch) noexcept {
  // Encoding failures are local and leave the session usable.
  auto body = encoder_.EncodeBatch(batch);
  if (!body)
    return std::unexpected(Error(ErrorKind::kEncode, body.error()));
  FlightTimestampBounds bounds;
  bool timestamp = options_.partition_by_hour;
  for (const auto &field : batch.schema.fields)
    for (const auto &[key, value] : field.metadata)
      timestamp |= (key == kSemanticTypeKey && value == "timestamp") ||
                   (key == kTimeIndexKey && value == "true");
  if (timestamp) {
    auto found = TimestampBounds(batch);
    if (!found)
      return std::unexpected(Error(ErrorKind::kEncode, found.error()));
    bounds = *found;
  }
  if (next_request_id_ == std::numeric_limits<std::int64_t>::max() ||
      stats_.batches_sent == std::numeric_limits<std::size_t>::max())
    return std::unexpected(
        Error(ErrorKind::kEncode, "DoPut request counter overflowed"));
  auto metadata =
      DoPutMetadata{next_request_id_, bounds.first, bounds.second}.Encode();
  if (!metadata)
    return std::unexpected(metadata.error());
  ::arrow::flight::protocol::FlightData frame;
  SetBody(frame, *body);
  frame.set_app_metadata(*metadata);
  auto bytes = Serialize(frame);
  if (!bytes)
    return std::unexpected(bytes.error());
  auto ready = DrainReady();
  if (!ready)
    return ready;
  auto capacity = WaitForCapacity();
  if (!capacity)
    return capacity;
  auto sent = call_->Send(*bytes);
  if (!sent)
    return std::unexpected(TransportError(sent.error()));
  pending_[(pending_head_ + pending_count_) % options_.max_in_flight] =
      next_request_id_++;
  ++pending_count_;
  ++stats_.batches_sent;
  return {};
}

Result<BulkWriteStats> BulkStreamWriter::Finish() noexcept {
  auto active = EnsureActive();
  if (!active)
    return std::unexpected(active.error());
  state_ = State::kDraining;
  if (!call_->TrailingMetadata().WireValue("grpc-status")) {
    auto closed = call_->HalfClose();
    if (!closed)
      return std::unexpected(TransportError(closed.error()));
  }
  for (;;) {
    auto response = call_->RecvNext();
    if (!response)
      return std::unexpected(TransportError(response.error()));
    if (!*response)
      break;
    auto accepted = AcceptResult(**response);
    if (!accepted)
      return std::unexpected(accepted.error());
  }
  auto trailers = CheckTrailers();
  if (!trailers)
    return std::unexpected(trailers.error());
  if (pending_count_ != 0)
    return std::unexpected(
        Fail(Protocol("DoPut ended before all batches were acknowledged")));
  state_ = State::kFinished;
  return stats_;
}

Result<void> BulkStreamWriter::Cancel() noexcept {
  if (std::this_thread::get_id() != owner_)
    return std::unexpected(
        Protocol("bulk writer cancelled from a different thread"));
  if (state_ == State::kFinished || state_ == State::kCancelled)
    return {};
  if (state_ == State::kFailed)
    return std::unexpected(*failure_);
  auto cancelled = call_->Cancel();
  if (!cancelled)
    return std::unexpected(TransportError(cancelled.error()));
  state_ = State::kCancelled;
  return {};
}

} // namespace greptimedb::flight
