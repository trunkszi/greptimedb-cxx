#include "greptimedb/transport/mock_grpc_server.h"

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <new>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <grpcxx/compression/compression_registry.h>
#include <grpcxx/framing/length_prefixed_frame.h>

namespace greptimedb {
namespace {
using grpcxx::Status;
using grpcxx::StatusCode;
auto Failure(std::string message) noexcept -> Status {
  return Status{StatusCode::kInternal, std::move(message)};
}
auto Header(std::string_view key, std::string_view value) noexcept
    -> nghttp2_nv {
  return {reinterpret_cast<std::uint8_t *>(const_cast<char *>(key.data())),
          reinterpret_cast<std::uint8_t *>(const_cast<char *>(value.data())),
          key.size(), value.size(), NGHTTP2_NV_FLAG_NONE};
}
auto AddReceived(grpcxx::MetadataMap &map, std::string_view key,
                 std::string_view value) noexcept
    -> std::expected<void, Status> {
  if (key.ends_with("-bin")) {
    do {
      const auto separator = value.find(',');
      auto token = value.substr(0, separator);
      while (token.starts_with(' '))
        token.remove_prefix(1);
      while (token.ends_with(' '))
        token.remove_suffix(1);
      std::string padded{token};
      while (padded.size() % 4)
        padded.push_back('=');
      auto bytes = grpcxx::MetadataMap::Base64Decode(padded);
      if (!bytes)
        return std::unexpected(Failure("Malformed binary request metadata"));
      if (auto added = map.AddBinary(key, *bytes); !added)
        return added;
      if (separator == value.npos)
        break;
      value.remove_prefix(separator + 1);
    } while (true);
    return {};
  }
  return map.Add(key, value);
}
auto ApplicationKey(std::string_view key) noexcept -> bool {
  return !key.starts_with(':') && key != "content-type" &&
         key != "grpc-encoding" && key != "grpc-accept-encoding" &&
         key != "grpc-status" && key != "grpc-message";
}
} // namespace

struct MockGrpcServer::Impl final {
  static constexpr std::size_t kMaxStreams = 32;
  struct Stream final {
    std::int32_t id_{};
    std::string path_;
    grpcxx::MetadataMap request_metadata_, initial_, trailers_;
    grpcxx::CompressionAlgorithm request_encoding_{
        grpcxx::CompressionAlgorithm::kIdentity};
    grpcxx::CompressionAlgorithm response_encoding_{
        grpcxx::CompressionAlgorithm::kIdentity};
    grpcxx::LengthPrefixedFrame parser_{0};
    std::deque<std::vector<std::byte>> output_;
    std::size_t offset_{}, queued_{}, partial_{}, header_bytes_{};
    Status status_;
    bool started_{}, finished_{}, half_closed_{}, valid_method_{},
        valid_type_{}, validated_{}, rejected_{};
  };
  std::unique_ptr<EndpointTransport> transport_;
  nghttp2_session *native_{};
  std::array<Stream, kMaxStreams> streams_;
  Handler handler_{};
  void *context_{};
  MockGrpcServer *owner_{};
  MockServerOptions options_;
  grpcxx::CompressionRegistry compression_{0};
  std::vector<std::byte> wire_;
  std::size_t wire_offset_{}, cancelled_{};
  bool polling_{};
  std::size_t window_updates_{};
  Status error_;

  ~Impl() {
    if (native_)
      nghttp2_session_del(native_);
  }
  auto Find(std::int32_t id) noexcept -> Stream * {
    auto found = std::ranges::find(streams_, id, &Stream::id_);
    return found == streams_.end() ? nullptr : &*found;
  }
  auto Dispatch(Stream &stream, MockRequest::Event event,
                std::span<const std::byte> payload = {}) noexcept -> void {
    if (handler_) {
      const MockRequest request{stream.id_, stream.path_, payload,
                                stream.request_metadata_, event};
      handler_(context_, *owner_, request);
    } else if (event == MockRequest::Event::kHalfClose) {
      auto result =
          owner_->Finish(stream.id_, Status{StatusCode::kUnimplemented});
      if (!result)
        error_ = result.error();
    }
  }
  static auto Begin(nghttp2_session *, const nghttp2_frame *frame,
                    void *context) noexcept -> int {
    auto &self = *static_cast<Impl *>(context);
    if (frame->hd.type != NGHTTP2_HEADERS ||
        frame->headers.cat != NGHTTP2_HCAT_REQUEST)
      return 0;
    auto slot = std::ranges::find(self.streams_, 0, &Stream::id_);
    if (slot == self.streams_.end())
      return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    *slot = Stream{};
    slot->id_ = frame->hd.stream_id;
    slot->parser_ =
        grpcxx::LengthPrefixedFrame{self.options_.max_message_bytes};
    return 0;
  }
  static auto OnHeader(nghttp2_session *, const nghttp2_frame *frame,
                       const std::uint8_t *name, std::size_t name_size,
                       const std::uint8_t *value, std::size_t value_size,
                       std::uint8_t, void *context) noexcept -> int {
    auto &self = *static_cast<Impl *>(context);
    auto *stream = self.Find(frame->hd.stream_id);
    if (!stream)
      return 0;
    stream->header_bytes_ += name_size + value_size;
    if (stream->header_bytes_ > 65536)
      return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    const std::string_view key{reinterpret_cast<const char *>(name), name_size};
    const std::string_view text{reinterpret_cast<const char *>(value),
                                value_size};
    if (key == ":path")
      stream->path_ = text;
    else if (key == ":method")
      stream->valid_method_ = text == "POST";
    else if (key == "content-type")
      stream->valid_type_ =
          text == "application/grpc" || text.starts_with("application/grpc+");
    else if (key == "grpc-encoding") {
      auto encoding = grpcxx::CompressionRegistry::ParseEncoding(text);
      if (!encoding)
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      stream->request_encoding_ = *encoding;
    } else if (key == "grpc-accept-encoding") {
      const auto desired = self.options_.response_compression;
      const auto token = grpcxx::CompressionRegistry::EncodingName(desired);
      /** A single-token negotiation uses the same parser as production
       * compression. */
      std::size_t offset{};
      while (offset < text.size()) {
        const auto end = text.find(',', offset);
        auto part = text.substr(offset, end == text.npos ? text.size() - offset
                                                         : end - offset);
        while (part.starts_with(' '))
          part.remove_prefix(1);
        while (part.ends_with(' '))
          part.remove_suffix(1);
        if (part == token)
          stream->response_encoding_ = desired;
        if (end == text.npos)
          break;
        offset = end + 1;
      }
    }
    if (!key.starts_with(':')) {
      auto added = AddReceived(stream->request_metadata_, key, text);
      if (!added)
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    }
    return 0;
  }
  static auto OnData(nghttp2_session *native, std::uint8_t, std::int32_t id,
                     const std::uint8_t *bytes, std::size_t size,
                     void *context) noexcept -> int {
    auto &self = *static_cast<Impl *>(context);
    auto *stream = self.Find(id);
    if (!stream || stream->rejected_)
      return 0;
    auto reject = [&]() noexcept {
      stream->rejected_ = true;
      return nghttp2_submit_rst_stream(native, NGHTTP2_FLAG_NONE, id,
                                       NGHTTP2_PROTOCOL_ERROR);
    };
    if (stream->half_closed_ || !stream->validated_)
      return reject();
    stream->partial_ += size;
    auto frames = stream->parser_.Feed(
        {reinterpret_cast<const std::byte *>(bytes), size});
    if (!frames)
      return reject();
    for (auto &frame : *frames) {
      stream->partial_ -= frame.WireSize();
      if (frame.Compressed()) {
        if (stream->request_encoding_ ==
            grpcxx::CompressionAlgorithm::kIdentity)
          return reject();
        auto decoded = self.compression_.Decompress(
            stream->request_encoding_, frame.Payload(),
            self.options_.max_message_bytes);
        if (!decoded)
          return reject();
        self.Dispatch(*stream, MockRequest::Event::kMessage,
                      decoded->Payload());
      } else
        self.Dispatch(*stream, MockRequest::Event::kMessage, frame.Payload());
    }
    return 0;
  }
  static auto OnFrame(nghttp2_session *native, const nghttp2_frame *frame,
                      void *context) noexcept -> int {
    auto &self = *static_cast<Impl *>(context);
    if (frame->hd.type == NGHTTP2_WINDOW_UPDATE)
      ++self.window_updates_;
    if (frame->hd.type == NGHTTP2_HEADERS) {
      if (auto *stream = self.Find(frame->hd.stream_id);
          stream && !stream->validated_) {
        if (!stream->valid_method_ || !stream->valid_type_ ||
            stream->path_.empty()) {
          stream->rejected_ = true;
          return nghttp2_submit_rst_stream(native, NGHTTP2_FLAG_NONE,
                                           stream->id_, NGHTTP2_PROTOCOL_ERROR);
        }
        stream->validated_ = true;
      }
    }
    if (frame->hd.type == NGHTTP2_RST_STREAM) {
      if (auto *stream = self.Find(frame->hd.stream_id)) {
        ++self.cancelled_;
        self.Dispatch(*stream, MockRequest::Event::kCancel);
      }
    }
    if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
        (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
      if (auto *stream = self.Find(frame->hd.stream_id);
          stream && !stream->half_closed_ && !stream->rejected_) {
        stream->half_closed_ = true;
        if (stream->partial_ || !stream->valid_method_ ||
            !stream->valid_type_ || stream->path_.empty())
          return nghttp2_submit_rst_stream(native, NGHTTP2_FLAG_NONE,
                                           stream->id_, NGHTTP2_PROTOCOL_ERROR);
        self.Dispatch(*stream, MockRequest::Event::kHalfClose);
      }
    }
    return 0;
  }
  static auto OnClose(nghttp2_session *, std::int32_t id, std::uint32_t,
                      void *context) noexcept -> int {
    auto &self = *static_cast<Impl *>(context);
    if (auto *stream = self.Find(id))
      *stream = Stream{};
    return 0;
  }
  static auto ReadData(nghttp2_session *native, std::int32_t id,
                       std::uint8_t *bytes, std::size_t size,
                       std::uint32_t *flags, nghttp2_data_source *source,
                       void *) noexcept -> nghttp2_ssize {
    auto &stream = *static_cast<Stream *>(source->ptr);
    if (stream.output_.empty() && !stream.finished_)
      return NGHTTP2_ERR_DEFERRED;
    std::size_t count{};
    if (!stream.output_.empty()) {
      auto &front = stream.output_.front();
      count = std::min(size, front.size() - stream.offset_);
      if (count)
        std::memcpy(bytes, front.data() + stream.offset_, count);
      stream.offset_ += count;
      stream.queued_ -= count;
      if (stream.offset_ == front.size()) {
        stream.output_.pop_front();
        stream.offset_ = 0;
      }
    }
    if (stream.output_.empty() && stream.finished_) {
      *flags |= NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
      const auto code =
          std::to_string(static_cast<unsigned>(stream.status_.Code()));
      const auto message = Status::PercentEncode(stream.status_.Message());
      std::vector<nghttp2_nv> trailers{Header("grpc-status", code)};
      if (!message.empty())
        trailers.push_back(Header("grpc-message", message));
      for (const auto &entry : stream.trailers_.Entries())
        trailers.push_back(Header(entry.key, entry.value));
      if (nghttp2_submit_trailer(native, id, trailers.data(),
                                 trailers.size()) != 0)
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    return static_cast<nghttp2_ssize>(count);
  }
  auto Start(Stream &stream) noexcept -> std::expected<void, Status> {
    if (stream.started_)
      return {};
    std::vector<nghttp2_nv> headers{
        Header(":status", "200"), Header("content-type", "application/grpc"),
        Header("grpc-accept-encoding",
               grpcxx::CompressionRegistry::AcceptEncoding()),
        Header("grpc-encoding", grpcxx::CompressionRegistry::EncodingName(
                                    stream.response_encoding_))};
    for (const auto &entry : stream.initial_.Entries())
      headers.push_back(Header(entry.key, entry.value));
    nghttp2_data_provider2 provider{};
    provider.source.ptr = &stream;
    provider.read_callback = ReadData;
    if (nghttp2_submit_response2(native_, stream.id_, headers.data(),
                                 headers.size(), &provider) != 0)
      return std::unexpected(Failure("Cannot submit HTTP/2 response"));
    stream.started_ = true;
    return {};
  }
  auto Flush() noexcept -> std::expected<void, Status> {
    for (std::size_t budget = 0; budget < 32; ++budget) {
      if (wire_offset_ == wire_.size()) {
        wire_.clear();
        wire_offset_ = 0;
        const std::uint8_t *data{};
        const auto count = nghttp2_session_mem_send2(native_, &data);
        if (count < 0)
          return std::unexpected(Failure("HTTP/2 server serialization failed"));
        if (!count)
          return {};
        wire_.assign_range(std::span{reinterpret_cast<const std::byte *>(data),
                                     static_cast<std::size_t>(count)});
      }
      auto written = transport_->Write(std::span{wire_}.subspan(wire_offset_));
      if (!written)
        return std::unexpected(written.error());
      wire_offset_ += *written;
      if (!*written)
        return {};
    }
    return {};
  }
};

MockGrpcServer::MockGrpcServer(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {
  impl_->owner_ = this;
}
MockGrpcServer::~MockGrpcServer() = default;
auto MockGrpcServer::Create(std::unique_ptr<EndpointTransport> transport,
                            Handler handler, void *context,
                            MockServerOptions options) noexcept
    -> std::expected<std::unique_ptr<MockGrpcServer>, Status> {
  if (!transport || !options.max_message_bytes ||
      options.max_message_bytes > UINT32_MAX || options.max_queued_bytes < 5)
    return std::unexpected(Status{StatusCode::kInvalidArgument,
                                  "Invalid mock server configuration"});
  auto impl = std::unique_ptr<Impl>{new (std::nothrow) Impl};
  if (!impl)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  impl->transport_ = std::move(transport);
  impl->handler_ = handler;
  impl->context_ = context;
  impl->options_ = options;
  nghttp2_session_callbacks *callbacks{};
  if (nghttp2_session_callbacks_new(&callbacks) != 0)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                          Impl::Begin);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, Impl::OnHeader);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks,
                                                            Impl::OnData);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                       Impl::OnFrame);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks,
                                                         Impl::OnClose);
  const auto result =
      nghttp2_session_server_new(&impl->native_, callbacks, impl.get());
  nghttp2_session_callbacks_del(callbacks);
  if (result != 0)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  const nghttp2_settings_entry settings{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS,
                                        Impl::kMaxStreams};
  if (nghttp2_submit_settings(impl->native_, NGHTTP2_FLAG_NONE, &settings, 1) !=
      0)
    return std::unexpected(Failure("Cannot initialize HTTP/2 server settings"));
  auto server = std::unique_ptr<MockGrpcServer>{
      new (std::nothrow) MockGrpcServer{std::move(impl)}};
  if (!server)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  return server;
}
auto MockGrpcServer::PollOnce() noexcept -> std::expected<void, Status> {
  if (impl_->polling_)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "Reentrant server poll"});
  struct Guard {
    bool &active_;
    ~Guard() { active_ = false; }
  } guard{impl_->polling_};
  impl_->polling_ = true;
  if (auto polled = impl_->transport_->PollOnce(); !polled)
    return polled;
  if (auto flushed = impl_->Flush(); !flushed)
    return flushed;
  std::array<std::byte, 16384> input{};
  for (std::size_t budget = 0; budget < 32; ++budget) {
    auto count = impl_->transport_->Read(input);
    if (!count)
      return std::unexpected(count.error());
    if (!*count)
      break;
    const auto consumed = nghttp2_session_mem_recv2(
        impl_->native_, reinterpret_cast<const std::uint8_t *>(input.data()),
        *count);
    if (consumed < 0 || static_cast<std::size_t>(consumed) != *count)
      return std::unexpected(Failure("Invalid HTTP/2 client frames"));
    if (!impl_->error_.Ok())
      return std::unexpected(impl_->error_);
  }
  return impl_->Flush();
}
auto MockGrpcServer::SetInitialMetadata(
    std::int32_t id, const grpcxx::MetadataMap &metadata) noexcept
    -> std::expected<void, Status> {
  auto *stream = impl_->Find(id);
  if (!stream || stream->started_)
    return std::unexpected(Status{StatusCode::kFailedPrecondition,
                                  "Response headers already committed"});
  for (const auto &entry : metadata.Entries())
    if (!ApplicationKey(entry.key))
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Reserved response header"});
  stream->initial_ = metadata;
  return {};
}
auto MockGrpcServer::Send(std::int32_t id,
                          std::span<const std::byte> payload) noexcept
    -> std::expected<void, Status> {
  auto *stream = impl_->Find(id);
  if (!stream || stream->finished_)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "Response stream is closed"});
  if (payload.size() > impl_->options_.max_message_bytes)
    return std::unexpected(Status{StatusCode::kResourceExhausted,
                                  "Response exceeds message limit"});
  auto encoded =
      impl_->compression_.Compress(stream->response_encoding_, payload);
  if (!encoded)
    return std::unexpected(encoded.error());
  if (encoded->WireSize() > impl_->options_.max_queued_bytes - stream->queued_)
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "Response queue is full"});
  if (auto started = impl_->Start(*stream); !started)
    return started;
  stream->output_.emplace_back();
  stream->output_.back().assign_range(encoded->WireBytes());
  stream->queued_ += encoded->WireSize();
  const auto result = nghttp2_session_resume_data(impl_->native_, id);
  if (result != 0 && result != NGHTTP2_ERR_INVALID_ARGUMENT)
    return std::unexpected(Failure("Cannot resume HTTP/2 response data"));
  return {};
}
auto MockGrpcServer::Finish(std::int32_t id, Status status,
                            const grpcxx::MetadataMap &trailers) noexcept
    -> std::expected<void, Status> {
  auto *stream = impl_->Find(id);
  if (!stream || stream->finished_)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "Response stream is closed"});
  for (const auto &entry : trailers.Entries())
    if (!ApplicationKey(entry.key))
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Reserved response trailer"});
  if (auto started = impl_->Start(*stream); !started)
    return started;
  stream->status_ = std::move(status);
  stream->trailers_ = trailers;
  stream->finished_ = true;
  const auto result = nghttp2_session_resume_data(impl_->native_, id);
  if (result != 0 && result != NGHTTP2_ERR_INVALID_ARGUMENT)
    return std::unexpected(Failure("Cannot finish HTTP/2 response data"));
  return {};
}
auto MockGrpcServer::CancelledStreams() const noexcept -> std::size_t {
  return impl_->cancelled_;
}
auto MockGrpcServer::WindowUpdatesReceived() const noexcept -> std::size_t {
  return impl_->window_updates_;
}
auto MockGrpcServer::ActiveStreams() const noexcept -> std::size_t {
  return std::ranges::count_if(
      impl_->streams_, [](const auto &stream) { return stream.id_ != 0; });
}

} // namespace greptimedb
