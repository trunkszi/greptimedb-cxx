#include "greptimedb/transport/channel_internal.h"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <limits>

#include <grpcxx/core/timeout.h>
#include <grpcxx/h2/content_type.h>

namespace greptimedb {
namespace {
using grpcxx::Status;
using grpcxx::StatusCode;
auto H2Error(int code) noexcept -> Status {
  return {StatusCode::kInternal,
          std::string("HTTP/2: ") + nghttp2_strerror(code)};
}
auto Nv(std::string_view key, std::string_view value) noexcept -> nghttp2_nv {
  return {reinterpret_cast<std::uint8_t *>(const_cast<char *>(key.data())),
          reinterpret_cast<std::uint8_t *>(const_cast<char *>(value.data())),
          key.size(), value.size(), NGHTTP2_NV_FLAG_NONE};
}
auto ParseInteger(std::string_view value, int &output) noexcept -> bool {
  const auto result =
      std::from_chars(value.data(), value.data() + value.size(), output);
  return result.ec == std::errc{} && result.ptr == value.data() + value.size();
}
auto AddBinaryHeader(grpcxx::MetadataMap &metadata, std::string_view key,
                     std::string_view value) noexcept
    -> std::expected<void, Status> {
  while (true) {
    const auto comma = value.find(',');
    auto part = value.substr(0, comma);
    while (part.starts_with(' ') || part.starts_with('\t'))
      part.remove_prefix(1);
    while (part.ends_with(' ') || part.ends_with('\t'))
      part.remove_suffix(1);
    std::string padded(part);
    while (padded.size() % 4 != 0)
      padded.push_back('=');
    auto decoded = grpcxx::MetadataMap::Base64Decode(padded);
    if (!decoded)
      return std::unexpected(
          Status{StatusCode::kInternal, "invalid binary metadata"});
    auto added = metadata.AddBinary(key, *decoded);
    if (!added)
      return added;
    if (comma == value.npos)
      break;
    value.remove_prefix(comma + 1);
  }
  return {};
}
auto Now() noexcept -> std::uint64_t {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
} // namespace

SynchronousChannel::Impl::Impl(std::unique_ptr<EndpointTransport> value,
                               std::string_view name) noexcept
    : transport(std::move(value)), authority(name),
      mux(grpcxx::StreamMultiplexer::Options{
          .max_streams = 16,
          .queue_depth = 1,
          .max_message_bytes = kChannelMessageLimit,
          .high_water_bytes = kChannelMessageLimit,
          .low_water_bytes = kChannelMessageLimit / 2,
          .initial_window = kChannelQueueLimit}),
      compression(256), secure(transport->IsTls()) {}
SynchronousChannel::Impl::~Impl() {
  if (h2)
    nghttp2_session_del(h2);
}
auto SynchronousChannel::Impl::Initialize() noexcept
    -> std::expected<void, Status> {
  if (!mux.Ready())
    return std::unexpected(Status{StatusCode::kResourceExhausted,
                                  "stream slots allocation failed"});
  nghttp2_session_callbacks *callbacks = nullptr;
  int result = nghttp2_session_callbacks_new(&callbacks);
  if (result < 0)
    return std::unexpected(H2Error(result));
  nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                          OnBeginHeaders);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, OnHeader);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, OnFrame);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, OnData);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, OnClose);
  nghttp2_option *options = nullptr;
  result = nghttp2_option_new(&options);
  if (result == 0) {
    nghttp2_option_set_no_auto_window_update(options, 1);
    result = nghttp2_session_client_new2(&h2, callbacks, this, options);
    nghttp2_option_del(options);
  }
  nghttp2_session_callbacks_del(callbacks);
  if (result < 0)
    return std::unexpected(H2Error(result));
  const nghttp2_settings_entry settings[] = {
      {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 16}};
  result = nghttp2_submit_settings(h2, NGHTTP2_FLAG_NONE, settings, 2);
  if (result < 0)
    return std::unexpected(H2Error(result));
  mux.SetWireHandler([this](const grpcxx::StreamFrame &frame) noexcept {
    return Wire(frame);
  });
  return {};
}
auto SynchronousChannel::Impl::Find(std::int32_t id) noexcept
    -> RpcStream::State * {
  for (auto *stream : streams)
    if (stream && stream->id == id)
      return stream;
  return nullptr;
}
auto SynchronousChannel::Impl::CheckOwner() const noexcept
    -> std::expected<void, Status> {
  if (std::this_thread::get_id() != owner)
    return std::unexpected(Status{StatusCode::kFailedPrecondition,
                                  "channel belongs to another caller thread"});
  return {};
}
auto SynchronousChannel::Impl::Wire(const grpcxx::StreamFrame &frame) noexcept
    -> std::expected<void, Status> {
  auto *stream = streams[frame.stream.index];
  if (!stream || stream->handle != frame.stream) {
    if (frame.kind == grpcxx::StreamFrameKind::kReset)
      return {};
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "stream handle is detached"});
  }
  switch (frame.kind) {
  case grpcxx::StreamFrameKind::kHeaders: {
    std::array<char, 9> timeout_buffer{};
    const auto timeout = grpcxx::GrpcTimeout::FormatTo(
        static_cast<std::uint64_t>(
            std::max(std::int64_t{0}, stream->options.timeout.count())),
        timeout_buffer);
    std::vector<nghttp2_nv> headers{
        Nv(":method", "POST"),
        Nv(":scheme", secure ? "https" : "http"),
        Nv(":authority", authority),
        Nv(":path", stream->path),
        Nv("content-type", "application/grpc"),
        Nv("te", "trailers"),
        Nv("grpc-encoding", grpcxx::CompressionRegistry::EncodingName(
                                stream->options.compression)),
        Nv("grpc-accept-encoding",
           grpcxx::CompressionRegistry::AcceptEncoding()),
        Nv("grpc-timeout", timeout)};
    for (const auto &entry : frame.metadata->Entries())
      headers.push_back(Nv(entry.key, entry.value));
    nghttp2_data_provider provider{};
    provider.source.ptr = stream;
    provider.read_callback = ReadData;
    const auto id = nghttp2_submit_request(h2, nullptr, headers.data(),
                                           headers.size(), &provider, stream);
    if (id < 0)
      return std::unexpected(H2Error(id));
    stream->id = id;
    break;
  }
  case grpcxx::StreamFrameKind::kMessage: {
    if (!frame.message_end)
      return std::unexpected(Status{
          StatusCode::kInternal, "unexpected fragmented multiplexed message"});
    auto encoded =
        compression.Compress(stream->options.compression, frame.payload);
    if (!encoded)
      return std::unexpected(encoded.error());
    stream->outgoing_bytes += encoded->WireSize();
    stream->outgoing.push_back(std::move(*encoded));
    stream->window_credit += frame.payload.size();
    const auto resumed = nghttp2_session_resume_data(h2, stream->id);
    if (resumed < 0 && resumed != NGHTTP2_ERR_INVALID_ARGUMENT)
      return std::unexpected(H2Error(resumed));
    break;
  }
  case grpcxx::StreamFrameKind::kHalfClose:
    stream->request_ended = true;
    (void)nghttp2_session_resume_data(h2, stream->id);
    break;
  case grpcxx::StreamFrameKind::kReset: {
    if (stream->id == 0 || stream->peer_closed)
      break;
    const auto result = nghttp2_submit_rst_stream(h2, NGHTTP2_FLAG_NONE,
                                                  stream->id, NGHTTP2_CANCEL);
    if (result < 0)
      return std::unexpected(H2Error(result));
    break;
  }
  case grpcxx::StreamFrameKind::kTrailers:
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client cannot send trailers"});
  }
  return {};
}
auto SynchronousChannel::Impl::ReadData(nghttp2_session *, std::int32_t,
                                        std::uint8_t *output,
                                        std::size_t length,
                                        std::uint32_t *flags,
                                        nghttp2_data_source *source,
                                        void *) noexcept -> ssize_t {
  auto &stream = *static_cast<RpcStream::State *>(source->ptr);
  if (stream.outgoing.empty()) {
    if (stream.request_ended) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      return 0;
    }
    return NGHTTP2_ERR_DEFERRED;
  }
  const auto bytes =
      stream.outgoing.front().WireBytes().subspan(stream.outgoing_offset);
  const auto count = std::min(bytes.size(), length);
  if (count)
    std::memcpy(output, bytes.data(), count);
  stream.outgoing_offset += count;
  stream.outgoing_bytes -= count;
  if (count == bytes.size()) {
    stream.outgoing.pop_front();
    stream.outgoing_offset = 0;
  }
  if (stream.outgoing.empty() && stream.request_ended)
    *flags |= NGHTTP2_DATA_FLAG_EOF;
  return static_cast<ssize_t>(count);
}
auto SynchronousChannel::Impl::OnBeginHeaders(nghttp2_session *,
                                              const nghttp2_frame *frame,
                                              void *context) noexcept -> int {
  auto *stream = static_cast<Impl *>(context)->Find(frame->hd.stream_id);
  if (stream) {
    stream->block.Clear();
    stream->header_bytes = 0;
  }
  return 0;
}
auto SynchronousChannel::Impl::OnHeader(
    nghttp2_session *, const nghttp2_frame *frame, const std::uint8_t *name,
    std::size_t name_length, const std::uint8_t *value,
    std::size_t value_length, std::uint8_t, void *context) noexcept -> int {
  auto *self = static_cast<Impl *>(context);
  auto *stream = self->Find(frame->hd.stream_id);
  if (!stream || stream->terminal)
    return 0;
  stream->header_bytes += name_length + value_length;
  if (stream->header_bytes > 65536) {
    (void)self->Abort(*stream, {StatusCode::kResourceExhausted,
                                "response metadata exceeds limit"});
    return 0;
  }
  const std::string_view key(reinterpret_cast<const char *>(name), name_length);
  const std::string_view text(reinterpret_cast<const char *>(value),
                              value_length);
  if (key == ":status") {
    if (!ParseInteger(text, stream->http_status))
      (void)self->Abort(*stream,
                        {StatusCode::kInternal, "invalid HTTP status"});
    return 0;
  }
  if (key.starts_with(':'))
    return 0;
  if ((key == "grpc-status" || key == "grpc-message" ||
       key == "grpc-encoding" || key == "content-type") &&
      stream->block.WireValue(key)) {
    (void)self->Abort(*stream, {StatusCode::kInternal,
                                "duplicate reserved response metadata"});
    return 0;
  }
  if (key == "content-type") {
    stream->content_type_valid = grpcxx::h2_detail::ValidContentType(text);
  }
  if (key == "grpc-encoding") {
    if (stream->received_headers) {
      (void)self->Abort(*stream, {StatusCode::kInternal,
                                  "grpc-encoding is not allowed in trailers"});
      return 0;
    }
    auto algorithm = grpcxx::CompressionRegistry::ParseEncoding(text);
    if (!algorithm) {
      (void)self->Abort(*stream, algorithm.error());
      return 0;
    }
    stream->incoming_encoding = *algorithm;
  }
  std::expected<void, Status> added;
  if (key.ends_with("-bin")) {
    added = AddBinaryHeader(stream->block, key, text);
  } else {
    added = stream->block.Add(key, text);
  }
  if (!added)
    (void)self->Abort(*stream, added.error());
  return 0;
}
auto SynchronousChannel::Impl::OnFrame(nghttp2_session *,
                                       const nghttp2_frame *frame,
                                       void *context) noexcept -> int {
  auto *self = static_cast<Impl *>(context);
  if (frame->hd.type == NGHTTP2_GOAWAY) {
    self->draining = true;
    for (auto *stream : self->streams) {
      if (stream && !stream->terminal &&
          (stream->id > frame->goaway.last_stream_id ||
           frame->goaway.error_code != NGHTTP2_NO_ERROR))
        (void)self->Abort(
            *stream, {StatusCode::kUnavailable, "peer sent HTTP/2 GOAWAY"});
    }
    return 0;
  }
  auto *stream = self->Find(frame->hd.stream_id);
  if (!stream || stream->terminal)
    return 0;
  if (frame->hd.type == NGHTTP2_HEADERS) {
    if (!stream->received_headers) {
      stream->received_headers = true;
      stream->initial = stream->block;
      auto received = self->mux.ReceiveHeaders(stream->handle, stream->initial);
      if (!received) {
        (void)self->Abort(*stream, received.error());
        return 0;
      }
      if (stream->http_status != 200 || !stream->content_type_valid) {
        auto status =
            stream->http_status != 200
                ? Status::FromHttpStatus(stream->http_status)
                : Status{StatusCode::kInternal, "response is not gRPC"};
        (void)self->Abort(*stream, std::move(status));
        return 0;
      }
    }
    if (auto code = stream->block.Get("grpc-status")) {
      int numeric{};
      if (!ParseInteger(*code, numeric) || numeric < 0 || numeric > 16) {
        (void)self->Abort(
            *stream, {StatusCode::kInternal, "invalid grpc-status trailer"});
        return 0;
      }
      stream->received_status = true;
      std::string message;
      if (auto value = stream->block.Get("grpc-message")) {
        auto decoded = Status::PercentDecode(*value);
        if (!decoded) {
          (void)self->Abort(*stream, decoded.error());
          return 0;
        }
        message = std::move(*decoded);
      }
      stream->status =
          Status{static_cast<StatusCode>(numeric), std::move(message)};
      stream->trailers = stream->block;
      if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0)
        (void)self->Abort(
            *stream, {StatusCode::kInternal, "grpc-status without END_STREAM"});
    }
  }
  if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0 && !stream->terminal) {
    if (!stream->received_status) {
      (void)self->Abort(*stream, {StatusCode::kInternal,
                                  "response ended without grpc-status"});
      return 0;
    }
    if (stream->partial_frame_bytes != 0 ||
        stream->parser.State() !=
            grpcxx::LengthPrefixedFrame::ParseState::kReadingHeader ||
        stream->parser.BufferedPayloadBytes() != 0) {
      (void)self->Abort(*stream,
                        {StatusCode::kDataLoss, "truncated gRPC message"});
      return 0;
    }
    auto trailers = self->mux.ReceiveTrailers(stream->handle, stream->status,
                                              stream->trailers);
    if (!trailers) {
      (void)self->Abort(*stream, trailers.error());
      return 0;
    }
    stream->terminal = true;
    stream->phase = RpcStream::State::Phase::kClosed;
  }
  return 0;
}
auto SynchronousChannel::Impl::OnData(nghttp2_session *, std::uint8_t,
                                      std::int32_t id,
                                      const std::uint8_t *bytes,
                                      std::size_t size, void *context) noexcept
    -> int {
  auto *self = static_cast<Impl *>(context);
  auto *stream = self->Find(id);
  const auto consumed =
      stream ? nghttp2_session_consume(self->h2, id, size)
             : nghttp2_session_consume_connection(self->h2, size);
  if (consumed != 0)
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  if (!stream || stream->terminal)
    return 0;
  if (!stream->received_headers) {
    (void)self->Abort(*stream,
                      {StatusCode::kInternal, "DATA before response headers"});
    return 0;
  }
  stream->partial_frame_bytes += size;
  auto frames =
      stream->parser.Feed({reinterpret_cast<const std::byte *>(bytes), size});
  if (!frames) {
    (void)self->Abort(*stream, frames.error());
    return 0;
  }
  for (auto &message : *frames) {
    stream->partial_frame_bytes =
        stream->partial_frame_bytes > message.WireSize()
            ? stream->partial_frame_bytes - message.WireSize()
            : 0;
    auto decoded = self->compression.Decompress(
        message.Compressed() ? stream->incoming_encoding
                             : grpcxx::CompressionAlgorithm::kIdentity,
        message.Payload(), kChannelMessageLimit);
    if (!decoded) {
      (void)self->Abort(*stream, decoded.error());
      return 0;
    }
    if (message.Compressed() &&
        stream->incoming_encoding == grpcxx::CompressionAlgorithm::kIdentity) {
      (void)self->Abort(*stream,
                        {StatusCode::kInternal,
                         "compressed message without negotiated encoding"});
      return 0;
    }
    if (stream->incoming_bytes + decoded->PayloadSize() > kChannelQueueLimit ||
        stream->incoming.size() >= 256) {
      (void)self->Abort(*stream, {StatusCode::kResourceExhausted,
                                  "response queue exceeds limit"});
      return 0;
    }
    auto received =
        self->mux.ReceiveMessage(stream->handle, decoded->Payload());
    if (!received) {
      (void)self->Abort(*stream, received.error());
      return 0;
    }
    stream->incoming_bytes += decoded->PayloadSize();
    stream->incoming.emplace_back(decoded->Payload().begin(),
                                  decoded->Payload().end());
    ++stream->received_messages;
  }
  return 0;
}
auto SynchronousChannel::Impl::OnClose(nghttp2_session *, std::int32_t id,
                                       std::uint32_t error,
                                       void *context) noexcept -> int {
  auto *self = static_cast<Impl *>(context);
  auto *stream = self->Find(id);
  if (!stream)
    return 0;
  stream->peer_closed = true;
  if (!stream->terminal) {
    (void)self->mux.ReceiveReset(stream->handle);
    stream->status = Status{error == NGHTTP2_CANCEL ? StatusCode::kCancelled
                                                    : StatusCode::kUnavailable,
                            "HTTP/2 stream reset before trailers"};
    stream->terminal = true;
    stream->local_failure = true;
    stream->phase = RpcStream::State::Phase::kClosed;
  }
  return 0;
}
auto SynchronousChannel::Impl::Abort(RpcStream::State &stream,
                                     Status status) noexcept
    -> std::expected<void, Status> {
  if (stream.terminal)
    return {};
  stream.status = std::move(status);
  stream.local_failure = true;
  stream.terminal = true;
  stream.phase = RpcStream::State::Phase::kCancelled;
  auto cancelled = mux.Cancel(stream.handle);
  if (!cancelled)
    return std::unexpected(cancelled.error());
  return {};
}
void SynchronousChannel::Impl::FailAll(const Status &status) noexcept {
  failure = status;
  for (auto *stream : streams)
    if (stream && !stream->terminal)
      (void)Abort(*stream, status);
}
auto SynchronousChannel::Impl::Flush() noexcept -> std::expected<void, Status> {
  for (unsigned frame = 0; frame < 64; ++frame) {
    if (write_offset < pending_write.size()) {
      auto count = transport->Write(
          std::span<const std::byte>(pending_write).subspan(write_offset));
      if (!count)
        return std::unexpected(count.error());
      write_offset += *count;
      if (write_offset < pending_write.size())
        return {};
    }
    pending_write.clear();
    write_offset = 0;
    const std::uint8_t *bytes = nullptr;
    const auto count = nghttp2_session_mem_send(h2, &bytes);
    if (count < 0)
      return std::unexpected(H2Error(static_cast<int>(count)));
    if (count == 0)
      return {};
    pending_write.assign(reinterpret_cast<const std::byte *>(bytes),
                         reinterpret_cast<const std::byte *>(bytes) + count);
  }
  return {};
}
auto SynchronousChannel::Impl::Poll() noexcept -> std::expected<void, Status> {
  auto owned = CheckOwner();
  if (!owned)
    return owned;
  if (polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel polling"});
  if (!failure.Ok())
    return std::unexpected(failure);
  polling = true;
  const auto work = [this]() noexcept -> std::expected<void, Status> {
    const auto now = std::chrono::steady_clock::now();
    for (auto *stream : streams) {
      if (stream && !stream->terminal && now >= stream->deadline)
        (void)Abort(*stream,
                    {StatusCode::kDeadlineExceeded, "RPC deadline exceeded"});
    }
    auto multiplexed = mux.PollOnce({}, Now());
    if (!multiplexed)
      return std::unexpected(multiplexed.error());
    for (auto *stream : streams) {
      if (stream && stream->window_credit != 0) {
        auto updated = mux.UpdateWindow(stream->handle, stream->window_credit);
        stream->window_credit = 0;
        if (!updated && !stream->terminal)
          return std::unexpected(updated.error());
      }
    }
    auto polled = transport->PollOnce();
    if (!polled)
      return std::unexpected(polled.error());
    auto flushed = Flush();
    if (!flushed)
      return std::unexpected(flushed.error());
    if (progress) {
      auto progressed = progress(progress_context);
      if (!progressed)
        return std::unexpected(progressed.error());
    }
    for (unsigned i = 0; i < 64; ++i) {
      auto count = transport->Read(read_buffer);
      if (!count)
        return std::unexpected(count.error());
      if (*count == 0)
        break;
      const auto received = nghttp2_session_mem_recv(
          h2, reinterpret_cast<const std::uint8_t *>(read_buffer.data()),
          *count);
      if (received < 0)
        return std::unexpected(H2Error(static_cast<int>(received)));
      if (static_cast<std::size_t>(received) != *count)
        return std::unexpected(Status{StatusCode::kInternal,
                                      "HTTP/2 input was not fully consumed"});
    }
    return Flush();
  }();
  polling = false;
  if (!work)
    FailAll(work.error());
  for (std::size_t index = 0; index < retired.size(); ++index) {
    if (retired[index] && retired[index]->peer_closed) {
      auto released = mux.Release(retired[index]->handle);
      if (released) {
        streams[index] = nullptr;
        retired[index].reset();
      }
    }
  }
  return work;
}
} // namespace greptimedb
