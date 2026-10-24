#include "greptimedb/transport/channel_internal.h"

#include "greptimedb/core/config.h"
#include <algorithm>
#include <charconv>
#include <new>
#include <utility>

namespace greptimedb {
namespace {
using grpcxx::Status;
using grpcxx::StatusCode;
auto Invalid(std::string text) noexcept -> Status {
  return {StatusCode::kInvalidArgument, std::move(text)};
}
auto Exhausted() noexcept -> Status {
  return {StatusCode::kResourceExhausted, "channel allocation failed"};
}
auto Reserved(std::string_view key) noexcept -> bool {
  return key.starts_with(':') || key.starts_with("grpc-") ||
         key == "content-type" || key == "te" || key == "host" ||
         key == "connection" || key == "transfer-encoding";
}
auto ValidText(std::string_view text) noexcept -> bool {
  return std::ranges::all_of(
      text, [](unsigned char c) noexcept { return c > 32 && c < 127; });
}
} // namespace

SynchronousChannel::SynchronousChannel(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SynchronousChannel::~SynchronousChannel() = default;
auto SynchronousChannel::Create(std::unique_ptr<EndpointTransport> transport,
                                std::string_view authority) noexcept
    -> std::expected<std::unique_ptr<SynchronousChannel>, Status> {
  if (!transport || !transport->IsOpen() || authority.empty() ||
      !ValidText(authority))
    return std::unexpected(
        Invalid("channel requires an open transport and valid authority"));
  std::unique_ptr<Impl> impl{new (std::nothrow)
                                 Impl(std::move(transport), authority)};
  if (!impl)
    return std::unexpected(Exhausted());
  auto ready = impl->Initialize();
  if (!ready)
    return std::unexpected(ready.error());
  std::unique_ptr<SynchronousChannel> result{
      new (std::nothrow) SynchronousChannel(std::move(impl))};
  if (!result)
    return std::unexpected(Exhausted());
  return result;
}
auto SynchronousChannel::Connect(std::string_view endpoint,
                                 const TlsOptions &tls) noexcept
    -> std::expected<std::unique_ptr<SynchronousChannel>, Status> {
  auto parsed = Endpoint::Parse(endpoint);
  if (!parsed)
    return std::unexpected(Invalid(std::string(parsed.error().Message())));
  return Connect(*parsed, tls);
}
auto SynchronousChannel::Connect(const Endpoint &endpoint,
                                 const TlsOptions &tls) noexcept
    -> std::expected<std::unique_ptr<SynchronousChannel>, Status> {
  return Connect(endpoint, ClientOptions{endpoint}, tls);
}
auto SynchronousChannel::Connect(const Endpoint &endpoint,
                                 const ClientOptions &options,
                                 const TlsOptions &tls) noexcept
    -> std::expected<std::unique_ptr<SynchronousChannel>, Status> {
  if (auto valid = options.Validate(); !valid)
    return std::unexpected(Invalid(std::string(valid.error().Message())));
  const TransportOptions transport{options.connect_timeout,
                                   options.read_timeout, options.write_timeout,
                                   options.tcp_nodelay};
  std::expected<std::unique_ptr<EndpointTransport>, Status> wire =
      std::unexpected(Status{StatusCode::kInvalidArgument});
  switch (endpoint.Scheme()) {
  case TransportScheme::kGrpc:
    wire = EndpointTransport::ConnectTcp(endpoint.Host(), endpoint.Port(),
                                         transport);
    break;
  case TransportScheme::kGrpcs:
    wire = EndpointTransport::ConnectTls(endpoint.Host(), endpoint.Port(), tls,
                                         transport);
    break;
  case TransportScheme::kUnix:
    wire = EndpointTransport::ConnectUnix(endpoint.SocketAddress(), transport);
    break;
  }
  if (!wire)
    return std::unexpected(wire.error());
  return Create(std::move(*wire), endpoint.Scheme() == TransportScheme::kUnix
                                      ? "localhost"
                                      : endpoint.Authority());
}
auto SynchronousChannel::NativeHandle() const noexcept -> int {
  return impl_->CheckOwner() ? impl_->transport->NativeHandle() : -1;
}
void SynchronousChannel::SetProgressHook(void *context,
                                         ProgressHook hook) noexcept {
  if (!impl_->CheckOwner() || impl_->polling)
    return;
  impl_->progress_context = context;
  impl_->progress = hook;
}
auto SynchronousChannel::PollOnce() noexcept -> std::expected<void, Status> {
  return impl_->Poll();
}
auto SynchronousChannel::Open(std::string_view path, grpcxx::RpcKind kind,
                              CallOptions options) noexcept
    -> std::expected<std::unique_ptr<RpcStream>, Status> {
  auto owned = impl_->CheckOwner();
  if (!owned)
    return std::unexpected(owned.error());
  if (impl_->polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel operation"});
  if (!impl_->failure.Ok())
    return std::unexpected(impl_->failure);
  if (impl_->draining)
    return std::unexpected(Status{StatusCode::kUnavailable,
                                  "HTTP/2 channel is draining after GOAWAY"});
  if (!path.starts_with('/') || path.size() > 8192 || !ValidText(path) ||
      path.find_first_of("?#") != path.npos || path.find('/', 1) == path.npos)
    return std::unexpected(Invalid("RPC path must be /service/method"));
  if (options.timeout.count() <= 0)
    return std::unexpected(Invalid("RPC timeout must be positive"));
  if (grpcxx::CompressionRegistry::EncodingName(options.compression).empty())
    return std::unexpected(
        Invalid("unsupported request compression algorithm"));
  const auto valid_kind = [kind]() noexcept {
    switch (kind) {
    case grpcxx::RpcKind::kUnary:
    case grpcxx::RpcKind::kClientStreaming:
    case grpcxx::RpcKind::kServerStreaming:
    case grpcxx::RpcKind::kBidiStreaming:
      return true;
    }
    return false;
  }();
  if (!valid_kind)
    return std::unexpected(Invalid("unsupported RPC kind"));
  std::size_t metadata_bytes = 0;
  for (const auto &entry : options.metadata.Entries()) {
    if (Reserved(entry.key))
      return std::unexpected(
          Invalid("reserved request metadata key: " + entry.key));
    metadata_bytes += entry.key.size() + entry.value.size();
  }
  if (metadata_bytes > 65536)
    return std::unexpected(Status{StatusCode::kResourceExhausted,
                                  "request metadata exceeds limit"});
  std::unique_ptr<RpcStream::State> state{new (std::nothrow) RpcStream::State};
  if (!state)
    return std::unexpected(Exhausted());
  auto handle = impl_->mux.Open(kind);
  if (!handle)
    return std::unexpected(handle.error());
  state->handle = *handle;
  state->kind = kind;
  state->path = path;
  state->options = std::move(options);
  const auto now = std::chrono::steady_clock::now();
  const auto remaining = std::chrono::steady_clock::time_point::max() - now;
  state->deadline = state->options.timeout >= remaining
                        ? std::chrono::steady_clock::time_point::max()
                        : now + state->options.timeout;
  impl_->streams[handle->index] = state.get();
  std::unique_ptr<RpcStream> result{new (std::nothrow)
                                        RpcStream(*this, std::move(state))};
  if (!result) {
    (void)impl_->mux.Cancel(*handle);
    (void)impl_->mux.PollOnce({}, 0);
    auto released = impl_->mux.Release(*handle);
    if (released) {
      impl_->streams[handle->index] = nullptr;
    } else {
      state->peer_closed = true;
      impl_->retired[handle->index] = std::move(state);
    }
    return std::unexpected(Exhausted());
  }
  auto headers =
      impl_->mux.SendHeaders(*handle, result->state_->options.metadata);
  if (!headers)
    return std::unexpected(headers.error());
  auto polled = impl_->mux.PollOnce({}, 0);
  if (!polled)
    return std::unexpected(polled.error());
  return result;
}
auto SynchronousChannel::Unary(std::string_view path,
                               std::span<const std::byte> payload,
                               CallOptions options) noexcept
    -> std::expected<RpcResponse, Status> {
  auto stream = Open(path, grpcxx::RpcKind::kUnary, std::move(options));
  if (!stream)
    return std::unexpected(stream.error());
  auto sent = (*stream)->Send(payload);
  if (!sent)
    return std::unexpected(sent.error());
  return (*stream)->Finish();
}
auto SynchronousChannel::ClientStreamCall(std::string_view path,
                                          CallOptions options) noexcept
    -> std::expected<std::unique_ptr<RpcStream>, Status> {
  return Open(path, grpcxx::RpcKind::kClientStreaming, std::move(options));
}
auto SynchronousChannel::ServerStreamCall(std::string_view path,
                                          std::span<const std::byte> payload,
                                          CallOptions options) noexcept
    -> std::expected<std::unique_ptr<RpcStream>, Status> {
  auto stream =
      Open(path, grpcxx::RpcKind::kServerStreaming, std::move(options));
  if (!stream)
    return std::unexpected(stream.error());
  auto sent = (*stream)->Send(payload);
  if (!sent)
    return std::unexpected(sent.error());
  auto closed = (*stream)->HalfClose();
  if (!closed)
    return std::unexpected(closed.error());
  return stream;
}
auto SynchronousChannel::BidiStreamCall(std::string_view path,
                                        CallOptions options) noexcept
    -> std::expected<std::unique_ptr<RpcStream>, Status> {
  return Open(path, grpcxx::RpcKind::kBidiStreaming, std::move(options));
}

RpcStream::RpcStream(SynchronousChannel &channel,
                     std::unique_ptr<State> state) noexcept
    : channel_(&channel), state_(std::move(state)) {}
RpcStream::~RpcStream() {
  auto &impl = *channel_->impl_;
  if (!state_->peer_closed) {
    if (!state_->terminal)
      (void)impl.Abort(*state_,
                       {StatusCode::kCancelled, "stream handle destroyed"});
    else {
      (void)impl.mux.Cancel(state_->handle);
      (void)nghttp2_submit_rst_stream(impl.h2, NGHTTP2_FLAG_NONE, state_->id,
                                      NGHTTP2_CANCEL);
    }
  }
  (void)impl.mux.PollOnce({}, 0);
  (void)impl.Flush();
  const auto index = state_->handle.index;
  if (state_->peer_closed || state_->id == 0) {
    auto released = impl.mux.Release(state_->handle);
    if (released) {
      impl.streams[index] = nullptr;
    } else {
      impl.retired[index] = std::move(state_);
    }
  } else {
    impl.retired[index] = std::move(state_);
  }
}
auto RpcStream::Send(std::span<const std::byte> bytes) noexcept
    -> std::expected<void, Status> {
  auto owned = channel_->impl_->CheckOwner();
  if (!owned)
    return owned;
  if (channel_->impl_->polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel operation"});
  if (state_->phase != State::Phase::kOpen || state_->terminal)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "stream is not writable"});
  if (std::chrono::steady_clock::now() >= state_->deadline) {
    (void)channel_->impl_->Abort(
        *state_, {StatusCode::kDeadlineExceeded, "RPC deadline exceeded"});
    (void)channel_->PollOnce();
    return std::unexpected(state_->status);
  }
  if (bytes.size() > kChannelMessageLimit)
    return std::unexpected(Status{StatusCode::kResourceExhausted,
                                  "request message exceeds limit"});
  while (state_->outgoing_bytes + bytes.size() + 5 > kChannelQueueLimit) {
    auto polled = channel_->PollOnce();
    if (!polled)
      return std::unexpected(polled.error());
    if (state_->terminal)
      return std::unexpected(state_->status);
  }
  auto accepted = channel_->impl_->mux.Send(state_->handle, bytes);
  if (!accepted)
    return std::unexpected(accepted.error());
  ++state_->sent_messages;
  auto polled = channel_->impl_->mux.PollOnce({}, 0);
  if (!polled) {
    (void)channel_->impl_->Abort(*state_, polled.error());
    return std::unexpected(polled.error());
  }
  if (state_->window_credit != 0) {
    auto updated = channel_->impl_->mux.UpdateWindow(state_->handle,
                                                     state_->window_credit);
    state_->window_credit = 0;
    if (!updated)
      return std::unexpected(updated.error());
  }
  return {};
}
auto RpcStream::HalfClose() noexcept -> std::expected<void, Status> {
  auto owned = channel_->impl_->CheckOwner();
  if (!owned)
    return owned;
  if (channel_->impl_->polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel operation"});
  if (state_->phase != State::Phase::kOpen || state_->terminal)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "stream cannot half-close"});
  if (std::chrono::steady_clock::now() >= state_->deadline) {
    (void)channel_->impl_->Abort(
        *state_, {StatusCode::kDeadlineExceeded, "RPC deadline exceeded"});
    (void)channel_->PollOnce();
    return std::unexpected(state_->status);
  }
  if ((state_->kind == grpcxx::RpcKind::kUnary ||
       state_->kind == grpcxx::RpcKind::kServerStreaming) &&
      state_->sent_messages != 1)
    return std::unexpected(Status{StatusCode::kFailedPrecondition,
                                  "RPC requires exactly one request message"});
  auto closed = channel_->impl_->mux.HalfClose(state_->handle);
  if (!closed)
    return std::unexpected(closed.error());
  state_->phase = State::Phase::kHalfClosed;
  auto polled = channel_->impl_->mux.PollOnce({}, 0);
  if (!polled)
    return std::unexpected(polled.error());
  return {};
}
auto RpcStream::TryRecvNext() noexcept
    -> std::expected<std::optional<std::vector<std::byte>>, Status> {
  auto owned = channel_->impl_->CheckOwner();
  if (!owned)
    return std::unexpected(owned.error());
  if (channel_->impl_->polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel operation"});
  if (!state_->terminal && state_->incoming.empty()) {
    auto polled = channel_->PollOnce();
    if (!polled && !state_->terminal)
      return std::unexpected(polled.error());
  }
  if (!state_->incoming.empty()) {
    auto message = std::move(state_->incoming.front());
    state_->incoming.pop_front();
    state_->incoming_bytes -= message.size();
    state_->last_payload = message;
    return std::optional<std::vector<std::byte>>(std::move(message));
  }
  if (state_->terminal && !state_->status.Ok())
    return std::unexpected(state_->status);
  return std::nullopt;
}
auto RpcStream::Read() noexcept
    -> std::expected<std::optional<std::vector<std::byte>>, Status> {
  while (true) {
    auto message = TryRecvNext();
    if (!message || message->has_value() || state_->terminal)
      return message;
  }
}
auto RpcStream::Finish() noexcept -> std::expected<RpcResponse, Status> {
  auto owned = channel_->impl_->CheckOwner();
  if (!owned)
    return std::unexpected(owned.error());
  if (channel_->impl_->polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel operation"});
  if (!state_->terminal && state_->phase == State::Phase::kOpen) {
    auto closed = HalfClose();
    if (!closed)
      return std::unexpected(closed.error());
  }
  while (!state_->terminal) {
    auto polled = channel_->PollOnce();
    if (!polled && !state_->terminal)
      return std::unexpected(polled.error());
  }
  while (!state_->incoming.empty()) {
    state_->last_payload = std::move(state_->incoming.front());
    state_->incoming.pop_front();
  }
  state_->incoming_bytes = 0;
  if (state_->status.Ok() &&
      (state_->kind == grpcxx::RpcKind::kUnary ||
       state_->kind == grpcxx::RpcKind::kClientStreaming) &&
      state_->received_messages != 1) {
    state_->status = {StatusCode::kInternal,
                      "RPC requires exactly one response message"};
    state_->local_failure = true;
  }
  if (state_->local_failure && !state_->status.Ok())
    return std::unexpected(state_->status);
  return RpcResponse{state_->last_payload, state_->initial, state_->trailers,
                     state_->status};
}
auto RpcStream::Cancel() noexcept -> std::expected<void, Status> {
  auto owned = channel_->impl_->CheckOwner();
  if (!owned)
    return owned;
  if (channel_->impl_->polling)
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "recursive channel operation"});
  if (state_->terminal)
    return {};
  auto aborted = channel_->impl_->Abort(
      *state_, {StatusCode::kCancelled, "RPC cancelled"});
  if (!aborted)
    return std::unexpected(aborted.error());
  auto polled = channel_->impl_->mux.PollOnce({}, 0);
  if (!polled)
    return std::unexpected(polled.error());
  return channel_->impl_->Flush();
}
auto RpcStream::Id() const noexcept -> std::int32_t { return state_->id; }
auto RpcStream::Done() const noexcept -> bool {
  return state_->terminal && state_->incoming.empty();
}
auto RpcStream::InitialMetadata() const noexcept
    -> const grpcxx::MetadataMap & {
  return state_->initial;
}
auto RpcStream::TrailingMetadata() const noexcept
    -> const grpcxx::MetadataMap & {
  return state_->trailers;
}
auto RpcStream::FinalStatus() const noexcept -> const Status & {
  return state_->status;
}
} // namespace greptimedb
