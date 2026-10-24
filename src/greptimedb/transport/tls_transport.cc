#include "greptimedb/transport/endpoint_transport.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <new>
#include <utility>

#include "greptimedb/transport/tls_options.h"

namespace greptimedb {
using grpcxx::Status;
using grpcxx::StatusCode;

struct TlsTransport::Impl {
  Impl(std::unique_ptr<EndpointTransport> transport,
       grpcxx::TlsSession value) noexcept
      : wire(std::move(transport)), session(std::move(value)) {}
  std::unique_ptr<EndpointTransport> wire;
  grpcxx::TlsSession session;
  std::array<std::byte, 65536> outgoing{}, incoming{}, plaintext{};
  std::array<std::byte, 16384> pending_plaintext{};
  std::size_t pending_size{}, pending_offset{};
  std::size_t out_offset{}, out_size{}, in_offset{}, in_size{};
  std::size_t plain_offset{}, plain_size{};
  bool verified{}, eof{}, output_failed{}, terminal{}, closed{};
  Status failure;

  void MarkWireFailure(const Status &error, bool read_failed = true) noexcept {
    if (failure.Ok())
      failure = error;
    if (read_failed) {
      eof = true;
      session.MarkTransportEof();
    } else {
      output_failed = true;
    }
  }
  void MarkTerminal(const Status &error) noexcept {
    if (failure.Ok())
      failure = error;
    terminal = true;
  }
};

TlsTransport::TlsTransport(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TlsTransport::~TlsTransport() { Close(); }
auto TlsTransport::Create(std::unique_ptr<EndpointTransport> wire,
                          grpcxx::TlsSession session) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  if (!wire || !wire->IsOpen())
    return std::unexpected(Status{StatusCode::kInvalidArgument,
                                  "TLS requires an open byte transport"});
  std::unique_ptr<Impl> impl{new (std::nothrow)
                                 Impl(std::move(wire), std::move(session))};
  if (!impl)
    return std::unexpected(Status{StatusCode::kResourceExhausted,
                                  "TLS transport allocation failed"});
  std::unique_ptr<EndpointTransport> result{new (std::nothrow)
                                                TlsTransport(std::move(impl))};
  if (!result)
    return std::unexpected(Status{StatusCode::kResourceExhausted,
                                  "TLS transport allocation failed"});
  return result;
}
auto TlsTransport::PollOnce() noexcept -> std::expected<void, Status> {
  if (!IsOpen())
    return std::unexpected(
        Status{StatusCode::kUnavailable, "TLS transport is closed"});
  if (!impl_->eof) {
    auto wire_poll = impl_->wire->PollOnce();
    if (!wire_poll)
      impl_->MarkWireFailure(wire_poll.error());
  }
  for (unsigned tick = 0; tick < 4; ++tick) {
    if (impl_->terminal)
      break;
    if (!impl_->eof && !impl_->output_failed &&
        impl_->out_offset < impl_->out_size) {
      auto written = impl_->wire->Write(
          std::span(impl_->outgoing)
              .subspan(impl_->out_offset, impl_->out_size - impl_->out_offset));
      if (!written)
        impl_->MarkWireFailure(written.error(), false);
      else
        impl_->out_offset += *written;
    }
    if (impl_->out_offset == impl_->out_size && !impl_->output_failed) {
      auto drained = impl_->session.DrainCiphertext(impl_->outgoing);
      if (!drained) {
        impl_->MarkTerminal(drained.error());
        break;
      }
      impl_->out_size = *drained;
      impl_->out_offset = 0;
    }
    if (!impl_->eof && impl_->in_offset == impl_->in_size) {
      auto received = impl_->wire->Read(impl_->incoming);
      if (!received) {
        impl_->MarkWireFailure(received.error());
      } else {
        impl_->in_size = *received;
        impl_->in_offset = 0;
        if (*received == 0 && impl_->output_failed)
          impl_->MarkWireFailure(impl_->failure);
      }
    }
    if (impl_->in_offset < impl_->in_size) {
      auto fed = impl_->session.FeedCiphertext(
          std::span(impl_->incoming)
              .subspan(impl_->in_offset, impl_->in_size - impl_->in_offset));
      if (!fed) {
        impl_->MarkTerminal(fed.error());
        break;
      }
      impl_->in_offset += *fed;
    }
    auto advanced = impl_->session.PollOnce();
    if (!advanced) {
      impl_->MarkTerminal(advanced.error());
      break;
    }
    switch (*advanced) {
    case grpcxx::TlsHandshakeState::kHandshaking:
      break;
    case grpcxx::TlsHandshakeState::kEstablished:
      if (!impl_->verified) {
        auto verified = TlsOptions::VerifySession(impl_->session);
        if (!verified) {
          impl_->MarkTerminal(verified.error());
          break;
        }
        impl_->verified = true;
      }
      break;
    case grpcxx::TlsHandshakeState::kClosing:
      break;
    case grpcxx::TlsHandshakeState::kClosed:
      impl_->MarkTerminal(
          Status{StatusCode::kUnavailable, "TLS peer closed connection"});
      break;
    case grpcxx::TlsHandshakeState::kFailed:
      impl_->MarkTerminal(
          Status{StatusCode::kUnauthenticated, "TLS connection failed"});
      break;
    }
    if (!impl_->verified || impl_->terminal)
      continue;
    if (!impl_->output_failed && !impl_->eof &&
        impl_->pending_offset < impl_->pending_size) {
      auto written = impl_->session.WritePlaintext(
          std::span(impl_->pending_plaintext)
              .subspan(impl_->pending_offset,
                       impl_->pending_size - impl_->pending_offset));
      if (!written) {
        impl_->MarkTerminal(written.error());
        break;
      }
      impl_->pending_offset += *written;
    }
    if (impl_->plain_offset != 0) {
      std::memmove(impl_->plaintext.data(),
                   impl_->plaintext.data() + impl_->plain_offset,
                   impl_->plain_size);
      impl_->plain_offset = 0;
    }
    if (impl_->plain_size == impl_->plaintext.size())
      break;
    auto read = impl_->session.ReadPlaintext(
        std::span(impl_->plaintext).subspan(impl_->plain_size));
    if (!read) {
      impl_->MarkTerminal(read.error());
      break;
    }
    impl_->plain_size += *read;
  }
  if ((impl_->terminal || impl_->output_failed) && impl_->plain_size == 0)
    return std::unexpected(impl_->failure);
  return {};
}
auto TlsTransport::Read(std::span<std::byte> output) noexcept
    -> std::expected<std::size_t, Status> {
  auto polled = PollOnce();
  if (!polled)
    return std::unexpected(polled.error());
  const auto count = std::min(output.size(), impl_->plain_size);
  if (count)
    std::memcpy(output.data(), impl_->plaintext.data() + impl_->plain_offset,
                count);
  impl_->plain_offset += count;
  impl_->plain_size -= count;
  return count;
}
auto TlsTransport::Write(std::span<const std::byte> bytes) noexcept
    -> std::expected<std::size_t, Status> {
  auto polled = PollOnce();
  if (!polled)
    return std::unexpected(polled.error());
  if (impl_->output_failed)
    return std::unexpected(impl_->failure);
  if (!impl_->verified || impl_->eof || impl_->terminal)
    return 0;
  if (impl_->pending_offset != impl_->pending_size || bytes.empty())
    return 0;
  const auto count = std::min(bytes.size(), impl_->pending_plaintext.size());
  std::memcpy(impl_->pending_plaintext.data(), bytes.data(), count);
  impl_->pending_offset = 0;
  impl_->pending_size = count;
  return count;
}
void TlsTransport::Close() noexcept {
  if (impl_ && impl_->wire) {
    impl_->closed = true;
    impl_->wire->Close();
  }
}
auto TlsTransport::IsOpen() const noexcept -> bool {
  return impl_ && impl_->wire && !impl_->closed;
}
auto TlsTransport::IsReady() const noexcept -> bool {
  return impl_ && impl_->verified;
}
auto TlsTransport::NativeHandle() const noexcept -> int {
  return impl_->wire->NativeHandle();
}
} // namespace greptimedb
