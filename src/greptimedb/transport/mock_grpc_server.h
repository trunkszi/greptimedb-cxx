#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include <grpcxx/compression/compression_algorithm.h>
#include <grpcxx/core/metadata.h>
#include <grpcxx/core/status.h>

#include "greptimedb/transport/endpoint_transport.h"

namespace greptimedb {

/** Borrowed request event, valid only during the synchronous handler callback.
 */
struct MockRequest final {
  enum class Event : std::uint8_t { kMessage, kHalfClose, kCancel };
  std::int32_t stream_id{};
  std::string_view path;
  std::span<const std::byte> payload;
  const grpcxx::MetadataMap &metadata;
  Event event{Event::kMessage};
};

/** Bounds for a caller-driven HTTP/2 gRPC test peer. */
struct MockServerOptions final {
  std::size_t max_message_bytes{4U * 1024U * 1024U};
  std::size_t max_queued_bytes{8U * 1024U * 1024U};
  grpcxx::CompressionAlgorithm response_compression{
      grpcxx::CompressionAlgorithm::kIdentity};
};

/** Real HTTP/2 peer for unary and all three streaming RPC shapes.
 * No worker thread is created. PollOnce drives the owned transport; install it
 * as the client's progress hook to run both peers on the same caller thread.
 * A handler must not destroy this server or reenter PollOnce from its callback.
 */
class MockGrpcServer final {
public:
  using Handler = void (*)(void *, MockGrpcServer &,
                           const MockRequest &) noexcept;
  [[nodiscard]] static auto Create(std::unique_ptr<EndpointTransport> transport,
                                   Handler handler, void *context = nullptr,
                                   MockServerOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<MockGrpcServer>, grpcxx::Status>;
  ~MockGrpcServer();
  MockGrpcServer(const MockGrpcServer &) = delete;
  auto operator=(const MockGrpcServer &) -> MockGrpcServer & = delete;
  /** Bounded progress, including partial transport writes and HTTP/2 flow
   * control. */
  [[nodiscard]] auto PollOnce() noexcept -> std::expected<void, grpcxx::Status>;
  /** Metadata must be set before the first Send or Finish for this stream. */
  [[nodiscard]] auto
  SetInitialMetadata(std::int32_t stream_id,
                     const grpcxx::MetadataMap &metadata) noexcept
      -> std::expected<void, grpcxx::Status>;
  [[nodiscard]] auto Send(std::int32_t stream_id,
                          std::span<const std::byte> payload) noexcept
      -> std::expected<void, grpcxx::Status>;
  /** Queues trailers after all previously accepted messages. */
  [[nodiscard]] auto Finish(std::int32_t stream_id, grpcxx::Status status = {},
                            const grpcxx::MetadataMap &trailers = {}) noexcept
      -> std::expected<void, grpcxx::Status>;
  [[nodiscard]] auto CancelledStreams() const noexcept -> std::size_t;
  [[nodiscard]] auto ActiveStreams() const noexcept -> std::size_t;
  [[nodiscard]] auto WindowUpdatesReceived() const noexcept -> std::size_t;

private:
  struct Impl;
  explicit MockGrpcServer(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace greptimedb
