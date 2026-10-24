#pragma once

#include "greptimedb/transport/synchronous_channel.h"

#include <array>
#include <deque>
#include <string>
#include <thread>

#include <grpcxx/compression/compression_registry.h>
#include <grpcxx/framing/length_prefixed_frame.h>
#include <grpcxx/streaming/stream_multiplexer.h>
#include <nghttp2/nghttp2.h>

namespace greptimedb {
inline constexpr std::size_t kChannelMessageLimit = 4 * 1024 * 1024;
inline constexpr std::size_t kChannelQueueLimit = 16 * 1024 * 1024;

struct RpcStream::State {
  enum class Phase : std::uint8_t { kOpen, kHalfClosed, kClosed, kCancelled };
  grpcxx::StreamHandle handle;
  std::int32_t id{};
  grpcxx::RpcKind kind{};
  std::string path;
  CallOptions options;
  std::chrono::steady_clock::time_point deadline;
  Phase phase{Phase::kOpen};
  grpcxx::MetadataMap initial, trailers, block;
  grpcxx::Status status;
  grpcxx::CompressionAlgorithm incoming_encoding{
      grpcxx::CompressionAlgorithm::kIdentity};
  grpcxx::LengthPrefixedFrame parser{kChannelMessageLimit};
  std::deque<grpcxx::AlignedMessageBuf> outgoing;
  std::deque<std::vector<std::byte>> incoming;
  std::vector<std::byte> last_payload;
  std::size_t outgoing_offset{}, outgoing_bytes{}, incoming_bytes{},
      window_credit{}, header_bytes{};
  std::size_t sent_messages{}, received_messages{}, partial_frame_bytes{};
  int http_status{};
  bool received_headers{}, received_status{}, request_ended{},
      content_type_valid{};
  bool peer_closed{}, terminal{}, local_failure{};
};

struct SynchronousChannel::Impl {
  explicit Impl(std::unique_ptr<EndpointTransport> value,
                std::string_view name) noexcept;
  ~Impl();
  auto Initialize() noexcept -> std::expected<void, grpcxx::Status>;
  auto Wire(const grpcxx::StreamFrame &) noexcept
      -> std::expected<void, grpcxx::Status>;
  auto Poll() noexcept -> std::expected<void, grpcxx::Status>;
  auto Flush() noexcept -> std::expected<void, grpcxx::Status>;
  auto Abort(RpcStream::State &,
             grpcxx::Status) noexcept -> std::expected<void, grpcxx::Status>;
  void FailAll(const grpcxx::Status &) noexcept;
  auto Find(std::int32_t) noexcept -> RpcStream::State *;
  auto CheckOwner() const noexcept -> std::expected<void, grpcxx::Status>;
  static auto OnBeginHeaders(nghttp2_session *, const nghttp2_frame *,
                             void *) noexcept -> int;
  static auto OnHeader(nghttp2_session *, const nghttp2_frame *,
                       const std::uint8_t *, std::size_t, const std::uint8_t *,
                       std::size_t, std::uint8_t, void *) noexcept -> int;
  static auto OnFrame(nghttp2_session *, const nghttp2_frame *,
                      void *) noexcept -> int;
  static auto OnData(nghttp2_session *, std::uint8_t, std::int32_t,
                     const std::uint8_t *, std::size_t, void *) noexcept -> int;
  static auto OnClose(nghttp2_session *, std::int32_t, std::uint32_t,
                      void *) noexcept -> int;
  static auto ReadData(nghttp2_session *, std::int32_t, std::uint8_t *,
                       std::size_t, std::uint32_t *, nghttp2_data_source *,
                       void *) noexcept -> ssize_t;
  std::unique_ptr<EndpointTransport> transport;
  std::string authority;
  grpcxx::StreamMultiplexer mux;
  grpcxx::CompressionRegistry compression;
  nghttp2_session *h2{};
  std::array<RpcStream::State *, 16> streams{};
  std::array<std::unique_ptr<RpcStream::State>, 16> retired{};
  std::array<std::byte, 65536> read_buffer{};
  std::vector<std::byte> pending_write;
  std::size_t write_offset{};
  void *progress_context{};
  ProgressHook progress{};
  grpcxx::Status failure;
  const std::thread::id owner{std::this_thread::get_id()};
  bool polling{}, secure{}, draining{};
};
} // namespace greptimedb
