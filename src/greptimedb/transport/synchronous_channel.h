#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <grpcxx/compression/compression_algorithm.h>
#include <grpcxx/core/metadata.h>
#include <grpcxx/reflection/annotations.h>

#include "greptimedb/transport/endpoint_transport.h"
#include "greptimedb/transport/tls_options.h"

namespace greptimedb {

/** Per-RPC options; timeout must be positive and includes TCP/TLS progress.
 * Messages are limited to 4 MiB, per-stream queues to 16 MiB, and metadata to
 * 64 KiB. Hostname resolution during Connect is outside this RPC budget.
 */
struct CallOptions {
  grpcxx::MetadataMap metadata;
  std::chrono::nanoseconds timeout{std::chrono::seconds(30)};
  grpcxx::CompressionAlgorithm compression{
      grpcxx::CompressionAlgorithm::kIdentity};
};

/** A completed RPC preserves status and trailers even when status is non-OK.
 * Local transport, deadline and protocol failures use std::unexpected instead.
 */
struct RpcResponse {
  std::vector<std::byte> payload;
  grpcxx::MetadataMap initial_metadata;
  grpcxx::MetadataMap trailing_metadata;
  grpcxx::Status status;
};

class RpcStream;
class Endpoint;
struct ClientOptions;

/** Synchronous RPC facade driven on the caller thread, without a worker loop.
 * The progress hook composes a peer, ProactorService, or other RTC services
 * into waits. A channel must outlive all of its RpcStream handles.
 */
class SynchronousChannel final {
public:
  using ProgressHook = std::expected<void, grpcxx::Status> (*)(void *) noexcept;
  [[nodiscard]] static auto
  Create(std::unique_ptr<EndpointTransport>,
         std::string_view authority = "localhost") noexcept
      -> std::expected<std::unique_ptr<SynchronousChannel>, grpcxx::Status>;
  [[nodiscard]] static auto Connect(std::string_view endpoint,
                                    const TlsOptions &tls = {}) noexcept
      -> std::expected<std::unique_ptr<SynchronousChannel>, grpcxx::Status>;
  [[nodiscard]] static auto Connect(const Endpoint &endpoint,
                                    const TlsOptions &tls = {}) noexcept
      -> std::expected<std::unique_ptr<SynchronousChannel>, grpcxx::Status>;
  [[nodiscard]] static auto Connect(const Endpoint &endpoint,
                                    const ClientOptions &options,
                                    const TlsOptions &tls = {}) noexcept
      -> std::expected<std::unique_ptr<SynchronousChannel>, grpcxx::Status>;
  ~SynchronousChannel();
  SynchronousChannel(const SynchronousChannel &) = delete;
  auto operator=(const SynchronousChannel &) -> SynchronousChannel & = delete;
  /** Borrowed socket descriptor for diagnostics; never close it externally. */
  [[nodiscard]] auto NativeHandle() const noexcept -> int;
  void SetProgressHook(void *context, ProgressHook) noexcept;
  [[nodiscard]] auto PollOnce() noexcept -> std::expected<void, grpcxx::Status>;
  [[nodiscard]] auto Open(std::string_view path, grpcxx::RpcKind,
                          CallOptions = {}) noexcept
      -> std::expected<std::unique_ptr<RpcStream>, grpcxx::Status>;
  [[nodiscard]] auto Unary(std::string_view path, std::span<const std::byte>,
                           CallOptions = {}) noexcept
      -> std::expected<RpcResponse, grpcxx::Status>;
  [[nodiscard]] auto CallUnary(std::string_view path,
                               std::span<const std::byte> bytes,
                               CallOptions options = {}) noexcept
      -> std::expected<RpcResponse, grpcxx::Status> {
    return Unary(path, bytes, std::move(options));
  }
  [[nodiscard]] auto ClientStreamCall(std::string_view path,
                                      CallOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<RpcStream>, grpcxx::Status>;
  [[nodiscard]] auto ServerStreamCall(std::string_view path,
                                      std::span<const std::byte>,
                                      CallOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<RpcStream>, grpcxx::Status>;
  [[nodiscard]] auto BidiStreamCall(std::string_view path,
                                    CallOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<RpcStream>, grpcxx::Status>;

private:
  friend class RpcStream;
  struct Impl;
  explicit SynchronousChannel(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

class RpcStream final {
public:
  /** An unfinished stream queues an HTTP/2 CANCEL reset when destroyed. */
  ~RpcStream();
  RpcStream(const RpcStream &) = delete;
  auto operator=(const RpcStream &) -> RpcStream & = delete;
  [[nodiscard]] auto Send(std::span<const std::byte>) noexcept
      -> std::expected<void, grpcxx::Status>;
  [[nodiscard]] auto HalfClose() noexcept
      -> std::expected<void, grpcxx::Status>;
  [[nodiscard]] auto Read() noexcept
      -> std::expected<std::optional<std::vector<std::byte>>, grpcxx::Status>;
  [[nodiscard]] auto RecvNext() noexcept
      -> std::expected<std::optional<std::vector<std::byte>>, grpcxx::Status> {
    return Read();
  }
  /** Empty means no message yet or clean EOF; Done() distinguishes the two. */
  [[nodiscard]] auto TryRecvNext() noexcept
      -> std::expected<std::optional<std::vector<std::byte>>, grpcxx::Status>;
  /** Half-closes, drains responses, and returns the last payload and trailers.
   * Streaming consumers should use RecvNext to inspect every response payload.
   */
  [[nodiscard]] auto Finish() noexcept
      -> std::expected<RpcResponse, grpcxx::Status>;
  [[nodiscard]] auto Cancel() noexcept -> std::expected<void, grpcxx::Status>;
  [[nodiscard]] auto Id() const noexcept -> std::int32_t;
  [[nodiscard]] auto Done() const noexcept -> bool;
  [[nodiscard]] auto InitialMetadata() const noexcept
      -> const grpcxx::MetadataMap &;
  [[nodiscard]] auto TrailingMetadata() const noexcept
      -> const grpcxx::MetadataMap &;
  [[nodiscard]] auto FinalStatus() const noexcept -> const grpcxx::Status &;

private:
  friend class SynchronousChannel;
  struct State;
  RpcStream(SynchronousChannel &, std::unique_ptr<State>) noexcept;
  SynchronousChannel *channel_;
  std::unique_ptr<State> state_;
};

} // namespace greptimedb
