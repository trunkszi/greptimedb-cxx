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

#include <grpcxx/core/status.h>
#include <grpcxx/tls/tls_session.h>

namespace greptimedb {

struct TlsOptions;

/** Caller-driven establishment deadline and stalled application-I/O budgets.
 * DNS resolution is synchronous and outside connect_timeout. A successful
 * partial read/write resets its corresponding stall budget.
 */
struct TransportOptions {
  std::chrono::nanoseconds connect_timeout{std::chrono::seconds{5}};
  std::optional<std::chrono::nanoseconds> read_timeout;
  std::optional<std::chrono::nanoseconds> write_timeout;
  bool tcp_nodelay{true};
  [[nodiscard]] auto Validate() const noexcept
      -> std::expected<void, grpcxx::Status>;
};

/** Caller-thread byte transport. Zero bytes means backpressure; EOF is an
 * error. Read, Write and PollOnce are nonblocking. PollOnce advances connection
 * and TLS state.
 */
class EndpointTransport {
public:
  virtual ~EndpointTransport() = default;
  /** Connects a nonblocking TCP socket within the configured deadline. */
  [[nodiscard]] static auto ConnectTcp(std::string_view host,
                                       std::uint16_t port,
                                       TransportOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  /** Validates trust/identity before dialing; PollOnce drives the TLS
   * handshake. */
  [[nodiscard]] static auto
  ConnectTls(std::string_view host, std::uint16_t port,
             const TlsOptions &options,
             TransportOptions transport_options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  [[nodiscard]] static auto ConnectUnix(std::string_view path,
                                        TransportOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  /** Applies deadlines to an existing caller-driven transport. */
  [[nodiscard]] static auto Configure(std::unique_ptr<EndpointTransport>,
                                      TransportOptions) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  [[nodiscard]] virtual auto IsReady() const noexcept -> bool {
    return IsOpen();
  }
  [[nodiscard]] virtual auto IsTls() const noexcept -> bool { return false; }
  [[nodiscard]] virtual auto Read(std::span<std::byte>) noexcept
      -> std::expected<std::size_t, grpcxx::Status> = 0;
  [[nodiscard]] virtual auto Write(std::span<const std::byte>) noexcept
      -> std::expected<std::size_t, grpcxx::Status> = 0;
  [[nodiscard]] virtual auto PollOnce() noexcept
      -> std::expected<void, grpcxx::Status> = 0;
  virtual void Close() noexcept = 0;
  [[nodiscard]] virtual auto IsOpen() const noexcept -> bool = 0;
  [[nodiscard]] virtual auto NativeHandle() const noexcept -> int { return -1; }
};

class TcpTransport final {
public:
  /** DNS resolution happens only during Connect, before channel polling begins.
   */
  [[nodiscard]] static auto Connect(std::string_view host, std::uint16_t port,
                                    TransportOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  /** Takes ownership on both success and failure; configures TCP_NODELAY. */
  [[nodiscard]] static auto Adopt(int fd,
                                  TransportOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
};

class UnixTransport final {
public:
  [[nodiscard]] static auto Connect(std::string_view path,
                                    TransportOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  /** Takes ownership on both success and failure. */
  [[nodiscard]] static auto Adopt(int fd,
                                  TransportOptions options = {}) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
};

class MemoryPipe final {
public:
  using Pair = std::pair<std::unique_ptr<EndpointTransport>,
                         std::unique_ptr<EndpointTransport>>;
  [[nodiscard]] static auto CreatePair(std::size_t capacity = 262144) noexcept
      -> std::expected<Pair, grpcxx::Status>;
};

/** TLS 1.3 memory-BIO composition; the supplied session owns its TLS context
 * reference. */
class TlsTransport final : public EndpointTransport {
public:
  [[nodiscard]] static auto Create(std::unique_ptr<EndpointTransport> wire,
                                   grpcxx::TlsSession session) noexcept
      -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status>;
  ~TlsTransport() override;
  auto Read(std::span<std::byte>) noexcept
      -> std::expected<std::size_t, grpcxx::Status> override;
  auto Write(std::span<const std::byte>) noexcept
      -> std::expected<std::size_t, grpcxx::Status> override;
  auto PollOnce() noexcept -> std::expected<void, grpcxx::Status> override;
  void Close() noexcept override;
  auto IsOpen() const noexcept -> bool override;
  auto NativeHandle() const noexcept -> int override;
  auto IsReady() const noexcept -> bool override;
  auto IsTls() const noexcept -> bool override { return true; }

private:
  struct Impl;
  explicit TlsTransport(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace greptimedb
