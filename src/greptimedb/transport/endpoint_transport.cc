#include "greptimedb/transport/endpoint_transport.h"
#include "greptimedb/transport/tls_options.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <limits>
#include <new>
#include <string>

namespace greptimedb {
namespace {
using grpcxx::Status;
using grpcxx::StatusCode;
auto Error(std::string_view operation) noexcept -> Status {
  return {StatusCode::kUnavailable,
          std::string(operation) + ": " + std::strerror(errno)};
}
auto Exhausted() noexcept -> Status {
  return {StatusCode::kResourceExhausted, "transport allocation failed"};
}

class SocketTransport final : public EndpointTransport {
public:
  explicit SocketTransport(int fd, bool connecting,
                           addrinfo *addresses = nullptr,
                           bool nodelay = true) noexcept
      : fd_(fd), connecting_(connecting), addresses_(addresses),
        next_address_(addresses), nodelay_(nodelay) {}
  ~SocketTransport() override { Close(); }
  auto ConnectNextAddress() noexcept -> std::expected<void, Status> {
    if (fd_ >= 0)
      ::close(std::exchange(fd_, -1));
    Status failure{StatusCode::kUnavailable, "no usable TCP address"};
    while (next_address_) {
      auto *address = next_address_;
      next_address_ = address->ai_next;
      fd_ = ::socket(address->ai_family,
                     SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
      if (fd_ < 0) {
        failure = Error("socket");
        continue;
      }
      const int enabled = nodelay_ ? 1 : 0;
      if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &enabled,
                       sizeof(enabled)) != 0) {
        failure = Error("configure socket");
        ::close(std::exchange(fd_, -1));
        continue;
      }
      const int connected =
          ::connect(fd_, address->ai_addr, address->ai_addrlen);
      if (connected == 0 || errno == EINPROGRESS) {
        connecting_ = connected != 0;
        if (!connecting_)
          ReleaseAddresses();
        return {};
      }
      failure = Error("connect");
      ::close(std::exchange(fd_, -1));
    }
    connecting_ = false;
    ReleaseAddresses();
    return std::unexpected(std::move(failure));
  }
  auto WaitConnected(std::chrono::nanoseconds timeout) noexcept
      -> std::expected<void, Status> {
    const auto start = std::chrono::steady_clock::now();
    const auto deadline =
        start +
        std::min(timeout,
                 std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::steady_clock::time_point::max() - start));
    while (connecting_) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        Close();
        return std::unexpected(Status{StatusCode::kDeadlineExceeded,
                                      "TCP connect deadline exceeded"});
      }
      const auto remaining =
          std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
      pollfd event{fd_, POLLOUT, 0};
      const int waited =
          ::poll(&event, 1,
                 static_cast<int>(std::min<std::int64_t>(
                     remaining, std::numeric_limits<int>::max())));
      if (waited < 0 && errno == EINTR)
        continue;
      if (waited < 0)
        return std::unexpected(Error("poll connect"));
      if (auto advanced = PollOnce(); !advanced)
        return advanced;
    }
    return {};
  }
  auto Read(std::span<std::byte> output) noexcept
      -> std::expected<std::size_t, Status> override {
    if (fd_ < 0)
      return std::unexpected(
          Status{StatusCode::kUnavailable, "transport is closed"});
    if (connecting_ || output.empty())
      return 0;
    const auto n = ::recv(fd_, output.data(), output.size(), MSG_DONTWAIT);
    if (n > 0)
      return static_cast<std::size_t>(n);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
      return 0;
    if (n == 0)
      return std::unexpected(
          Status{StatusCode::kUnavailable, "peer closed transport"});
    return std::unexpected(Error("recv"));
  }
  auto Write(std::span<const std::byte> bytes) noexcept
      -> std::expected<std::size_t, Status> override {
    if (fd_ < 0)
      return std::unexpected(
          Status{StatusCode::kUnavailable, "transport is closed"});
    if (connecting_ || bytes.empty())
      return 0;
    const auto n =
        ::send(fd_, bytes.data(), bytes.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
    if (n >= 0)
      return static_cast<std::size_t>(n);
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
      return 0;
    return std::unexpected(Error("send"));
  }
  auto PollOnce() noexcept -> std::expected<void, Status> override {
    if (fd_ < 0)
      return std::unexpected(
          Status{StatusCode::kUnavailable, "transport is closed"});
    if (!connecting_)
      return {};
    pollfd readiness{fd_, POLLOUT, 0};
    const int ready = ::poll(&readiness, 1, 0);
    if (ready == 0 || (ready < 0 && errno == EINTR))
      return {};
    if (ready < 0)
      return std::unexpected(Error("poll connect"));
    sockaddr_storage peer{};
    socklen_t length = sizeof(peer);
    if (::getpeername(fd_, reinterpret_cast<sockaddr *>(&peer), &length) == 0) {
      connecting_ = false;
      ReleaseAddresses();
      return {};
    }
    int error = 0;
    length = sizeof(error);
    if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &length) < 0)
      return std::unexpected(Error("getsockopt"));
    if (error != 0) {
      if (next_address_)
        return ConnectNextAddress();
      errno = error;
      auto failure = Error("connect");
      Close();
      return std::unexpected(std::move(failure));
    }
    return {};
  }
  void Close() noexcept override {
    if (fd_ >= 0)
      ::close(std::exchange(fd_, -1));
    ReleaseAddresses();
  }
  auto IsOpen() const noexcept -> bool override { return fd_ >= 0; }
  auto NativeHandle() const noexcept -> int override { return fd_; }
  auto IsReady() const noexcept -> bool override {
    return fd_ >= 0 && !connecting_;
  }

private:
  void ReleaseAddresses() noexcept {
    if (addresses_)
      ::freeaddrinfo(std::exchange(addresses_, nullptr));
    next_address_ = nullptr;
  }
  int fd_;
  bool connecting_;
  addrinfo *addresses_{};
  addrinfo *next_address_{};
  bool nodelay_;
};

auto WrapSocket(int fd, bool tcp, bool connecting,
                TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  if (auto valid = options.Validate(); !valid) {
    if (fd >= 0)
      ::close(fd);
    return std::unexpected(valid.error());
  }
  if (fd < 0)
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "invalid socket fd"});
  const int flags = ::fcntl(fd, F_GETFL, 0);
  const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
  const int enabled = options.tcp_nodelay ? 1 : 0;
  if (flags < 0 || descriptor_flags < 0 ||
      ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
      ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0 ||
      (tcp && ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                           sizeof(enabled)) < 0)) {
    auto status = Error("configure socket");
    ::close(fd);
    return std::unexpected(std::move(status));
  }
  std::unique_ptr<EndpointTransport> result{
      new (std::nothrow) SocketTransport(fd, connecting)};
  if (!result) {
    ::close(fd);
    return std::unexpected(Exhausted());
  }
  return EndpointTransport::Configure(std::move(result), options);
}

struct MemoryState {
  struct Queue {
    std::unique_ptr<std::byte[]> bytes;
    std::size_t head{}, size{};
  } queues[2];
  std::size_t capacity{};
  unsigned references{2};
  bool open[2]{true, true};
};

class MemoryEndpoint final : public EndpointTransport {
public:
  MemoryEndpoint(MemoryState *state, unsigned side) noexcept
      : state_(state), side_(side) {}
  ~MemoryEndpoint() override {
    Close();
    if (--state_->references == 0)
      delete state_;
  }
  auto Read(std::span<std::byte> output) noexcept
      -> std::expected<std::size_t, Status> override {
    if (!IsOpen())
      return std::unexpected(
          Status{StatusCode::kUnavailable, "memory pipe is closed"});
    auto &queue = state_->queues[side_];
    const auto count = std::min(output.size(), queue.size);
    for (std::size_t i = 0; i < count; ++i)
      output[i] = queue.bytes[(queue.head + i) % state_->capacity];
    queue.head = (queue.head + count) % state_->capacity;
    queue.size -= count;
    if (count == 0 && !state_->open[1 - side_] && !output.empty())
      return std::unexpected(
          Status{StatusCode::kUnavailable, "memory pipe peer closed"});
    return count;
  }
  auto Write(std::span<const std::byte> bytes) noexcept
      -> std::expected<std::size_t, Status> override {
    if (!IsOpen() || !state_->open[1 - side_])
      return std::unexpected(
          Status{StatusCode::kUnavailable, "memory pipe is closed"});
    auto &queue = state_->queues[1 - side_];
    const auto count = std::min(bytes.size(), state_->capacity - queue.size);
    for (std::size_t i = 0; i < count; ++i)
      queue.bytes[(queue.head + queue.size + i) % state_->capacity] = bytes[i];
    queue.size += count;
    return count;
  }
  auto PollOnce() noexcept -> std::expected<void, Status> override {
    if (!IsOpen())
      return std::unexpected(
          Status{StatusCode::kUnavailable, "memory pipe is closed"});
    return {};
  }
  void Close() noexcept override { state_->open[side_] = false; }
  auto IsOpen() const noexcept -> bool override { return state_->open[side_]; }

private:
  MemoryState *state_;
  unsigned side_;
};
} // namespace

auto EndpointTransport::ConnectTcp(std::string_view host, std::uint16_t port,
                                   TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  return TcpTransport::Connect(host, port, options);
}
auto EndpointTransport::ConnectUnix(std::string_view path,
                                    TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  return UnixTransport::Connect(path, options);
}
auto EndpointTransport::ConnectTls(std::string_view host, std::uint16_t port,
                                   const TlsOptions &options,
                                   TransportOptions transport_options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  if (auto valid = transport_options.Validate(); !valid)
    return std::unexpected(valid.error());
  const auto started = std::chrono::steady_clock::now();
  auto context = options.CreateContext();
  if (!context)
    return std::unexpected(context.error());
  auto session = options.CreateSession(*context, host);
  if (!session)
    return std::unexpected(session.error());
  auto wire_options = transport_options;
  const auto prepared = std::chrono::steady_clock::now() - started;
  if (prepared >= wire_options.connect_timeout)
    return std::unexpected(Status{StatusCode::kDeadlineExceeded,
                                  "TCP/TLS connect deadline exceeded"});
  wire_options.connect_timeout -=
      std::chrono::duration_cast<std::chrono::nanoseconds>(prepared);
  wire_options.read_timeout.reset();
  wire_options.write_timeout.reset();
  auto wire = TcpTransport::Connect(host, port, wire_options);
  if (!wire)
    return std::unexpected(wire.error());
  auto transport = TlsTransport::Create(std::move(*wire), std::move(*session));
  if (!transport)
    return std::unexpected(transport.error());
  const auto elapsed = std::chrono::steady_clock::now() - started;
  if (elapsed >= transport_options.connect_timeout)
    return std::unexpected(Status{StatusCode::kDeadlineExceeded,
                                  "TCP/TLS connect deadline exceeded"});
  transport_options.connect_timeout -=
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed);
  return Configure(std::move(*transport), transport_options);
}

auto TcpTransport::Connect(std::string_view host, std::uint16_t port,
                           TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  if (auto valid = options.Validate(); !valid)
    return std::unexpected(valid.error());
  if (host.empty() || host.find('\0') != host.npos || port == 0)
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "invalid TCP endpoint"});
  std::array<char, 6> service{};
  const auto converted =
      std::to_chars(service.data(), service.data() + service.size(), port);
  *converted.ptr = '\0';
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;
  addrinfo *addresses = nullptr;
  const std::string name(host);
  const auto resolved =
      ::getaddrinfo(name.c_str(), service.data(), &hints, &addresses);
  if (resolved != 0)
    return std::unexpected(
        Status{StatusCode::kUnavailable, ::gai_strerror(resolved)});
  auto transport = std::unique_ptr<SocketTransport>{new (
      std::nothrow) SocketTransport(-1, false, addresses, options.tcp_nodelay)};
  if (!transport) {
    ::freeaddrinfo(addresses);
    return std::unexpected(Exhausted());
  }
  if (auto connected = transport->ConnectNextAddress(); !connected)
    return std::unexpected(connected.error());
  if (auto completed = transport->WaitConnected(options.connect_timeout);
      !completed)
    return std::unexpected(completed.error());
  return EndpointTransport::Configure(std::move(transport), options);
}
auto TcpTransport::Adopt(int fd, TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  return WrapSocket(fd, true, false, options);
}
auto UnixTransport::Connect(std::string_view path,
                            TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  if (auto valid = options.Validate(); !valid)
    return std::unexpected(valid.error());
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (path.empty() || path.size() >= sizeof(address.sun_path) ||
      path.find('\0') != path.npos)
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "invalid Unix socket path"});
  std::memcpy(address.sun_path, path.data(), path.size());
  const int fd =
      ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0)
    return std::unexpected(Error("socket"));
  const auto connected = ::connect(
      fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address));
  if (connected != 0 && errno != EINPROGRESS) {
    auto error = Error("connect");
    ::close(fd);
    return std::unexpected(std::move(error));
  }
  return WrapSocket(fd, false, connected != 0, options);
}
auto UnixTransport::Adopt(int fd, TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, Status> {
  return WrapSocket(fd, false, false, options);
}
auto MemoryPipe::CreatePair(std::size_t capacity) noexcept
    -> std::expected<Pair, Status> {
  if (capacity == 0 || capacity > 64 * 1024 * 1024)
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "invalid memory pipe capacity"});
  std::unique_ptr<MemoryState> state{new (std::nothrow) MemoryState};
  if (!state)
    return std::unexpected(Exhausted());
  state->capacity = capacity;
  for (auto &queue : state->queues) {
    queue.bytes.reset(new (std::nothrow) std::byte[capacity]);
    if (!queue.bytes)
      return std::unexpected(Exhausted());
  }
  auto *first = new (std::nothrow) MemoryEndpoint(state.get(), 0);
  if (!first)
    return std::unexpected(Exhausted());
  auto *second = new (std::nothrow) MemoryEndpoint(state.get(), 1);
  if (!second) {
    state->references = 1;
    state.release();
    delete first;
    return std::unexpected(Exhausted());
  }
  state.release();
  return Pair{std::unique_ptr<EndpointTransport>(first),
              std::unique_ptr<EndpointTransport>(second)};
}
} // namespace greptimedb
