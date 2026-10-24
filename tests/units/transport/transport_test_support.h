#pragma once

#include <greptimedb/transport/endpoint_transport.h>
#include <greptimedb/transport/tls_options.h>
#include <grpcxx/tls/tls_session.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace greptimedb::testing {
using TransportPair = std::pair<std::unique_ptr<EndpointTransport>,
                                std::unique_ptr<EndpointTransport>>;

inline auto
Failure(std::string_view message) -> std::unexpected<grpcxx::Status> {
  return std::unexpected(
      grpcxx::Status{grpcxx::StatusCode::kInternal, std::string(message)});
}

struct FileDescriptor {
  int value{-1};
  ~FileDescriptor() {
    if (value >= 0)
      ::close(value);
  }
};

/** Both socket endpoints are advanced by the test's caller thread. */
inline auto TcpPair(std::string_view host = "127.0.0.1")
    -> std::expected<TransportPair, grpcxx::Status> {
  FileDescriptor listener{
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
  if (listener.value < 0)
    return Failure("test TCP socket failed");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(listener.value, reinterpret_cast<sockaddr *>(&address),
             sizeof(address)) != 0 ||
      ::listen(listener.value, 4) != 0)
    return Failure("test TCP listen failed");
  socklen_t length = sizeof(address);
  if (::getsockname(listener.value, reinterpret_cast<sockaddr *>(&address),
                    &length) != 0)
    return Failure("test TCP getsockname failed");
  auto client = TcpTransport::Connect(host, ntohs(address.sin_port));
  if (!client)
    return std::unexpected(client.error());
  int accepted = -1;
  for (int tick = 0; tick != 8192 && accepted < 0; ++tick) {
    if (!(*client)->PollOnce())
      return Failure("test TCP connect poll failed");
    accepted = ::accept4(listener.value, nullptr, nullptr,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);
  }
  if (accepted < 0)
    return Failure("test TCP accept failed");
  auto server = TcpTransport::Adopt(accepted);
  if (!server)
    return std::unexpected(server.error());
  return TransportPair{std::move(*client), std::move(*server)};
}

inline auto UnixPair() -> std::expected<TransportPair, grpcxx::Status> {
  static unsigned sequence = 0;
  std::error_code error;
  std::filesystem::create_directories(GREPTIMEDB_TEST_SCRATCH_DIR, error);
  if (error)
    return Failure("test Unix scratch directory failed");
  const auto path = std::string(GREPTIMEDB_TEST_SCRATCH_DIR) + "/" +
                    std::to_string(::getpid()) + "-" +
                    std::to_string(sequence++) + ".sock";
  struct Cleanup {
    std::string path;
    ~Cleanup() { ::unlink(path.c_str()); }
  } cleanup{path};
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (path.size() >= sizeof(address.sun_path))
    return Failure("test Unix path too long");
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  FileDescriptor listener{
      ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
  if (listener.value < 0 ||
      ::bind(listener.value, reinterpret_cast<sockaddr *>(&address),
             sizeof(address)) != 0 ||
      ::listen(listener.value, 4) != 0)
    return Failure("test Unix listen failed");
  auto client = UnixTransport::Connect(path);
  if (!client)
    return std::unexpected(client.error());
  const int accepted =
      ::accept4(listener.value, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (accepted < 0)
    return Failure("test Unix accept failed");
  auto server = UnixTransport::Adopt(accepted);
  if (!server)
    return std::unexpected(server.error());
  return TransportPair{std::move(*client), std::move(*server)};
}

inline auto FixturePath(std::string_view name) -> std::string {
  return std::string(GREPTIMEDB_TLS_FIXTURE_DIR) + "/" + std::string(name);
}
inline auto ReadFixture(std::string_view name) -> std::string {
  std::ifstream file{FixturePath(name)};
  return {std::istreambuf_iterator<char>{file},
          std::istreambuf_iterator<char>{}};
}
inline auto ServerTlsOptions(bool mutual = false) -> grpcxx::TlsContextOptions {
  grpcxx::TlsContextOptions options;
  options.mode = grpcxx::TlsMode::kServer;
  options.verify_peer = mutual;
  options.require_client_certificate = mutual;
  options.ca_file = FixturePath("ca.pem");
  options.certificate_file = FixturePath("server-cert.pem");
  options.private_key_file = FixturePath("server-key.pem");
  return options;
}
inline auto ClientTlsOptions(bool mutual = false) -> TlsOptions {
  TlsOptions options;
  options.ca_file = FixturePath("ca.pem");
  options.server_name_override = "localhost";
  if (mutual) {
    options.certificate_pem = ReadFixture("client-cert.pem");
    options.private_key_pem = ReadFixture("client-key.pem");
  }
  return options;
}

inline auto TlsPair(TransportPair underlying, bool mutual = false)
    -> std::expected<TransportPair, grpcxx::Status> {
  auto options = ClientTlsOptions(mutual);
  auto client_context = options.CreateContext();
  auto server_context = grpcxx::TlsContext::Create(ServerTlsOptions(mutual));
  if (!client_context)
    return std::unexpected(client_context.error());
  if (!server_context)
    return std::unexpected(server_context.error());
  auto client_session = options.CreateSession(*client_context, "127.0.0.1");
  auto server_session = grpcxx::TlsSession::Create(*server_context);
  if (!client_session)
    return std::unexpected(client_session.error());
  if (!server_session)
    return std::unexpected(server_session.error());
  auto client = TlsTransport::Create(std::move(underlying.first),
                                     std::move(*client_session));
  auto server = TlsTransport::Create(std::move(underlying.second),
                                     std::move(*server_session));
  if (!client)
    return std::unexpected(client.error());
  if (!server)
    return std::unexpected(server.error());
  return TransportPair{std::move(*client), std::move(*server)};
}

inline auto Bytes(std::string_view text) -> std::span<const std::byte> {
  return std::as_bytes(std::span{text});
}
} // namespace greptimedb::testing
