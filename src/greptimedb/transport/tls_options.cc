#include "greptimedb/transport/tls_options.h"

#include <arpa/inet.h>

#include <array>
#include <utility>

namespace greptimedb {
namespace {

auto InvalidConfiguration(std::string message) noexcept -> grpcxx::Status {
  return grpcxx::Status{grpcxx::StatusCode::kInvalidArgument,
                        std::move(message)};
}

auto IsAsciiAlphanumeric(char value) noexcept -> bool {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
         (value >= '0' && value <= '9');
}

auto ValidServerName(std::string_view name) noexcept -> bool {
  /** DNS names have at most 253 characters, plus an optional final root dot. */
  if (name.empty() || name.size() > 254 ||
      name.find('\0') != std::string_view::npos) {
    return false;
  }
  std::array<char, 255> terminated{};
  name.copy(terminated.data(), name.size());
  std::array<unsigned char, 16> address{};
  if (inet_pton(AF_INET, terminated.data(), address.data()) == 1 ||
      inet_pton(AF_INET6, terminated.data(), address.data()) == 1) {
    return true;
  }
  if (name.back() == '.')
    name.remove_suffix(1);
  if (name.empty() || name.size() > 253)
    return false;
  while (!name.empty()) {
    const auto separator = name.find('.');
    const auto label = name.substr(0, separator);
    if (label.empty() || label.size() > 63 ||
        !IsAsciiAlphanumeric(label.front()) ||
        !IsAsciiAlphanumeric(label.back())) {
      return false;
    }
    for (const auto character : label) {
      if (!IsAsciiAlphanumeric(character) && character != '-')
        return false;
    }
    if (separator == std::string_view::npos)
      return true;
    name.remove_prefix(separator + 1);
    if (name.empty())
      return false;
  }
  return true;
}

} // namespace

auto TlsOptions::CreateContext() const noexcept
    -> std::expected<grpcxx::TlsContext, grpcxx::Status> {
  if (!server_name_override.empty() && !ValidServerName(server_name_override)) {
    return std::unexpected(
        InvalidConfiguration("Invalid TLS server name override"));
  }
  if (certificate_pem.empty() != private_key_pem.empty()) {
    return std::unexpected(InvalidConfiguration(
        "TLS client certificate and private key must be provided together"));
  }
  if (private_key_pem.find("ENCRYPTED PRIVATE KEY") != std::string::npos ||
      private_key_pem.find("Proc-Type: 4,ENCRYPTED") != std::string::npos) {
    return std::unexpected(InvalidConfiguration(
        "TLS client private key must be an unencrypted PEM key"));
  }
  grpcxx::TlsContextOptions options;
  options.mode = grpcxx::TlsMode::kClient;
  options.verify_peer = true;
  options.ca_file = ca_file;
  options.ca_pem = ca_pem;
  options.certificate_pem = certificate_pem;
  options.private_key_pem = private_key_pem;
  return grpcxx::TlsContext::Create(options);
}

auto TlsOptions::CreateSession(const grpcxx::TlsContext &context,
                               std::string_view endpoint_host) const noexcept
    -> std::expected<grpcxx::TlsSession, grpcxx::Status> {
  const auto name =
      server_name_override.empty() ? endpoint_host : server_name_override;
  if (!ValidServerName(name)) {
    return std::unexpected(InvalidConfiguration(
        "TLS server name must be a DNS name or IP address"));
  }
  return grpcxx::TlsSession::Create(context, name);
}

auto TlsOptions::VerifySession(const grpcxx::TlsSession &session) noexcept
    -> std::expected<void, grpcxx::Status> {
  if (session.State() != grpcxx::TlsHandshakeState::kEstablished) {
    return std::unexpected(
        grpcxx::Status{grpcxx::StatusCode::kFailedPrecondition,
                       "TLS handshake has not established a session"});
  }
  if (session.ProtocolVersion() != "TLSv1.3") {
    return std::unexpected(
        grpcxx::Status{grpcxx::StatusCode::kUnavailable,
                       "TLS peer did not negotiate TLS 1.3"});
  }
  return session.VerifyAlpnH2();
}

} // namespace greptimedb
