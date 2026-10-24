#pragma once

#include <expected>
#include <string>
#include <string_view>

#include "grpcxx/tls/tls_context.h"
#include "grpcxx/tls/tls_session.h"

namespace greptimedb {

/** Client trust and identity loaded before a connection enters the caller loop.
 * With no custom CA, grpcxx uses OpenSSL's system root paths. A custom CA file,
 * PEM string, or both selects an explicit trust store instead. Client identity
 * consists of an unencrypted PEM private key and a matching certificate chain.
 * Certificate verification, TLS 1.3, and HTTP/2 ALPN are mandatory.
 */
struct TlsOptions final {
  std::string ca_file;
  std::string ca_pem;
  std::string certificate_pem;
  std::string private_key_pem;
  /** Overrides both certificate verification and DNS SNI; empty uses the host.
   * IP literals are verified as IP addresses and do not send a DNS SNI value.
   */
  std::string server_name_override;

  [[nodiscard]] auto CreateContext() const noexcept
      -> std::expected<grpcxx::TlsContext, grpcxx::Status>;

  /** Creates a memory-BIO client session; the caller drives its handshake.
   * context must have been created by CreateContext().
   * endpoint_host is a DNS name or an unbracketed IP literal, without a port.
   * The returned session owns its TLS state and does not borrow these options.
   */
  [[nodiscard]] auto CreateSession(const grpcxx::TlsContext &context,
                                   std::string_view endpoint_host)
      const noexcept -> std::expected<grpcxx::TlsSession, grpcxx::Status>;

  /** Checks the negotiated protocol before a transport accepts application IO.
   */
  [[nodiscard]] static auto
  VerifySession(const grpcxx::TlsSession &session) noexcept
      -> std::expected<void, grpcxx::Status>;
};

} // namespace greptimedb
