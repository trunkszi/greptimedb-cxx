#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <greptime/v1/common.pb.h>
#include <grpcxx/core/metadata.h>

#include "greptimedb/core/error.h"
#include "greptimedb/core/hints.h"

namespace greptimedb {

/** Endpoint transport; parsing performs neither resolution nor network I/O. */
enum class TransportScheme { kGrpc, kGrpcs, kUnix };

/** Immutable validated TCP authority or absolute Unix socket path. */
class Endpoint final {
public:
  [[nodiscard]] static Result<Endpoint> Parse(std::string_view raw);
  [[nodiscard]] TransportScheme Scheme() const noexcept { return scheme_; }
  [[nodiscard]] std::string_view Host() const noexcept { return host_; }
  [[nodiscard]] std::uint16_t Port() const noexcept { return port_; }
  [[nodiscard]] std::optional<std::string_view> UnixPath() const noexcept;
  [[nodiscard]] std::string SocketAddress() const;
  [[nodiscard]] std::string Authority() const;
  bool operator==(const Endpoint &) const = default;

private:
  Endpoint() = default;
  TransportScheme scheme_{TransportScheme::kGrpc};
  std::string host_;
  std::uint16_t port_{4001};
  std::optional<std::string> unix_path_;
};

/** Credentials shared by HTTP/2 metadata and protobuf authentication. */
class Auth final {
public:
  [[nodiscard]] static Auth Basic(std::string username, std::string password);
  [[nodiscard]] static Auth Token(std::string token);
  [[nodiscard]] Result<void> Validate() const;
  [[nodiscard]] std::string ToMetadataValue() const;
  [[nodiscard]] ::greptime::v1::AuthHeader ToProtoAuthHeader() const;
  /** Diagnostic representation never includes a password or bearer token. */
  [[nodiscard]] std::string DebugString() const;

private:
  enum class Kind { kBasic, kToken };
  Auth(Kind kind, std::string username, std::string secret)
      : kind_(kind), username_(std::move(username)),
        secret_(std::move(secret)) {}
  Kind kind_;
  std::string username_;
  std::string secret_;
};

/** Explicit per-call compression; identity never inherits a channel's previous
 * choice. */
enum class Compression { kIdentity, kGzip, kDeflate, kSnappy, kZstd, kLz4 };
[[nodiscard]] std::string_view
CompressionName(Compression compression) noexcept;

/** Owned, mutable connection defaults; validate again before constructing each
 * call. */
struct ClientOptions final {
  ClientOptions() = default;
  explicit ClientOptions(std::vector<Endpoint> addresses)
      : endpoints(std::move(addresses)) {}
  explicit ClientOptions(Endpoint endpoint) : endpoints{std::move(endpoint)} {}

  std::vector<Endpoint> endpoints;
  std::string catalog{kDefaultCatalog};
  std::string schema{kDefaultSchema};
  std::optional<Auth> auth;
  std::optional<std::string> timezone;
  WriteHints write_hints;
  Compression compression{Compression::kIdentity};
  std::chrono::nanoseconds connect_timeout{std::chrono::seconds(5)};
  std::chrono::nanoseconds request_timeout{std::chrono::seconds(30)};
  std::optional<std::chrono::nanoseconds> read_timeout;
  std::optional<std::chrono::nanoseconds> write_timeout;
  bool tcp_nodelay{true};
  std::size_t max_in_flight{8};

  [[nodiscard]] Result<void> Validate() const;
  /** An empty catalog intentionally yields the schema alone. */
  [[nodiscard]] std::string DbName() const;
  /** Redacted diagnostics omit credentials and arbitrary user-provided hint
   * values. */
  [[nodiscard]] std::string DebugString() const;
  /** Split on the first hyphen; a name without a hyphen changes only the
   * schema. */
  ClientOptions &WithDbName(std::string dbname);
  [[nodiscard]] ::greptime::v1::RequestHeader BuildRequestHeader(
      const std::unordered_map<std::string, std::string> &tracing_context = {})
      const;
  [[nodiscard]] Result<grpcxx::MetadataMap>
  BuildCallMetadata(const WriteHints *extra_hints = nullptr) const;
};

} // namespace greptimedb
