#include "greptimedb/core/config.h"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <ranges>
#include <span>
#include <system_error>
#include <utility>

namespace greptimedb {
namespace {

bool IsAsciiAlphanumeric(unsigned char ch) noexcept {
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
         (ch >= '0' && ch <= '9');
}

bool IsPrintableAscii(std::string_view value) noexcept {
  return std::ranges::all_of(
      value, [](unsigned char ch) { return ch >= 0x20 && ch <= 0x7e; });
}

bool HasAsciiControl(std::string_view value) noexcept {
  return std::ranges::any_of(
      value, [](unsigned char ch) { return ch < 0x20 || ch == 0x7f; });
}

Result<std::uint16_t> ParsePort(std::string_view raw) {
  if (raw.empty() || !std::ranges::all_of(raw, [](unsigned char ch) {
        return ch >= '0' && ch <= '9';
      })) {
    return std::unexpected(
        Error::InvalidConfig("endpoint port must be a decimal integer"));
  }
  unsigned int port{};
  const auto parsed =
      std::from_chars(raw.data(), raw.data() + raw.size(), port);
  if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size() ||
      port == 0 || port > 65535) {
    return std::unexpected(
        Error::InvalidConfig("endpoint port must be between 1 and 65535"));
  }
  return static_cast<std::uint16_t>(port);
}

bool ValidHost(std::string_view host) {
  const std::string owned(host);
  in_addr ipv4{};
  if (inet_pton(AF_INET, owned.c_str(), &ipv4) == 1)
    return true;
  if (host.ends_with('.'))
    host.remove_suffix(1);
  if (host.empty() || host.size() > 253)
    return false;
  if (host.contains('.') && std::ranges::all_of(host, [](unsigned char ch) {
        return (ch >= '0' && ch <= '9') || ch == '.';
      })) {
    return false;
  }
  for (const auto label_view : host | std::views::split('.')) {
    const std::string_view label(label_view.data(), label_view.size());
    if (label.empty() || label.size() > 63 || label.front() == '-' ||
        label.back() == '-' ||
        !std::ranges::all_of(label, [](unsigned char ch) {
          return IsAsciiAlphanumeric(ch) || ch == '-';
        })) {
      return false;
    }
  }
  return true;
}

} // namespace

Result<Endpoint> Endpoint::Parse(std::string_view raw) {
  if (raw.empty() || std::ranges::any_of(raw, [](unsigned char ch) {
        return ch <= 0x20 || ch == 0x7f;
      })) {
    return std::unexpected(Error::InvalidConfig("invalid endpoint address"));
  }
  Endpoint endpoint;
  if (const auto separator = raw.find("://");
      separator != std::string_view::npos) {
    const auto scheme = raw.substr(0, separator);
    if (scheme == "grpc" || scheme == "http") {
      endpoint.scheme_ = TransportScheme::kGrpc;
    } else if (scheme == "grpcs" || scheme == "https") {
      endpoint.scheme_ = TransportScheme::kGrpcs;
    } else if (scheme == "unix") {
      endpoint.scheme_ = TransportScheme::kUnix;
    } else {
      return std::unexpected(
          Error::InvalidConfig("unsupported endpoint scheme"));
    }
    raw.remove_prefix(separator + 3);
  }
  if (endpoint.scheme_ == TransportScheme::kUnix) {
    if (!raw.starts_with('/') || raw == "/" ||
        raw.find_first_of("?#") != raw.npos) {
      return std::unexpected(Error::InvalidConfig(
          "Unix endpoint must have an absolute socket path"));
    }
    endpoint.unix_path_ = std::string(raw);
    return endpoint;
  }
  if (raw.empty() || raw.find_first_of("/?#@\\") != raw.npos) {
    return std::unexpected(Error::InvalidConfig("invalid endpoint authority"));
  }
  std::string_view host;
  if (raw.starts_with('[')) {
    const auto end = raw.find(']');
    if (end == raw.npos)
      return std::unexpected(Error::InvalidConfig("unclosed IPv6 address"));
    host = raw.substr(1, end - 1);
    in6_addr ipv6{};
    const std::string owned(host);
    if (inet_pton(AF_INET6, owned.c_str(), &ipv6) != 1) {
      return std::unexpected(Error::InvalidConfig("invalid IPv6 address"));
    }
    const auto suffix = raw.substr(end + 1);
    if (!suffix.empty()) {
      if (!suffix.starts_with(':')) {
        return std::unexpected(
            Error::InvalidConfig("invalid IPv6 port suffix"));
      }
      auto port = ParsePort(suffix.substr(1));
      if (!port)
        return std::unexpected(port.error());
      endpoint.port_ = *port;
    }
  } else {
    const auto separator = raw.find(':');
    host = raw.substr(0, separator);
    if (!ValidHost(host))
      return std::unexpected(Error::InvalidConfig("invalid endpoint host"));
    if (separator != raw.npos) {
      auto port = ParsePort(raw.substr(separator + 1));
      if (!port)
        return std::unexpected(port.error());
      endpoint.port_ = *port;
    }
  }
  endpoint.host_ = std::string(host);
  return endpoint;
}

std::optional<std::string_view> Endpoint::UnixPath() const noexcept {
  if (unix_path_)
    return *unix_path_;
  return std::nullopt;
}

std::string Endpoint::SocketAddress() const {
  if (unix_path_)
    return *unix_path_;
  const auto port = std::to_string(port_);
  if (host_.contains(':'))
    return "[" + host_ + "]:" + port;
  return host_ + ":" + port;
}

std::string Endpoint::Authority() const {
  return scheme_ == TransportScheme::kUnix ? "localhost" : SocketAddress();
}

Auth Auth::Basic(std::string username, std::string password) {
  return Auth(Kind::kBasic, std::move(username), std::move(password));
}

Auth Auth::Token(std::string token) {
  return Auth(Kind::kToken, {}, std::move(token));
}

Result<void> Auth::Validate() const {
  switch (kind_) {
  case Kind::kBasic:
    if (username_.contains(':') || HasAsciiControl(username_) ||
        HasAsciiControl(secret_)) {
      return std::unexpected(
          Error::InvalidConfig("Basic credentials must not contain ASCII "
                               "controls or a colon in the username"));
    }
    return {};
  case Kind::kToken:
    if (!IsPrintableAscii(secret_)) {
      return std::unexpected(
          Error::InvalidConfig("bearer tokens must contain printable ASCII"));
    }
    return {};
  }
  return std::unexpected(Error::InvalidConfig("invalid authentication scheme"));
}

std::string Auth::ToMetadataValue() const {
  switch (kind_) {
  case Kind::kBasic: {
    const auto credentials = username_ + ":" + secret_;
    return "Basic " + grpcxx::MetadataMap::Base64Encode(std::as_bytes(
                          std::span(credentials.data(), credentials.size())));
  }
  case Kind::kToken:
    return "Bearer " + secret_;
  }
  return {};
}

::greptime::v1::AuthHeader Auth::ToProtoAuthHeader() const {
  ::greptime::v1::AuthHeader header;
  switch (kind_) {
  case Kind::kBasic:
    header.mutable_basic()->set_username(username_);
    header.mutable_basic()->set_password(secret_);
    break;
  case Kind::kToken:
    header.mutable_token()->set_token(secret_);
    break;
  }
  return header;
}

std::string Auth::DebugString() const {
  switch (kind_) {
  case Kind::kBasic:
    return "Basic { password: <redacted> }";
  case Kind::kToken:
    return "Token(<redacted>)";
  }
  return "<redacted>";
}

std::string_view CompressionName(Compression compression) noexcept {
  switch (compression) {
  case Compression::kIdentity:
    return "identity";
  case Compression::kGzip:
    return "gzip";
  case Compression::kDeflate:
    return "deflate";
  case Compression::kSnappy:
    return "snappy";
  case Compression::kZstd:
    return "zstd";
  case Compression::kLz4:
    return "lz4";
  }
  return {};
}

Result<void> ClientOptions::Validate() const {
  if (endpoints.empty() || schema.empty() || max_in_flight == 0) {
    return std::unexpected(Error::InvalidConfig(
        "endpoints, schema, and in-flight capacity must be nonempty"));
  }
  for (const auto &endpoint : endpoints) {
    switch (endpoint.Scheme()) {
    case TransportScheme::kGrpc:
    case TransportScheme::kGrpcs:
      if (endpoint.Host().empty()) {
        return std::unexpected(
            Error::InvalidConfig("endpoint host must be nonempty"));
      }
      break;
    case TransportScheme::kUnix: {
      const auto path = endpoint.UnixPath();
      if (!path || path->size() <= 1 || !path->starts_with('/')) {
        return std::unexpected(Error::InvalidConfig(
            "Unix endpoint path must be absolute and nonempty"));
      }
      break;
    }
    }
  }
  if (!IsPrintableAscii(DbName())) {
    return std::unexpected(
        Error::InvalidConfig("database metadata must contain printable ASCII"));
  }
  const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::time_point::max() -
      std::chrono::steady_clock::now());
  for (const auto timeout :
       {std::optional{connect_timeout}, std::optional{request_timeout},
        read_timeout, write_timeout}) {
    if (timeout && (*timeout <= std::chrono::nanoseconds::zero() ||
                    *timeout > remaining)) {
      return std::unexpected(Error::InvalidConfig(
          "timeouts must fit a nonzero monotonic deadline"));
    }
  }
  if (CompressionName(compression).empty()) {
    return std::unexpected(
        Error::InvalidConfig("invalid compression algorithm"));
  }
  if (auth)
    return auth->Validate();
  return {};
}

std::string ClientOptions::DbName() const {
  return catalog.empty() ? schema : catalog + "-" + schema;
}

std::string ClientOptions::DebugString() const {
  return "ClientOptions { endpoints: " + std::to_string(endpoints.size()) +
         ", auth: " + (auth ? auth->DebugString() : "None") + " }";
}

ClientOptions &ClientOptions::WithDbName(std::string dbname) {
  if (const auto separator = dbname.find('-'); separator != dbname.npos) {
    catalog = dbname.substr(0, separator);
    schema = dbname.substr(separator + 1);
  } else {
    schema = std::move(dbname);
  }
  return *this;
}

::greptime::v1::RequestHeader ClientOptions::BuildRequestHeader(
    const std::unordered_map<std::string, std::string> &tracing_context) const {
  ::greptime::v1::RequestHeader header;
  header.set_catalog(catalog);
  header.set_schema(schema);
  header.set_dbname(DbName());
  if (auth)
    *header.mutable_authorization() = auth->ToProtoAuthHeader();
  if (timezone)
    header.set_timezone(*timezone);
  for (const auto &[key, value] : tracing_context)
    (*header.mutable_tracing_context())[key] = value;
  return header;
}

Result<grpcxx::MetadataMap>
ClientOptions::BuildCallMetadata(const WriteHints *extra_hints) const {
  if (auto valid = Validate(); !valid)
    return std::unexpected(valid.error());
  grpcxx::MetadataMap metadata;
  auto add = [&metadata](std::string_view key,
                         std::string_view value) -> Result<void> {
    if (auto result = metadata.Add(key, value); !result) {
      return std::unexpected(Error::InvalidConfig(
          "invalid ASCII metadata for " + std::string(key)));
    }
    return {};
  };
  if (auto result = add(kGreptimeDbHeader, DbName()); !result)
    return std::unexpected(result.error());
  if (auth) {
    if (auto result = add(kGreptimeAuthHeader, auth->ToMetadataValue());
        !result) {
      return std::unexpected(result.error());
    }
  }
  const auto hints =
      extra_hints ? write_hints.MergedWith(*extra_hints) : write_hints;
  if (!hints.IsEmpty()) {
    if (auto result = add(kGreptimeHintsHeader, hints.FormatHeaderValue());
        !result) {
      return std::unexpected(result.error());
    }
  }
  return metadata;
}

} // namespace greptimedb
