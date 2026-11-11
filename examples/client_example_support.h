#pragma once

#include <greptime/v1/health.pb.h>
#include <greptimedb/client/client.h>
#include <grpcxx/sync/synchronous_server.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
#include <print>
#include <span>
#include <string>
#include <string_view>

namespace client_examples {
using namespace greptimedb;
namespace proto = greptime::v1;
inline constexpr std::string_view kHealth = "/greptime.v1.HealthCheck/HealthCheck";
inline constexpr std::string_view kHandle = "/greptime.v1.GreptimeDatabase/Handle";
inline constexpr std::string_view kProm = "/greptime.v1.PrometheusGateway/Handle";
inline constexpr std::string_view kDoGet = "/arrow.flight.protocol.FlightService/DoGet";

inline bool Check(bool condition, std::string_view message) {
  if (!condition) std::println(stderr, "{}", message);
  return condition;
}
template <class T, class E> bool Checked(const std::expected<T, E> &value, std::string_view operation) {
  if (value) return true;
  if constexpr (requires { value.error().Message(); })
    std::println(stderr, "{}: {}", operation, value.error().Message());
  else if constexpr (std::convertible_to<E, std::string_view>)
    std::println(stderr, "{}: {}", operation, std::string_view(value.error()));
  else
    std::println(stderr, "{} failed", operation);
  return false;
}
inline Result<ClientOptions> Options(std::string_view endpoint = "grpc://localhost:4001") {
  auto parsed = Endpoint::Parse(endpoint);
  if (!parsed) return std::unexpected(parsed.error());
  ClientOptions options(std::move(*parsed));
  options.request_timeout = std::chrono::seconds(3);
  return options;
}

/** Both real HTTP/2 peers run inline on the calling thread. Sessions must die
 * before this object. Handler failures propagate through the progress hook. */
class OfflinePeer final {
public:
  std::optional<std::string> failure;
  std::size_t requests{};
  std::unique_ptr<grpcxx::SynchronousServer> server;
  std::unique_ptr<Client> client;

  bool Open(grpcxx::SynchronousServer::RawHandler handler, void *context,
            std::string_view endpoint = "grpc://localhost:4001") {
    auto pipe = nexora::MemoryPipe::CreatePair(97);
    if (!Checked(pipe, "memory pipe")) return false;
    return OpenPair(std::move(*pipe), handler, context, endpoint);
  }
  bool OpenPair(nexora::MemoryPipePair pipe, grpcxx::SynchronousServer::RawHandler handler, void *context,
                std::string_view endpoint = "grpc://localhost:4001") {
    auto options = Options(endpoint);
    if (!Checked(options, "client options")) return false;
    auto peer = grpcxx::SynchronousServer::FromMemoryPipe(std::move(pipe.server), handler, context);
    if (!Checked(peer, "mock HTTP/2 server")) return false;
    server = std::move(*peer);
    auto channel =
        grpcxx::SynchronousChannel::FromMemoryPipe(std::move(pipe.client), options->endpoints.front().Authority());
    if (!Checked(channel, "HTTP/2 channel")) return false;
    (*channel)->SetProgressHook(this, [](void *context) noexcept -> std::expected<void, grpcxx::Status> {
      auto &self = *static_cast<OfflinePeer *>(context);
      if (self.failure) return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInternal, *self.failure});
      return self.server->PollOnce();
    });
    auto opened = Client::FromChannel(std::move(*options), std::move(*channel));
    if (!Checked(opened, "open client")) return false;
    client = std::move(*opened);
    return true;
  }
  bool Require(bool condition, std::string_view message) {
    if (!condition && !failure) failure = message;
    return condition;
  }
  template <class Message> bool Parse(const grpcxx::ServerRequest &event, Message &message) {
    return Require(message.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size())),
                   "malformed request protobuf");
  }
  template <class Message> bool Send(grpcxx::SynchronousServer &peer, std::int32_t stream, const Message &message) {
    std::string bytes;
    if (!Require(message.SerializeToString(&bytes), "response encoding failed")) return false;
    auto result = peer.Send(stream, std::as_bytes(std::span{bytes}));
    return Require(result.has_value(), "server response send failed");
  }
  bool Finish(grpcxx::SynchronousServer &peer, std::int32_t stream, grpcxx::Status status = {}) {
    auto result = peer.Finish(stream, std::move(status));
    return Require(result.has_value(), "server response finish failed");
  }
  static void Health(void *context, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    if (event.event != grpcxx::ServerRequest::Event::kMessage) return;
    auto &self = *static_cast<OfflinePeer *>(context);
    if (!self.Require(event.path == kHealth, "expected HealthCheck")) return;
    ++self.requests;
    if (self.Send(server, event.stream_id, proto::HealthCheckResponse{})) self.Finish(server, event.stream_id);
  }
};

inline bool LiveProbe() {
  const char *endpoint = std::getenv("GREPTIMEDB_ENDPOINT");
  if (endpoint == nullptr || *endpoint == '\0') return true;
  auto options = Options(endpoint);
  if (!Checked(options, "live endpoint")) return false;
  grpcxx::SyncTlsClientOptions tls;
  if (const auto *ca = std::getenv("GREPTIMEDB_CA_PEM"); ca != nullptr && *ca != '\0') tls.ca_cert_path = ca;
  auto client = Client::Connect(std::move(*options), tls);
  if (!Checked(client, "live connection")) return false;
  auto health = (*client)->ProbeHealth();
  if (!Checked(health, "live HealthCheck")) return false;
  std::println("Live endpoint {} healthy={}", health->endpoint, health->healthy);
  return Check(health->healthy, "live endpoint reported unhealthy");
}
} // namespace client_examples
