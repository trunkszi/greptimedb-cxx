/** Lazy caller-thread factories for the native, non-movable Client. */
#pragma once

#include <cares/types/endpoint.hpp>
#include <nexora/pool/endpoint_failover.hpp>

#include "greptimedb/client/client.h"
#include "greptimedb/threading/worker_pool.h"

namespace greptimedb {

/** Owned connection settings copied into per-thread client factories.
 * Each invocation connects only to the first endpoint in the supplied order.
 * RPC failover requires a policy-aware factory instead of this adapter.
 */
class NativeClientFactory {
public:
  [[nodiscard]] static Result<NativeClientFactory> Create(ClientOptions options,
                                                          grpcxx::SyncTlsClientOptions tls = {}) {
    if (auto valid = options.Validate(); !valid) return std::unexpected(valid.error());
    std::vector<std::string> names;
    for (const auto &endpoint : options.endpoints) {
      std::string name;
      switch (endpoint.Scheme()) {
      case TransportScheme::kGrpc: name = "grpc://"; break;
      case TransportScheme::kGrpcs: name = "grpcs://"; break;
      case TransportScheme::kUnix: name = "unix://"; break;
      }
      names.push_back(name + endpoint.SocketAddress());
    }
    return NativeClientFactory{std::move(options), std::move(tls), std::move(names)};
  }

  [[nodiscard]] const std::vector<std::string> &Endpoints() const noexcept { return names_; }

  [[nodiscard]] Result<std::unique_ptr<Client>> operator()(const std::vector<std::string> &names) const {
    auto options = options_;
    options.endpoints.clear();
    for (const auto &name : names) {
      auto endpoint = Endpoint::Parse(name);
      if (!endpoint) return std::unexpected(endpoint.error());
      options.endpoints.push_back(std::move(*endpoint));
    }
    return Client::Connect(std::move(options), tls_);
  }

  [[nodiscard]] Result<std::unique_ptr<Client>> operator()(const std::vector<std::string> &names, std::uint64_t,
                                                           std::size_t) const {
    return (*this)(names);
  }

private:
  NativeClientFactory(ClientOptions options, grpcxx::SyncTlsClientOptions tls, std::vector<std::string> names)
      : options_(std::move(options)), tls_(std::move(tls)), names_(std::move(names)) {}
  ClientOptions options_;
  grpcxx::SyncTlsClientOptions tls_;
  std::vector<std::string> names_;
};

using NativeThreadLocalClient = ThreadLocalClient<std::unique_ptr<Client>, NativeClientFactory>;
using NativeWorkerClient = WorkerClient<std::unique_ptr<Client>>;

/** Validates settings now; connects on each caller thread's first WithClient.
 */
[[nodiscard]] inline Result<NativeThreadLocalClient> ConnectThreadLocalClient(ClientOptions options,
                                                                              grpcxx::SyncTlsClientOptions tls = {}) {
  auto factory = NativeClientFactory::Create(std::move(options), std::move(tls));
  if (!factory) return std::unexpected(factory.error());
  auto endpoints = factory->Endpoints();
  return NativeThreadLocalClient::Create(std::move(endpoints), std::move(*factory));
}

/** Creates slots lazily, preserving TLS settings and endpoint striping. */
[[nodiscard]] inline Result<NativeWorkerClient> ConnectWorkerClient(ClientOptions options, WorkerConfig config = {},
                                                                    grpcxx::SyncTlsClientOptions tls = {}) {
  auto factory = NativeClientFactory::Create(std::move(options), std::move(tls));
  if (!factory) return std::unexpected(factory.error());
  auto endpoints = factory->Endpoints();
  return NativeWorkerClient::Create(std::move(endpoints), config, std::move(*factory));
}

} // namespace greptimedb

namespace greptimedb {
/** Endpoint ordering is shared with ClientOptions; policy time is supplied by the caller.
 * Only connection failures are retried, before any domain request can reach the wire. */
template <std::size_t Capacity>
[[nodiscard]] Result<std::unique_ptr<Client>>
DialWithFailover(ClientOptions options, nexora::EndpointFailover<Capacity> &policy, std::uint64_t now_ns,
                 const grpcxx::SyncTlsClientOptions &tls = {}) {
  if (auto valid = options.Validate(); !valid) return std::unexpected(valid.error());
  if (options.endpoints.size() != policy.Size())
    return std::unexpected(Error::InvalidConfig("client endpoints and failover endpoints must have the same order"));
  for (std::size_t i = 0; i < policy.Size(); ++i) {
    const auto &endpoint = options.endpoints[i];
    const auto &target = policy.EndpointAt(i);
    if (endpoint.UnixPath().has_value())
      return std::unexpected(
          Error::InvalidConfig("client endpoints and failover endpoints must have matching IP and port order"));
    const auto ip = cares::ParseIpLiteral(endpoint.Host());
    if (!ip || *ip != target.ip || endpoint.Port() != target.port)
      return std::unexpected(Error::InvalidConfig("client endpoints and failover endpoints must have the same order"));
  }
  std::optional<Error> last;
  std::size_t attempts{};
  for (std::size_t attempt = 0; attempt < policy.Size(); ++attempt) {
    auto selected = policy.SelectNext(now_ns);
    if (!selected) break;
    auto single = options;
    single.endpoints = {options.endpoints[*selected]};
    ++attempts;
    auto client = Client::Connect(std::move(single), tls);
    if (client) {
      policy.MarkSuccess(*selected);
      return client;
    }
    last = std::move(client.error());
    if (last->Kind() != ErrorKind::kTransport && last->GrpcCode() != grpcxx::StatusCode::kUnavailable &&
        last->GrpcCode() != grpcxx::StatusCode::kDeadlineExceeded)
      return std::unexpected(*last);
    policy.MarkFailure(*selected, now_ns);
  }
  if (last) return std::unexpected(Error::AllEndpointsFailed(attempts, *last));
  return std::unexpected(Error{ErrorKind::kCircuitOpen, "no endpoint is ready"});
}
} // namespace greptimedb
