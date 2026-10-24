/** Lazy caller-thread factories for the native, non-movable Client. */
#pragma once

#include "greptimedb/client/client.h"
#include "greptimedb/threading/worker_pool.h"

namespace greptimedb {

/** Immutable connection settings shared by per-thread client factories.
 * Each invocation connects only to the first endpoint in the supplied order.
 * RPC failover requires a policy-aware factory instead of this adapter.
 */
class NativeClientFactory {
public:
  [[nodiscard]] static Result<NativeClientFactory> Create(ClientOptions options,
                                                          TlsOptions tls = {}) {
    if (auto valid = options.Validate(); !valid)
      return std::unexpected(valid.error());
    auto state = std::make_shared<State>();
    for (const auto &endpoint : options.endpoints) {
      std::string name;
      switch (endpoint.Scheme()) {
      case TransportScheme::kGrpc:
        name = "grpc://";
        break;
      case TransportScheme::kGrpcs:
        name = "grpcs://";
        break;
      case TransportScheme::kUnix:
        name = "unix://";
        break;
      }
      state->names.push_back(name + endpoint.SocketAddress());
    }
    state->options = std::move(options);
    state->tls = std::move(tls);
    return NativeClientFactory{std::move(state)};
  }

  [[nodiscard]] const std::vector<std::string> &Endpoints() const noexcept {
    return state_->names;
  }

  [[nodiscard]] Result<std::unique_ptr<Client>>
  operator()(const std::vector<std::string> &names) const {
    auto options = state_->options;
    options.endpoints.clear();
    for (const auto &name : names) {
      auto endpoint = Endpoint::Parse(name);
      if (!endpoint)
        return std::unexpected(endpoint.error());
      options.endpoints.push_back(std::move(*endpoint));
    }
    return Client::Connect(std::move(options), state_->tls);
  }

  [[nodiscard]] Result<std::unique_ptr<Client>>
  operator()(const std::vector<std::string> &names, std::uint64_t,
             std::size_t) const {
    return (*this)(names);
  }

private:
  struct State {
    ClientOptions options;
    TlsOptions tls;
    std::vector<std::string> names;
  };
  explicit NativeClientFactory(std::shared_ptr<const State> state)
      : state_(std::move(state)) {}
  std::shared_ptr<const State> state_;
};

using NativeThreadLocalClient =
    ThreadLocalClient<std::unique_ptr<Client>, NativeClientFactory>;
using NativeWorkerClient = WorkerClient<std::unique_ptr<Client>>;

/** Validates settings now; connects on each caller thread's first WithClient.
 */
[[nodiscard]] inline Result<NativeThreadLocalClient>
ConnectThreadLocalClient(ClientOptions options, TlsOptions tls = {}) {
  auto factory =
      NativeClientFactory::Create(std::move(options), std::move(tls));
  if (!factory)
    return std::unexpected(factory.error());
  auto endpoints = factory->Endpoints();
  return NativeThreadLocalClient::Create(std::move(endpoints),
                                         std::move(*factory));
}

/** Creates slots lazily, preserving TLS settings and endpoint striping. */
[[nodiscard]] inline Result<NativeWorkerClient>
ConnectWorkerClient(ClientOptions options, WorkerConfig config = {},
                    TlsOptions tls = {}) {
  auto factory =
      NativeClientFactory::Create(std::move(options), std::move(tls));
  if (!factory)
    return std::unexpected(factory.error());
  auto endpoints = factory->Endpoints();
  return NativeWorkerClient::Create(std::move(endpoints), config,
                                    std::move(*factory));
}

} // namespace greptimedb
