#include "client_example_support.h"

#include <greptimedb/threading/client_pool.h>
#include <grpcxx/sync/in_memory_channel_pair.h>
#include <nexora/pool/endpoint_failover.hpp>
#include <nexora/tls/tls_config.hpp>

#include <array>
#include <cstdlib>

namespace {
using namespace client_examples;

bool Tls() {
  const std::string directory = GREPTIMEDB_EXAMPLE_TLS_DIR;
  auto server_tls = nexora::TlsServerConfig::FromPemFilesWithClientAuth(
      directory + "/server-cert.pem", directory + "/server-key.pem", directory + "/ca.pem");
  auto client_tls = nexora::TlsClientConfig::CreateDefault();
  if (!Checked(server_tls, "server TLS") || !Checked(client_tls, "client TLS")) return false;
  if (!Checked(client_tls->SetCaCertFile(directory + "/ca.pem"), "CA") ||
      !Checked(client_tls->SetClientIdentityFiles(directory + "/client-cert.pem", directory + "/client-key.pem"),
               "client identity") ||
      !Checked(client_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2), "client ALPN") ||
      !Checked(server_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2), "server ALPN"))
    return false;
  auto pair = grpcxx::InMemoryChannelPair::CreateTls(
      *server_tls, *client_tls, [](grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
        if (event.event != grpcxx::ServerRequest::Event::kMessage) return;
        const auto _ = server.Send(event.stream_id, {});
        const auto _ = server.Finish(event.stream_id, {});
      });
  if (!Checked(pair, "TLS channel pair")) return false;
  auto response = (*pair)->Channel().Unary(kHealth, {});
  return Checked(response, "mutual TLS health") && Check(response->status.Ok(), "TLS health status");
}

bool Recovery() {
  auto listener = nexora::TcpListener::BindLoopbackV4();
  auto refused = nexora::TcpListener::BindLoopbackV4();
  if (!Checked(listener, "healthy listener") || !Checked(refused, "primary listener")) return false;
  const std::array endpoints{refused->LocalEndpoint(), listener->LocalEndpoint()};
  refused->Close();
  nexora::EndpointFailover policy(endpoints,
                                  {.base_cooldown_ns = 100, .policy = nexora::EndpointSelectionPolicy::kPickFirst});
  ClientOptions options;
  for (const auto &ep : endpoints) {
    auto parsed = Endpoint::Parse("grpc://127.0.0.1:" + std::to_string(ep.port));
    if (!Checked(parsed, "endpoint")) return false;
    options.endpoints.push_back(*parsed);
  }
  auto client = DialWithFailover(options, policy, 0);
  if (!Checked(client, "dial failover")) return false;
  OfflinePeer peer;
  auto server = grpcxx::SynchronousServer::AcceptTcp(
      *listener, std::chrono::seconds(2),
      [&peer](grpcxx::SynchronousServer &srv, const grpcxx::ServerRequest &event) noexcept {
        OfflinePeer::Health(&peer, srv, event);
      });
  if (!Checked(server, "accept healthy endpoint")) return false;
  peer.server = std::move(*server);
  (*client)->Channel().SetProgressHook(peer.server.get(), [](void *context) noexcept {
    return static_cast<grpcxx::SynchronousServer *>(context)->PollOnce();
  });
  if (!Checked((*client)->Ping(), "failover HealthCheck") ||
      !Check(!policy.IsHealthy(0, 1) && peer.requests == 1, "primary quarantine or healthy RPC failed"))
    return false;
  auto recovered = policy.SelectNext(100);
  if (!Checked(recovered, "cooldown recovery") || !Check(*recovered == 0, "endpoint did not recover")) return false;
  return Check(!nexora::ShouldFailoverWrite(true, true) && nexora::ShouldFailoverWrite(false, true) &&
                   !nexora::ShouldFailoverWrite(false, false),
               "write replay requires proof of no send and idempotency");
}

} // namespace

int main() { return Tls() && Recovery() && LiveProbe() ? EXIT_SUCCESS : EXIT_FAILURE; }
