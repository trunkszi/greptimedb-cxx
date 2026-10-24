/** TLS trust/identity, endpoint selection, circuit breaking and safe replay.
 * ./build.sh test -R example_09
 * GREPTIMEDB_ENDPOINT=grpcs://host:4001 and GREPTIMEDB_CA_PEM=/path/to/ca.pem
 * add a live health probe. Offline RPCs use caller-driven HTTP/2 peers. The
 * example composes FailoverPolicy with Client explicitly; injected policy time
 * makes cooldown/recovery deterministic without waiting or a server thread. */
#include "client_example_support.h"
#include <greptimedb/failover/failover.h>
#include <grpcxx/tls/tls_context.h>

#include <array>
#include <fstream>
#include <ranges>
#include <sstream>
#include <vector>

namespace {
using namespace client_examples;
using namespace std::chrono_literals;
constexpr auto kCooldown = 20ms;

bool SelectionStrategies() {
  for (const auto selection : {EndpointSelection::kRoundRobin, EndpointSelection::kSticky,
                              EndpointSelection::kRandom, EndpointSelection::kLatencyAware}) {
    FailoverConfig config;
    config.endpoints = {"primary", "secondary"};
    config.selection = selection; config.max_attempts = 2;
    config.failure_threshold = 1; config.circuit_cooldown = kCooldown;
    config.random_seed = 7;
    auto policy = FailoverPolicy::FromConfig(config);
    policy.RecordSuccess(0, 900us); policy.RecordSuccess(1, 100us); policy.RecordSuccess(1, 140us);
    auto chosen = policy.Select(FailoverTime{} + 1s);
    if (!Checked(chosen, "select endpoint")) return false;
    if (selection == EndpointSelection::kLatencyAware &&
        !Check(*chosen == 1, "latency-aware selection must prefer lower RTT")) return false;
    const auto health = policy.Health(1);
    if (!Check(health != nullptr && health->ewma_rtt_us >= 100 && health->ewma_rtt_us <= 140,
               "EWMA RTT must reflect observed samples")) return false;
    fmt::print("Selection {}: endpoint {}, secondary EWMA={} us\n",
        static_cast<unsigned>(selection), *chosen, health->ewma_rtt_us);
  }
  const auto uncertain = Error::Grpc(grpcxx::Status{grpcxx::StatusCode::kUnavailable, "lost after send"});
  const auto retry = Error::ServerStatus(4003, "server permits replay", true);
  WriteHints hints;
  if (!Checked(hints.SetAutoCreateTable(true), "write hint")) return false;
  if (!Check(!FailoverPolicy::ShouldFailoverWrite(true, uncertain) &&
      FailoverPolicy::ShouldFailoverWrite(false, uncertain) &&
      FailoverPolicy::ShouldFailoverWrite(true, retry), "uncertain write replay rules differ")) return false;
  fmt::print("Write hints ({}) do not override server replay permission\n", hints.FormatHeaderValue());
  return true;
}

struct EndpointService {
  OfflinePeer peer;
  bool reject_first{};
  std::size_t health_checks{}, queries{};
  static void Handle(void *context, MockGrpcServer &server, const MockRequest &event) noexcept {
    if (event.event != MockRequest::Event::kMessage) return;
    auto &self = *static_cast<EndpointService *>(context);
    if (event.path == kHealth) {
      ++self.health_checks;
      if (self.peer.Send(server, event.stream_id, proto::HealthCheckResponse{}))
        self.peer.Finish(server, event.stream_id);
      return;
    }
    proto::PromqlRequest request;
    if (!self.peer.Require(event.path == kProm, "expected PromQL") ||
        !self.peer.Parse(event, request) ||
        !self.peer.Require(request.has_instant_query() && request.instant_query().query() == "up",
                           "unexpected recovery request")) return;
    ++self.queries;
    if (self.reject_first && self.queries == 1) {
      self.peer.Finish(server, event.stream_id,
          grpcxx::Status{grpcxx::StatusCode::kUnavailable, "primary temporarily unavailable"});
      return;
    }
    proto::PromqlResponse response;
    response.mutable_header()->mutable_status()->set_status_code(0);
    response.set_body(R"({"status":"success","data":{"resultType":"vector","result":[]}})");
    if (self.peer.Send(server, event.stream_id, response)) self.peer.Finish(server, event.stream_id);
  }
};

/** Read retries may cross endpoints. A half-open endpoint must pass a real
 * HealthCheck before its first application RPC is admitted. */
Result<std::size_t> ReadWithFailover(FailoverPolicy &policy, FailoverTime now,
                                    std::array<EndpointService, 2> &services) {
  std::vector<std::size_t> attempted;
  std::optional<Error> last;
  for (std::size_t attempt = 0; attempt < services.size(); ++attempt) {
    auto chosen = policy.SelectExcluding(now, attempted);
    if (!chosen) return std::unexpected(chosen.error());
    attempted.push_back(*chosen);
    auto &client = *services[*chosen].peer.client;
    if (policy.NeedsProbe(*chosen, now)) {
      auto probe = client.ProbeHealth();
      if (!probe || !probe->healthy) {
        last = probe ? Error::Transport("admission health probe failed") : probe.error();
        policy.RecordFailure(*chosen, now);
        continue;
      }
      policy.RecordProbeSuccess(*chosen);
    }
    const auto started = std::chrono::steady_clock::now();
    auto response = client.PromInstantQuery("up");
    if (response) {
      auto body = response->AsJson();
      if (!body) return std::unexpected(Error{ErrorKind::kDecode, "PromQL response is not UTF-8"});
      if (body->find("success") == std::string_view::npos)
        return std::unexpected(Error{ErrorKind::kProtocol, "PromQL response did not succeed"});
      policy.RecordSuccess(*chosen, std::chrono::steady_clock::now() - started);
      return *chosen;
    }
    last = response.error();
    policy.RecordFailure(*chosen, now);
  }
  return std::unexpected(last ? *last : Error::Transport("all read endpoints failed"));
}

bool Recovery() {
  std::array<EndpointService, 2> services;
  services[0].reject_first = true;
  if (!services[0].peer.Open(&EndpointService::Handle, &services[0]) ||
      !services[1].peer.Open(&EndpointService::Handle, &services[1])) return false;
  FailoverConfig config;
  config.endpoints = {"primary", "secondary"}; config.max_attempts = 2;
  config.selection = EndpointSelection::kRoundRobin;
  config.failure_threshold = 1; config.circuit_cooldown = kCooldown;
  auto policy = FailoverPolicy::FromConfig(config);
  std::vector<FailoverEvent> events;
  policy.OnFailover([&events](const FailoverEvent &event) { events.push_back(event); });
  const auto opened_at = FailoverTime{} + 1s;
  auto first = ReadWithFailover(policy, opened_at, services);
  if (!Checked(first, "read with primary failure") || !Check(*first == 1, "secondary was not selected"))
    return false;
  const auto failed_primary = policy.Health(0);
  if (!Check(failed_primary != nullptr && failed_primary->circuit_opened_at == opened_at,
             "primary circuit must open after its failure") ||
      !Check(!policy.NeedsProbe(0, opened_at), "open circuit is not eligible before cooldown"))
    return false;
  const auto recovered_at = opened_at + kCooldown + 1ns;
  if (!Check(policy.NeedsProbe(0, recovered_at), "cooled primary must require a probe")) return false;
  auto recovered = ReadWithFailover(policy, recovered_at, services);
  if (!Checked(recovered, "half-open primary recovery") ||
      !Check(*recovered == 0 && services[0].health_checks == 1 && services[0].queries == 2 &&
             services[1].queries == 1, "primary recovery must probe before its second query") ||
      !Check(policy.Health(0) != nullptr && !policy.Health(0)->circuit_opened_at &&
             events.size() == 2 && events[0].from_endpoint == "primary" &&
             events[0].to_endpoint == "secondary" && events[1].to_endpoint == "primary",
             "circuit state or observed switch history differs")) return false;
  for (const auto &event : events)
    fmt::print("Failover: {} -> {} ({}, attempt {})\n", event.from_endpoint,
        event.to_endpoint, event.reason, event.attempt);
  return Check(!services[0].peer.failure && !services[1].peer.failure, "server validation failed");
}

std::string ReadPem(std::string_view name) {
  std::ifstream input(std::string(GREPTIMEDB_EXAMPLE_TLS_DIR) + "/" + std::string(name));
  std::ostringstream contents;
  contents << input.rdbuf();
  return contents.str();
}
bool Tls() {
  TlsOptions options;
  options.ca_file = std::string(GREPTIMEDB_EXAMPLE_TLS_DIR) + "/ca.pem";
  options.certificate_pem = ReadPem("client-cert.pem");
  options.private_key_pem = ReadPem("client-key.pem");
  options.server_name_override = "localhost";
  auto client_context = options.CreateContext();
  if (!Checked(client_context, "mutual TLS client context")) return false;
  grpcxx::TlsContextOptions server_options;
  server_options.mode = grpcxx::TlsMode::kServer;
  server_options.verify_peer = true; server_options.require_client_certificate = true;
  server_options.ca_file = options.ca_file;
  server_options.certificate_file = std::string(GREPTIMEDB_EXAMPLE_TLS_DIR) + "/server-cert.pem";
  server_options.private_key_file = std::string(GREPTIMEDB_EXAMPLE_TLS_DIR) + "/server-key.pem";
  auto server_context = grpcxx::TlsContext::Create(server_options);
  if (!Checked(server_context, "mutual TLS server context")) return false;
  auto client_session = options.CreateSession(*client_context, "localhost");
  auto server_session = grpcxx::TlsSession::Create(*server_context);
  auto wire = MemoryPipe::CreatePair();
  if (!Checked(client_session, "TLS client session") || !Checked(server_session, "TLS server session") ||
      !Checked(wire, "TLS memory transport")) return false;
  auto client = TlsTransport::Create(std::move(wire->first), std::move(*client_session));
  auto server = TlsTransport::Create(std::move(wire->second), std::move(*server_session));
  if (!Checked(client, "TLS client transport") || !Checked(server, "TLS server transport")) return false;
  OfflinePeer peer;
  if (!peer.OpenPair({std::move(*client), std::move(*server)}, &OfflinePeer::Health, &peer,
                     "grpcs://localhost:4001") || !Checked(peer.client->Ping(), "mutual TLS HealthCheck"))
    return false;
  if (!Check(peer.requests == 1 && !peer.failure, "mutual TLS peer did not observe health check"))
    return false;
  fmt::print("Mutual TLS: CA verified, localhost SNI, client identity loaded (private key redacted)\n");
  return true;
}

bool Run() { return SelectionStrategies() && Recovery() && Tls() && LiveProbe(); }
} // namespace
int main() { return Run() ? EXIT_SUCCESS : EXIT_FAILURE; }
