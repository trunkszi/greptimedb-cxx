/** A Client's HTTP/2 state is thread-affine and has no internal client mutex.
 * Share the managers instead: each OS thread lazily creates its own clients.
 * The application creates three worker threads; each drives both TCP HTTP/2
 * peers inline. No server thread or GreptimeDB installation is required.
 * ./build.py test -R example_08
 * GREPTIMEDB_ENDPOINT optionally adds a live health probe on the main thread.
 */
#include "client_example_support.h"
#include <greptimedb/threading/worker_pool.h>

#include <array>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <ranges>
#include <thread>
#include <vector>

namespace {
using namespace client_examples;
constexpr std::size_t kSlots = 3;
using Session = std::unique_ptr<OfflinePeer>;

/** Each worker accepts and drives its own TCP peer on the same caller thread. */
Result<Session> ConnectLocal(const std::vector<std::string> &endpoints) {
  if (endpoints.empty()) return std::unexpected(Error::InvalidConfig("missing shard labels"));
  auto session = std::make_unique<OfflinePeer>();
  auto listener = nexora::TcpListener::BindLoopbackV4();
  if (!listener) return std::unexpected(Error::Transport(std::string(listener.error().Message())));
  auto channel = grpcxx::SynchronousChannel::ConnectTcp("127.0.0.1", listener->LocalPort());
  if (!channel) return std::unexpected(Error::ChannelStatus(channel.error()));
  auto server = grpcxx::SynchronousServer::AcceptTcp(
      *listener, std::chrono::seconds(2),
      [peer = session.get()](grpcxx::SynchronousServer &srv, const grpcxx::ServerRequest &event) noexcept {
        OfflinePeer::Health(peer, srv, event);
      });
  if (!server) return std::unexpected(Error::ChannelStatus(server.error()));
  session->server = std::move(*server);
  (*channel)->SetProgressHook(session->server.get(), [](void *context) noexcept {
    return static_cast<grpcxx::SynchronousServer *>(context)->PollOnce();
  });
  auto options = Options(endpoints.front());
  if (!options) return std::unexpected(options.error());
  auto client = Client::FromChannel(std::move(*options), std::move(*channel));
  if (!client) return std::unexpected(client.error());
  session->client = std::move(*client);
  return session;
}
std::uint64_t TableKey(std::string_view name) {
  std::uint64_t key = 0xcbf29ce484222325ULL;
  for (const auto byte : name) {
    key ^= static_cast<unsigned char>(byte);
    key *= 0x100000001b3ULL;
  }
  return key;
}
std::size_t SlotForKey(std::uint64_t key) {
  std::uint64_t hash = 0xcbf29ce484222325ULL;
  for (const auto shift : std::views::iota(0, 8)) {
    hash ^= (key >> (shift * 8)) & 0xffU;
    hash *= 0x100000001b3ULL;
  }
  return hash % kSlots;
}

bool Run() {
  const std::vector<std::string> endpoints{"grpc://primary.local:4001", "grpc://secondary.local:4001"};
  auto factory = [](const std::vector<std::string> &addresses) { return ConnectLocal(addresses); };
  auto local = ThreadLocalClient<Session, decltype(factory)>::Create(endpoints, factory);
  WorkerConfig config;
  config.slots_per_thread = kSlots;
  config.balance = WorkerBalance::kKeyHash;
  config.stripe_endpoints = true;
  auto workers = WorkerClient<Session>::Create(
      endpoints, config,
      [](const std::vector<std::string> &addresses, std::uint64_t, std::size_t) { return ConnectLocal(addresses); });
  if (!Checked(local, "thread-local manager") || !Checked(workers, "worker manager")) return false;
  if (!Check(workers->PreviewEndpoints(1, 0).front() == endpoints[1], "endpoint rotation differs")) return false;
  std::array<bool, 3> passed{};
  std::array<std::thread, 3> threads;
  for (const auto worker : std::views::iota(std::size_t{0}, threads.size())) {
    threads[worker] = std::thread([&, worker] {
      const Client *first = nullptr;
      auto once = local->WithClient([&](Session &session) {
        first = session->client.get();
        return session->client->Ping();
      });
      auto twice = local->WithClient([&](Session &session) -> Result<void> {
        if (session->client.get() != first || session->requests != 1)
          return std::unexpected(Error::InvalidConfig("per-thread client was not reused"));
        return session->client->Ping();
      });
      bool success = Checked(once, "first thread-local ping") && Checked(twice, "reused ping");
      success = Check(local->CloseCurrentThread(), "thread-local cache did not close") && success;
      for (const auto table : {"cpu_metrics", "mem_metrics", "net_metrics"}) {
        const auto key = TableKey(table);
        {
          auto lease = workers->LeaseByKey(key);
          if (!Checked(lease, "keyed worker lease")) {
            success = false;
            break;
          }
          success = Check(lease->Slot() == SlotForKey(key), "key placement differs") && success;
          success = Checked(lease->Client()->client->Ping(), "leased HealthCheck") && success;
          auto duplicate = workers->LeaseByKey(key);
          success = Check(!duplicate, "a leased keyed slot must reject another checkout") && success;
        }
        success = Checked(workers->WithClientByKey(key, [](Session &session) { return session->client->Ping(); }),
                          "RAII slot reuse") &&
                  success;
      }
      success = Check(workers->CloseCurrentThread(), "worker cache did not close") && success;
      success = Check(CloseThreadCaches() == 0, "thread caches must already be empty") && success;
      passed[worker] = success;
    });
  }
  for (auto &thread : threads)
    thread.join();
  if (!Check(std::ranges::all_of(passed, [](bool value) { return value; }), "worker demo failed")) return false;
  std::println("Three OS workers: thread-local reuse, {} keyed slots, TCP "
               "HealthCheck, RAII release",
               kSlots);
  return LiveProbe();
}
} // namespace
int main() { return Run() ? EXIT_SUCCESS : EXIT_FAILURE; }
