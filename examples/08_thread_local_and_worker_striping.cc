/** A Client's HTTP/2 state is thread-affine and has no internal client mutex.
 * Share the managers instead: each OS thread lazily creates its own clients.
 * The application creates three worker threads; each drives both TCP HTTP/2
 * peers inline. No server thread or GreptimeDB installation is required.
 * ./build.sh test -R example_08
 * GREPTIMEDB_ENDPOINT optionally adds a live health probe on the main thread. */
#include "client_example_support.h"
#include <greptimedb/threading/worker_pool.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <ranges>
#include <thread>
#include <vector>

namespace {
using namespace client_examples;
constexpr std::size_t kSlots = 3;
using Session = std::unique_ptr<OfflinePeer>;

/** A connection factory creates an isolated loopback listener for its client,
 * so accepted transports never cross thread boundaries. Routing labels choose
 * logical shards; every offline shard is served by this local health peer. */
Result<MemoryPipe::Pair> LoopbackPair() {
  struct Listener {
    int fd{-1};
    ~Listener() { if (fd >= 0) ::close(fd); }
  } listener{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
  if (listener.fd < 0) return std::unexpected(Error::Transport("loopback socket failed"));
  sockaddr_in address{};
  address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(listener.fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
      ::listen(listener.fd, 1) != 0)
    return std::unexpected(Error::Transport("loopback bind/listen failed"));
  socklen_t length = sizeof(address);
  if (::getsockname(listener.fd, reinterpret_cast<sockaddr *>(&address), &length) != 0)
    return std::unexpected(Error::Transport("loopback getsockname failed"));
  auto client = TcpTransport::Connect("127.0.0.1", ntohs(address.sin_port));
  if (!client) return std::unexpected(Error::Grpc(client.error()));
  for (int tick = 0; tick < 8192; ++tick) {
    auto progressed = (*client)->PollOnce();
    if (!progressed) return std::unexpected(Error::Grpc(progressed.error()));
    const int fd = ::accept4(listener.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd >= 0) {
      auto server = TcpTransport::Adopt(fd);
      if (!server) return std::unexpected(Error::Grpc(server.error()));
      return MemoryPipe::Pair{std::move(*client), std::move(*server)};
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
      return std::unexpected(Error::Transport("loopback accept failed"));
  }
  return std::unexpected(Error::Transport("loopback connection did not become ready"));
}
Result<Session> ConnectLocal(const std::vector<std::string> &endpoints) {
  if (endpoints.empty()) return std::unexpected(Error::InvalidConfig("missing shard labels"));
  Session session{new (std::nothrow) OfflinePeer};
  if (!session) return std::unexpected(Error::Transport("session allocation failed"));
  auto wire = LoopbackPair();
  if (!wire) return std::unexpected(wire.error());
  if (!session->OpenPair(std::move(*wire), &OfflinePeer::Health, session.get(), endpoints.front()))
    return std::unexpected(Error::Transport("offline TCP health client setup failed"));
  return session;
}
std::uint64_t TableKey(std::string_view name) {
  std::uint64_t key = 0xcbf29ce484222325ULL;
  for (const auto byte : name) { key ^= static_cast<unsigned char>(byte); key *= 0x100000001b3ULL; }
  return key;
}
std::size_t SlotForKey(std::uint64_t key) {
  std::uint64_t hash = 0xcbf29ce484222325ULL;
  for (const auto shift : std::views::iota(0, 8)) {
    hash ^= (key >> (shift * 8)) & 0xffU; hash *= 0x100000001b3ULL;
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
  auto workers = WorkerClient<Session>::Create(endpoints, config,
      [](const std::vector<std::string> &addresses, std::uint64_t, std::size_t) {
        return ConnectLocal(addresses);
      });
  if (!Checked(local, "thread-local manager") || !Checked(workers, "worker manager")) return false;
  if (!Check(workers->PreviewEndpoints(1, 0).front() == endpoints[1], "endpoint rotation differs"))
    return false;
  std::array<bool, 3> passed{};
  std::array<std::thread, 3> threads;
  for (const auto worker : std::views::iota(std::size_t{0}, threads.size())) {
    threads[worker] = std::thread([&, worker] {
      const Client *first = nullptr;
      auto once = local->WithClient([&](Session &session) {
        first = session->client.get(); return session->client->Ping();
      });
      auto twice = local->WithClient([&](Session &session) -> Result<void> {
        if (session->client.get() != first || session->requests != 1)
          return std::unexpected(Error::InvalidConfig("per-thread client was not reused"));
        return session->client->Ping();
      });
      bool success = Checked(once, "first thread-local ping") && Checked(twice, "reused ping");
      success = Check(local->CloseCurrentThread(), "thread-local cache did not close") && success;
      for (const auto table : {"l1_books", "trades", "funding"}) {
        const auto key = TableKey(table);
        {
          auto lease = workers->LeaseByKey(key);
          if (!Checked(lease, "keyed worker lease")) { success = false; break; }
          success = Check(lease->slot() == SlotForKey(key), "key placement differs") && success;
          success = Checked(lease->client()->client->Ping(), "leased HealthCheck") && success;
          auto duplicate = workers->LeaseByKey(key);
          success = Check(!duplicate, "a leased keyed slot must reject another checkout") && success;
        }
        success = Checked(workers->WithClientByKey(key, [](Session &session) {
          return session->client->Ping();
        }), "RAII slot reuse") && success;
      }
      success = Check(workers->CloseCurrentThread(), "worker cache did not close") && success;
      success = Check(CloseThreadCaches() == 0, "thread caches must already be empty") && success;
      passed[worker] = success;
    });
  }
  for (auto &thread : threads) thread.join();
  if (!Check(std::ranges::all_of(passed, [](bool value) { return value; }), "worker demo failed"))
    return false;
  fmt::print("Three OS workers: thread-local reuse, {} keyed slots, TCP HealthCheck, RAII release\n", kSlots);
  return LiveProbe();
}
} // namespace
int main() { return Run() ? EXIT_SUCCESS : EXIT_FAILURE; }
