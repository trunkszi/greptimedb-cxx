#include <greptimedb/threading/client_pool.h>
#include <grpcxx/sync/synchronous_server.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <thread>

namespace {
using namespace greptimedb;

TEST(FailoverIntegration, DialQuarantinesRefusedEndpoint) {
  auto listener = nexora::TcpListener::BindLoopbackV4();
  ASSERT_TRUE(listener);
  auto refused = nexora::TcpListener::BindLoopbackV4();
  ASSERT_TRUE(refused);
  const auto refused_endpoint = refused->LocalEndpoint();
  refused->Close();
  const std::array endpoints{refused_endpoint, listener->LocalEndpoint()};
  nexora::EndpointFailover policy(endpoints,
                                  {.base_cooldown_ns = 100, .policy = nexora::EndpointSelectionPolicy::kPickFirst});
  ClientOptions options;
  for (const auto &ep : endpoints) {
    auto endpoint = Endpoint::Parse("grpc://127.0.0.1:" + std::to_string(ep.port));
    ASSERT_TRUE(endpoint);
    options.endpoints.push_back(*endpoint);
  }
  auto client = DialWithFailover(options, policy, 10);
  ASSERT_TRUE(client);
  EXPECT_FALSE(policy.IsHealthy(0, 10));
  EXPECT_EQ(policy.FailureCount(0), 1U);
  EXPECT_TRUE(policy.IsHealthy(1, 10));
  auto recovered = policy.SelectNext(110);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(*recovered, 0U);
}

TEST(FailoverIntegration, PoolsUseCallerThreadChannels) {
  std::atomic<unsigned> calls{};
  auto listener = nexora::TcpListener::BindLoopbackV4();
  ASSERT_TRUE(listener);
  const auto endpoint = listener->LocalEndpoint();
  std::jthread server([&] {
    for (unsigned i = 0; i < 2; ++i) {
      auto peer = grpcxx::SynchronousServer::AcceptTcp(
          *listener, std::chrono::seconds(2),
          [&calls](grpcxx::SynchronousServer &srv, const grpcxx::ServerRequest &request) noexcept {
            if (request.event != grpcxx::ServerRequest::Event::kMessage) return;
            const auto _ = srv.Send(request.stream_id, {});
            const auto _ = srv.Finish(request.stream_id, {});
            calls.fetch_add(1);
          });
      if (!peer) return;
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
      while (std::chrono::steady_clock::now() < deadline && calls.load() <= i)
        if (!(*peer)->PollOnce()) return;
    }
  });
  const std::vector<std::string> names{"grpc://127.0.0.1:" + std::to_string(endpoint.port)};
  auto dial = [endpoint](const std::vector<std::string> &addresses) -> Result<std::unique_ptr<Client>> {
    auto parsed = Endpoint::Parse(addresses.front());
    if (!parsed) return std::unexpected(parsed.error());
    const std::array endpoints{endpoint};
    nexora::EndpointFailover policy(endpoints, {.policy = nexora::EndpointSelectionPolicy::kPickFirst});
    return DialWithFailover(ClientOptions(*parsed), policy, 0);
  };
  {
    auto local = ThreadLocalClient<std::unique_ptr<Client>, decltype(dial)>::Create(names, dial);
    ASSERT_TRUE(local);
    auto ping = local->WithClient([](auto &client) { return client->Ping(); });
    ASSERT_TRUE(ping);
  }
  {
    auto workers = NativeWorkerClient::Create(
        names, {.slots_per_thread = 1},
        [dial](const auto &addresses, std::uint64_t, std::size_t) { return dial(addresses); });
    ASSERT_TRUE(workers);
    auto lease = workers->Lease();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(lease->Client()->Ping());
  }
  server.join();
  EXPECT_EQ(calls.load(), 2U);
}

TEST(FailoverIntegration, WriteReplayRequiresNoSendProof) {
  EXPECT_FALSE(nexora::ShouldFailoverWrite(true, true));
  EXPECT_FALSE(nexora::ShouldFailoverWrite(true, false));
  EXPECT_FALSE(nexora::ShouldFailoverWrite(false, false));
  EXPECT_TRUE(nexora::ShouldFailoverWrite(false, true));
}
TEST(FailoverIntegration, ReversedEndpointsRejectBeforeDial) {
  auto first = nexora::TcpListener::BindLoopbackV4();
  auto second = nexora::TcpListener::BindLoopbackV4();
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  const std::array targets{first->LocalEndpoint(), second->LocalEndpoint()};
  nexora::EndpointFailover policy(targets);
  ClientOptions options;
  for (const auto &target : {targets[1], targets[0]}) {
    auto endpoint = Endpoint::Parse("grpc://127.0.0.1:" + std::to_string(target.port));
    ASSERT_TRUE(endpoint);
    options.endpoints.push_back(*endpoint);
  }
  options.connect_timeout = std::chrono::milliseconds(100);
  auto client = DialWithFailover(options, policy, 10);
  EXPECT_FALSE(client);
  if (!client) {
    EXPECT_EQ(client.error().Kind(), ErrorKind::kInvalidConfig);
    EXPECT_EQ(client.error().Attempts(), 0U);
  }
  EXPECT_EQ(policy.FailureCount(0), 0U);
  EXPECT_EQ(policy.FailureCount(1), 0U);
  EXPECT_FALSE(first->Accept(0));
  EXPECT_FALSE(second->Accept(0));
}

TEST(FailoverIntegration, EndpointIdentityRejectsBeforeDial) {
  auto listener = nexora::TcpListener::BindLoopbackV4();
  auto other = nexora::TcpListener::BindLoopbackV4();
  ASSERT_TRUE(listener);
  ASSERT_TRUE(other);
  const std::array targets{listener->LocalEndpoint()};
  const auto port = std::to_string(targets[0].port);
  const std::array addresses{"grpc://127.0.0.1:" + std::to_string(other->LocalEndpoint().port),
                             "grpc://127.0.0.2:" + port, "grpc://[::1]:" + port, std::string{"unix:///tmp/test.sock"},
                             "grpc://localhost:" + port};
  for (const auto &address : addresses) {
    SCOPED_TRACE(address);
    nexora::EndpointFailover policy(targets);
    auto endpoint = Endpoint::Parse(address);
    ASSERT_TRUE(endpoint);
    ClientOptions options(*endpoint);
    options.connect_timeout = std::chrono::milliseconds(100);
    auto client = DialWithFailover(options, policy, 10);
    EXPECT_FALSE(client);
    if (!client) {
      EXPECT_EQ(client.error().Kind(), ErrorKind::kInvalidConfig);
      EXPECT_EQ(client.error().Attempts(), 0U);
    }
    EXPECT_EQ(policy.FailureCount(0), 0U);
    EXPECT_EQ(policy.QuarantinedUntilNs(0), 0U);
    EXPECT_FALSE(listener->Accept(0));
    EXPECT_FALSE(other->Accept(0));
  }
}

} // namespace
