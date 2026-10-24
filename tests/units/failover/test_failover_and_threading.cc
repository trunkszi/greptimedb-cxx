/** Endpoint selection, circuit breaking, write replay, and per-thread slot
 * leasing. */
#include <greptimedb/failover/failover.h>
#include <greptimedb/threading/thread_local_pool.h>
#include <greptimedb/threading/worker_pool.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using greptimedb::EndpointHealthState;
using greptimedb::EndpointSelection;
using greptimedb::Error;
using greptimedb::ErrorKind;
using greptimedb::FailoverConfig;
using greptimedb::FailoverEvent;
using greptimedb::FailoverPolicy;
using greptimedb::Result;
using greptimedb::ThreadLocalClient;
using greptimedb::WorkerBalance;
using greptimedb::WorkerClient;
using greptimedb::WorkerConfig;
using Clock = std::chrono::steady_clock;

FailoverPolicy Names(std::initializer_list<const char *> names) {
  return FailoverPolicy{std::vector<std::string>{names.begin(), names.end()}};
}

std::chrono::nanoseconds AsCooldown(std::chrono::nanoseconds cooldown) {
  return cooldown;
}

template <typename Duration>
std::chrono::nanoseconds AsCooldown(Duration cooldown) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(cooldown);
}

std::array<std::size_t, 6> DrawSix(FailoverPolicy &policy,
                                   Clock::time_point now) {
  std::array<std::size_t, 6> picks{};
  for (auto &pick : picks) {
    auto selected = policy.Select(now);
    EXPECT_TRUE(selected);
    if (selected)
      pick = *selected;
  }
  return picks;
}

struct FakeClient {
  std::vector<std::string> endpoints;
  std::uint64_t worker_id{};
  std::size_t slot{};
  std::thread::id owner{std::this_thread::get_id()};
  int stamp{};
};

template <typename Lease> FakeClient &ClientOf(Lease &lease) { return *lease; }

FakeClient MakeClient(const auto &endpoints, std::uint64_t worker_id,
                      std::size_t slot, int stamp) {
  FakeClient client;
  client.endpoints.assign(std::begin(endpoints), std::end(endpoints));
  client.worker_id = worker_id;
  client.slot = slot;
  client.owner = std::this_thread::get_id();
  client.stamp = stamp;
  return client;
}

TEST(FailoverPolicy, RoundRobinWalksThenWraps) {
  auto policy = Names({"a", "b", "c"});
  const auto now = Clock::now();
  EXPECT_EQ(policy.Selection(), EndpointSelection::kRoundRobin);
  EXPECT_EQ(policy.MaxAttempts(), 3U);
  const auto config = policy.Config();
  EXPECT_EQ(config.selection, EndpointSelection::kRoundRobin);
  EXPECT_EQ(config.failure_threshold, 3U);
  EXPECT_EQ(AsCooldown(config.circuit_cooldown), std::chrono::seconds(30));
  EXPECT_EQ(config.max_attempts, 3U);
  static_assert(std::is_copy_constructible_v<FailoverConfig>);
  EXPECT_EQ(config.endpoints, (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_FALSE(config.random_seed.has_value());

  std::array<std::size_t, 4> picks{};
  for (auto &pick : picks) {
    auto selected = policy.Select(now);
    ASSERT_TRUE(selected);
    pick = *selected;
  }
  EXPECT_EQ(picks, (std::array<std::size_t, 4>{0, 1, 2, 0}));
}

TEST(FailoverPolicy, StickyStaysUntilTheCircuitOpens) {
  std::vector<FailoverEvent> events;
  auto policy = Names({"a", "b"});
  policy.WithSelection(EndpointSelection::kSticky)
      .WithCircuitBreaker(1, std::chrono::seconds(60))
      .OnFailover([&events](const FailoverEvent &event) noexcept {
        events.push_back(event);
      });
  const auto now = Clock::now();
  EXPECT_TRUE(policy.HasObserver());
  auto first = policy.Select(now);
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, 0U);
  auto second = policy.Select(now);
  ASSERT_TRUE(second);
  EXPECT_EQ(*second, 0U);
  EXPECT_TRUE(events.empty());

  policy.RecordFailure(0, now);
  auto moved = policy.Select(now);
  ASSERT_TRUE(moved);
  EXPECT_EQ(*moved, 1U);
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].from_endpoint, "a");
  EXPECT_EQ(events[0].to_endpoint, "b");
  EXPECT_EQ(events[0].reason, "circuit open");
  EXPECT_GE(events[0].attempt, 1U);
}

TEST(FailoverPolicy, RandomSeedRepeatsAndZeroIsStoredAsOne) {
  const auto now = Clock::now();
  auto left = Names({"a", "b", "c", "d"});
  left.WithSelection(EndpointSelection::kRandom).WithRandomSeed(0);
  EXPECT_EQ(left.RandomSeed(), std::optional<std::uint64_t>{1});
  EXPECT_EQ(left.Config().random_seed, std::optional<std::uint64_t>{1});
  const auto left_picks = DrawSix(left, now);

  auto restored = FailoverPolicy::FromConfig(left.Config());
  EXPECT_EQ(restored.Selection(), EndpointSelection::kRandom);
  EXPECT_EQ(restored.RandomSeed(), std::optional<std::uint64_t>{1});
  EXPECT_FALSE(restored.HasObserver());

  auto again = Names({"a", "b", "c", "d"});
  again.WithSelection(EndpointSelection::kRandom).WithRandomSeed(1);
  EXPECT_EQ(left_picks, DrawSix(again, now));
  bool varied = false;
  for (const auto pick : left_picks)
    varied = varied || pick != left_picks[0];
  EXPECT_TRUE(varied);
}

TEST(FailoverPolicy, FromConfigDoesNotRestoreObserver) {
  auto policy = Names({"a", "b"});
  policy.WithRandomSeed(0).OnFailover([](const FailoverEvent &) noexcept {});
  EXPECT_TRUE(policy.HasObserver());
  auto config = policy.Config();
  static_assert(std::is_copy_constructible_v<FailoverConfig>);
  EXPECT_EQ(config.random_seed, std::optional<std::uint64_t>{1});
  auto restored = FailoverPolicy::FromConfig(std::move(config));
  EXPECT_FALSE(restored.HasObserver());
  EXPECT_EQ(restored.RandomSeed(), std::optional<std::uint64_t>{1});

  auto injected = policy.Config();
  auto dropped = FailoverPolicy::FromConfig(std::move(injected));
  EXPECT_FALSE(dropped.HasObserver());
}

TEST(FailoverPolicy, LatencyPrefersTheSmallerSampleAndUnknownsLose) {
  const auto now = Clock::now();
  auto policy = Names({"slow", "fast", "unknown"});
  policy.WithSelection(EndpointSelection::kLatencyAware);
  auto unsampled = policy.Select(now);
  ASSERT_TRUE(unsampled);
  EXPECT_EQ(*unsampled, 0U);

  policy.RecordSuccess(0, std::chrono::milliseconds(40));
  policy.RecordSuccess(1, std::chrono::milliseconds(2));
  const auto *fast = policy.Health(1);
  const auto *unknown = policy.Health(2);
  ASSERT_NE(fast, nullptr);
  ASSERT_NE(unknown, nullptr);
  EXPECT_EQ(fast->ewma_rtt_us, 2000U);
  EXPECT_EQ(policy.Health(0)->ewma_rtt_us, 40000U);
  EXPECT_EQ(unknown->ewma_rtt_us, 0U);
  EXPECT_TRUE(fast->healthy);

  auto chosen = policy.Select(now);
  ASSERT_TRUE(chosen);
  EXPECT_EQ(*chosen, 1U);

  policy.RecordSuccess(1, std::chrono::milliseconds(10));
  EXPECT_EQ(policy.Health(1)->ewma_rtt_us, 3000U);
  EXPECT_EQ(policy.Health(2)->ewma_rtt_us, 0U);
}

TEST(FailoverPolicy, CircuitSkipsUntilCooldownThenSuccessClosesIt) {
  const auto now = Clock::now();
  auto policy = Names({"a", "b"});
  policy.WithCircuitBreaker(2, std::chrono::seconds(60));
  policy.RecordFailure(0, now);
  const auto *once = policy.Health(0);
  ASSERT_NE(once, nullptr);
  EXPECT_FALSE(once->circuit_opened_at.has_value());
  EXPECT_EQ(once->consecutive_failures, 1U);

  policy.RecordFailure(0, now);
  const auto *open = policy.Health(0);
  ASSERT_NE(open, nullptr);
  EXPECT_FALSE(open->healthy);
  ASSERT_TRUE(open->circuit_opened_at.has_value());
  EXPECT_EQ(*open->circuit_opened_at, now);
  auto other = policy.Select(now);
  ASSERT_TRUE(other);
  EXPECT_EQ(*other, 1U);

  const auto later = now + std::chrono::seconds(61);
  auto half_open = policy.Select(later);
  ASSERT_TRUE(half_open);
  EXPECT_EQ(*half_open, 0U);
  EXPECT_TRUE(policy.Health(0)->circuit_opened_at.has_value());

  const auto refreshed = now + std::chrono::seconds(5);
  policy.RecordFailure(0, refreshed);
  ASSERT_TRUE(policy.Health(0)->circuit_opened_at.has_value());
  EXPECT_EQ(*policy.Health(0)->circuit_opened_at, refreshed);
  EXPECT_EQ(policy.Health(0)->consecutive_failures, 3U);

  policy.RecordSuccess(0, std::chrono::milliseconds(3));
  const auto *closed = policy.Health(0);
  ASSERT_NE(closed, nullptr);
  EXPECT_TRUE(closed->healthy);
  EXPECT_EQ(closed->consecutive_failures, 0U);
  EXPECT_FALSE(closed->circuit_opened_at.has_value());
  EXPECT_EQ(closed->ewma_rtt_us, 3000U);
}

TEST(FailoverPolicy, DisabledBreakerAndBadIndexesDoNotOpen) {
  const auto now = Clock::now();
  auto policy = Names({"only"});
  policy.WithCircuitBreaker(0, std::chrono::seconds(30));
  policy.RecordFailure(0, now);
  policy.RecordFailure(4, now);
  policy.RecordSuccess(4, std::chrono::milliseconds(1));
  const auto *health = policy.Health(0);
  ASSERT_NE(health, nullptr);
  EXPECT_FALSE(health->circuit_opened_at.has_value());
  EXPECT_EQ(health->ewma_rtt_us, 0U);
  auto selected = policy.Select(now);
  ASSERT_TRUE(selected);
  EXPECT_EQ(*selected, 0U);
  const auto known = policy.Endpoint(0);
  ASSERT_TRUE(known.has_value());
  EXPECT_EQ(*known, "only");
  EXPECT_FALSE(policy.Endpoint(3).has_value());
  EXPECT_EQ(policy.Health(4), nullptr);
}

TEST(FailoverPolicy, EmptyPolicyAndZeroAttemptsFail) {
  const auto now = Clock::now();
  auto empty = Names({});
  EXPECT_TRUE(empty.Empty());
  auto missing = empty.Select(now);
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error().Kind(), ErrorKind::kInvalidConfig);

  auto blocked = Names({"a"});
  blocked.WithMaxAttempts(0);
  const auto *before = blocked.Health(0);
  ASSERT_NE(before, nullptr);
  EXPECT_TRUE(before->healthy);
  EXPECT_EQ(before->consecutive_failures, 0U);
  auto selected = blocked.Select(now);
  ASSERT_FALSE(selected);
  EXPECT_EQ(selected.error().Kind(), ErrorKind::kAllEndpointsFailed);
  EXPECT_EQ(selected.error().Attempts(), 0U);
  EXPECT_TRUE(blocked.Health(0)->healthy);
  EXPECT_FALSE(blocked.Health(0)->circuit_opened_at.has_value());
}

TEST(FailoverPolicy, OneAttemptSkipsAnOpenCircuit) {
  const auto now = Clock::now();
  auto policy = Names({"down", "up"});
  policy.WithCircuitBreaker(1, std::chrono::seconds(120)).WithMaxAttempts(1);
  policy.RecordFailure(0, now);
  auto selected = policy.Select(now);
  ASSERT_TRUE(selected);
  EXPECT_EQ(*selected, 1U);
  const std::array<std::size_t, 1> excluded{1};
  auto exhausted = policy.SelectExcluding(now, excluded);
  ASSERT_FALSE(exhausted);
  EXPECT_EQ(exhausted.error().Kind(), ErrorKind::kAllEndpointsFailed);
}

TEST(FailoverPolicy, WriteReplayRequiresAnExplicitHintAfterBytesAreSent) {
  const auto hinted = Error::ServerStatus(1000, "busy", true);
  const auto refused = Error::ServerStatus(1000, "busy", false);
  const auto transport = Error::Transport("reset");
  EXPECT_EQ(hinted.RetryHint(), std::optional<bool>{true});
  EXPECT_EQ(refused.RetryHint(), std::optional<bool>{false});
  EXPECT_EQ(transport.RetryHint(), std::nullopt);
  EXPECT_TRUE(hinted.IsRetryable());
  EXPECT_FALSE(refused.IsRetryable());
  EXPECT_FALSE(transport.IsRetryable());

  EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(false, transport));
  EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(false, refused));
  EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(false, hinted));
  EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(true, hinted));
  EXPECT_FALSE(FailoverPolicy::ShouldFailoverWrite(true, refused));
  EXPECT_FALSE(FailoverPolicy::ShouldFailoverWrite(true, transport));
}

TEST(WorkerClient, DefaultsStripeFormulaAndPreviewFollowTheSlot) {
  const WorkerConfig defaults;
  EXPECT_EQ(defaults.slots_per_thread, 4U);
  EXPECT_EQ(defaults.balance, WorkerBalance::kRoundRobin);
  EXPECT_TRUE(defaults.stripe_endpoints);
  WorkerConfig zero = defaults;
  zero.slots_per_thread = 0;
  EXPECT_EQ(zero.SlotCount(), 1U);

  EXPECT_EQ(greptimedb::StripeShift(3, 0, 1), 1U);
  EXPECT_EQ(greptimedb::StripeShift(3, 1, 1), 2U);
  const std::vector<std::string> three{"a", "b", "c"};
  EXPECT_EQ(greptimedb::StripedEndpoints(three, 0, 1, true),
            (std::vector<std::string>{"b", "c", "a"}));
  const std::vector<std::string> two{"a", "b"};
  EXPECT_EQ(greptimedb::StripedEndpoints(two, 5, 1, false), two);

  std::atomic<int> stamps{0};
  auto factory = [&](const auto &endpoints, std::uint64_t worker_id,
                     std::size_t slot) -> Result<FakeClient> {
    return MakeClient(endpoints, worker_id, slot, stamps.fetch_add(1) + 1);
  };
  auto opened = WorkerClient<FakeClient>::Create(three, defaults, factory);
  ASSERT_TRUE(opened);
  const auto preview = opened->PreviewEndpoints(0, 1);
  ASSERT_GE(preview.size(), 2U);
  EXPECT_EQ(preview[0], "b");
  EXPECT_EQ(preview[1], "c");
}

TEST(WorkerClient, ZeroSlotsStillLeaseOnceAndReleaseOnDestruction) {
  WorkerConfig config;
  config.slots_per_thread = 0;
  config.stripe_endpoints = false;
  std::atomic<int> stamps{0};
  auto factory = [&](const auto &endpoints, std::uint64_t worker_id,
                     std::size_t slot) -> Result<FakeClient> {
    return MakeClient(endpoints, worker_id, slot, stamps.fetch_add(1) + 1);
  };
  auto opened = WorkerClient<FakeClient>::Create(std::vector<std::string>{"a"},
                                                 config, factory);
  ASSERT_TRUE(opened);
  int stamp = 0;
  {
    auto first = opened->Lease();
    ASSERT_TRUE(first);
    EXPECT_EQ(ClientOf(*first).slot, 0U);
    auto blocked = opened->Lease();
    EXPECT_FALSE(blocked);
    if (!blocked) {
      EXPECT_EQ(blocked.error().Kind(), ErrorKind::kInvalidConfig);
    }
    stamp = ClientOf(*first).stamp;
  }
  auto again = opened->Lease();
  ASSERT_TRUE(again);
  EXPECT_EQ(ClientOf(*again).stamp, stamp);
  EXPECT_EQ(stamps.load(), 1);
}

TEST(WorkerClient, RoundRobinReleasesSlotsAndWraps) {
  WorkerConfig config;
  config.slots_per_thread = 3;
  config.balance = WorkerBalance::kRoundRobin;
  config.stripe_endpoints = false;
  std::atomic<int> stamps{0};
  auto factory = [&](const auto &endpoints, std::uint64_t worker_id,
                     std::size_t slot) -> Result<FakeClient> {
    return MakeClient(endpoints, worker_id, slot, stamps.fetch_add(1) + 1);
  };
  auto opened = WorkerClient<FakeClient>::Create(
      std::vector<std::string>{"a", "b", "c"}, config, factory);
  ASSERT_TRUE(opened);
  std::array<std::size_t, 4> order{};
  for (std::size_t index = 0; index < order.size(); ++index) {
    auto leased = opened->Lease();
    ASSERT_TRUE(leased);
    order[index] = ClientOf(*leased).slot;
  }
  EXPECT_EQ(order, (std::array<std::size_t, 4>{0, 1, 2, 0}));

  auto held_a = opened->Lease();
  ASSERT_TRUE(held_a);
  EXPECT_EQ(ClientOf(*held_a).slot, 1U);
  std::size_t released = 0;
  {
    auto held_b = opened->Lease();
    auto held_c = opened->Lease();
    ASSERT_TRUE(held_b);
    ASSERT_TRUE(held_c);
    EXPECT_EQ(ClientOf(*held_b).slot, 2U);
    EXPECT_EQ(ClientOf(*held_c).slot, 0U);
    auto overflow = opened->Lease();
    EXPECT_FALSE(overflow);
    if (!overflow) {
      EXPECT_EQ(overflow.error().Kind(), ErrorKind::kInvalidConfig);
    }
    released = ClientOf(*held_b).slot;
  }
  auto reused = opened->Lease();
  ASSERT_TRUE(reused);
  EXPECT_EQ(ClientOf(*reused).slot, released);
  EXPECT_NE(ClientOf(*reused).slot, ClientOf(*held_a).slot);
}

TEST(WorkerClient, LeastInFlightSkipsLeasedSlotsAndRotatesTies) {
  WorkerConfig config;
  config.slots_per_thread = 3;
  config.balance = WorkerBalance::kLeastInFlight;
  config.stripe_endpoints = false;
  auto factory = [](const auto &endpoints, std::uint64_t worker_id,
                    std::size_t slot) -> Result<FakeClient> {
    return MakeClient(endpoints, worker_id, slot, 0);
  };
  auto opened = WorkerClient<FakeClient>::Create(
      std::vector<std::string>{"a", "b", "c"}, config, factory);
  ASSERT_TRUE(opened);
  std::array<std::size_t, 3> sequential{};
  for (std::size_t index = 0; index < sequential.size(); ++index) {
    auto leased = opened->Lease();
    ASSERT_TRUE(leased);
    sequential[index] = ClientOf(*leased).slot;
  }
  EXPECT_EQ(sequential, (std::array<std::size_t, 3>{0, 1, 2}));

  auto slot_zero = opened->Lease();
  auto slot_one = opened->Lease();
  ASSERT_TRUE(slot_zero);
  ASSERT_TRUE(slot_one);
  std::size_t freed = 0;
  {
    auto slot_two = opened->Lease();
    ASSERT_TRUE(slot_two);
    std::array<std::size_t, 3> held{ClientOf(*slot_zero).slot,
                                    ClientOf(*slot_one).slot,
                                    ClientOf(*slot_two).slot};
    std::ranges::sort(held);
    EXPECT_EQ(held, (std::array<std::size_t, 3>{0, 1, 2}));
    EXPECT_FALSE(opened->Lease());
    freed = ClientOf(*slot_two).slot;
  }
  auto next = opened->Lease();
  ASSERT_TRUE(next);
  EXPECT_EQ(ClientOf(*next).slot, freed);
  EXPECT_NE(ClientOf(*next).slot, ClientOf(*slot_zero).slot);
  EXPECT_NE(ClientOf(*next).slot, ClientOf(*slot_one).slot);
}

TEST(WorkerClient, KeyHashReusesOneSlotAndRejectsASecondLease) {
  WorkerConfig config;
  config.slots_per_thread = 4;
  config.balance = WorkerBalance::kKeyHash;
  config.stripe_endpoints = false;
  auto factory = [](const auto &endpoints, std::uint64_t worker_id,
                    std::size_t slot) -> Result<FakeClient> {
    return MakeClient(endpoints, worker_id, slot, 0);
  };
  auto opened = WorkerClient<FakeClient>::Create(
      std::vector<std::string>{"a", "b", "c", "d"}, config, factory);
  ASSERT_TRUE(opened);

  std::array<bool, 4> seen{};
  for (std::uint64_t key = 0; key < 8; ++key) {
    std::size_t slot = 0;
    {
      auto leased = opened->LeaseByKey(key);
      ASSERT_TRUE(leased);
      slot = ClientOf(*leased).slot;
      ASSERT_LT(slot, seen.size());
      seen[slot] = true;
    }
    auto again = opened->LeaseByKey(key);
    ASSERT_TRUE(again);
    EXPECT_EQ(ClientOf(*again).slot, slot);
  }
  EXPECT_GT(std::ranges::count(seen, true), 1);

  std::size_t slot = 0;
  {
    auto held = opened->LeaseByKey(42);
    ASSERT_TRUE(held);
    slot = ClientOf(*held).slot;
    auto blocked = opened->LeaseByKey(42);
    EXPECT_FALSE(blocked);
    if (!blocked) {
      EXPECT_EQ(blocked.error().Kind(), ErrorKind::kInvalidConfig);
    }
  }
  auto restored = opened->LeaseByKey(42);
  ASSERT_TRUE(restored);
  EXPECT_EQ(ClientOf(*restored).slot, slot);

  std::array<std::size_t, 2> unkeyed{};
  for (auto &slot_index : unkeyed) {
    auto leased = opened->Lease();
    ASSERT_TRUE(leased);
    slot_index = ClientOf(*leased).slot;
  }
  EXPECT_NE(unkeyed[0], unkeyed[1]);
}

TEST(SharedPools, RejectFailoverObserversAndSurfaceFactoryFailures) {
  std::vector<std::string> endpoints{"down"};
  auto local_watch = Names({"down"});
  local_watch.OnFailover([](const FailoverEvent &) noexcept {});
  EXPECT_TRUE(local_watch.HasObserver());

  std::atomic<int> local_builds{0};
  auto local_factory = [&](const auto &assigned) -> Result<FakeClient> {
    local_builds.fetch_add(1);
    return std::unexpected(Error::Transport(std::string{"closed "} +
                                            std::to_string(assigned.size())));
  };
  auto rejected_local =
      ThreadLocalClient<FakeClient, decltype(local_factory)>::Create(
          endpoints, local_factory, std::move(local_watch));
  EXPECT_FALSE(rejected_local);
  if (!rejected_local) {
    EXPECT_EQ(rejected_local.error().Kind(), ErrorKind::kInvalidConfig);
  }

  auto local = ThreadLocalClient<FakeClient, decltype(local_factory)>::Create(
      endpoints, local_factory);
  ASSERT_TRUE(local);
  auto first = local->WithClient([](FakeClient &) -> Result<int> { return 1; });
  auto second =
      local->WithClient([](FakeClient &) -> Result<int> { return 1; });
  EXPECT_FALSE(first);
  EXPECT_FALSE(second);
  if (!first) {
    EXPECT_EQ(first.error().Kind(), ErrorKind::kTransport);
  }
  if (!second) {
    EXPECT_EQ(second.error().Kind(), ErrorKind::kTransport);
  }
  EXPECT_EQ(local_builds.load(), 2);

  WorkerConfig config;
  std::atomic<int> worker_builds{0};
  auto worker_factory = [&](const auto &, std::uint64_t,
                            std::size_t) -> Result<FakeClient> {
    worker_builds.fetch_add(1);
    return std::unexpected(Error::Transport("closed"));
  };
  auto worker_watch = Names({"down"});
  worker_watch.OnFailover([](const FailoverEvent &) noexcept {});
  auto rejected_worker = WorkerClient<FakeClient>::Create(
      endpoints, config, worker_factory, std::move(worker_watch));
  EXPECT_FALSE(rejected_worker);
  if (!rejected_worker) {
    EXPECT_EQ(rejected_worker.error().Kind(), ErrorKind::kInvalidConfig);
  }

  auto worker =
      WorkerClient<FakeClient>::Create(endpoints, config, worker_factory);
  ASSERT_TRUE(worker);
  EXPECT_FALSE(worker->Lease());
  EXPECT_FALSE(worker->LeaseByKey(42));
  EXPECT_EQ(worker->Lease().error().Kind(), ErrorKind::kTransport);
  EXPECT_EQ(worker->LeaseByKey(7).error().Kind(), ErrorKind::kTransport);

  std::atomic<bool> other_failed{false};
  std::thread([&] {
    auto leased = worker->Lease();
    other_failed.store(!leased &&
                       leased.error().Kind() == ErrorKind::kTransport);
  }).join();
  EXPECT_TRUE(other_failed.load());
  EXPECT_GE(worker_builds.load(), 5);
}

TEST(ThreadLocalClient,
     EightThreadsKeepDistinctClientsOnTheOriginalEndpointOrder) {
  std::atomic<int> builds{0};
  const std::vector<std::string> endpoints{"a", "b", "c"};
  auto factory = [&](const auto &assigned) -> Result<FakeClient> {
    const int stamp = builds.fetch_add(1) + 1;
    return MakeClient(assigned, 0, 0, stamp);
  };
  auto opened = ThreadLocalClient<FakeClient, decltype(factory)>::Create(
      endpoints, factory);
  ASSERT_TRUE(opened);
  std::array<std::thread::id, 8> owners{};
  std::array<std::string, 8> first{};
  std::vector<std::thread> threads;
  threads.reserve(owners.size());
  for (std::size_t index = 0; index < owners.size(); ++index) {
    threads.emplace_back([&, index] {
      int stamps[2] = {};
      for (int attempt = 0; attempt < 2; ++attempt) {
        auto used = opened->WithClient([&](FakeClient &client) -> Result<int> {
          EXPECT_EQ(client.owner, std::this_thread::get_id());
          EXPECT_EQ(client.endpoints, endpoints);
          return client.stamp;
        });
        EXPECT_TRUE(used);
        if (used)
          stamps[attempt] = *used;
      }
      EXPECT_EQ(stamps[0], stamps[1]);
      owners[index] = std::this_thread::get_id();
      first[index] = endpoints.front();
    });
  }
  for (auto &thread : threads)
    thread.join();
  EXPECT_EQ(builds.load(), 8);
  std::ranges::sort(owners);
  EXPECT_EQ(std::ranges::unique(owners).begin(), owners.end());
  for (const auto &name : first)
    EXPECT_EQ(name, "a");
}

TEST(ThreadLocalClient, CachedClientStaysBoundToTheThreadThatCreatedIt) {
  auto factory = [](const auto &endpoints) -> Result<FakeClient> {
    return MakeClient(endpoints, 0, 0, 1);
  };
  auto opened = ThreadLocalClient<FakeClient, decltype(factory)>::Create(
      std::vector<std::string>{"a"}, factory);
  ASSERT_TRUE(opened);
  FakeClient *escaped = nullptr;
  std::thread::id owner;
  auto used = opened->WithClient([&](FakeClient &client) -> Result<int> {
    escaped = &client;
    owner = client.owner;
    EXPECT_EQ(client.owner, std::this_thread::get_id());
    return client.stamp;
  });
  ASSERT_TRUE(used);
  ASSERT_NE(escaped, nullptr);
  std::atomic<bool> other_thread{false};
  std::thread([&] {
    auto local = opened->WithClient([&](FakeClient &client) -> Result<int> {
      EXPECT_EQ(client.owner, std::this_thread::get_id());
      EXPECT_NE(client.owner, owner);
      EXPECT_NE(&client, escaped);
      return 1;
    });
    other_thread.store(local.has_value());
  }).join();
  EXPECT_TRUE(other_thread.load());
  EXPECT_EQ(escaped->owner, owner);
  EXPECT_NE(escaped->owner, std::thread::id{});
}

TEST(WorkerClient, EightThreadsStripeLeasesAcrossEndpoints) {
  const std::vector<std::string> endpoints{"a", "b", "c"};
  WorkerConfig config;
  config.slots_per_thread = 1;
  config.balance = WorkerBalance::kRoundRobin;
  config.stripe_endpoints = true;
  std::atomic<int> builds{0};
  auto factory = [&](const auto &assigned, std::uint64_t worker_id,
                     std::size_t slot) -> Result<FakeClient> {
    return MakeClient(assigned, worker_id, slot, builds.fetch_add(1) + 1);
  };
  auto opened = WorkerClient<FakeClient>::Create(endpoints, config, factory);
  ASSERT_TRUE(opened);
  std::array<char, 8> chosen{};
  std::vector<std::thread> threads;
  threads.reserve(chosen.size());
  for (std::size_t index = 0; index < chosen.size(); ++index) {
    threads.emplace_back([&, index] {
      auto leased = opened->Lease();
      EXPECT_TRUE(leased);
      if (!leased || ClientOf(*leased).endpoints.empty())
        return;
      EXPECT_EQ(ClientOf(*leased).owner, std::this_thread::get_id());
      EXPECT_EQ(ClientOf(*leased).slot, 0U);
      chosen[index] = ClientOf(*leased).endpoints.front().front();
    });
  }
  for (auto &thread : threads)
    thread.join();
  std::array<int, 3> counts{};
  for (const char name : chosen) {
    ASSERT_GE(name, 'a');
    ASSERT_LE(name, 'c');
    ++counts[static_cast<std::size_t>(name - 'a')];
  }
  std::ranges::sort(counts);
  EXPECT_EQ(counts, (std::array<int, 3>{2, 3, 3}));
  EXPECT_EQ(builds.load(), 8);
}

TEST(WorkerClient, FailedFactoryReleasesTheSlotForAnotherAttempt) {
  std::atomic<int> builds{0};
  auto factory = [&](const auto &endpoints, std::uint64_t worker_id,
                     std::size_t slot) -> Result<FakeClient> {
    const int attempt = builds.fetch_add(1);
    if (attempt == 0)
      return std::unexpected(Error::Transport("closed"));
    return MakeClient(endpoints, worker_id, slot, attempt + 1);
  };
  WorkerConfig config;
  config.slots_per_thread = 1;
  auto opened = WorkerClient<FakeClient>::Create(std::vector<std::string>{"a"},
                                                 config, factory);
  ASSERT_TRUE(opened);
  auto failed = opened->Lease();
  EXPECT_FALSE(failed);
  if (!failed) {
    EXPECT_EQ(failed.error().Kind(), ErrorKind::kTransport);
  }
  auto leased = opened->Lease();
  ASSERT_TRUE(leased);
  EXPECT_EQ(ClientOf(*leased).stamp, 2);
  EXPECT_EQ(builds.load(), 2);
}

} // namespace
