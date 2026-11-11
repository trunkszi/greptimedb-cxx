#include <greptimedb/threading/thread_cache.h>

#include "threading_test_support.h"

#include <cares/types/host_types.hpp>
#include <nexora/pool/endpoint_failover.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace greptimedb;
using namespace greptimedb::threading_test;

TEST(WorkerPoolTest, StripeFormulaAndConfigAccessors) {
  const WorkerConfig defaults;
  EXPECT_EQ(defaults.slots_per_thread, 4uz);
  EXPECT_EQ(defaults.balance, WorkerBalance::kRoundRobin);
  EXPECT_TRUE(defaults.stripe_endpoints);

  WorkerConfig zero = defaults;
  zero.slots_per_thread = 0;
  EXPECT_EQ(zero.SlotCount(), 1uz);

  const auto max_val = std::numeric_limits<std::size_t>::max();
  EXPECT_EQ(StripeShift(3, max_val, 1), (max_val % 3 + 1) % 3);
  EXPECT_EQ(StripeShift(max_val, max_val - 1, max_val - 1), max_val - 2);
  EXPECT_EQ(StripeShift(max_val, max_val, max_val), 0uz);
  EXPECT_EQ(StripeShift(0, max_val, max_val), 0uz);

  const std::vector<std::string> three{"ep0", "ep1", "ep2"};
  EXPECT_EQ(StripedEndpoints(three, 0, 1, true), (std::vector<std::string>{"ep1", "ep2", "ep0"}));
  EXPECT_EQ(StripedEndpoints(three, 1, 1, false), three);

  auto factory = [](const std::vector<std::string> &eps, std::uint64_t wid, std::size_t slot) -> Result<FakeSession> {
    return MakeSession(eps, wid, slot, 1);
  };
  auto pool = WorkerClient<FakeSession>::Create(three, defaults, factory);
  ASSERT_TRUE(pool);
  EXPECT_EQ(pool->Config().slots_per_thread, 4uz);
  EXPECT_EQ(pool->Endpoints(), three);
  EXPECT_EQ(pool->PreviewEndpoints(1, 1), (std::vector<std::string>{"ep2", "ep0", "ep1"}));
}

TEST(WorkerPoolTest, ZeroSlotsAndInvalidSettings) {
  auto factory = [](const std::vector<std::string> &eps, std::uint64_t wid, std::size_t slot) -> Result<FakeSession> {
    return MakeSession(eps, wid, slot, 7);
  };
  WorkerConfig bad_cfg;
  bad_cfg.balance = static_cast<WorkerBalance>(255);
  EXPECT_FALSE(WorkerClient<FakeSession>::Create({"a"}, bad_cfg, factory));

  bad_cfg.balance = WorkerBalance::kRoundRobin;
  bad_cfg.slots_per_thread = std::numeric_limits<std::size_t>::max();
  EXPECT_FALSE(WorkerClient<FakeSession>::Create({"a"}, bad_cfg, factory));

  WorkerConfig zero_cfg{.slots_per_thread = 0, .balance = WorkerBalance::kRoundRobin, .stripe_endpoints = false};
  auto pool = WorkerClient<FakeSession>::Create({"a"}, zero_cfg, factory);
  ASSERT_TRUE(pool);
  {
    auto first = pool->Lease();
    ASSERT_TRUE(first);
    EXPECT_EQ(first->Slot(), 0uz);
    EXPECT_EQ(first->Client().stamp, 7);
    auto exhausted = pool->Lease();
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error().Kind(), ErrorKind::kInvalidConfig);
  }
  EXPECT_TRUE(pool->Lease());
}

TEST(WorkerPoolTest, RoundRobinLeastInFlightAndKeyHash) {
  std::atomic<int> stamps{0};
  auto factory = [&](const std::vector<std::string> &eps, std::uint64_t wid, std::size_t slot) -> Result<FakeSession> {
    return MakeSession(eps, wid, slot, stamps.fetch_add(1, std::memory_order_relaxed) + 1);
  };

  for (const auto balance : {WorkerBalance::kRoundRobin, WorkerBalance::kLeastInFlight}) {
    WorkerConfig cfg{.slots_per_thread = 3, .balance = balance, .stripe_endpoints = false};
    auto pool = WorkerClient<FakeSession>::Create({"a", "b", "c"}, cfg, factory);
    ASSERT_TRUE(pool);
    std::array<std::size_t, 4> visited{};
    for (std::size_t idx = 0; idx < visited.size(); ++idx) {
      auto guard = pool->Lease();
      ASSERT_TRUE(guard);
      visited[idx] = guard->Slot();
    }
    EXPECT_EQ(visited, (std::array<std::size_t, 4>{0, 1, 2, 0}));

    auto a = pool->Lease();
    auto b = pool->Lease();
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    std::size_t freed_slot = 0;
    {
      auto c = pool->Lease();
      ASSERT_TRUE(c);
      EXPECT_FALSE(pool->Lease());
      freed_slot = c->Slot();
    }
    auto reused = pool->Lease();
    ASSERT_TRUE(reused);
    EXPECT_EQ(reused->Slot(), freed_slot);
  }

  WorkerConfig key_cfg{.slots_per_thread = 8, .balance = WorkerBalance::kKeyHash, .stripe_endpoints = false};
  auto keyed_pool = WorkerClient<FakeSession>::Create({"a"}, key_cfg, factory);
  ASSERT_TRUE(keyed_pool);
  std::array<unsigned, 8> histogram{};
  for (std::uint64_t key = 0; key < 2048; ++key) {
    auto guard = keyed_pool->LeaseByKey(key << 32);
    ASSERT_TRUE(guard);
    EXPECT_EQ(guard->Slot(), detail::Fnv1a(key << 32) % 8uz);
    ++histogram[guard->Slot()];
  }
  for (const unsigned count : histogram) {
    EXPECT_GT(count, 180U);
  }

  auto held = keyed_pool->LeaseByKey(99ULL);
  ASSERT_TRUE(held);
  const std::size_t target_slot = held->Slot();
  auto duplicate = keyed_pool->LeaseByKey(99ULL);
  ASSERT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error().Kind(), ErrorKind::kInvalidConfig);
  held = std::unexpected(Error::InvalidConfig("release"));
  auto again = keyed_pool->LeaseByKey(99ULL);
  ASSERT_TRUE(again);
  EXPECT_EQ(again->Slot(), target_slot);
}

TEST(ThreadingPoolsTest, EightThreadsIsolateAndStripeEndpoints) {
  std::atomic<int> local_builds{0};
  const std::vector<std::string> endpoints{"n0", "n1", "n2"};
  auto local_factory = [&](const std::vector<std::string> &eps) -> Result<FakeSession> {
    return MakeSession(eps, 0, 0, local_builds.fetch_add(1, std::memory_order_relaxed) + 1);
  };
  auto local_pool = ThreadLocalClient<FakeSession, decltype(local_factory)>::Create(endpoints, local_factory);
  ASSERT_TRUE(local_pool);
  EXPECT_EQ(local_pool->Endpoints(), endpoints);

  WorkerConfig worker_cfg{.slots_per_thread = 1, .balance = WorkerBalance::kRoundRobin, .stripe_endpoints = true};
  std::atomic<int> worker_builds{0};
  auto worker_pool = WorkerClient<FakeSession>::Create(
      endpoints, worker_cfg,
      [&](const std::vector<std::string> &eps, std::uint64_t wid, std::size_t slot) -> Result<FakeSession> {
        return MakeSession(eps, wid, slot, worker_builds.fetch_add(1, std::memory_order_relaxed) + 1);
      });
  ASSERT_TRUE(worker_pool);

  std::array<char, 8> dialed_heads{};
  std::array<std::thread, 8> threads;
  for (std::size_t idx = 0; idx < threads.size(); ++idx) {
    threads[idx] = std::thread([&, idx] {
      auto s1 = local_pool->WithClient([&](FakeSession &s) -> int {
        EXPECT_EQ(s.owner, std::this_thread::get_id());
        EXPECT_EQ(s.endpoints, endpoints);
        return s.stamp;
      });
      auto s2 = local_pool->WithClient([](FakeSession &s) -> int { return s.stamp; });
      ASSERT_TRUE(s1);
      ASSERT_TRUE(s2);
      EXPECT_EQ(*s1, *s2);

      auto lease = worker_pool->Lease();
      ASSERT_TRUE(lease);
      EXPECT_EQ(lease->Client().owner, std::this_thread::get_id());
      dialed_heads[idx] = lease->Client().endpoints.front().back();
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  EXPECT_EQ(local_builds.load(), 8);
  EXPECT_EQ(worker_builds.load(), 8);
  std::array<int, 3> stripe_counts{};
  for (const char ch : dialed_heads) {
    ASSERT_GE(ch, '0');
    ASSERT_LE(ch, '2');
    ++stripe_counts[static_cast<std::size_t>(ch - '0')];
  }
  std::ranges::sort(stripe_counts);
  EXPECT_EQ(stripe_counts, (std::array<int, 3>{2, 3, 3}));
}

TEST(ThreadingPoolsTest, AbandonedPoolsPrunedWithoutSharedPtr) {
  std::atomic<int> drops{0};
  auto local_factory = [&](const std::vector<std::string> &) -> Result<std::unique_ptr<BoundClient>> {
    return std::make_unique<BoundClient>(&drops);
  };
  using LocalPool = ThreadLocalClient<std::unique_ptr<BoundClient>, decltype(local_factory)>;
  using LeasedPool = WorkerClient<std::unique_ptr<BoundClient>>;

  auto local_made = LocalPool::Create({"a"}, local_factory);
  auto worker_made = LeasedPool::Create(
      {"a"}, WorkerConfig{.slots_per_thread = 1},
      [&](const std::vector<std::string> &, std::uint64_t, std::size_t) -> Result<std::unique_ptr<BoundClient>> {
        return std::make_unique<BoundClient>(&drops);
      });
  ASSERT_TRUE(local_made);
  ASSERT_TRUE(worker_made);

  auto owned_local = std::make_unique<LocalPool>(std::move(*local_made));
  auto owned_worker = std::make_unique<LeasedPool>(std::move(*worker_made));
  auto *raw_local = owned_local.get();
  auto *raw_worker = owned_worker.get();

  std::atomic<bool> populated{false};
  std::atomic<bool> abandoned{false};
  std::atomic<std::size_t> remaining_on_close{99};

  std::thread caller([&] {
    EXPECT_TRUE(raw_local->WithClient([](auto &c) { EXPECT_EQ(c->owner, std::this_thread::get_id()); }));
    EXPECT_TRUE(raw_worker->WithClient([](auto &c) { EXPECT_EQ(c->owner, std::this_thread::get_id()); }));
    populated.store(true, std::memory_order_release);
    populated.notify_one();
    abandoned.wait(false, std::memory_order_acquire);

    auto fresh_local = LocalPool::Create({"b"}, local_factory);
    auto fresh_worker = LeasedPool::Create(
        {"b"}, WorkerConfig{.slots_per_thread = 1},
        [&](const std::vector<std::string> &, std::uint64_t, std::size_t) -> Result<std::unique_ptr<BoundClient>> {
          return std::make_unique<BoundClient>(&drops);
        });
    ASSERT_TRUE(fresh_local);
    ASSERT_TRUE(fresh_worker);
    EXPECT_TRUE(fresh_local->WithClient([](auto &) {}));
    EXPECT_TRUE(fresh_worker->WithClient([](auto &) {}));
    // Both abandoned pools must already have been pruned on this thread.
    EXPECT_EQ(drops.load(), 2);
    remaining_on_close.store(CloseThreadCaches(), std::memory_order_relaxed);
  });

  populated.wait(false, std::memory_order_acquire);
  owned_local.reset();
  owned_worker.reset();
  EXPECT_EQ(drops.load(), 0);
  abandoned.store(true, std::memory_order_release);
  abandoned.notify_one();
  caller.join();

  EXPECT_EQ(remaining_on_close.load(), 2uz);
  EXPECT_EQ(drops.load(), 4);
}

TEST(ThreadingPoolsTest, LivenessSpansMultipleSegmentedBlocks) {
  auto factory = [](const std::vector<std::string> &) -> Result<int> { return 11; };
  using LocalPool = ThreadLocalClient<int, decltype(factory)>;

  std::optional<LocalPool> first_block_survivor;
  for (std::size_t idx = 0; idx < 4200; ++idx) {
    auto pool = LocalPool::Create({"ep"}, factory);
    ASSERT_TRUE(pool);
    if (idx == 0) {
      first_block_survivor.emplace(std::move(*pool));
      EXPECT_TRUE(first_block_survivor->WithClient([](int &v) { EXPECT_EQ(v, 11); }));
    } else if (idx == 4199) {
      EXPECT_TRUE(pool->WithClient([](int &v) { EXPECT_EQ(v, 11); }));
    }
  }
  // `pool` at idx 4199 went out of scope and called CloseCurrentThread();
  // `first_block_survivor` remains alive in block 0 while block 1 was allocated.
  EXPECT_EQ(CloseThreadCaches(), 1uz);
}

TEST(ThreadingPoolsTest, GenerationInvalidatesStaleCheckoutAndMoveErase) {
  int builds = 0;
  auto local_factory = [&](const std::vector<std::string> &) -> Result<FakeSession> {
    return MakeSession({"a"}, 0, 0, ++builds);
  };
  auto local_pool = ThreadLocalClient<FakeSession, decltype(local_factory)>::Create({"a"}, local_factory);
  ASSERT_TRUE(local_pool);

  auto outer = local_pool->WithClient([&](FakeSession &old) -> Result<int> {
    EXPECT_EQ(old.stamp, 1);
    EXPECT_TRUE(local_pool->CloseCurrentThread());
    auto inner = local_pool->WithClient([](FakeSession &newer) { return newer.stamp; });
    EXPECT_TRUE(inner);
    if (inner) {
      EXPECT_EQ(*inner, 2);
    }
    return old.stamp;
  });
  ASSERT_TRUE(outer);
  EXPECT_EQ(*outer, 1);
  auto post = local_pool->WithClient([](FakeSession &current) { return current.stamp; });
  ASSERT_TRUE(post);
  EXPECT_EQ(*post, 2);

  for (const bool during_return : {false, true}) {
    using WPool = WorkerClient<MoveProbe>;
    auto wpool = WPool::Create({"a"}, WorkerConfig{.slots_per_thread = 1},
                               [](const auto &, auto, auto) -> Result<MoveProbe> { return MoveProbe{}; });
    ASSERT_TRUE(wpool);
    EXPECT_TRUE(wpool->WithClient([](MoveProbe &) {}));
    struct HookCtx {
      WPool *pool{nullptr};
      std::optional<LeaseGuard<MoveProbe>> replacement{};
    } ctx{.pool = &*wpool};
    const auto erase_and_reopen = [](void *ptr) {
      auto &state = *static_cast<HookCtx *>(ptr);
      EXPECT_TRUE(state.pool->CloseCurrentThread());
      auto next = state.pool->Lease();
      ASSERT_TRUE(next);
      state.replacement.emplace(std::move(*next));
    };
    MoveProbe::context = &ctx;
    if (!during_return) {
      MoveProbe::hook = erase_and_reopen;
    }
    auto stale = wpool->Lease();
    ASSERT_TRUE(stale);
    if (during_return) {
      MoveProbe::hook = erase_and_reopen;
    }
    stale = std::unexpected(Error::InvalidConfig("drop stale"));
    ASSERT_TRUE(ctx.replacement.has_value());
    EXPECT_FALSE(wpool->Lease());
    ctx.replacement.reset();
    EXPECT_TRUE(wpool->Lease());
  }
}

TEST(ThreadingPoolsTest, ReentrantDestructorAndTlsTeardownSafety) {
  std::atomic<int> drops{0};
  auto local_factory = [&](const std::vector<std::string> &) -> Result<ReentrantDrop> { return ReentrantDrop{&drops}; };
  auto local_pool = ThreadLocalClient<ReentrantDrop, decltype(local_factory)>::Create({"a"}, local_factory);
  auto worker_pool =
      WorkerClient<ReentrantDrop>::Create({"a"}, WorkerConfig{.slots_per_thread = 2},
                                          [&](const std::vector<std::string> &, std::uint64_t,
                                              std::size_t) -> Result<ReentrantDrop> { return ReentrantDrop{&drops}; });
  ASSERT_TRUE(local_pool);
  ASSERT_TRUE(worker_pool);

  std::thread t([&] {
    using ThreadOwnedPool = WorkerClient<ReentrantDrop>;
    static thread_local std::optional<ThreadOwnedPool> tls_owner;
    auto inner = ThreadOwnedPool::Create({"a"}, WorkerConfig{.slots_per_thread = 1},
                                         [&](const std::vector<std::string> &, std::uint64_t,
                                             std::size_t) -> Result<ReentrantDrop> { return ReentrantDrop{&drops}; });
    ASSERT_TRUE(inner);
    tls_owner.emplace(std::move(*inner));
    EXPECT_TRUE(tls_owner->WithClient([](ReentrantDrop &) {}));

    EXPECT_TRUE(local_pool->WithClient([](ReentrantDrop &) {}));
    EXPECT_TRUE(worker_pool->WithClient([](ReentrantDrop &) {}));
    EXPECT_TRUE(local_pool->CloseCurrentThread());
    // `~ReentrantDrop` invokes `CloseThreadCaches()`, clearing `tls_owner` and `worker_pool` too.
    EXPECT_FALSE(worker_pool->CloseCurrentThread());
    EXPECT_EQ(drops.load(), 3);

    EXPECT_TRUE(local_pool->WithClient([](ReentrantDrop &) {}));
    EXPECT_TRUE(worker_pool->WithClient([](ReentrantDrop &) {}));
  });
  t.join();
  EXPECT_EQ(drops.load(), 5);
}

TEST(ThreadingPoolsTest, NexoraStylePolicySnapshotAndValidation) {
  MockEndpointFailover policy{2, MockEndpointFailoverConfig{.failure_threshold = 5, .random_seed = 99ULL}};
  std::atomic<int> local_calls{0};
  auto local_factory = [&](const std::vector<std::string> &eps,
                           const MockEndpointFailoverConfig *cfg) -> Result<FakeSession> {
    if (cfg == nullptr || cfg->failure_threshold != 5U || cfg->random_seed != 99ULL) {
      return std::unexpected(Error::InvalidConfig("unexpected policy snapshot"));
    }
    return MakeSession(eps, 0, 0, local_calls.fetch_add(1, std::memory_order_relaxed) + 1);
  };
  auto local_pool =
      ThreadLocalClient<FakeSession, decltype(local_factory)>::Create({"ep0", "ep1"}, local_factory, policy);
  ASSERT_TRUE(local_pool);
  EXPECT_TRUE(local_pool->WithClient([](FakeSession &s) { EXPECT_EQ(s.stamp, 1); }));

  auto worker_factory = [](const std::vector<std::string> &eps, std::uint64_t wid, std::size_t slot,
                           const MockEndpointFailoverConfig *cfg) -> Result<FakeSession> {
    if (cfg == nullptr || cfg->failure_threshold != 5U) {
      return std::unexpected(Error::InvalidConfig("missing worker policy snapshot"));
    }
    return MakeSession(eps, wid, slot, 3);
  };
  auto worker_pool =
      WorkerClient<FakeSession>::Create({"ep0", "ep1"}, WorkerConfig{.slots_per_thread = 2}, worker_factory, policy);
  ASSERT_TRUE(worker_pool);
  auto leased = worker_pool->Lease();
  ASSERT_TRUE(leased);
  EXPECT_EQ(leased->Client().stamp, 3);

  // Cardinality mismatch rejected.
  EXPECT_FALSE((ThreadLocalClient<FakeSession, decltype(local_factory)>::Create({"only_one"}, local_factory, policy)));
  EXPECT_FALSE(WorkerClient<FakeSession>::Create({"only_one"}, {}, worker_factory, policy));

  // Observer-bearing policy rejected.
  MockEndpointFailoverConfig observed_cfg{};
  observed_cfg.observer = []() noexcept {};
  MockEndpointFailover observed_policy{2, std::move(observed_cfg)};
  EXPECT_FALSE((
      ThreadLocalClient<FakeSession, decltype(local_factory)>::Create({"ep0", "ep1"}, local_factory, observed_policy)));
  EXPECT_FALSE(WorkerClient<FakeSession>::Create({"ep0", "ep1"}, {}, worker_factory, observed_policy));

  // Policy-unaware factory rejected when policy is supplied.
  auto plain_local = [](const std::vector<std::string> &) -> Result<FakeSession> { return FakeSession{}; };
  auto plain_worker = [](const std::vector<std::string> &, std::uint64_t, std::size_t) -> Result<FakeSession> {
    return FakeSession{};
  };
  EXPECT_FALSE((ThreadLocalClient<FakeSession, decltype(plain_local)>::Create({"ep0", "ep1"}, plain_local, policy)));
  EXPECT_FALSE(WorkerClient<FakeSession>::Create({"ep0", "ep1"}, {}, plain_worker, policy));
}

TEST(ThreadingPoolsTest, BoundedTableReclaimsAndRejects) {
  auto factory = [](const std::vector<std::string> &) -> Result<int> { return 1; };
  using Pool = ThreadLocalClient<int, decltype(factory)>;
  constexpr auto kCap = detail::kMaxThreadPools;

  std::vector<Pool> live;
  live.reserve(kCap);
  for (std::size_t idx = 0; idx < kCap; ++idx) {
    auto pool = Pool::Create({"ep"}, factory);
    ASSERT_TRUE(pool);
    ASSERT_TRUE(pool->WithClient([](int &value) { EXPECT_EQ(value, 1); }));
    live.push_back(std::move(*pool));
  }
  auto extra = Pool::Create({"ep"}, factory);
  ASSERT_TRUE(extra);
  auto denied = extra->WithClient([](int &) {});
  ASSERT_FALSE(denied);
  EXPECT_EQ(denied.error().Kind(), ErrorKind::kInvalidConfig);
  live.pop_back();
  ASSERT_TRUE(extra->WithClient([](int &value) { EXPECT_EQ(value, 1); }));
  live.clear();

  for (std::size_t idx = 0; idx < 4200; ++idx) {
    auto pool = Pool::Create({"ep"}, factory);
    ASSERT_TRUE(pool);
    ASSERT_TRUE(pool->WithClient([](int &) {}));
  }

  auto once = Pool::Create({"ep"}, factory);
  ASSERT_TRUE(once);
  auto outer = once->WithClient([&](int &) -> Result<int> {
    auto inner = once->WithClient([](int &) { return 1; });
    EXPECT_FALSE(inner);
    if (!inner) {
      EXPECT_EQ(inner.error().Kind(), ErrorKind::kInvalidConfig);
    }
    return 7;
  });
  ASSERT_TRUE(outer);
  EXPECT_EQ(*outer, 7);
}

TEST(ThreadingPoolsTest, NexoraEndpointFailoverSnapshot) {
  const std::array<cares::Endpoint, 2> endpoints{
      cares::Endpoint{.ip = cares::IpAddress::FromV4({127, 0, 0, 1}), .port = 4001},
      cares::Endpoint{.ip = cares::IpAddress::FromV4({127, 0, 0, 1}), .port = 4002},
  };
  nexora::EndpointFailoverConfig cfg{
      .failure_threshold = 4,
      .base_cooldown_ns = 2'000'000'000ULL,
      .random_seed = 11ULL,
  };
  nexora::EndpointFailover<> policy{endpoints, cfg};
  ASSERT_EQ(policy.Size(), 2uz);
  ASSERT_FALSE(static_cast<bool>(policy.Config().observer));

  auto local_factory = [](const std::vector<std::string> &names,
                          const nexora::EndpointFailoverConfig *snap) -> Result<int> {
    if (snap == nullptr || snap->failure_threshold != 4U || snap->random_seed != 11ULL ||
        snap->base_cooldown_ns != 2'000'000'000ULL || static_cast<bool>(snap->observer) || names.size() != 2uz)
      return std::unexpected(Error::InvalidConfig("nexora snapshot mismatch"));
    return 1;
  };
  auto local_pool = ThreadLocalClient<int, decltype(local_factory)>::Create({"ep0", "ep1"}, local_factory, policy);
  ASSERT_TRUE(local_pool);
  EXPECT_TRUE(local_pool->WithClient([](int &value) { EXPECT_EQ(value, 1); }));

  auto worker_factory = [](const std::vector<std::string> &names, std::uint64_t, std::size_t,
                           const nexora::EndpointFailoverConfig *snap) -> Result<int> {
    if (snap == nullptr || snap->failure_threshold != 4U || snap->random_seed != 11ULL || names.size() != 2uz)
      return std::unexpected(Error::InvalidConfig("nexora worker snapshot mismatch"));
    return 2;
  };
  auto worker_pool =
      WorkerClient<int>::Create({"ep0", "ep1"}, WorkerConfig{.slots_per_thread = 1}, worker_factory, policy);
  ASSERT_TRUE(worker_pool);
  auto leased = worker_pool->Lease();
  ASSERT_TRUE(leased);
  EXPECT_EQ(leased->Client(), 2);
}

TEST(ThreadingPoolsDeathTest, ForeignThreadLeaseAccessAndMoveFailClosed) {
  auto make_pool = [] {
    return WorkerClient<MoveWatched>::Create({"a"}, WorkerConfig{.slots_per_thread = 1},
                                             [](const std::vector<std::string> &, std::uint64_t,
                                                std::size_t) -> Result<MoveWatched> { return MoveWatched{}; });
  };
  auto pool = make_pool();
  ASSERT_TRUE(pool);
  auto lease = pool->Lease();
  ASSERT_TRUE(lease);

  std::thread inspector([&] {
    auto foreign = lease->TryClient();
    EXPECT_FALSE(foreign);
    if (!foreign) {
      EXPECT_EQ(foreign.error().Kind(), ErrorKind::kStream);
    }
  });
  inspector.join();

  auto moved = std::move(*lease);
  EXPECT_FALSE(lease->TryClient());
  EXPECT_TRUE(moved.TryClient());

  EXPECT_EXIT(
      {
        std::set_terminate([] { std::_Exit(76); });
        auto p = make_pool();
        auto guard = p->Lease();
        guard->Client().watch = true;
        std::thread foreign([&] { [[maybe_unused]] auto stolen = std::move(*guard); });
        foreign.join();
        std::_Exit(79);
      },
      ::testing::ExitedWithCode(76), "");
}

} // namespace
