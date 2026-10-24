/** Unit lifecycle and scenario regressions for client ownership and failover.
 */
#include <greptimedb/threading/client_pool.h>
#include <grpcxx/core/status.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <future>
#include <limits>
#include <thread>

namespace {
using namespace greptimedb;
using namespace std::chrono_literals;

struct MoveWatched {
  std::thread::id owner{std::this_thread::get_id()};
  bool watch{false};
  MoveWatched() = default;
  MoveWatched(const MoveWatched &) = delete;
  MoveWatched &operator=(const MoveWatched &) = delete;
  MoveWatched(MoveWatched &&other) noexcept { *this = std::move(other); }
  MoveWatched &operator=(MoveWatched &&other) noexcept {
    if (other.watch && other.owner != std::this_thread::get_id())
      std::_Exit(77);
    owner = other.owner;
    watch = other.watch;
    return *this;
  }
  ~MoveWatched() {
    if (watch && owner != std::this_thread::get_id())
      std::_Exit(78);
  }
};

using WatchPool = WorkerClient<MoveWatched>;
auto MakeWatchPool(std::size_t count = 1) {
  WorkerConfig config;
  config.slots_per_thread = count;
  config.balance = WorkerBalance::kLeastInFlight;
  return WatchPool::Create({"a"}, config,
                           [](const auto &, auto, auto) -> Result<MoveWatched> {
                             return MoveWatched{};
                           });
}

TEST(WorkerProductionDeathTest, ForeignMoveChecksBeforeTouchingClient) {
  EXPECT_EXIT(
      {
        std::set_terminate([] { std::_Exit(76); });
        auto pool = MakeWatchPool();
        auto lease = pool->Lease();
        lease->client().watch = true;
        std::thread worker([&] { auto foreign = std::move(*lease); });
        worker.join();
        std::_Exit(79);
      },
      ::testing::ExitedWithCode(76), "");
}

TEST(WorkerProductionDeathTest, ForeignAssignmentChecksBeforeReleasingTarget) {
  EXPECT_EXIT(
      {
        std::set_terminate([] { std::_Exit(76); });
        auto pool = MakeWatchPool();
        auto source = pool->Lease();
        source->client().watch = true;
        std::thread worker([&] {
          auto destination = pool->Lease();
          *destination = std::move(*source);
        });
        worker.join();
        std::_Exit(79);
      },
      ::testing::ExitedWithCode(76), "");
}

TEST(WorkerProductionDeathTest, MovedFromGuardHasNoForeignDestructorPayload) {
  EXPECT_EXIT(
      {
        auto pool = MakeWatchPool();
        auto lease = pool->Lease();
        lease->client().watch = true;
        auto destination = std::move(*lease);
        std::thread worker(
            [&] { lease = std::unexpected(Error::InvalidConfig("released")); });
        worker.join();
        std::_Exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST(WorkerProductionDeathTest, ExitedOwnerCannotAliasAReplacementOsThread) {
  EXPECT_EXIT(
      {
        auto pool = MakeWatchPool();
        std::optional<LeaseGuard<MoveWatched>> orphan;
        std::thread owner([&] {
          auto lease = pool->Lease();
          orphan.emplace(std::move(*lease));
        });
        owner.join();
        bool rejected = true;
        for (int pass = 0; pass < 32; ++pass) {
          std::thread replacement(
              [&] { rejected = rejected && !orphan->TryClient().has_value(); });
          replacement.join();
        }
        // The owner has exited; no foreign thread may destroy its live client.
        std::_Exit(rejected ? 0 : 79);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST(WorkerProduction,
     MoveAssignmentReturnsExactlyOneSlotAndSelfMoveKeepsLease) {
  auto pool = MakeWatchPool(2);
  ASSERT_TRUE(pool);
  auto first = pool->Lease();
  auto second = pool->Lease();
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_FALSE(pool->Lease());
  const auto freed = first->slot();
  *first = std::move(*second);
  EXPECT_FALSE(second->TryClient());
  auto &self = *first;
  *first = std::move(self);
  EXPECT_TRUE(first->TryClient());
  auto third = pool->Lease();
  ASSERT_TRUE(third);
  EXPECT_EQ(third->slot(), freed);
  EXPECT_FALSE(pool->Lease());
  first = std::unexpected(Error::InvalidConfig("released"));
  third = std::unexpected(Error::InvalidConfig("released"));
  for (int pass = 0; pass < 1000; ++pass) {
    auto a = pool->Lease();
    auto b = pool->Lease();
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_NE(a->slot(), b->slot());
    EXPECT_FALSE(pool->Lease());
  }
}

TEST(WorkerProduction, KeyHashDistributesSequentialAndHighBitKeys) {
  WorkerConfig config;
  config.slots_per_thread = 16;
  config.balance = WorkerBalance::kKeyHash;
  auto pool = WorkerClient<int>::Create(
      {"a"}, config, [](const auto &, auto, std::size_t slot) -> Result<int> {
        return static_cast<int>(slot);
      });
  ASSERT_TRUE(pool);
  for (bool high_bits : {false, true}) {
    std::array<unsigned, 16> histogram{};
    for (std::uint64_t key = 0; key < 4096; ++key) {
      auto lease = pool->LeaseByKey(high_bits ? key << 32 : key);
      ASSERT_TRUE(lease);
      ++histogram[lease->slot()];
    }
    for (auto count : histogram) {
      EXPECT_GE(count, 200U);
      EXPECT_LE(count, 320U);
    }
  }
}

struct AffineDrop {
  std::atomic<int> *drops;
  std::thread::id owner{std::this_thread::get_id()};
  ~AffineDrop() {
    EXPECT_EQ(owner, std::this_thread::get_id());
    drops->fetch_add(1);
    static_cast<void>(CloseThreadCaches());
  }
};

TEST(ThreadLocalProduction, DestroyedPoolDoesNotOutliveFactoryAtThreadExit) {
  std::atomic<int> drops{};
  auto factory = [&](const auto &) -> Result<std::unique_ptr<AffineDrop>> {
    return std::make_unique<AffineDrop>(&drops);
  };
  using Pool =
      ThreadLocalClient<std::unique_ptr<AffineDrop>, decltype(factory)>;
  auto made = Pool::Create({"a"}, factory);
  ASSERT_TRUE(made);
  auto pool = std::make_unique<Pool>(std::move(*made));
  std::promise<void> ready, release;
  auto released = release.get_future();
  auto *shared = pool.get();
  std::thread worker([&] {
    EXPECT_TRUE(shared->WithClient([](auto &) {}));
    ready.set_value();
    released.wait();
  });
  ready.get_future().wait();
  pool.reset();
  EXPECT_EQ(drops.load(), 0);
  release.set_value();
  worker.join();
  EXPECT_EQ(drops.load(), 1);
}

TEST(WorkerProduction, PoolDestructionCanRaceWithIndependentTlsTeardown) {
  for (int pass = 0; pass < 50; ++pass) {
    std::atomic<int> drops{};
    auto made = WorkerClient<std::unique_ptr<AffineDrop>>::Create(
        {"a"}, {},
        [&](const auto &, auto, auto) -> Result<std::unique_ptr<AffineDrop>> {
          return std::make_unique<AffineDrop>(&drops);
        });
    ASSERT_TRUE(made);
    auto pool = std::make_unique<decltype(made)::value_type>(std::move(*made));
    std::promise<void> ready, release;
    auto released = release.get_future();
    auto *shared = pool.get();
    std::thread worker([&] {
      EXPECT_TRUE(shared->WithClient([](auto &) {}));
      ready.set_value();
      released.wait();
    });
    ready.get_future().wait();
    release.set_value();
    pool.reset();
    worker.join();
    EXPECT_EQ(drops.load(), 1);
  }
}

struct MoveProbe {
  static inline thread_local void (*hook)(void *){};
  static inline thread_local void *context{};
  MoveProbe() = default;
  MoveProbe(const MoveProbe &) = delete;
  MoveProbe &operator=(const MoveProbe &) = delete;
  MoveProbe(MoveProbe &&other) noexcept { *this = std::move(other); }
  MoveProbe &operator=(MoveProbe &&) noexcept {
    if (auto fn = std::exchange(hook, nullptr))
      fn(context);
    return *this;
  }
};

TEST(WorkerProduction, SlotRemainsExclusiveDuringClientMoves) {
  auto pool = WorkerClient<MoveProbe>::Create(
      {"a"}, WorkerConfig{.slots_per_thread = 1},
      [](const auto &, auto, auto) -> Result<MoveProbe> {
        return MoveProbe{};
      });
  ASSERT_TRUE(pool);
  EXPECT_TRUE(pool->WithClient([](auto &) {}));
  MoveProbe::context = &*pool;
  const auto hook = [](void *ptr) {
    EXPECT_FALSE(static_cast<WorkerClient<MoveProbe> *>(ptr)->Lease());
  };
  MoveProbe::hook = hook;
  auto lease = pool->Lease();
  ASSERT_TRUE(lease);
  EXPECT_EQ(MoveProbe::hook, nullptr);
  MoveProbe::hook = hook;
  lease = std::unexpected(Error::InvalidConfig("released"));
  EXPECT_EQ(MoveProbe::hook, nullptr);
  EXPECT_TRUE(pool->Lease());
}

TEST(WorkerProduction, InvalidBalancesAndImpossibleSlotCountsAreRejected) {
  const auto factory = [](const auto &, auto, auto) -> Result<int> {
    return 1;
  };
  WorkerConfig config;
  config.balance = static_cast<WorkerBalance>(255);
  EXPECT_FALSE(WorkerClient<int>::Create({"a"}, config, factory));
  config.balance = WorkerBalance::kRoundRobin;
  config.slots_per_thread = std::numeric_limits<std::size_t>::max();
  EXPECT_FALSE(WorkerClient<int>::Create({"a"}, config, factory));
}

TEST(ThreadLocalProduction,
     TlsPoolDestructorAfterCacheDestructorDoesNotReopenCache) {
  std::atomic<int> drops{};
  std::thread worker([&] {
    auto factory = [&](const auto &) -> Result<std::unique_ptr<AffineDrop>> {
      return std::make_unique<AffineDrop>(&drops);
    };
    using Pool =
        ThreadLocalClient<std::unique_ptr<AffineDrop>, decltype(factory)>;
    // The optional is initialized first and is therefore destroyed after cache.
    static thread_local std::optional<Pool> owner;
    auto pool = Pool::Create({"a"}, factory);
    ASSERT_TRUE(pool);
    owner.emplace(std::move(*pool));
    EXPECT_TRUE(owner->WithClient([](auto &) {}));
  });
  worker.join();
  EXPECT_EQ(drops.load(), 1);
}

TEST(FailoverProduction, NegativeCooldownAndUnknownStrategyFailClosed) {
  FailoverPolicy policy{{"a"}};
  policy.WithCircuitBreaker(1, -1ns);
  policy.RecordFailure(0, FailoverTime{});
  auto selected = policy.Select(FailoverTime::max());
  ASSERT_FALSE(selected);
  EXPECT_EQ(selected.error().Kind(), ErrorKind::kInvalidConfig);
  EXPECT_FALSE(policy.NeedsProbe(0, FailoverTime::max()));
  policy.WithCircuitBreaker(1, 0ns);
  EXPECT_TRUE(policy.Select(FailoverTime{}));
  policy.WithSelection(static_cast<EndpointSelection>(255));
  EXPECT_FALSE(policy.Select(FailoverTime{}));
}

TEST(FailoverProduction, CooldownClockExtremesDoNotWrapOrOpenEarly) {
  FailoverPolicy policy{{"a"}};
  policy.WithCircuitBreaker(1, FailoverDuration::max());
  policy.RecordFailure(0, FailoverTime{1ns});
  EXPECT_FALSE(policy.Select(FailoverTime::max()));
  policy.RecordFailure(0, FailoverTime::min());
  EXPECT_FALSE(policy.Select(FailoverTime{-2ns}));
  EXPECT_TRUE(policy.Select(FailoverTime{-1ns}));
  policy.WithCircuitBreaker(1, 0ns);
  policy.RecordFailure(0, FailoverTime::max());
  EXPECT_FALSE(policy.Select(FailoverTime::min()));
  EXPECT_TRUE(policy.Select(FailoverTime::max()));
  policy.WithCircuitBreaker(1, 2ns);
  policy.RecordFailure(0, FailoverTime::max() - 2ns);
  EXPECT_FALSE(policy.Select(FailoverTime::max() - 1ns));
  EXPECT_TRUE(policy.Select(FailoverTime::max()));
}

TEST(FailoverProduction, MaximumRttAndNegativeSamplesCannotWrapEwma) {
  FailoverPolicy policy{{"a"}};
  policy.RecordSuccess(0, FailoverDuration::max());
  const auto maximum =
      static_cast<std::uint64_t>(FailoverDuration::max().count() / 1000);
  EXPECT_EQ(policy.Health(0)->ewma_rtt_us, maximum);
  for (int i = 0; i < 100; ++i) {
    policy.RecordSuccess(0, FailoverDuration::max());
    EXPECT_EQ(policy.Health(0)->ewma_rtt_us, maximum);
  }
  auto previous = maximum;
  for (int i = 0; i < 100; ++i) {
    policy.RecordSuccess(0, i % 2 ? FailoverDuration::min() : 0ns);
    const auto value = policy.Health(0)->ewma_rtt_us;
    EXPECT_LE(value, previous);
    EXPECT_GT(value, 0U);
    previous = value;
  }
}

TEST(FailoverProduction, RecursiveObserverCannotAdvanceSelectionAgain) {
  FailoverPolicy policy{{"a", "b"}};
  unsigned notifications{};
  policy.OnFailover([&](const auto &) {
    ++notifications;
    if (notifications == 1) {
      auto nested = policy.Select(FailoverTime{});
      EXPECT_FALSE(nested);
      if (!nested) {
        EXPECT_EQ(nested.error().Kind(), ErrorKind::kInvalidConfig);
      }
    }
  });
  EXPECT_EQ(policy.Select(FailoverTime{}), 0U);
  EXPECT_EQ(policy.Select(FailoverTime{}), 1U);
  EXPECT_EQ(notifications, 1U);
  EXPECT_EQ(policy.Select(FailoverTime{}), 0U);
}

TEST(FailoverProduction, RetryableTransportDoesNotAuthorizeAWriteReplay) {
  const auto unavailable =
      Error::Grpc(grpcxx::Status{grpcxx::StatusCode::kUnavailable, "offline"});
  const auto refused = Error::ServerStatus(4003, "busy", false);
  const auto permitted = Error::ServerStatus(4003, "busy", true);
  for (const auto *error : {&unavailable, &refused}) {
    EXPECT_FALSE(FailoverPolicy::ShouldFailoverWrite(true, *error));
    EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(false, *error));
    const auto exhausted = Error::AllEndpointsFailed(2, *error);
    EXPECT_FALSE(FailoverPolicy::ShouldFailoverWrite(true, exhausted));
  }
  EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(
      true, Error::AllEndpointsFailed(2, permitted)));
}

TEST(NativePoolsProduction, InvalidSettingsFailBeforeAnyConnection) {
  EXPECT_FALSE(ConnectThreadLocalClient(ClientOptions{}));
  EXPECT_FALSE(ConnectWorkerClient(ClientOptions{}));
  auto endpoint = Endpoint::Parse("unix:///tmp/greptimedb-audit-no-socket");
  ASSERT_TRUE(endpoint);
  auto local = ConnectThreadLocalClient(ClientOptions{*endpoint});
  auto worker = ConnectWorkerClient(ClientOptions{*endpoint});
  ASSERT_TRUE(local);
  ASSERT_TRUE(worker);
  EXPECT_FALSE(local->WithClient([](auto &) {}));
  EXPECT_FALSE(worker->Lease());
}
TEST(FailoverProduction,
     PositiveSubMicrosecondSamplesRankBeforeSlowerEndpoints) {
  FailoverPolicy policy{{"fast", "slow"}};
  policy.WithSelection(EndpointSelection::kLatencyAware);
  policy.RecordSuccess(0, 999ns);
  policy.RecordSuccess(1, 2us);
  EXPECT_EQ(policy.Health(0)->ewma_rtt_us, 1U);
  EXPECT_EQ(policy.Select(FailoverTime{}), 0U);
  policy.RecordSuccess(0, 1ns);
  EXPECT_EQ(policy.Select(FailoverTime{}), 0U);
}

TEST(WorkerProduction, StripeModuloCannotOverflowAtIntegerLimits) {
  const auto maximum = std::numeric_limits<std::size_t>::max();
  EXPECT_EQ(StripeShift(3, maximum, 1), (maximum % 3 + 1) % 3);
  EXPECT_EQ(StripeShift(maximum, maximum - 1, maximum - 1), maximum - 2);
  EXPECT_EQ(StripeShift(maximum, maximum, maximum), 0U);
  EXPECT_EQ(StripeShift(0, maximum, maximum), 0U);
}

TEST(PoolIdentityProduction, ExhaustionCannotProduceZeroOrReuseALivePoolId) {
  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  std::atomic<std::uint64_t> next{maximum - 1};
  EXPECT_EQ(detail::AllocateIdentity(next), maximum - 1);
  EXPECT_EQ(detail::AllocateIdentity(next), 0U);
  EXPECT_EQ(detail::AllocateIdentity(next), 0U);
  EXPECT_EQ(next.load(), maximum);
}

TEST(WorkerProduction,
     TlsPoolDestructorAfterCacheDestructorDoesNotReopenCache) {
  std::atomic<int> drops{};
  std::thread worker([&] {
    using Pool = WorkerClient<std::unique_ptr<AffineDrop>>;
    static thread_local std::optional<Pool> owner;
    auto pool = Pool::Create(
        {"a"}, {},
        [&](const auto &, auto, auto) -> Result<std::unique_ptr<AffineDrop>> {
          return std::make_unique<AffineDrop>(&drops);
        });
    ASSERT_TRUE(pool);
    owner.emplace(std::move(*pool));
    EXPECT_TRUE(owner->WithClient([](auto &) {}));
  });
  worker.join();
  EXPECT_EQ(drops.load(), 1);
}

TEST(WorkerProduction,
     CacheErasureDuringCheckoutAndReturnDoesNotReleaseNewLease) {
  using Pool = WorkerClient<MoveProbe>;
  for (bool during_return : {false, true}) {
    auto pool = Pool::Create({"a"}, WorkerConfig{.slots_per_thread = 1},
                             [](const auto &, auto, auto) -> Result<MoveProbe> {
                               return MoveProbe{};
                             });
    ASSERT_TRUE(pool);
    EXPECT_TRUE(pool->WithClient([](auto &) {}));
    struct Context {
      Pool *pool;
      std::optional<LeaseGuard<MoveProbe>> newer;
    } context{&*pool, std::nullopt};
    const auto hook = [](void *ptr) {
      auto &ctx = *static_cast<Context *>(ptr);
      EXPECT_TRUE(ctx.pool->CloseCurrentThread());
      auto lease = ctx.pool->Lease();
      ASSERT_TRUE(lease);
      ctx.newer.emplace(std::move(*lease));
    };
    MoveProbe::context = &context;
    if (!during_return)
      MoveProbe::hook = hook;
    auto old = pool->Lease();
    ASSERT_TRUE(old);
    if (during_return)
      MoveProbe::hook = hook;
    old = std::unexpected(Error::InvalidConfig("released"));
    ASSERT_TRUE(context.newer);
    EXPECT_FALSE(pool->Lease());
    context.newer.reset();
    EXPECT_TRUE(pool->Lease());
  }
}

TEST(ThreadLocalProduction, ClientMovesCannotCheckOutTheSameClientTwice) {
  auto factory = [](const auto &) -> Result<MoveProbe> { return MoveProbe{}; };
  using Pool = ThreadLocalClient<MoveProbe, decltype(factory)>;
  auto pool = Pool::Create({"a"}, factory);
  ASSERT_TRUE(pool);
  EXPECT_TRUE(pool->WithClient([](auto &) {}));
  MoveProbe::context = &*pool;
  MoveProbe::hook = [](void *ptr) {
    EXPECT_FALSE(static_cast<Pool *>(ptr)->WithClient([](auto &) {}));
  };
  EXPECT_TRUE(pool->WithClient([](auto &) {}));
  EXPECT_EQ(MoveProbe::hook, nullptr);
}

TEST(ThreadLocalProduction,
     CacheErasureDuringClientMovesPreservesNewGeneration) {
  for (bool during_return : {false, true}) {
    int builds{};
    auto factory = [&](const auto &) -> Result<MoveProbe> {
      ++builds;
      return MoveProbe{};
    };
    using Pool = ThreadLocalClient<MoveProbe, decltype(factory)>;
    auto pool = Pool::Create({"a"}, factory);
    ASSERT_TRUE(pool);
    EXPECT_TRUE(pool->WithClient([](auto &) {}));
    MoveProbe::context = &*pool;
    const auto hook = [](void *ptr) {
      auto &current = *static_cast<Pool *>(ptr);
      EXPECT_TRUE(current.CloseCurrentThread());
      EXPECT_TRUE(current.WithClient([](auto &) {}));
    };
    if (!during_return)
      MoveProbe::hook = hook;
    EXPECT_TRUE(pool->WithClient([&](auto &) {
      if (during_return)
        MoveProbe::hook = hook;
    }));
    EXPECT_EQ(MoveProbe::hook, nullptr);
    EXPECT_EQ(builds, 2);
    EXPECT_TRUE(pool->WithClient([](auto &) {}));
    EXPECT_EQ(builds, 2);
  }
}
} // namespace
