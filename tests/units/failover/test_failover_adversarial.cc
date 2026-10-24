/** Circuit admission, write-replay hints, trailer diagnostics, and cache
 * pruning. */
#include <greptime/v1/common.pb.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/hints.h>
#include <greptimedb/failover/failover.h>
#include <greptimedb/threading/thread_local_pool.h>
#include <greptimedb/threading/worker_pool.h>
#include <grpcxx/core/metadata.h>
#include <grpcxx/core/status.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using greptimedb::EndpointSelection;
using greptimedb::Error;
using greptimedb::ErrorKind;
using greptimedb::FailoverPolicy;
using greptimedb::Result;
using greptimedb::ThreadLocalClient;
using greptimedb::WorkerClient;
using greptimedb::WorkerConfig;
using Clock = std::chrono::steady_clock;

FailoverPolicy Names(std::initializer_list<const char *> names) {
  return FailoverPolicy{std::vector<std::string>{names.begin(), names.end()}};
}

struct FakeClient {
  std::thread::id owner{std::this_thread::get_id()};
  int stamp{};
};

TEST(FailoverPolicy, StickyAndLatencySkipAFailedEndpointBeforeTheCircuitOpens) {
  const auto now = Clock::now();
  for (const auto selection :
       {EndpointSelection::kSticky, EndpointSelection::kLatencyAware}) {
    auto policy = Names({"a", "b"});
    policy.WithSelection(selection).WithCircuitBreaker(
        3, std::chrono::seconds(60));
    if (selection == EndpointSelection::kLatencyAware) {
      policy.RecordSuccess(0, std::chrono::milliseconds(1));
    } else {
      auto stuck = policy.Select(now);
      ASSERT_TRUE(stuck);
      EXPECT_EQ(*stuck, 0U);
    }
    policy.RecordFailure(0, now);
    const auto *failed = policy.Health(0);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->consecutive_failures, 1U);
    EXPECT_FALSE(failed->circuit_opened_at.has_value());
    EXPECT_FALSE(failed->healthy);
    const std::array<std::size_t, 1> excluded{0};
    auto rotated = policy.SelectExcluding(now, excluded);
    ASSERT_TRUE(rotated);
    EXPECT_EQ(*rotated, 1U);
    EXPECT_EQ(policy.Health(0)->consecutive_failures, 1U);
    EXPECT_FALSE(policy.Health(0)->circuit_opened_at.has_value());
  }
}

TEST(FailoverPolicy, HalfOpenCircuitStaysOpenUntilASuccessfulProbe) {
  const auto now = Clock::now();
  auto policy = Names({"a", "b"});
  policy.WithCircuitBreaker(1, std::chrono::nanoseconds::zero());
  policy.RecordFailure(1, now);
  const auto *open = policy.Health(1);
  ASSERT_NE(open, nullptr);
  ASSERT_TRUE(open->circuit_opened_at.has_value());
  EXPECT_EQ(open->consecutive_failures, 1U);
  EXPECT_FALSE(open->healthy);
  EXPECT_TRUE(policy.NeedsProbe(1, now));
  EXPECT_FALSE(policy.NeedsProbe(0, now));

  const std::array<std::size_t, 1> skip_primary{0};
  auto probed = policy.SelectExcluding(now, skip_primary);
  ASSERT_TRUE(probed);
  EXPECT_EQ(*probed, 1U);
  EXPECT_TRUE(policy.Health(1)->circuit_opened_at.has_value());

  const auto refreshed = now + std::chrono::seconds(5);
  policy.RecordFailure(1, refreshed);
  ASSERT_TRUE(policy.Health(1)->circuit_opened_at.has_value());
  EXPECT_EQ(*policy.Health(1)->circuit_opened_at, refreshed);
  EXPECT_EQ(policy.Health(1)->consecutive_failures, 2U);
  EXPECT_EQ(policy.Health(1)->ewma_rtt_us, 0U);

  policy.RecordStreamSuccess(1);
  const auto *closed = policy.Health(1);
  ASSERT_NE(closed, nullptr);
  EXPECT_TRUE(closed->healthy);
  EXPECT_EQ(closed->consecutive_failures, 0U);
  EXPECT_FALSE(closed->circuit_opened_at.has_value());
  EXPECT_EQ(closed->ewma_rtt_us, 0U);
}

TEST(FailoverPolicy, StreamSuccessClearsFailuresWithoutSamplingLatency) {
  const auto now = Clock::now();
  auto policy = Names({"a", "b"});
  policy.WithCircuitBreaker(1, std::chrono::seconds(60));
  policy.RecordFailure(0, now);
  auto selected = policy.Select(now);
  ASSERT_TRUE(selected);
  EXPECT_EQ(*selected, 1U);
  EXPECT_TRUE(policy.Health(0)->circuit_opened_at.has_value());

  policy.RecordStreamSuccess(1);
  const auto *state = policy.Health(1);
  ASSERT_NE(state, nullptr);
  EXPECT_TRUE(state->healthy);
  EXPECT_EQ(state->consecutive_failures, 0U);
  EXPECT_EQ(state->ewma_rtt_us, 0U);
  EXPECT_FALSE(state->circuit_opened_at.has_value());
  EXPECT_EQ(policy.Health(0)->consecutive_failures, 1U);
}

TEST(FailoverPolicy, OneFailureOpensTheCircuitAndTheNextSelectSkipsIt) {
  const auto now = Clock::now();
  auto policy = Names({"a", "b"});
  policy.WithCircuitBreaker(1, std::chrono::seconds(60));
  auto first = policy.Select(now);
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, 0U);
  policy.RecordFailure(1, now);
  const auto *state = policy.Health(1);
  ASSERT_NE(state, nullptr);
  EXPECT_EQ(state->consecutive_failures, 1U);
  ASSERT_TRUE(state->circuit_opened_at.has_value());
  auto next = policy.Select(now);
  ASSERT_TRUE(next);
  EXPECT_EQ(*next, 0U);
  EXPECT_EQ(policy.Health(1)->consecutive_failures, 1U);
  EXPECT_FALSE(policy.Health(0)->circuit_opened_at.has_value());
}

TEST(FailoverPolicy, AbsentOrFalseApplicationHintsDoNotAuthorizeReplay) {
  for (const char *hint : {"false", ""}) {
    ::greptime::v1::ResponseHeader header;
    auto *status = header.mutable_status();
    status->set_status_code(4003);
    status->set_err_msg("region busy");
    status->set_retry_hint(hint);
    auto parsed = Error::FromResponseHeader(header);
    ASSERT_FALSE(parsed);
    EXPECT_EQ(parsed.error().Kind(), ErrorKind::kServerStatus);
    EXPECT_EQ(parsed.error().RawCode(), 4003U);
    EXPECT_EQ(parsed.error().RetryHint(), std::optional<bool>{false});
    EXPECT_FALSE(FailoverPolicy::ShouldFailoverWrite(true, parsed.error()));
    EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(false, parsed.error()));
  }

  ::greptime::v1::ResponseHeader granted;
  auto *status = granted.mutable_status();
  status->set_status_code(4003);
  status->set_err_msg("region busy");
  status->set_retry_hint("true");
  auto parsed = Error::FromResponseHeader(granted);
  ASSERT_FALSE(parsed);
  EXPECT_EQ(parsed.error().RetryHint(), std::optional<bool>{true});
  EXPECT_TRUE(FailoverPolicy::ShouldFailoverWrite(true, parsed.error()));
}

TEST(ErrorDiagnostics, OkGrpcTrailersCannotHideAnApplicationError) {
  grpcxx::MetadataMap trailers;
  ASSERT_TRUE(trailers.Add(greptimedb::kGreptimeErrorCodeHeader, "4003"));
  auto hidden = Error::CheckOkTrailers(trailers);
  ASSERT_FALSE(hidden);
  EXPECT_EQ(hidden.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(hidden.error().RawCode(), 4003U);

  const auto from_status =
      Error::FromGrpcStatusAndTrailers(grpcxx::Status::OkStatus(), trailers);
  EXPECT_EQ(from_status.Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(from_status.RawCode(), 4003U);
  EXPECT_EQ(from_status.RetryHint(), std::optional<bool>{false});
  EXPECT_FALSE(FailoverPolicy::ShouldFailoverWrite(true, from_status));
}

TEST(ThreadLocalClient, AbandonedFactoryEntriesArePrunedOnTheCallingThread) {
  std::atomic<int> stamps{0};
  auto factory = [&](const auto &) -> Result<FakeClient> {
    FakeClient client;
    client.stamp = stamps.fetch_add(1) + 1;
    return client;
  };
  auto opened = ThreadLocalClient<FakeClient, decltype(factory)>::Create(
      std::vector<std::string>{"a"}, factory);
  ASSERT_TRUE(opened);
  auto shared =
      std::make_shared<ThreadLocalClient<FakeClient, decltype(factory)>>(
          std::move(*opened));
  std::promise<void> cached;
  std::promise<void> released;
  std::atomic<std::size_t> closed{0};
  std::thread worker([shared, &cached, &released, &closed, &factory]() mutable {
    auto used = shared->WithClient(
        [](FakeClient &client) -> Result<int> { return client.stamp; });
    EXPECT_TRUE(used);
    shared.reset();
    cached.set_value();
    released.get_future().wait();
    auto fresh = ThreadLocalClient<FakeClient, decltype(factory)>::Create(
        std::vector<std::string>{"b"}, factory);
    EXPECT_TRUE(fresh);
    if (fresh) {
      auto again = fresh->WithClient(
          [](FakeClient &client) -> Result<int> { return client.stamp; });
      EXPECT_TRUE(again);
    }
    closed.store(greptimedb::CloseThreadCaches());
  });
  cached.get_future().wait();
  shared.reset();
  released.set_value();
  worker.join();
  EXPECT_EQ(closed.load(), 1U);
}

TEST(WorkerClient, AbandonedFactoryEntriesArePrunedOnTheCallingThread) {
  std::atomic<int> stamps{0};
  auto factory = [&](const auto &, std::uint64_t,
                     std::size_t) -> Result<FakeClient> {
    FakeClient client;
    client.stamp = stamps.fetch_add(1) + 1;
    return client;
  };
  WorkerConfig config;
  config.slots_per_thread = 1;
  auto opened = WorkerClient<FakeClient>::Create(std::vector<std::string>{"a"},
                                                 config, factory);
  ASSERT_TRUE(opened);
  auto shared = std::make_shared<WorkerClient<FakeClient>>(std::move(*opened));
  std::promise<void> cached;
  std::promise<void> released;
  std::atomic<std::size_t> closed{0};
  std::thread worker([shared, &cached, &released, &closed, &factory,
                      config]() mutable {
    {
      auto leased = shared->Lease();
      EXPECT_TRUE(leased);
    }
    shared.reset();
    cached.set_value();
    released.get_future().wait();
    auto fresh = WorkerClient<FakeClient>::Create(std::vector<std::string>{"b"},
                                                  config, factory);
    EXPECT_TRUE(fresh);
    if (fresh) {
      EXPECT_TRUE(fresh->Lease());
    }
    closed.store(greptimedb::CloseThreadCaches());
  });
  cached.get_future().wait();
  shared.reset();
  released.set_value();
  worker.join();
  EXPECT_EQ(closed.load(), 1U);
}

TEST(FailoverPolicy, RebindingRejectsCardinalityChangesAtomically) {
  auto policy = Names({"a", "b"});
  const auto now = Clock::now();
  ASSERT_TRUE(policy.Select(now));
  policy.RecordFailure(1, now);
  EXPECT_FALSE(policy.bind_endpoints({"single"}));
  EXPECT_EQ(*policy.Endpoint(0), "a");
  EXPECT_EQ(policy.Health(1)->consecutive_failures, 1U);
  ASSERT_TRUE(policy.bind_endpoints({"renamed-a", "renamed-b"}));
  auto next = policy.Select(now);
  ASSERT_TRUE(next);
  EXPECT_EQ(*next, 1U);
  EXPECT_EQ(*policy.Endpoint(*next), "renamed-b");
}

TEST(WorkerClient, CallbackErrorsAreFlatAndReleaseTheSlot) {
  auto factory = [](const auto &, std::uint64_t,
                    std::size_t) -> Result<FakeClient> { return FakeClient{}; };
  greptimedb::WorkerConfig config;
  config.slots_per_thread = 1;
  auto pool = WorkerClient<FakeClient>::Create({"a"}, config, factory);
  ASSERT_TRUE(pool);
  auto result = pool->WithClient([](FakeClient &) -> Result<int> {
    return std::unexpected(Error::Transport("callback failure"));
  });
  static_assert(std::is_same_v<decltype(result), Result<int>>);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().Message(), "callback failure");
  EXPECT_TRUE(pool->Lease());
  auto keyed = pool->WithClientByKey(42, [](FakeClient &) -> Result<void> {
    return std::unexpected(Error::Transport("keyed failure"));
  });
  static_assert(std::is_same_v<decltype(keyed), Result<void>>);
  ASSERT_FALSE(keyed);
  EXPECT_EQ(keyed.error().Message(), "keyed failure");
  EXPECT_TRUE(pool->LeaseByKey(42));
}

TEST(ThreadLocalClient, ClearedCheckoutCannotReplaceANewerGeneration) {
  int builds{};
  auto factory = [&](const auto &) -> Result<FakeClient> {
    FakeClient client;
    client.stamp = ++builds;
    return client;
  };
  auto pool =
      ThreadLocalClient<FakeClient, decltype(factory)>::Create({"a"}, factory);
  ASSERT_TRUE(pool);
  auto first = pool->WithClient([&](FakeClient &old) -> Result<int> {
    EXPECT_EQ(old.stamp, 1);
    EXPECT_TRUE(pool->CloseCurrentThread());
    auto newer =
        pool->WithClient([](FakeClient &client) { return client.stamp; });
    EXPECT_TRUE(newer);
    if (newer) {
      EXPECT_EQ(*newer, 2);
    }
    return old.stamp;
  });
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, 1);
  auto reused =
      pool->WithClient([](FakeClient &client) { return client.stamp; });
  ASSERT_TRUE(reused);
  EXPECT_EQ(*reused, 2);
  EXPECT_EQ(builds, 2);
}

TEST(WorkerClient, FailedOldFactoryCannotReleaseANewerLease) {
  using Pool = WorkerClient<FakeClient>;
  Pool *current{};
  bool first = true;
  std::optional<greptimedb::LeaseGuard<FakeClient>> newer;
  auto factory = [&](const auto &, std::uint64_t,
                     std::size_t) -> Result<FakeClient> {
    if (std::exchange(first, false)) {
      EXPECT_TRUE(current->CloseCurrentThread());
      auto replacement = current->Lease();
      EXPECT_TRUE(replacement);
      if (replacement)
        newer.emplace(std::move(*replacement));
      return std::unexpected(Error::Transport("old dial failed"));
    }
    return FakeClient{};
  };
  greptimedb::WorkerConfig config;
  config.slots_per_thread = 1;
  auto pool = Pool::Create({"a"}, config, factory);
  ASSERT_TRUE(pool);
  current = &*pool;
  auto failed = pool->Lease();
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().Message(), "old dial failed");
  ASSERT_TRUE(newer);
  EXPECT_FALSE(pool->Lease());
  newer.reset();
  EXPECT_TRUE(pool->Lease());
}

TEST(WorkerClient, ForeignThreadAccessAndMovedFromLeasesFailClosed) {
  auto factory = [](const auto &, std::uint64_t,
                    std::size_t) -> Result<FakeClient> { return FakeClient{}; };
  auto pool = WorkerClient<FakeClient>::Create({"a"}, {}, factory);
  ASSERT_TRUE(pool);
  auto lease = pool->Lease();
  ASSERT_TRUE(lease);
  std::thread worker([&] {
    auto foreign = lease->TryClient();
    EXPECT_FALSE(foreign);
    if (!foreign) {
      EXPECT_EQ(foreign.error().Kind(), ErrorKind::kStream);
    }
  });
  worker.join();
  EXPECT_TRUE(lease->TryClient());
  auto moved = std::move(*lease);
  EXPECT_FALSE(lease->TryClient());
  EXPECT_TRUE(moved.TryClient());
}

template <std::size_t Index> struct TaggedClient {
  int value = 7;
};
template <std::size_t Index> auto TypedFactory() {
  auto factory = [](const auto &) -> Result<TaggedClient<Index>> {
    return TaggedClient<Index>{};
  };
  return ThreadLocalClient<TaggedClient<Index>, decltype(factory)>::Create(
      {"a"}, factory);
}
template <std::size_t... Index>
void CheckAllTypes(std::index_sequence<Index...>) {
  auto pools = std::tuple{TypedFactory<Index>()...};
  std::apply(
      [](auto &...pool) {
        (([&] {
           ASSERT_TRUE(pool);
           EXPECT_TRUE(
               pool->WithClient([](auto &client) { return client.value; }));
         }()),
         ...);
      },
      pools);
  EXPECT_EQ(greptimedb::CloseThreadCaches(), sizeof...(Index));
}
TEST(ThreadLocalClient, GlobalCloseCoversMoreThanThirtyTwoClientTypes) {
  static_cast<void>(greptimedb::CloseThreadCaches());
  CheckAllTypes(std::make_index_sequence<40>{});
}

struct ReentrantDrop {
  std::atomic<int> *drops{};
  explicit ReentrantDrop(std::atomic<int> *counter) : drops(counter) {}
  ReentrantDrop(const ReentrantDrop &) = delete;
  ReentrantDrop &operator=(const ReentrantDrop &) = delete;
  ReentrantDrop(ReentrantDrop &&other) noexcept
      : drops(std::exchange(other.drops, nullptr)) {}
  ReentrantDrop &operator=(ReentrantDrop &&other) noexcept {
    if (this != &other) {
      Drop();
      drops = std::exchange(other.drops, nullptr);
    }
    return *this;
  }
  ~ReentrantDrop() { Drop(); }
  void Drop() {
    if (auto *counter = std::exchange(drops, nullptr)) {
      counter->fetch_add(1);
      static_cast<void>(greptimedb::CloseThreadCaches());
    }
  }
};
TEST(ThreadLocalClient, ThreadExitAndExplicitCloseAllowReentrantDestructors) {
  std::atomic<int> drops{};
  auto factory = [&](const auto &) -> Result<ReentrantDrop> {
    return ReentrantDrop{&drops};
  };
  auto pool = ThreadLocalClient<ReentrantDrop, decltype(factory)>::Create(
      {"a"}, factory);
  ASSERT_TRUE(pool);
  std::thread worker([&] {
    EXPECT_TRUE(pool->WithClient([](ReentrantDrop &) {}));
    EXPECT_TRUE(pool->CloseCurrentThread());
    EXPECT_EQ(drops.load(), 1);
    EXPECT_TRUE(pool->WithClient([](ReentrantDrop &) {}));
  });
  worker.join();
  EXPECT_EQ(drops.load(), 2);
}
TEST(WorkerClient, ThreadExitAndExplicitCloseAllowReentrantDestructors) {
  std::atomic<int> drops{};
  auto factory = [&](const auto &, std::uint64_t,
                     std::size_t) -> Result<ReentrantDrop> {
    return ReentrantDrop{&drops};
  };
  auto pool = WorkerClient<ReentrantDrop>::Create({"a"}, {}, factory);
  ASSERT_TRUE(pool);
  std::thread worker([&] {
    EXPECT_TRUE(pool->WithClient([](ReentrantDrop &) {}));
    EXPECT_TRUE(pool->CloseCurrentThread());
    EXPECT_EQ(drops.load(), 1);
    EXPECT_TRUE(pool->WithClient([](ReentrantDrop &) {}));
  });
  worker.join();
  EXPECT_EQ(drops.load(), 2);
}

TEST(ThreadLocalClient,
     PolicySettingsReachEachThreadFactoryWithoutHealthSharing) {
  auto policy = Names({"display-a", "display-b"});
  policy.WithSelection(EndpointSelection::kSticky)
      .WithCircuitBreaker(1, std::chrono::seconds(30));
  std::atomic<int> builds{};
  auto factory =
      [&](const auto &names,
          const greptimedb::FailoverConfig *settings) -> Result<FakeClient> {
    EXPECT_NE(settings, nullptr);
    if (!settings)
      return std::unexpected(Error::InvalidConfig("missing policy"));
    EXPECT_EQ(settings->endpoints, names);
    EXPECT_EQ(settings->selection, EndpointSelection::kSticky);
    auto own = FailoverPolicy::FromConfig(*settings);
    auto first = own.Select(Clock::now());
    EXPECT_TRUE(first);
    if (first) {
      EXPECT_EQ(*first, 0U);
    }
    own.RecordFailure(0, Clock::now());
    auto second = own.Select(Clock::now());
    EXPECT_TRUE(second);
    if (second) {
      EXPECT_EQ(*second, 1U);
    }
    FakeClient client;
    client.stamp = builds.fetch_add(1) + 1;
    return client;
  };
  auto pool = ThreadLocalClient<FakeClient, decltype(factory)>::Create(
      {"a", "b"}, factory, policy);
  ASSERT_TRUE(pool);
  std::array<std::thread, 4> workers;
  for (auto &worker : workers)
    worker = std::thread([&] {
      auto first =
          pool->WithClient([](FakeClient &client) { return client.stamp; });
      auto second =
          pool->WithClient([](FakeClient &client) { return client.stamp; });
      ASSERT_TRUE(first);
      ASSERT_TRUE(second);
      EXPECT_EQ(*first, *second);
    });
  for (auto &worker : workers)
    worker.join();
  EXPECT_EQ(builds.load(), 4);
  EXPECT_EQ(*policy.Endpoint(0), "display-a");
  EXPECT_EQ(policy.Health(0)->consecutive_failures, 0U);
}

TEST(WorkerClient, PolicySettingsFollowTheStripedEndpointOrder) {
  auto policy = Names({"display-a", "display-b"});
  policy.WithMaxAttempts(1).WithRandomSeed(17);
  std::atomic<int> builds{};
  auto factory =
      [&](const auto &names, std::uint64_t worker, std::size_t slot,
          const greptimedb::FailoverConfig *settings) -> Result<FakeClient> {
    EXPECT_NE(settings, nullptr);
    if (!settings)
      return std::unexpected(Error::InvalidConfig("missing policy"));
    EXPECT_EQ(settings->endpoints, names);
    EXPECT_EQ(settings->max_attempts, 1U);
    EXPECT_EQ(settings->random_seed, 17U);
    const std::vector<std::string> original{"a", "b"};
    EXPECT_EQ(names,
              greptimedb::StripedEndpoints(original, worker, slot, true));
    FakeClient client;
    client.stamp = builds.fetch_add(1) + 1;
    return client;
  };
  greptimedb::WorkerConfig config;
  config.slots_per_thread = 2;
  auto pool =
      WorkerClient<FakeClient>::Create({"a", "b"}, config, factory, policy);
  ASSERT_TRUE(pool);
  auto check = [&] {
    auto first = pool->Lease();
    auto second = pool->Lease();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_NE(first->client().stamp, second->client().stamp);
  };
  check();
  std::thread worker(check);
  worker.join();
  EXPECT_EQ(builds.load(), 4);
  EXPECT_EQ(*policy.Endpoint(0), "display-a");
}

TEST(SharedFactories, PolicyCannotBeSilentlyIgnoredByAnIncompatibleFactory) {
  auto policy = Names({"a"});
  auto local = [](const auto &) -> Result<FakeClient> { return FakeClient{}; };
  auto worker = [](const auto &, std::uint64_t,
                   std::size_t) -> Result<FakeClient> { return FakeClient{}; };
  EXPECT_FALSE((ThreadLocalClient<FakeClient, decltype(local)>::Create(
      {"a"}, local, policy)));
  EXPECT_FALSE(WorkerClient<FakeClient>::Create({"a"}, {}, worker, policy));
}

struct BoundClient {
  std::thread::id owner{std::this_thread::get_id()};
  std::atomic<int> *drops;
  explicit BoundClient(std::atomic<int> *count) : drops(count) {}
  BoundClient(const BoundClient &) = delete;
  BoundClient &operator=(const BoundClient &) = delete;
  ~BoundClient() {
    EXPECT_EQ(owner, std::this_thread::get_id());
    drops->fetch_add(1);
  }
};
TEST(SharedFactories, OwnedNonMovableClientsStayOnTheirCreatingThread) {
  using Handle = std::unique_ptr<BoundClient>;
  static_assert(!std::is_move_constructible_v<BoundClient>);
  std::atomic<int> drops{};
  auto local_factory = [&](const auto &) -> Result<Handle> {
    return std::make_unique<BoundClient>(&drops);
  };
  auto worker_factory = [&](const auto &, std::uint64_t,
                            std::size_t) -> Result<Handle> {
    return std::make_unique<BoundClient>(&drops);
  };
  auto local = ThreadLocalClient<Handle, decltype(local_factory)>::Create(
      {"a"}, local_factory);
  auto leased = WorkerClient<Handle>::Create({"a"}, {}, worker_factory);
  ASSERT_TRUE(local);
  ASSERT_TRUE(leased);
  auto use = [&](Handle &handle) {
    EXPECT_EQ(handle->owner, std::this_thread::get_id());
  };
  std::array<std::thread, 2> threads;
  for (auto &thread : threads)
    thread = std::thread([&] {
      EXPECT_TRUE(local->WithClient(use));
      EXPECT_TRUE(leased->WithClient(use));
      EXPECT_EQ(greptimedb::CloseThreadCaches(), 2U);
    });
  for (auto &thread : threads)
    thread.join();
  EXPECT_EQ(drops.load(), 4);
}

} // namespace
