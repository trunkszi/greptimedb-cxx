#pragma once

#include <greptimedb/threading/thread_local_pool.h>
#include <greptimedb/threading/worker_pool.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace greptimedb::threading_test {

struct FakeSession {
  std::vector<std::string> endpoints{};
  std::uint64_t worker_id{0};
  std::size_t slot{0};
  int stamp{0};
  std::thread::id owner{std::this_thread::get_id()};
};

[[nodiscard]] inline FakeSession MakeSession(std::vector<std::string> endpoints, std::uint64_t worker_id,
                                             std::size_t slot, int stamp) {
  return FakeSession{
      .endpoints = std::move(endpoints),
      .worker_id = worker_id,
      .slot = slot,
      .stamp = stamp,
      .owner = std::this_thread::get_id(),
  };
}

/** Non-movable, non-copyable client verifying strict single-thread destruction. */
struct BoundClient final {
  std::thread::id owner{std::this_thread::get_id()};
  std::atomic<int> *drops{nullptr};

  explicit BoundClient(std::atomic<int> *counter) noexcept : drops(counter) {}
  BoundClient(const BoundClient &) = delete("BoundClient is pinned to its creating OS thread");
  BoundClient &operator=(const BoundClient &) = delete("BoundClient is pinned to its creating OS thread");
  BoundClient(BoundClient &&) = delete("BoundClient is non-movable across addresses or threads");
  BoundClient &operator=(BoundClient &&) = delete("BoundClient is non-movable across addresses or threads");

  ~BoundClient() {
    EXPECT_EQ(owner, std::this_thread::get_id());
    if (drops != nullptr) {
      drops->fetch_add(1, std::memory_order_relaxed);
    }
  }
};

/** Reentrant destructor that invokes `CloseThreadCaches()` during teardown. */
struct ReentrantDrop final {
  std::atomic<int> *drops{nullptr};
  std::thread::id owner{std::this_thread::get_id()};

  explicit ReentrantDrop(std::atomic<int> *counter) noexcept : drops(counter) {}
  ReentrantDrop(const ReentrantDrop &) = delete("ReentrantDrop is move-only");
  ReentrantDrop &operator=(const ReentrantDrop &) = delete("ReentrantDrop is move-only");
  ReentrantDrop(ReentrantDrop &&other) noexcept : drops(std::exchange(other.drops, nullptr)), owner(other.owner) {}
  ReentrantDrop &operator=(ReentrantDrop &&other) noexcept {
    if (this != &other) {
      Drop();
      drops = std::exchange(other.drops, nullptr);
      owner = other.owner;
    }
    return *this;
  }
  ~ReentrantDrop() { Drop(); }

  void Drop() {
    if (auto *counter = std::exchange(drops, nullptr)) {
      EXPECT_EQ(owner, std::this_thread::get_id());
      counter->fetch_add(1, std::memory_order_relaxed);
      [[maybe_unused]] const std::size_t closed = CloseThreadCaches();
    }
  }
};

/** Client probe that fires an optional hook during move operations. */
struct MoveProbe final {
  static inline thread_local void (*hook)(void *){nullptr};
  static inline thread_local void *context{nullptr};

  MoveProbe() = default;
  MoveProbe(const MoveProbe &) = delete("MoveProbe is move-only");
  MoveProbe &operator=(const MoveProbe &) = delete("MoveProbe is move-only");
  MoveProbe(MoveProbe &&other) noexcept { *this = std::move(other); }
  MoveProbe &operator=(MoveProbe &&) noexcept {
    if (auto *fn = std::exchange(hook, nullptr)) {
      fn(context);
    }
    return *this;
  }
};

/** Client watching cross-thread moves/destructions for death tests. */
struct MoveWatched final {
  std::thread::id owner{std::this_thread::get_id()};
  bool watch{false};

  MoveWatched() = default;
  MoveWatched(const MoveWatched &) = delete("MoveWatched is move-only");
  MoveWatched &operator=(const MoveWatched &) = delete("MoveWatched is move-only");
  MoveWatched(MoveWatched &&other) noexcept { *this = std::move(other); }
  MoveWatched &operator=(MoveWatched &&other) noexcept {
    if (other.watch && other.owner != std::this_thread::get_id()) {
      std::_Exit(77);
    }
    owner = other.owner;
    watch = other.watch;
    return *this;
  }
  ~MoveWatched() {
    if (watch && owner != std::this_thread::get_id()) {
      std::_Exit(78);
    }
  }
};

/** Mock endpoint failover config shaped like `nexora::EndpointFailoverConfig`. */
struct MockEndpointFailoverConfig {
  std::uint32_t failure_threshold{2};
  std::uint64_t base_cooldown_ns{1'000'000'000ULL};
  std::uint64_t random_seed{42ULL};
  void (*observer)() noexcept {nullptr};
};

/** Mock endpoint failover policy shaped like `nexora::EndpointFailover`. */
class MockEndpointFailover final {
public:
  MockEndpointFailover(std::size_t endpoint_count, MockEndpointFailoverConfig cfg = {}) noexcept
      : count_(endpoint_count), config_(std::move(cfg)) {}

  [[nodiscard]] constexpr std::size_t Size() const noexcept { return count_; }
  [[nodiscard]] constexpr const MockEndpointFailoverConfig &Config() const noexcept { return config_; }

private:
  std::size_t count_{0};
  MockEndpointFailoverConfig config_{};
};

} // namespace greptimedb::threading_test
