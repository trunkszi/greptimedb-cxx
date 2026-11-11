/**
 * @file worker_pool.h
 * @brief Per-thread leased worker client slots with endpoint striping and O(1) liveness.
 */
#pragma once

#include "greptimedb/threading/thread_local_pool.h"

#include <grpcxx/core/inplace_delegate.h>
#include <uarch/uarch.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace greptimedb {

/** How a worker picks a slot on the calling thread. */
enum class WorkerBalance : std::uint8_t {
  kRoundRobin = 0,
  kLeastInFlight = 1,
  kKeyHash = 2,
};

/** Slot count and placement for one `WorkerClient`. */
struct WorkerConfig {
  /** Clients created on each OS thread. Zero is treated as one. */
  std::size_t slots_per_thread{4};
  WorkerBalance balance{WorkerBalance::kRoundRobin};
  /** Rotate endpoints by `(worker_id + slot) % endpoints.size()`. */
  bool stripe_endpoints{true};

  [[nodiscard]] constexpr std::size_t SlotCount() const noexcept {
    return slots_per_thread == 0 ? 1uz : slots_per_thread;
  }
};

namespace detail {

[[nodiscard]] inline std::size_t StripeEndpoints(std::size_t endpoint_count, std::uint64_t worker_index,
                                                 std::size_t offset) noexcept {
  if (endpoint_count == 0) return 0;
  const uarch::FastDivisorU64 ep_div = *uarch::FastDivisorU64::try_new(static_cast<std::uint64_t>(endpoint_count));
  const auto a = static_cast<std::size_t>(ep_div.rem(worker_index));
  const auto b = static_cast<std::size_t>(ep_div.rem(static_cast<std::uint64_t>(offset)));
  return a >= endpoint_count - b ? a - (endpoint_count - b) : a + b;
}

} // namespace detail

/** Index of the endpoint that should be dialed first for this worker slot. */
[[nodiscard]] inline std::size_t StripeShift(std::size_t endpoint_count, std::uint64_t worker_id,
                                             std::size_t slot) noexcept {
  return detail::StripeEndpoints(endpoint_count, worker_id, slot);
}

/** Rotates `endpoints` so index `(worker_id + slot) % size` is first. */
[[nodiscard]] inline std::vector<std::string>
StripedEndpoints(std::span<const std::string> endpoints, std::uint64_t worker_id, std::size_t slot, bool enabled) {
  std::vector<std::string> rotated(endpoints.begin(), endpoints.end());
  if (!enabled) return rotated;
  const std::size_t shift = StripeShift(rotated.size(), worker_id, slot);
  if (shift != 0) std::rotate(rotated.begin(), rotated.begin() + static_cast<std::ptrdiff_t>(shift), rotated.end());
  return rotated;
}

/** Arguments the worker factory receives for one newly dialed slot. */
struct SlotDial {
  std::uint64_t worker_id{0};
  std::size_t slot{0};
  std::vector<std::string> endpoints{};
};

/**
 * RAII lease of one slot.
 *
 * Destruction returns the client to the slot on the thread that leased it.
 * Access and destruction must occur on the leasing thread; violations terminate.
 */
template <typename ClientT> class LeaseGuard {
public:
  LeaseGuard() = default;
  LeaseGuard(const LeaseGuard &) = delete("LeaseGuard holds an exclusive worker slot lease; move instead");
  LeaseGuard &operator=(const LeaseGuard &) = delete("LeaseGuard holds an exclusive worker slot lease; move instead");
  LeaseGuard(LeaseGuard &&other) noexcept { MoveFrom(other); }
  LeaseGuard &operator=(LeaseGuard &&other) noexcept {
    other.CheckMoveThread();
    if (this != &other) {
      Release();
      MoveFrom(other);
    }
    return *this;
  }
  ~LeaseGuard() { Release(); }

  [[nodiscard]] Result<std::reference_wrapper<ClientT>> TryClient() noexcept {
    if (!AffinityHolds() || !client_)
      return std::unexpected(Error{ErrorKind::kStream, "lease is empty or belongs to another thread"});
    return std::ref(*client_);
  }

  [[nodiscard]] Result<std::reference_wrapper<const ClientT>> TryClient() const noexcept {
    if (!AffinityHolds() || !client_)
      return std::unexpected(Error{ErrorKind::kStream, "lease is empty or belongs to another thread"});
    return std::cref(*client_);
  }

  [[nodiscard]] ClientT &Client() {
    if (uarch::unlikely(!AffinityHolds() || !client_)) std::terminate();
    return *client_;
  }

  [[nodiscard]] const ClientT &Client() const {
    if (uarch::unlikely(!AffinityHolds() || !client_)) std::terminate();
    return *client_;
  }

  [[nodiscard]] ClientT &operator*() { return Client(); }
  [[nodiscard]] const ClientT &operator*() const { return Client(); }
  [[nodiscard]] ClientT *operator->() { return &Client(); }
  [[nodiscard]] const ClientT *operator->() const { return &Client(); }
  [[nodiscard]] std::size_t Slot() const noexcept { return slot_; }
  [[nodiscard]] bool AffinityHolds() const noexcept { return armed_ && detail::ThreadIdentity() == owner_thread_; }

private:
  template <typename> friend class WorkerClient;

  void CheckMoveThread() const noexcept {
    if (armed_ && uarch::unlikely(!AffinityHolds())) std::terminate();
  }

  void MoveFrom(LeaseGuard &other) noexcept {
    other.CheckMoveThread();
    client_ = std::move(other.client_);
    pool_id_ = other.pool_id_;
    generation_ = std::exchange(other.generation_, 0);
    slot_ = other.slot_;
    owner_thread_ = other.owner_thread_;
    armed_ = std::exchange(other.armed_, false);
    other.client_.reset();
  }

  void Release() noexcept;

  std::optional<ClientT> client_{};
  std::uint64_t pool_id_{0};
  std::uint64_t generation_{0};
  std::size_t slot_{0};
  std::uint64_t owner_thread_{0};
  bool armed_{false};
};

namespace detail {

template <typename ClientT> struct WorkerCache {
  enum class SlotState : std::uint8_t {
    kIdle = 0,
    kLeased = 1,
  };

  struct Slot {
    SlotState state{SlotState::kIdle};
    std::optional<ClientT> client{};
  };

  struct Entry {
    std::uint64_t generation{0};
    std::uint64_t worker_id{0};
    std::vector<Slot> slots{};
    std::size_t rr{0};
    uarch::FastDivisorU64 slots_div = *uarch::FastDivisorU64::try_new(1);
    std::uint32_t pins{0};
    bool retired{false};
  };

  [[nodiscard]] static std::size_t SelectSlot(const uarch::FastDivisorU64 &slots_div, std::uint64_t cursor,
                                              std::size_t offset) noexcept {
    return static_cast<std::size_t>(slots_div.rem(cursor + offset));
  }

  using Storage = CacheStorage<Entry, LivenessRegistry>;
  [[nodiscard]] static bool Closing() noexcept { return Storage::Closing(); }
  static std::size_t Clear() { return Storage::Prune(std::nullopt, true).first; }
  static void EnsureRegistered() noexcept {
    static thread_local CacheCloser closer{.clear = &Clear};
    [[maybe_unused]] static thread_local const bool registered = (RegisterCacheCloser(closer), true);
  }

  static void Return(std::uint64_t pool_id, std::uint64_t generation, std::size_t slot,
                     std::optional<ClientT> &client) noexcept {
    if (uarch::unlikely(Closing())) {
      client.reset();
      return;
    }
    auto &local = Storage::State();
    auto *entry = Storage::Find(local, pool_id);
    if (entry == nullptr || generation == 0 || entry->generation != generation) {
      client.reset();
      return;
    }
    if (!LivenessRegistry::IsAlive(pool_id)) {
      Storage::Prune(pool_id, false);
      client.reset();
      return;
    }
    if (slot < entry->slots.size()) {
      typename Storage::PinGuard pin{*entry};
      entry->slots[slot].client = std::move(client);
      if (!entry->retired) entry->slots[slot].state = SlotState::kIdle;
    }
    client.reset();
  }

  static void ReleaseEmpty(std::uint64_t pool_id, std::uint64_t generation, std::size_t slot) noexcept {
    std::optional<ClientT> empty{};
    Return(pool_id, generation, slot, empty);
  }
};

[[nodiscard]] constexpr std::uint64_t Fnv1a(std::uint64_t key) noexcept {
  constexpr std::uint64_t kOffset = 0xcbf29ce484222325ULL;
  constexpr std::uint64_t kPrime = 0x100000001b3ULL;
  std::uint64_t hash = kOffset;
  for (int shift = 0; shift < 64; shift += 8) {
    hash ^= (key >> shift) & 0xffU;
    hash *= kPrime;
  }
  return hash;
}

} // namespace detail

template <typename ClientT> void LeaseGuard<ClientT>::Release() noexcept {
  if (!armed_) return;
  armed_ = false;
  if (uarch::unlikely(detail::ThreadIdentity() != owner_thread_)) std::terminate();
  detail::WorkerCache<ClientT>::Return(pool_id_, generation_, slot_, client_);
}

/**
 * Shared factory for per-thread leased clients.
 *
 * The pool must outlive active member calls. A `const Snapshot*` passed into
 * the factory is valid only for that call. Slot `s` on worker `w` dials
 * endpoints rotated by `(w + s) % size` when striping is enabled. Cached
 * clients that outlive the pool are dropped on their owning thread or at exit.
 */
template <typename ClientT> class WorkerClient {
public:
  WorkerClient(const WorkerClient &) = delete("WorkerClient owns a unique pool identity; move instead");
  WorkerClient &operator=(const WorkerClient &) = delete("WorkerClient owns a unique pool identity; move instead");

  WorkerClient(WorkerClient &&other) noexcept
      : lifetime_(std::move(other.lifetime_)), endpoints_(std::move(other.endpoints_)), config_(other.config_),
        factory_(std::move(other.factory_)), next_worker_(std::move(other.next_worker_)) {}

  WorkerClient &operator=(WorkerClient &&other) noexcept {
    if (this != &other) {
      ReleaseCurrent();
      lifetime_ = std::move(other.lifetime_);
      endpoints_ = std::move(other.endpoints_);
      config_ = other.config_;
      factory_ = std::move(other.factory_);
      next_worker_ = std::move(other.next_worker_);
    }
    return *this;
  }

  ~WorkerClient() { ReleaseCurrent(); }

  template <typename FactoryFn>
  [[nodiscard]] static std::expected<WorkerClient, Error> Create(std::vector<std::string> endpoints,
                                                                 WorkerConfig config, FactoryFn factory) {
    if constexpr (!std::is_invocable_r_v<Result<ClientT>, const FactoryFn &, const std::vector<std::string> &,
                                         std::uint64_t, std::size_t>) {
      return std::unexpected(Error::InvalidConfig("worker factory signature mismatch"));
    } else {
      auto invoker = [fn = std::move(factory)](const std::vector<std::string> &names, std::uint64_t worker,
                                               std::size_t slot) -> Result<ClientT> { return fn(names, worker, slot); };
      using Invoker = decltype(invoker);
      if constexpr (sizeof(Invoker) <= Dial::kCapacity && alignof(Invoker) <= alignof(std::max_align_t) &&
                    std::is_nothrow_move_constructible_v<Invoker>) {
        return CreateWithDial(std::move(endpoints), config, Dial(std::move(invoker)));
      } else {
        auto boxed = std::make_unique<const Invoker>(std::move(invoker));
        if (uarch::unlikely(!boxed)) return std::unexpected(Error::InvalidConfig("worker factory allocation failed"));
        return CreateWithDial(std::move(endpoints), config,
                              Dial([owned = std::move(boxed)](
                                       const std::vector<std::string> &names, std::uint64_t worker,
                                       std::size_t slot) -> Result<ClientT> { return (*owned)(names, worker, slot); }));
      }
    }
  }

  template <typename FactoryFn, detail::PolicyConfigProvider Policy>
  [[nodiscard]] static std::expected<WorkerClient, Error>
  Create(std::vector<std::string> endpoints, WorkerConfig config, FactoryFn factory, const Policy &policy) {
    using Snapshot = detail::PolicySnapshotOf<Policy>;
    if (detail::PolicyHasObserver(policy))
      return std::unexpected(Error::InvalidConfig("failover observers are thread-affine and cannot be shared"));
    if constexpr (!std::is_invocable_r_v<Result<ClientT>, const FactoryFn &, const std::vector<std::string> &,
                                         std::uint64_t, std::size_t, const Snapshot *>) {
      return std::unexpected(Error::InvalidConfig("factory must accept failover settings"));
    } else {
      if (detail::PolicyEndpointCount(policy) != endpoints.size())
        return std::unexpected(Error::InvalidConfig("policy endpoint count differs from client options"));
      struct PolicyDialState {
        FactoryFn fn;
        Snapshot snapshot;
      };
      Snapshot base_snapshot = detail::ExtractPolicySnapshot(policy);
      if constexpr (requires { base_snapshot.endpoints = endpoints; }) base_snapshot.endpoints = endpoints;
      auto state = std::make_unique<const PolicyDialState>(
          PolicyDialState{.fn = std::move(factory), .snapshot = std::move(base_snapshot)});
      if (uarch::unlikely(!state)) return std::unexpected(Error::InvalidConfig("worker policy allocation failed"));
      return CreateWithDial(std::move(endpoints), config,
                            Dial([owned = std::move(state)](const std::vector<std::string> &names, std::uint64_t worker,
                                                            std::size_t slot) -> Result<ClientT> {
                              if constexpr (requires(Snapshot copy) { copy.endpoints = names; }) {
                                Snapshot striped = owned->snapshot;
                                striped.endpoints = names;
                                return owned->fn(names, worker, slot, &striped);
                              } else {
                                return owned->fn(names, worker, slot, &owned->snapshot);
                              }
                            }));
    }
  }

  [[nodiscard]] const WorkerConfig &Config() const noexcept { return config_; }
  [[nodiscard]] const std::vector<std::string> &Endpoints() const noexcept { return endpoints_; }

  [[nodiscard]] std::vector<std::string> PreviewEndpoints(std::uint64_t worker_id, std::size_t slot) const {
    return StripedEndpoints(endpoints_, worker_id, slot, config_.stripe_endpoints);
  }

  [[nodiscard]] std::expected<LeaseGuard<ClientT>, Error> Lease() const { return LeaseSlot(std::nullopt); }
  [[nodiscard]] std::expected<LeaseGuard<ClientT>, Error> LeaseByKey(std::uint64_t key) const { return LeaseSlot(key); }

  template <typename Fn>
  auto WithClient(Fn &&fn) const
      -> std::expected<typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type, Error> {
    return WithSlot(std::nullopt, std::forward<Fn>(fn));
  }

  template <typename Fn>
  auto WithClientByKey(std::uint64_t key, Fn &&fn) const
      -> std::expected<typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type, Error> {
    return WithSlot(key, std::forward<Fn>(fn));
  }

  bool CloseCurrentThread() const {
    const std::uint64_t pool_id = lifetime_.Id();
    return pool_id != 0 && Cache::Storage::Prune(pool_id, false).second;
  }

  static std::size_t CloseThreadCaches() { return detail::WorkerCache<ClientT>::Clear(); }

private:
  using Cache = detail::WorkerCache<ClientT>;
  using Entry = typename Cache::Entry;
  using Dial =
      grpcxx::InplaceDelegate<Result<ClientT>(const std::vector<std::string> &, std::uint64_t, std::size_t), 64>;

  WorkerClient() = default;

  void ReleaseCurrent() {
    if (lifetime_) {
      CloseCurrentThread();
      lifetime_.Reset();
    }
  }

  template <typename Fn>
  auto WithSlot(std::optional<std::uint64_t> key, Fn &&fn) const
      -> std::expected<typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type, Error> {
    auto guard = LeaseSlot(key);
    if (!guard.has_value()) return std::unexpected(std::move(guard.error()));
    return detail::InvokeWithClient(std::forward<Fn>(fn), guard->Client());
  }

  [[nodiscard]] static std::expected<WorkerClient, Error> CreateWithDial(std::vector<std::string> endpoints,
                                                                         WorkerConfig config, Dial dial) {
    bool valid_balance = false;
    switch (config.balance) {
    case WorkerBalance::kRoundRobin:
    case WorkerBalance::kLeastInFlight:
    case WorkerBalance::kKeyHash: valid_balance = true; break;
    }
    if (!valid_balance || config.SlotCount() > std::vector<typename Cache::Slot>{}.max_size())
      return std::unexpected(Error::InvalidConfig("invalid worker settings"));
    Cache::EnsureRegistered();
    WorkerClient pool;
    pool.lifetime_ = detail::PoolLifetime::Acquire();
    if (uarch::unlikely(!pool.lifetime_)) return std::unexpected(Error::InvalidConfig("pool identity exhausted"));
    pool.next_worker_ = std::make_unique<std::atomic<std::uint64_t>>(0);
    if (uarch::unlikely(!pool.next_worker_))
      return std::unexpected(Error::InvalidConfig("worker counter allocation failed"));
    pool.endpoints_ = std::move(endpoints);
    pool.config_ = config;
    pool.factory_ = std::move(dial);
    return pool;
  }

  [[nodiscard]] std::expected<LeaseGuard<ClientT>, Error> LeaseSlot(std::optional<std::uint64_t> key) const {
    Cache::EnsureRegistered();
    auto taken = TakeSlot(key);
    if (uarch::unlikely(!taken.has_value())) return std::unexpected(std::move(taken.error()));
    const std::uint64_t pool_id = lifetime_.Id();
    auto [worker_id, slot, existing, generation] = std::move(*taken);
    if (!existing.has_value()) {
      if (uarch::unlikely(!factory_)) {
        Cache::ReleaseEmpty(pool_id, generation, slot);
        return std::unexpected(Error::InvalidConfig("worker factory is empty"));
      }
      const std::vector<std::string> dial = StripedEndpoints(endpoints_, worker_id, slot, config_.stripe_endpoints);
      auto created = factory_(dial, worker_id, slot);
      if (uarch::unlikely(!created.has_value())) {
        Cache::ReleaseEmpty(pool_id, generation, slot);
        return std::unexpected(std::move(created.error()));
      }
      existing = std::move(*created);
    }
    LeaseGuard<ClientT> guard;
    guard.client_ = std::move(existing);
    guard.pool_id_ = pool_id;
    guard.generation_ = generation;
    guard.slot_ = slot;
    guard.owner_thread_ = detail::ThreadIdentity();
    guard.armed_ = true;
    return guard;
  }

  struct Taken {
    std::uint64_t worker_id{0};
    std::size_t slot{0};
    std::optional<ClientT> client{};
    std::uint64_t generation{0};
  };

  [[nodiscard]] std::expected<Taken, Error> TakeSlot(std::optional<std::uint64_t> key) const {
    if (uarch::unlikely(Cache::Closing()))
      return std::unexpected(Error::InvalidConfig("thread-local storage is closing"));
    const std::uint64_t pool_id = lifetime_.Id();
    if (uarch::unlikely(pool_id == 0 || !next_worker_))
      return std::unexpected(Error::InvalidConfig("worker pool is empty"));
    auto &local = Cache::Storage::State();
    auto *entry = Cache::Storage::Find(local, pool_id);
    if (entry == nullptr) {
      auto slot = Cache::Storage::Reserve(local, pool_id);
      if (uarch::unlikely(!slot)) return std::unexpected(Error::InvalidConfig("per-thread pool capacity exhausted"));
      auto created = std::make_unique<Entry>();
      created->generation = detail::NextGeneration();
      created->worker_id = next_worker_->fetch_add(1, std::memory_order_relaxed);
      created->slots.resize(config_.SlotCount());
      created->slots_div =
          *uarch::FastDivisorU64::try_new(static_cast<std::uint64_t>(std::max<std::size_t>(created->slots.size(), 1)));
      entry = Cache::Storage::Commit(local, *slot, pool_id, std::move(created));
    }
    auto picked = PickSlot(*entry, config_.balance, key);
    if (uarch::unlikely(!picked.has_value())) return std::unexpected(std::move(picked.error()));
    const std::size_t slot = *picked;
    entry->slots[slot].state = Cache::SlotState::kLeased;
    Taken taken{.worker_id = entry->worker_id, .slot = slot, .client = std::nullopt, .generation = entry->generation};
    if (entry->slots[slot].client.has_value()) {
      typename Cache::Storage::PinGuard pin{*entry};
      taken.client = std::move(entry->slots[slot].client);
      entry->slots[slot].client.reset();
    }
    return taken;
  }

  [[nodiscard]] static std::expected<std::size_t, Error> PickSlot(Entry &pool, WorkerBalance balance,
                                                                  std::optional<std::uint64_t> key) {
    if (key.has_value()) {
      const std::size_t slot = Cache::SelectSlot(pool.slots_div, detail::Fnv1a(*key), 0);
      if (uarch::unlikely(slot < pool.slots.size() && pool.slots[slot].state == Cache::SlotState::kLeased))
        return std::unexpected(Error::InvalidConfig("keyed slot is already leased"));
      return slot;
    }
    switch (balance) {
    case WorkerBalance::kLeastInFlight: return LeastInFlight(pool);
    case WorkerBalance::kRoundRobin:
    case WorkerBalance::kKeyHash: return RoundRobin(pool);
    }
    return std::unexpected(Error::InvalidConfig("invalid worker balance"));
  }

  [[nodiscard]] static std::expected<std::size_t, Error> RoundRobin(Entry &pool) {
    const std::size_t count = pool.slots.size();
    for (std::size_t step = 0; step < count; ++step) {
      const std::size_t slot = Cache::SelectSlot(pool.slots_div, pool.rr, step);
      if (pool.slots[slot].state == Cache::SlotState::kIdle) {
        pool.rr = Cache::SelectSlot(pool.slots_div, slot, 1);
        return slot;
      }
    }
    return std::unexpected(Error::InvalidConfig("all local slots are leased"));
  }

  [[nodiscard]] static std::expected<std::size_t, Error> LeastInFlight(Entry &pool) {
    // Each leased client holds exclusive slot ownership, so idle slots tie at zero load.
    return RoundRobin(pool);
  }

  detail::PoolLifetime lifetime_{};
  std::vector<std::string> endpoints_{};
  WorkerConfig config_{};
  mutable Dial factory_{};
  std::unique_ptr<std::atomic<std::uint64_t>> next_worker_{};
};

/** Drops every registered thread-local client and worker pool on this thread. */
[[nodiscard]] inline std::size_t CloseThreadCaches() { return detail::RunCacheClosers(); }

} // namespace greptimedb
