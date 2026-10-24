/** Leased worker slots. Each OS thread keeps its own pool of clients. */
#pragma once

#include "greptimedb/threading/thread_local_pool.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace greptimedb {

/** How a worker picks a slot on the calling thread. */
enum class WorkerBalance {
  kRoundRobin,
  kLeastInFlight,
  kKeyHash,
};

/** Slot count and placement for one `WorkerClient`. */
struct WorkerConfig {
  /** Clients created on each OS thread. Zero is treated as one. */
  std::size_t slots_per_thread{4};
  WorkerBalance balance{WorkerBalance::kRoundRobin};
  /** Rotate endpoints by `(worker_id + slot) % endpoints.size()`. */
  bool stripe_endpoints{true};

  [[nodiscard]] constexpr std::size_t SlotCount() const noexcept {
    return slots_per_thread == 0 ? 1 : slots_per_thread;
  }
};

/** Index of the endpoint that should be dialed first for this worker slot. */
[[nodiscard]] inline std::size_t StripeShift(std::size_t endpoint_count,
                                             std::uint64_t worker_id,
                                             std::size_t slot) noexcept {
  if (endpoint_count == 0) {
    return 0;
  }
  const auto a = static_cast<std::size_t>(worker_id % endpoint_count);
  const auto b = slot % endpoint_count;
  return a >= endpoint_count - b ? a - (endpoint_count - b) : a + b;
}

/** Rotates `endpoints` so index `(worker_id + slot) % size` is first. */
[[nodiscard]] inline std::vector<std::string>
StripedEndpoints(std::span<const std::string> endpoints,
                 std::uint64_t worker_id, std::size_t slot, bool enabled) {
  std::vector<std::string> rotated(endpoints.begin(), endpoints.end());
  if (!enabled) {
    return rotated;
  }
  const std::size_t shift = StripeShift(rotated.size(), worker_id, slot);
  if (shift != 0) {
    std::rotate(rotated.begin(),
                rotated.begin() + static_cast<std::ptrdiff_t>(shift),
                rotated.end());
  }
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
 * Access and destruction must occur on the leasing thread. Violations terminate
 * before touching the thread-affine client or its cache.
 */
template <typename ClientT> class LeaseGuard {
public:
  LeaseGuard() = default;
  LeaseGuard(const LeaseGuard &) = delete;
  LeaseGuard &operator=(const LeaseGuard &) = delete;
  LeaseGuard(LeaseGuard &&other) noexcept { MoveFrom(other); }
  LeaseGuard &operator=(LeaseGuard &&other) noexcept {
    if (this != &other) {
      other.CheckMoveThread();
      Release();
      MoveFrom(other);
    }
    return *this;
  }
  ~LeaseGuard() { Release(); }

  [[nodiscard]] Result<std::reference_wrapper<ClientT>> TryClient() noexcept {
    if (!AffinityHolds() || !client_)
      return std::unexpected(Error{
          ErrorKind::kStream, "lease is empty or belongs to another thread"});
    return std::ref(*client_);
  }
  [[nodiscard]] Result<std::reference_wrapper<const ClientT>>
  TryClient() const noexcept {
    if (!AffinityHolds() || !client_)
      return std::unexpected(Error{
          ErrorKind::kStream, "lease is empty or belongs to another thread"});
    return std::cref(*client_);
  }
  [[nodiscard]] ClientT &client() {
    if (!AffinityHolds() || !client_)
      std::terminate();
    return *client_;
  }
  [[nodiscard]] const ClientT &client() const {
    if (!AffinityHolds() || !client_)
      std::terminate();
    return *client_;
  }
  [[nodiscard]] ClientT &operator*() { return client(); }
  [[nodiscard]] const ClientT &operator*() const { return client(); }
  [[nodiscard]] ClientT *operator->() { return &client(); }
  [[nodiscard]] const ClientT *operator->() const { return &client(); }
  [[nodiscard]] std::size_t slot() const noexcept { return slot_; }
  [[nodiscard]] bool AffinityHolds() const noexcept {
    return armed_ && detail::ThreadIdentity() == owner_thread_;
  }

private:
  template <typename> friend class WorkerClient;

  void CheckMoveThread() const noexcept {
    if (armed_ && !AffinityHolds())
      std::terminate();
  }

  void MoveFrom(LeaseGuard &other) noexcept {
    other.CheckMoveThread();
    client_ = std::move(other.client_);
    pool_id_ = other.pool_id_;
    generation_ = std::move(other.generation_);
    slot_ = other.slot_;
    owner_thread_ = other.owner_thread_;
    armed_ = std::exchange(other.armed_, false);
    other.client_.reset();
  }

  void Release() noexcept;

  std::optional<ClientT> client_{};
  std::uint64_t pool_id_{0};
  std::weak_ptr<detail::GenerationToken> generation_{};
  std::size_t slot_{0};
  std::uint64_t owner_thread_{};
  bool armed_{false};
};

namespace detail {

template <typename ClientT> struct WorkerCache {
  enum class SlotState { kIdle, kLeased };

  struct Slot {
    SlotState state{SlotState::kIdle};
    std::optional<ClientT> client{};
  };

  struct Entry {
    std::weak_ptr<OwnerToken> owner{};
    std::shared_ptr<GenerationToken> generation{};
    std::uint64_t worker_id{0};
    std::vector<Slot> slots{};
    std::size_t rr{0};
  };

  using Storage = CacheStorage<std::shared_ptr<Entry>>;
  static bool Closing() { return Storage::Closing(); }
  static auto &Map() { return Storage::Map(); }

  static std::uint64_t NextId() {
    static std::atomic<std::uint64_t> next{1};
    return AllocateIdentity(next);
  }

  static std::size_t Clear() {
    if (Closing())
      return 0;
    auto &entries = Map();
    typename Storage::MapType removed;
    removed.swap(entries);
    return removed.size();
  }

  static void EnsureRegistered() {
    static thread_local CacheCloser closer{&Clear};
    static thread_local const bool registered =
        (RegisterCacheCloser(closer), true);
    (void)registered;
  }

  static void Return(std::uint64_t pool_id,
                     const std::weak_ptr<GenerationToken> &generation,
                     std::size_t slot, std::optional<ClientT> &client) {
    if (Closing()) {
      client.reset();
      return;
    }
    auto &entries = Map();
    const auto found = entries.find(pool_id);
    if (found == entries.end()) {
      client.reset();
      return;
    }
    const auto mine = generation.lock();
    if (mine == nullptr || mine.get() != found->second->generation.get()) {
      client.reset();
      return;
    }
    if (found->second->owner.expired()) {
      auto removed = entries.extract(found);
      client.reset();
      return;
    }
    auto entry = found->second;
    if (slot < entry->slots.size()) {
      entry->slots[slot].client = std::move(client);
      entry->slots[slot].state = SlotState::kIdle;
    }
    client.reset();
  }

  static void ReleaseEmpty(std::uint64_t pool_id,
                           const std::weak_ptr<GenerationToken> &generation,
                           std::size_t slot) {
    if (Closing())
      return;
    auto &entries = Map();
    const auto found = entries.find(pool_id);
    if (found == entries.end()) {
      return;
    }
    auto mine = generation.lock();
    if (!mine || mine.get() != found->second->generation.get())
      return;
    auto entry = found->second;
    if (slot < entry->slots.size()) {
      entry->slots[slot].client.reset();
      entry->slots[slot].state = SlotState::kIdle;
    }
  }
};

[[nodiscard]] inline std::uint64_t Fnv1a(std::uint64_t key) noexcept {
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
  if (!armed_) {
    return;
  }
  armed_ = false;
  if (detail::ThreadIdentity() != owner_thread_)
    std::terminate();
  detail::WorkerCache<ClientT>::Return(pool_id_, generation_, slot_, client_);
}

/**
 * Shared factory for per-thread leased clients.
 *
 * Slot `s` on worker `w` dials endpoints rotated by `(w + s) % size` when
 * striping is enabled. The guard releases its slot when destroyed.
 * The pool must outlive active member calls. Cached clients may outlive the
 * pool and are destroyed on their owning OS thread at pruning or thread exit.
 * The factory must support concurrent const invocations on caller threads.
 */
template <typename ClientT> class WorkerClient {
public:
  WorkerClient(const WorkerClient &) = delete;
  WorkerClient &operator=(const WorkerClient &) = delete;
  WorkerClient(WorkerClient &&other) noexcept
      : id_(std::exchange(other.id_, 0)),
        endpoints_(std::move(other.endpoints_)), config_(other.config_),
        factory_(std::move(other.factory_)),
        next_worker_(std::move(other.next_worker_)),
        lifetime_(std::move(other.lifetime_)),
        failover_(std::move(other.failover_)) {}
  WorkerClient &operator=(WorkerClient &&other) noexcept {
    if (this != &other) {
      if (id_ != 0) {
        CloseCurrentThread();
      }
      id_ = std::exchange(other.id_, 0);
      endpoints_ = std::move(other.endpoints_);
      config_ = other.config_;
      factory_ = std::move(other.factory_);
      next_worker_ = std::move(other.next_worker_);
      lifetime_ = std::move(other.lifetime_);
      failover_ = std::move(other.failover_);
    }
    return *this;
  }
  ~WorkerClient() {
    if (id_ != 0) {
      CloseCurrentThread();
    }
  }

  template <typename FactoryFn>
  [[nodiscard]] static std::expected<WorkerClient, Error>
  Create(std::vector<std::string> endpoints, WorkerConfig config,
         FactoryFn factory) {
    bool valid_balance = false;
    switch (config.balance) {
    case WorkerBalance::kRoundRobin:
    case WorkerBalance::kLeastInFlight:
    case WorkerBalance::kKeyHash:
      valid_balance = true;
      break;
    }
    if (!valid_balance ||
        config.SlotCount() > std::vector<typename Cache::Slot>{}.max_size())
      return std::unexpected(Error::InvalidConfig("invalid worker settings"));
    detail::WorkerCache<ClientT>::EnsureRegistered();
    WorkerClient pool;
    pool.id_ = detail::WorkerCache<ClientT>::NextId();
    if (pool.id_ == 0)
      return std::unexpected(Error::InvalidConfig("pool identity exhausted"));
    pool.endpoints_ = std::move(endpoints);
    pool.config_ = config;
    pool.factory_ = Dial([fn = std::move(factory)](
                             const std::vector<std::string> &names,
                             std::uint64_t worker, std::size_t slot,
                             const FailoverConfig *policy) -> Result<ClientT> {
      if constexpr (std::is_invocable_r_v<Result<ClientT>, const FactoryFn &,
                                          const std::vector<std::string> &,
                                          std::uint64_t, std::size_t,
                                          const FailoverConfig *>)
        return fn(names, worker, slot, policy);
      else
        return fn(names, worker, slot);
    });
    pool.next_worker_ = std::make_shared<std::atomic<std::uint64_t>>(0);
    pool.lifetime_ = std::make_shared<detail::OwnerToken>();
    return pool;
  }

  template <typename FactoryFn, typename Policy>
  [[nodiscard]] static std::expected<WorkerClient, Error>
  Create(std::vector<std::string> endpoints, WorkerConfig config,
         FactoryFn factory, const Policy &policy) {
    if (policy.has_observer()) {
      return std::unexpected(Error::InvalidConfig(
          "failover observers are thread-affine and cannot be shared"));
    }
    if constexpr (!std::is_invocable_r_v<Result<ClientT>, const FactoryFn &,
                                         const std::vector<std::string> &,
                                         std::uint64_t, std::size_t,
                                         const FailoverConfig *>)
      return std::unexpected(
          Error::InvalidConfig("factory must accept failover settings"));
    if (policy.len() != endpoints.size())
      return std::unexpected(Error::InvalidConfig(
          "policy endpoint count differs from client options"));
    auto pool = Create(std::move(endpoints), config, std::move(factory));
    if (pool.has_value()) {
      pool->failover_ = policy.config();
      pool->failover_->endpoints = pool->endpoints_;
    }
    return pool;
  }

  [[nodiscard]] const WorkerConfig &config() const noexcept { return config_; }
  [[nodiscard]] const FailoverConfig *failover_config() const noexcept {
    return failover_.has_value() ? &*failover_ : nullptr;
  }

  /** Endpoint order a slot would dial, without connecting. */
  [[nodiscard]] std::vector<std::string>
  PreviewEndpoints(std::uint64_t worker_id, std::size_t slot) const {
    return StripedEndpoints(endpoints_, worker_id, slot,
                            config_.stripe_endpoints);
  }

  [[nodiscard]] std::expected<LeaseGuard<ClientT>, Error> Lease() const {
    return LeaseSlot(std::nullopt);
  }

  [[nodiscard]] std::expected<LeaseGuard<ClientT>, Error>
  LeaseByKey(std::uint64_t key) const {
    return LeaseSlot(key);
  }

  template <typename Fn>
  auto WithClient(Fn &&fn) const -> std::expected<
      typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type,
      Error> {
    auto guard = Lease();
    if (!guard.has_value()) {
      return std::unexpected(std::move(guard.error()));
    }
    if constexpr (std::is_void_v<std::invoke_result_t<Fn, ClientT &>>) {
      std::forward<Fn>(fn)(guard->client());
      return {};
    } else {
      return std::forward<Fn>(fn)(guard->client());
    }
  }

  template <typename Fn>
  auto WithClientByKey(std::uint64_t key, Fn &&fn) const -> std::expected<
      typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type,
      Error> {
    auto guard = LeaseByKey(key);
    if (!guard.has_value()) {
      return std::unexpected(std::move(guard.error()));
    }
    if constexpr (std::is_void_v<std::invoke_result_t<Fn, ClientT &>>) {
      std::forward<Fn>(fn)(guard->client());
      return {};
    } else {
      return std::forward<Fn>(fn)(guard->client());
    }
  }

  bool CloseCurrentThread() const { return Prune(id_, false).second; }

  static std::size_t CloseThreadCaches() {
    return detail::WorkerCache<ClientT>::Clear();
  }

private:
  using Cache = detail::WorkerCache<ClientT>;
  using Entry = typename Cache::Entry;
  using Dial =
      grpcxx::InplaceDelegate<Result<ClientT>(const std::vector<std::string> &,
                                              std::uint64_t, std::size_t,
                                              const FailoverConfig *),
                              48>;

  WorkerClient() = default;

  [[nodiscard]] std::expected<LeaseGuard<ClientT>, Error>
  LeaseSlot(std::optional<std::uint64_t> key) const {
    Cache::EnsureRegistered();
    Prune(std::nullopt, false);
    auto taken = TakeSlot(key);
    if (!taken.has_value()) {
      return std::unexpected(std::move(taken.error()));
    }
    auto [worker_id, slot, existing, generation] = std::move(*taken);
    if (!existing.has_value()) {
      if (!factory_) {
        Cache::ReleaseEmpty(id_, generation, slot);
        return std::unexpected(Error::InvalidConfig("worker factory is empty"));
      }
      const std::vector<std::string> dial = StripedEndpoints(
          endpoints_, worker_id, slot, config_.stripe_endpoints);
      auto snapshot = failover_;
      if (snapshot)
        snapshot->endpoints = dial;
      auto created =
          factory_(dial, worker_id, slot, snapshot ? &*snapshot : nullptr);
      if (!created.has_value()) {
        Cache::ReleaseEmpty(id_, generation, slot);
        return std::unexpected(std::move(created.error()));
      }
      existing = std::move(*created);
    }
    LeaseGuard<ClientT> guard;
    guard.client_ = std::move(existing);
    guard.pool_id_ = id_;
    guard.generation_ = std::move(generation);
    guard.slot_ = slot;
    guard.owner_thread_ = detail::ThreadIdentity();
    guard.armed_ = true;
    return guard;
  }

  struct Taken {
    std::uint64_t worker_id{0};
    std::size_t slot{0};
    std::optional<ClientT> client{};
    std::weak_ptr<detail::GenerationToken> generation{};
  };

  [[nodiscard]] std::expected<Taken, Error>
  TakeSlot(std::optional<std::uint64_t> key) const {
    if (Cache::Closing())
      return std::unexpected(
          Error::InvalidConfig("thread-local storage is closing"));
    auto &entries = Cache::Map();
    auto found = entries.find(id_);
    if (found == entries.end()) {
      if (lifetime_ == nullptr || next_worker_ == nullptr) {
        return std::unexpected(Error::InvalidConfig("worker pool is empty"));
      }
      auto created = std::make_shared<Entry>();
      created->owner = lifetime_;
      created->generation = std::make_shared<detail::GenerationToken>();
      created->worker_id =
          next_worker_->fetch_add(1, std::memory_order_relaxed);
      created->slots.resize(config_.SlotCount());
      found = entries.emplace(id_, std::move(created)).first;
    }
    auto entry = found->second;
    auto picked = PickSlot(*entry, config_.balance, key);
    if (!picked.has_value()) {
      return std::unexpected(std::move(picked.error()));
    }
    const std::size_t slot = *picked;
    Taken taken;
    taken.worker_id = entry->worker_id;
    taken.slot = slot;
    entry->slots[slot].state = Cache::SlotState::kLeased;
    taken.client = std::move(entry->slots[slot].client);
    entry->slots[slot].client.reset();
    taken.generation = entry->generation;
    return taken;
  }

  static std::expected<std::size_t, Error>
  PickSlot(Entry &pool, WorkerBalance balance,
           std::optional<std::uint64_t> key) {
    const std::size_t count = std::max<std::size_t>(pool.slots.size(), 1);
    if (key.has_value()) {
      const std::size_t slot = static_cast<std::size_t>(
          detail::Fnv1a(*key) % static_cast<std::uint64_t>(count));
      if (slot < pool.slots.size() &&
          pool.slots[slot].state == Cache::SlotState::kLeased) {
        return std::unexpected(
            Error::InvalidConfig("keyed slot is already leased"));
      }
      return slot;
    }
    switch (balance) {
    case WorkerBalance::kLeastInFlight:
      return LeastInFlight(pool);
    case WorkerBalance::kRoundRobin:
    case WorkerBalance::kKeyHash:
      return RoundRobin(pool);
    }
    return std::unexpected(Error::InvalidConfig("invalid worker balance"));
  }

  static std::expected<std::size_t, Error> RoundRobin(Entry &pool) {
    const std::size_t count = pool.slots.size();
    for (std::size_t step = 0; step < count; ++step) {
      const std::size_t slot = (pool.rr + step) % count;
      if (pool.slots[slot].state == Cache::SlotState::kIdle) {
        pool.rr = (slot + 1) % count;
        return slot;
      }
    }
    return std::unexpected(Error::InvalidConfig("all local slots are leased"));
  }

  static std::expected<std::size_t, Error> LeastInFlight(Entry &pool) {
    // A client is exclusively leased, so every available slot has zero load.
    // Rotating idle slots gives fair ties without a second, fallible counter.
    return RoundRobin(pool);
  }

  static std::pair<std::size_t, bool> Prune(std::optional<std::uint64_t> id,
                                            bool all) {
    if (Cache::Closing())
      return {0, false};
    auto &entries = Cache::Map();
    std::vector<std::uint64_t> keys;
    for (const auto &[key, entry] : entries) {
      if (all || id == key || entry->owner.expired()) {
        keys.push_back(key);
      }
    }
    std::vector<typename std::remove_reference_t<decltype(entries)>::node_type>
        removed;
    removed.reserve(keys.size());
    bool found = false;
    for (const std::uint64_t key : keys) {
      found = found || id == key;
      removed.push_back(entries.extract(key));
    }
    return {keys.size(), found};
  }

  std::uint64_t id_{0};
  std::vector<std::string> endpoints_{};
  WorkerConfig config_{};
  mutable Dial factory_{};
  std::shared_ptr<std::atomic<std::uint64_t>> next_worker_{};
  std::shared_ptr<detail::OwnerToken> lifetime_{};
  std::optional<FailoverConfig> failover_{};
};

/** Drops every registered thread-local client and worker pool on this thread.
 */
[[nodiscard]] inline std::size_t CloseThreadCaches() {
  return detail::RunCacheClosers();
}

} // namespace greptimedb
