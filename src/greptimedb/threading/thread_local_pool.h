/**
 * @file thread_local_pool.h
 * @brief One lazily created client per calling OS thread with lock-free pool liveness.
 */
#pragma once

#include "greptimedb/core/error.h"

#include <grpcxx/core/inplace_delegate.h>
#include <uarch/uarch.hpp>

#include <array>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#if (defined(__SANITIZE_ADDRESS__) ||                                                                                  \
     (defined(__has_feature) && (__has_feature(address_sanitizer) || __has_feature(leak_sanitizer)))) &&               \
    __has_include(<sanitizer/lsan_interface.h>)
#include <sanitizer/lsan_interface.h>
#define GREPTIMEDB_HAS_LSAN_INTERFACE 1
#else
#define GREPTIMEDB_HAS_LSAN_INTERFACE 0
#endif

namespace greptimedb::detail {

inline void IgnoreProcessLifetimeLeak([[maybe_unused]] const void *ptr) noexcept {
#if GREPTIMEDB_HAS_LSAN_INTERFACE
  __lsan_ignore_object(ptr);
#endif
}

/** Each thread registers only its own cache types, without cross-thread publication. */
struct CacheCloser {
  std::size_t (*clear)();
  CacheCloser *next{nullptr};
};

[[nodiscard]] inline CacheCloser *&CacheClosers() noexcept {
  static thread_local CacheCloser *head{nullptr};
  return head;
}

inline void RegisterCacheCloser(CacheCloser &closer) noexcept {
  closer.next = CacheClosers();
  CacheClosers() = &closer;
}

[[nodiscard]] inline std::size_t RunCacheClosers() {
  std::size_t removed = 0;
  for (auto *closer = CacheClosers(); closer != nullptr; closer = closer->next)
    removed += closer->clear();
  return removed;
}

/** Zero signals exhaustion; live identities are never recycled. */
[[nodiscard]] inline std::uint64_t AllocateIdentity(std::atomic<std::uint64_t> &next) noexcept {
  auto value = next.load(std::memory_order_relaxed);
  do {
    if (uarch::unlikely(value == std::numeric_limits<std::uint64_t>::max())) return 0;
  } while (!next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed));
  return value;
}

/** Monotonic identities cannot alias a later OS thread after thread-id reuse. */
[[nodiscard]] inline std::uint64_t ThreadIdentity() noexcept {
  static std::atomic<std::uint64_t> next{1};
  static thread_local const std::uint64_t identity = []() noexcept {
    const auto value = AllocateIdentity(next);
    if (uarch::unlikely(value == 0)) std::terminate();
    return value;
  }();
  return identity;
}

/** Monotonic per-thread cache entry generation; zero denotes unarmed state. */
[[nodiscard]] inline std::uint64_t NextGeneration() noexcept {
  static thread_local std::uint64_t next{0};
  if (uarch::unlikely(++next == 0)) std::terminate();
  return next;
}

/**
 * Lock-free O(1) liveness directory indexed by monotonic `pool_id`.
 *
 * Segments are lazily allocated once via CAS and retained for the process
 * lifetime so reader threads probe liveness with two atomic loads.
 */
class LivenessRegistry final {
public:
  static constexpr std::size_t kBlockShift = 12;
  static constexpr std::size_t kBlockSize = 1uz << kBlockShift;
  static constexpr std::size_t kBlockMask = kBlockSize - 1uz;
  static constexpr std::size_t kMaxBlocks = 16384uz;
  static constexpr std::uint64_t kMaxPoolId = static_cast<std::uint64_t>(kMaxBlocks) * kBlockSize - 1ULL;

  struct LivenessBlock {
    std::array<std::atomic<std::uint8_t>, kBlockSize> alive{};
  };

  [[nodiscard]] static std::uint64_t Acquire() noexcept {
    static std::atomic<std::uint64_t> next_pool_id{1};
    const std::uint64_t id = AllocateIdentity(next_pool_id);
    if (uarch::unlikely(id == 0 || id > kMaxPoolId)) return 0;
    const auto block_idx = static_cast<std::size_t>(id >> kBlockShift);
    const auto slot_idx = static_cast<std::size_t>(id & kBlockMask);
    auto &blocks = Blocks();
    auto *block = blocks[block_idx].load(std::memory_order_acquire);
    if (uarch::unlikely(block == nullptr)) {
      auto candidate = std::make_unique<LivenessBlock>();
      auto *raw = candidate.get();
      LivenessBlock *expected = nullptr;
      if (blocks[block_idx].compare_exchange_strong(expected, raw, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
        candidate.release();
        IgnoreProcessLifetimeLeak(raw);
        block = raw;
      } else {
        block = expected;
      }
    }
    block->alive[slot_idx].store(1U, std::memory_order_release);
    return id;
  }

  static void MarkDead(std::uint64_t id) noexcept {
    if (uarch::unlikely(id == 0 || id > kMaxPoolId)) return;
    const auto block_idx = static_cast<std::size_t>(id >> kBlockShift);
    const auto slot_idx = static_cast<std::size_t>(id & kBlockMask);
    if (auto *block = Blocks()[block_idx].load(std::memory_order_acquire); block != nullptr)
      block->alive[slot_idx].store(0U, std::memory_order_release);
  }

  [[nodiscard]] static bool IsAlive(std::uint64_t id) noexcept {
    if (uarch::unlikely(id == 0 || id > kMaxPoolId)) return false;
    const auto block_idx = static_cast<std::size_t>(id >> kBlockShift);
    const auto slot_idx = static_cast<std::size_t>(id & kBlockMask);
    const auto *block = Blocks()[block_idx].load(std::memory_order_acquire);
    return block != nullptr && block->alive[slot_idx].load(std::memory_order_acquire) != 0U;
  }

private:
  [[nodiscard]] static std::array<std::atomic<LivenessBlock *>, kMaxBlocks> &Blocks() noexcept {
    static std::array<std::atomic<LivenessBlock *>, kMaxBlocks> blocks{};
    return blocks;
  }
};

/** Move-only RAII handle owning one pool liveness slot in `LivenessRegistry`. */
class PoolLifetime final {
public:
  constexpr PoolLifetime() noexcept = default;
  PoolLifetime(const PoolLifetime &) = delete("PoolLifetime owns a unique pool liveness slot; move instead");
  PoolLifetime &operator=(const PoolLifetime &) = delete("PoolLifetime owns a unique pool liveness slot; move instead");
  PoolLifetime(PoolLifetime &&other) noexcept : id_(std::exchange(other.id_, 0)) {}
  PoolLifetime &operator=(PoolLifetime &&other) noexcept {
    if (this != &other) {
      Reset();
      id_ = std::exchange(other.id_, 0);
    }
    return *this;
  }
  ~PoolLifetime() noexcept { Reset(); }

  [[nodiscard]] static PoolLifetime Acquire() noexcept { return PoolLifetime{LivenessRegistry::Acquire()}; }
  void Reset() noexcept {
    if (id_ != 0) LivenessRegistry::MarkDead(std::exchange(id_, 0));
  }
  [[nodiscard]] constexpr std::uint64_t Id() const noexcept { return id_; }
  [[nodiscard]] constexpr explicit operator bool() const noexcept { return id_ != 0; }

private:
  explicit constexpr PoolLifetime(std::uint64_t id) noexcept : id_(id) {}
  std::uint64_t id_{0};
};

template <typename T> struct UnwrapResult {
  using type = T;
};
template <typename T> struct UnwrapResult<std::expected<T, Error>> {
  using type = T;
};

template <typename Fn, typename ClientT>
auto InvokeWithClient(Fn &&fn, ClientT &client)
    -> std::expected<typename UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type, Error> {
  if constexpr (std::is_void_v<std::invoke_result_t<Fn, ClientT &>>) {
    std::invoke(std::forward<Fn>(fn), client);
    return {};
  } else {
    return std::invoke(std::forward<Fn>(fn), client);
  }
}

template <typename P>
concept PolicyConfigProvider =
    (requires(const P &p) { p.Config(); } || requires(const P &p) { p.config(); }) &&
    (requires(const P &p) { requires std::convertible_to<decltype(p.Size()), std::size_t>; } ||
     requires(const P &p) { requires std::convertible_to<decltype(p.size()), std::size_t>; } ||
     requires(const P &p) { requires std::convertible_to<decltype(p.len()), std::size_t>; });

template <PolicyConfigProvider P> [[nodiscard]] constexpr bool PolicyHasObserver(const P &p) noexcept {
  if constexpr (requires { requires std::convertible_to<decltype(p.HasObserver()), bool>; })
    return static_cast<bool>(p.HasObserver());
  else if constexpr (requires { requires std::convertible_to<decltype(p.has_observer()), bool>; })
    return static_cast<bool>(p.has_observer());
  else if constexpr (requires { static_cast<bool>(p.Config().observer); })
    return static_cast<bool>(p.Config().observer);
  else
    return false;
}

template <PolicyConfigProvider P> [[nodiscard]] constexpr std::size_t PolicyEndpointCount(const P &p) noexcept {
  if constexpr (requires { requires std::convertible_to<decltype(p.Size()), std::size_t>; })
    return static_cast<std::size_t>(p.Size());
  else if constexpr (requires { requires std::convertible_to<decltype(p.size()), std::size_t>; })
    return static_cast<std::size_t>(p.size());
  else
    return static_cast<std::size_t>(p.len());
}

template <PolicyConfigProvider P> [[nodiscard]] constexpr auto ExtractPolicySnapshot(const P &p) {
  if constexpr (requires { p.Config(); })
    return auto(p.Config());
  else
    return auto(p.config());
}

template <PolicyConfigProvider P> using PolicySnapshotOf = decltype(ExtractPolicySnapshot(std::declval<const P &>()));

} // namespace greptimedb::detail

#include "greptimedb/threading/thread_cache.h"

namespace greptimedb::detail {

template <typename ClientT> struct ThreadCache {
  struct Entry {
    std::uint64_t generation{0};
    std::optional<ClientT> client{};
    std::uint32_t pins{0};
    bool checked_out{true};
    bool retired{false};
  };

  using Storage = CacheStorage<Entry, LivenessRegistry>;
  [[nodiscard]] static bool Closing() noexcept { return Storage::Closing(); }
  static std::size_t Clear() { return Storage::Prune(std::nullopt, true).first; }
  static void EnsureRegistered() noexcept {
    static thread_local CacheCloser closer{.clear = &Clear};
    [[maybe_unused]] static thread_local const bool registered = (RegisterCacheCloser(closer), true);
  }
};

} // namespace greptimedb::detail

namespace greptimedb {

/**
 * Creates and reuses one `ClientT` on each OS thread that calls `WithClient`.
 *
 * The pool must outlive active member calls. A `const Snapshot*` passed into
 * the factory is valid only for that call. Cached clients that outlive the
 * pool are dropped on their owning thread or at thread exit.
 */
template <typename ClientT, typename FactoryFn> class ThreadLocalClient {
public:
  ThreadLocalClient(const ThreadLocalClient &) = delete("ThreadLocalClient owns a unique pool identity; move instead");
  ThreadLocalClient &
  operator=(const ThreadLocalClient &) = delete("ThreadLocalClient owns a unique pool identity; move instead");

  ThreadLocalClient(ThreadLocalClient &&other) noexcept
      : lifetime_(std::move(other.lifetime_)), endpoints_(std::move(other.endpoints_)),
        factory_(std::move(other.factory_)), policy_invoker_(std::move(other.policy_invoker_)) {}

  ThreadLocalClient &operator=(ThreadLocalClient &&other) noexcept {
    if (this != &other) {
      ReleaseCurrent();
      lifetime_ = std::move(other.lifetime_);
      endpoints_ = std::move(other.endpoints_);
      factory_.reset();
      if (other.factory_) factory_.emplace(std::move(*other.factory_));
      policy_invoker_ = std::move(other.policy_invoker_);
    }
    return *this;
  }

  ~ThreadLocalClient() { ReleaseCurrent(); }

  [[nodiscard]] static std::expected<ThreadLocalClient, Error> Create(std::vector<std::string> endpoints,
                                                                      FactoryFn factory) {
    detail::ThreadCache<ClientT>::EnsureRegistered();
    ThreadLocalClient pool;
    pool.lifetime_ = detail::PoolLifetime::Acquire();
    if (uarch::unlikely(!pool.lifetime_)) return std::unexpected(Error::InvalidConfig("pool identity exhausted"));
    pool.endpoints_ = std::move(endpoints);
    pool.factory_.emplace(std::move(factory));
    return pool;
  }

  /** Forwards `const Snapshot*` for the duration of one factory call per thread. */
  template <detail::PolicyConfigProvider Policy>
  [[nodiscard]] static std::expected<ThreadLocalClient, Error> Create(std::vector<std::string> endpoints,
                                                                      FactoryFn factory, const Policy &policy) {
    using Snapshot = detail::PolicySnapshotOf<Policy>;
    if (detail::PolicyHasObserver(policy))
      return std::unexpected(Error::InvalidConfig("failover observers are thread-affine and cannot be shared"));
    if constexpr (!std::is_invocable_r_v<Result<ClientT>, const FactoryFn &, const std::vector<std::string> &,
                                         const Snapshot *>) {
      return std::unexpected(Error::InvalidConfig("factory must accept failover settings"));
    } else {
      if (detail::PolicyEndpointCount(policy) != endpoints.size())
        return std::unexpected(Error::InvalidConfig("policy endpoint count differs from client options"));
      auto pool = Create(std::move(endpoints), std::move(factory));
      if (!pool.has_value()) return pool;
      Snapshot snapshot = detail::ExtractPolicySnapshot(policy);
      if constexpr (requires { snapshot.endpoints = pool->endpoints_; }) snapshot.endpoints = pool->endpoints_;
      auto owned_snapshot = std::make_unique<const Snapshot>(std::move(snapshot));
      if (uarch::unlikely(!owned_snapshot))
        return std::unexpected(Error::InvalidConfig("policy snapshot allocation failed"));
      pool->policy_invoker_ =
          PolicyInvoker([snap = std::move(owned_snapshot)](const FactoryFn &fn,
                                                           const std::vector<std::string> &names) -> Result<ClientT> {
            return fn(names, snap.get());
          });
      return pool;
    }
  }

  /** Runs `fn` on this thread's client, creating it on first use. */
  template <typename Fn>
  auto WithClient(Fn &&fn) const
      -> std::expected<typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type, Error> {
    const std::uint64_t pool_id = lifetime_.Id();
    if (uarch::unlikely(pool_id == 0)) return std::unexpected(Error::InvalidConfig("thread-local factory is empty"));
    detail::ThreadCache<ClientT>::EnsureRegistered();
    auto checkout = Enter(pool_id);
    if (uarch::unlikely(!checkout.has_value())) return std::unexpected(std::move(checkout.error()));
    if (!checkout->client.has_value()) {
      if (uarch::unlikely(!factory_.has_value()))
        return std::unexpected(Error::InvalidConfig("thread-local factory is empty"));
      auto created = [&]() -> Result<ClientT> {
        if (policy_invoker_) return policy_invoker_(*factory_, endpoints_);
        if constexpr (std::is_invocable_r_v<Result<ClientT>, const FactoryFn &, const std::vector<std::string> &>)
          return (*factory_)(endpoints_);
        else
          return std::unexpected(Error::InvalidConfig("policy-aware factory requires policy configuration"));
      }();
      if (uarch::unlikely(!created.has_value())) return std::unexpected(std::move(created.error()));
      checkout->client = std::move(*created);
    }
    if (uarch::unlikely(!checkout->client.has_value()))
      return std::unexpected(Error::InvalidConfig("reentrant client call on this thread"));
    return detail::InvokeWithClient(std::forward<Fn>(fn), *checkout->client);
  }

  /** Drops this factory's cached client on the calling thread. */
  bool CloseCurrentThread() const {
    const std::uint64_t pool_id = lifetime_.Id();
    return pool_id != 0 && detail::ThreadCache<ClientT>::Storage::Prune(pool_id, false).second;
  }

  [[nodiscard]] const std::vector<std::string> &Endpoints() const noexcept { return endpoints_; }
  static std::size_t CloseThreadCaches() { return detail::ThreadCache<ClientT>::Clear(); }

private:
  using PolicyInvoker =
      grpcxx::InplaceDelegate<Result<ClientT>(const FactoryFn &, const std::vector<std::string> &), 16>;

  class Checkout final {
  public:
    std::optional<ClientT> client{};
    Checkout(std::uint64_t id, std::uint64_t generation) noexcept : id_(id), generation_(generation), armed_(true) {}
    Checkout(const Checkout &) = delete("Checkout holds an exclusive thread-local client lease; move instead");
    Checkout &
    operator=(const Checkout &) = delete("Checkout holds an exclusive thread-local client lease; move instead");
    Checkout(Checkout &&other) noexcept { MoveFrom(other); }
    Checkout &operator=(Checkout &&other) noexcept {
      if (this != &other) {
        Release();
        MoveFrom(other);
      }
      return *this;
    }
    ~Checkout() { Release(); }

  private:
    void MoveFrom(Checkout &other) noexcept {
      client = std::move(other.client);
      id_ = other.id_;
      generation_ = std::exchange(other.generation_, 0);
      armed_ = std::exchange(other.armed_, false);
    }

    void Release() noexcept {
      if (!armed_) return;
      armed_ = false;
      using Cache = detail::ThreadCache<ClientT>;
      using Storage = typename Cache::Storage;
      if (uarch::unlikely(Cache::Closing())) {
        client.reset();
        return;
      }
      auto &local = Storage::State();
      auto *entry = Storage::Find(local, id_);
      if (entry == nullptr || generation_ == 0 || entry->generation != generation_) {
        client.reset();
        return;
      }
      if (client.has_value() && detail::LivenessRegistry::IsAlive(id_)) {
        typename Storage::PinGuard pin{*entry};
        entry->client = std::move(client);
        if (!entry->retired) entry->checked_out = false;
      } else {
        Storage::Prune(id_, false);
      }
      client.reset();
    }

    std::uint64_t id_{0};
    std::uint64_t generation_{0};
    bool armed_{false};
  };

  ThreadLocalClient() = default;

  void ReleaseCurrent() {
    if (lifetime_) {
      CloseCurrentThread();
      lifetime_.Reset();
    }
  }

  [[nodiscard]] static std::expected<Checkout, Error> Enter(std::uint64_t id) {
    using Cache = detail::ThreadCache<ClientT>;
    using Storage = typename Cache::Storage;
    if (uarch::unlikely(Cache::Closing()))
      return std::unexpected(Error::InvalidConfig("thread-local storage is closing"));
    auto &local = Storage::State();
    auto *entry = Storage::Find(local, id);
    if (entry == nullptr) {
      auto slot = Storage::Reserve(local, id);
      if (uarch::unlikely(!slot)) return std::unexpected(Error::InvalidConfig("per-thread pool capacity exhausted"));
      auto created = std::make_unique<typename Cache::Entry>();
      created->generation = detail::NextGeneration();
      created->checked_out = true;
      const std::uint64_t generation = created->generation;
      Storage::Commit(local, *slot, id, std::move(created));
      return Checkout{id, generation};
    }
    if (uarch::unlikely(entry->checked_out || !entry->client.has_value()))
      return std::unexpected(Error::InvalidConfig("reentrant client call on this thread"));
    entry->checked_out = true;
    Checkout checkout{id, entry->generation};
    typename Storage::PinGuard pin{*entry};
    checkout.client = std::move(*entry->client);
    entry->client.reset();
    return checkout;
  }

  detail::PoolLifetime lifetime_{};
  std::vector<std::string> endpoints_{};
  std::optional<FactoryFn> factory_{};
  mutable PolicyInvoker policy_invoker_{};
};

} // namespace greptimedb
