/** One client per calling OS thread. The factory object itself may be shared.
 */
#pragma once

#include "greptimedb/failover/failover.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace greptimedb::detail {

/** Each thread registers only its own cache types, without cross-thread
 * publication. */
struct CacheCloser {
  std::size_t (*clear)();
  CacheCloser *next{};
};

inline CacheCloser *&CacheClosers() {
  static thread_local CacheCloser *head{};
  return head;
}

inline void RegisterCacheCloser(CacheCloser &closer) noexcept {
  closer.next = CacheClosers();
  CacheClosers() = &closer;
}

inline std::size_t RunCacheClosers() {
  std::size_t removed{};
  for (auto *closer = CacheClosers(); closer != nullptr; closer = closer->next)
    removed += closer->clear();
  return removed;
}

/** Zero signals exhaustion; live identities are never recycled. */
inline std::uint64_t
AllocateIdentity(std::atomic<std::uint64_t> &next) noexcept {
  auto value = next.load(std::memory_order_relaxed);
  do {
    if (value == std::numeric_limits<std::uint64_t>::max())
      return 0;
  } while (
      !next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed));
  return value;
}

/** Monotonic identities cannot alias a later OS thread after thread-id reuse.
 */
inline std::uint64_t ThreadIdentity() noexcept {
  static std::atomic<std::uint64_t> next{1};
  static thread_local const std::uint64_t identity = [] {
    const auto value = AllocateIdentity(next);
    if (value == 0)
      std::terminate();
    return value;
  }();
  return identity;
}

struct OwnerToken {};
struct GenerationToken {};

template <typename T> struct UnwrapResult {
  using type = T;
  static constexpr bool nested = false;
};

template <typename T> struct UnwrapResult<std::expected<T, Error>> {
  using type = T;
  static constexpr bool nested = true;
};

/** Keep teardown reentrancy from resurrecting storage whose destruction began.
 */
template <typename Entry> struct CacheStorage {
  using MapType = std::unordered_map<std::uint64_t, Entry>;
  static bool &Closing() {
    static thread_local bool closing{};
    return closing;
  }
  struct Local {
    MapType entries;
    ~Local() { Closing() = true; }
  };
  static MapType &Map() {
    static thread_local Local local;
    return local.entries;
  }
};

template <typename ClientT> struct ThreadCache {
  struct Entry {
    std::weak_ptr<OwnerToken> owner{};
    std::shared_ptr<GenerationToken> generation{};
    std::optional<ClientT> client{};
    bool checked_out{true};
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
};

} // namespace greptimedb::detail

namespace greptimedb {

/**
 * Creates and reuses one `ClientT` on each OS thread that calls `WithClient`.
 *
 * Cached clients live in storage owned by that thread. Factories of the same
 * `ClientT` share the cache map and are distinguished by pool id.
 * The pool must outlive active member calls. Cached clients may outlive the
 * pool and are destroyed on their owning OS thread at pruning or thread exit.
 * The factory must support concurrent invocations on different caller threads.
 */
template <typename ClientT, typename FactoryFn> class ThreadLocalClient {
public:
  ThreadLocalClient(const ThreadLocalClient &) = delete;
  ThreadLocalClient &operator=(const ThreadLocalClient &) = delete;
  ThreadLocalClient(ThreadLocalClient &&other) noexcept
      : id_(std::exchange(other.id_, 0)),
        endpoints_(std::move(other.endpoints_)),
        factory_(std::move(other.factory_)),
        lifetime_(std::move(other.lifetime_)),
        failover_(std::move(other.failover_)) {}
  ThreadLocalClient &operator=(ThreadLocalClient &&other) noexcept {
    if (this != &other) {
      if (id_ != 0) {
        CloseCurrentThread();
      }
      id_ = std::exchange(other.id_, 0);
      endpoints_ = std::move(other.endpoints_);
      factory_.reset();
      if (other.factory_)
        factory_.emplace(std::move(*other.factory_));
      lifetime_ = std::move(other.lifetime_);
      failover_ = std::move(other.failover_);
    }
    return *this;
  }
  ~ThreadLocalClient() {
    if (id_ != 0) {
      CloseCurrentThread();
    }
  }

  [[nodiscard]] static std::expected<ThreadLocalClient, Error>
  Create(std::vector<std::string> endpoints, FactoryFn factory) {
    detail::ThreadCache<ClientT>::EnsureRegistered();
    ThreadLocalClient pool;
    pool.id_ = detail::ThreadCache<ClientT>::NextId();
    if (pool.id_ == 0)
      return std::unexpected(Error::InvalidConfig("pool identity exhausted"));
    pool.endpoints_ = std::move(endpoints);
    pool.factory_.emplace(std::move(factory));
    pool.lifetime_ = std::make_shared<detail::OwnerToken>();
    return pool;
  }

  /** Rejects observers and factories that cannot receive the immutable policy
   * snapshot. Policy-aware factories accept (endpoints, const FailoverConfig*).
   */
  template <typename Policy>
  [[nodiscard]] static std::expected<ThreadLocalClient, Error>
  Create(std::vector<std::string> endpoints, FactoryFn factory,
         const Policy &policy) {
    if (policy.has_observer()) {
      return std::unexpected(Error::InvalidConfig(
          "failover observers are thread-affine and cannot be shared"));
    }
    if constexpr (!std::is_invocable_r_v<Result<ClientT>, const FactoryFn &,
                                         const std::vector<std::string> &,
                                         const FailoverConfig *>)
      return std::unexpected(
          Error::InvalidConfig("factory must accept failover settings"));
    if (policy.len() != endpoints.size())
      return std::unexpected(Error::InvalidConfig(
          "policy endpoint count differs from client options"));
    auto pool = Create(std::move(endpoints), std::move(factory));
    if (!pool.has_value()) {
      return pool;
    }
    pool->failover_ = policy.config();
    pool->failover_->endpoints = pool->endpoints_;
    return pool;
  }

  /**
   * Runs `fn` on this thread's client, creating it on first use.
   *
   * A nested call on this pool while `fn` is running returns `kInvalidConfig`.
   * A factory failure leaves no cached client.
   */
  template <typename Fn>
  auto WithClient(Fn &&fn) const -> std::expected<
      typename detail::UnwrapResult<std::invoke_result_t<Fn, ClientT &>>::type,
      Error> {
    using Raw = std::invoke_result_t<Fn, ClientT &>;
    if (id_ == 0 || !lifetime_)
      return std::unexpected(
          Error::InvalidConfig("thread-local factory is empty"));
    detail::ThreadCache<ClientT>::EnsureRegistered();
    Prune(std::nullopt, false);
    auto checkout = Enter(id_, lifetime_);
    if (!checkout.has_value()) {
      return std::unexpected(std::move(checkout.error()));
    }
    if (!checkout->client.has_value()) {
      if (!factory_.has_value()) {
        return std::unexpected(
            Error::InvalidConfig("thread-local factory is empty"));
      }
      auto created = [&]() -> Result<ClientT> {
        if constexpr (std::is_invocable_r_v<Result<ClientT>, const FactoryFn &,
                                            const std::vector<std::string> &,
                                            const FailoverConfig *>)
          return (*factory_)(endpoints_, failover_config());
        else
          return (*factory_)(endpoints_);
      }();
      if (!created.has_value()) {
        return std::unexpected(std::move(created.error()));
      }
      checkout->client = std::move(*created);
    }
    if (!checkout->client.has_value()) {
      return std::unexpected(
          Error::InvalidConfig("reentrant client call on this thread"));
    }
    if constexpr (detail::UnwrapResult<Raw>::nested) {
      return std::forward<Fn>(fn)(*checkout->client);
    } else if constexpr (std::is_void_v<Raw>) {
      std::forward<Fn>(fn)(*checkout->client);
      return {};
    } else {
      return std::forward<Fn>(fn)(*checkout->client);
    }
  }

  /** Drops this factory's cached client on the calling thread. */
  bool CloseCurrentThread() const { return Prune(id_, false).second; }

  [[nodiscard]] const FailoverConfig *failover_config() const noexcept {
    return failover_.has_value() ? &*failover_ : nullptr;
  }

  static std::size_t CloseThreadCaches() {
    return detail::ThreadCache<ClientT>::Clear();
  }

private:
  class Checkout {
  public:
    std::optional<ClientT> client{};

    Checkout() = default;
    Checkout(const Checkout &) = delete;
    Checkout &operator=(const Checkout &) = delete;
    Checkout(Checkout &&other) noexcept { MoveFrom(other); }
    Checkout &operator=(Checkout &&other) noexcept {
      if (this != &other) {
        Release();
        MoveFrom(other);
      }
      return *this;
    }
    ~Checkout() { Release(); }

    static Checkout
    Armed(std::uint64_t id,
          const std::shared_ptr<detail::GenerationToken> &generation) {
      Checkout checkout;
      checkout.id_ = id;
      checkout.generation_ = generation;
      checkout.armed_ = true;
      return checkout;
    }

  private:
    void MoveFrom(Checkout &other) noexcept {
      client = std::move(other.client);
      id_ = other.id_;
      generation_ = std::move(other.generation_);
      armed_ = std::exchange(other.armed_, false);
    }

    void Release() noexcept {
      if (!armed_) {
        return;
      }
      armed_ = false;
      if (detail::ThreadCache<ClientT>::Closing()) {
        client.reset();
        return;
      }
      auto &entries = detail::ThreadCache<ClientT>::Map();
      const auto found = entries.find(id_);
      if (found == entries.end()) {
        client.reset();
        return;
      }
      const auto mine = generation_.lock();
      if (mine == nullptr || mine.get() != found->second->generation.get()) {
        client.reset();
        return;
      }
      auto entry = found->second;
      if (client.has_value() && !entry->owner.expired()) {
        entry->client = std::move(client);
        entry->checked_out = false;
      } else {
        entries.erase(found);
      }
      client.reset();
    }

    std::uint64_t id_{0};
    std::weak_ptr<detail::GenerationToken> generation_{};
    bool armed_{false};
  };

  ThreadLocalClient() = default;

  static std::pair<std::size_t, bool> Prune(std::optional<std::uint64_t> id,
                                            bool all) {
    if (detail::ThreadCache<ClientT>::Closing())
      return {0, false};
    auto &entries = detail::ThreadCache<ClientT>::Map();
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

  static std::expected<Checkout, Error>
  Enter(std::uint64_t id, const std::shared_ptr<detail::OwnerToken> &owner) {
    if (detail::ThreadCache<ClientT>::Closing())
      return std::unexpected(
          Error::InvalidConfig("thread-local storage is closing"));
    auto &entries = detail::ThreadCache<ClientT>::Map();
    const auto found = entries.find(id);
    if (found == entries.end()) {
      auto local =
          std::make_shared<typename detail::ThreadCache<ClientT>::Entry>();
      local->owner = owner;
      local->generation = std::make_shared<detail::GenerationToken>();
      auto checkout = Checkout::Armed(id, local->generation);
      entries.emplace(id, std::move(local));
      return checkout;
    }
    auto entry = found->second;
    if (entry->checked_out || !entry->client.has_value()) {
      return std::unexpected(
          Error::InvalidConfig("reentrant client call on this thread"));
    }
    entry->checked_out = true;
    auto checkout = Checkout::Armed(id, entry->generation);
    checkout.client = std::move(*entry->client);
    entry->client.reset();
    return checkout;
  }

  std::uint64_t id_{0};
  std::vector<std::string> endpoints_{};
  std::optional<FactoryFn> factory_{};
  std::shared_ptr<detail::OwnerToken> lifetime_{};
  std::optional<FailoverConfig> failover_{};
};

} // namespace greptimedb
