/** Fixed per-thread map from pool id to one cache entry. */
#pragma once

#include <uarch/uarch.hpp>

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace greptimedb::detail {

/** Live pools resident in one thread's open-addressed table. */
inline constexpr std::size_t kMaxThreadPools = 64;
static_assert(std::has_single_bit(kMaxThreadPools));

template <typename Registry>
concept PoolLivenessRegistry = requires(std::uint64_t id) {
  { Registry::IsAlive(id) } -> std::same_as<bool>;
};

/**
 * Fixed open-addressed map from `pool_id` to one entry.
 *
 * A hit reads the one-entry MRU or the home slot. Dead ids are reclaimed on
 * insert. More than `kMaxThreadPools` live ids is `kInvalidConfig`.
 * `Closing()` blocks teardown reentrancy; `PinGuard` defers an erase while pinned.
 */
template <typename Entry, PoolLivenessRegistry Registry> struct CacheStorage {
  static constexpr std::uint64_t kEmpty = 0;
  static constexpr std::uint64_t kTomb = ~std::uint64_t{0};
  static constexpr std::size_t kIndexMask = kMaxThreadPools - 1uz;
  static constexpr unsigned kHashShift = 64U - static_cast<unsigned>(std::bit_width(kMaxThreadPools) - 1);

  struct Slot {
    std::uint64_t pool_id{kEmpty};
    std::unique_ptr<Entry> entry{};
  };
  using TrashType = std::vector<std::unique_ptr<Entry>>;

  [[nodiscard]] static constexpr std::size_t Home(std::uint64_t pool_id) noexcept {
    constexpr std::uint64_t kFib = 11400714819323198485ULL;
    return static_cast<std::size_t>((pool_id * kFib) >> kHashShift);
  }

  [[nodiscard]] static bool &Closing() noexcept {
    static thread_local bool closing{false};
    return closing;
  }

  struct Local {
    std::array<Slot, kMaxThreadPools> slots{};
    std::uint64_t mru_id{kEmpty};
    Entry *mru{nullptr};
    TrashType deferred{};
    ~Local() { Closing() = true; }
  };

  [[nodiscard]] static Local &State() {
    static thread_local Local local{};
    return local;
  }

  static void ForgetMru(Local &local, std::uint64_t pool_id) noexcept {
    if (pool_id != kEmpty && pool_id != kTomb && local.mru_id == pool_id) {
      local.mru_id = kEmpty;
      local.mru = nullptr;
    }
  }

  static void Remember(Local &local, std::uint64_t pool_id, Entry *entry) noexcept {
    local.mru_id = pool_id;
    local.mru = entry;
  }

  [[nodiscard]] static Entry *Find(Local &local, std::uint64_t pool_id) noexcept {
    if (local.mru_id == pool_id) return local.mru;
    const std::size_t home = Home(pool_id);
    auto &first = local.slots[home];
    if (first.pool_id == pool_id) {
      Remember(local, pool_id, first.entry.get());
      return local.mru;
    }
    if (first.pool_id == kEmpty) return nullptr;
    for (std::size_t step = 1; step < kMaxThreadPools; ++step) {
      auto &slot = local.slots[(home + step) & kIndexMask];
      if (slot.pool_id == pool_id) {
        Remember(local, pool_id, slot.entry.get());
        return local.mru;
      }
      if (slot.pool_id == kEmpty) return nullptr;
    }
    return nullptr;
  }

  static void RetireOne(Local &local, std::unique_ptr<Entry> victim, TrashType &immediate_drop) {
    if (!victim) return;
    if (uarch::unlikely(victim->pins > 0)) {
      victim->retired = true;
      local.deferred.push_back(std::move(victim));
    } else {
      immediate_drop.push_back(std::move(victim));
    }
  }

  static void ReclaimDead(Local &local) {
    TrashType drop;
    for (auto &slot : local.slots) {
      if (slot.pool_id == kEmpty || slot.pool_id == kTomb || Registry::IsAlive(slot.pool_id)) continue;
      ForgetMru(local, slot.pool_id);
      RetireOne(local, std::move(slot.entry), drop);
      slot.pool_id = kTomb;
    }
  }

  /** Reclaims dead ids, then returns a home, hole, or existing slot. */
  [[nodiscard]] static std::optional<std::size_t> Reserve(Local &local, std::uint64_t pool_id) {
    ReclaimDead(local);
    const std::size_t home = Home(pool_id);
    for (std::size_t step = 0; step < kMaxThreadPools; ++step) {
      const std::size_t idx = (home + step) & kIndexMask;
      const auto key = local.slots[idx].pool_id;
      if (key == pool_id || key == kEmpty || key == kTomb) return idx;
    }
    return std::nullopt;
  }

  static Entry *Commit(Local &local, std::size_t idx, std::uint64_t pool_id, std::unique_ptr<Entry> created) {
    auto &slot = local.slots[idx];
    TrashType drop;
    if (slot.pool_id != pool_id) {
      ForgetMru(local, slot.pool_id);
      RetireOne(local, std::move(slot.entry), drop);
    }
    slot.pool_id = pool_id;
    slot.entry = std::move(created);
    Remember(local, pool_id, slot.entry.get());
    return local.mru;
  }

  static std::pair<std::size_t, bool> Prune(std::optional<std::uint64_t> target_id, bool all) {
    if (uarch::unlikely(Closing())) return {0uz, false};
    auto &local = State();
    TrashType drop;
    if (all) {
      std::size_t removed = 0;
      for (auto &slot : local.slots) {
        if (slot.pool_id == kEmpty || slot.pool_id == kTomb) {
          slot = {};
          continue;
        }
        ++removed;
        RetireOne(local, std::move(slot.entry), drop);
        slot = {};
      }
      local.mru_id = kEmpty;
      local.mru = nullptr;
      return {removed, false};
    }
    if (!target_id) return {0uz, false};
    const std::uint64_t id = *target_id;
    const std::size_t home = Home(id);
    for (std::size_t step = 0; step < kMaxThreadPools; ++step) {
      auto &slot = local.slots[(home + step) & kIndexMask];
      if (slot.pool_id == id) {
        RetireOne(local, std::move(slot.entry), drop);
        slot.pool_id = kTomb;
        ForgetMru(local, id);
        return {1uz, true};
      }
      if (slot.pool_id == kEmpty) return {0uz, false};
    }
    return {0uz, false};
  }

  static void SweepRetired(Entry *target) noexcept {
    if (uarch::unlikely(Closing())) return;
    auto &deferred = State().deferred;
    for (std::size_t idx = 0; idx < deferred.size(); ++idx) {
      if (deferred[idx].get() == target) {
        auto victim = std::move(deferred[idx]);
        deferred.erase(deferred.begin() + static_cast<std::ptrdiff_t>(idx));
        return;
      }
    }
  }

  class PinGuard final {
  public:
    explicit PinGuard(Entry &entry) noexcept : entry_(&entry) { ++entry_->pins; }
    PinGuard(const PinGuard &) = delete("PinGuard scopes a single stack pin; copying is disabled");
    PinGuard &operator=(const PinGuard &) = delete("PinGuard scopes a single stack pin; copying is disabled");
    PinGuard(PinGuard &&) = delete("PinGuard is bound to one stack frame; moving is disabled");
    PinGuard &operator=(PinGuard &&) = delete("PinGuard is bound to one stack frame; moving is disabled");
    ~PinGuard() noexcept {
      if (--entry_->pins == 0 && uarch::unlikely(entry_->retired)) SweepRetired(entry_);
    }

  private:
    Entry *entry_;
  };
};

} // namespace greptimedb::detail
