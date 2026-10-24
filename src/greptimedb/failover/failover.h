/** Multi-endpoint selection, circuit breaking, and write-replay policy.
 *
 * A write that already placed bytes on the wire may move to another endpoint
 * only when the server retry hint is explicitly true.
 */
#pragma once

#include <greptimedb/core/error.h>
#include <grpcxx/core/inplace_delegate.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <initializer_list>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace greptimedb {

/** Clock used for cooldown checks. Tests inject instants; nothing sleeps. */
using FailoverTime = std::chrono::steady_clock::time_point;

/** Cooldown and round-trip durations. Millisecond values convert implicitly. */
using FailoverDuration = std::chrono::nanoseconds;

inline constexpr std::uint32_t kDefaultFailureThreshold = 3;
inline constexpr FailoverDuration kDefaultCircuitCooldown =
    std::chrono::seconds(30);
inline constexpr std::uint64_t kDefaultRandomSeed = 0x9E3779B97F4A7C15ULL;

/** Inline observer capacity. Larger callables are not constructible. */
inline constexpr std::size_t kFailoverObserverCapacity = 48;

/** How the next eligible endpoint is chosen. */
enum class EndpointSelection : std::uint8_t {
  kRoundRobin,
  kRandom,
  kLatencyAware,
  kSticky,
};

/** One observed switch between endpoints. */
struct FailoverEvent {
  /** Endpoint that was current before the switch. */
  std::string from_endpoint;
  /** Endpoint selected by this call. */
  std::string to_endpoint;
  /** Why the previous endpoint was left. */
  std::string reason;
  /** Candidates examined by the call, counting from 1. */
  std::size_t attempt = 1;

  [[nodiscard]] bool operator==(const FailoverEvent &) const = default;
};

/** Circuit and latency state for one endpoint. */
struct EndpointHealthState {
  /** Failures since the last success. */
  std::uint32_t consecutive_failures = 0;
  /** When the circuit last opened. Empty means the circuit is closed. */
  std::optional<FailoverTime> circuit_opened_at;
  /** Smoothed round trip in microseconds. Zero means no sample yet. */
  std::uint64_t ewma_rtt_us = 0;
  /** Whether the last completed attempt succeeded. */
  bool healthy = true;

  [[nodiscard]] bool operator==(const EndpointHealthState &) const = default;
};

using FailoverObserver = grpcxx::InplaceDelegate<void(const FailoverEvent &),
                                                 kFailoverObserverCapacity>;

/**
 * Policy settings. The observer is thread-affine and is not restored by
 * `FailoverPolicy::from_config`.
 */
struct FailoverConfig {
  EndpointSelection selection = EndpointSelection::kRoundRobin;
  /** Consecutive failures that open the circuit. Zero disables the breaker. */
  std::uint32_t failure_threshold = kDefaultFailureThreshold;
  /** How long an open circuit stays ineligible before it becomes half-open. */
  FailoverDuration circuit_cooldown = kDefaultCircuitCooldown;
  /** Endpoint attempts allowed in one client operation. Zero selects nothing.
   */
  std::size_t max_attempts = 0;
  /** Endpoint names in selection order. */
  std::vector<std::string> endpoints{};
  /** Seed for `kRandom`. Empty uses the built-in seed. Zero is stored as one.
   */
  std::optional<std::uint64_t> random_seed{};
};

/** Selection, attempt limit, breaker, and random seed without the observer. */
struct FailoverConfigParts {
  EndpointSelection selection = EndpointSelection::kRoundRobin;
  std::size_t max_attempts = 0;
  std::uint32_t failure_threshold = kDefaultFailureThreshold;
  FailoverDuration circuit_cooldown = kDefaultCircuitCooldown;
  std::optional<std::uint64_t> random_seed{};
};

/**
 * Per-endpoint health and the rule for choosing the next attempt.
 *
 * Move-only: the observer is an inplace callback, not a shared or cross-thread
 * handle. It runs on the thread that calls `select_endpoint`.
 */
class FailoverPolicy {
public:
  explicit FailoverPolicy(std::vector<std::string> endpoints);

  /** Names in selection order. A braced list is one `string_view` per endpoint.
   */
  [[nodiscard]] static FailoverPolicy
  create(std::initializer_list<std::string_view> endpoints);

  FailoverPolicy(const FailoverPolicy &) = delete;
  FailoverPolicy &operator=(const FailoverPolicy &) = delete;
  FailoverPolicy(FailoverPolicy &&) noexcept = default;
  FailoverPolicy &operator=(FailoverPolicy &&) noexcept = default;

  FailoverPolicy &with_selection(EndpointSelection selection) & noexcept;
  [[nodiscard]] FailoverPolicy
  with_selection(EndpointSelection selection) && noexcept {
    this->with_selection(selection);
    return std::move(*this);
  }

  FailoverPolicy &with_max_attempts(std::size_t max_attempts) & noexcept;
  [[nodiscard]] FailoverPolicy
  with_max_attempts(std::size_t max_attempts) && noexcept {
    this->with_max_attempts(max_attempts);
    return std::move(*this);
  }

  FailoverPolicy &with_circuit_breaker(std::uint32_t threshold,
                                       FailoverDuration cooldown) & noexcept;
  [[nodiscard]] FailoverPolicy
  with_circuit_breaker(std::uint32_t threshold,
                       FailoverDuration cooldown) && noexcept {
    this->with_circuit_breaker(threshold, cooldown);
    return std::move(*this);
  }

  FailoverPolicy &with_random_seed(std::uint64_t seed) & noexcept;
  [[nodiscard]] FailoverPolicy
  with_random_seed(std::uint64_t seed) && noexcept {
    this->with_random_seed(seed);
    return std::move(*this);
  }

  template <typename Callback>
  FailoverPolicy &on_failover(Callback &&callback) & noexcept {
    on_failover_ = FailoverObserver(std::forward<Callback>(callback));
    return *this;
  }

  template <typename Callback>
  [[nodiscard]] FailoverPolicy on_failover(Callback &&callback) && noexcept {
    this->on_failover(std::forward<Callback>(callback));
    return std::move(*this);
  }

  /**
   * Rebuilds a policy from settings. Health, cursor, and the observer are
   * reset. A stored seed of zero becomes one.
   */
  [[nodiscard]] static FailoverPolicy from_config(const FailoverConfig &config);

  /** Settings that can move to another thread. The observer is left empty. */
  [[nodiscard]] FailoverConfig config() const;

  [[nodiscard]] FailoverConfigParts config_parts() const noexcept;

  /**
   * Instants used by overloads that do not take a time argument.
   * Passing a time to `select_endpoint` or `record_failure` does not change it.
   */
  void set_now(FailoverTime now) noexcept { now_ = now; }

  [[nodiscard]] FailoverTime now() const noexcept { return now_; }

  /** Chooses the next endpoint index using the stored clock. */
  [[nodiscard]] std::expected<std::size_t, Error> select_endpoint() {
    return select_endpoint(now_);
  }

  /**
   * Chooses the next endpoint index.
   *
   * `kInvalidConfig` means no endpoints are configured. `kAllEndpointsFailed`
   * means `max_attempts` is zero or every examined candidate is open.
   * Open circuits are skipped and do not reduce `max_attempts`.
   */
  [[nodiscard]] std::expected<std::size_t, Error>
  select_endpoint(FailoverTime now) {
    return select_endpoint_excluding(now, std::span<const std::size_t>{});
  }

  [[nodiscard]] std::expected<std::size_t, Error>
  select_endpoint_excluding(std::span<const std::size_t> excluded) {
    return select_endpoint_excluding(now_, excluded);
  }

  [[nodiscard]] std::expected<std::size_t, Error>
  select_endpoint_excluding(FailoverTime now,
                            std::span<const std::size_t> excluded);

  [[nodiscard]] std::expected<std::size_t, Error>
  select_endpoint_excluding(FailoverTime now,
                            std::initializer_list<std::size_t> excluded) {
    return select_endpoint_excluding(
        now, std::span<const std::size_t>(excluded.begin(), excluded.end()));
  }

  /** Replaces display names without resetting health or the cursor. */
  [[nodiscard]] Result<void> bind_endpoints(std::vector<std::string> endpoints);

  /** Whether an available endpoint still has an open circuit timestamp. */
  [[nodiscard]] bool needs_probe(std::size_t index) const noexcept {
    return needs_probe(index, now_);
  }

  [[nodiscard]] bool needs_probe(std::size_t index,
                                 FailoverTime now) const noexcept;

  /** Records a dial without clearing circuit history or sampling latency. */
  void record_connect_success(std::size_t index) noexcept;

  /** Admits a half-open probe and closes the circuit without sampling latency.
   */
  void record_probe_success(std::size_t index) noexcept;

  /** Records stream completion the same way as a successful probe. */
  void record_stream_success(std::size_t index) noexcept {
    record_probe_success(index);
  }

  /**
   * Records a completed attempt and closes the circuit.
   * An out-of-range index leaves every endpoint unchanged.
   */
  void record_success(std::size_t index, FailoverDuration rtt) noexcept;

  /** Records a failure using the stored clock. */
  void record_failure(std::size_t index) noexcept {
    record_failure(index, now_);
  }

  /**
   * Records a failed attempt and opens the circuit when the threshold is met.
   *
   * An out-of-range index leaves every endpoint unchanged. A threshold of zero
   * never opens a closed circuit. A later failure while the circuit is open or
   * half-open refreshes `circuit_opened_at`. Selection after the cooldown does
   * not close it.
   */
  void record_failure(std::size_t index, FailoverTime now) noexcept;

  /**
   * Whether a failed write may be sent to another endpoint.
   *
   * True when nothing was transmitted, or when `retry_hint` is true. A write
   * that already placed bytes on the wire stays put when the hint is absent
   * or false.
   */
  [[nodiscard]] static bool should_failover_write(bool bytes_sent_on_wire,
                                                  const Error &error) noexcept;

  [[nodiscard]] std::optional<EndpointHealthState>
  health(std::size_t index) const;
  [[nodiscard]] std::optional<std::string_view>
  endpoint(std::size_t index) const noexcept;
  [[nodiscard]] std::size_t len() const noexcept { return endpoints_.size(); }
  [[nodiscard]] bool is_empty() const noexcept { return endpoints_.empty(); }
  [[nodiscard]] EndpointSelection selection() const noexcept {
    return selection_;
  }
  [[nodiscard]] std::size_t max_attempts() const noexcept {
    return max_attempts_;
  }
  [[nodiscard]] std::uint32_t failure_threshold() const noexcept {
    return failure_threshold_;
  }
  [[nodiscard]] FailoverDuration circuit_cooldown() const noexcept {
    return circuit_cooldown_;
  }
  [[nodiscard]] std::optional<std::uint64_t> random_seed() const noexcept {
    return random_seed_;
  }
  [[nodiscard]] bool has_observer() const noexcept {
    return static_cast<bool>(on_failover_);
  }
  [[nodiscard]] std::span<const std::string> endpoint_names() const noexcept {
    return endpoints_;
  }

  FailoverPolicy &WithSelection(EndpointSelection value) & noexcept {
    return with_selection(value);
  }
  [[nodiscard]] FailoverPolicy
  WithSelection(EndpointSelection value) && noexcept {
    return std::move(*this).with_selection(value);
  }
  FailoverPolicy &WithMaxAttempts(std::size_t value) & noexcept {
    return with_max_attempts(value);
  }
  [[nodiscard]] FailoverPolicy WithMaxAttempts(std::size_t value) && noexcept {
    return std::move(*this).with_max_attempts(value);
  }
  FailoverPolicy &WithCircuitBreaker(std::uint32_t threshold,
                                     FailoverDuration cooldown) & noexcept {
    return with_circuit_breaker(threshold, cooldown);
  }
  [[nodiscard]] FailoverPolicy
  WithCircuitBreaker(std::uint32_t threshold,
                     FailoverDuration cooldown) && noexcept {
    return std::move(*this).with_circuit_breaker(threshold, cooldown);
  }
  FailoverPolicy &WithRandomSeed(std::uint64_t seed) & noexcept {
    return with_random_seed(seed);
  }
  [[nodiscard]] FailoverPolicy WithRandomSeed(std::uint64_t seed) && noexcept {
    return std::move(*this).with_random_seed(seed);
  }
  template <typename Callback>
  FailoverPolicy &OnFailover(Callback &&callback) & noexcept {
    return on_failover(std::forward<Callback>(callback));
  }
  template <typename Callback>
  [[nodiscard]] FailoverPolicy OnFailover(Callback &&callback) && noexcept {
    return std::move(*this).on_failover(std::forward<Callback>(callback));
  }
  [[nodiscard]] static FailoverPolicy
  FromConfig(const FailoverConfig &snapshot) {
    return from_config(snapshot);
  }
  [[nodiscard]] FailoverConfig Config() const { return config(); }
  [[nodiscard]] std::expected<std::size_t, Error> Select(FailoverTime instant) {
    return select_endpoint(instant);
  }
  [[nodiscard]] std::expected<std::size_t, Error>
  SelectExcluding(FailoverTime instant, std::span<const std::size_t> excluded) {
    return select_endpoint_excluding(instant, excluded);
  }
  void RecordFailure(std::size_t index, FailoverTime instant) noexcept {
    record_failure(index, instant);
  }
  void RecordSuccess(std::size_t index, FailoverDuration rtt) noexcept {
    record_success(index, rtt);
  }
  [[nodiscard]] const EndpointHealthState *
  Health(std::size_t index) const noexcept {
    return index < health_.size() ? &health_[index] : nullptr;
  }
  [[nodiscard]] std::optional<std::string_view>
  Endpoint(std::size_t index) const noexcept {
    return endpoint(index);
  }
  [[nodiscard]] bool Empty() const noexcept { return is_empty(); }
  [[nodiscard]] EndpointSelection Selection() const noexcept {
    return selection();
  }
  [[nodiscard]] std::size_t MaxAttempts() const noexcept {
    return max_attempts();
  }
  [[nodiscard]] std::optional<std::uint64_t> RandomSeed() const noexcept {
    return random_seed();
  }
  [[nodiscard]] bool HasObserver() const noexcept { return has_observer(); }
  [[nodiscard]] static bool ShouldFailoverWrite(bool bytes_sent_on_wire,
                                                const Error &error) noexcept {
    return should_failover_write(bytes_sent_on_wire, error);
  }
  [[nodiscard]] bool NeedsProbe(std::size_t index,
                                FailoverTime instant) const noexcept {
    return needs_probe(index, instant);
  }
  void RecordConnectSuccess(std::size_t index) noexcept {
    record_connect_success(index);
  }
  void RecordProbeSuccess(std::size_t index) noexcept {
    record_probe_success(index);
  }
  void RecordStreamSuccess(std::size_t index) noexcept {
    record_stream_success(index);
  }

private:
  [[nodiscard]] bool is_available(std::size_t index,
                                  FailoverTime now) const noexcept;
  void commit_selection(std::size_t chosen, std::size_t examined,
                        FailoverTime now);
  [[nodiscard]] Error exhausted(std::size_t attempts, std::size_t index) const;
  [[nodiscard]] std::uint64_t next_random() noexcept;

  std::vector<std::string> endpoints_;
  EndpointSelection selection_ = EndpointSelection::kRoundRobin;
  std::size_t max_attempts_ = 0;
  std::uint32_t failure_threshold_ = kDefaultFailureThreshold;
  FailoverDuration circuit_cooldown_ = kDefaultCircuitCooldown;
  std::optional<std::uint64_t> random_seed_;
  std::uint64_t rng_ = kDefaultRandomSeed;
  FailoverObserver on_failover_{};
  std::vector<EndpointHealthState> health_;
  std::vector<std::size_t> candidates_;
  std::size_t cursor_ = 0;
  std::optional<std::size_t> current_;
  FailoverTime now_{};
  bool selecting_{false};
};

} // namespace greptimedb
