#include "greptimedb/failover/failover.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

namespace greptimedb {
namespace {

/** True when `opened + cooldown` is past the clock's representable range. */
[[nodiscard]] bool CooldownAddOverflows(FailoverTime opened,
                                        FailoverDuration cooldown) noexcept {
  if (cooldown.count() <= 0) {
    return false;
  }
  const auto opened_ns = opened.time_since_epoch();
  if (opened_ns.count() < 0) {
    return false;
  }
  const auto room = FailoverTime::max().time_since_epoch() - opened_ns;
  return cooldown > room;
}

/** True when the open interval has elapsed and the endpoint may be selected. */
[[nodiscard]] bool CooldownElapsed(FailoverTime opened,
                                   FailoverDuration cooldown,
                                   FailoverTime now) noexcept {
  if (cooldown.count() < 0)
    return false;
  if (cooldown.count() == 0) {
    return now >= opened;
  }
  if (CooldownAddOverflows(opened, cooldown)) {
    return false;
  }
  return now >= opened + cooldown;
}

[[nodiscard]] bool ContainsIndex(std::span<const std::size_t> excluded,
                                 std::size_t index) noexcept {
  for (const std::size_t item : excluded) {
    if (item == index) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] std::uint64_t DurationUs(FailoverDuration rtt) noexcept {
  if (rtt.count() <= 0) {
    return 0;
  }
  return std::max<std::uint64_t>(
      1, static_cast<std::uint64_t>(rtt.count() / 1000));
}

/** First sample is stored raw. Later samples keep seven eighths of the history.
 */
[[nodiscard]] std::uint64_t SmoothEwma(std::uint64_t current,
                                       std::uint64_t sample) noexcept {
  if (current == 0) {
    return sample;
  }
  const std::uint64_t decayed = current - current / 8;
  const std::uint64_t eighth = sample / 8;
  if (decayed > std::numeric_limits<std::uint64_t>::max() - eighth) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return decayed + eighth;
}

[[nodiscard]] std::uint64_t LatencyRank(std::uint64_t ewma,
                                        bool any_positive) noexcept {
  if (any_positive && ewma == 0) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return ewma;
}

[[nodiscard]] std::size_t
LowestLatency(const std::vector<EndpointHealthState> &health,
              const std::vector<std::size_t> &available) noexcept {
  bool any_positive = false;
  for (const std::size_t index : available) {
    if (health[index].ewma_rtt_us > 0) {
      any_positive = true;
      break;
    }
  }
  std::size_t best = available.front();
  for (std::size_t offset = 1; offset < available.size(); ++offset) {
    const std::size_t index = available[offset];
    if (LatencyRank(health[index].ewma_rtt_us, any_positive) <
        LatencyRank(health[best].ewma_rtt_us, any_positive)) {
      best = index;
    }
  }
  return best;
}

[[nodiscard]] const char *SwitchReason(EndpointSelection selection,
                                       const EndpointHealthState &previous,
                                       FailoverTime now,
                                       FailoverDuration cooldown) noexcept {
  const bool still_open =
      previous.circuit_opened_at.has_value() &&
      !CooldownElapsed(*previous.circuit_opened_at, cooldown, now);
  if (still_open) {
    return "circuit open";
  }
  if (previous.healthy) {
    if (selection == EndpointSelection::kRoundRobin) {
      return "round-robin rotation";
    }
    return "endpoint rotation";
  }
  return "endpoint unavailable";
}

} // namespace

FailoverPolicy::FailoverPolicy(std::vector<std::string> endpoints)
    : endpoints_(std::move(endpoints)),
      max_attempts_(endpoints_.empty() ? 0 : endpoints_.size()),
      health_(endpoints_.size()), candidates_(endpoints_.size()) {}

FailoverPolicy
FailoverPolicy::create(std::initializer_list<std::string_view> endpoints) {
  std::vector<std::string> names;
  names.reserve(endpoints.size());
  for (const std::string_view endpoint : endpoints) {
    names.emplace_back(endpoint);
  }
  return FailoverPolicy(std::move(names));
}

FailoverPolicy &
FailoverPolicy::with_selection(EndpointSelection selection) & noexcept {
  selection_ = selection;
  return *this;
}

FailoverPolicy &
FailoverPolicy::with_max_attempts(std::size_t max_attempts) & noexcept {
  max_attempts_ = max_attempts;
  return *this;
}

FailoverPolicy &
FailoverPolicy::with_circuit_breaker(std::uint32_t threshold,
                                     FailoverDuration cooldown) & noexcept {
  failure_threshold_ = threshold;
  circuit_cooldown_ = cooldown;
  return *this;
}

FailoverPolicy &
FailoverPolicy::with_random_seed(std::uint64_t seed) & noexcept {
  // A zero seed is stored as one so it matches an explicit seed of one.
  const std::uint64_t stored = seed == 0 ? 1 : seed;
  random_seed_ = stored;
  rng_ = stored;
  return *this;
}

FailoverPolicy FailoverPolicy::from_config(const FailoverConfig &config) {
  FailoverPolicy policy(config.endpoints);
  policy.selection_ = config.selection;
  policy.max_attempts_ = config.max_attempts;
  policy.failure_threshold_ = config.failure_threshold;
  policy.circuit_cooldown_ = config.circuit_cooldown;
  if (config.random_seed.has_value()) {
    policy.with_random_seed(*config.random_seed);
  }
  return policy;
}

FailoverConfig FailoverPolicy::config() const {
  FailoverConfig snapshot;
  snapshot.selection = selection_;
  snapshot.failure_threshold = failure_threshold_;
  snapshot.circuit_cooldown = circuit_cooldown_;
  snapshot.max_attempts = max_attempts_;
  snapshot.endpoints = endpoints_;
  snapshot.random_seed = random_seed_;
  return snapshot;
}

FailoverConfigParts FailoverPolicy::config_parts() const noexcept {
  return FailoverConfigParts{
      .selection = selection_,
      .max_attempts = max_attempts_,
      .failure_threshold = failure_threshold_,
      .circuit_cooldown = circuit_cooldown_,
      .random_seed = random_seed_,
  };
}

std::expected<std::size_t, Error> FailoverPolicy::select_endpoint_excluding(
    FailoverTime now, std::span<const std::size_t> excluded) {
  if (selecting_)
    return std::unexpected(
        Error::InvalidConfig("reentrant endpoint selection"));
  struct SelectionScope {
    bool &active;
    ~SelectionScope() { active = false; }
  } scope{selecting_};
  selecting_ = true;
  bool valid_selection = false;
  switch (selection_) {
  case EndpointSelection::kRoundRobin:
  case EndpointSelection::kRandom:
  case EndpointSelection::kLatencyAware:
  case EndpointSelection::kSticky:
    valid_selection = true;
    break;
  }
  if (!valid_selection || circuit_cooldown_.count() < 0)
    return std::unexpected(Error::InvalidConfig("invalid failover settings"));
  if (endpoints_.empty()) {
    return std::unexpected(
        Error::InvalidConfig("failover policy has no endpoints"));
  }
  if (max_attempts_ == 0) {
    return std::unexpected(exhausted(0, 0));
  }
  if (selection_ == EndpointSelection::kSticky && current_.has_value() &&
      !ContainsIndex(excluded, *current_) && is_available(*current_, now)) {
    return *current_;
  }

  const std::size_t count = endpoints_.size();
  std::size_t origin = cursor_ % count;
  if (selection_ == EndpointSelection::kSticky) {
    origin = current_.has_value() ? (*current_ + 1) % count : 0;
  }

  auto &available = candidates_;
  available.clear();
  std::size_t examined = 0;
  for (std::size_t step = 0; step < count; ++step) {
    const std::size_t index = (origin + step) % count;
    if (ContainsIndex(excluded, index)) {
      continue;
    }
    ++examined;
    if (is_available(index, now)) {
      available.push_back(index);
    }
  }
  if (available.empty()) {
    const std::size_t back = examined == 0 ? 0 : examined - 1;
    return std::unexpected(exhausted(examined, (origin + back) % count));
  }

  std::size_t chosen = available.front();
  switch (selection_) {
  case EndpointSelection::kRandom: {
    const std::uint64_t draw = next_random();
    const auto width = static_cast<std::uint64_t>(available.size());
    chosen = available[static_cast<std::size_t>(draw % width)];
    break;
  }
  case EndpointSelection::kLatencyAware:
    chosen = LowestLatency(health_, available);
    break;
  case EndpointSelection::kRoundRobin:
  case EndpointSelection::kSticky:
    chosen = available.front();
    break;
  }
  commit_selection(chosen, examined, now);
  return chosen;
}

Result<void>
FailoverPolicy::bind_endpoints(std::vector<std::string> endpoints) {
  if (endpoints.size() != health_.size())
    return std::unexpected(
        Error::InvalidConfig("endpoint rebinding cannot change cardinality"));
  endpoints_ = std::move(endpoints);
  return {};
}

bool FailoverPolicy::needs_probe(std::size_t index,
                                 FailoverTime now) const noexcept {
  if (index >= health_.size() ||
      !health_[index].circuit_opened_at.has_value()) {
    return false;
  }
  return is_available(index, now);
}

void FailoverPolicy::record_connect_success(std::size_t index) noexcept {
  if (index >= health_.size() || health_[index].circuit_opened_at.has_value()) {
    return;
  }
  health_[index].healthy = true;
}

void FailoverPolicy::record_probe_success(std::size_t index) noexcept {
  if (index >= health_.size()) {
    return;
  }
  EndpointHealthState &state = health_[index];
  state.healthy = true;
  state.consecutive_failures = 0;
  state.circuit_opened_at = std::nullopt;
}

void FailoverPolicy::record_success(std::size_t index,
                                    FailoverDuration rtt) noexcept {
  if (index >= health_.size()) {
    return;
  }
  EndpointHealthState &state = health_[index];
  state.consecutive_failures = 0;
  state.healthy = true;
  state.circuit_opened_at = std::nullopt;
  state.ewma_rtt_us = SmoothEwma(state.ewma_rtt_us, DurationUs(rtt));
}

void FailoverPolicy::record_failure(std::size_t index,
                                    FailoverTime now) noexcept {
  if (index >= health_.size()) {
    return;
  }
  EndpointHealthState &state = health_[index];
  if (state.consecutive_failures < std::numeric_limits<std::uint32_t>::max()) {
    ++state.consecutive_failures;
  }
  state.healthy = false;
  if (state.circuit_opened_at.has_value() ||
      (failure_threshold_ > 0 &&
       state.consecutive_failures >= failure_threshold_)) {
    state.circuit_opened_at = now;
  }
}

bool FailoverPolicy::should_failover_write(bool bytes_sent_on_wire,
                                           const Error &error) noexcept {
  if (!bytes_sent_on_wire) {
    return true;
  }
  const std::optional<bool> hint = error.RetryHint();
  return hint.has_value() && *hint;
}

std::optional<EndpointHealthState>
FailoverPolicy::health(std::size_t index) const {
  if (index >= health_.size()) {
    return std::nullopt;
  }
  return health_[index];
}

std::optional<std::string_view>
FailoverPolicy::endpoint(std::size_t index) const noexcept {
  if (index >= endpoints_.size()) {
    return std::nullopt;
  }
  return endpoints_[index];
}

bool FailoverPolicy::is_available(std::size_t index,
                                  FailoverTime now) const noexcept {
  if (index >= health_.size()) {
    return false;
  }
  const std::optional<FailoverTime> opened = health_[index].circuit_opened_at;
  if (!opened.has_value()) {
    return true;
  }
  return CooldownElapsed(*opened, circuit_cooldown_, now);
}

void FailoverPolicy::commit_selection(std::size_t chosen, std::size_t examined,
                                      FailoverTime now) {
  const std::optional<std::size_t> previous = current_;
  current_ = chosen;
  if (!endpoints_.empty()) {
    cursor_ = (chosen + 1) % endpoints_.size();
  }
  if (!on_failover_ || !previous.has_value() || *previous == chosen ||
      *previous >= endpoints_.size() || *previous >= health_.size() ||
      chosen >= endpoints_.size()) {
    return;
  }
  FailoverEvent event;
  event.from_endpoint = endpoints_[*previous];
  event.to_endpoint = endpoints_[chosen];
  event.reason =
      SwitchReason(selection_, health_[*previous], now, circuit_cooldown_);
  event.attempt = examined == 0 ? 1 : examined;
  if (on_failover_) {
    on_failover_(event);
  }
}

Error FailoverPolicy::exhausted(std::size_t attempts, std::size_t index) const {
  std::string endpoint = "unknown";
  if (index < endpoints_.size()) {
    endpoint = endpoints_[index];
  }
  return Error::AllEndpointsFailed(
      attempts,
      Error{ErrorKind::kCircuitOpen, "circuit open for endpoint " + endpoint});
}

std::uint64_t FailoverPolicy::next_random() noexcept {
  std::uint64_t state = rng_ == 0 ? 1 : rng_;
  state ^= state << 13U;
  state ^= state >> 7U;
  state ^= state << 17U;
  rng_ = state;
  return state;
}

} // namespace greptimedb
