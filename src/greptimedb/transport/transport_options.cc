#include "greptimedb/transport/endpoint_transport.h"

#include <algorithm>
#include <new>

namespace greptimedb {
namespace {
using Clock = std::chrono::steady_clock;
using grpcxx::Status;
using grpcxx::StatusCode;
auto DeadlineAfter(std::chrono::nanoseconds timeout) noexcept
    -> Clock::time_point {
  const auto now = Clock::now();
  const auto available = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::time_point::max() - now);
  return now + std::min(timeout, available);
}
class ConfiguredTransport final : public EndpointTransport {
public:
  ConfiguredTransport(std::unique_ptr<EndpointTransport> wire,
                      TransportOptions options) noexcept
      : wire_(std::move(wire)), options_(options),
        connect_deadline_(DeadlineAfter(options.connect_timeout)),
        ready_(wire_->IsReady()) {}
  auto PollOnce() noexcept -> std::expected<void, Status> override {
    if (!failure_.Ok())
      return std::unexpected(failure_);
    const auto now = Clock::now();
    if ((!ready_ && now >= connect_deadline_) ||
        (read_deadline_ && now >= *read_deadline_) ||
        (write_deadline_ && now >= *write_deadline_)) {
      failure_ = Status{StatusCode::kDeadlineExceeded,
                        !ready_ ? "transport connect/TLS deadline exceeded"
                        : read_deadline_ && now >= *read_deadline_
                            ? "transport read timeout"
                            : "transport write timeout"};
      wire_->Close();
      return std::unexpected(failure_);
    }
    auto result = wire_->PollOnce();
    if (!result) {
      failure_ = result.error();
      return result;
    }
    ready_ |= wire_->IsReady();
    return {};
  }
  auto Read(std::span<std::byte> bytes) noexcept
      -> std::expected<std::size_t, Status> override {
    if (bytes.empty())
      return 0;
    if (auto polled = PollOnce(); !polled)
      return std::unexpected(polled.error());
    auto result = wire_->Read(bytes);
    if (result && *result)
      read_deadline_.reset();
    else if (result && ready_ && options_.read_timeout && !read_deadline_)
      read_deadline_ = DeadlineAfter(*options_.read_timeout);
    return result;
  }
  auto Write(std::span<const std::byte> bytes) noexcept
      -> std::expected<std::size_t, Status> override {
    if (bytes.empty())
      return 0;
    if (auto polled = PollOnce(); !polled)
      return std::unexpected(polled.error());
    auto result = wire_->Write(bytes);
    if (result && *result)
      write_deadline_.reset();
    else if (result && ready_ && options_.write_timeout && !write_deadline_)
      write_deadline_ = DeadlineAfter(*options_.write_timeout);
    return result;
  }
  void Close() noexcept override { wire_->Close(); }
  auto IsOpen() const noexcept -> bool override {
    return failure_.Ok() && wire_->IsOpen();
  }
  auto NativeHandle() const noexcept -> int override {
    return wire_->NativeHandle();
  }
  auto IsReady() const noexcept -> bool override { return ready_; }
  auto IsTls() const noexcept -> bool override { return wire_->IsTls(); }

private:
  std::unique_ptr<EndpointTransport> wire_;
  TransportOptions options_;
  Clock::time_point connect_deadline_;
  std::optional<Clock::time_point> read_deadline_, write_deadline_;
  bool ready_;
  Status failure_;
};
} // namespace

auto TransportOptions::Validate() const noexcept
    -> std::expected<void, grpcxx::Status> {
  const auto available = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::time_point::max() - Clock::now());
  for (auto value :
       {std::optional{connect_timeout}, read_timeout, write_timeout}) {
    if (value &&
        (*value <= std::chrono::nanoseconds::zero() || *value > available))
      return std::unexpected(grpcxx::Status{
          grpcxx::StatusCode::kInvalidArgument,
          "transport timeouts must fit a positive monotonic deadline"});
  }
  return {};
}
auto EndpointTransport::Configure(std::unique_ptr<EndpointTransport> wire,
                                  TransportOptions options) noexcept
    -> std::expected<std::unique_ptr<EndpointTransport>, grpcxx::Status> {
  if (auto valid = options.Validate(); !valid)
    return std::unexpected(valid.error());
  if (!wire || !wire->IsOpen())
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument,
                                          "transport is not open"});
  auto result = std::unique_ptr<EndpointTransport>{
      new (std::nothrow) ConfiguredTransport(std::move(wire), options)};
  if (!result)
    return std::unexpected(
        grpcxx::Status{grpcxx::StatusCode::kResourceExhausted});
  return result;
}
} // namespace greptimedb
