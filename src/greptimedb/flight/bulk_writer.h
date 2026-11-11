#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include <greptimedb/core/error.h>
#include <greptimedb/flight/ipc_codec.h>
#include <grpcxx/sync/synchronous_channel.h>
#include <uarch/uarch.hpp>

namespace arrow::flight::protocol {
class PutResult;
}

namespace greptimedb::flight {

/** Exact integer metadata; timestamps retain the Arrow column's native unit. */
struct DoPutMetadata {
  std::int64_t request_id{};
  std::optional<std::int64_t> min_timestamp;
  std::optional<std::int64_t> max_timestamp;
  [[nodiscard]] Result<std::string> Encode() const noexcept;
  [[nodiscard]] static Result<DoPutMetadata> Decode(std::string_view json) noexcept;
};

struct DoPutResponse {
  std::int64_t request_id{};
  std::uint64_t affected_rows{};
  double elapsed_secs{};
  [[nodiscard]] static Result<DoPutResponse> Decode(std::string_view json) noexcept;
  [[nodiscard]] static Result<DoPutResponse> Decode(const ::arrow::flight::protocol::PutResult &response) noexcept;
};

struct BulkWriteOptions {
  FlightCompression compression{FlightCompression::kNone};
  std::size_t max_in_flight{8}; // ACK window, in [1, 65536].
  bool partition_by_hour{false};
  std::chrono::nanoseconds timeout{std::chrono::seconds(30)};
};

struct BulkWriteStats {
  std::uint64_t total_affected_rows{};
  std::size_t batches_sent{};
  double elapsed_secs{};
};
static_assert(uarch::NoPadding<BulkWriteStats>);

/** Caller-thread DoPut session. The channel must outlive the writer.
 * Open waits for schema acknowledgement; unfinished destruction cancels the
 * RPC.
 */
class BulkStreamWriter final {
  class ConstructionKey {
    friend class BulkStreamWriter;
    ConstructionKey() = default;
  };

public:
  BulkStreamWriter(ConstructionKey, std::unique_ptr<grpcxx::RpcStream> call, FlightSchema schema,
                   BulkWriteOptions options) noexcept;

  [[nodiscard]] static Result<std::unique_ptr<BulkStreamWriter>>
  Open(grpcxx::SynchronousChannel &channel, std::string_view table_name, const FlightSchema &schema,
       BulkWriteOptions options = {}, grpcxx::CallOptions call_options = {}) noexcept;
  ~BulkStreamWriter();
  BulkStreamWriter(const BulkStreamWriter &) = delete("single-owner state; copying or moving is disabled");
  BulkStreamWriter &operator=(const BulkStreamWriter &) = delete("single-owner state; copying or moving is disabled");
  BulkStreamWriter(BulkStreamWriter &&) = delete("single-owner state; copying or moving is disabled");
  BulkStreamWriter &operator=(BulkStreamWriter &&) = delete("single-owner state; copying or moving is disabled");

  [[nodiscard]] Result<void> WriteRecordBatch(const RecordBatch &batch) noexcept;
  [[nodiscard]] Result<void> WriteBatch(const RecordBatch &batch) noexcept { return WriteRecordBatch(batch); }
  [[nodiscard]] Result<BulkWriteStats> Finish() noexcept;
  [[nodiscard]] Result<void> Cancel() noexcept;
  [[nodiscard]] std::size_t InFlight() const noexcept { return pending_count_; }

private:
  enum class State { kOpening, kActive, kDraining, kFinished, kFailed, kCancelled };

  [[nodiscard]] Result<void> EnsureActive() const noexcept;
  [[nodiscard]] Result<void> SendSchema(std::string_view table_name) noexcept;
  [[nodiscard]] Result<void> SendBatch(const RecordBatch &batch) noexcept;
  [[nodiscard]] Result<void> DrainReady() noexcept;
  [[nodiscard]] Result<void> WaitForCapacity() noexcept;
  [[nodiscard]] Result<void> AcceptResult(std::span<const std::byte> payload, bool schema_ack = false) noexcept;
  [[nodiscard]] Result<void> CheckTrailers() noexcept;
  [[nodiscard]] Error Fail(Error error) noexcept;
  [[nodiscard]] Error TransportError(grpcxx::Status status) noexcept;

  std::unique_ptr<grpcxx::RpcStream> call_;
  FlightSchema schema_;
  FlightIpcEncoder encoder_;
  BulkWriteOptions options_;
  uarch::FastDivisorU64 in_flight_div_;
  std::thread::id owner_;
  State state_{State::kOpening};
  std::optional<Error> failure_;
  std::int64_t next_request_id_{1};
  std::unique_ptr<std::int64_t[]> pending_;
  std::size_t pending_head_{};
  std::size_t pending_count_{};
  BulkWriteStats stats_;
};

} // namespace greptimedb::flight
