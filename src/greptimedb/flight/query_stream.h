#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <greptimedb/core/error.h>
#include <greptimedb/flight/ipc_codec.h>
#include <greptimedb/proto/greptime_proto.h>
#include <greptimedb/transport/synchronous_channel.h>

namespace greptimedb::flight {

struct QueryResult {
  std::optional<std::uint64_t> affected_rows;
  std::vector<std::string> metrics;
};

/** Caller-thread DoGet session. The channel must outlive this stream.
 * Metadata frames are accumulated while NextBatch continues to the next batch.
 * Destroying an unfinished stream cancels its HTTP/2 request.
 */
class QueryStream final {
public:
  [[nodiscard]] static Result<std::unique_ptr<QueryStream>>
  Open(SynchronousChannel &channel, const ::greptime::v1::RequestHeader &header,
       const ::greptime::v1::QueryRequest &query,
       CallOptions options = {}) noexcept;
  [[nodiscard]] static Result<std::unique_ptr<QueryStream>>
  Sql(SynchronousChannel &channel, const ::greptime::v1::RequestHeader &header,
      std::string_view sql, CallOptions options = {}) noexcept;
  [[nodiscard]] static Result<std::unique_ptr<QueryStream>>
  PromRangeQuery(SynchronousChannel &channel,
                 const ::greptime::v1::RequestHeader &header,
                 const ::greptime::v1::PromRangeQuery &query,
                 CallOptions options = {}) noexcept;
  [[nodiscard]] static Result<std::unique_ptr<QueryStream>> LogicalPlan(
      SynchronousChannel &channel, const ::greptime::v1::RequestHeader &header,
      std::span<const std::byte> plan, CallOptions options = {}) noexcept;
  [[nodiscard]] static Result<std::unique_ptr<QueryStream>>
  InsertIntoPlan(SynchronousChannel &channel,
                 const ::greptime::v1::RequestHeader &header,
                 std::string_view table_name, std::span<const std::byte> plan,
                 CallOptions options = {}) noexcept;

  ~QueryStream();
  QueryStream(const QueryStream &) = delete;
  QueryStream &operator=(const QueryStream &) = delete;
  QueryStream(QueryStream &&) = delete;
  QueryStream &operator=(QueryStream &&) = delete;

  [[nodiscard]] Result<std::optional<RecordBatch>> NextBatch() noexcept;
  [[nodiscard]] Result<QueryResult> Finish() noexcept;
  [[nodiscard]] Result<void> Cancel() noexcept;
  [[nodiscard]] const std::optional<FlightSchema> &Schema() const noexcept {
    return schema_;
  }
  [[nodiscard]] std::uint64_t AffectedRows() const noexcept {
    return affected_rows_;
  }
  [[nodiscard]] const std::vector<std::string> &Metrics() const noexcept {
    return metrics_;
  }

  [[nodiscard]] static Result<::arrow::flight::protocol::Ticket>
  EncodeTicket(const ::greptime::v1::RequestHeader &header,
               const ::greptime::v1::QueryRequest &query) noexcept;

private:
  enum class State {
    kAwaitingSchema,
    kReading,
    kFinished,
    kFailed,
    kCancelled
  };
  explicit QueryStream(std::unique_ptr<RpcStream> call) noexcept;
  [[nodiscard]] Result<void> CheckOwner() const noexcept;
  [[nodiscard]] Result<void> AcceptMetadata(std::string_view bytes) noexcept;
  [[nodiscard]] Result<void> CheckTrailers() noexcept;
  [[nodiscard]] Error Fail(Error error) noexcept;
  [[nodiscard]] Error TransportError(grpcxx::Status status) noexcept;

  std::unique_ptr<RpcStream> call_;
  std::thread::id owner_;
  State state_{State::kAwaitingSchema};
  std::optional<Error> failure_;
  std::optional<FlightSchema> schema_;
  std::uint64_t affected_rows_{};
  bool saw_affected_rows_{};
  std::vector<std::string> metrics_;
};

} // namespace greptimedb::flight
