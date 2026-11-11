#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <thread>

#include <greptimedb/core/error.h>
#include <greptimedb/proto/greptime_proto.h>
#include <grpcxx/sync/synchronous_channel.h>

namespace greptimedb {

/** Caller-thread ingestion session; sending does not acknowledge a server
 * commit. The channel must outlive the session. Destruction cancels an
 * unfinished RPC without replaying requests whose server-side outcome may be
 * uncertain.
 */
class StreamInserter final {
  class ConstructionKey {
    friend class StreamInserter;
    ConstructionKey() = default;
  };

public:
  StreamInserter(ConstructionKey, std::unique_ptr<grpcxx::RpcStream> call,
                 ::greptime::v1::RequestHeader header) noexcept;

  static constexpr std::string_view kPath = "/greptime.v1.GreptimeDatabase/HandleRequests";

  [[nodiscard]] static Result<std::unique_ptr<StreamInserter>> Open(grpcxx::SynchronousChannel &channel,
                                                                    ::greptime::v1::RequestHeader header = {},
                                                                    grpcxx::CallOptions options = {}) noexcept;
  ~StreamInserter();
  StreamInserter(const StreamInserter &) = delete("single-owner state; copying or moving is disabled");
  StreamInserter &operator=(const StreamInserter &) = delete("single-owner state; copying or moving is disabled");
  StreamInserter(StreamInserter &&) = delete("single-owner state; copying or moving is disabled");
  StreamInserter &operator=(StreamInserter &&) = delete("single-owner state; copying or moving is disabled");

  [[nodiscard]] Result<void> SendBatch(const ::greptime::v1::InsertRequest &batch) noexcept;
  [[nodiscard]] Result<void> SendBatch(const ::greptime::v1::InsertRequests &batches) noexcept;
  [[nodiscard]] Result<void> SendBatchTo(std::string_view table_name,
                                         const ::greptime::v1::InsertRequest &batch) noexcept;
  [[nodiscard]] Result<void> SendBatches(std::span<const ::greptime::v1::InsertRequest> batches) noexcept;
  [[nodiscard]] Result<void> SendRows(const ::greptime::v1::RowInsertRequest &batch) noexcept;
  [[nodiscard]] Result<void> SendRows(const ::greptime::v1::RowInsertRequests &batches) noexcept;
  [[nodiscard]] Result<void> SendDeleteBatch(const ::greptime::v1::DeleteRequest &batch) noexcept;
  [[nodiscard]] Result<void> SendDeleteBatch(const ::greptime::v1::DeleteRequests &batches) noexcept;
  [[nodiscard]] Result<void> SendDeleteRows(const ::greptime::v1::RowDeleteRequest &batch) noexcept;
  [[nodiscard]] Result<void> SendDeleteRows(const ::greptime::v1::RowDeleteRequests &batches) noexcept;

  /** Replaces caller-supplied routing with this session's immutable header.
   * Only the four ingestion frame kinds are accepted. Counters advance after
   * the transport accepts the complete encoded message.
   */
  [[nodiscard]] Result<void> SendRequest(::greptime::v1::GreptimeRequest request) noexcept;
  [[nodiscard]] Result<std::uint32_t> Finish() noexcept;
  [[nodiscard]] Result<void> Cancel() noexcept;
  [[nodiscard]] std::size_t BatchesSent() const noexcept { return batches_sent_; }
  [[nodiscard]] std::size_t RowsSent() const noexcept { return rows_sent_; }

private:
  enum class State { kActive, kFinishing, kFinished, kFailed, kCancelled };

  [[nodiscard]] Result<void> EnsureActive() const noexcept;
  [[nodiscard, gnu::cold, gnu::noinline]] Error Fail(Error error) noexcept;
  [[nodiscard]] Error TransportError(grpcxx::Status status) const noexcept;

  std::unique_ptr<grpcxx::RpcStream> call_;
  ::greptime::v1::RequestHeader header_;
  std::thread::id owner_;
  State state_{State::kActive};
  std::optional<Error> failure_;
  std::size_t batches_sent_{};
  std::size_t rows_sent_{};
};

} // namespace greptimedb
