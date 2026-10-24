#include "greptimedb/client/stream_inserter.h"

#include <limits>
#include <new>
#include <string>
#include <utility>

namespace greptimedb {
namespace {

Error Protocol(std::string message) noexcept {
  return Error{ErrorKind::kProtocol, std::move(message)};
}

Result<std::size_t>
RequestRows(const ::greptime::v1::GreptimeRequest &request) noexcept {
  std::size_t total{};
  const auto add = [&total](std::size_t count) noexcept {
    if (count > std::numeric_limits<std::size_t>::max() - total)
      return false;
    total += count;
    return true;
  };
  using Request = ::greptime::v1::GreptimeRequest;
  switch (request.request_case()) {
  case Request::kInserts:
    for (const auto &batch : request.inserts().inserts()) {
      if (!add(batch.row_count()))
        return std::unexpected(Protocol("row counter overflow"));
    }
    break;
  case Request::kDeletes:
    for (const auto &batch : request.deletes().deletes()) {
      if (!add(batch.row_count()))
        return std::unexpected(Protocol("row counter overflow"));
    }
    break;
  case Request::kRowInserts:
    for (const auto &batch : request.row_inserts().inserts()) {
      if (!add(static_cast<std::size_t>(batch.rows().rows_size())))
        return std::unexpected(Protocol("row counter overflow"));
    }
    break;
  case Request::kRowDeletes:
    for (const auto &batch : request.row_deletes().deletes()) {
      if (!add(static_cast<std::size_t>(batch.rows().rows_size())))
        return std::unexpected(Protocol("row counter overflow"));
    }
    break;
  case Request::kQuery:
  case Request::kDdl:
  case Request::REQUEST_NOT_SET:
    return std::unexpected(
        Protocol("ingestion stream requires an insert or delete frame"));
  }
  return total;
}

} // namespace

StreamInserter::StreamInserter(std::unique_ptr<RpcStream> call,
                               ::greptime::v1::RequestHeader header) noexcept
    : call_(std::move(call)), header_(std::move(header)),
      owner_(std::this_thread::get_id()) {}

Result<std::unique_ptr<StreamInserter>>
StreamInserter::Open(SynchronousChannel &channel,
                     ::greptime::v1::RequestHeader header,
                     CallOptions options) noexcept {
  auto call = channel.ClientStreamCall(kPath, std::move(options));
  if (!call)
    return std::unexpected(Error::Grpc(call.error()));
  auto *session =
      new (std::nothrow) StreamInserter(std::move(*call), std::move(header));
  if (session == nullptr)
    return std::unexpected(Error::Grpc(grpcxx::Status{
        grpcxx::StatusCode::kResourceExhausted, "allocate ingestion session"}));
  return std::unique_ptr<StreamInserter>{session};
}

StreamInserter::~StreamInserter() {
  switch (state_) {
  case State::kActive:
  case State::kFinishing:
  case State::kFailed:
    // Cancellation is best-effort in destruction; RpcStream owns reset cleanup.
    if (call_)
      static_cast<void>(call_->Cancel());
    break;
  case State::kFinished:
  case State::kCancelled:
    break;
  }
}

Result<void> StreamInserter::EnsureActive() const noexcept {
  if (owner_ != std::this_thread::get_id())
    return std::unexpected(Error{ErrorKind::kStream,
                                 "ingestion session used from another thread"});
  switch (state_) {
  case State::kActive:
    return {};
  case State::kFailed:
    if (failure_)
      return std::unexpected(*failure_);
    return std::unexpected(
        Error{ErrorKind::kStream, "ingestion session failed"});
  case State::kFinishing:
  case State::kFinished:
  case State::kCancelled:
    return std::unexpected(
        Error{ErrorKind::kStream, "ingestion session is closed"});
  }
  return std::unexpected(
      Error{ErrorKind::kStream, "invalid ingestion session state"});
}

Error StreamInserter::TransportError(grpcxx::Status status) const noexcept {
  if (!call_->FinalStatus().Ok())
    status = call_->FinalStatus();
  return Error::FromGrpcStatusAndTrailers(std::move(status),
                                          call_->TrailingMetadata());
}

Error StreamInserter::Fail(Error error) noexcept {
  state_ = State::kFailed;
  failure_ = error;
  static_cast<void>(call_->Cancel());
  return error;
}

Result<void>
StreamInserter::SendRequest(::greptime::v1::GreptimeRequest request) noexcept {
  if (auto active = EnsureActive(); !active)
    return active;
  if (call_->Done() || call_->TrailingMetadata().WireValue("grpc-status")) {
    auto result = Finish();
    if (!result)
      return std::unexpected(result.error());
    return std::unexpected(Protocol("ingestion stream closed before Finish"));
  }
  auto rows = RequestRows(request);
  if (!rows)
    return std::unexpected(rows.error());
  if (batches_sent_ == std::numeric_limits<std::size_t>::max() ||
      *rows > std::numeric_limits<std::size_t>::max() - rows_sent_)
    return std::unexpected(Protocol("ingestion stream counter overflow"));
  *request.mutable_header() = header_;
  std::string payload;
  if (!request.SerializeToString(&payload))
    return std::unexpected(
        Error{ErrorKind::kEncode, "encode ingestion request"});
  auto sent =
      call_->Send(std::as_bytes(std::span{payload.data(), payload.size()}));
  if (!sent)
    return std::unexpected(Fail(TransportError(sent.error())));
  ++batches_sent_;
  rows_sent_ += *rows;
  return {};
}

Result<void>
StreamInserter::SendBatch(const ::greptime::v1::InsertRequest &batch) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_inserts()->add_inserts() = batch;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendBatch(
    const ::greptime::v1::InsertRequests &batches) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_inserts() = batches;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendBatchTo(
    std::string_view table_name,
    const ::greptime::v1::InsertRequest &batch) noexcept {
  if (table_name.empty())
    return std::unexpected(Protocol("insert table name is empty"));
  ::greptime::v1::GreptimeRequest request;
  auto *insert = request.mutable_inserts()->add_inserts();
  *insert = batch;
  insert->set_table_name(table_name.data(), table_name.size());
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendBatches(
    std::span<const ::greptime::v1::InsertRequest> batches) noexcept {
  ::greptime::v1::GreptimeRequest request;
  auto *inserts = request.mutable_inserts();
  for (const auto &batch : batches)
    *inserts->add_inserts() = batch;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendRows(
    const ::greptime::v1::RowInsertRequest &batch) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_row_inserts()->add_inserts() = batch;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendRows(
    const ::greptime::v1::RowInsertRequests &batches) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_row_inserts() = batches;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendDeleteBatch(
    const ::greptime::v1::DeleteRequest &batch) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_deletes()->add_deletes() = batch;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendDeleteBatch(
    const ::greptime::v1::DeleteRequests &batches) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_deletes() = batches;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendDeleteRows(
    const ::greptime::v1::RowDeleteRequest &batch) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_row_deletes()->add_deletes() = batch;
  return SendRequest(std::move(request));
}

Result<void> StreamInserter::SendDeleteRows(
    const ::greptime::v1::RowDeleteRequests &batches) noexcept {
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_row_deletes() = batches;
  return SendRequest(std::move(request));
}

Result<std::uint32_t> StreamInserter::Finish() noexcept {
  if (auto active = EnsureActive(); !active)
    return std::unexpected(active.error());
  state_ = State::kFinishing;
  if (!call_->TrailingMetadata().WireValue("grpc-status")) {
    auto closed = call_->HalfClose();
    if (!closed)
      return std::unexpected(Fail(TransportError(closed.error())));
  }
  auto first = call_->RecvNext();
  if (!first)
    return std::unexpected(Fail(TransportError(first.error())));
  if (!*first) {
    auto trailers = Error::CheckOkTrailers(call_->TrailingMetadata());
    if (!trailers)
      return std::unexpected(Fail(trailers.error()));
    return std::unexpected(Fail(Protocol("missing ingestion response")));
  }
  const auto &bytes = **first;
  if (bytes.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(
        Fail(Protocol("ingestion response exceeds protobuf limit")));
  ::greptime::v1::GreptimeResponse response;
  if (!response.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())))
    return std::unexpected(
        Fail(Error{ErrorKind::kDecode, "decode ingestion response"}));
  auto extra = call_->RecvNext();
  if (!extra)
    return std::unexpected(Fail(TransportError(extra.error())));
  if (*extra)
    return std::unexpected(Fail(Protocol("multiple ingestion responses")));
  auto finished = call_->Finish();
  if (!finished)
    return std::unexpected(Fail(TransportError(finished.error())));
  if (!finished->status.Ok())
    return std::unexpected(Fail(Error::FromGrpcStatusAndTrailers(
        finished->status, finished->trailing_metadata)));
  if (auto trailers = Error::CheckOkTrailers(finished->trailing_metadata);
      !trailers)
    return std::unexpected(Fail(trailers.error()));
  if (auto header = Error::FromResponseHeader(
          response.has_header() ? &response.header() : nullptr);
      !header)
    return std::unexpected(Fail(header.error()));
  if (!response.has_affected_rows())
    return std::unexpected(
        Fail(Protocol("ingestion response has no affected rows")));
  state_ = State::kFinished;
  return response.affected_rows().value();
}

Result<void> StreamInserter::Cancel() noexcept {
  if (owner_ != std::this_thread::get_id())
    return std::unexpected(Error{ErrorKind::kStream,
                                 "ingestion session used from another thread"});
  switch (state_) {
  case State::kFinished:
  case State::kCancelled:
    return std::unexpected(
        Error{ErrorKind::kStream, "ingestion session is closed"});
  case State::kFailed:
    return std::unexpected(*failure_);
  case State::kActive:
  case State::kFinishing:
    break;
  }
  auto cancelled = call_->Cancel();
  if (!cancelled)
    return std::unexpected(Fail(TransportError(cancelled.error())));
  state_ = State::kCancelled;
  return {};
}

} // namespace greptimedb
