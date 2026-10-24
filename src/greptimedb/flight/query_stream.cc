#include <greptimedb/flight/query_stream.h>

#include <limits>
#include <new>
#include <utility>

namespace greptimedb::flight {
namespace {
constexpr std::string_view kDoGetPath =
    "/arrow.flight.protocol.FlightService/DoGet";

std::span<const std::uint8_t> IpcBytes(const std::string &value) noexcept {
  return {reinterpret_cast<const std::uint8_t *>(value.data()), value.size()};
}

Error Protocol(std::string message) noexcept {
  return {ErrorKind::kProtocol, std::move(message)};
}

Error DecodeError(std::string message) noexcept {
  return {ErrorKind::kDecode, std::move(message)};
}
} // namespace

QueryStream::QueryStream(std::unique_ptr<RpcStream> call) noexcept
    : call_(std::move(call)), owner_(std::this_thread::get_id()) {}

QueryStream::~QueryStream() {
  switch (state_) {
  case State::kAwaitingSchema:
  case State::kReading:
  case State::kFailed:
    // Destruction cannot return a cancellation diagnostic; RpcStream owns the
    // reset.
    static_cast<void>(call_->Cancel());
    break;
  case State::kFinished:
  case State::kCancelled:
    break;
  }
}

Result<::arrow::flight::protocol::Ticket>
QueryStream::EncodeTicket(const ::greptime::v1::RequestHeader &header,
                          const ::greptime::v1::QueryRequest &query) noexcept {
  if (query.query_case() == ::greptime::v1::QueryRequest::QUERY_NOT_SET)
    return std::unexpected(Protocol("query request has no query"));
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_header() = header;
  *request.mutable_query() = query;
  ::arrow::flight::protocol::Ticket ticket;
  if (!request.SerializeToString(ticket.mutable_ticket()))
    return std::unexpected(
        Error{ErrorKind::kEncode, "cannot encode Flight query ticket"});
  return ticket;
}

Result<std::unique_ptr<QueryStream>> QueryStream::Open(
    SynchronousChannel &channel, const ::greptime::v1::RequestHeader &header,
    const ::greptime::v1::QueryRequest &query, CallOptions options) noexcept {
  auto ticket = EncodeTicket(header, query);
  if (!ticket)
    return std::unexpected(std::move(ticket.error()));
  std::string wire;
  if (!ticket->SerializeToString(&wire))
    return std::unexpected(
        Error{ErrorKind::kEncode, "cannot encode Flight Ticket"});
  auto call = channel.ServerStreamCall(
      kDoGetPath, std::as_bytes(std::span{wire}), std::move(options));
  if (!call)
    return std::unexpected(Error::Grpc(std::move(call.error())));
  auto stream = std::unique_ptr<QueryStream>(new (std::nothrow)
                                                 QueryStream(std::move(*call)));
  if (!stream)
    return std::unexpected(
        Error{ErrorKind::kStream, "cannot allocate query stream"});
  return stream;
}

Result<std::unique_ptr<QueryStream>>
QueryStream::Sql(SynchronousChannel &channel,
                 const ::greptime::v1::RequestHeader &header,
                 std::string_view sql, CallOptions options) noexcept {
  ::greptime::v1::QueryRequest query;
  query.set_sql(std::string{sql});
  return Open(channel, header, query, std::move(options));
}

Result<std::unique_ptr<QueryStream>>
QueryStream::PromRangeQuery(SynchronousChannel &channel,
                            const ::greptime::v1::RequestHeader &header,
                            const ::greptime::v1::PromRangeQuery &range_query,
                            CallOptions options) noexcept {
  ::greptime::v1::QueryRequest query;
  *query.mutable_prom_range_query() = range_query;
  return Open(channel, header, query, std::move(options));
}

Result<std::unique_ptr<QueryStream>> QueryStream::LogicalPlan(
    SynchronousChannel &channel, const ::greptime::v1::RequestHeader &header,
    std::span<const std::byte> plan, CallOptions options) noexcept {
  ::greptime::v1::QueryRequest query;
  if (plan.empty())
    query.set_logical_plan("");
  else
    query.set_logical_plan(plan.data(), plan.size());
  return Open(channel, header, query, std::move(options));
}

Result<std::unique_ptr<QueryStream>> QueryStream::InsertIntoPlan(
    SynchronousChannel &channel, const ::greptime::v1::RequestHeader &header,
    std::string_view table_name, std::span<const std::byte> plan,
    CallOptions options) noexcept {
  ::greptime::v1::QueryRequest query;
  auto *insert = query.mutable_insert_into_plan();
  auto *table = insert->mutable_table_name();
  table->set_catalog_name(header.catalog());
  table->set_schema_name(header.schema());
  table->set_table_name(std::string{table_name});
  if (plan.empty())
    insert->set_logical_plan("");
  else
    insert->set_logical_plan(plan.data(), plan.size());
  return Open(channel, header, query, std::move(options));
}

Result<void> QueryStream::CheckOwner() const noexcept {
  if (std::this_thread::get_id() != owner_)
    return std::unexpected(
        Protocol("query stream used from a different thread"));
  return {};
}

Error QueryStream::TransportError(grpcxx::Status status) noexcept {
  if (!call_->FinalStatus().Ok())
    status = call_->FinalStatus();
  return Error::FromGrpcStatusAndTrailers(std::move(status),
                                          call_->TrailingMetadata());
}

Error QueryStream::Fail(Error error) noexcept {
  state_ = State::kFailed;
  failure_ = error;
  // Preserve the original query failure if resetting the transport also fails.
  static_cast<void>(call_->Cancel());
  return error;
}

Result<void> QueryStream::CheckTrailers() noexcept {
  if (!call_->FinalStatus().Ok())
    return std::unexpected(TransportError(call_->FinalStatus()));
  return Error::CheckOkTrailers(call_->TrailingMetadata());
}

Result<void> QueryStream::AcceptMetadata(std::string_view bytes) noexcept {
  if (bytes.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(
        DecodeError("Flight metadata exceeds protobuf size limit"));
  ::greptime::v1::FlightMetadata metadata;
  if (!metadata.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())))
    return std::unexpected(
        DecodeError("cannot decode GreptimeDB Flight metadata"));
  if (!metadata.has_affected_rows() && !metadata.has_metrics())
    return std::unexpected(Protocol("empty Flight metadata"));
  if (metadata.has_affected_rows()) {
    const auto rows = std::uint64_t{metadata.affected_rows().value()};
    if (rows > std::numeric_limits<std::uint64_t>::max() - affected_rows_)
      return std::unexpected(Protocol("affected row count overflowed"));
    affected_rows_ += rows;
    saw_affected_rows_ = true;
  }
  if (metadata.has_metrics())
    metrics_.push_back(metadata.metrics().metrics());
  return {};
}

Result<std::optional<RecordBatch>> QueryStream::NextBatch() noexcept {
  if (auto owner = CheckOwner(); !owner)
    return std::unexpected(std::move(owner.error()));
  switch (state_) {
  case State::kFinished:
    return std::nullopt;
  case State::kFailed:
    return std::unexpected(*failure_);
  case State::kCancelled:
    return std::unexpected(Protocol("query stream is cancelled"));
  case State::kAwaitingSchema:
  case State::kReading:
    break;
  }
  for (;;) {
    auto received = call_->RecvNext();
    if (!received)
      return std::unexpected(Fail(TransportError(std::move(received.error()))));
    if (!*received) {
      auto trailers = CheckTrailers();
      if (!trailers)
        return std::unexpected(Fail(std::move(trailers.error())));
      state_ = State::kFinished;
      return std::nullopt;
    }
    const auto &payload = **received;
    if (payload.size() >
        static_cast<std::size_t>(std::numeric_limits<int>::max()))
      return std::unexpected(
          Fail(DecodeError("Flight frame exceeds protobuf size limit")));
    ::arrow::flight::protocol::FlightData frame;
    if (!frame.ParseFromArray(payload.data(), static_cast<int>(payload.size())))
      return std::unexpected(Fail(DecodeError("cannot decode FlightData")));
    auto message = ipc::DecodeMessage(IpcBytes(frame.data_header()));
    if (!message)
      return std::unexpected(Fail(DecodeError(std::move(message.error()))));
    if (message->body_length < 0 ||
        static_cast<std::uint64_t>(message->body_length) !=
            frame.data_body().size())
      return std::unexpected(
          Fail(Protocol("Flight IPC body length does not match header")));
    switch (message->header_type) {
    case ipc::HeaderType::kNone: {
      auto accepted = AcceptMetadata(frame.app_metadata());
      if (!accepted)
        return std::unexpected(Fail(std::move(accepted.error())));
      break;
    }
    case ipc::HeaderType::kSchema: {
      if (schema_)
        return std::unexpected(Fail(Protocol("duplicate Flight schema")));
      auto schema = DecodeFlightSchema(IpcBytes(frame.data_header()));
      if (!schema)
        return std::unexpected(Fail(DecodeError(std::move(schema.error()))));
      schema_ = std::move(*schema);
      state_ = State::kReading;
      break;
    }
    case ipc::HeaderType::kDictionaryBatch:
      return std::unexpected(
          Fail(Protocol("Flight dictionary batches are not supported")));
    case ipc::HeaderType::kRecordBatch: {
      if (!schema_)
        return std::unexpected(
            Fail(Protocol("record batch preceded Flight schema")));
      auto batch = DecodeFlightRecordBatch(
          *schema_, IpcBytes(frame.data_header()), IpcBytes(frame.data_body()));
      if (!batch)
        return std::unexpected(Fail(DecodeError(std::move(batch.error()))));
      if (!frame.app_metadata().empty()) {
        auto accepted = AcceptMetadata(frame.app_metadata());
        if (!accepted)
          return std::unexpected(Fail(std::move(accepted.error())));
      }
      return std::optional<RecordBatch>{std::move(*batch)};
    }
    }
  }
}

Result<QueryResult> QueryStream::Finish() noexcept {
  for (;;) {
    auto batch = NextBatch();
    if (!batch)
      return std::unexpected(std::move(batch.error()));
    if (!*batch)
      break;
  }
  QueryResult result;
  if (saw_affected_rows_)
    result.affected_rows = affected_rows_;
  result.metrics = metrics_;
  return result;
}

Result<void> QueryStream::Cancel() noexcept {
  if (auto owner = CheckOwner(); !owner)
    return std::unexpected(std::move(owner.error()));
  switch (state_) {
  case State::kFinished:
  case State::kCancelled:
    return {};
  case State::kFailed:
    return std::unexpected(*failure_);
  case State::kAwaitingSchema:
  case State::kReading:
    break;
  }
  auto cancelled = call_->Cancel();
  if (!cancelled) {
    state_ = State::kFailed;
    failure_ = TransportError(std::move(cancelled.error()));
    return std::unexpected(*failure_);
  }
  state_ = State::kCancelled;
  return {};
}

} // namespace greptimedb::flight
