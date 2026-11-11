/** Bounded example-only HTTP/2 peer driven by the client's caller thread. */
#pragma once

#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/query_stream.h>
#include <grpcxx/sync/in_memory_channel_pair.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace example {
namespace db = greptimedb;
namespace flight = db::flight;
namespace v1 = greptime::v1;
namespace af = arrow::flight::protocol;

class OfflinePeer {
public:
  std::string table;
  std::string sql;
  std::vector<flight::RecordBatch> stored;
  std::size_t schema_handshakes{};
  std::size_t bulk_batches{};
  std::size_t bulk_rows{};
  std::size_t insert_messages{};
  std::size_t insert_rows{};
  std::size_t rejected{};
  bool defer_first_bulk_ack{};
  std::size_t deferred_ack_pairs{};
  std::optional<flight::ipc::Compression> expected_compression;
  bool failed{};
  std::unique_ptr<grpcxx::InMemoryChannelPair> pair;

  bool Open(std::size_t capacity = 16384) {
    auto created = grpcxx::InMemoryChannelPair::Create(&Handle, this, {}, {}, capacity);
    if (!created) return false;
    pair = std::move(*created);
    return true;
  }
  void Pump() {
    for (unsigned i = 0; i < 16; ++i)
      if (!pair->Channel().PollOnce()) failed = true;
  }

private:
  struct Call {
    std::size_t rows{};
    std::int64_t next_id{};
    std::optional<flight::FlightSchema> schema;
    bool done{};
  };
  std::vector<std::pair<std::int32_t, Call>> calls_;
  struct HeldAck {
    std::int32_t stream;
    std::int64_t request;
    std::size_t rows;
  };
  std::optional<HeldAck> held_ack_;

  static void Handle(void *context, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    auto &self = *static_cast<OfflinePeer *>(context);
    switch (event.event) {
    case grpcxx::ServerRequest::Event::kCancel: self.DropCall(event.stream_id); return;
    case grpcxx::ServerRequest::Event::kHalfClose: self.Close(server, event); return;
    case grpcxx::ServerRequest::Event::kMessage:
      if (event.path == db::StreamInserter::kPath)
        self.Insert(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoPut")
        self.Put(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoGet")
        self.Get(server, event);
      else
        self.Reject(server, event.stream_id, "unexpected method");
      return;
    }
  }
  void Reject(grpcxx::SynchronousServer &server, std::int32_t id, const char *reason, bool expected = false) {
    if (!expected) failed = true;
    EnsureCall(id).done = true;
    if (!server.Finish(id, {grpcxx::StatusCode::kUnavailable, reason})) failed = true;
  }
  template <class T> bool Send(grpcxx::SynchronousServer &server, std::int32_t id, const T &message) {
    std::string bytes;
    if (!message.SerializeToString(&bytes) || !server.Send(id, std::as_bytes(std::span{bytes}))) {
      failed = true;
      return false;
    }
    return true;
  }
  bool SendFrame(grpcxx::SynchronousServer &server, std::int32_t id, const flight::EncodedFlightBody &encoded) {
    af::FlightData frame;
    frame.set_data_header(encoded.data_header.data(), encoded.data_header.size());
    frame.set_data_body(encoded.data_body.data(), encoded.data_body.size());
    return Send(server, id, frame);
  }
  bool Ack(grpcxx::SynchronousServer &server, std::int32_t id, std::int64_t request, std::size_t rows) {
    af::PutResult ack;
    ack.set_app_metadata("{\"request_id\":" + std::to_string(request) + ",\"affected_rows\":" + std::to_string(rows) +
                         ",\"elapsed_secs\":0}");
    return Send(server, id, ack);
  }
  void Insert(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) {
    v1::GreptimeRequest request;
    if (!request.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size()))) {
      Reject(server, event.stream_id, "invalid request");
      return;
    }
    std::size_t rows = 0;
    if (request.has_inserts()) {
      for (const auto &insert : request.inserts().inserts()) {
        if (insert.table_name() == "_reject") {
          ++rejected;
          Reject(server, event.stream_id, "simulated region failure", true);
          return;
        }
        rows += insert.row_count();
      }
    } else if (request.has_row_inserts()) {
      for (const auto &insert : request.row_inserts().inserts())
        rows += insert.rows().rows_size();
    } else if (request.has_deletes()) {
      for (const auto &deletion : request.deletes().deletes())
        rows += deletion.row_count();
    } else {
      Reject(server, event.stream_id, "unexpected ingestion frame");
      return;
    }
    EnsureCall(event.stream_id).rows += rows;
    insert_rows += rows;
    ++insert_messages;
  }
  void Put(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) {
    af::FlightData frame;
    if (!frame.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size()))) {
      Reject(server, event.stream_id, "invalid FlightData");
      return;
    }
    auto metadata = flight::DoPutMetadata::Decode(frame.app_metadata());
    auto &call = EnsureCall(event.stream_id);
    if (!metadata || metadata->request_id != call.next_id) {
      Reject(server, event.stream_id, "non-monotone request id");
      return;
    }
    ++call.next_id;
    const auto header_bytes = CopyBytes(frame.data_header());
    const std::span<const std::uint8_t> header{header_bytes};
    if (!call.schema) {
      auto schema = flight::DecodeFlightSchema(header);
      if (!schema || !frame.data_body().empty() || frame.flight_descriptor().type() != af::FlightDescriptor::PATH ||
          frame.flight_descriptor().path_size() != 1 || frame.flight_descriptor().path(0) != table ||
          metadata->request_id != 0) {
        Reject(server, event.stream_id, "invalid schema handshake");
        return;
      }
      call.schema = std::move(*schema);
      ++schema_handshakes;
      Ack(server, event.stream_id, 0, 0);
      return;
    }
    const auto body_bytes = CopyBytes(frame.data_body());
    const std::span<const std::uint8_t> body{body_bytes};
    auto message = flight::ipc::DecodeMessage(header);
    if (!message || (expected_compression && message->batch.compression != *expected_compression)) {
      Reject(server, event.stream_id, "unexpected IPC compression");
      return;
    }
    auto record = flight::DecodeFlightRecordBatch(*call.schema, header, body);
    if (!record || stored.size() >= 64 || bulk_rows + record->row_count > 4096) {
      Reject(server, event.stream_id, "invalid or excessive batch");
      return;
    }
    auto bounds = flight::TimestampBounds(*record);
    if (!bounds || metadata->min_timestamp != bounds->first || metadata->max_timestamp != bounds->second) {
      Reject(server, event.stream_id, "timestamp bounds differ");
      return;
    }
    ++bulk_batches;
    bulk_rows += record->row_count;
    const auto rows = record->row_count;
    stored.push_back(std::move(*record));
    // Hold only the first ACK until a second frame arrives. This fills the
    // two-frame client window without withholding the final odd batch's ACK.
    if (defer_first_bulk_ack && bulk_batches == 1) {
      held_ack_ = HeldAck{event.stream_id, metadata->request_id, rows};
      return;
    }
    if (held_ack_) {
      if (!Ack(server, held_ack_->stream, held_ack_->request, held_ack_->rows)) return;
      held_ack_.reset();
      ++deferred_ack_pairs;
    }
    Ack(server, event.stream_id, metadata->request_id, rows);
  }
  void Get(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) {
    af::Ticket ticket;
    v1::GreptimeRequest request;
    if (!ticket.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size())) ||
        !request.ParseFromString(ticket.ticket()) || !request.has_query() || request.query().sql() != sql ||
        stored.empty()) {
      Reject(server, event.stream_id, "unexpected query ticket");
      return;
    }
    flight::FlightIpcEncoder encoder(flight::FlightCompression::kLz4Frame);
    auto schema = encoder.EncodeSchema(stored.front().schema);
    if (!schema || !SendFrame(server, event.stream_id, *schema)) {
      Reject(server, event.stream_id, "cannot encode replay schema");
      return;
    }
    for (const auto &batch : stored) {
      auto encoded = encoder.EncodeBatch(batch);
      if (!encoded || !SendFrame(server, event.stream_id, *encoded)) {
        Reject(server, event.stream_id, "cannot encode replay batch");
        return;
      }
    }
  }
  static std::vector<std::uint8_t> CopyBytes(std::string_view text) {
    std::vector<std::uint8_t> out(text.size());
    if (!out.empty()) std::memcpy(out.data(), text.data(), out.size());
    return out;
  }
  Call &EnsureCall(std::int32_t id) {
    for (auto &entry : calls_) {
      if (entry.first == id) return entry.second;
    }
    calls_.emplace_back(id, Call{});
    return calls_.back().second;
  }
  void DropCall(std::int32_t id) {
    std::erase_if(calls_, [id](const auto &entry) { return entry.first == id; });
  }
  void Close(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) {
    auto &call = EnsureCall(event.stream_id);
    if (call.done) return;
    if (event.path == db::StreamInserter::kPath) {
      v1::GreptimeResponse response;
      response.mutable_affected_rows()->set_value(static_cast<std::uint32_t>(call.rows));
      if (!Send(server, event.stream_id, response)) return;
    }
    call.done = true;
    if (!server.Finish(event.stream_id, {})) failed = true;
  }
};
} // namespace example
