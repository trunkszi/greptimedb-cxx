#pragma once
/** HTTP/2 streaming sessions against a deterministic caller-driven nexora::MemoryPipe
 * peer. */
#include <gtest/gtest.h>

#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/query_stream.h>
#include <grpcxx/sync/in_memory_channel_pair.h>

#include <array>
#include <chrono>
#include <limits>
#include <string>
#include <thread>
#include <type_traits>
#include <uarch/uarch.hpp>
#include <vector>

namespace {
using namespace greptimedb;
using namespace greptimedb::flight;
namespace v1 = ::greptime::v1;
namespace af = ::arrow::flight::protocol;

static_assert(!std::is_move_constructible_v<StreamInserter>);
static_assert(!std::is_move_constructible_v<BulkStreamWriter>);
static_assert(!std::is_move_constructible_v<QueryStream>);
static_assert(uarch::NoPadding<BulkWriteStats>);
static_assert(uarch::NoPadding<ipc::Node> && uarch::RegisterPassable<ipc::Node> && uarch::GprPair<ipc::Node>);
static_assert(uarch::NoPadding<ipc::Buffer> && uarch::RegisterPassable<ipc::Buffer> && uarch::GprPair<ipc::Buffer>);

enum class Scenario {
  kNormal,
  kHeaderError,
  kMissingRows,
  kExtraResponse,
  kTrailerError,
  kEarlyStatus,
  kBadHandshake,
  kOutOfOrderAck,
  kUnknownAck,
  kMissingAck,
  kDelayedAck,
  kQueryError,
  kQueryHold,
  kBatchBeforeSchema,
  kDuplicateSchema,
};

RecordBatch Batch() {
  std::array<v1::Column, 2> columns;
  columns[0].set_column_name("ts");
  columns[0].set_datatype(v1::TIMESTAMP_NANOSECOND);
  columns[0].set_semantic_type(v1::TIMESTAMP);
  columns[0].mutable_values()->add_timestamp_nanosecond_values(100);
  columns[0].mutable_values()->add_timestamp_nanosecond_values(200);
  columns[1].set_column_name("value");
  columns[1].set_datatype(v1::FLOAT64);
  columns[1].set_semantic_type(v1::FIELD);
  columns[1].mutable_values()->add_f64_values(4.0);
  columns[1].mutable_values()->add_f64_values(5.0);
  auto batch = RecordBatchFromProtoColumns(columns, 2);
  EXPECT_TRUE(batch.has_value());
  return batch ? std::move(*batch) : RecordBatch{};
}

v1::RequestHeader Header() {
  v1::RequestHeader header;
  header.set_catalog("research");
  header.set_schema("samples");
  header.set_dbname("research-samples");
  header.mutable_authorization()->mutable_token()->set_token("test-secret");
  return header;
}

template <class Message>
void SendProto(grpcxx::SynchronousServer &server, std::int32_t id, const Message &message) noexcept {
  std::string encoded;
  ASSERT_TRUE(message.SerializeToString(&encoded));
  EXPECT_TRUE(server.Send(id, std::as_bytes(std::span{encoded})).has_value());
}

void SendAck(grpcxx::SynchronousServer &server, std::int32_t id, std::int64_t request_id,
             std::uint64_t rows = 2) noexcept {
  af::PutResult response;
  response.set_app_metadata("{\"request_id\":" + std::to_string(request_id) +
                            ",\"affected_rows\":" + std::to_string(rows) + ",\"elapsed_secs\":0.125}");
  SendProto(server, id, response);
}

void SendBody(grpcxx::SynchronousServer &server, std::int32_t id, const EncodedFlightBody &body) noexcept {
  af::FlightData frame;
  frame.set_data_header(body.data_header.data(), body.data_header.size());
  if (!body.data_body.empty()) frame.set_data_body(body.data_body.data(), body.data_body.size());
  SendProto(server, id, frame);
}

void SendMetadata(grpcxx::SynchronousServer &server, std::int32_t id, std::uint32_t rows,
                  std::string_view metrics) noexcept {
  // Arrow Message FlatBuffer with MetadataVersion V5 and HeaderType NONE.
  constexpr std::array<std::uint8_t, 20> header{12, 0, 0, 0, 6, 0, 8, 0, 6, 0, 0, 0, 8, 0, 0, 0, 0, 0, 4, 0};
  af::FlightData frame;
  frame.set_data_header(header.data(), header.size());
  v1::FlightMetadata metadata;
  metadata.mutable_affected_rows()->set_value(rows);
  metadata.mutable_metrics()->set_metrics(metrics.data(), metrics.size());
  ASSERT_TRUE(metadata.SerializeToString(frame.mutable_app_metadata()));
  SendProto(server, id, frame);
}

struct Peer {
  Scenario scenario{Scenario::kNormal};
  std::vector<v1::GreptimeRequest> writes;
  std::vector<DoPutMetadata> bulk_metadata;
  std::vector<af::FlightData> bulk_frames;
  std::optional<v1::GreptimeRequest> ticket;
  std::unique_ptr<grpcxx::InMemoryChannelPair> pair;
  std::int32_t delayed_stream{};
  std::int64_t delayed_id{};
  unsigned delay_ticks{};
  unsigned delayed_polls{};
  bool delayed_finish{};
  bool failed{};

  static void Handle(void *context, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    auto &self = *static_cast<Peer *>(context);
    switch (event.event) {
    case grpcxx::ServerRequest::Event::kCancel: return;
    case grpcxx::ServerRequest::Event::kMessage:
      if (event.path == StreamInserter::kPath)
        self.InsertMessage(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoPut")
        self.BulkMessage(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoGet")
        self.QueryMessage(server, event);
      else
        self.failed = true;
      return;
    case grpcxx::ServerRequest::Event::kHalfClose:
      if (event.path == StreamInserter::kPath)
        self.InsertFinish(server, event);
      else if (event.path == "/arrow.flight.protocol.FlightService/DoPut" && self.scenario == Scenario::kDelayedAck &&
               self.delayed_stream != 0)
        self.delayed_finish = true;
      else if (event.path == "/arrow.flight.protocol.FlightService/DoGet" && self.scenario == Scenario::kQueryHold)
        return;
      else {
        auto status = self.scenario == Scenario::kQueryError
                          ? grpcxx::Status{grpcxx::StatusCode::kUnavailable, "query interrupted"}
                          : grpcxx::Status{};
        if (!server.Finish(event.stream_id, std::move(status))) self.failed = true;
      }
      return;
    }
  }

  bool Open() noexcept {
    auto created = grpcxx::InMemoryChannelPair::Create(&Handle, this);
    if (!created) return false;
    pair = std::move(*created);
    return true;
  }

  void Pump() noexcept {
    for (unsigned i = 0; i != 16; ++i)
      EXPECT_TRUE(pair->Channel().PollOnce().has_value());
  }

  void InsertMessage(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    v1::GreptimeRequest request;
    if (!request.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    writes.push_back(std::move(request));
    if (scenario == Scenario::kEarlyStatus) {
      v1::GreptimeResponse response;
      response.mutable_header()->mutable_status()->set_status_code(3001);
      response.mutable_header()->mutable_status()->set_err_msg("early rejection");
      response.mutable_affected_rows()->set_value(0);
      SendProto(server, event.stream_id, response);
      EXPECT_TRUE(server.Finish(event.stream_id, {}).has_value());
    }
  }

  void InsertFinish(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    if (scenario == Scenario::kTrailerError) {
      grpcxx::MetadataMap trailers;
      EXPECT_TRUE(trailers.Add("inner_error_msg", "{\"code\":4003,\"err_msg\":\"stream busy\"}").has_value());
      EXPECT_TRUE(trailers.Add("x-greptime-err-retry-hint", "true").has_value());
      EXPECT_TRUE(server.Finish(event.stream_id, {}, trailers).has_value());
      return;
    }
    v1::GreptimeResponse response;
    if (scenario == Scenario::kHeaderError) {
      auto *status = response.mutable_header()->mutable_status();
      status->set_status_code(3001);
      status->set_err_msg("destination absent");
      status->set_retry_hint("false");
    }
    if (scenario != Scenario::kMissingRows) response.mutable_affected_rows()->set_value(17);
    SendProto(server, event.stream_id, response);
    if (scenario == Scenario::kExtraResponse) SendProto(server, event.stream_id, response);
    EXPECT_TRUE(server.Finish(event.stream_id, {}).has_value());
  }

  void BulkMessage(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    af::FlightData frame;
    if (!frame.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    auto metadata = DoPutMetadata::Decode(frame.app_metadata());
    if (!metadata) {
      failed = true;
      return;
    }
    bulk_metadata.push_back(*metadata);
    bulk_frames.push_back(frame);
    if (metadata->request_id == 0) {
      SendAck(server, event.stream_id, scenario == Scenario::kBadHandshake ? 9 : 0, 0);
      return;
    }
    if (scenario == Scenario::kMissingAck) return;
    if (scenario == Scenario::kDelayedAck) {
      delayed_stream = event.stream_id;
      delayed_id = metadata->request_id;
      delay_ticks = 12;
      return;
    }
    auto id = metadata->request_id;
    if (scenario == Scenario::kOutOfOrderAck) ++id;
    if (scenario == Scenario::kUnknownAck) id = 999;
    SendAck(server, event.stream_id, id);
  }

  static auto DelayedProgress(void *context) noexcept -> std::expected<void, grpcxx::Status> {
    auto &self = *static_cast<Peer *>(context);
    auto polled = self.pair->Server().PollOnce();
    if (!polled) return polled;
    if (self.delayed_stream != 0) {
      ++self.delayed_polls;
      if (--self.delay_ticks == 0) {
        SendAck(self.pair->Server(), self.delayed_stream, self.delayed_id);
        if (self.delayed_finish) {
          EXPECT_TRUE(self.pair->Server().Finish(self.delayed_stream, {}).has_value());
        }
        self.delayed_stream = 0;
      }
    }
    return {};
  }

  void QueryMessage(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &event) noexcept {
    af::Ticket encoded;
    v1::GreptimeRequest request;
    if (!encoded.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size())) ||
        !request.ParseFromString(encoded.ticket())) {
      failed = true;
      return;
    }
    ticket = std::move(request);
    const auto batch = Batch();
    FlightIpcEncoder encoder;
    auto schema = encoder.EncodeSchema(batch.schema);
    auto body = encoder.EncodeBatch(batch);
    ASSERT_TRUE(schema.has_value());
    ASSERT_TRUE(body.has_value());
    if (scenario == Scenario::kBatchBeforeSchema) {
      SendBody(server, event.stream_id, *body);
      return;
    }
    SendMetadata(server, event.stream_id, 7, "before-schema");
    SendBody(server, event.stream_id, *schema);
    if (scenario == Scenario::kDuplicateSchema) SendBody(server, event.stream_id, *schema);
    SendBody(server, event.stream_id, *body);
    if (scenario == Scenario::kQueryError || scenario == Scenario::kQueryHold) return;
    SendMetadata(server, event.stream_id, 5, "between-batches");
    SendBody(server, event.stream_id, *body);
  }
};

} // namespace
