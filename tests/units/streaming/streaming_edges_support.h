#pragma once
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/query_stream.h>
#include <grpcxx/sync/in_memory_channel_pair.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace greptimedb::flight::testing {
namespace {
using ::arrow::flight::protocol::FlightData;
using ::arrow::flight::protocol::PutResult;

RecordBatch SampleBatch() {
  ::greptime::v1::Column column;
  column.set_column_name("value");
  column.set_datatype(::greptime::v1::INT64);
  column.set_semantic_type(::greptime::v1::FIELD);
  column.mutable_values()->add_i64_values(37);
  auto batch = RecordBatchFromProtoColumns(std::span{&column, 1}, 1);
  EXPECT_TRUE(batch);
  return batch ? std::move(*batch) : RecordBatch{};
}

template <typename Message>
bool Send(grpcxx::SynchronousServer &server, std::int32_t stream, const Message &message) noexcept {
  std::string wire;
  return message.SerializeToString(&wire) && server.Send(stream, std::as_bytes(std::span{wire})).has_value();
}

FlightData Frame(const EncodedFlightBody &body) {
  FlightData frame;
  frame.set_data_header(body.data_header.data(), body.data_header.size());
  if (!body.data_body.empty()) frame.set_data_body(body.data_body.data(), body.data_body.size());
  return frame;
}

FlightData MetadataFrame(const ::greptime::v1::FlightMetadata &metadata) {
  FlightData frame;
  auto header = ipc::EncodeMetadataMessage();
  EXPECT_TRUE(header);
  if (header) frame.set_data_header(header->data(), header->size());
  EXPECT_TRUE(metadata.SerializeToString(frame.mutable_app_metadata()));
  return frame;
}

struct BulkPeer {
  enum class Behavior {
    kNormal,
    kUnknown,
    kDuplicate,
    kMissing,
    kEarlyEof,
    kDeniedTrailer,
    kEarlySuccess,
    kEarlyDenied
  };
  Behavior behavior{Behavior::kNormal};
  std::size_t batches{};
  bool ok{true};
  bool ended{};

  void Ack(grpcxx::SynchronousServer &server, std::int32_t stream, std::int64_t id) noexcept {
    PutResult result;
    result.set_app_metadata("{\"request_id\":" + std::to_string(id) + ",\"affected_rows\":1,\"elapsed_secs\":0.25}");
    ok &= Send(server, stream, result);
  }

  static void Handle(void *context, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &request) noexcept {
    auto &self = *static_cast<BulkPeer *>(context);
    if (request.event == grpcxx::ServerRequest::Event::kCancel) return;
    if (request.event == grpcxx::ServerRequest::Event::kHalfClose) {
      if (!self.ended) {
        const auto status = self.behavior == Behavior::kDeniedTrailer
                                ? grpcxx::Status{grpcxx::StatusCode::kPermissionDenied, "write denied"}
                                : grpcxx::Status{};
        self.ok &= server.Finish(request.stream_id, status).has_value();
        self.ended = true;
      }
      return;
    }
    self.ok &= request.path == "/arrow.flight.protocol.FlightService/DoPut";
    FlightData frame;
    if (!frame.ParseFromArray(request.payload.data(), static_cast<int>(request.payload.size()))) {
      self.ok = false;
      return;
    }
    auto metadata = DoPutMetadata::Decode(frame.app_metadata());
    if (!metadata) {
      self.ok = false;
      return;
    }
    if (metadata->request_id == 0) {
      self.Ack(server, request.stream_id, 0);
      if (self.behavior == Behavior::kEarlyEof) {
        self.ok &= server.Finish(request.stream_id, {}).has_value();
        self.ended = true;
      }
      return;
    }
    ++self.batches;
    switch (self.behavior) {
    case Behavior::kUnknown: self.Ack(server, request.stream_id, 99); break;
    case Behavior::kDuplicate:
      self.Ack(server, request.stream_id, metadata->request_id);
      self.Ack(server, request.stream_id, metadata->request_id);
      break;
    case Behavior::kMissing:
    case Behavior::kEarlyEof: break;
    case Behavior::kNormal:
    case Behavior::kDeniedTrailer:
    case Behavior::kEarlySuccess:
    case Behavior::kEarlyDenied:
      self.Ack(server, request.stream_id, metadata->request_id);
      if (self.behavior == Behavior::kEarlySuccess || self.behavior == Behavior::kEarlyDenied) {
        const auto status = self.behavior == Behavior::kEarlyDenied
                                ? grpcxx::Status{grpcxx::StatusCode::kPermissionDenied, "early denial"}
                                : grpcxx::Status{};
        self.ok &= server.Finish(request.stream_id, status).has_value();
        self.ended = true;
      }
      break;
    }
  }
};

inline RecordBatch HourBoundaryBatch() {
  std::array<::greptime::v1::Column, 2> columns;
  columns[0].set_column_name("ts");
  columns[0].set_datatype(::greptime::v1::TIMESTAMP_NANOSECOND);
  columns[0].set_semantic_type(::greptime::v1::TIMESTAMP);
  columns[1].set_column_name("value");
  columns[1].set_datatype(::greptime::v1::INT64);
  columns[1].set_semantic_type(::greptime::v1::FIELD);
  std::int64_t index = 0;
  for (auto timestamp :
       {3600000000000LL, -1LL, 0LL, -3600000000001LL, -3600000000000LL, 9007199254740993LL, 9007199254740994LL}) {
    columns[0].mutable_values()->add_timestamp_nanosecond_values(timestamp);
    columns[1].mutable_values()->add_i64_values(index++);
  }
  auto batch = RecordBatchFromProtoColumns(columns, 7);
  EXPECT_TRUE(batch);
  return batch ? std::move(*batch) : RecordBatch{};
}

struct PartitionPeer {
  std::optional<FlightSchema> schema;
  std::vector<RecordBatch> batches;
  std::vector<DoPutMetadata> metadata;
  std::vector<ipc::Compression> compression;
  std::size_t messages{};
  bool ok{true};
  bool reject_second_batch{};

  static void Handle(void *context, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &request) noexcept {
    auto &self = *static_cast<PartitionPeer *>(context);
    if (request.event == grpcxx::ServerRequest::Event::kCancel) return;
    if (request.event == grpcxx::ServerRequest::Event::kHalfClose) {
      self.ok &= server.Finish(request.stream_id, {}).has_value();
      return;
    }
    ++self.messages;
    FlightData frame;
    self.ok &= request.path == "/arrow.flight.protocol.FlightService/DoPut";
    if (!frame.ParseFromArray(request.payload.data(), static_cast<int>(request.payload.size()))) {
      self.ok = false;
      return;
    }
    auto meta = DoPutMetadata::Decode(frame.app_metadata());
    if (!meta) {
      self.ok = false;
      return;
    }
    const auto header =
        std::span{reinterpret_cast<const std::uint8_t *>(frame.data_header().data()), frame.data_header().size()};
    std::size_t rows = 0;
    if (meta->request_id == 0) {
      auto decoded = DecodeFlightSchema(header);
      if (!decoded) {
        self.ok = false;
        return;
      }
      self.schema = std::move(*decoded);
      self.ok &= frame.flight_descriptor().type() == ::arrow::flight::protocol::FlightDescriptor::PATH;
      self.ok &= frame.flight_descriptor().path_size() == 1 && frame.flight_descriptor().path(0) == "samples";
    } else {
      if (!self.schema) {
        self.ok = false;
        return;
      }
      const auto body =
          std::span{reinterpret_cast<const std::uint8_t *>(frame.data_body().data()), frame.data_body().size()};
      auto decoded = DecodeFlightRecordBatch(*self.schema, header, body);
      auto message = ipc::DecodeMessage(header);
      if (!decoded || !message) {
        self.ok = false;
        return;
      }
      rows = decoded->row_count;
      self.batches.push_back(std::move(*decoded));
      self.metadata.push_back(*meta);
      self.compression.push_back(message->batch.compression);
    }
    PutResult ack;
    const auto ack_id = self.reject_second_batch && meta->request_id == 2 ? 999 : meta->request_id;
    ack.set_app_metadata("{\"request_id\":" + std::to_string(ack_id) + ",\"affected_rows\":" + std::to_string(rows) +
                         ",\"elapsed_secs\":0.125}");
    self.ok &= Send(server, request.stream_id, ack);
  }
};

struct QueryPeer {
  std::vector<FlightData> frames;
  bool ok{true};
  static void Handle(void *context, grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &request) noexcept {
    auto &self = *static_cast<QueryPeer *>(context);
    if (request.event != grpcxx::ServerRequest::Event::kMessage) return;
    self.ok &= request.path == "/arrow.flight.protocol.FlightService/DoGet";
    for (const auto &frame : self.frames)
      self.ok &= Send(server, request.stream_id, frame);
    self.ok &= server.Finish(request.stream_id, {}).has_value();
  }
};

void ExpectQueryFailure(std::vector<FlightData> frames, ErrorKind kind, std::string_view diagnostic) {
  QueryPeer peer{std::move(frames)};
  auto pair = grpcxx::InMemoryChannelPair::Create(QueryPeer::Handle, &peer);
  ASSERT_TRUE(pair);
  auto stream = QueryStream::Sql((*pair)->Channel(), {}, "SELECT value FROM samples");
  ASSERT_TRUE(stream);
  auto batch = (*stream)->NextBatch();
  ASSERT_FALSE(batch);
  EXPECT_EQ(batch.error().Kind(), kind);
  EXPECT_NE(batch.error().Message().find(diagnostic), std::string_view::npos);
  auto repeated = (*stream)->NextBatch();
  ASSERT_FALSE(repeated);
  EXPECT_EQ(repeated.error().Message(), batch.error().Message());
  EXPECT_TRUE(peer.ok);
}

} // namespace
} // namespace greptimedb::flight::testing
