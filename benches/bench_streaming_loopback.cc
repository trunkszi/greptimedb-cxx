#include "bench_fixture.h"

#include <Flight.pb.h>
#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/query_stream.h>
#include <greptimedb/row/wire_row_view.h>
#include <greptimedb/transport/in_memory_channel_pair.h>

#include <array>
#include <chrono>
#include <span>
#include <string>

namespace greptimedb::bench_fixture {
namespace {

using namespace flight;
namespace af = ::arrow::flight::protocol;
namespace v1 = ::greptime::v1;
constexpr std::size_t kBatchesPerCall = 4;
constexpr std::string_view kPut = "/arrow.flight.protocol.FlightService/DoPut";
constexpr std::string_view kGet = "/arrow.flight.protocol.FlightService/DoGet";

/** A single caller drives both HTTP/2 peers. Query bodies are pre-encoded so
 * DoGet measures transport, client decoding and row projection. DoPut decodes
 * each received batch and withholds ACKs until the configured window fills. */
struct Peer {
  std::unique_ptr<InMemoryChannelPair> pair;
  RecordBatch batch;
  af::FlightData query_schema;
  af::FlightData query_batch;
  FlightSchema put_schema;
  std::array<std::int64_t, 4> pending_ids{};
  std::array<std::uint64_t, 4> pending_rows{};
  std::size_t pending_count{};
  std::size_t ack_window{1};
  std::uint32_t insert_rows{};
  std::uint64_t received_rows{};
  std::uint64_t acknowledged_rows{};
  bool failed{};

  template <class Message>
  bool Send(MockGrpcServer &server, std::int32_t id,
            const Message &response) noexcept {
    std::string payload;
    if (!response.SerializeToString(&payload) ||
        !server.Send(id, std::as_bytes(std::span(payload)))) {
      failed = true;
      return false;
    }
    return true;
  }

  bool Ack(MockGrpcServer &server, std::int32_t stream, std::int64_t id,
           std::uint64_t rows) noexcept {
    af::PutResult result;
    result.set_app_metadata("{\"request_id\":" + std::to_string(id) +
                            ",\"affected_rows\":" + std::to_string(rows) +
                            ",\"elapsed_secs\":0}");
    if (!Send(server, stream, result))
      return false;
    acknowledged_rows += rows;
    return true;
  }

  void FlushAcks(MockGrpcServer &server, std::int32_t stream) noexcept {
    for (std::size_t i = 0; i < pending_count; ++i) {
      if (!Ack(server, stream, pending_ids[i], pending_rows[i]))
        break;
    }
    pending_count = 0;
  }

  bool Open(std::size_t rows) {
    auto fixture = MakeBatch(rows);
    if (!fixture)
      return false;
    batch = std::move(*fixture);
    FlightIpcEncoder encoder;
    auto schema = encoder.EncodeSchema(batch.schema);
    auto body = encoder.EncodeBatch(batch);
    if (!schema || !body)
      return false;
    query_schema.set_data_header(schema->data_header.data(),
                                 schema->data_header.size());
    query_batch.set_data_header(body->data_header.data(),
                                body->data_header.size());
    query_batch.set_data_body(body->data_body.data(), body->data_body.size());
    auto opened = InMemoryChannelPair::Create(&Handle, this);
    if (!opened)
      return false;
    pair = std::move(*opened);
    return true;
  }

  void Insert(MockGrpcServer &server, const MockRequest &event) noexcept {
    v1::GreptimeRequest request;
    if (!request.ParseFromArray(event.payload.data(),
                                static_cast<int>(event.payload.size())) ||
        !request.has_inserts()) {
      failed = true;
      return;
    }
    for (const auto &insert : request.inserts().inserts()) {
      insert_rows += insert.row_count();
      received_rows += insert.row_count();
    }
    static_cast<void>(server);
  }

  void Put(MockGrpcServer &server, const MockRequest &event) noexcept {
    af::FlightData frame;
    if (!frame.ParseFromArray(event.payload.data(),
                              static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    auto metadata = DoPutMetadata::Decode(frame.app_metadata());
    if (!metadata) {
      failed = true;
      return;
    }
    const auto header = std::span(
        reinterpret_cast<const std::uint8_t *>(frame.data_header().data()),
        frame.data_header().size());
    if (metadata->request_id == 0) {
      auto schema = DecodeFlightSchema(header);
      if (!schema || pending_count != 0) {
        failed = true;
        return;
      }
      put_schema = std::move(*schema);
      static_cast<void>(Ack(server, event.stream_id, 0, 0));
      return;
    }
    const auto body = std::span(
        reinterpret_cast<const std::uint8_t *>(frame.data_body().data()),
        frame.data_body().size());
    auto decoded = DecodeFlightRecordBatch(put_schema, header, body);
    if (!decoded || pending_count >= pending_ids.size()) {
      failed = true;
      return;
    }
    received_rows += decoded->row_count;
    pending_ids[pending_count] = metadata->request_id;
    pending_rows[pending_count++] = decoded->row_count;
    if (pending_count == ack_window)
      FlushAcks(server, event.stream_id);
  }

  void Get(MockGrpcServer &server, const MockRequest &event) noexcept {
    af::Ticket ticket;
    v1::GreptimeRequest request;
    if (!ticket.ParseFromArray(event.payload.data(),
                               static_cast<int>(event.payload.size())) ||
        !request.ParseFromString(ticket.ticket()) || !request.has_query()) {
      failed = true;
      return;
    }
    if (!Send(server, event.stream_id, query_schema))
      return;
    for (std::size_t i = 0; i < kBatchesPerCall; ++i) {
      if (!Send(server, event.stream_id, query_batch))
        return;
    }
  }

  static void Handle(void *context, MockGrpcServer &server,
                     const MockRequest &event) noexcept {
    auto &self = *static_cast<Peer *>(context);
    switch (event.event) {
    case MockRequest::Event::kCancel:
      self.failed = true;
      return;
    case MockRequest::Event::kMessage:
      if (event.path == StreamInserter::kPath)
        self.Insert(server, event);
      else if (event.path == kPut)
        self.Put(server, event);
      else if (event.path == kGet)
        self.Get(server, event);
      else
        self.failed = true;
      if (self.failed) {
        static_cast<void>(server.Finish(event.stream_id,
                                        {grpcxx::StatusCode::kInternal,
                                         "benchmark peer rejected message"}));
      }
      return;
    case MockRequest::Event::kHalfClose:
      if (event.path == StreamInserter::kPath) {
        v1::GreptimeResponse response;
        response.mutable_affected_rows()->set_value(self.insert_rows);
        if (!self.Send(server, event.stream_id, response))
          return;
        self.insert_rows = 0;
      } else if (event.path == kPut) {
        self.FlushAcks(server, event.stream_id);
      }
      if (!server.Finish(event.stream_id))
        self.failed = true;
      return;
    }
  }
};

CallOptions CallBudget() {
  CallOptions options;
  options.timeout = std::chrono::seconds(2);
  return options;
}

void StreamInserterLoopback(benchmark::State &state) {
  Peer peer;
  if (!peer.Open(static_cast<std::size_t>(state.range(0)))) {
    state.SkipWithError("loopback fixture failed");
    return;
  }
  v1::InsertRequest request;
  request.set_table_name("benchmark_ticks");
  request.set_row_count(static_cast<std::uint32_t>(peer.batch.row_count));
  for (const auto &column : peer.batch.columns)
    *request.add_columns() = column;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    const auto before = peer.received_rows;
    auto stream = StreamInserter::Open(peer.pair->channel(), {}, CallBudget());
    if (!stream) {
      state.SkipWithError("StreamInserter open failed");
      return;
    }
    for (std::size_t i = 0; i < kBatchesPerCall; ++i) {
      if (!(*stream)->SendBatch(request)) {
        state.SkipWithError("StreamInserter send failed");
        return;
      }
    }
    auto result = (*stream)->Finish();
    const auto expected = peer.batch.row_count * kBatchesPerCall;
    if (!result || *result != expected ||
        peer.received_rows - before != expected || peer.failed) {
      state.SkipWithError("StreamInserter response/count mismatch");
      return;
    }
    benchmark::DoNotOptimize(*result);
  }
  Throughput(state, state.range(0) * kBatchesPerCall,
             LogicalBytes(peer.batch.row_count) * kBatchesPerCall);
}

void BulkWriterLoopback(benchmark::State &state) {
  Peer peer;
  peer.ack_window = static_cast<std::size_t>(state.range(1));
  if (!peer.Open(static_cast<std::size_t>(state.range(0)))) {
    state.SkipWithError("DoPut loopback fixture failed");
    return;
  }
  BulkWriteOptions options;
  options.max_in_flight = peer.ack_window;
  options.timeout = std::chrono::seconds(2);
  std::size_t high_water = 0;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    const auto before = peer.received_rows;
    const auto acked_before = peer.acknowledged_rows;
    auto writer =
        BulkStreamWriter::Open(peer.pair->channel(), "benchmark_ticks",
                               peer.batch.schema, options, CallBudget());
    if (!writer) {
      state.SkipWithError("DoPut handshake failed");
      return;
    }
    for (std::size_t i = 0; i < kBatchesPerCall; ++i) {
      if (!(*writer)->WriteBatch(peer.batch)) {
        state.SkipWithError("DoPut batch failed");
        return;
      }
      high_water = std::max(high_water, (*writer)->InFlight());
    }
    auto stats = (*writer)->Finish();
    const auto expected = peer.batch.row_count * kBatchesPerCall;
    if (!stats || stats->total_affected_rows != expected ||
        stats->batches_sent != kBatchesPerCall ||
        peer.received_rows - before != expected ||
        peer.acknowledged_rows - acked_before != expected || peer.failed ||
        high_water != peer.ack_window) {
      state.SkipWithError("DoPut ACK window or row accounting mismatch");
      return;
    }
    benchmark::DoNotOptimize(stats->total_affected_rows);
  }
  state.counters["max_in_flight"] = static_cast<double>(high_water);
  Throughput(state, state.range(0) * kBatchesPerCall,
             LogicalBytes(peer.batch.row_count) * kBatchesPerCall);
}

void QueryStreamLoopback(benchmark::State &state) {
  Peer peer;
  if (!peer.Open(static_cast<std::size_t>(state.range(0)))) {
    state.SkipWithError("DoGet loopback fixture failed");
    return;
  }
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto stream =
        QueryStream::Sql(peer.pair->channel(), {},
                         "SELECT * FROM benchmark_ticks", CallBudget());
    if (!stream) {
      state.SkipWithError("DoGet open failed");
      return;
    }
    std::size_t rows = 0;
    double sum = 0;
    for (;;) {
      auto next = (*stream)->NextBatch();
      if (!next) {
        state.SkipWithError("DoGet decode failed");
        return;
      }
      if (!*next)
        break;
      const auto &batch = **next;
      for (std::size_t row = 0; row < batch.row_count; ++row) {
        auto view = ViewDecodedRow<Tick>(batch.columns, row, batch.row_count);
        if (!view) {
          state.SkipWithError("DoGet RowView projection failed");
          return;
        }
        sum += view->Get<2>();
        benchmark::DoNotOptimize(view->Get<0>().data());
      }
      rows += batch.row_count;
    }
    if (!(*stream)->Finish() ||
        rows != peer.batch.row_count * kBatchesPerCall || peer.failed) {
      state.SkipWithError("DoGet terminal status or row count mismatch");
      return;
    }
    benchmark::DoNotOptimize(sum);
  }
  Throughput(state, state.range(0) * kBatchesPerCall,
             LogicalBytes(peer.batch.row_count) * kBatchesPerCall);
}

BENCHMARK(StreamInserterLoopback)->Arg(32)->Arg(256);
BENCHMARK(BulkWriterLoopback)
    ->Args({32, 1})
    ->Args({32, 4})
    ->Args({256, 1})
    ->Args({256, 4});
BENCHMARK(QueryStreamLoopback)->Arg(32)->Arg(256);

} // namespace
} // namespace greptimedb::bench_fixture
