/** Adversarial regressions across batch, row, Arrow IPC, and streaming. */
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/client/stream_inserter.h>
#include <greptimedb/flight/bulk_writer.h>
#include <greptimedb/flight/ipc_codec.h>
#include <greptimedb/flight/query_stream.h>
#include <greptimedb/row/reflected_row.h>
#include <greptimedb/row/wire_row_view.h>
#include <greptimedb/transport/in_memory_channel_pair.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace allocation_probe {
thread_local bool enabled = false;
thread_local bool fail_nothrow = false;
thread_local std::size_t nothrow_remaining = 0;

void *Allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t)) noexcept {
  size = std::max(size, std::size_t{1});
  if (alignment <= alignof(std::max_align_t)) return std::malloc(size);
  void *result = nullptr;
  return posix_memalign(&result, alignment, size) == 0 ? result : nullptr;
}
void *Require(void *memory) noexcept {
  if (!memory) std::abort();
  return memory;
}
bool RejectNothrow() noexcept {
  if (!fail_nothrow) return false;
  if (nothrow_remaining == 0) return true;
  --nothrow_remaining;
  return false;
}
} // namespace allocation_probe

void *operator new(std::size_t size) { return allocation_probe::Require(allocation_probe::Allocate(size)); }
void *operator new[](std::size_t size) { return ::operator new(size); }
void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  return allocation_probe::RejectNothrow() ? nullptr : allocation_probe::Allocate(size);
}
void *operator new[](std::size_t size, const std::nothrow_t &tag) noexcept {
  return ::operator new(size, tag);
}
void *operator new(std::size_t size, std::align_val_t alignment) {
  return allocation_probe::Require(
      allocation_probe::Allocate(size, static_cast<std::size_t>(alignment)));
}
void *operator new[](std::size_t size, std::align_val_t alignment) {
  return ::operator new(size, alignment);
}
void *operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t &) noexcept {
  return allocation_probe::RejectNothrow()
             ? nullptr
             : allocation_probe::Allocate(size, static_cast<std::size_t>(alignment));
}
void *operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t &tag) noexcept {
  return ::operator new(size, alignment, tag);
}
void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete(void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete[](void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete[](void *memory, std::align_val_t, const std::nothrow_t &) noexcept { std::free(memory); }

namespace {
using namespace greptimedb;
namespace v1 = ::greptime::v1;
namespace af = ::arrow::flight::protocol;
namespace ipc = ::greptimedb::flight::ipc;
using Bytes = std::vector<std::uint8_t>;

struct BorrowedDocument {
  [[= timestamp{}]] TimestampNanosecond time;
  std::string_view text;
  BinaryView bytes;
  [[= field{.json = true}]] std::string_view document;
};

struct WireRow {
  [[= timestamp{.unit = TimestampUnit::kMillisecond}]] TimestampMillisecond time;
  std::optional<std::int32_t> value;
};

Bytes ReadFixture(std::string_view name) {
  std::ifstream input(std::string(ADVERSARIAL_FIXTURE_DIR) + "/" + std::string(name),
                      std::ios::binary);
  return Bytes(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

void WriteLittle(std::vector<std::uint8_t> &bytes, std::size_t at,
                 std::uint64_t value, std::size_t width) {
  for (std::size_t i = 0; i < width; ++i) bytes[at + i] = value >> (8 * i);
}

Bytes Encapsulate(const Bytes &raw, bool marker) {
  Bytes result(marker ? 8 : 4);
  if (marker) WriteLittle(result, 0, 0xffffffffU, 4);
  WriteLittle(result, marker ? 4 : 0, raw.size(), 4);
  result.insert(result.end(), raw.begin(), raw.end());
  return result;
}

flight::RecordBatch Int32Batch(std::size_t rows) {
  greptime::v1::Column column;
  column.set_column_name("value");
  column.set_datatype(greptime::v1::INT32);
  column.set_semantic_type(greptime::v1::FIELD);
  for (std::size_t row = 0; row < rows; ++row)
    column.mutable_values()->add_i32_values(static_cast<std::int32_t>(row));
  auto batch = flight::RecordBatchFromProtoColumns(std::span{&column, 1}, rows);
  EXPECT_TRUE(batch);
  return batch ? std::move(*batch) : flight::RecordBatch{};
}

template <class Message>
void SendProto(MockGrpcServer &server, std::int32_t id,
               const Message &message) noexcept {
  std::string encoded;
  EXPECT_TRUE(message.SerializeToString(&encoded));
  EXPECT_TRUE(server.Send(id, std::as_bytes(std::span{encoded})).has_value());
}

void SendFrame(MockGrpcServer &server, std::int32_t id,
               std::span<const std::uint8_t> header,
               std::span<const std::uint8_t> body = {}) noexcept {
  af::FlightData frame;
  frame.set_data_header(header.data(), header.size());
  if (!body.empty()) frame.set_data_body(body.data(), body.size());
  SendProto(server, id, frame);
}

enum class PeerMode {
  kNormal,
  kStreamTrailer,
  kBulkUnknown,
  kBulkDuplicate,
  kBulkOutOfOrder,
  kQueryDictionary,
  kQueryTruncated,
  kQueryHold
};

struct Peer {
  explicit Peer(PeerMode value = PeerMode::kNormal) : mode(value) {}

  PeerMode mode{PeerMode::kNormal};
  std::unique_ptr<InMemoryChannelPair> pair;
  bool failed{};

  static void Handle(void *context, MockGrpcServer &server,
                     const MockRequest &event) noexcept {
    auto &self = *static_cast<Peer *>(context);
    if (event.event == MockRequest::Event::kCancel) return;
    if (event.path == StreamInserter::kPath) {
      if (event.event == MockRequest::Event::kHalfClose) self.FinishInsert(server, event);
      return;
    }
    if (event.path == "/arrow.flight.protocol.FlightService/DoPut") {
      if (event.event == MockRequest::Event::kHalfClose)
        EXPECT_TRUE(server.Finish(event.stream_id).has_value());
      else
        self.BulkMessage(server, event);
      return;
    }
    if (event.path == "/arrow.flight.protocol.FlightService/DoGet") {
      if (event.event == MockRequest::Event::kMessage) {
        self.QueryStart(server, event);
      } else if (event.event == MockRequest::Event::kHalfClose &&
                 self.mode != PeerMode::kQueryHold) {
        EXPECT_TRUE(server.Finish(event.stream_id).has_value());
      }
      return;
    }
    self.failed = true;
  }

  bool Open() {
    auto created = InMemoryChannelPair::Create(&Handle, this);
    if (!created) return false;
    pair = std::move(*created);
    return true;
  }

  void Pump() {
    for (unsigned tick = 0; tick < 16; ++tick)
      EXPECT_TRUE(pair->channel().PollOnce().has_value());
  }

  void FinishInsert(MockGrpcServer &server, const MockRequest &event) noexcept {
    if (mode == PeerMode::kStreamTrailer) {
      grpcxx::MetadataMap trailers;
      EXPECT_TRUE(trailers.Add("INNER_ERROR_MSG",
                               R"({"code":4003,"err_msg":"stream busy"})")
                      .has_value());
      EXPECT_TRUE(trailers.Add("x-greptime-err-retry-hint", "true").has_value());
      EXPECT_TRUE(server.Finish(event.stream_id, {}, trailers).has_value());
      return;
    }
    v1::GreptimeResponse response;
    response.mutable_affected_rows()->set_value(3);
    SendProto(server, event.stream_id, response);
    EXPECT_TRUE(server.Finish(event.stream_id).has_value());
  }

  void Ack(MockGrpcServer &server, std::int32_t id, std::int64_t request_id) noexcept {
    af::PutResult result;
    result.set_app_metadata(R"({"request_id":)" + std::to_string(request_id) +
                            R"(,"affected_rows":1,"elapsed_secs":0.25})");
    SendProto(server, id, result);
  }

  void BulkMessage(MockGrpcServer &server, const MockRequest &event) noexcept {
    af::FlightData frame;
    if (!frame.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    auto metadata = flight::DoPutMetadata::Decode(frame.app_metadata());
    if (!metadata) {
      failed = true;
      return;
    }
    if (metadata->request_id == 0) {
      Ack(server, event.stream_id, 0);
      return;
    }
    switch (mode) {
    case PeerMode::kBulkUnknown:
      Ack(server, event.stream_id, 99);
      break;
    case PeerMode::kBulkDuplicate:
      Ack(server, event.stream_id, metadata->request_id);
      Ack(server, event.stream_id, metadata->request_id);
      break;
    case PeerMode::kBulkOutOfOrder:
      Ack(server, event.stream_id, metadata->request_id + 1);
      break;
    default:
      Ack(server, event.stream_id, metadata->request_id);
      break;
    }
  }

  void QueryStart(MockGrpcServer &server, const MockRequest &event) noexcept {
    af::Ticket encoded;
    if (!encoded.ParseFromArray(event.payload.data(), static_cast<int>(event.payload.size()))) {
      failed = true;
      return;
    }
    auto batch = Int32Batch(2);
    flight::FlightIpcEncoder encoder;
    auto schema = encoder.EncodeSchema(batch.schema);
    auto body = encoder.EncodeBatch(batch);
    EXPECT_TRUE(schema.has_value());
    EXPECT_TRUE(body.has_value());
    if (!schema || !body) {
      failed = true;
      return;
    }
    SendFrame(server, event.stream_id, schema->data_header);
    if (mode == PeerMode::kQueryDictionary) {
      auto header = ReadFixture("dictionary.header.bin");
      auto message = ipc::DecodeMessage(header);
      EXPECT_TRUE(message.has_value());
      if (message)
        SendFrame(server, event.stream_id, header,
                  std::vector<std::uint8_t>(static_cast<std::size_t>(message->body_length)));
      return;
    }
    if (mode == PeerMode::kQueryTruncated) {
      SendFrame(server, event.stream_id, std::span{schema->data_header}.first(2));
      return;
    }
    SendFrame(server, event.stream_id, body->data_header, body->data_body);
    if (mode == PeerMode::kQueryHold) return;
  }
};

TEST(AdversarialIpc, RejectsMalformedContinuationCompressionAndUnalignedBuffers) {
  auto batch = Int32Batch(11);
  auto schema = flight::FlightIpcEncoder().EncodeSchema(batch.schema);
  auto frame = flight::FlightIpcEncoder().EncodeBatch(batch);
  ASSERT_TRUE(schema);
  ASSERT_TRUE(frame);
  for (std::size_t size = 0; size < 12; ++size) {
    EXPECT_FALSE(flight::DecodeFlightSchema(std::span(schema->data_header).first(size)));
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(
        batch.schema, std::span(frame->data_header).first(size), frame->data_body));
  }
  auto bad = Encapsulate(schema->data_header, true);
  WriteLittle(bad, 4, 0, 4);
  EXPECT_FALSE(ipc::DecodeMessage(bad));
  bad = Encapsulate(schema->data_header, true);
  WriteLittle(bad, 4, bad.size(), 4);
  EXPECT_FALSE(ipc::DecodeMessage(bad));
  bad = Encapsulate(schema->data_header, true);
  WriteLittle(bad, 0, 0x12345678U, 4);
  EXPECT_FALSE(ipc::DecodeMessage(bad));
  auto body = frame->data_body;
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(
      batch.schema, frame->data_header, std::span(body).first(body.size() - 1)));
  auto message = ipc::DecodeMessage(frame->data_header);
  ASSERT_TRUE(message);
  auto meta = message->batch;
  ASSERT_FALSE(meta.buffers.empty());
  meta.buffers[0].offset += 1;
  auto unaligned = ipc::EncodeRecordBatch(meta, body.size());
  ASSERT_TRUE(unaligned);
  EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch.schema, *unaligned, body));
  for (auto compression :
       {flight::FlightCompression::kLz4Frame, flight::FlightCompression::kZstd}) {
    auto compressed = flight::FlightIpcEncoder(compression).EncodeBatch(batch);
    ASSERT_TRUE(compressed);
    auto decoded = ipc::DecodeMessage(compressed->data_header);
    ASSERT_TRUE(decoded);
    const auto buffer = decoded->batch.buffers[1];
    auto corrupt = compressed->data_body;
    corrupt[static_cast<std::size_t>(buffer.offset + 8)] ^= 0xff;
    EXPECT_FALSE(flight::DecodeFlightRecordBatch(batch.schema, compressed->data_header, corrupt));
  }
}

TEST(AdversarialIpc, DecimalScaleBoundariesAndDictionaryDetection) {
  auto schema = [](std::int32_t scale) {
    ipc::Schema value;
    ipc::Field field;
    field.name = "decimal";
    field.type.id = ipc::TypeId::kDecimal;
    field.type.bit_width = 128;
    field.type.decimal_precision = 38;
    field.type.scale = scale;
    value.fields.push_back(field);
    return value;
  };
  auto lower = ipc::EncodeSchema(schema(-128));
  ASSERT_TRUE(lower) << lower.error();
  auto decoded = ipc::DecodeMessage(*lower);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->schema.fields[0].type.scale, -128);
  EXPECT_FALSE(ipc::EncodeSchema(schema(-129)));
  EXPECT_FALSE(ipc::EncodeSchema(schema(128)));
  auto dictionary = ReadFixture("dictionary.header.bin");
  auto message = ipc::DecodeMessage(dictionary);
  ASSERT_TRUE(message);
  EXPECT_EQ(message->header_type, ipc::HeaderType::kDictionaryBatch);
  EXPECT_FALSE(flight::DecodeFlightSchema(dictionary));
}

TEST(AdversarialNullMask, CoversEveryByteBoundaryAndClearState) {
  for (std::size_t rows : std::array<std::size_t, 8>{0, 1, 7, 8, 9, 63, 64, 65}) {
    NullMask mask(rows);
    for (std::size_t row = 0; row < rows; ++row)
      ASSERT_TRUE(mask.Push(row == 0 || row == rows - 1));
    EXPECT_EQ(mask.RowCount(), rows);
    if (rows == 0) {
      EXPECT_TRUE(mask.Bytes().empty());
      continue;
    }
    EXPECT_EQ(mask.NullCount(), rows == 1 ? 1U : 2U);
    EXPECT_EQ(mask.Bytes().size(), (rows + 7) / 8);
    EXPECT_TRUE(mask.IsNull(0));
    EXPECT_TRUE(mask.IsNull(rows - 1));
    if (rows > 2) {
      EXPECT_FALSE(mask.IsNull(1));
    }
  }
  NullMask mask(9);
  for (int row = 0; row < 9; ++row) ASSERT_TRUE(mask.Push(false));
  EXPECT_TRUE(mask.Bytes().empty());
  mask.ClearUnbounded();
  EXPECT_EQ(mask.RowCount(), 0U);
  EXPECT_TRUE(mask.Bytes().empty());
}

TEST(AdversarialWireRow, RejectsNullCountMismatchNonNullableNullAndUnitMismatch) {
  TypedColumnarBatch<WireRow> batch;
  ASSERT_TRUE(batch.Push(WireRow{TimestampMillisecond{1}, 10}));
  ASSERT_TRUE(batch.Push(WireRow{TimestampMillisecond{2}, std::nullopt}));
  auto wire = batch.EncodeInsertRequest();
  ASSERT_TRUE(wire);
  ASSERT_EQ(wire->row_count(), 2U);
  auto first = ViewDecodedRow<WireRow>(wire->columns(), 0, wire->row_count());
  ASSERT_TRUE(first);
  EXPECT_EQ(first->Get<0>(), TimestampMillisecond{1});
  EXPECT_EQ(first->Get<1>(), std::optional<std::int32_t>{10});
  auto damaged = *wire;
  damaged.mutable_columns(1)->mutable_values()->add_i32_values(99);
  EXPECT_FALSE(ViewDecodedRow<WireRow>(damaged.columns(), 0, damaged.row_count()));
  damaged = *wire;
  damaged.mutable_columns(0)->set_null_mask(std::string(1, '\x01'));
  EXPECT_FALSE(ViewDecodedRow<WireRow>(damaged.columns(), 1, damaged.row_count()));
  damaged = *wire;
  damaged.mutable_columns(0)->set_datatype(greptime::v1::TIMESTAMP_SECOND);
  EXPECT_FALSE(ViewDecodedRow<WireRow>(damaged.columns(), 0, damaged.row_count()));
  damaged = *wire;
  damaged.mutable_columns(0)->set_column_name("wrong");
  EXPECT_FALSE(ViewDecodedRow<WireRow>(damaged.columns(), 0, damaged.row_count()));
  EXPECT_FALSE(ViewDecodedRow<WireRow>(wire->columns(), 2, wire->row_count()));
}

TEST(AdversarialBufferPolicy, ExactWatermarksRetainAndOneLessReleases) {
  const BufferPolicy exact{8, 64};
  EXPECT_FALSE(exact.ShouldShrink(8, 64));
  EXPECT_TRUE(exact.ShouldShrink(9, 0));
  EXPECT_TRUE(exact.ShouldShrink(0, 65));
  EXPECT_FALSE(BufferPolicy::Unbounded().ShouldShrink(
      std::numeric_limits<std::size_t>::max(), std::numeric_limits<std::size_t>::max()));
  EXPECT_TRUE(BufferPolicy::Zero().ShouldShrink(1, 0));
  ColumnarBatch batch("ticks", 16);
  ASSERT_TRUE(batch.AddTimestampColumn("time", TimestampUnit::kMillisecond));
  const std::array row{ValueView{TimestampMillisecond{1}}};
  ASSERT_TRUE(batch.PushRow(row));
  const auto bytes = batch.AllocatedBytes();
  const auto capacity = batch.RowCapacity();
  EXPECT_GT(bytes, 0U);
  batch.ClearUnbounded();
  EXPECT_EQ(batch.AllocatedBytes(), bytes);
  EXPECT_EQ(batch.RowCapacity(), capacity);
  batch.ClearWithPolicy(BufferPolicy{capacity, bytes});
  EXPECT_EQ(batch.AllocatedBytes(), bytes);
  EXPECT_EQ(batch.RowCapacity(), capacity);
  batch.ClearWithPolicy(BufferPolicy{capacity - 1, bytes});
  EXPECT_EQ(batch.AllocatedBytes(), 0U);
  EXPECT_EQ(batch.RowCapacity(), 0U);
}

TEST(AdversarialReflected, SelfAliasingAndMidRowAllocationFailureStayAtomic) {
  TypedColumnarBatch<BorrowedDocument> batch{4};
  const std::array initial{std::byte{3}, std::byte{4}};
  ASSERT_TRUE(batch.Push(BorrowedDocument{TimestampNanosecond{1}, "original", initial, "{}"}));
  auto view = batch.GetRow(0);
  ASSERT_TRUE(view);
  const BorrowedRow<BorrowedDocument> alias{TimestampNanosecond{2}, view->Get<1>(),
                                             view->Get<2>(), view->Get<3>()};
  auto aliased = batch.PushRef(alias);
  ASSERT_FALSE(aliased);
  EXPECT_EQ(aliased.error(), ColumnBufError::kInvalidSchema);
  EXPECT_EQ(batch.RowCount(), 1U);
  const std::string text(1024, 'x');
  const std::vector<std::byte> payload(1024, std::byte{7});
  const BorrowedDocument next{TimestampNanosecond{3}, text, payload, "[]"};
  allocation_probe::nothrow_remaining = 1;
  allocation_probe::fail_nothrow = true;
  auto failed = batch.Push(next);
  allocation_probe::fail_nothrow = false;
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error(), ColumnBufError::kAllocationFailure);
  EXPECT_EQ(batch.RowCount(), 1U);
  EXPECT_EQ(batch.Column<0>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<1>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<2>().RowCount(), 1U);
  EXPECT_EQ(batch.Column<3>().RowCount(), 1U);
  ASSERT_TRUE(batch.Push(next));
  EXPECT_EQ(batch.RowCount(), 2U);
}

TEST(AdversarialStreaming, TrailerOnlyErrorAndDropCancelKeepChannelReusable) {
  {
    Peer peer{PeerMode::kStreamTrailer};
    ASSERT_TRUE(peer.Open());
    auto stream = StreamInserter::Open(peer.pair->channel());
    ASSERT_TRUE(stream);
    auto failed = (*stream)->Finish();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().Kind(), ErrorKind::kServerStatus);
    EXPECT_EQ(failed.error().RawCode(), 4003U);
    EXPECT_EQ(failed.error().RetryHint(), true);
    peer.mode = PeerMode::kNormal;
    auto recovery = StreamInserter::Open(peer.pair->channel());
    ASSERT_TRUE(recovery);
    EXPECT_TRUE((*recovery)->Finish());
  }
  {
    Peer peer;
    ASSERT_TRUE(peer.Open());
    {
      auto stream = StreamInserter::Open(peer.pair->channel());
      ASSERT_TRUE(stream);
      v1::InsertRequest insert;
      insert.set_table_name("ticks");
      insert.set_row_count(1);
      ASSERT_TRUE((*stream)->SendBatch(insert));
      peer.Pump();
    }
    peer.Pump();
    EXPECT_EQ(peer.pair->server().CancelledStreams(), 1U);
    auto recovery = StreamInserter::Open(peer.pair->channel());
    ASSERT_TRUE(recovery);
    EXPECT_TRUE((*recovery)->Finish());
  }
}

TEST(AdversarialStreaming, BulkAckAnomaliesLatchAndChannelRecovers) {
  for (auto mode : {PeerMode::kBulkUnknown, PeerMode::kBulkDuplicate,
                    PeerMode::kBulkOutOfOrder}) {
    Peer peer{mode};
    ASSERT_TRUE(peer.Open());
    auto batch = Int32Batch(1);
    auto writer = flight::BulkStreamWriter::Open(peer.pair->channel(), "ticks", batch.schema);
    ASSERT_TRUE(writer);
    ASSERT_TRUE((*writer)->WriteBatch(batch));
    auto failed = (*writer)->Finish();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().Kind(), ErrorKind::kProtocol);
    peer.mode = PeerMode::kNormal;
    auto recovery =
        flight::BulkStreamWriter::Open(peer.pair->channel(), "ticks", batch.schema);
    ASSERT_TRUE(recovery);
    ASSERT_TRUE((*recovery)->WriteBatch(batch));
    EXPECT_TRUE((*recovery)->Finish());
    EXPECT_FALSE(peer.failed);
  }
}

TEST(AdversarialStreaming, BulkDestructionCancelsAndChannelRecovers) {
  Peer peer;
  ASSERT_TRUE(peer.Open());
  auto batch = Int32Batch(1);
  {
    auto writer =
        flight::BulkStreamWriter::Open(peer.pair->channel(), "ticks", batch.schema);
    ASSERT_TRUE(writer);
    ASSERT_TRUE((*writer)->WriteBatch(batch));
    peer.Pump();
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->server().CancelledStreams(), 1U);
  auto recovery =
      flight::BulkStreamWriter::Open(peer.pair->channel(), "ticks", batch.schema);
  ASSERT_TRUE(recovery);
  ASSERT_TRUE((*recovery)->WriteBatch(batch));
  EXPECT_TRUE((*recovery)->Finish());
}

TEST(AdversarialStreaming, QueryRejectsDictionaryAndTruncation) {
  for (auto mode : {PeerMode::kQueryDictionary, PeerMode::kQueryTruncated}) {
    Peer peer{mode};
    ASSERT_TRUE(peer.Open());
    auto query = flight::QueryStream::Sql(peer.pair->channel(), {}, "SELECT 1");
    ASSERT_TRUE(query);
    auto result = (*query)->NextBatch();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().Kind(),
              mode == PeerMode::kQueryDictionary ? ErrorKind::kProtocol : ErrorKind::kDecode);
    EXPECT_FALSE(peer.failed);
  }
}

TEST(AdversarialStreaming, QueryDestructionCancelsAndChannelRecovers) {
  Peer peer{PeerMode::kQueryHold};
  ASSERT_TRUE(peer.Open());
  {
    auto query = flight::QueryStream::Sql(peer.pair->channel(), {}, "SELECT 1");
    ASSERT_TRUE(query);
    auto batch = (*query)->NextBatch();
    ASSERT_TRUE(batch);
    ASSERT_TRUE(batch->has_value());
  }
  peer.Pump();
  EXPECT_EQ(peer.pair->server().CancelledStreams(), 1U);
  peer.mode = PeerMode::kNormal;
  auto recovery = flight::QueryStream::Sql(peer.pair->channel(), {}, "SELECT 1");
  ASSERT_TRUE(recovery);
  EXPECT_TRUE((*recovery)->Finish());
}

} // namespace
