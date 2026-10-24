/** Generated GreptimeDB messages and contracts over a caller-driven HTTP/2
 * loopback. */
#include <greptime/v1/flow/server.pb.h>
#include <greptime/v1/region/server.pb.h>
#include <greptimedb/proto/greptime_proto.h>
#include <grpcxx/codec/protobuf_codec.h>
#include <grpcxx/h2/nghttp2_server_session.h>
#include <grpcxx/reflection/client_stub.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace {
using HealthContract = greptime::v1::HealthCheckContract;
using HealthResponse = greptime::v1::HealthCheckResponse;
using HealthResult = std::expected<grpcxx::Response<HealthResponse>, grpcxx::Status>;

static_assert(grpcxx::ServiceContract<greptime::v1::GreptimeDatabaseContract>);
static_assert(grpcxx::ServiceContract<HealthContract>);
static_assert(grpcxx::ServiceContract<greptime::v1::PrometheusGatewayContract>);
static_assert(grpcxx::ServiceContract<arrow::flight::protocol::FlightServiceContract>);

/** Check generated service annotations and RPC cardinalities independently of
 * handlers. */
template <typename Contract, std::size_t N>
consteval bool Describes(std::string_view name, const std::array<grpcxx::RpcKind, N> &kinds) {
  if (grpcxx::reflection_detail::ServiceName<Contract>() != name) return false;
  constexpr auto methods = grpcxx::reflection_detail::Methods<Contract>();
  if constexpr (std::tuple_size_v<decltype(methods)> != N) {
    return false;
  } else {
    std::size_t index = 0;
    bool matching = true;
    std::apply(
        [&](auto... method) { ((matching &= decltype(method)::kKind == kinds[index++]), ...); },
        methods);
    return matching;
  }
}
static_assert(Describes<greptime::v1::GreptimeDatabaseContract>(
    "greptime.v1.GreptimeDatabase",
    std::array{grpcxx::RpcKind::kUnary, grpcxx::RpcKind::kClientStreaming}));
static_assert(Describes<HealthContract>("greptime.v1.HealthCheck",
                                        std::array{grpcxx::RpcKind::kUnary}));
static_assert(Describes<greptime::v1::PrometheusGatewayContract>(
    "greptime.v1.PrometheusGateway", std::array{grpcxx::RpcKind::kUnary}));
static_assert(Describes<arrow::flight::protocol::FlightServiceContract>(
    "arrow.flight.protocol.FlightService",
    std::array{grpcxx::RpcKind::kServerStreaming, grpcxx::RpcKind::kBidiStreaming}));

/** Compare native Protobuf and grpcxx codec bytes and decode through both
 * paths. */
template <typename Message>
void CheckRoundTrip(const Message &source) {
  grpcxx::AlignedMessageBuf wire;
  ASSERT_TRUE(grpcxx::ProtobufCodec::Encode(source, wire));
  std::string native;
  ASSERT_TRUE(source.SerializeToString(&native));
  EXPECT_TRUE(std::ranges::equal(wire.Payload(), std::as_bytes(std::span{native})));
  Message codec_decoded;
  ASSERT_TRUE(grpcxx::ProtobufCodec::Decode(wire.Payload(), codec_decoded));
  EXPECT_EQ(codec_decoded.SerializeAsString(), native);
  Message native_decoded;
  ASSERT_TRUE(native_decoded.ParseFromString(native));
  EXPECT_EQ(native_decoded.SerializeAsString(), native);
}

TEST(ProtoSmoke, PreservesGreptimeOneofsHeadersAndNullableColumns) {
  greptime::v1::GreptimeRequest request;
  auto *header = request.mutable_header();
  header->set_catalog("greptime");
  header->set_schema("public");
  header->set_dbname("greptime-public");
  header->set_timezone("UTC");
  header->mutable_authorization()->mutable_basic()->set_username("reader");
  header->mutable_authorization()->mutable_basic()->set_password("secret");
  auto *insert = request.mutable_inserts()->add_inserts();
  insert->set_table_name("ticks");
  insert->set_row_count(3);
  auto *timestamp = insert->add_columns();
  timestamp->set_column_name("ts");
  timestamp->set_semantic_type(greptime::v1::SemanticType::TIMESTAMP);
  timestamp->set_datatype(greptime::v1::ColumnDataType::TIMESTAMP_MILLISECOND);
  timestamp->mutable_values()->add_timestamp_millisecond_values(1700000000000);
  timestamp->mutable_values()->add_timestamp_millisecond_values(1700000000002);
  timestamp->set_null_mask(std::string(1, '\x02'));
  auto *price = insert->add_columns();
  price->set_column_name("price");
  price->set_semantic_type(greptime::v1::SemanticType::FIELD);
  price->set_datatype(greptime::v1::ColumnDataType::FLOAT64);
  for (double value : std::array{101.5, 101.75, 102.0})
    price->mutable_values()->add_f64_values(value);
  CheckRoundTrip(request);
  grpcxx::AlignedMessageBuf wire;
  ASSERT_TRUE(grpcxx::ProtobufCodec::Encode(request, wire));
  greptime::v1::GreptimeRequest decoded;
  ASSERT_TRUE(grpcxx::ProtobufCodec::Decode(wire.Payload(), decoded));
  ASSERT_EQ(decoded.inserts().inserts_size(), 1);
  const auto &decoded_insert = decoded.inserts().inserts(0);
  ASSERT_EQ(decoded_insert.columns_size(), 2);
  EXPECT_EQ(decoded_insert.row_count(), 3U);
  EXPECT_EQ(decoded_insert.columns(0).null_mask(), std::string(1, '\x02'));
  EXPECT_EQ(decoded_insert.columns(0).values().timestamp_millisecond_values_size(), 2);
  EXPECT_EQ(decoded_insert.columns(1).values().f64_values(2), 102.0);
  request.mutable_query()->set_sql("SELECT ts, price FROM ticks ORDER BY ts");
  EXPECT_FALSE(request.has_inserts());
  CheckRoundTrip(request);
}

TEST(ProtoSmoke, PreservesFlightBinaryFieldsAndPromqlRequests) {
  arrow::flight::protocol::FlightData data;
  data.mutable_flight_descriptor()->set_type(arrow::flight::protocol::FlightDescriptor::CMD);
  data.mutable_flight_descriptor()->set_cmd("{\"table\":\"ticks\"}");
  data.set_data_header(std::string{"\xff\0\x01\0", 4});
  data.set_data_body(std::string{"\0\x01\x02\xff", 4});
  data.set_app_metadata(std::string{"\0\0\0\0\0\0\0\x07", 8});
  CheckRoundTrip(data);
  arrow::flight::protocol::PutResult ack;
  ack.set_app_metadata("{\"request_id\":7,\"affected_rows\":3}");
  CheckRoundTrip(ack);
  arrow::flight::protocol::Ticket ticket;
  greptime::v1::GreptimeRequest query;
  query.mutable_query()->set_sql("SELECT price FROM ticks");
  ASSERT_TRUE(query.SerializeToString(ticket.mutable_ticket()));
  CheckRoundTrip(ticket);
  greptime::v1::PromqlRequest prom;
  prom.mutable_header()->set_dbname("greptime-public");
  prom.mutable_range_query()->set_query("rate(http_requests_total[5m])");
  prom.mutable_range_query()->set_start("1700000000");
  prom.mutable_range_query()->set_end("1700000060");
  prom.mutable_range_query()->set_step("15s");
  CheckRoundTrip(prom);
}

TEST(ProtoSmoke, SameStemRegionAndFlowImportsRemainDistinct) {
  greptime::v1::region::RegionRequest region;
  region.mutable_header()->set_dbname("greptime-public");
  region.mutable_create()->set_region_id(4096);
  region.mutable_create()->set_engine("mito");
  CheckRoundTrip(region);
  greptime::v1::flow::FlowResponse flow;
  flow.set_affected_rows(29);
  (*flow.mutable_extensions())["checkpoint"] = std::string{"\0\xff", 2};
  CheckRoundTrip(flow);
}

TEST(ProtoSmoke, PreservesPrometheusAndSubstraitImports) {
  prometheus::WriteRequest write;
  auto *series = write.add_timeseries();
  auto *label = series->add_labels();
  label->set_name("__name__");
  label->set_value("http_requests_total");
  auto *sample = series->add_samples();
  sample->set_timestamp(1700000000000);
  sample->set_value(42.5);
  CheckRoundTrip(write);
  substrait_extension::InstantManipulate plan;
  plan.set_start(1700000000000);
  plan.set_end(1700000060000);
  plan.set_interval(60000);
  plan.set_lookback_delta(300000);
  plan.set_time_index_idx(0);
  plan.set_field_index_idx(1);
  CheckRoundTrip(plan);
}

TEST(ProtoSmoke, HandlesEmptyMessagesAndRejectsTruncatedWire) {
  CheckRoundTrip(greptime::v1::HealthCheckRequest{});
  CheckRoundTrip(HealthResponse{});
  const std::array malformed{std::byte{0x0a}, std::byte{0xff}};
  greptime::v1::GreptimeRequest request;
  auto decoded = grpcxx::ProtobufCodec::Decode(malformed, request);
  ASSERT_FALSE(decoded);
  EXPECT_EQ(decoded.error().Code(), grpcxx::StatusCode::kInvalidArgument);
}

/** An actual generated unary method with observable dispatch and explicit
 * server failure. */
struct HealthHandler {
  int calls_{};
  bool fail_{};
  HealthResult HealthCheck(grpcxx::ServerContext &,
                           const greptime::v1::HealthCheckRequest &) noexcept {
    ++calls_;
    if (fail_) return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kUnavailable});
    return grpcxx::Response<HealthResponse>{};
  }
};

/** Drive real H2 sessions in fragments without sockets, sleeps, threads, or a
 * scheduler. */
struct HealthLoopback {
  using Router = grpcxx::ServiceRouter<HealthContract, HealthHandler, grpcxx::ProtobufCodec>;
  grpcxx::Nghttp2ClientSession client_;
  grpcxx::Nghttp2ServerSession server_{Router::kContentType};
  HealthHandler handler_;
  Router router_{handler_};
  grpcxx::ServerContext context_;
  int dispatch_failures_{};

  HealthLoopback() {
    server_.SetRequestHandler(this, [](void *opaque, std::int32_t stream, std::uint64_t generation,
                                       std::string_view path,
                                       std::span<const std::byte> payload) noexcept {
      auto &self = *static_cast<HealthLoopback *>(opaque);
      auto dispatched =
          self.router_.Dispatch(path, self.context_, payload, self.Hooks(), 1, stream, generation);
      if (!dispatched) ++self.dispatch_failures_;
    });
  }
  grpcxx::UnaryResponderSink Hooks() noexcept {
    return {
        .context = this,
        .is_current =
            [](void *opaque, std::uint64_t session, std::int32_t stream,
               std::uint64_t generation) noexcept {
              return session == 1 &&
                     static_cast<HealthLoopback *>(opaque)->server_.IsCurrent(stream, generation);
            },
        .complete = [](void *opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                       std::span<const std::byte> payload, grpcxx::MetadataMap &&,
                       grpcxx::MetadataMap &&) noexcept -> std::expected<void, grpcxx::Status> {
          return static_cast<HealthLoopback *>(opaque)->server_.Complete(stream, generation,
                                                                         payload);
        },
        .fail = [](void *opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                   grpcxx::Status status) noexcept -> std::expected<void, grpcxx::Status> {
          return static_cast<HealthLoopback *>(opaque)->server_.Complete(
              stream, generation, {}, static_cast<int>(status.Code()));
        }};
  }
  template <typename From, typename To>
  static std::expected<bool, grpcxx::Status> Transfer(From &from, To &to) {
    auto bytes = from.Send();
    if (!bytes) return std::unexpected(std::move(bytes.error()));
    if (bytes->empty()) return false;
    auto remaining = *bytes;
    while (!remaining.empty()) {
      auto fragment = remaining.first(std::min(remaining.size(), std::size_t{7}));
      auto received = to.Receive(fragment);
      if (!received) return std::unexpected(std::move(received.error()));
      if (*received != fragment.size())
        return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInternal});
      remaining = remaining.subspan(*received);
    }
    return true;
  }
  std::expected<void, grpcxx::Status> Drive() {
    for (auto tick : std::views::iota(0, 512)) {
      static_cast<void>(tick);
      auto outgoing = Transfer(client_, server_);
      if (!outgoing) return std::unexpected(std::move(outgoing.error()));
      auto incoming = Transfer(server_, client_);
      if (!incoming) return std::unexpected(std::move(incoming.error()));
      if (!*outgoing && !*incoming) return {};
    }
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kDeadlineExceeded});
  }
};

TEST(ProtoSmoke, GeneratedHealthUnaryTraversesHttp2AndPropagatesStatus) {
  HealthLoopback pair;
  ASSERT_TRUE(pair.client_.Ready());
  ASSERT_TRUE(pair.server_.Ready());
  grpcxx::ClientStub<HealthContract, grpcxx::ProtobufCodec> stub(pair.client_);
  grpcxx::ClientContext context;
  int callbacks = 0;
  bool succeeded = false;
  ASSERT_TRUE(stub.CallUnary<0>(
      context, greptime::v1::HealthCheckRequest{},
      [&callbacks, &succeeded](grpcxx::ClientContext &, HealthResult response) noexcept {
        ++callbacks;
        succeeded = response.has_value();
      }));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(callbacks, 1);
  EXPECT_TRUE(succeeded);
  EXPECT_EQ(pair.handler_.calls_, 1);
  EXPECT_EQ(pair.client_.ActiveStreams(), 0U);
  EXPECT_EQ(pair.server_.ActiveStreams(), 0U);
  pair.handler_.fail_ = true;
  int status = -1;
  ASSERT_TRUE(stub.CallUnary<0>(
      context, greptime::v1::HealthCheckRequest{},
      [&callbacks, &status](grpcxx::ClientContext &, HealthResult response) noexcept {
        ++callbacks;
        if (!response) status = static_cast<int>(response.error().Code());
      }));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(status, static_cast<int>(grpcxx::StatusCode::kUnavailable));
  EXPECT_EQ(pair.handler_.calls_, 2);
  EXPECT_EQ(pair.dispatch_failures_, 0);
  EXPECT_EQ(pair.client_.ActiveStreams(), 0U);
  EXPECT_EQ(pair.server_.ActiveStreams(), 0U);
  EXPECT_EQ(pair.client_.RejectedTransitions(), 0U);
  EXPECT_EQ(pair.server_.RejectedTransitions(), 0U);
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(callbacks, 2);
}
}  // namespace
