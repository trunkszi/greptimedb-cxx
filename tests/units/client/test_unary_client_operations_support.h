#pragma once
#include <greptime/v1/health.pb.h>
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>
#include <greptimedb/client/client.h>
#include <greptimedb/client/ddl.h>
#include <greptimedb/client/prom.h>
#include <greptimedb/row/reflected_row.h>
#include <grpcxx/sync/synchronous_server.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace greptimedb;
namespace proto = greptime::v1;
constexpr std::string_view kHandle = "/greptime.v1.GreptimeDatabase/Handle";
constexpr std::string_view kProm = "/greptime.v1.PrometheusGateway/Handle";
constexpr std::string_view kHealth = "/greptime.v1.HealthCheck/HealthCheck";

static_assert(!std::is_copy_constructible_v<Client>);
static_assert(!std::is_move_constructible_v<Client>);

ClientOptions Options() {
  auto endpoint = Endpoint::Parse("grpc://127.0.0.1:4001");
  EXPECT_TRUE(endpoint);
  ClientOptions options(std::move(*endpoint));
  options.catalog = "observability";
  options.schema = "samples";
  return options;
}

proto::GreptimeResponse Response(std::uint32_t rows) {
  proto::GreptimeResponse response;
  response.mutable_header()->mutable_status()->set_status_code(0);
  response.mutable_affected_rows()->set_value(rows);
  return response;
}

inline void CheckHeader(const proto::RequestHeader &header) {
  EXPECT_EQ(header.catalog(), "observability");
  EXPECT_EQ(header.schema(), "samples");
  EXPECT_EQ(header.dbname(), "observability-samples");
}

void CheckRows(Result<std::uint32_t> result, std::uint32_t expected) {
  ASSERT_TRUE(result) << result.error().Message();
  EXPECT_EQ(*result, expected);
}

template <typename Batch> void FillMetric(Batch &batch) {
  ASSERT_TRUE(batch.AddTagColumn("host", ColumnDataType::kString));
  ASSERT_TRUE(batch.AddTimestampColumn("time", TimestampUnit::kMillisecond));
  ASSERT_TRUE(batch.AddFieldColumn("value", ColumnDataType::kFloat64));
  const std::array values{ValueView{std::string_view{"srv"}}, ValueView{TimestampMillisecond{1700000000000}},
                          ValueView{42.5}};
  ASSERT_TRUE(batch.PushRow(values));
}

// The progress hook advances a real HTTP/2 peer on the SDK caller's thread.
class UnaryPeer {
public:
  using Handler = void (*)(void *, grpcxx::SynchronousServer &, const grpcxx::ServerRequest &) noexcept;
  std::unique_ptr<grpcxx::SynchronousServer> server;
  std::unique_ptr<Client> client;
  const std::thread::id owner{std::this_thread::get_id()};
  std::size_t messages{};

  void Open(Handler handler, void *context, ClientOptions options = Options()) {
    auto pipe = nexora::MemoryPipe::CreatePair(97);
    ASSERT_TRUE(pipe);
    auto peer = grpcxx::SynchronousServer::FromMemoryPipe(std::move(pipe->server), handler, context);
    ASSERT_TRUE(peer);
    server = std::move(*peer);
    auto opened =
        Client::FromMemoryPipe(std::move(options), std::move(pipe->client), server.get(), [](void *pointer) noexcept {
          return static_cast<grpcxx::SynchronousServer *>(pointer)->PollOnce();
        });
    ASSERT_TRUE(opened);
    client = std::move(*opened);
  }

  bool Message(const grpcxx::ServerRequest &request, std::string_view path) {
    if (request.event != grpcxx::ServerRequest::Event::kMessage) return false;
    EXPECT_EQ(std::this_thread::get_id(), owner);
    EXPECT_EQ(request.path, path);
    EXPECT_EQ(request.metadata.Get("x-greptime-db-name"), "observability-samples");
    ++messages;
    return true;
  }

  template <typename Message>
  static void Send(grpcxx::SynchronousServer &server, const grpcxx::ServerRequest &request, const Message &message,
                   grpcxx::Status status = {}, const grpcxx::MetadataMap &trailers = {}) {
    std::string bytes;
    ASSERT_TRUE(message.SerializeToString(&bytes));
    ASSERT_TRUE(server.Send(request.stream_id, std::as_bytes(std::span{bytes})));
    ASSERT_TRUE(server.Finish(request.stream_id, std::move(status), trailers));
  }

  template <typename Message> static Message Parse(const grpcxx::ServerRequest &request) {
    Message message;
    EXPECT_TRUE(message.ParseFromArray(request.payload.data(), static_cast<int>(request.payload.size())));
    return message;
  }
};

} // namespace
