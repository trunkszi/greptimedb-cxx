#include "transport_test_support.h"

#include <greptimedb/transport/in_memory_channel_pair.h>
#include <greptimedb/transport/mock_grpc_server.h>
#include <greptimedb/transport/synchronous_channel.h>
#include <grpcxx/compression/compression_registry.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <new>
#include <optional>
#include <thread>
#include <vector>

namespace greptimedb::testing {
namespace {
enum class Medium { kMemory, kTcp, kUnix, kTls };

/** Counts accepted application wire bytes, including HTTP/2 framing. */
class CountingTransport final : public EndpointTransport {
public:
  CountingTransport(std::unique_ptr<EndpointTransport> wire,
                    std::size_t &written) noexcept
      : wire_(std::move(wire)), written_(written) {}
  auto Read(std::span<std::byte> bytes) noexcept
      -> std::expected<std::size_t, grpcxx::Status> override {
    return wire_->Read(bytes);
  }
  auto Write(std::span<const std::byte> bytes) noexcept
      -> std::expected<std::size_t, grpcxx::Status> override {
    auto accepted = wire_->Write(bytes);
    if (accepted)
      written_ += *accepted;
    return accepted;
  }
  auto PollOnce() noexcept -> std::expected<void, grpcxx::Status> override {
    return wire_->PollOnce();
  }
  void Close() noexcept override { wire_->Close(); }
  auto IsOpen() const noexcept -> bool override { return wire_->IsOpen(); }
  auto NativeHandle() const noexcept -> int override {
    return wire_->NativeHandle();
  }

private:
  std::unique_ptr<EndpointTransport> wire_;
  std::size_t &written_;
};

auto MakePair(Medium medium) -> std::expected<TransportPair, grpcxx::Status> {
  switch (medium) {
  case Medium::kMemory:
    return MemoryPipe::CreatePair(97);
  case Medium::kTcp:
    return TcpPair();
  case Medium::kUnix:
    return UnixPair();
  case Medium::kTls: {
    auto pair = TcpPair();
    if (!pair)
      return std::unexpected(pair.error());
    return TlsPair(std::move(*pair), true);
  }
  }
  return Failure("invalid test medium");
}

/** The test handler observes real HTTP/2 message, END_STREAM and RST events. */
struct EchoPeer {
  grpcxx::RpcKind kind{grpcxx::RpcKind::kUnary};
  std::vector<std::byte> aggregate;
  std::size_t messages{}, half_closes{}, cancels{};
  bool ok{true};
  std::thread::id owner{std::this_thread::get_id()};

  static void Handle(void *opaque, MockGrpcServer &server,
                     const MockRequest &request) noexcept {
    auto &self = *static_cast<EchoPeer *>(opaque);
    self.ok &= std::this_thread::get_id() == self.owner;
    switch (request.event) {
    case MockRequest::Event::kCancel:
      ++self.cancels;
      return;
    case MockRequest::Event::kMessage: {
      self.ok &= request.path == "/test.Echo/Call" ||
                 request.path == "/test.Echo/Hold";
      self.ok &= request.metadata.Get("x-request") == "caller-thread";
      if (request.path == "/test.Echo/Hold")
        return;
      if (self.messages++ == 0) {
        grpcxx::MetadataMap metadata;
        self.ok &= metadata.Add("x-server", "echo").has_value();
        self.ok &=
            server.SetInitialMetadata(request.stream_id, metadata).has_value();
      }
      self.aggregate.insert(self.aggregate.end(), request.payload.begin(),
                            request.payload.end());
      switch (self.kind) {
      case grpcxx::RpcKind::kUnary:
      case grpcxx::RpcKind::kClientStreaming:
        break;
      case grpcxx::RpcKind::kServerStreaming:
        for (int index = 0; index != 3; ++index)
          self.ok &=
              server.Send(request.stream_id, request.payload).has_value();
        break;
      case grpcxx::RpcKind::kBidiStreaming:
        self.ok &= server.Send(request.stream_id, request.payload).has_value();
        break;
      }
      return;
    }
    case MockRequest::Event::kHalfClose: {
      ++self.half_closes;
      if (request.path == "/test.Echo/Hold")
        return;
      if (self.kind == grpcxx::RpcKind::kUnary ||
          self.kind == grpcxx::RpcKind::kClientStreaming)
        self.ok &= server.Send(request.stream_id, self.aggregate).has_value();
      grpcxx::MetadataMap trailers;
      self.ok &= trailers.Add("x-finished", "yes").has_value();
      self.ok &= server.Finish(request.stream_id, {}, trailers).has_value();
      return;
    }
    }
  }
  void Reset() {
    aggregate.clear();
    messages = half_closes = 0;
  }
};

struct RpcPair {
  EchoPeer peer;
  std::size_t request_wire_bytes{}, response_wire_bytes{};
  std::unique_ptr<MockGrpcServer> server;
  std::unique_ptr<SynchronousChannel> client;

  auto Init(Medium medium, grpcxx::CompressionAlgorithm compression =
                               grpcxx::CompressionAlgorithm::kIdentity)
      -> std::expected<void, grpcxx::Status> {
    auto pair = MakePair(medium);
    if (!pair)
      return std::unexpected(pair.error());
    pair->first.reset(new (std::nothrow) CountingTransport(
        std::move(pair->first), request_wire_bytes));
    pair->second.reset(new (std::nothrow) CountingTransport(
        std::move(pair->second), response_wire_bytes));
    if (!pair->first || !pair->second)
      return Failure("test transport allocation failed");
    auto created_server =
        MockGrpcServer::Create(std::move(pair->second), EchoPeer::Handle, &peer,
                               {.response_compression = compression});
    if (!created_server)
      return std::unexpected(created_server.error());
    server = std::move(*created_server);
    auto created_client = SynchronousChannel::Create(std::move(pair->first));
    if (!created_client)
      return std::unexpected(created_client.error());
    client = std::move(*created_client);
    client->SetProgressHook(server.get(), [](void *context) noexcept {
      return static_cast<MockGrpcServer *>(context)->PollOnce();
    });
    return {};
  }
};

auto Options(grpcxx::CompressionAlgorithm compression =
                 grpcxx::CompressionAlgorithm::kIdentity) -> CallOptions {
  CallOptions options;
  options.timeout = std::chrono::seconds{3};
  options.compression = compression;
  static_cast<void>(options.metadata.Add("x-request", "caller-thread"));
  return options;
}

void CheckShape(Medium medium, grpcxx::RpcKind kind,
                grpcxx::CompressionAlgorithm compression) {
  RpcPair pair;
  ASSERT_TRUE(pair.Init(medium, compression));
  pair.peer.kind = kind;
  const std::string body(4096, 'q');
  if (kind == grpcxx::RpcKind::kUnary) {
    auto reply = pair.client->Unary("/test.Echo/Call", Bytes(body),
                                    Options(compression));
    ASSERT_TRUE(reply);
    EXPECT_TRUE(std::ranges::equal(reply->payload, Bytes(body)));
    EXPECT_EQ(reply->initial_metadata.Get("x-server"), "echo");
    EXPECT_EQ(reply->trailing_metadata.Get("x-finished"), "yes");
    EXPECT_EQ(pair.peer.messages, 1U);
  } else {
    auto stream =
        pair.client->Open("/test.Echo/Call", kind, Options(compression));
    ASSERT_TRUE(stream);
    const unsigned requests = kind == grpcxx::RpcKind::kServerStreaming ? 1 : 3;
    for (unsigned request = 0; request != requests; ++request) {
      ASSERT_TRUE((*stream)->Send(Bytes(body)));
      if (kind == grpcxx::RpcKind::kBidiStreaming) {
        // Read before half-close proves actual bidirectional progress.
        auto reply = (*stream)->Read();
        ASSERT_TRUE(reply);
        ASSERT_TRUE(reply->has_value());
        EXPECT_TRUE(std::ranges::equal(**reply, Bytes(body)));
      }
    }
    ASSERT_TRUE((*stream)->HalfClose());
    unsigned responses = 0;
    for (;;) {
      auto reply = (*stream)->Read();
      ASSERT_TRUE(reply);
      if (!*reply)
        break;
      ++responses;
      const std::string expected(kind == grpcxx::RpcKind::kClientStreaming
                                     ? 3 * body.size()
                                     : body.size(),
                                 'q');
      EXPECT_TRUE(std::ranges::equal(**reply, Bytes(expected)));
    }
    EXPECT_EQ(responses, kind == grpcxx::RpcKind::kBidiStreaming     ? 0U
                         : kind == grpcxx::RpcKind::kServerStreaming ? 3U
                                                                     : 1U);
    auto finished = (*stream)->Finish();
    ASSERT_TRUE(finished);
    EXPECT_EQ(finished->initial_metadata.Get("x-server"), "echo");
    EXPECT_EQ(finished->trailing_metadata.Get("x-finished"), "yes");
    EXPECT_EQ(pair.peer.messages, requests);
  }
  EXPECT_EQ(pair.peer.half_closes, 1U);
  EXPECT_TRUE(pair.peer.ok);
  if (compression != grpcxx::CompressionAlgorithm::kIdentity) {
    // A compressible 4096-byte message must shrink on the actual transport.
    EXPECT_LT(pair.request_wire_bytes, body.size());
    EXPECT_LT(pair.response_wire_bytes, body.size());
  }
  for (int tick = 0; tick != 8; ++tick) {
    ASSERT_TRUE(pair.client->PollOnce());
    ASSERT_TRUE(pair.server->PollOnce());
  }
  EXPECT_EQ(pair.server->ActiveStreams(), 0U);
}

TEST(RpcTransport, AllFourShapesOverMemoryTcpUnixAndTls13) {
  for (auto medium :
       {Medium::kMemory, Medium::kTcp, Medium::kUnix, Medium::kTls}) {
    for (auto kind :
         {grpcxx::RpcKind::kUnary, grpcxx::RpcKind::kClientStreaming,
          grpcxx::RpcKind::kServerStreaming, grpcxx::RpcKind::kBidiStreaming}) {
      SCOPED_TRACE(static_cast<int>(medium));
      SCOPED_TRACE(static_cast<int>(kind));
      CheckShape(medium, kind, grpcxx::CompressionAlgorithm::kIdentity);
    }
  }
}

TEST(RpcTransport,
     CompressionEncodesAndDecodesBothDirectionsForEveryStreamingShape) {
  for (auto compression :
       {grpcxx::CompressionAlgorithm::kGzip,
        grpcxx::CompressionAlgorithm::kDeflate,
        grpcxx::CompressionAlgorithm::kZstd, grpcxx::CompressionAlgorithm::kLz4,
        grpcxx::CompressionAlgorithm::kSnappy}) {
    for (auto kind :
         {grpcxx::RpcKind::kUnary, grpcxx::RpcKind::kClientStreaming,
          grpcxx::RpcKind::kServerStreaming, grpcxx::RpcKind::kBidiStreaming}) {
      SCOPED_TRACE(grpcxx::CompressionRegistry::EncodingName(compression));
      SCOPED_TRACE(static_cast<int>(kind));
      CheckShape(Medium::kMemory, kind, compression);
    }
  }
}

TEST(RpcTransport,
     ExplicitAndDestructorCancellationSendRstAndKeepConnectionUsable) {
  for (bool explicit_cancel : {false, true}) {
    RpcPair pair;
    ASSERT_TRUE(pair.Init(Medium::kMemory));
    {
      auto held = pair.client->Open("/test.Echo/Hold",
                                    grpcxx::RpcKind::kBidiStreaming, Options());
      ASSERT_TRUE(held);
      ASSERT_TRUE((*held)->Send(Bytes("pending")));
      for (int tick = 0; tick != 32; ++tick) {
        ASSERT_TRUE(pair.client->PollOnce());
        ASSERT_TRUE(pair.server->PollOnce());
      }
      EXPECT_EQ(pair.server->ActiveStreams(), 1U);
      if (explicit_cancel) {
        ASSERT_TRUE((*held)->Cancel());
      }
    }
    for (int tick = 0; tick != 64; ++tick) {
      ASSERT_TRUE(pair.client->PollOnce());
      ASSERT_TRUE(pair.server->PollOnce());
    }
    EXPECT_EQ(pair.server->CancelledStreams(), 1U);
    EXPECT_EQ(pair.peer.cancels, 1U);
    EXPECT_EQ(pair.server->ActiveStreams(), 0U);
    auto reply =
        pair.client->Unary("/test.Echo/Call", Bytes("after reset"), Options());
    ASSERT_TRUE(reply);
    EXPECT_TRUE(std::ranges::equal(reply->payload, Bytes("after reset")));
    EXPECT_TRUE(pair.peer.ok);
  }
}

TEST(RpcTransport, TrailersPropagateFailureInsteadOfReturningAnEmptySuccess) {
  auto transports = MemoryPipe::CreatePair(97);
  ASSERT_TRUE(transports);
  auto server = MockGrpcServer::Create(
      std::move(transports->second),
      [](void *, MockGrpcServer &peer, const MockRequest &request) noexcept {
        if (request.event == MockRequest::Event::kHalfClose) {
          grpcxx::MetadataMap trailers;
          static_cast<void>(trailers.Add("x-denied", "policy"));
          static_cast<void>(
              peer.Finish(request.stream_id,
                          grpcxx::Status{grpcxx::StatusCode::kPermissionDenied,
                                         "test denied"},
                          trailers));
        }
      });
  ASSERT_TRUE(server);
  auto client = SynchronousChannel::Create(std::move(transports->first));
  ASSERT_TRUE(client);
  (*client)->SetProgressHook(server->get(), [](void *pointer) noexcept {
    return static_cast<MockGrpcServer *>(pointer)->PollOnce();
  });
  auto response =
      (*client)->Unary("/test.Echo/Denied", Bytes("request"), Options());
  ASSERT_TRUE(response);
  EXPECT_EQ(response->status.Code(), grpcxx::StatusCode::kPermissionDenied);
  EXPECT_EQ(response->status.Message(), "test denied");
  EXPECT_EQ(response->trailing_metadata.Get("x-denied"), "policy");
}

TEST(RpcTransport, ResetOfConcurrentStreamPreservesBidirectionalPeer) {
  RpcPair pair;
  ASSERT_TRUE(pair.Init(Medium::kMemory));
  pair.peer.kind = grpcxx::RpcKind::kBidiStreaming;
  auto held = pair.client->Open("/test.Echo/Hold",
                                grpcxx::RpcKind::kBidiStreaming, Options());
  auto live = pair.client->Open("/test.Echo/Call",
                                grpcxx::RpcKind::kBidiStreaming, Options());
  ASSERT_TRUE(held);
  ASSERT_TRUE(live);
  ASSERT_TRUE((*held)->Send(Bytes("pending")));
  ASSERT_TRUE((*live)->Send(Bytes("first")));
  for (int tick = 0; tick != 32; ++tick) {
    ASSERT_TRUE(pair.client->PollOnce());
    ASSERT_TRUE(pair.server->PollOnce());
  }
  ASSERT_EQ(pair.server->ActiveStreams(), 2U);
  ASSERT_TRUE((*held)->Cancel());
  auto first = (*live)->Read();
  ASSERT_TRUE(first);
  ASSERT_TRUE(*first);
  EXPECT_TRUE(std::ranges::equal(**first, Bytes("first")));
  ASSERT_TRUE((*live)->Send(Bytes("after sibling reset")));
  auto second = (*live)->Read();
  ASSERT_TRUE(second);
  ASSERT_TRUE(*second);
  EXPECT_TRUE(std::ranges::equal(**second, Bytes("after sibling reset")));
  ASSERT_TRUE((*live)->HalfClose());
  ASSERT_TRUE((*live)->Finish());
  EXPECT_EQ(pair.server->CancelledStreams(), 1U);
  EXPECT_EQ(pair.peer.cancels, 1U);
  EXPECT_TRUE(pair.peer.ok);
}

TEST(RpcTransport,
     InMemoryChannelPairDrivesPeerInlineAndRejectsInvalidCapacity) {
  EchoPeer peer;
  auto pair = InMemoryChannelPair::Create(EchoPeer::Handle, &peer, {}, 23);
  ASSERT_TRUE(pair);
  auto response =
      (*pair)->channel().Unary("/test.Echo/Call", Bytes("paired"), Options());
  ASSERT_TRUE(response);
  EXPECT_TRUE(std::ranges::equal(response->payload, Bytes("paired")));
  EXPECT_EQ(peer.messages, 1U);
  EXPECT_EQ(peer.half_closes, 1U);
  EXPECT_TRUE(peer.ok);
  EXPECT_FALSE(InMemoryChannelPair::Create(EchoPeer::Handle, &peer, {}, 0));
}

TEST(RpcTransport,
     EndpointUriValidationRejectsUnsupportedAndMalformedEndpoints) {
  for (std::string_view endpoint :
       {"", "ws://localhost:4001", "grpc://", "grpc://localhost:0",
        "grpc://localhost:70000", "unix://", "grpcs://:4001"}) {
    EXPECT_FALSE(SynchronousChannel::Connect(endpoint)) << endpoint;
  }
}

} // namespace
} // namespace greptimedb::testing
