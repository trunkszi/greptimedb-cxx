#include "transport_test_support.h"

#include <greptimedb/transport/in_memory_channel_pair.h>
#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <ranges>

namespace greptimedb::testing {
namespace {
using namespace std::string_view_literals;
struct BoundedEcho final {
  bool ok{true};
  static void Handle(void *context, MockGrpcServer &server,
                     const MockRequest &request) noexcept {
    auto &self = *static_cast<BoundedEcho *>(context);
    if (request.path == "/test.Echo/Hold")
      return;
    if (request.event == MockRequest::Event::kMessage) {
      grpcxx::MetadataMap headers;
      self.ok &= headers.AddBinary("echo-bin", Bytes("\0\1\2"sv)).has_value();
      self.ok &=
          server.SetInitialMetadata(request.stream_id, headers).has_value();
      self.ok &= server.Send(request.stream_id, request.payload).has_value();
    }
    if (request.event == MockRequest::Event::kHalfClose) {
      grpcxx::MetadataMap trailers;
      self.ok &= trailers.Add("x-complete", "true").has_value();
      self.ok &= server.Finish(request.stream_id, {}, trailers).has_value();
    }
  }
};

TEST(TransportHardening,
     LargeUncompressedMessageCrossesConnectionAndStreamWindows) {
  BoundedEcho echo;
  auto pair = InMemoryChannelPair::Create(BoundedEcho::Handle, &echo, {}, 4093);
  ASSERT_TRUE(pair);
  std::vector<std::byte> payload(1024U * 1024U + 17U);
  for (std::size_t index = 0; index < payload.size(); ++index)
    payload[index] = static_cast<std::byte>(index % 251U);
  CallOptions options;
  options.timeout = std::chrono::seconds{5};
  auto response =
      (*pair)->channel().CallUnary("/test.Echo/Call", payload, options);
  ASSERT_TRUE(response) << response.error().Message();
  EXPECT_TRUE(response->status.Ok());
  EXPECT_TRUE(std::ranges::equal(response->payload, payload));
  EXPECT_EQ(response->trailing_metadata.Get("x-complete"), "true");
  EXPECT_TRUE(echo.ok);
}

TEST(TransportHardening, EmptyMessagesAndBinaryMetadataRemainDistinctFromEof) {
  BoundedEcho echo;
  auto pair = InMemoryChannelPair::Create(BoundedEcho::Handle, &echo, {}, 11);
  ASSERT_TRUE(pair);
  auto response = (*pair)->channel().CallUnary("/test.Echo/Call", {});
  ASSERT_TRUE(response) << response.error().Message();
  EXPECT_TRUE(response->payload.empty());
  auto binary = response->initial_metadata.GetBinary("echo-bin");
  ASSERT_TRUE(binary);
  EXPECT_TRUE(std::ranges::equal(*binary, Bytes("\0\1\2"sv)));
  EXPECT_TRUE(echo.ok);
}

TEST(TransportHardening,
     DeadlineResetsUnansweredStreamAndPermitsSubsequentRpc) {
  BoundedEcho echo;
  auto pair = InMemoryChannelPair::Create(BoundedEcho::Handle, &echo);
  ASSERT_TRUE(pair);
  CallOptions options;
  options.timeout = std::chrono::milliseconds{2};
  auto held = (*pair)->channel().BidiStreamCall("/test.Echo/Hold", options);
  ASSERT_TRUE(held);
  ASSERT_TRUE((*held)->Send(Bytes("request")));
  auto pending = (*held)->RecvNext();
  ASSERT_FALSE(pending);
  EXPECT_EQ(pending.error().Code(), grpcxx::StatusCode::kDeadlineExceeded);
  for (unsigned tick = 0; tick < 32; ++tick) {
    ASSERT_TRUE((*pair)->channel().PollOnce());
    ASSERT_TRUE((*pair)->server().PollOnce());
  }
  EXPECT_EQ((*pair)->server().CancelledStreams(), 1U);
  auto response =
      (*pair)->channel().CallUnary("/test.Echo/Call", Bytes("after deadline"));
  ASSERT_TRUE(response) << response.error().Message();
  EXPECT_TRUE(std::ranges::equal(response->payload, Bytes("after deadline")));
}

TEST(TransportHardening, InvalidOptionsAndOversizedSendLeaveChannelUsable) {
  BoundedEcho echo;
  auto pair = InMemoryChannelPair::Create(BoundedEcho::Handle, &echo);
  ASSERT_TRUE(pair);
  for (auto timeout :
       {std::chrono::nanoseconds{0}, std::chrono::nanoseconds{-1}}) {
    CallOptions options;
    options.timeout = timeout;
    auto stream = (*pair)->channel().BidiStreamCall("/test.Echo/Call", options);
    ASSERT_FALSE(stream);
    EXPECT_EQ(stream.error().Code(), grpcxx::StatusCode::kInvalidArgument);
  }
  for (std::string_view path :
       {"", "missing-leading-slash", "/test.Echo/Call\r\n"}) {
    EXPECT_FALSE((*pair)->channel().BidiStreamCall(path));
  }
  auto stream = (*pair)->channel().BidiStreamCall("/test.Echo/Hold");
  ASSERT_TRUE(stream);
  std::vector<std::byte> oversized(4U * 1024U * 1024U + 1U);
  auto sent = (*stream)->Send(oversized);
  ASSERT_FALSE(sent);
  EXPECT_EQ(sent.error().Code(), grpcxx::StatusCode::kResourceExhausted);
  ASSERT_TRUE((*stream)->Cancel());
  auto response =
      (*pair)->channel().CallUnary("/test.Echo/Call", Bytes("still usable"));
  ASSERT_TRUE(response) << response.error().Message();
  EXPECT_TRUE(echo.ok);
}

TEST(TransportHardening,
     CompletedFailureRetainsDiagnosticsAndPercentEncodedStatus) {
  auto pair = InMemoryChannelPair::Create(
      [](void *, MockGrpcServer &server, const MockRequest &request) noexcept {
        if (request.event != MockRequest::Event::kHalfClose)
          return;
        grpcxx::MetadataMap trailers;
        const auto added =
            trailers.Add("x-greptime-err-retry-hint", "non_retryable");
        if (!added)
          return;
        const auto finished =
            server.Finish(request.stream_id,
                          grpcxx::Status{grpcxx::StatusCode::kPermissionDenied,
                                         "denied 100%: λ"},
                          trailers);
        static_cast<void>(finished);
      });
  ASSERT_TRUE(pair);
  auto response = (*pair)->channel().CallUnary("/test.Echo/Denied", {});
  ASSERT_TRUE(response) << response.error().Message();
  EXPECT_EQ(response->status.Code(), grpcxx::StatusCode::kPermissionDenied);
  EXPECT_EQ(response->status.Message(), "denied 100%: λ");
  EXPECT_EQ(response->trailing_metadata.Get("x-greptime-err-retry-hint"),
            "non_retryable");
}
} // namespace
} // namespace greptimedb::testing
