#include <greptimedb/core/config.h>
#include <greptimedb/core/hints.h>
#include <gtest/gtest.h>

namespace greptimedb {
namespace {
TEST(ErrorAudit, MalformedInnerDiagnosticsCannotBecomeGrpcOkSuccess) {
  for (const auto json :
       {"not-json", "[]", R"({"code":4294967296,"err_msg":"x"})",
        R"({"code":-1,"err_msg":"x"})", R"({"code":1,"err_msg":"\uD800"})",
        R"({"code":1,"err_msg":"\q"})",
        R"({"code":1,"err_msg":"x","extra":[1,]})",
        R"({"code":1,"err_msg":"x","code":2})",
        R"({"code":00,"err_msg":"x"})"}) {
    SCOPED_TRACE(json);
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add(kGreptimeInnerErrorHeader, json));
    auto checked = Error::CheckOkTrailers(trailers);
    ASSERT_FALSE(checked);
    EXPECT_EQ(checked.error().Kind(), ErrorKind::kProtocol);
    EXPECT_FALSE(checked.error().IsRetryable());
    auto original = Error::FromGrpcStatusAndTrailers(
        grpcxx::Status{grpcxx::StatusCode::kUnavailable, "original"}, trailers);
    EXPECT_EQ(original.GrpcCode(), grpcxx::StatusCode::kUnavailable);
    EXPECT_EQ(original.Message(), "original");
  }
}
TEST(ErrorAudit,
     EscapedUnicodeAndControlsDecodeWithoutChangingSuccessSemantics) {
  grpcxx::MetadataMap trailers;
  ASSERT_TRUE(trailers.Add(
      kGreptimeInnerErrorHeader,
      R"({"co\u0064e":4003,"err_msg":"line\n\t\"\\\u0000\uD83D\uDE80"})"));
  auto checked = Error::CheckOkTrailers(trailers);
  ASSERT_FALSE(checked);
  EXPECT_EQ(checked.error().RawCode(), 4003U);
  EXPECT_EQ(checked.error().Message(), std::string("line\n\t\"\\\0", 9) + "🚀");
  grpcxx::MetadataMap success;
  ASSERT_TRUE(
      success.Add(kGreptimeInnerErrorHeader, R"({"code":0,"err_msg":""})"));
  EXPECT_TRUE(Error::CheckOkTrailers(success));
  ASSERT_TRUE(success.Add(kGreptimeInnerErrorHeader, ""));
  EXPECT_TRUE(Error::CheckOkTrailers(success));
}
TEST(ErrorAudit, ConflictingOrOversizedInnerHeadersFailClosed) {
  for (const auto &second :
       {std::string{"malformed"},
        std::string{R"({"code":4004,"err_msg":"different"})"},
        std::string(65537, 'x')}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add(kGreptimeInnerErrorHeader,
                             R"({"code":4003,"err_msg":"busy"})"));
    ASSERT_TRUE(trailers.Add(kGreptimeInnerErrorHeader, second));
    auto checked = Error::CheckOkTrailers(trailers);
    ASSERT_FALSE(checked);
    EXPECT_EQ(checked.error().Kind(), ErrorKind::kProtocol);
  }
}
TEST(ConfigAudit, OptionalIoTimeoutsRequirePositiveRepresentableDurations) {
  auto endpoint = Endpoint::Parse("grpc://localhost:4001");
  ASSERT_TRUE(endpoint);
  ClientOptions options{*endpoint};
  EXPECT_TRUE(options.Validate());
  options.read_timeout = std::chrono::nanoseconds{0};
  EXPECT_FALSE(options.Validate());
  options.read_timeout = std::chrono::milliseconds{1};
  options.write_timeout = std::chrono::nanoseconds{-1};
  EXPECT_FALSE(options.Validate());
  options.write_timeout = std::chrono::milliseconds{1};
  EXPECT_TRUE(options.Validate());
}
} // namespace
} // namespace greptimedb
