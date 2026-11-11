/** Application error codes and trailer diagnostics. */
#include <greptime/v1/common.pb.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/hints.h>
#include <grpcxx/core/metadata.h>
#include <grpcxx/core/status.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace {
using namespace greptimedb;

Error TrailerError(const grpcxx::MetadataMap &metadata) {
  return Error::FromGrpcStatusAndTrailers(grpcxx::Status{grpcxx::StatusCode::kUnavailable, "original gRPC failure"},
                                          metadata);
}

TEST(Errors, MapsEveryLegacyCategoryAndPreservesUnknownRawCodes) {
  for (const auto &[raw, code] : std::array{std::pair{0U, ErrorCode::kSuccess},
                                            std::pair{1000U, ErrorCode::kUnknown},
                                            std::pair{1001U, ErrorCode::kUnsupported},
                                            std::pair{1002U, ErrorCode::kUnexpected},
                                            std::pair{1003U, ErrorCode::kInternal},
                                            std::pair{1004U, ErrorCode::kInvalidArguments},
                                            std::pair{1005U, ErrorCode::kCancelled},
                                            std::pair{1006U, ErrorCode::kIllegalState},
                                            std::pair{2000U, ErrorCode::kSyntax},
                                            std::pair{2001U, ErrorCode::kPlan},
                                            std::pair{2002U, ErrorCode::kEngineExecute},
                                            std::pair{3000U, ErrorCode::kTableAlreadyExists},
                                            std::pair{3001U, ErrorCode::kTableNotFound},
                                            std::pair{3002U, ErrorCode::kTableColumnNotFound},
                                            std::pair{3003U, ErrorCode::kTableColumnExists},
                                            std::pair{3004U, ErrorCode::kDatabaseNotFound},
                                            std::pair{3005U, ErrorCode::kDatabaseAlreadyExists},
                                            std::pair{4000U, ErrorCode::kStorageUnavailable},
                                            std::pair{4001U, ErrorCode::kRegionNotFound},
                                            std::pair{4002U, ErrorCode::kRegionReadonly},
                                            std::pair{4003U, ErrorCode::kRegionBusy},
                                            std::pair{4004U, ErrorCode::kRateLimited},
                                            std::pair{4005U, ErrorCode::kFlowControl},
                                            std::pair{5000U, ErrorCode::kMetaSrv},
                                            std::pair{6000U, ErrorCode::kAuthFailed},
                                            std::pair{6999U, ErrorCode::kAuthFailed}}) {
    EXPECT_EQ(ClassifyErrorCode(raw), code) << raw;
    auto error = Error::ServerStatus(raw, "wire diagnostic");
    EXPECT_EQ(error.RawCode(), raw);
    EXPECT_EQ(error.Code(), code);
  }
  for (auto raw : {1U, 1007U, 5999U, 7000U, std::numeric_limits<std::uint32_t>::max()}) {
    auto error = Error::ServerStatus(raw, "unknown");
    EXPECT_EQ(error.Code(), ErrorCode::kOther);
    EXPECT_EQ(error.RawCode(), raw);
  }
}

TEST(Errors, VersionedClassifierIsExplicitAndKeepsLegacyNumbersStable) {
  for (const auto &[raw, code] :
       std::array{std::pair{0U, ErrorCode::kSuccess}, std::pair{3000U, ErrorCode::kPlan},
                  std::pair{3001U, ErrorCode::kEngineExecute}, std::pair{4000U, ErrorCode::kTableAlreadyExists},
                  std::pair{4001U, ErrorCode::kTableNotFound}, std::pair{4003U, ErrorCode::kRegionBusy},
                  std::pair{4005U, ErrorCode::kRegionNotFound}, std::pair{5000U, ErrorCode::kStorageUnavailable},
                  std::pair{6000U, ErrorCode::kResourceExhausted}, std::pair{6001U, ErrorCode::kRateLimited},
                  std::pair{7000U, ErrorCode::kAuthFailed}, std::pair{7002U, ErrorCode::kAuthFailed},
                  std::pair{7006U, ErrorCode::kAuthFailed},
                  std::pair{std::numeric_limits<std::uint32_t>::max(), ErrorCode::kOther}}) {
    EXPECT_EQ(ClassifyV121ErrorCode(raw), code) << raw;
  }
  EXPECT_EQ(ClassifyErrorCode(3000), ErrorCode::kTableAlreadyExists);
  EXPECT_EQ(ClassifyErrorCode(4001), ErrorCode::kRegionNotFound);
  EXPECT_EQ(ClassifyErrorCode(6001), ErrorCode::kAuthFailed);
  EXPECT_EQ(ClassifyErrorCode(7000), ErrorCode::kOther);
  EXPECT_EQ(ClassifyErrorCode(7002), ErrorCode::kAuthFailed);
}

TEST(Errors, AggregateFailurePreservesFinalAttemptRetryGuidance) {
  auto aggregate = Error::AllEndpointsFailed(2, Error::ServerStatus(4003, "busy", true));
  EXPECT_EQ(aggregate.Kind(), ErrorKind::kAllEndpointsFailed);
  EXPECT_EQ(aggregate.Attempts(), 2U);
  EXPECT_EQ(aggregate.Code(), ErrorCode::kRegionBusy);
  EXPECT_EQ(aggregate.RawCode(), 4003U);
  EXPECT_TRUE(aggregate.IsRetryable());
  EXPECT_EQ(aggregate.RetryHint(), true);
  auto uncertain = Error::AllEndpointsFailed(3, Error::Transport("unknown delivery"));
  EXPECT_FALSE(uncertain.IsRetryable());
  EXPECT_EQ(uncertain.RetryHint(), std::nullopt);
}

TEST(Errors, ResponseHeadersAreCheckedEvenWhenGrpcSucceeds) {
  EXPECT_TRUE(Error::FromResponseHeader(nullptr));
  greptime::v1::ResponseHeader header;
  EXPECT_TRUE(Error::FromResponseHeader(header));
  header.mutable_status()->set_status_code(0);
  header.mutable_status()->set_retry_hint("true");
  EXPECT_TRUE(Error::FromResponseHeader(header));
  for (const auto &[hint, expected] : std::array{
           std::pair{"true", true}, std::pair{"retryable", true}, std::pair{"false", false},
           std::pair{"non_retryable", false}, std::pair{"", false}, std::pair{"TRUE", false}, std::pair{"True", false},
           std::pair{"1", false}, std::pair{" true ", false}, std::pair{"junk", false}}) {
    header.mutable_status()->set_status_code(6999);
    header.mutable_status()->set_err_msg("access denied");
    header.mutable_status()->set_retry_hint(hint);
    auto checked = Error::FromResponseHeader(header);
    ASSERT_FALSE(checked);
    EXPECT_EQ(checked.error().Kind(), ErrorKind::kServerStatus);
    EXPECT_EQ(checked.error().Code(), ErrorCode::kAuthFailed);
    EXPECT_EQ(checked.error().RawCode(), 6999U);
    EXPECT_EQ(checked.error().Message(), "access denied");
    EXPECT_EQ(checked.error().RetryHint(), expected);
    EXPECT_EQ(checked.error().IsRetryable(), expected);
  }
}

TEST(Errors, TrailerJsonRetainsCodeMessageAndStrictRetryVocabulary) {
  for (std::string_view hint :
       {"true", "retryable", "false", "non_retryable", "TRUE", "Retryable", "1", " true", "true "}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("INNER_ERROR_MSG", R"({"code":4003,"err_msg":"region busy"})"));
    ASSERT_TRUE(trailers.Add(kGreptimeRetryHintHeader, hint));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Code(), ErrorCode::kRegionBusy);
    EXPECT_EQ(error.RawCode(), 4003U);
    EXPECT_EQ(error.Message(), "region busy");
    EXPECT_EQ(error.RetryHint(), hint == "true" || hint == "retryable");
    EXPECT_EQ(error.IsRetryable(), hint == "true" || hint == "retryable");
  }
  grpcxx::MetadataMap unknown;
  ASSERT_TRUE(unknown.Add("inner_error_msg", R"({"code":999999,"err_msg":"unknown failure"})"));
  auto error = TrailerError(unknown);
  EXPECT_EQ(error.Code(), ErrorCode::kOther);
  EXPECT_EQ(error.RawCode(), 999999U);
  EXPECT_FALSE(error.IsRetryable());
}

TEST(Errors, MalformedInnerErrorsPreserveOriginalGrpcFailure) {
  for (std::string_view inner :
       {"not json", "[]", "null", R"({"code":0,"err_msg":"success"})", R"({"code":-1,"err_msg":"bad"})",
        R"({"code":4294967296,"err_msg":"overflow"})", R"({"code":"4003","err_msg":"string"})",
        R"({"code":4003.5,"err_msg":"fraction"})", R"({"code":4003})", R"({"code":4003,"err_msg":2})",
        R"({"code":4003,"err_msg":"x"}garbage)", R"({"code":4003,"err_msg":"\uD800"})",
        R"({"code":4003,"err_msg":"\uDC00"})", R"({"code":4003,"err_msg":"\q"})",
        R"({"code":4003,"err_msg":"unterminated})"}) {
    SCOPED_TRACE(inner);
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("inner_error_msg", inner));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Kind(), ErrorKind::kGrpc);
    EXPECT_EQ(error.GrpcCode(), grpcxx::StatusCode::kUnavailable);
    EXPECT_EQ(error.Message(), "original gRPC failure");
    EXPECT_EQ(error.RetryHint(), std::nullopt);
  }
  auto error = Error::FromGrpcStatusAndTrailers(grpcxx::Status{grpcxx::StatusCode::kUnavailable, "original"}, nullptr);
  EXPECT_EQ(error.Kind(), ErrorKind::kGrpc);
}

TEST(Errors, JsonEscapesUnknownFieldsAndBoundaryCodesAreDecoded) {
  grpcxx::MetadataMap trailers;
  ASSERT_TRUE(trailers.Add(
      "INNER_ERROR_MSG",
      R"({"ignored":{"array":[1,true,null,{"x":"y"}]},"err_msg":"line\n\"quoted\"\u00e9\uD83D\uDE80","code":4294967295})"));
  auto error = TrailerError(trailers);
  EXPECT_EQ(error.RawCode(), std::numeric_limits<std::uint32_t>::max());
  EXPECT_EQ(error.Message(), "line\n\"quoted\"é🚀");
  EXPECT_EQ(error.Code(), ErrorCode::kOther);
  EXPECT_FALSE(error.IsRetryable());
}

TEST(Errors, InnerJsonRejectsMalformedUnknownFieldsAndDuplicateDiagnostics) {
  for (std::string_view inner :
       {R"({"code":4003,"err_msg":"busy","unknown":[1,]})", R"({"code":4003,"err_msg":"busy","unknown":{"x":}})",
        R"({"code":4003,"err_msg":"busy","unknown":{"x":false,}})", R"({"code":4003,"err_msg":"busy","unknown":01})",
        R"({"code":4003,"err_msg":"busy","unknown":1e})", R"({"code":04003,"err_msg":"busy"})",
        R"({"code":4003,"err_msg":"busy"} true)", R"({"code":4003,"code":4004,"err_msg":"busy"})",
        R"({"code":4003,"code":4003,"err_msg":"busy"})", R"({"code":4003,"err_msg":"busy","err_msg":"retry"})"}) {
    SCOPED_TRACE(inner);
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("inner_error_msg", inner));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Kind(), ErrorKind::kGrpc);
    EXPECT_EQ(error.GrpcCode(), grpcxx::StatusCode::kUnavailable);
    EXPECT_EQ(error.Message(), "original gRPC failure");
    EXPECT_FALSE(error.IsRetryable());
  }
}

TEST(Errors, InnerJsonNestingLimitAcceptsBoundaryAndRejectsExcessDepth) {
  for (std::size_t depth : {31U, 32U}) {
    const auto nested = std::string(depth, '[') + "0" + std::string(depth, ']');
    const auto json = std::string{R"({"code":4003,"err_msg":"busy","unknown":)"} + nested + "}";
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("inner_error_msg", json));
    auto error = TrailerError(trailers);
    if (depth == 31) {
      EXPECT_EQ(error.Kind(), ErrorKind::kServerStatus);
      EXPECT_EQ(error.RawCode(), 4003U);
      EXPECT_EQ(error.Message(), "busy");
    } else {
      EXPECT_EQ(error.Kind(), ErrorKind::kGrpc);
      EXPECT_EQ(error.Message(), "original gRPC failure");
    }
  }
}

TEST(Errors, NumericTrailersRequireConsistentNonzeroDecimalU32) {
  for (std::string_view code : {"", "junk", "4294967296", "-1", "+4003", "0", "4003.0", " 4003", "4003 "}) {
    SCOPED_TRACE(code);
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("x-greptime-err-code", code));
    EXPECT_EQ(TrailerError(trailers).Kind(), ErrorKind::kGrpc);
  }
  for (const auto &codes : {std::array{"4003", "4004"}, std::array{"4003", "invalid"}, std::array{"invalid", "4003"}}) {
    grpcxx::MetadataMap trailers;
    for (auto code : codes)
      ASSERT_TRUE(trailers.Add("x-greptime-err-code", code));
    EXPECT_EQ(TrailerError(trailers).Kind(), ErrorKind::kGrpc);
  }
  grpcxx::MetadataMap repeated;
  ASSERT_TRUE(repeated.Add("x-greptime-err-code", "4294967295"));
  ASSERT_TRUE(repeated.Add("x-greptime-err-code", "4294967295"));
  auto error = TrailerError(repeated);
  EXPECT_EQ(error.Kind(), ErrorKind::kServerStatus);
  EXPECT_EQ(error.RawCode(), std::numeric_limits<std::uint32_t>::max());
  EXPECT_FALSE(error.IsRetryable());
}

TEST(Errors, InnerJsonTakesPrecedenceAndMalformedJsonFallsBackToNumeric) {
  grpcxx::MetadataMap trailers;
  ASSERT_TRUE(trailers.Add("inner_error_msg", R"({"code":3001,"err_msg":"missing table"})"));
  ASSERT_TRUE(trailers.Add("x-greptime-err-code", "4003"));
  auto error = TrailerError(trailers);
  EXPECT_EQ(error.Code(), ErrorCode::kTableNotFound);
  EXPECT_EQ(error.RawCode(), 3001U);
  EXPECT_EQ(error.Message(), "missing table");
  grpcxx::MetadataMap fallback;
  ASSERT_TRUE(fallback.Add("inner_error_msg", "not JSON"));
  ASSERT_TRUE(fallback.Add("X-GREPTIME-ERR-CODE", "4003"));
  ASSERT_TRUE(fallback.Add("x-greptime-err-msg", "numeric diagnostic"));
  auto numeric = TrailerError(fallback);
  EXPECT_EQ(numeric.RawCode(), 4003U);
  EXPECT_EQ(numeric.Message(), "original gRPC failure");
  auto ok = Error::CheckOkTrailers(fallback);
  ASSERT_FALSE(ok);
  EXPECT_EQ(ok.error().Kind(), ErrorKind::kProtocol);
}

TEST(Errors, HintsWithoutDiagnosticsAndConflictingDuplicatesAreConservative) {
  for (std::string_view hint : {"true", "false"}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("inner_error_msg", "invalid JSON"));
    ASSERT_TRUE(trailers.Add(kGreptimeRetryHintHeader, hint));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Code(), ErrorCode::kUnknown);
    EXPECT_EQ(error.RawCode(), 1000U);
    EXPECT_EQ(error.Message(), "original gRPC failure");
    EXPECT_EQ(error.RetryHint(), hint == "true");
  }
  for (const auto &hints :
       {std::array{"true", "false"}, std::array{"false", "true"}, std::array{"true", "junk"},
        std::array{"junk", "true"}, std::array{"retryable", "non_retryable"}, std::array{"retryable", "TRUE"}}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("inner_error_msg", R"({"code":4003,"err_msg":"busy"})"));
    for (auto hint : hints)
      ASSERT_TRUE(trailers.Add(kGreptimeRetryHintHeader, hint));
    EXPECT_FALSE(TrailerError(trailers).IsRetryable());
  }
  grpcxx::MetadataMap agreeing;
  ASSERT_TRUE(agreeing.Add(kGreptimeRetryHintHeader, "true"));
  ASSERT_TRUE(agreeing.Add(kGreptimeRetryHintHeader, "retryable"));
  EXPECT_TRUE(TrailerError(agreeing).IsRetryable());
}

TEST(Errors, OkTrailersRequireActualApplicationFailure) {
  EXPECT_TRUE(Error::CheckOkTrailers(grpcxx::MetadataMap{}));
  for (std::string_view hint : {"true", "false", "retryable", "non_retryable"}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add(kGreptimeRetryHintHeader, hint));
    EXPECT_TRUE(Error::CheckOkTrailers(trailers));
    ASSERT_TRUE(trailers.Add("x-greptime-err-code", "0"));
    EXPECT_TRUE(Error::CheckOkTrailers(trailers));
  }
  grpcxx::MetadataMap success;
  ASSERT_TRUE(success.Add("inner_error_msg", R"({"code":0,"err_msg":""})"));
  EXPECT_TRUE(Error::CheckOkTrailers(success));
  grpcxx::MetadataMap numeric;
  ASSERT_TRUE(numeric.Add("x-greptime-err-code", "4003"));
  ASSERT_TRUE(numeric.Add("x-greptime-err-msg", "region unavailable"));
  auto numeric_result = Error::CheckOkTrailers(numeric);
  ASSERT_FALSE(numeric_result);
  EXPECT_EQ(numeric_result.error().RawCode(), 4003U);
  EXPECT_EQ(numeric_result.error().Message(), "region unavailable");
  grpcxx::MetadataMap diagnostic;
  ASSERT_TRUE(diagnostic.Add("x-greptime-err-msg", "diagnostic without code"));
  EXPECT_FALSE(Error::CheckOkTrailers(diagnostic));
  grpcxx::MetadataMap inner;
  ASSERT_TRUE(inner.Add("inner_error_msg", R"({"code":4000,"err_msg":"storage failed"})"));
  auto inner_result = Error::CheckOkTrailers(inner);
  ASSERT_FALSE(inner_result);
  EXPECT_EQ(inner_result.error().RawCode(), 4000U);
}

TEST(Errors, OkTrailersCannotHideNonzeroCodesBehindConflictingDuplicates) {
  for (const auto &codes : {std::array{"4003", "4004"}, std::array{"4003", "junk"}, std::array{"junk", "4003"},
                            std::array{"0", "4003"}, std::array{"4003", "0"}}) {
    grpcxx::MetadataMap trailers;
    for (auto code : codes)
      ASSERT_TRUE(trailers.Add("x-greptime-err-code", code));
    auto checked = Error::CheckOkTrailers(trailers);
    ASSERT_FALSE(checked);
    EXPECT_EQ(checked.error().Kind(), ErrorKind::kProtocol);
    EXPECT_FALSE(checked.error().IsRetryable());
    EXPECT_EQ(TrailerError(trailers).Kind(), ErrorKind::kGrpc);
    ASSERT_TRUE(trailers.Add("inner_error_msg", R"({"code":3001,"err_msg":"missing table"})"));
    auto inner = Error::CheckOkTrailers(trailers);
    ASSERT_FALSE(inner);
    EXPECT_EQ(inner.error().Kind(), ErrorKind::kServerStatus);
    EXPECT_EQ(inner.error().RawCode(), 3001U);
    EXPECT_EQ(inner.error().Message(), "missing table");
  }
  grpcxx::MetadataMap malformed;
  ASSERT_TRUE(malformed.Add("x-greptime-err-code", "junk"));
  EXPECT_TRUE(Error::CheckOkTrailers(malformed));
}

std::string Octets(std::initializer_list<unsigned char> bytes) {
  std::string text;
  text.reserve(bytes.size());
  for (const auto byte : bytes)
    text.push_back(static_cast<char>(byte));
  return text;
}

TEST(Errors, JsonEscapesRoundTripUtf8Boundaries) {
  struct Case {
    std::string_view escaped;
    std::string decoded;
  };
  for (const auto &item : std::array{
           Case{R"(\u0080)", Octets({0xC2, 0x80})},
           Case{R"(\u07FF)", Octets({0xDF, 0xBF})},
           Case{R"(\u0800)", Octets({0xE0, 0xA0, 0x80})},
           Case{R"(\uD7FF)", Octets({0xED, 0x9F, 0xBF})},
           Case{R"(\uE000)", Octets({0xEE, 0x80, 0x80})},
           Case{R"(\uDBFF\uDFFF)", Octets({0xF4, 0x8F, 0xBF, 0xBF})},
       }) {
    SCOPED_TRACE(item.escaped);
    grpcxx::MetadataMap trailers;
    const auto json = std::string(R"({"code":4003,"err_msg":")") + std::string(item.escaped) + "\"}";
    ASSERT_TRUE(trailers.Add("inner_error_msg", json));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Kind(), ErrorKind::kServerStatus);
    EXPECT_EQ(error.RawCode(), 4003U);
    EXPECT_EQ(error.Message(), item.decoded);
  }
}

TEST(Errors, ParseCodeRejectsLeadingZeroSignOverflowAndNonDigits) {
  for (std::string_view code : {"00", "01", "04003", "000", "+4003", "-1", "+0", "-0", "4294967296", "42949672950",
                                "18446744073709551616", "12a", "0x10", "4_003", "", " 4003", "4003 "}) {
    SCOPED_TRACE(code);
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("x-greptime-err-code", code));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Kind(), ErrorKind::kGrpc);
    EXPECT_NE(error.RawCode(), 4003U);
  }
  for (std::string_view code : {"00", "01", "04003", "000", "+4003", "-1", "+0", "-0", "4294967296", "42949672950",
                                "18446744073709551616", "12a", "0x10", "4_003", ""}) {
    SCOPED_TRACE(code);
    const auto json = std::string(R"({"code":)") + std::string(code) + R"(,"err_msg":"busy"})";
    grpcxx::MetadataMap inner;
    ASSERT_TRUE(inner.Add("inner_error_msg", json));
    auto parsed = TrailerError(inner);
    EXPECT_EQ(parsed.Kind(), ErrorKind::kGrpc);
    EXPECT_NE(parsed.RawCode(), 4003U);
    EXPECT_NE(parsed.Message(), "busy");
  }
  grpcxx::MetadataMap accepted;
  ASSERT_TRUE(accepted.Add("x-greptime-err-code", "4003"));
  EXPECT_EQ(TrailerError(accepted).RawCode(), 4003U);
  grpcxx::MetadataMap limit;
  ASSERT_TRUE(limit.Add("x-greptime-err-code", "4294967295"));
  EXPECT_EQ(TrailerError(limit).RawCode(), std::numeric_limits<std::uint32_t>::max());
}

TEST(Errors, TransportAndLocalFailuresNeverAuthorizeUncertainWriteReplay) {
  EXPECT_FALSE(Error::Transport("write outcome unknown").IsRetryable());
  EXPECT_EQ(Error::Transport("unknown").RetryHint(), std::nullopt);
  auto grpc = Error::Grpc(grpcxx::Status{grpcxx::StatusCode::kUnavailable});
  EXPECT_FALSE(grpc.IsRetryable());
  EXPECT_EQ(grpc.RetryHint(), std::nullopt);
  for (auto kind : {ErrorKind::kStream, ErrorKind::kInvalidConfig, ErrorKind::kSchemaViolation, ErrorKind::kEncode,
                    ErrorKind::kDecode, ErrorKind::kProtocol, ErrorKind::kCircuitOpen}) {
    Error error{kind, "failure"};
    EXPECT_FALSE(error.IsRetryable());
    EXPECT_EQ(error.RetryHint(), std::nullopt);
  }
  for (auto code : {4000U, 4001U, 4002U, 4003U, 4004U, 4005U, 5000U}) {
    EXPECT_FALSE(Error::ServerStatus(code, "transient").IsRetryable());
    EXPECT_TRUE(Error::ServerStatus(code, "transient", true).IsRetryable());
    EXPECT_FALSE(Error::ServerStatus(code, "transient", false).IsRetryable());
  }
}
} // namespace
