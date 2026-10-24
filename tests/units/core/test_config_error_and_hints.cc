/** Configuration and dual-layer application-error wire contracts. */
#include <greptimedb/core/config.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/hints.h>
#include <grpcxx/core/metadata.h>
#include <grpcxx/core/status.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
using namespace greptimedb;
using namespace std::chrono_literals;

ClientOptions Options() {
  auto endpoint = Endpoint::Parse("localhost:4001");
  EXPECT_TRUE(endpoint);
  if (!endpoint)
    return {};
  return ClientOptions{std::move(*endpoint)};
}

Error TrailerError(const grpcxx::MetadataMap &metadata) {
  return Error::FromGrpcStatusAndTrailers(
      grpcxx::Status{grpcxx::StatusCode::kUnavailable, "original gRPC failure"},
      metadata);
}

TEST(Config, EndpointSchemesPortsAndAuthorities) {
  struct Case {
    std::string_view raw;
    TransportScheme scheme;
    std::string_view host;
    std::uint16_t port;
    std::string_view authority;
  };
  for (const auto &item : std::array{
           Case{"node.example", TransportScheme::kGrpc, "node.example", 4001,
                "node.example:4001"},
           Case{"node.example:5000", TransportScheme::kGrpc, "node.example",
                5000, "node.example:5000"},
           Case{"grpc://127.0.0.1", TransportScheme::kGrpc, "127.0.0.1", 4001,
                "127.0.0.1:4001"},
           Case{"http://localhost:42", TransportScheme::kGrpc, "localhost", 42,
                "localhost:42"},
           Case{"grpcs://node.example:65535", TransportScheme::kGrpcs,
                "node.example", 65535, "node.example:65535"},
           Case{"https://localhost", TransportScheme::kGrpcs, "localhost", 4001,
                "localhost:4001"},
           Case{"[::1]", TransportScheme::kGrpc, "::1", 4001, "[::1]:4001"},
           Case{"grpc://[::1]:1", TransportScheme::kGrpc, "::1", 1, "[::1]:1"},
           Case{"https://[2001:db8::1]:443", TransportScheme::kGrpcs,
                "2001:db8::1", 443, "[2001:db8::1]:443"}}) {
    SCOPED_TRACE(item.raw);
    auto endpoint = Endpoint::Parse(item.raw);
    ASSERT_TRUE(endpoint);
    EXPECT_EQ(endpoint->Scheme(), item.scheme);
    EXPECT_EQ(endpoint->Host(), item.host);
    EXPECT_EQ(endpoint->Port(), item.port);
    EXPECT_EQ(endpoint->Authority(), item.authority);
    EXPECT_EQ(endpoint->SocketAddress(), item.authority);
    EXPECT_FALSE(endpoint->UnixPath());
  }
  auto local = Endpoint::Parse("unix:///tmp/greptime.sock");
  ASSERT_TRUE(local);
  EXPECT_EQ(local->Scheme(), TransportScheme::kUnix);
  EXPECT_EQ(local->UnixPath(), "/tmp/greptime.sock");
  EXPECT_EQ(local->Authority(), "localhost");
}

TEST(Config, RejectsAmbiguousOrUnsafeEndpoints) {
  for (std::string_view raw : {"",
                               " ",
                               "grpc://",
                               "ftp://localhost",
                               "localhost:0",
                               "localhost:65536",
                               "localhost:184467440737095516160",
                               "localhost:-1",
                               "localhost:+1",
                               "localhost:",
                               "localhost:a",
                               "localhost/path",
                               "localhost?query",
                               "localhost#fragment",
                               "user:pass@localhost",
                               "a\nb",
                               "::1",
                               "[::1",
                               "[not-ip]:42",
                               "[::1]garbage",
                               "[::1]:0",
                               "unix://relative",
                               "unix:///",
                               "unix:///tmp/sock?query",
                               "999.999.999.999",
                               "-bad.example",
                               "bad..example",
                               "localhost:00000",
                               "grpcs://[::1]:0"}) {
    SCOPED_TRACE(raw);
    auto endpoint = Endpoint::Parse(raw);
    ASSERT_FALSE(endpoint);
    EXPECT_EQ(endpoint.error().Kind(), ErrorKind::kInvalidConfig);
  }
  EXPECT_FALSE(Endpoint::Parse(std::string_view{"a\0b", 3}));
}

TEST(Config, AuthMetadataAndProtobufAgree) {
  auto basic = Auth::Basic("alice", "secret");
  ASSERT_TRUE(basic.Validate());
  EXPECT_EQ(basic.ToMetadataValue(), "Basic YWxpY2U6c2VjcmV0");
  auto auth = basic.ToProtoAuthHeader();
  ASSERT_TRUE(auth.has_basic());
  EXPECT_EQ(auth.basic().username(), "alice");
  EXPECT_EQ(auth.basic().password(), "secret");
  auto token = Auth::Token("opaque.token");
  ASSERT_TRUE(token.Validate());
  EXPECT_EQ(token.ToMetadataValue(), "Bearer opaque.token");
  ASSERT_TRUE(token.ToProtoAuthHeader().has_token());
  EXPECT_EQ(token.ToProtoAuthHeader().token().token(), "opaque.token");
}

TEST(Config, BasicCredentialsPreserveEmptyUtf8AndPasswordColons) {
  struct Case {
    std::string username;
    std::string password;
    std::string_view wire;
  };
  for (const auto &item : std::array{
           Case{"", "", "Basic Og=="}, Case{"", "secret", "Basic OnNlY3JldA=="},
           Case{"alice", "", "Basic YWxpY2U6"},
           Case{"üser", "päss:密碼", "Basic w7xzZXI6cMOkc3M65a+G56K8"},
           Case{"user", "password:with:colons",
                "Basic dXNlcjpwYXNzd29yZDp3aXRoOmNvbG9ucw=="}}) {
    auto auth = Auth::Basic(item.username, item.password);
    ASSERT_TRUE(auth.Validate());
    EXPECT_EQ(auth.ToMetadataValue(), item.wire);
    auto options = Options();
    options.auth = auth;
    auto metadata = options.BuildCallMetadata();
    ASSERT_TRUE(metadata);
    EXPECT_EQ(metadata->Get(kGreptimeAuthHeader), item.wire);
    EXPECT_EQ(options.BuildRequestHeader().authorization().basic().password(),
              item.password);
  }
  EXPECT_FALSE(Auth::Basic("tenant:user", "secret").Validate());
  for (unsigned int value : std::views::iota(0U, 128U)) {
    if (value > 31 && value != 127)
      continue;
    const std::string control(1, static_cast<char>(value));
    EXPECT_FALSE(Auth::Basic(control, "ok").Validate());
    EXPECT_FALSE(Auth::Basic("ok", control).Validate());
    EXPECT_FALSE(Auth::Token(control).Validate());
  }
  EXPECT_FALSE(Auth::Token("bad\r\ntoken").Validate());
}

TEST(Config, DebugOutputRedactsSecrets) {
  for (const auto &auth : {Auth::Basic("alice", "private-password"),
                           Auth::Token("private-token")}) {
    const auto debug = auth.DebugString();
    EXPECT_NE(debug.find("<redacted>"), std::string::npos);
    EXPECT_EQ(debug.find("private-password"), std::string::npos);
    EXPECT_EQ(debug.find("private-token"), std::string::npos);
    auto options = Options();
    options.auth = auth;
    const auto nested = options.DebugString();
    EXPECT_NE(nested.find("<redacted>"), std::string::npos);
    EXPECT_EQ(nested.find("private-password"), std::string::npos);
    EXPECT_EQ(nested.find("private-token"), std::string::npos);
  }
}

TEST(Config, DefaultsDatabaseRoutingAndRequestHeader) {
  auto options = Options();
  EXPECT_EQ(options.catalog, "greptime");
  EXPECT_EQ(options.schema, "public");
  EXPECT_EQ(options.DbName(), "greptime-public");
  EXPECT_TRUE(options.tcp_nodelay);
  EXPECT_EQ(options.max_in_flight, 8U);
  EXPECT_EQ(options.compression, Compression::kIdentity);
  EXPECT_FALSE(options.auth);
  EXPECT_FALSE(options.timezone);
  EXPECT_GT(options.connect_timeout, 0ns);
  EXPECT_GT(options.request_timeout, 0ns);
  options.WithDbName("analytics-ticks-live");
  EXPECT_EQ(options.catalog, "analytics");
  EXPECT_EQ(options.schema, "ticks-live");
  EXPECT_EQ(options.DbName(), "analytics-ticks-live");
  options.WithDbName("ticks");
  EXPECT_EQ(options.catalog, "analytics");
  EXPECT_EQ(options.schema, "ticks");
  options.catalog.clear();
  EXPECT_EQ(options.DbName(), "ticks");
  auto header = options.BuildRequestHeader();
  EXPECT_EQ(header.dbname(), "ticks");
  EXPECT_TRUE(header.timezone().empty());
  EXPECT_TRUE(header.tracing_context().empty());
  EXPECT_FALSE(header.has_authorization());
}

TEST(Config, HeaderLayersAgreeAndHintOverlayDoesNotMutateDefaults) {
  auto options = Options();
  options.WithDbName("analytics-ticks");
  options.auth = Auth::Basic("alice", "secret");
  options.timezone = "UTC";
  options.request_timeout = 7s;
  options.compression = Compression::kZstd;
  ASSERT_TRUE(options.write_hints.SetAutoCreateTable(true));
  ASSERT_TRUE(options.write_hints.SetTtl("7d"));
  auto header =
      options.BuildRequestHeader({{"traceparent", "00-trace-span-01"}});
  EXPECT_EQ(header.catalog(), "analytics");
  EXPECT_EQ(header.schema(), "ticks");
  EXPECT_EQ(header.dbname(), "analytics-ticks");
  EXPECT_EQ(header.timezone(), "UTC");
  EXPECT_EQ(header.tracing_context().at("traceparent"), "00-trace-span-01");
  EXPECT_EQ(header.authorization().SerializeAsString(),
            options.auth->ToProtoAuthHeader().SerializeAsString());
  WriteHints extra;
  ASSERT_TRUE(extra.SetTtl("1d"));
  ASSERT_TRUE(extra.SetAppendMode(true));
  auto metadata = options.BuildCallMetadata(&extra);
  ASSERT_TRUE(metadata);
  EXPECT_EQ(metadata->Get(kGreptimeDbHeader), "analytics-ticks");
  EXPECT_EQ(metadata->Get(kGreptimeAuthHeader), "Basic YWxpY2U6c2VjcmV0");
  EXPECT_EQ(metadata->Get(kGreptimeHintsHeader),
            "auto_create_table=true,ttl=1d,append_mode=true");
  EXPECT_EQ(options.write_hints.FormatHeaderValue(),
            "auto_create_table=true,ttl=7d");
  WriteHints empty;
  auto retained = options.BuildCallMetadata(&empty);
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->Get(kGreptimeHintsHeader),
            options.write_hints.FormatHeaderValue());
}

TEST(Config, MutableInvalidConfigurationFailsBeforeCalls) {
  for (int which : std::views::iota(0, 9)) {
    auto options = Options();
    switch (which) {
    case 0:
      options.endpoints.clear();
      break;
    case 1:
      options.schema.clear();
      break;
    case 2:
      options.max_in_flight = 0;
      break;
    case 3:
      options.request_timeout = 0ns;
      break;
    case 4:
      options.connect_timeout = std::chrono::nanoseconds::max();
      break;
    case 5:
      options.auth = Auth::Token("bad\r\ntoken");
      break;
    case 6:
      options.schema = "bad\nschema";
      break;
    case 7:
      options.connect_timeout = 0ns;
      break;
    case 8:
      options.request_timeout = -1ns;
      break;
    }
    auto metadata = options.BuildCallMetadata();
    ASSERT_FALSE(metadata) << which;
    EXPECT_EQ(metadata.error().Kind(), ErrorKind::kInvalidConfig);
  }
  for (auto timeout : {1ns, 1000ns}) {
    auto options = Options();
    options.connect_timeout = timeout;
    options.request_timeout = timeout;
    EXPECT_TRUE(options.BuildCallMetadata());
    EXPECT_EQ(options.request_timeout, timeout);
  }
}

TEST(Config, MovedFromEndpointsCannotIntroduceEmptyConnectionAddresses) {
  for (std::string_view address :
       {"grpc://localhost:4001", "unix:///tmp/greptime.sock"}) {
    auto parsed = Endpoint::Parse(address);
    ASSERT_TRUE(parsed);
    auto destination = std::move(*parsed);
    EXPECT_TRUE(ClientOptions{std::move(destination)}.Validate());
    const bool missing_address =
        parsed->Scheme() == TransportScheme::kUnix
            ? (!parsed->UnixPath() || parsed->UnixPath()->empty())
            : parsed->Host().empty();
    /** Moved-from string contents are unspecified; reject every observed empty
     * address. */
    if (missing_address) {
      auto reused = ClientOptions{*parsed};
      auto checked = reused.Validate();
      ASSERT_FALSE(checked);
      EXPECT_EQ(checked.error().Kind(), ErrorKind::kInvalidConfig);
      EXPECT_FALSE(reused.BuildCallMetadata());
    }
  }
}

TEST(Config, RejectsUnknownEnumValuesWithoutMutatingHints) {
  auto options = Options();
  options.compression = static_cast<Compression>(255);
  EXPECT_FALSE(options.Validate());
  EXPECT_FALSE(options.BuildCallMetadata());
  WriteHints hints;
  ASSERT_TRUE(hints.SetMergeMode(MergeMode::kLastRow));
  EXPECT_FALSE(hints.SetMergeMode(static_cast<MergeMode>(255)));
  EXPECT_EQ(hints.FormatHeaderValue(), "merge_mode=last_row");
}

TEST(Config, CompressionNamesAreExplicit) {
  for (const auto &[algorithm, expected] :
       std::array{std::pair{Compression::kIdentity, "identity"},
                  std::pair{Compression::kGzip, "gzip"},
                  std::pair{Compression::kDeflate, "deflate"},
                  std::pair{Compression::kSnappy, "snappy"},
                  std::pair{Compression::kZstd, "zstd"},
                  std::pair{Compression::kLz4, "lz4"}}) {
    EXPECT_EQ(CompressionName(algorithm), expected);
  }
}

TEST(Hints, FormatsOrderedDirectivesAndReplacesReservedAndCustomValues) {
  WriteHints hints;
  EXPECT_TRUE(hints.IsEmpty());
  EXPECT_TRUE(hints.FormatHeaderValue().empty());
  EXPECT_EQ(MergeModeName(MergeMode::kLastRow), "last_row");
  EXPECT_EQ(MergeModeName(MergeMode::kLastNonNull), "last_non_null");
  ASSERT_TRUE(hints.SetAutoCreateTable(true));
  ASSERT_TRUE(hints.SetTtl("7d"));
  ASSERT_TRUE(hints.SetAppendMode(false));
  ASSERT_TRUE(hints.SetMergeMode(MergeMode::kLastNonNull));
  ASSERT_TRUE(hints.SetPhysicalTable("metrics"));
  ASSERT_TRUE(hints.SetHint("custom", "a=b"));
  EXPECT_EQ(hints.FormatHeaderValue(),
            "auto_create_table=true,ttl=7d,append_mode=false,merge_mode=last_"
            "non_null,physical_table=metrics,custom=a=b");
  ASSERT_TRUE(hints.SetTtl("1d"));
  ASSERT_TRUE(hints.SetHint("auto_create_table", "false"));
  EXPECT_EQ(hints.FormatHeaderValue(),
            "auto_create_table=false,ttl=1d,append_mode=false,merge_mode=last_"
            "non_null,physical_table=metrics,custom=a=b");
  ASSERT_TRUE(hints.SetHint("ttl", "2d"));
  ASSERT_TRUE(hints.SetHint("skip_wal", "false"));
  ASSERT_TRUE(hints.SetHint("skip_wal", "true"));
  EXPECT_EQ(hints.Entries().size(), 7U);
  EXPECT_NE(hints.FormatHeaderValue().find("ttl=2d"), std::string::npos);
  EXPECT_NE(hints.FormatHeaderValue().find("skip_wal=true"), std::string::npos);
}

TEST(Hints, RejectsInjectionAtomicallyAndAcceptsEmptyValues) {
  WriteHints hints;
  ASSERT_TRUE(hints.SetTtl("7d"));
  for (const auto &[key, value] : std::array{
           std::pair{"", "x"}, std::pair{"a=b", "x"}, std::pair{"a,b", "x"},
           std::pair{"a", "7d,append_mode=true"}, std::pair{"a", "\nx"},
           std::pair{"a", "café"}, std::pair{" a", "x"}, std::pair{"a", " x"},
           std::pair{"a ", "x"}, std::pair{"a", "7d "},
           std::pair{"a", "\t7d"}}) {
    EXPECT_FALSE(hints.SetHint(key, value));
    EXPECT_EQ(hints.FormatHeaderValue(), "ttl=7d");
  }
  EXPECT_FALSE(hints.SetHint("ttl", std::string{"value\0", 6}));
  EXPECT_EQ(hints.FormatHeaderValue(), "ttl=7d");
  ASSERT_TRUE(hints.SetHint("custom", ""));
  EXPECT_EQ(hints.FormatHeaderValue(), "ttl=7d,custom=");
}

TEST(Hints, MetadataNamesMatchWireContract) {
  EXPECT_EQ(kGreptimeDbHeader, "x-greptime-db-name");
  EXPECT_EQ(kGreptimeAuthHeader, "x-greptime-auth");
  EXPECT_EQ(kGreptimeHintsHeader, "x-greptime-hints");
  EXPECT_EQ(kGreptimeRetryHintHeader, "x-greptime-err-retry-hint");
  EXPECT_EQ(kGreptimeInnerErrorHeader, "INNER_ERROR_MSG");
}

TEST(Errors, MapsEveryLegacyCategoryAndPreservesUnknownRawCodes) {
  for (const auto &[raw, code] :
       std::array{std::pair{0U, ErrorCode::kSuccess},
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
  for (auto raw :
       {1U, 1007U, 5999U, 7000U, std::numeric_limits<std::uint32_t>::max()}) {
    auto error = Error::ServerStatus(raw, "unknown");
    EXPECT_EQ(error.Code(), ErrorCode::kOther);
    EXPECT_EQ(error.RawCode(), raw);
  }
}

TEST(Errors, VersionedClassifierIsExplicitAndKeepsLegacyNumbersStable) {
  for (const auto &[raw, code] :
       std::array{std::pair{0U, ErrorCode::kSuccess},
                  std::pair{3000U, ErrorCode::kPlan},
                  std::pair{3001U, ErrorCode::kEngineExecute},
                  std::pair{4000U, ErrorCode::kTableAlreadyExists},
                  std::pair{4001U, ErrorCode::kTableNotFound},
                  std::pair{4003U, ErrorCode::kRegionBusy},
                  std::pair{4005U, ErrorCode::kRegionNotFound},
                  std::pair{5000U, ErrorCode::kStorageUnavailable},
                  std::pair{6000U, ErrorCode::kResourceExhausted},
                  std::pair{6001U, ErrorCode::kRateLimited},
                  std::pair{7000U, ErrorCode::kAuthFailed},
                  std::pair{7002U, ErrorCode::kAuthFailed},
                  std::pair{7006U, ErrorCode::kAuthFailed},
                  std::pair{std::numeric_limits<std::uint32_t>::max(),
                            ErrorCode::kOther}}) {
    EXPECT_EQ(ClassifyV121ErrorCode(raw), code) << raw;
  }
  EXPECT_EQ(ClassifyErrorCode(3000), ErrorCode::kTableAlreadyExists);
  EXPECT_EQ(ClassifyErrorCode(4001), ErrorCode::kRegionNotFound);
  EXPECT_EQ(ClassifyErrorCode(6001), ErrorCode::kAuthFailed);
  EXPECT_EQ(ClassifyErrorCode(7000), ErrorCode::kOther);
  EXPECT_EQ(ClassifyErrorCode(7002), ErrorCode::kAuthFailed);
}

TEST(Errors, AggregateFailurePreservesFinalAttemptRetryGuidance) {
  auto aggregate =
      Error::AllEndpointsFailed(2, Error::ServerStatus(4003, "busy", true));
  EXPECT_EQ(aggregate.Kind(), ErrorKind::kAllEndpointsFailed);
  EXPECT_EQ(aggregate.Attempts(), 2U);
  EXPECT_EQ(aggregate.Code(), ErrorCode::kRegionBusy);
  EXPECT_EQ(aggregate.RawCode(), 4003U);
  EXPECT_TRUE(aggregate.IsRetryable());
  EXPECT_EQ(aggregate.RetryHint(), true);
  auto uncertain =
      Error::AllEndpointsFailed(3, Error::Transport("unknown delivery"));
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
  for (const auto &[hint, expected] :
       std::array{std::pair{"true", true}, std::pair{"retryable", true},
                  std::pair{"false", false}, std::pair{"non_retryable", false},
                  std::pair{"", false}, std::pair{"TRUE", false},
                  std::pair{"True", false}, std::pair{"1", false},
                  std::pair{" true ", false}, std::pair{"junk", false}}) {
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
  for (std::string_view hint : {"true", "retryable", "false", "non_retryable",
                                "TRUE", "Retryable", "1", " true", "true "}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("INNER_ERROR_MSG",
                             R"({"code":4003,"err_msg":"region busy"})"));
    ASSERT_TRUE(trailers.Add(kGreptimeRetryHintHeader, hint));
    auto error = TrailerError(trailers);
    EXPECT_EQ(error.Code(), ErrorCode::kRegionBusy);
    EXPECT_EQ(error.RawCode(), 4003U);
    EXPECT_EQ(error.Message(), "region busy");
    EXPECT_EQ(error.RetryHint(), hint == "true" || hint == "retryable");
    EXPECT_EQ(error.IsRetryable(), hint == "true" || hint == "retryable");
  }
  grpcxx::MetadataMap unknown;
  ASSERT_TRUE(unknown.Add("inner_error_msg",
                          R"({"code":999999,"err_msg":"unknown failure"})"));
  auto error = TrailerError(unknown);
  EXPECT_EQ(error.Code(), ErrorCode::kOther);
  EXPECT_EQ(error.RawCode(), 999999U);
  EXPECT_FALSE(error.IsRetryable());
}

TEST(Errors, MalformedInnerErrorsPreserveOriginalGrpcFailure) {
  for (std::string_view inner :
       {"not json", "[]", "null", R"({"code":0,"err_msg":"success"})",
        R"({"code":-1,"err_msg":"bad"})",
        R"({"code":4294967296,"err_msg":"overflow"})",
        R"({"code":"4003","err_msg":"string"})",
        R"({"code":4003.5,"err_msg":"fraction"})", R"({"code":4003})",
        R"({"code":4003,"err_msg":2})", R"({"code":4003,"err_msg":"x"}garbage)",
        R"({"code":4003,"err_msg":"\uD800"})",
        R"({"code":4003,"err_msg":"\uDC00"})",
        R"({"code":4003,"err_msg":"\q"})",
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
  auto error = Error::FromGrpcStatusAndTrailers(
      grpcxx::Status{grpcxx::StatusCode::kUnavailable, "original"}, nullptr);
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
       {R"({"code":4003,"err_msg":"busy","unknown":[1,]})",
        R"({"code":4003,"err_msg":"busy","unknown":{"x":}})",
        R"({"code":4003,"err_msg":"busy","unknown":{"x":false,}})",
        R"({"code":4003,"err_msg":"busy","unknown":01})",
        R"({"code":4003,"err_msg":"busy","unknown":1e})",
        R"({"code":04003,"err_msg":"busy"})",
        R"({"code":4003,"err_msg":"busy"} true)",
        R"({"code":4003,"code":4004,"err_msg":"busy"})",
        R"({"code":4003,"code":4003,"err_msg":"busy"})",
        R"({"code":4003,"err_msg":"busy","err_msg":"retry"})"}) {
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
    const auto json =
        std::string{R"({"code":4003,"err_msg":"busy","unknown":)"} + nested +
        "}";
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
  for (std::string_view code : {"", "junk", "4294967296", "-1", "+4003", "0",
                                "4003.0", " 4003", "4003 "}) {
    SCOPED_TRACE(code);
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(trailers.Add("x-greptime-err-code", code));
    EXPECT_EQ(TrailerError(trailers).Kind(), ErrorKind::kGrpc);
  }
  for (const auto &codes :
       {std::array{"4003", "4004"}, std::array{"4003", "invalid"},
        std::array{"invalid", "4003"}}) {
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
  ASSERT_TRUE(trailers.Add("inner_error_msg",
                           R"({"code":3001,"err_msg":"missing table"})"));
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
       {std::array{"true", "false"}, std::array{"false", "true"},
        std::array{"true", "junk"}, std::array{"junk", "true"},
        std::array{"retryable", "non_retryable"},
        std::array{"retryable", "TRUE"}}) {
    grpcxx::MetadataMap trailers;
    ASSERT_TRUE(
        trailers.Add("inner_error_msg", R"({"code":4003,"err_msg":"busy"})"));
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
  for (std::string_view hint :
       {"true", "false", "retryable", "non_retryable"}) {
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
  ASSERT_TRUE(inner.Add("inner_error_msg",
                        R"({"code":4000,"err_msg":"storage failed"})"));
  auto inner_result = Error::CheckOkTrailers(inner);
  ASSERT_FALSE(inner_result);
  EXPECT_EQ(inner_result.error().RawCode(), 4000U);
}

TEST(Errors, OkTrailersCannotHideNonzeroCodesBehindConflictingDuplicates) {
  for (const auto &codes :
       {std::array{"4003", "4004"}, std::array{"4003", "junk"},
        std::array{"junk", "4003"}, std::array{"0", "4003"},
        std::array{"4003", "0"}}) {
    grpcxx::MetadataMap trailers;
    for (auto code : codes)
      ASSERT_TRUE(trailers.Add("x-greptime-err-code", code));
    auto checked = Error::CheckOkTrailers(trailers);
    ASSERT_FALSE(checked);
    EXPECT_EQ(checked.error().Kind(), ErrorKind::kProtocol);
    EXPECT_FALSE(checked.error().IsRetryable());
    EXPECT_EQ(TrailerError(trailers).Kind(), ErrorKind::kGrpc);
    ASSERT_TRUE(trailers.Add("inner_error_msg",
                             R"({"code":3001,"err_msg":"missing table"})"));
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

TEST(Errors, TransportAndLocalFailuresNeverAuthorizeUncertainWriteReplay) {
  EXPECT_FALSE(Error::Transport("write outcome unknown").IsRetryable());
  EXPECT_EQ(Error::Transport("unknown").RetryHint(), std::nullopt);
  auto grpc = Error::Grpc(grpcxx::Status{grpcxx::StatusCode::kUnavailable});
  EXPECT_FALSE(grpc.IsRetryable());
  EXPECT_EQ(grpc.RetryHint(), std::nullopt);
  for (auto kind :
       {ErrorKind::kStream, ErrorKind::kInvalidConfig,
        ErrorKind::kSchemaViolation, ErrorKind::kEncode, ErrorKind::kDecode,
        ErrorKind::kProtocol, ErrorKind::kCircuitOpen}) {
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
