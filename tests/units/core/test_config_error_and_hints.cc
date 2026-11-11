/** Configuration and dual-layer application-error wire contracts. */
#include <greptimedb/core/config.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/hints.h>
#include <grpcxx/core/metadata.h>
#include <grpcxx/core/status.h>
#include <gtest/gtest.h>

#include <array>
#include <bit>
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
  if (!endpoint) return {};
  return ClientOptions{std::move(*endpoint)};
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
           Case{"node.example", TransportScheme::kGrpc, "node.example", 4001, "node.example:4001"},
           Case{"node.example:5000", TransportScheme::kGrpc, "node.example", 5000, "node.example:5000"},
           Case{"grpc://127.0.0.1", TransportScheme::kGrpc, "127.0.0.1", 4001, "127.0.0.1:4001"},
           Case{"http://localhost:42", TransportScheme::kGrpc, "localhost", 42, "localhost:42"},
           Case{"grpcs://node.example:65535", TransportScheme::kGrpcs, "node.example", 65535, "node.example:65535"},
           Case{"https://localhost", TransportScheme::kGrpcs, "localhost", 4001, "localhost:4001"},
           // clang-format off
           Case{"[::1]", TransportScheme::kGrpc, "::1", 4001, "[::1]:4001"},
           Case{"grpc://[::1]:1", TransportScheme::kGrpc, "::1", 1, "[::1]:1"},
           // clang-format on
           Case{"https://[2001:db8::1]:443", TransportScheme::kGrpcs, "2001:db8::1", 443, "[2001:db8::1]:443"}}) {
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
  for (std::string_view raw :
       {"", " ", "grpc://", "ftp://localhost", "localhost:0", "localhost:65536", "localhost:184467440737095516160",
        "localhost:-1", "localhost:+1", "localhost:", "localhost:a", "localhost/path", "localhost?query",
        "localhost#fragment", "user:pass@localhost", "a\nb", "::1",
        // clang-format off
                               "[::1",
                               "[not-ip]:42",
                               "[::1]garbage",
                               "[::1]:0",
        // clang-format on
        "unix://relative", "unix:///", "unix:///tmp/sock?query", "999.999.999.999", "-bad.example", "bad..example",
        "localhost:00000",
        // clang-format off
                               "grpcs://[::1]:0"}) {
    // clang-format on
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
  for (const auto &item :
       std::array{Case{"", "", "Basic Og=="}, Case{"", "secret", "Basic OnNlY3JldA=="},
                  Case{"alice", "", "Basic YWxpY2U6"}, Case{"üser", "päss:密碼", "Basic w7xzZXI6cMOkc3M65a+G56K8"},
                  Case{"user", "password:with:colons", "Basic dXNlcjpwYXNzd29yZDp3aXRoOmNvbG9ucw=="}}) {
    auto auth = Auth::Basic(item.username, item.password);
    ASSERT_TRUE(auth.Validate());
    EXPECT_EQ(auth.ToMetadataValue(), item.wire);
    auto options = Options();
    options.auth = auth;
    auto metadata = options.BuildCallMetadata();
    ASSERT_TRUE(metadata);
    EXPECT_EQ(metadata->Get(kGreptimeAuthHeader), item.wire);
    EXPECT_EQ(options.BuildRequestHeader().authorization().basic().password(), item.password);
  }
  EXPECT_FALSE(Auth::Basic("tenant:user", "secret").Validate());
  for (unsigned int value : std::views::iota(0U, 128U)) {
    if (value > 31 && value != 127) continue;
    const std::string control(1, static_cast<char>(value));
    EXPECT_FALSE(Auth::Basic(control, "ok").Validate());
    EXPECT_FALSE(Auth::Basic("ok", control).Validate());
    EXPECT_FALSE(Auth::Token(control).Validate());
  }
  EXPECT_FALSE(Auth::Token("bad\r\ntoken").Validate());
}

TEST(Config, DebugOutputRedactsSecrets) {
  for (const auto &auth : {Auth::Basic("alice", "private-password"), Auth::Token("private-token")}) {
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
  options.WithDbName("analytics-samples-live");
  EXPECT_EQ(options.catalog, "analytics");
  EXPECT_EQ(options.schema, "samples-live");
  EXPECT_EQ(options.DbName(), "analytics-samples-live");
  options.WithDbName("samples");
  EXPECT_EQ(options.catalog, "analytics");
  EXPECT_EQ(options.schema, "samples");
  options.catalog.clear();
  EXPECT_EQ(options.DbName(), "samples");
  auto header = options.BuildRequestHeader();
  EXPECT_EQ(header.dbname(), "samples");
  EXPECT_TRUE(header.timezone().empty());
  EXPECT_TRUE(header.tracing_context().empty());
  EXPECT_FALSE(header.has_authorization());
}

TEST(Config, HeaderLayersAgreeAndHintOverlayDoesNotMutateDefaults) {
  auto options = Options();
  options.WithDbName("analytics-samples");
  options.auth = Auth::Basic("alice", "secret");
  options.timezone = "UTC";
  options.request_timeout = 7s;
  options.compression = Compression::kZstd;
  ASSERT_TRUE(options.write_hints.SetAutoCreateTable(true));
  ASSERT_TRUE(options.write_hints.SetTtl("7d"));
  auto header = options.BuildRequestHeader({{"traceparent", "00-trace-span-01"}});
  EXPECT_EQ(header.catalog(), "analytics");
  EXPECT_EQ(header.schema(), "samples");
  EXPECT_EQ(header.dbname(), "analytics-samples");
  EXPECT_EQ(header.timezone(), "UTC");
  EXPECT_EQ(header.tracing_context().at("traceparent"), "00-trace-span-01");
  EXPECT_EQ(header.authorization().SerializeAsString(), options.auth->ToProtoAuthHeader().SerializeAsString());
  WriteHints extra;
  ASSERT_TRUE(extra.SetTtl("1d"));
  ASSERT_TRUE(extra.SetAppendMode(true));
  auto metadata = options.BuildCallMetadata(&extra);
  ASSERT_TRUE(metadata);
  EXPECT_EQ(metadata->Get(kGreptimeDbHeader), "analytics-samples");
  EXPECT_EQ(metadata->Get(kGreptimeAuthHeader), "Basic YWxpY2U6c2VjcmV0");
  EXPECT_EQ(metadata->Get(kGreptimeHintsHeader), "auto_create_table=true,ttl=1d,append_mode=true");
  EXPECT_EQ(options.write_hints.FormatHeaderValue(), "auto_create_table=true,ttl=7d");
  WriteHints empty;
  auto retained = options.BuildCallMetadata(&empty);
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->Get(kGreptimeHintsHeader), options.write_hints.FormatHeaderValue());
}

TEST(Config, MutableInvalidConfigurationFailsBeforeCalls) {
  for (int which : std::views::iota(0, 9)) {
    auto options = Options();
    switch (which) {
    case 0: options.endpoints.clear(); break;
    case 1: options.schema.clear(); break;
    case 2: options.max_in_flight = 0; break;
    case 3: options.request_timeout = 0ns; break;
    case 4: options.connect_timeout = std::chrono::nanoseconds::max(); break;
    case 5: options.auth = Auth::Token("bad\r\ntoken"); break;
    case 6: options.schema = "bad\nschema"; break;
    case 7: options.connect_timeout = 0ns; break;
    case 8: options.request_timeout = -1ns; break;
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
  for (std::string_view address : {"grpc://localhost:4001", "unix:///tmp/greptime.sock"}) {
    auto parsed = Endpoint::Parse(address);
    ASSERT_TRUE(parsed);
    auto destination = std::move(*parsed);
    EXPECT_TRUE(ClientOptions{std::move(destination)}.Validate());
    const bool missing_address = parsed->Scheme() == TransportScheme::kUnix
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
  options.compression = std::bit_cast<Compression>(255);
  EXPECT_FALSE(options.Validate());
  EXPECT_FALSE(options.BuildCallMetadata());
  WriteHints hints;
  ASSERT_TRUE(hints.SetMergeMode(MergeMode::kLastRow));
  EXPECT_FALSE(hints.SetMergeMode(std::bit_cast<MergeMode>(255)));
  EXPECT_EQ(hints.FormatHeaderValue(), "merge_mode=last_row");
}

TEST(Config, CompressionNamesAreExplicit) {
  for (const auto &[algorithm, expected] :
       std::array{std::pair{Compression::kIdentity, "identity"}, std::pair{Compression::kGzip, "gzip"},
                  std::pair{Compression::kDeflate, "deflate"}, std::pair{Compression::kSnappy, "snappy"},
                  std::pair{Compression::kZstd, "zstd"}, std::pair{Compression::kLz4, "lz4"}}) {
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
  EXPECT_EQ(hints.FormatHeaderValue(), "auto_create_table=true,ttl=7d,append_mode=false,merge_mode=last_"
                                       "non_null,physical_table=metrics,custom=a=b");
  ASSERT_TRUE(hints.SetTtl("1d"));
  ASSERT_TRUE(hints.SetHint("auto_create_table", "false"));
  EXPECT_EQ(hints.FormatHeaderValue(), "auto_create_table=false,ttl=1d,append_mode=false,merge_mode=last_"
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
  for (const auto &[key, value] : std::array{std::pair{"", "x"}, std::pair{"a=b", "x"}, std::pair{"a,b", "x"},
                                             std::pair{"a", "7d,append_mode=true"}, std::pair{"a", "\nx"},
                                             std::pair{"a", "café"}, std::pair{" a", "x"}, std::pair{"a", " x"},
                                             std::pair{"a ", "x"}, std::pair{"a", "7d "}, std::pair{"a", "\t7d"}}) {
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

TEST(Config, TimezoneAndTracingDropControlsNulAndInvalidUtf8) {
  auto options = Options();
  options.timezone = "UTC";
  EXPECT_TRUE(options.Validate());
  EXPECT_EQ(options.BuildRequestHeader().timezone(), "UTC");
  options.timezone = "欧洲/巴黎";
  EXPECT_TRUE(options.Validate());
  EXPECT_EQ(options.BuildRequestHeader().timezone(), "欧洲/巴黎");
  for (const auto &zone :
       {std::string{"UT\0C", 4}, std::string{"UT\nC"}, std::string{"\xff"}, std::string{"\xc3\x28"}}) {
    options.timezone = zone;
    EXPECT_FALSE(options.Validate());
    EXPECT_TRUE(options.BuildRequestHeader().timezone().empty());
  }
  options.timezone = "UTC";
  const std::array pairs{
      std::pair<std::string_view, std::string_view>{std::string_view{"trace\0parent", 12}, "00"},
      std::pair<std::string_view, std::string_view>{"bad", std::string_view{"a\nb", 3}},
      std::pair<std::string_view, std::string_view>{"bad-utf8", std::string_view{"\xff", 1}},
      std::pair<std::string_view, std::string_view>{"", "dropped"},
      std::pair<std::string_view, std::string_view>{"traceparent", "00-ok"},
  };
  auto header = options.BuildRequestHeader({pairs[0], pairs[1], pairs[2], pairs[3], pairs[4]});
  EXPECT_EQ(header.tracing_context().size(), 1);
  EXPECT_EQ(header.tracing_context().at("traceparent"), "00-ok");
  EXPECT_EQ(header.timezone(), "UTC");
}

std::string Octets(std::initializer_list<unsigned char> bytes) {
  std::string text;
  text.reserve(bytes.size());
  for (const auto byte : bytes)
    text.push_back(static_cast<char>(byte));
  return text;
}

TEST(Config, TryBuildRejectsHeaderTextAndEmptyTrace) {
  struct Case {
    std::string text;
    bool accepted;
  };
  const auto cases = std::array{
      Case{Octets({0xC0, 0xAF}), false},
      Case{Octets({0xE0, 0x80, 0xAF}), false},
      Case{Octets({0xED, 0xA0, 0x80}), false},
      Case{Octets({0xF4, 0x90, 0x80, 0x80}), false},
      Case{Octets({0xC2}), false},
      Case{Octets({0xE0, 0xA0}), false},
      Case{Octets({0xF0, 0x90, 0x80}), false},
      Case{Octets({0x0D}), false},
      Case{Octets({0x7F}), false},
      Case{Octets({0x61, 0x00, 0x62}), false},
      Case{Octets({0xC2, 0x80}), true},
      Case{Octets({0xDF, 0xBF}), true},
      Case{Octets({0xE0, 0xA0, 0x80}), true},
      Case{Octets({0xED, 0x9F, 0xBF}), true},
      Case{Octets({0xEE, 0x80, 0x80}), true},
      Case{Octets({0xF4, 0x8F, 0xBF, 0xBF}), true},
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.text.size());
    auto options = Options();
    options.timezone = item.text;
    auto validated = options.Validate();
    auto built = options.TryBuildRequestHeader();
    if (item.accepted) {
      EXPECT_TRUE(validated);
      ASSERT_TRUE(built);
      EXPECT_EQ(built->timezone(), item.text);
      auto traced = options.TryBuildRequestHeader({{"traceparent", item.text}});
      ASSERT_TRUE(traced);
      EXPECT_EQ(traced->tracing_context().at("traceparent"), item.text);
    } else {
      ASSERT_FALSE(validated);
      EXPECT_EQ(validated.error().Kind(), ErrorKind::kInvalidConfig);
      ASSERT_FALSE(built);
      EXPECT_EQ(built.error().Kind(), ErrorKind::kInvalidConfig);
      options.timezone = "UTC";
      auto traced = options.TryBuildRequestHeader({{"traceparent", item.text}});
      ASSERT_FALSE(traced);
      EXPECT_EQ(traced.error().Kind(), ErrorKind::kInvalidConfig);
      auto keyed = options.TryBuildRequestHeader({{item.text, "span"}});
      ASSERT_FALSE(keyed);
      EXPECT_EQ(keyed.error().Kind(), ErrorKind::kInvalidConfig);
    }
  }
  auto options = Options();
  options.timezone = "UTC";
  for (const auto &pair : std::array{std::pair<std::string_view, std::string_view>{"", "span"},
                                     std::pair<std::string_view, std::string_view>{"traceparent", ""},
                                     std::pair<std::string_view, std::string_view>{"", ""}}) {
    auto traced = options.TryBuildRequestHeader({pair});
    ASSERT_FALSE(traced);
    EXPECT_EQ(traced.error().Kind(), ErrorKind::kInvalidConfig);
  }
  auto kept = options.TryBuildRequestHeader({{"traceparent", "00-ok"}});
  ASSERT_TRUE(kept);
  EXPECT_EQ(kept->timezone(), "UTC");
  EXPECT_EQ(kept->tracing_context().at("traceparent"), "00-ok");
  EXPECT_EQ(kept->schema(), options.schema);
}

} // namespace
