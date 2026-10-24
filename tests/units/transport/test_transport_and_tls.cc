#include "transport_test_support.h"

#include <gtest/gtest.h>
#include <netinet/tcp.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <ranges>
#include <vector>

namespace greptimedb::testing {
namespace {

void CheckBytes(TransportPair pair, std::size_t size = 40'000) {
  std::vector<std::byte> input(size), received;
  for (std::size_t i = 0; i != size; ++i)
    input[i] = static_cast<std::byte>(i % 251);
  std::array<std::byte, 37> output{};
  for (const bool reverse : {false, true}) {
    auto &writer = reverse ? *pair.second : *pair.first;
    auto &reader = reverse ? *pair.first : *pair.second;
    std::size_t sent = 0;
    received.clear();
    for (int tick = 0; tick != 200'000 && received.size() != size; ++tick) {
      ASSERT_TRUE(writer.PollOnce());
      ASSERT_TRUE(reader.PollOnce());
      if (sent != size) {
        auto written = writer.Write(std::span{input}.subspan(sent));
        ASSERT_TRUE(written);
        ASSERT_LE(*written, size - sent);
        sent += *written;
      }
      auto read = reader.Read(output);
      ASSERT_TRUE(read);
      ASSERT_LE(*read, output.size());
      received.insert(received.end(), output.begin(), output.begin() + *read);
    }
    EXPECT_EQ(sent, size);
    EXPECT_EQ(received, input);
    EXPECT_TRUE(writer.IsOpen());
    EXPECT_TRUE(reader.IsOpen());
  }
}

TEST(Transport, MemoryPipeBackpressurePreservesOrderingAndDrainsBeforeEof) {
  auto pair = MemoryPipe::CreatePair(7);
  ASSERT_TRUE(pair);
  std::array<std::byte, 16> out{};
  auto empty = pair->second->Read(out);
  ASSERT_TRUE(empty);
  EXPECT_EQ(*empty, 0U);
  const std::string input = "abcdefghijk";
  auto first = pair->first->Write(Bytes(input));
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, 7U);
  auto blocked = pair->first->Write(Bytes(input).subspan(*first));
  ASSERT_TRUE(blocked);
  EXPECT_EQ(*blocked, 0U);
  auto read = pair->second->Read(std::span{out}.first(3));
  ASSERT_TRUE(read);
  ASSERT_EQ(*read, 3U);
  EXPECT_TRUE(std::ranges::equal(std::span{out}.first(3), Bytes("abc")));
  auto tail = pair->first->Write(Bytes(input).subspan(7));
  ASSERT_TRUE(tail);
  EXPECT_EQ(*tail, 3U);
  pair->first->Close();
  EXPECT_FALSE(pair->first->IsOpen());
  auto drained = pair->second->Read(out);
  ASSERT_TRUE(drained);
  ASSERT_EQ(*drained, 7U);
  EXPECT_TRUE(
      std::ranges::equal(std::span{out}.first(*drained), Bytes("defghij")));
  auto eof = pair->second->Read(out);
  ASSERT_FALSE(eof);
  EXPECT_EQ(eof.error().Code(), grpcxx::StatusCode::kUnavailable);
  EXPECT_FALSE(pair->second->Write(Bytes("x")));
  pair->first->Close();
  EXPECT_FALSE(pair->first->Write(Bytes("x")));
}

TEST(Transport, FragmentedMemoryPipeExchangesBothDirections) {
  auto pair = MemoryPipe::CreatePair(11);
  ASSERT_TRUE(pair);
  EXPECT_EQ(pair->first->NativeHandle(), -1);
  CheckBytes(std::move(*pair));
}

TEST(Transport, TcpLoopbackIsNonblockingAndDisablesNagle) {
  auto pair = TcpPair();
  ASSERT_TRUE(pair);
  for (const auto *transport : {pair->first.get(), pair->second.get()}) {
    ASSERT_GE(transport->NativeHandle(), 0);
    int nodelay = 0;
    socklen_t size = sizeof(nodelay);
    ASSERT_EQ(::getsockopt(transport->NativeHandle(), IPPROTO_TCP, TCP_NODELAY,
                           &nodelay, &size),
              0);
    EXPECT_EQ(nodelay, 1);
    EXPECT_NE(::fcntl(transport->NativeHandle(), F_GETFL) & O_NONBLOCK, 0);
  }
  CheckBytes(std::move(*pair));
}

TEST(Transport, UnixDomainSocketExchangesBothDirections) {
  auto pair = UnixPair();
  ASSERT_TRUE(pair);
  EXPECT_NE(::fcntl(pair->first->NativeHandle(), F_GETFL) & O_NONBLOCK, 0);
  CheckBytes(std::move(*pair));
}

TEST(Transport, LocalhostFallsBackToIpv4OnlyListener) {
  auto pair = TcpPair("localhost");
  ASSERT_TRUE(pair);
  CheckBytes(std::move(*pair), 1024);
}

TEST(Transport, TcpEofAndCloseAreObservableWithoutBlocking) {
  auto pair = TcpPair();
  ASSERT_TRUE(pair);
  pair->second->Close();
  std::array<std::byte, 8> output{};
  bool eof = false;
  for (int tick = 0; tick != 8192; ++tick) {
    auto read = pair->first->Read(output);
    if (!read) {
      EXPECT_EQ(read.error().Code(), grpcxx::StatusCode::kUnavailable);
      eof = true;
      break;
    }
  }
  EXPECT_TRUE(eof);
  pair->first->Close();
  EXPECT_EQ(pair->first->NativeHandle(), -1);
  EXPECT_FALSE(pair->first->Read(output));
}

TEST(Transport, InvalidConnectionsFailExplicitly) {
  EXPECT_FALSE(UnixTransport::Connect(std::string(200, 'x')));
  EXPECT_FALSE(UnixTransport::Connect(std::string(GREPTIMEDB_TEST_SCRATCH_DIR) +
                                      "/absent.sock"));
  EXPECT_FALSE(TcpTransport::Adopt(-1));
  EXPECT_FALSE(UnixTransport::Adopt(-1));
}

auto TransferTls(grpcxx::TlsSession &from, grpcxx::TlsSession &to) -> bool {
  std::array<std::byte, 17> fragment{};
  while (from.PendingCiphertext() != 0) {
    auto bytes = from.DrainCiphertext(fragment);
    if (!bytes || *bytes == 0)
      return false;
    auto accepted = to.FeedCiphertext(std::span{fragment}.first(*bytes));
    if (!accepted || *accepted != *bytes)
      return false;
  }
  return true;
}

auto Handshake(grpcxx::TlsSession &client, grpcxx::TlsSession &server) -> bool {
  for (int tick = 0; tick != 2048; ++tick) {
    if (!client.PollOnce() || !TransferTls(client, server))
      return false;
    if (!server.PollOnce() || !TransferTls(server, client))
      return false;
    if (client.State() == grpcxx::TlsHandshakeState::kEstablished &&
        server.State() == grpcxx::TlsHandshakeState::kEstablished)
      return true;
  }
  return false;
}

void CheckTlsOptions(TlsOptions options, bool mutual,
                     std::string_view endpoint = "127.0.0.1") {
  auto client_context = options.CreateContext();
  auto server_context = grpcxx::TlsContext::Create(ServerTlsOptions(mutual));
  ASSERT_TRUE(client_context);
  ASSERT_TRUE(server_context);
  auto client = options.CreateSession(*client_context, endpoint);
  auto server = grpcxx::TlsSession::Create(*server_context);
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  EXPECT_FALSE(TlsOptions::VerifySession(*client));
  ASSERT_TRUE(Handshake(*client, *server));
  EXPECT_EQ(client->ProtocolVersion(), "TLSv1.3");
  EXPECT_EQ(server->ProtocolVersion(), "TLSv1.3");
  EXPECT_TRUE(TlsOptions::VerifySession(*client));
  EXPECT_TRUE(TlsOptions::VerifySession(*server));
  const std::string text = "authenticated TLS payload";
  auto sent = client->WritePlaintext(Bytes(text));
  ASSERT_TRUE(sent);
  ASSERT_EQ(*sent, text.size());
  EXPECT_GT(client->PendingCiphertext(), text.size());
  ASSERT_TRUE(TransferTls(*client, *server));
  std::array<std::byte, 128> out{};
  auto read = server->ReadPlaintext(out);
  ASSERT_TRUE(read);
  EXPECT_TRUE(std::ranges::equal(std::span{out}.first(*read), Bytes(text)));
}

TEST(TlsOptions, CustomCaFileAndSniOverrideAuthenticateTls13H2) {
  CheckTlsOptions(ClientTlsOptions(), false);
}
TEST(TlsOptions, CustomCaPemAuthenticatesEndpointHostname) {
  auto options = ClientTlsOptions();
  options.ca_file.clear();
  options.ca_pem = ReadFixture("ca.pem");
  options.server_name_override.clear();
  CheckTlsOptions(std::move(options), false, "localhost");
}
TEST(TlsOptions, MutualTlsAcceptsSeparateTrustedClientIdentity) {
  CheckTlsOptions(ClientTlsOptions(true), true);
}
TEST(TlsOptions, SystemRootsRejectPrivateCaAndHostnameMismatchFails) {
  for (const bool untrusted : {false, true}) {
    auto options = ClientTlsOptions();
    if (untrusted)
      options.ca_file.clear();
    else
      options.server_name_override = "wrong.example";
    auto context = options.CreateContext();
    auto server_context = grpcxx::TlsContext::Create(ServerTlsOptions());
    ASSERT_TRUE(context);
    ASSERT_TRUE(server_context);
    auto client = options.CreateSession(*context, "127.0.0.1");
    auto server = grpcxx::TlsSession::Create(*server_context);
    ASSERT_TRUE(client);
    ASSERT_TRUE(server);
    EXPECT_FALSE(Handshake(*client, *server));
    EXPECT_FALSE(TlsOptions::VerifySession(*client));
  }
}
TEST(TlsOptions, MutualTlsRejectsMissingClientCertificate) {
  auto options = ClientTlsOptions();
  auto context = options.CreateContext();
  auto server_context = grpcxx::TlsContext::Create(ServerTlsOptions(true));
  ASSERT_TRUE(context);
  ASSERT_TRUE(server_context);
  auto client = options.CreateSession(*context, "127.0.0.1");
  auto server = grpcxx::TlsSession::Create(*server_context);
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  EXPECT_FALSE(Handshake(*client, *server));
  EXPECT_NE(server->State(), grpcxx::TlsHandshakeState::kEstablished);
}
TEST(TlsOptions, RejectsMalformedCredentialsAndServerNames) {
  auto options = ClientTlsOptions();
  options.ca_pem = "not a CA";
  EXPECT_FALSE(options.CreateContext());
  options = ClientTlsOptions();
  options.certificate_pem = ReadFixture("client-cert.pem");
  EXPECT_FALSE(options.CreateContext());
  options = ClientTlsOptions();
  options.private_key_pem = ReadFixture("client-key.pem");
  EXPECT_FALSE(options.CreateContext());
  options = ClientTlsOptions(true);
  options.private_key_pem = ReadFixture("server-key.pem");
  EXPECT_FALSE(options.CreateContext());
  options = ClientTlsOptions();
  auto context = options.CreateContext();
  ASSERT_TRUE(context);
  options.server_name_override.clear();
  for (std::string_view name :
       {std::string_view{}, std::string_view{"localhost:443"},
        std::string_view{"local host"}, std::string_view{"local\0host", 10}}) {
    EXPECT_FALSE(options.CreateSession(*context, name));
  }
}

void CheckRejectedNativeServer(int version, bool wrong_alpn) {
  auto options = ClientTlsOptions();
  auto context = options.CreateContext();
  ASSERT_TRUE(context);
  auto client = options.CreateSession(*context, "127.0.0.1");
  ASSERT_TRUE(client);
  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> raw_context{
      SSL_CTX_new(TLS_server_method()), SSL_CTX_free};
  ASSERT_TRUE(raw_context);
  ASSERT_EQ(SSL_CTX_set_min_proto_version(raw_context.get(), version), 1);
  ASSERT_EQ(SSL_CTX_set_max_proto_version(raw_context.get(), version), 1);
  ASSERT_EQ(SSL_CTX_use_certificate_chain_file(
                raw_context.get(), FixturePath("server-cert.pem").c_str()),
            1);
  ASSERT_EQ(SSL_CTX_use_PrivateKey_file(raw_context.get(),
                                        FixturePath("server-key.pem").c_str(),
                                        SSL_FILETYPE_PEM),
            1);
  if (wrong_alpn || version == TLS1_2_VERSION) {
    SSL_CTX_set_alpn_select_cb(
        raw_context.get(),
        [](SSL *, const unsigned char **selected, unsigned char *length,
           const unsigned char *, unsigned int, void *invalid) -> int {
          static constexpr unsigned char wrong[]{'h', 't', 't', 'p',
                                                 '/', '1', '.', '1'};
          static constexpr unsigned char h2[]{'h', '2'};
          *selected = *static_cast<bool *>(invalid) ? wrong : h2;
          *length = *static_cast<bool *>(invalid) ? sizeof(wrong) : sizeof(h2);
          return SSL_TLSEXT_ERR_OK;
        },
        &wrong_alpn);
  }
  std::unique_ptr<SSL, decltype(&SSL_free)> server{SSL_new(raw_context.get()),
                                                   SSL_free};
  ASSERT_TRUE(server);
  BIO *incoming = BIO_new(BIO_s_mem());
  BIO *outgoing = BIO_new(BIO_s_mem());
  ASSERT_NE(incoming, nullptr);
  ASSERT_NE(outgoing, nullptr);
  SSL_set_bio(server.get(), incoming, outgoing);
  SSL_set_accept_state(server.get());
  bool rejected = false;
  std::array<std::byte, 37> bytes{};
  for (int tick = 0; tick != 2048; ++tick) {
    if (!client->PollOnce()) {
      rejected = true;
      break;
    }
    while (client->PendingCiphertext() != 0) {
      auto drained = client->DrainCiphertext(bytes);
      ASSERT_TRUE(drained);
      ASSERT_GT(*drained, 0U);
      ASSERT_EQ(BIO_write(incoming, bytes.data(), static_cast<int>(*drained)),
                static_cast<int>(*drained));
    }
    static_cast<void>(SSL_do_handshake(server.get()));
    while (BIO_ctrl_pending(outgoing) != 0) {
      const int count = BIO_read(outgoing, bytes.data(), bytes.size());
      ASSERT_GT(count, 0);
      auto fed = client->FeedCiphertext(
          std::span{bytes}.first(static_cast<std::size_t>(count)));
      ASSERT_TRUE(fed);
      ASSERT_EQ(*fed, static_cast<std::size_t>(count));
    }
  }
  EXPECT_TRUE(rejected);
  EXPECT_EQ(client->State(), grpcxx::TlsHandshakeState::kFailed);
  EXPECT_FALSE(TlsOptions::VerifySession(*client));
}

TEST(TlsOptions, RejectsTls12OnlyServer) {
  CheckRejectedNativeServer(TLS1_2_VERSION, false);
}
TEST(TlsOptions, RejectsWrongOrMissingAlpn) {
  CheckRejectedNativeServer(TLS1_3_VERSION, false);
  CheckRejectedNativeServer(TLS1_3_VERSION, true);
}

TEST(TlsOptions, OverrideIsVisibleAsDnsSniInClientHello) {
  auto options = ClientTlsOptions();
  auto context = options.CreateContext();
  ASSERT_TRUE(context);
  auto client = options.CreateSession(*context, "127.0.0.1");
  ASSERT_TRUE(client);
  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> raw_context{
      SSL_CTX_new(TLS_server_method()), SSL_CTX_free};
  ASSERT_TRUE(raw_context);
  ASSERT_EQ(SSL_CTX_use_certificate_chain_file(
                raw_context.get(), FixturePath("server-cert.pem").c_str()),
            1);
  ASSERT_EQ(SSL_CTX_use_PrivateKey_file(raw_context.get(),
                                        FixturePath("server-key.pem").c_str(),
                                        SSL_FILETYPE_PEM),
            1);
  std::unique_ptr<SSL, decltype(&SSL_free)> server{SSL_new(raw_context.get()),
                                                   SSL_free};
  ASSERT_TRUE(server);
  BIO *incoming = BIO_new(BIO_s_mem());
  BIO *outgoing = BIO_new(BIO_s_mem());
  ASSERT_NE(incoming, nullptr);
  ASSERT_NE(outgoing, nullptr);
  SSL_set_bio(server.get(), incoming, outgoing);
  SSL_set_accept_state(server.get());
  ASSERT_TRUE(client->PollOnce());
  std::array<std::byte, 37> bytes{};
  while (client->PendingCiphertext() != 0) {
    auto drained = client->DrainCiphertext(bytes);
    ASSERT_TRUE(drained);
    ASSERT_GT(*drained, 0U);
    ASSERT_EQ(BIO_write(incoming, bytes.data(), static_cast<int>(*drained)),
              static_cast<int>(*drained));
  }
  static_cast<void>(SSL_do_handshake(server.get()));
  const char *name =
      SSL_get_servername(server.get(), TLSEXT_NAMETYPE_host_name);
  ASSERT_NE(name, nullptr);
  EXPECT_STREQ(name, "localhost");
}

TEST(TlsTransport, MemoryBioOverTcpAndMemoryPipePreservesLargePayloads) {
  for (const bool tcp : {false, true}) {
    auto underlying = tcp ? TcpPair() : MemoryPipe::CreatePair(47);
    ASSERT_TRUE(underlying);
    auto pair = TlsPair(std::move(*underlying), true);
    ASSERT_TRUE(pair);
    CheckBytes(std::move(*pair), 80'000);
  }
}

TEST(TlsTransport, AbruptEofDrainsAcceptedPlaintextThenFails) {
  auto underlying = TcpPair();
  ASSERT_TRUE(underlying);
  auto pair = TlsPair(std::move(*underlying));
  ASSERT_TRUE(pair);
  for (int tick = 0; tick != 2048; ++tick) {
    ASSERT_TRUE(pair->first->PollOnce());
    ASSERT_TRUE(pair->second->PollOnce());
  }
  const std::string final_message(4096, 'f');
  auto sent = pair->second->Write(Bytes(final_message));
  ASSERT_TRUE(sent);
  ASSERT_EQ(*sent, final_message.size());
  for (int tick = 0; tick != 32; ++tick)
    ASSERT_TRUE(pair->second->PollOnce());
  ASSERT_EQ(::shutdown(pair->second->NativeHandle(), SHUT_WR), 0);
  std::array<std::byte, 31> output{};
  std::vector<std::byte> received;
  bool eof = false;
  for (int tick = 0; tick != 8192; ++tick) {
    auto read = pair->first->Read(output);
    if (!read) {
      eof = true;
      break;
    }
    received.insert(received.end(), output.begin(), output.begin() + *read);
  }
  EXPECT_TRUE(std::ranges::equal(received, Bytes(final_message)));
  EXPECT_TRUE(eof);
  EXPECT_FALSE(pair->first->Write(Bytes("after abrupt EOF")));
}

} // namespace
} // namespace greptimedb::testing
