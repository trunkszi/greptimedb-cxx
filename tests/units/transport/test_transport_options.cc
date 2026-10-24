#include "transport_test_support.h"
#include <algorithm>
#include <greptimedb/core/config.h>
#include <greptimedb/transport/in_memory_channel_pair.h>
#include <gtest/gtest.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <ranges>
#include <sys/resource.h>

namespace greptimedb::testing {
namespace {
using namespace std::chrono_literals;
struct Listener {
  FileDescriptor socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
  sockaddr_in address{};
  bool Open(int backlog = 4) {
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (socket.value < 0 ||
        ::bind(socket.value, reinterpret_cast<sockaddr *>(&address),
               sizeof(address)) ||
        ::listen(socket.value, backlog))
      return false;
    socklen_t length = sizeof(address);
    return ::getsockname(socket.value, reinterpret_cast<sockaddr *>(&address),
                         &length) == 0;
  }
  auto Port() const -> std::uint16_t { return ntohs(address.sin_port); }
};
TEST(TransportOptionsAudit, ChannelOptionsReachTcpSocketAndValidateBeforeDial) {
  Listener listener;
  ASSERT_TRUE(listener.Open());
  auto endpoint =
      Endpoint::Parse("grpc://127.0.0.1:" + std::to_string(listener.Port()));
  ASSERT_TRUE(endpoint);
  for (bool enabled : {false, true}) {
    ClientOptions options{*endpoint};
    options.tcp_nodelay = enabled;
    auto channel = SynchronousChannel::Connect(*endpoint, options);
    ASSERT_TRUE(channel) << channel.error().Message();
    int value = -1;
    socklen_t length = sizeof(value);
    ASSERT_EQ(::getsockopt((*channel)->NativeHandle(), IPPROTO_TCP, TCP_NODELAY,
                           &value, &length),
              0);
    EXPECT_EQ(value, enabled ? 1 : 0);
  }
  TransportOptions invalid;
  invalid.connect_timeout = 0ns;
  auto transport =
      EndpointTransport::ConnectTcp("127.0.0.1", listener.Port(), invalid);
  ASSERT_FALSE(transport);
  EXPECT_EQ(transport.error().Code(), grpcxx::StatusCode::kInvalidArgument);
}
TEST(TransportOptionsAudit, BackloggedSynUsesBoundedPollInsteadOfBusySpinning) {
  Listener listener;
  ASSERT_TRUE(listener.Open(0));
  FileDescriptor filler{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
  ASSERT_GE(filler.value, 0);
  ASSERT_EQ(::connect(filler.value,
                      reinterpret_cast<sockaddr *>(&listener.address),
                      sizeof(listener.address)),
            0);
  rusage cpu_before{}, cpu_after{};
  ASSERT_EQ(::getrusage(RUSAGE_THREAD, &cpu_before), 0);
  const auto start = std::chrono::steady_clock::now();
  auto endpoint =
      Endpoint::Parse("127.0.0.1:" + std::to_string(listener.Port()));
  ASSERT_TRUE(endpoint);
  ClientOptions options{*endpoint};
  options.connect_timeout = 30ms;
  auto result = SynchronousChannel::Connect(*endpoint, options);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  ASSERT_EQ(::getrusage(RUSAGE_THREAD, &cpu_after), 0);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().Code(), grpcxx::StatusCode::kDeadlineExceeded);
  EXPECT_GE(elapsed, options.connect_timeout);
  EXPECT_LT(elapsed, 2s);
  const auto micros = [](const rusage &cpu) {
    return cpu.ru_utime.tv_sec * 1000000 + cpu.ru_utime.tv_usec +
           cpu.ru_stime.tv_sec * 1000000 + cpu.ru_stime.tv_usec;
  };
  EXPECT_LT(micros(cpu_after) - micros(cpu_before), 15000);
}
TEST(TransportOptionsAudit, ReadAndWriteStallsExpireButProgressResetsBudget) {
  for (bool read : {false, true}) {
    auto pair = MemoryPipe::CreatePair(1);
    ASSERT_TRUE(pair);
    TransportOptions options;
    if (read)
      options.read_timeout = 10ms;
    else
      options.write_timeout = 10ms;
    auto transport =
        EndpointTransport::Configure(std::move(pair->first), options);
    ASSERT_TRUE(transport);
    std::array<std::byte, 1> byte{};
    if (read) {
      auto pending = (*transport)->Read(byte);
      ASSERT_TRUE(pending);
      EXPECT_EQ(*pending, 0U);
    } else {
      ASSERT_EQ(*(*transport)->Write(byte), 1U);
      ASSERT_EQ(*(*transport)->Write(byte), 0U);
    }
    ::poll(nullptr, 0, 15);
    auto timed = (*transport)->PollOnce();
    ASSERT_FALSE(timed);
    EXPECT_EQ(timed.error().Code(), grpcxx::StatusCode::kDeadlineExceeded);
  }
  auto pair = MemoryPipe::CreatePair(1);
  ASSERT_TRUE(pair);
  TransportOptions options;
  options.write_timeout = 50ms;
  auto transport =
      EndpointTransport::Configure(std::move(pair->first), options);
  ASSERT_TRUE(transport);
  std::array<std::byte, 1> byte{};
  ASSERT_EQ(*(*transport)->Write(byte), 1U);
  ASSERT_EQ(*(*transport)->Write(byte), 0U);
  ASSERT_EQ(*pair->second->Read(byte), 1U);
  ASSERT_EQ(*(*transport)->Write(byte), 1U);
  ::poll(nullptr, 0, 60);
  EXPECT_TRUE((*transport)->PollOnce());
  EXPECT_EQ(*(*transport)->Read({}), 0U);
  EXPECT_EQ(*(*transport)->Write({}), 0U);
}
TEST(TransportOptionsAudit, TlsHandshakeSharesEstablishmentDeadline) {
  auto pipes = MemoryPipe::CreatePair();
  ASSERT_TRUE(pipes);
  const auto options = ClientTlsOptions();
  auto context = options.CreateContext();
  ASSERT_TRUE(context);
  auto session = options.CreateSession(*context, "localhost");
  ASSERT_TRUE(session);
  auto tls = TlsTransport::Create(std::move(pipes->first), std::move(*session));
  ASSERT_TRUE(tls);
  TransportOptions timeouts;
  timeouts.connect_timeout = 10ms;
  auto transport = EndpointTransport::Configure(std::move(*tls), timeouts);
  ASSERT_TRUE(transport);
  ASSERT_TRUE((*transport)->PollOnce());
  ::poll(nullptr, 0, 15);
  auto timed = (*transport)->PollOnce();
  ASSERT_FALSE(timed);
  EXPECT_EQ(timed.error().Code(), grpcxx::StatusCode::kDeadlineExceeded);
}
TEST(TransportOptionsAudit,
     TcpTlsFactoryBoundsSilentPeerAndPreservesSocketOptions) {
  Listener listener;
  ASSERT_TRUE(listener.Open());
  TransportOptions timeouts;
  timeouts.connect_timeout = 20ms;
  timeouts.read_timeout = 1ms;
  timeouts.write_timeout = 1ms;
  timeouts.tcp_nodelay = false;
  auto transport = EndpointTransport::ConnectTls("127.0.0.1", listener.Port(),
                                                 ClientTlsOptions(), timeouts);
  ASSERT_TRUE(transport) << transport.error().Message();
  EXPECT_TRUE((*transport)->IsTls());
  EXPECT_FALSE((*transport)->IsReady());
  int nodelay = -1;
  socklen_t size = sizeof(nodelay);
  ASSERT_EQ(::getsockopt((*transport)->NativeHandle(), IPPROTO_TCP, TCP_NODELAY,
                         &nodelay, &size),
            0);
  EXPECT_EQ(nodelay, 0);
  ASSERT_TRUE((*transport)->PollOnce());
  ::poll(nullptr, 0, 25);
  auto timed = (*transport)->PollOnce();
  ASSERT_FALSE(timed);
  EXPECT_EQ(timed.error().Code(), grpcxx::StatusCode::kDeadlineExceeded);
}
TEST(TlsAudit, AcceptedPlaintextIsOwnedWhileCiphertextIsBackpressured) {
  auto pipes = MemoryPipe::CreatePair(97);
  ASSERT_TRUE(pipes);
  auto pair = TlsPair(std::move(*pipes));
  ASSERT_TRUE(pair);
  for (int tick = 0;
       tick < 10000 && (!pair->first->IsReady() || !pair->second->IsReady());
       ++tick) {
    ASSERT_TRUE(pair->first->PollOnce());
    ASSERT_TRUE(pair->second->PollOnce());
  }
  ASSERT_TRUE(pair->first->IsReady());
  ASSERT_TRUE(pair->second->IsReady());
  EXPECT_EQ(*pair->first->Write({}), 0U);
  EXPECT_EQ(*pair->second->Read({}), 0U);
  std::array<std::byte, 16384> source{};
  std::vector<std::byte> expected, received;
  bool blocked = false;
  for (int chunk = 0; chunk < 256; ++chunk) {
    std::ranges::fill(source, static_cast<std::byte>(chunk % 251));
    auto accepted = pair->first->Write(source);
    ASSERT_TRUE(accepted) << accepted.error().Message();
    if (!*accepted) {
      blocked = true;
      break;
    }
    expected.append_range(std::span{source}.first(*accepted));
    std::ranges::fill(source, std::byte{0xff});
  }
  ASSERT_TRUE(blocked);
  ASSERT_GT(expected.size(), source.size());
  std::array<std::byte, 4096> output{};
  for (int tick = 0; tick < 200000 && received.size() < expected.size();
       ++tick) {
    ASSERT_TRUE(pair->first->PollOnce());
    auto read = pair->second->Read(output);
    ASSERT_TRUE(read) << read.error().Message();
    received.append_range(std::span{output}.first(*read));
  }
  EXPECT_EQ(received, expected);
}
TEST(TlsAudit, WireWriteFailureIsStickyAndPreservesBufferedInboundPlaintext) {
  struct FailingWire final : EndpointTransport {
    std::unique_ptr<EndpointTransport> wire;
    bool fail{};
    unsigned failed_writes{};
    explicit FailingWire(std::unique_ptr<EndpointTransport> value)
        : wire(std::move(value)) {}
    auto Read(std::span<std::byte> bytes) noexcept
        -> std::expected<std::size_t, grpcxx::Status> override {
      return wire->Read(bytes);
    }
    auto Write(std::span<const std::byte> bytes) noexcept
        -> std::expected<std::size_t, grpcxx::Status> override {
      if (fail && !bytes.empty()) {
        ++failed_writes;
        return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kUnavailable,
                                              "injected wire write failure"});
      }
      return wire->Write(bytes);
    }
    auto PollOnce() noexcept -> std::expected<void, grpcxx::Status> override {
      return wire->PollOnce();
    }
    auto IsOpen() const noexcept -> bool override { return wire->IsOpen(); }
    void Close() noexcept override { wire->Close(); }
  };
  auto pipes = MemoryPipe::CreatePair(4096);
  ASSERT_TRUE(pipes);
  auto controlled = std::make_unique<FailingWire>(std::move(pipes->first));
  auto *wire = controlled.get();
  pipes->first = std::move(controlled);
  auto pair = TlsPair(std::move(*pipes));
  ASSERT_TRUE(pair);
  for (int tick = 0; tick < 1024; ++tick) {
    ASSERT_TRUE(pair->first->PollOnce());
    ASSERT_TRUE(pair->second->PollOnce());
  }
  ASSERT_TRUE(pair->first->IsReady());
  auto sent = pair->second->Write(Bytes("buffered reply"));
  ASSERT_TRUE(sent);
  ASSERT_EQ(*sent, Bytes("buffered reply").size());
  for (int tick = 0; tick < 16; ++tick) {
    ASSERT_TRUE(pair->second->PollOnce());
    ASSERT_TRUE(pair->first->PollOnce());
  }
  auto queued = pair->first->Write(Bytes("outbound request"));
  ASSERT_TRUE(queued);
  ASSERT_EQ(*queued, Bytes("outbound request").size());
  wire->fail = true;
  EXPECT_TRUE(pair->first->PollOnce());
  ASSERT_EQ(wire->failed_writes, 1U);
  auto failed = pair->first->Write(Bytes("must fail immediately"));
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().Message(), "injected wire write failure");
  std::array<std::byte, 64> output{};
  auto received = pair->first->Read(output);
  ASSERT_TRUE(received);
  EXPECT_TRUE(std::ranges::equal(std::span{output}.first(*received),
                                 Bytes("buffered reply")));
  auto drained = pair->first->Read(output);
  ASSERT_FALSE(drained);
  EXPECT_EQ(drained.error().Message(), "injected wire write failure");
  EXPECT_EQ(wire->failed_writes, 1U);
}
TEST(FlowControlAudit,
     MultiMegabyteDoGetEmitsConnectionAndStreamWindowUpdates) {
  struct Peer {
    std::vector<std::byte> body =
        std::vector<std::byte>(2 * 1024 * 1024, std::byte{0x5a});
    bool ok{true};
    static void Handle(void *context, MockGrpcServer &server,
                       const MockRequest &request) noexcept {
      if (request.event != MockRequest::Event::kHalfClose)
        return;
      auto &peer = *static_cast<Peer *>(context);
      for (int batch = 0; batch < 3; ++batch)
        peer.ok &= server.Send(request.stream_id, peer.body).has_value();
      peer.ok &= server.Finish(request.stream_id).has_value();
    }
  } peer;
  auto pair = InMemoryChannelPair::Create(Peer::Handle, &peer, {}, 4093);
  ASSERT_TRUE(pair);
  CallOptions options;
  options.timeout = 2s;
  auto query = (*pair)->channel().ServerStreamCall(
      "/arrow.flight.protocol.FlightService/DoGet", {}, options);
  ASSERT_TRUE(query);
  for (int batch = 0; batch < 3; ++batch) {
    auto received = (*query)->RecvNext();
    ASSERT_TRUE(received) << received.error().Message();
    ASSERT_TRUE(*received);
    EXPECT_TRUE(std::ranges::equal(**received, peer.body));
  }
  auto end = (*query)->RecvNext();
  ASSERT_TRUE(end);
  EXPECT_FALSE(*end);
  ASSERT_TRUE((*query)->Finish());
  EXPECT_GE((*pair)->server().WindowUpdatesReceived(), 2U);
  EXPECT_TRUE(peer.ok);
}
} // namespace
} // namespace greptimedb::testing
