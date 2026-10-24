#include "transport_test_support.h"

#include <greptimedb/transport/mock_grpc_server.h>
#include <greptimedb/transport/synchronous_channel.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <thread>

namespace greptimedb::testing {
namespace {
/** Accept lazily from the channel's caller-thread progress hook. */
struct ListeningPeer {
  FileDescriptor listener;
  std::unique_ptr<MockGrpcServer> server;
  std::string socket_path;
  unsigned port{};
  bool unix_socket{}, tls{}, handler_ok{true};
  unsigned messages{}, half_closes{};
  std::thread::id owner{std::this_thread::get_id()};

  ~ListeningPeer() {
    if (!socket_path.empty())
      ::unlink(socket_path.c_str());
  }

  auto Bind(bool use_unix) -> std::expected<void, grpcxx::Status> {
    unix_socket = use_unix;
    if (unix_socket) {
      std::error_code error;
      std::filesystem::create_directories(GREPTIMEDB_TEST_SCRATCH_DIR, error);
      if (error)
        return Failure("facade Unix scratch directory failed");
      socket_path = std::string(GREPTIMEDB_TEST_SCRATCH_DIR) + "/facade-" +
                    std::to_string(::getpid()) + ".sock";
      sockaddr_un address{};
      address.sun_family = AF_UNIX;
      if (socket_path.size() >= sizeof(address.sun_path))
        return Failure("facade Unix path too long");
      std::memcpy(address.sun_path, socket_path.c_str(),
                  socket_path.size() + 1);
      listener.value =
          ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
      if (listener.value < 0 ||
          ::bind(listener.value, reinterpret_cast<sockaddr *>(&address),
                 sizeof(address)) != 0)
        return Failure("facade Unix bind failed");
    } else {
      listener.value =
          ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
      sockaddr_in address{};
      address.sin_family = AF_INET;
      address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      if (listener.value < 0 ||
          ::bind(listener.value, reinterpret_cast<sockaddr *>(&address),
                 sizeof(address)) != 0)
        return Failure("facade TCP bind failed");
      socklen_t length = sizeof(address);
      if (::getsockname(listener.value, reinterpret_cast<sockaddr *>(&address),
                        &length) != 0)
        return Failure("facade TCP getsockname failed");
      port = ntohs(address.sin_port);
    }
    if (::listen(listener.value, 4) != 0)
      return Failure("facade listen failed");
    return {};
  }
  auto Endpoint(std::string_view scheme) const -> std::string {
    return std::string(scheme) + "://" +
           (unix_socket ? socket_path : "127.0.0.1:" + std::to_string(port));
  }
  static void Handle(void *opaque, MockGrpcServer &peer,
                     const MockRequest &request) noexcept {
    auto &self = *static_cast<ListeningPeer *>(opaque);
    self.handler_ok &= std::this_thread::get_id() == self.owner;
    self.handler_ok &= request.path == "/test.Facade/Echo";
    switch (request.event) {
    case MockRequest::Event::kMessage: {
      ++self.messages;
      grpcxx::MetadataMap metadata;
      self.handler_ok &= metadata.Add("x-facade", "connected").has_value();
      self.handler_ok &=
          peer.SetInitialMetadata(request.stream_id, metadata).has_value();
      self.handler_ok &=
          peer.Send(request.stream_id, request.payload).has_value();
      break;
    }
    case MockRequest::Event::kHalfClose:
      ++self.half_closes;
      self.handler_ok &= peer.Finish(request.stream_id).has_value();
      break;
    case MockRequest::Event::kCancel:
      self.handler_ok = false;
      break;
    }
  }
  auto Poll() noexcept -> std::expected<void, grpcxx::Status> {
    if (!server) {
      const int accepted = ::accept4(listener.value, nullptr, nullptr,
                                     SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (accepted < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
          return {};
        return Failure("facade accept failed");
      }
      auto wire = unix_socket ? UnixTransport::Adopt(accepted)
                              : TcpTransport::Adopt(accepted);
      if (!wire)
        return std::unexpected(wire.error());
      if (tls) {
        auto context = grpcxx::TlsContext::Create(ServerTlsOptions(true));
        if (!context)
          return std::unexpected(context.error());
        auto session = grpcxx::TlsSession::Create(*context);
        if (!session)
          return std::unexpected(session.error());
        wire = TlsTransport::Create(std::move(*wire), std::move(*session));
        if (!wire)
          return std::unexpected(wire.error());
      }
      auto created = MockGrpcServer::Create(std::move(*wire), Handle, this);
      if (!created)
        return std::unexpected(created.error());
      server = std::move(*created);
    }
    return server->PollOnce();
  }
};

TEST(ConnectFacade, UnaryOverGrpcHttpGrpcsHttpsAndUnixUris) {
  for (std::string_view scheme : {"grpc", "http", "grpcs", "https", "unix"}) {
    SCOPED_TRACE(scheme);
    ListeningPeer peer;
    peer.tls = scheme == "grpcs" || scheme == "https";
    ASSERT_TRUE(peer.Bind(scheme == "unix"));
    auto channel = SynchronousChannel::Connect(peer.Endpoint(scheme),
                                               ClientTlsOptions(true));
    ASSERT_TRUE(channel);
    (*channel)->SetProgressHook(&peer, [](void *context) noexcept {
      return static_cast<ListeningPeer *>(context)->Poll();
    });
    CallOptions options;
    options.timeout = std::chrono::seconds{3};
    const std::string payload = "caller-driven endpoint facade";
    auto response =
        (*channel)->Unary("/test.Facade/Echo", Bytes(payload), options);
    ASSERT_TRUE(response);
    EXPECT_TRUE(response->status.Ok());
    EXPECT_TRUE(std::ranges::equal(response->payload, Bytes(payload)));
    EXPECT_EQ(response->initial_metadata.Get("x-facade"), "connected");
    EXPECT_TRUE(peer.handler_ok);
    EXPECT_EQ(peer.messages, 1U);
    EXPECT_EQ(peer.half_closes, 1U);
  }
}

TEST(ConnectFacade, InvalidTlsIdentityFailsBeforeDialing) {
  ListeningPeer peer;
  ASSERT_TRUE(peer.Bind(false));
  auto options = ClientTlsOptions();
  options.certificate_pem = ReadFixture("client-cert.pem");
  auto channel = SynchronousChannel::Connect(peer.Endpoint("grpcs"), options);
  ASSERT_FALSE(channel);
  EXPECT_EQ(channel.error().Code(), grpcxx::StatusCode::kInvalidArgument);
  FileDescriptor accepted{::accept4(peer.listener.value, nullptr, nullptr,
                                    SOCK_NONBLOCK | SOCK_CLOEXEC)};
  EXPECT_LT(accepted.value, 0);
  if (accepted.value < 0) {
    EXPECT_EQ(errno, EAGAIN);
  }
}
} // namespace
} // namespace greptimedb::testing
