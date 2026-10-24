#pragma once

#include <expected>
#include <memory>

#include "greptimedb/transport/mock_grpc_server.h"
#include "greptimedb/transport/synchronous_channel.h"

namespace greptimedb {

/** Owns both HTTP/2 peers; synchronous client waits drive the server inline.
 * This object and its channel must outlive every RPC stream created from it.
 */
class InMemoryChannelPair final {
public:
  [[nodiscard]] static auto
  Create(MockGrpcServer::Handler handler, void *context = nullptr,
         MockServerOptions options = {}, std::size_t capacity = 262144) noexcept
      -> std::expected<std::unique_ptr<InMemoryChannelPair>, grpcxx::Status>;
  ~InMemoryChannelPair() = default;
  InMemoryChannelPair(const InMemoryChannelPair &) = delete;
  auto operator=(const InMemoryChannelPair &) -> InMemoryChannelPair & = delete;
  [[nodiscard]] auto channel() noexcept -> SynchronousChannel & {
    return *channel_;
  }
  [[nodiscard]] auto server() noexcept -> MockGrpcServer & { return *server_; }

private:
  InMemoryChannelPair(std::unique_ptr<MockGrpcServer> server,
                      std::unique_ptr<SynchronousChannel> channel) noexcept;
  std::unique_ptr<MockGrpcServer> server_;
  std::unique_ptr<SynchronousChannel> channel_;
};

} // namespace greptimedb
