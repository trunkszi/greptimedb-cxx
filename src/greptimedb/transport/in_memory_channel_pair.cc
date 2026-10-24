#include "greptimedb/transport/in_memory_channel_pair.h"

#include <new>
#include <utility>

namespace greptimedb {
InMemoryChannelPair::InMemoryChannelPair(
    std::unique_ptr<MockGrpcServer> server,
    std::unique_ptr<SynchronousChannel> channel) noexcept
    : server_(std::move(server)), channel_(std::move(channel)) {
  channel_->SetProgressHook(server_.get(), [](void *context) noexcept {
    return static_cast<MockGrpcServer *>(context)->PollOnce();
  });
}
auto InMemoryChannelPair::Create(MockGrpcServer::Handler handler, void *context,
                                 MockServerOptions options,
                                 std::size_t capacity) noexcept
    -> std::expected<std::unique_ptr<InMemoryChannelPair>, grpcxx::Status> {
  auto pipes = MemoryPipe::CreatePair(capacity);
  if (!pipes)
    return std::unexpected(pipes.error());
  auto server = MockGrpcServer::Create(std::move(pipes->second), handler,
                                       context, options);
  if (!server)
    return std::unexpected(server.error());
  auto channel = SynchronousChannel::Create(std::move(pipes->first));
  if (!channel)
    return std::unexpected(channel.error());
  auto pair = std::unique_ptr<InMemoryChannelPair>{
      new (std::nothrow)
          InMemoryChannelPair{std::move(*server), std::move(*channel)}};
  if (!pair)
    return std::unexpected(
        grpcxx::Status{grpcxx::StatusCode::kResourceExhausted});
  return pair;
}
} // namespace greptimedb
