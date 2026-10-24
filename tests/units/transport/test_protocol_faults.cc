#include "transport_test_support.h"

#include <greptimedb/transport/synchronous_channel.h>
#include <gtest/gtest.h>
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <ranges>

namespace greptimedb::testing {
namespace {
/** Deliberately malformed peer independent of the SDK's framing encoder. */
class FaultPeer final {
public:
  enum class Fault {
    kPartialHeader,
    kCompressedIdentity,
    kOversized,
    kNoStatus,
    kReset,
    kGracefulGoaway,
    kUnacceptedGoaway,
    kValid
  };
  explicit FaultPeer(std::unique_ptr<EndpointTransport> transport, Fault fault)
      : transport_(std::move(transport)), fault_(fault) {}
  ~FaultPeer() {
    if (native_)
      nghttp2_session_del(native_);
  }
  auto Requests() const -> std::size_t { return requests_; }
  auto Initialize() -> bool {
    nghttp2_session_callbacks *callbacks{};
    if (nghttp2_session_callbacks_new(&callbacks))
      return false;
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, Received);
    const auto result = nghttp2_session_server_new(&native_, callbacks, this);
    nghttp2_session_callbacks_del(callbacks);
    if (result)
      return false;
    return nghttp2_submit_settings(native_, NGHTTP2_FLAG_NONE, nullptr, 0) == 0;
  }
  auto Poll() noexcept -> std::expected<void, grpcxx::Status> {
    if (auto flushed = Flush(); !flushed)
      return flushed;
    std::array<std::byte, 16384> buffer{};
    for (unsigned tick = 0; tick < 32; ++tick) {
      auto count = transport_->Read(buffer);
      if (!count)
        return std::unexpected(count.error());
      if (!*count)
        break;
      if (nghttp2_session_mem_recv2(
              native_, reinterpret_cast<const std::uint8_t *>(buffer.data()),
              *count) < 0)
        return Failure("fault peer HTTP/2 receive failed");
    }
    return Flush();
  }

private:
  struct Reply {
    std::vector<std::byte> wire;
    std::size_t offset{};
    bool no_status{}, reset{};
  };
  static auto Header(std::string_view key, std::string_view value)
      -> nghttp2_nv {
    return {reinterpret_cast<std::uint8_t *>(const_cast<char *>(key.data())),
            reinterpret_cast<std::uint8_t *>(const_cast<char *>(value.data())),
            key.size(), value.size(), NGHTTP2_NV_FLAG_NONE};
  }
  static auto Read(nghttp2_session *native, std::int32_t id,
                   std::uint8_t *buffer, std::size_t capacity,
                   std::uint32_t *flags, nghttp2_data_source *source,
                   void *) noexcept -> nghttp2_ssize {
    auto &reply = *static_cast<Reply *>(source->ptr);
    const auto count = std::min(capacity, reply.wire.size() - reply.offset);
    if (count)
      std::memcpy(buffer, reply.wire.data() + reply.offset, count);
    reply.offset += count;
    if (reply.offset == reply.wire.size()) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      if (reply.reset) {
        *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
        if (nghttp2_submit_rst_stream(native, NGHTTP2_FLAG_NONE, id,
                                      NGHTTP2_CANCEL))
          return NGHTTP2_ERR_CALLBACK_FAILURE;
      } else if (!reply.no_status) {
        *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
        const auto trailer = Header("grpc-status", "0");
        if (nghttp2_submit_trailer(native, id, &trailer, 1))
          return NGHTTP2_ERR_CALLBACK_FAILURE;
      }
    }
    return static_cast<nghttp2_ssize>(count);
  }
  static auto Received(nghttp2_session *native, const nghttp2_frame *frame,
                       void *context) noexcept -> int {
    auto &self = *static_cast<FaultPeer *>(context);
    if ((frame->hd.type != NGHTTP2_HEADERS && frame->hd.type != NGHTTP2_DATA) ||
        !(frame->hd.flags & NGHTTP2_FLAG_END_STREAM))
      return 0;
    if (self.requests_ >= self.replies_.size())
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    auto &reply = self.replies_[self.requests_++];
    const auto fault = self.requests_ == 1 ? self.fault_ : Fault::kValid;
    switch (fault) {
    case Fault::kUnacceptedGoaway:
      return nghttp2_submit_goaway(native, NGHTTP2_FLAG_NONE, 0,
                                   NGHTTP2_NO_ERROR, nullptr, 0);
    case Fault::kGracefulGoaway:
      if (nghttp2_submit_goaway(native, NGHTTP2_FLAG_NONE, frame->hd.stream_id,
                                NGHTTP2_NO_ERROR, nullptr, 0))
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      [[fallthrough]];
    case Fault::kValid:
      reply.wire = {std::byte{0},   std::byte{0},   std::byte{0},
                    std::byte{0},   std::byte{5},   std::byte{'v'},
                    std::byte{'a'}, std::byte{'l'}, std::byte{'i'},
                    std::byte{'d'}};
      break;
    case Fault::kReset:
      reply.reset = true;
      [[fallthrough]];
    case Fault::kPartialHeader:
      reply.wire = {std::byte{0}, std::byte{0}, std::byte{0}};
      break;
    case Fault::kCompressedIdentity:
      reply.wire = {std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},
                    std::byte{0}};
      break;
    case Fault::kOversized:
      reply.wire = {std::byte{0}, std::byte{0x7f}, std::byte{0xff},
                    std::byte{0xff}, std::byte{0xff}};
      break;
    case Fault::kNoStatus:
      reply.no_status = true;
      reply.wire = {std::byte{0},   std::byte{0},   std::byte{0},
                    std::byte{0},   std::byte{5},   std::byte{'v'},
                    std::byte{'a'}, std::byte{'l'}, std::byte{'i'},
                    std::byte{'d'}};
      break;
    }
    const nghttp2_nv headers[] = {Header(":status", "200"),
                                  Header("content-type", "application/grpc"),
                                  Header("echo-bin", "AQ,Ag")};
    nghttp2_data_provider2 provider{};
    provider.source.ptr = &reply;
    provider.read_callback = Read;
    return nghttp2_submit_response2(native, frame->hd.stream_id, headers, 3,
                                    &provider);
  }
  auto Flush() noexcept -> std::expected<void, grpcxx::Status> {
    for (unsigned tick = 0; tick < 32; ++tick) {
      if (offset_ == pending_.size()) {
        const std::uint8_t *data{};
        const auto count = nghttp2_session_mem_send2(native_, &data);
        if (count < 0)
          return Failure("fault peer HTTP/2 send failed");
        if (!count)
          return {};
        pending_.assign_range(
            std::span{reinterpret_cast<const std::byte *>(data),
                      static_cast<std::size_t>(count)});
        offset_ = 0;
      }
      auto written = transport_->Write(std::span{pending_}.subspan(offset_));
      if (!written)
        return std::unexpected(written.error());
      offset_ += *written;
      if (!*written)
        return {};
    }
    return {};
  }
  std::unique_ptr<EndpointTransport> transport_;
  Fault fault_;
  nghttp2_session *native_{};
  std::array<Reply, 4> replies_;
  std::size_t requests_{}, offset_{};
  std::vector<std::byte> pending_;
};

TEST(ProtocolHardening,
     MalformedResponseResetsOnlyItsStreamAndPreservesConnection) {
  for (auto fault :
       {FaultPeer::Fault::kPartialHeader, FaultPeer::Fault::kCompressedIdentity,
        FaultPeer::Fault::kOversized, FaultPeer::Fault::kNoStatus,
        FaultPeer::Fault::kReset}) {
    SCOPED_TRACE(static_cast<int>(fault));
    auto pipes = MemoryPipe::CreatePair(19);
    ASSERT_TRUE(pipes);
    FaultPeer server{std::move(pipes->second), fault};
    ASSERT_TRUE(server.Initialize());
    auto channel = SynchronousChannel::Create(std::move(pipes->first));
    ASSERT_TRUE(channel);
    (*channel)->SetProgressHook(&server, [](void *context) noexcept {
      return static_cast<FaultPeer *>(context)->Poll();
    });
    auto invalid = (*channel)->CallUnary("/test.Echo/Broken", {});
    ASSERT_FALSE(invalid);
    EXPECT_NE(invalid.error().Code(), grpcxx::StatusCode::kDeadlineExceeded);
    auto valid = (*channel)->CallUnary("/test.Echo/Valid", {});
    ASSERT_TRUE(valid) << valid.error().Message();
    EXPECT_TRUE(valid->status.Ok());
    EXPECT_TRUE(std::ranges::equal(valid->payload, Bytes("valid")));
    ASSERT_EQ(valid->initial_metadata.Size(), 3U);
    const auto entries = valid->initial_metadata.Entries();
    EXPECT_EQ(std::ranges::count(entries, std::string{"echo-bin"},
                                 &grpcxx::MetadataMap::Entry::key),
              2);
    EXPECT_EQ(valid->initial_metadata.Get("echo-bin"), std::string(1, '\1'));
  }
}
TEST(ProtocolHardening,
     GoawayDrainsAcceptedCallsAndRejectsNewCallsWithoutReplay) {
  for (auto fault : {FaultPeer::Fault::kGracefulGoaway,
                     FaultPeer::Fault::kUnacceptedGoaway}) {
    auto pipes = MemoryPipe::CreatePair(19);
    ASSERT_TRUE(pipes);
    FaultPeer server{std::move(pipes->second), fault};
    ASSERT_TRUE(server.Initialize());
    auto channel = SynchronousChannel::Create(std::move(pipes->first));
    ASSERT_TRUE(channel);
    (*channel)->SetProgressHook(&server, [](void *context) noexcept {
      return static_cast<FaultPeer *>(context)->Poll();
    });
    auto result = (*channel)->CallUnary("/test.Echo/Goaway", {});
    if (fault == FaultPeer::Fault::kGracefulGoaway) {
      ASSERT_TRUE(result) << result.error().Message();
      EXPECT_TRUE(result->status.Ok());
      EXPECT_TRUE(std::ranges::equal(result->payload, Bytes("valid")));
    } else {
      ASSERT_FALSE(result);
      EXPECT_EQ(result.error().Code(), grpcxx::StatusCode::kUnavailable);
    }
    auto next = (*channel)->CallUnary("/test.Echo/NoReplay", {});
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().Code(), grpcxx::StatusCode::kUnavailable);
    EXPECT_EQ(server.Requests(), 1U);
  }
}
} // namespace
} // namespace greptimedb::testing
