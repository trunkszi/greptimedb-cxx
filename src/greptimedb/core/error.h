#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <grpcxx/core/metadata.h>
#include <grpcxx/core/status.h>

namespace greptime::v1 {
class ResponseHeader;
}

namespace greptimedb {

/** Legacy SDK categories; the original wire code is always retained by Error.
 */
enum class ErrorCode : std::uint32_t {
  kSuccess = 0,
  kUnknown = 1000,
  kUnsupported = 1001,
  kUnexpected = 1002,
  kInternal = 1003,
  kInvalidArguments = 1004,
  kCancelled = 1005,
  kIllegalState = 1006,
  kSyntax = 2000,
  kPlan = 2001,
  kEngineExecute = 2002,
  kTableAlreadyExists = 3000,
  kTableNotFound = 3001,
  kTableColumnNotFound = 3002,
  kTableColumnExists = 3003,
  kDatabaseNotFound = 3004,
  kDatabaseAlreadyExists = 3005,
  kStorageUnavailable = 4000,
  kRegionNotFound = 4001,
  kRegionReadonly = 4002,
  kRegionBusy = 4003,
  kRateLimited = 4004,
  kFlowControl = 4005,
  kMetaSrv = 5000,
  kAuthFailed = 6000,
  /** A versioned category without a distinct legacy SDK wire code. */
  kResourceExhausted = 0xfffffffe,
  kOther = 0xffffffff,
};

/** Classifies legacy status codes without conflating versioned server
 * numbering. */
[[nodiscard]] ErrorCode ClassifyErrorCode(std::uint32_t code) noexcept;
/** Explicit classification for GreptimeDB v1.2.1 wire numbering. */
[[nodiscard]] ErrorCode ClassifyV121ErrorCode(std::uint32_t code) noexcept;

enum class ErrorKind : std::uint8_t {
  kTransport,
  kGrpc,
  kStream,
  kServerStatus,
  kInvalidConfig,
  kSchemaViolation,
  kEncode,
  kDecode,
  kProtocol,
  kCircuitOpen,
  kAllEndpointsFailed,
};

class Error;
template <typename T> using Result = std::expected<T, Error>;

/** Owned error diagnostics; retry permission alone never proves a write was not
 * sent. */
class Error final {
public:
  Error(ErrorKind kind, std::string message) noexcept;
  [[nodiscard]] static Error InvalidConfig(std::string message) noexcept;
  [[nodiscard]] static Error Transport(std::string message) noexcept;
  [[nodiscard]] static Error Grpc(grpcxx::Status status) noexcept;
  [[nodiscard]] static Error ServerStatus(std::uint32_t code,
                                          std::string message,
                                          bool retry_hint = false) noexcept;
  [[nodiscard]] static Error AllEndpointsFailed(std::size_t attempts,
                                                const Error &last) noexcept;

  [[nodiscard]] ErrorKind Kind() const noexcept { return kind_; }
  [[nodiscard]] ErrorCode Code() const noexcept { return code_; }
  [[nodiscard]] std::uint32_t RawCode() const noexcept { return raw_code_; }
  [[nodiscard]] std::string_view Message() const noexcept { return message_; }
  [[nodiscard]] grpcxx::StatusCode GrpcCode() const noexcept {
    return grpc_code_;
  }
  [[nodiscard]] std::optional<bool> RetryHint() const noexcept {
    return retry_hint_;
  }
  [[nodiscard]] bool IsRetryable() const noexcept {
    return retry_hint_ == true;
  }
  [[nodiscard]] std::size_t Attempts() const noexcept { return attempts_; }

  /** A missing response header or status is not an application failure. */
  [[nodiscard]] static Result<void>
  FromResponseHeader(const ::greptime::v1::ResponseHeader *header) noexcept;
  [[nodiscard]] static Result<void>
  FromResponseHeader(const ::greptime::v1::ResponseHeader &header) noexcept {
    return FromResponseHeader(&header);
  }
  /** Extracts valid application diagnostics, preserving the original gRPC
   * failure otherwise. */
  [[nodiscard]] static Error
  FromGrpcStatusAndTrailers(grpcxx::Status status,
                            const grpcxx::MetadataMap *trailers) noexcept;
  [[nodiscard]] static Error
  FromGrpcStatusAndTrailers(grpcxx::Status status,
                            const grpcxx::MetadataMap &trailers) noexcept {
    return FromGrpcStatusAndTrailers(std::move(status), &trailers);
  }
  /** Checks application diagnostics even when grpc-status is OK; retry hints
   * alone are benign. */
  [[nodiscard]] static Result<void>
  CheckOkTrailers(const grpcxx::MetadataMap &trailers) noexcept;

private:
  ErrorKind kind_;
  ErrorCode code_{ErrorCode::kUnknown};
  std::uint32_t raw_code_{1000};
  std::string message_;
  std::optional<bool> retry_hint_;
  grpcxx::StatusCode grpc_code_{grpcxx::StatusCode::kUnknown};
  std::size_t attempts_{0};
};

} // namespace greptimedb
