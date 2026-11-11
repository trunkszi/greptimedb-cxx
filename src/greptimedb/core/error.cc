#include "greptimedb/core/error.h"

#include <array>
#include <charconv>
#include <limits>
#include <utility>

#include <greptime/v1/common.pb.h>

#include "greptimedb/core/hints.h"

namespace greptimedb {
namespace {

std::optional<bool> ParseHint(std::string_view value) noexcept {
  if (value == "true" || value == "retryable") return true;
  if (value == "false" || value == "non_retryable") return false;
  return std::nullopt;
}

std::optional<std::uint32_t> ParseCode(std::string_view value) noexcept {
  if (value.empty() || (value.size() > 1 && value.front() == '0')) return std::nullopt;
  for (const auto byte : value) {
    if (byte < '0' || byte > '9') return std::nullopt;
  }
  std::uint32_t result = 0;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() || result == 0) {
    return std::nullopt;
  }
  return result;
}

bool EqualAscii(std::string_view a, std::string_view b) noexcept {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    const auto lower = [](char c) { return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c; };
    if (lower(a[i]) != lower(b[i])) return false;
  }
  return true;
}

struct InnerError {
  std::uint32_t code;
  std::string message;
};

/** Bounded-depth RFC 8259 parser: validates unknown fields as well as
 * diagnostic fields. */
class InnerParser final {
public:
  explicit InnerParser(std::string_view text) noexcept : text_(text) {}

  std::optional<InnerError> Parse() noexcept {
    SkipWhitespace();
    if (!Take('{')) return std::nullopt;
    bool has_code = false;
    bool has_message = false;
    InnerError result{};
    SkipWhitespace();
    if (Take('}')) return std::nullopt;
    for (;;) {
      auto key = String();
      if (!key) return std::nullopt;
      SkipWhitespace();
      if (!Take(':')) return std::nullopt;
      SkipWhitespace();
      if (*key == "code") {
        if (has_code) return std::nullopt;
        has_code = true;
        const auto start = pos_;
        if (!Number()) return std::nullopt;
        const auto number = text_.substr(start, pos_ - start);
        const auto code = number == "0" ? std::optional<std::uint32_t>{0} : ParseCode(number);
        if (!code) return std::nullopt;
        result.code = *code;
      } else if (*key == "err_msg") {
        if (has_message) return std::nullopt;
        auto message = String();
        if (!message) return std::nullopt;
        result.message = std::move(*message);
        has_message = true;
      } else if (!Value(1)) {
        return std::nullopt;
      }
      SkipWhitespace();
      if (Take('}')) break;
      if (!Take(',')) return std::nullopt;
      SkipWhitespace();
    }
    SkipWhitespace();
    if (pos_ != text_.size() || !has_code || !has_message) return std::nullopt;
    return result;
  }

private:
  void SkipWhitespace() noexcept {
    while (pos_ < text_.size() &&
           (text_[pos_] == ' ' || text_[pos_] == '\t' || text_[pos_] == '\n' || text_[pos_] == '\r'))
      ++pos_;
  }

  bool Take(char c) noexcept {
    if (pos_ == text_.size() || text_[pos_] != c) return false;
    ++pos_;
    return true;
  }

  bool Digits() noexcept {
    const auto start = pos_;
    while (pos_ < text_.size() && text_[pos_] >= '0' && text_[pos_] <= '9')
      ++pos_;
    return pos_ != start;
  }

  bool Number() noexcept {
    static_cast<void>(Take('-'));
    if (!Take('0') && !Digits()) return false;
    if (Take('.') && !Digits()) return false;
    if (Take('e') || Take('E')) {
      if (!Take('+')) static_cast<void>(Take('-'));
      if (!Digits()) return false;
    }
    return true;
  }

  std::optional<std::uint32_t> HexWord() noexcept {
    std::uint32_t code = 0;
    for (int i = 0; i < 4; ++i) {
      if (pos_ == text_.size()) return std::nullopt;
      const char c = text_[pos_++];
      std::uint32_t digit = 16;
      if (c >= '0' && c <= '9') digit = static_cast<std::uint32_t>(c - '0');
      if (c >= 'a' && c <= 'f') digit = static_cast<std::uint32_t>(c - 'a' + 10);
      if (c >= 'A' && c <= 'F') digit = static_cast<std::uint32_t>(c - 'A' + 10);
      if (digit == 16) return std::nullopt;
      code = code * 16 + digit;
    }
    return code;
  }

  struct Utf8Bytes {
    std::array<char, 4> bytes{};
    std::uint8_t size{0};
  };

  static Utf8Bytes EncodeCodepoint(std::uint32_t code) noexcept {
    Utf8Bytes encoded;
    if (code <= 0x7f) {
      encoded.bytes[0] = static_cast<char>(code);
      encoded.size = 1;
    } else if (code <= 0x7ff) {
      encoded.bytes[0] = static_cast<char>(0xc0 | (code >> 6));
      encoded.bytes[1] = static_cast<char>(0x80 | (code & 63));
      encoded.size = 2;
    } else if (code <= 0xffff) {
      encoded.bytes[0] = static_cast<char>(0xe0 | (code >> 12));
      encoded.bytes[1] = static_cast<char>(0x80 | ((code >> 6) & 63));
      encoded.bytes[2] = static_cast<char>(0x80 | (code & 63));
      encoded.size = 3;
    } else {
      encoded.bytes[0] = static_cast<char>(0xf0 | (code >> 18));
      encoded.bytes[1] = static_cast<char>(0x80 | ((code >> 12) & 63));
      encoded.bytes[2] = static_cast<char>(0x80 | ((code >> 6) & 63));
      encoded.bytes[3] = static_cast<char>(0x80 | (code & 63));
      encoded.size = 4;
    }
    return encoded;
  }

  std::optional<Utf8Bytes> UnicodeEscape() noexcept {
    auto code = HexWord();
    if (!code) return std::nullopt;
    if (*code >= 0xd800 && *code <= 0xdbff) {
      if (!Take('\\') || !Take('u')) return std::nullopt;
      const auto low = HexWord();
      if (!low || *low < 0xdc00 || *low > 0xdfff) return std::nullopt;
      *code = 0x10000 + ((*code - 0xd800) << 10) + (*low - 0xdc00);
    } else if (*code >= 0xdc00 && *code <= 0xdfff) {
      return std::nullopt;
    }
    return EncodeCodepoint(*code);
  }

  std::optional<Utf8Bytes> Utf8(unsigned char first) noexcept {
    std::uint32_t code = 0;
    int count = 0;
    std::uint32_t minimum = 0;
    if (first >= 0xc2 && first <= 0xdf) {
      code = first & 31;
      count = 1;
      minimum = 0x80;
    } else if (first >= 0xe0 && first <= 0xef) {
      code = first & 15;
      count = 2;
      minimum = 0x800;
    } else if (first >= 0xf0 && first <= 0xf4) {
      code = first & 7;
      count = 3;
      minimum = 0x10000;
    } else {
      return std::nullopt;
    }
    for (int i = 0; i < count; ++i) {
      if (pos_ == text_.size()) return std::nullopt;
      const auto byte = static_cast<unsigned char>(text_[pos_++]);
      if ((byte & 0xc0) != 0x80) return std::nullopt;
      code = (code << 6) | (byte & 63);
    }
    if (code < minimum || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return std::nullopt;
    return EncodeCodepoint(code);
  }

  std::optional<std::string> String() noexcept {
    if (!Take('"')) return std::nullopt;
    std::string output;
    while (pos_ < text_.size()) {
      const auto c = static_cast<unsigned char>(text_[pos_++]);
      if (c == '"') return output;
      if (c < 0x20) return std::nullopt;
      if (c >= 0x80) {
        auto encoded = Utf8(c);
        if (!encoded) return std::nullopt;
        output.append(encoded->bytes.data(), encoded->size);
      } else if (c != '\\') {
        output.push_back(static_cast<char>(c));
      } else {
        if (pos_ == text_.size()) return std::nullopt;
        const auto escape = text_[pos_++];
        if (escape == 'u') {
          auto encoded = UnicodeEscape();
          if (!encoded) return std::nullopt;
          output.append(encoded->bytes.data(), encoded->size);
          continue;
        }
        char decoded = 0;
        if (escape == '"' || escape == '/' || escape == '\\')
          decoded = escape;
        else if (escape == 'b')
          decoded = '\b';
        else if (escape == 'f')
          decoded = '\f';
        else if (escape == 'n')
          decoded = '\n';
        else if (escape == 'r')
          decoded = '\r';
        else if (escape == 't')
          decoded = '\t';
        else
          return std::nullopt;
        output.push_back(decoded);
      }
    }
    return std::nullopt;
  }

  bool Value(std::size_t depth) noexcept {
    if (depth > 32 || pos_ == text_.size()) return false;
    if (text_[pos_] == '"') return String().has_value();
    if (text_[pos_] == '-' || (text_[pos_] >= '0' && text_[pos_] <= '9')) return Number();
    for (const auto token : {std::string_view("true"), std::string_view("false"), std::string_view("null")}) {
      if (text_.substr(pos_, token.size()) == token) {
        pos_ += token.size();
        return true;
      }
    }
    const bool object = text_[pos_] == '{';
    if (!object && text_[pos_] != '[') return false;
    ++pos_;
    SkipWhitespace();
    const auto end = object ? '}' : ']';
    if (Take(end)) return true;
    for (;;) {
      if (object) {
        if (!String()) return false;
        SkipWhitespace();
        if (!Take(':')) return false;
        SkipWhitespace();
      }
      if (!Value(depth + 1)) return false;
      SkipWhitespace();
      if (Take(end)) return true;
      if (!Take(',')) return false;
      SkipWhitespace();
    }
  }

  std::string_view text_;
  std::size_t pos_{0};
};

struct Diagnostics {
  std::optional<InnerError> inner;
  std::optional<std::uint32_t> numeric_code;
  std::optional<std::string_view> message;
  bool invalid_code{false};
  bool invalid_inner{false};
  bool has_hint{false};
  bool allow_retry{true};
};

Diagnostics Inspect(const grpcxx::MetadataMap &trailers) noexcept {
  Diagnostics result;
  for (const auto &entry : trailers.Entries()) {
    if (entry.binary) continue;
    if (EqualAscii(entry.key, kGreptimeRetryHintHeader)) {
      const auto hint = ParseHint(entry.value);
      result.has_hint |= hint.has_value();
      result.allow_retry &= hint == true;
    } else if (EqualAscii(entry.key, kGreptimeInnerErrorHeader)) {
      if (!entry.value.empty()) {
        auto parsed = entry.value.size() <= 65536 ? InnerParser(entry.value).Parse() : std::nullopt;
        if (!parsed)
          result.invalid_inner = true;
        else if (parsed->code != 0) {
          if (result.inner && (result.inner->code != parsed->code || result.inner->message != parsed->message))
            result.invalid_inner = true;
          else if (!result.inner)
            result.inner = std::move(parsed);
        }
      }
    } else if (EqualAscii(entry.key, kGreptimeErrorCodeHeader)) {
      const auto code = ParseCode(entry.value);
      if (!code || (result.numeric_code && result.numeric_code != code))
        result.invalid_code = true;
      else
        result.numeric_code = code;
    } else if (EqualAscii(entry.key, kGreptimeErrorMessageHeader) && !entry.value.empty() && !result.message) {
      result.message = entry.value;
    }
  }
  return result;
}

} // namespace

ErrorCode ClassifyErrorCode(std::uint32_t code) noexcept {
  if ((code >= 6000 && code <= 6999) || (code >= 7001 && code <= 7006)) return ErrorCode::kAuthFailed;
  switch (code) {
  case 0: return ErrorCode::kSuccess;
  case 1000: return ErrorCode::kUnknown;
  case 1001: return ErrorCode::kUnsupported;
  case 1002: return ErrorCode::kUnexpected;
  case 1003: return ErrorCode::kInternal;
  case 1004: return ErrorCode::kInvalidArguments;
  case 1005: return ErrorCode::kCancelled;
  case 1006: return ErrorCode::kIllegalState;
  case 2000: return ErrorCode::kSyntax;
  case 2001: return ErrorCode::kPlan;
  case 2002: return ErrorCode::kEngineExecute;
  case 3000: return ErrorCode::kTableAlreadyExists;
  case 3001: return ErrorCode::kTableNotFound;
  case 3002: return ErrorCode::kTableColumnNotFound;
  case 3003: return ErrorCode::kTableColumnExists;
  case 3004: return ErrorCode::kDatabaseNotFound;
  case 3005: return ErrorCode::kDatabaseAlreadyExists;
  case 4000: return ErrorCode::kStorageUnavailable;
  case 4001: return ErrorCode::kRegionNotFound;
  case 4002: return ErrorCode::kRegionReadonly;
  case 4003: return ErrorCode::kRegionBusy;
  case 4004: return ErrorCode::kRateLimited;
  case 4005: return ErrorCode::kFlowControl;
  case 5000: return ErrorCode::kMetaSrv;
  default: return ErrorCode::kOther;
  }
}

ErrorCode ClassifyV121ErrorCode(std::uint32_t code) noexcept {
  if (code == 0 || (code >= 1000 && code <= 1006) || code == 2000) return ClassifyErrorCode(code);
  if (code == 3000) return ErrorCode::kPlan;
  if (code == 3001) return ErrorCode::kEngineExecute;
  if (code == 4000) return ErrorCode::kTableAlreadyExists;
  if (code == 4001) return ErrorCode::kTableNotFound;
  if (code == 4002) return ErrorCode::kTableColumnNotFound;
  if (code == 4003 || code == 4009) return ErrorCode::kRegionBusy;
  if (code == 4004) return ErrorCode::kDatabaseNotFound;
  if (code == 4005) return ErrorCode::kRegionNotFound;
  if (code == 4007) return ErrorCode::kRegionReadonly;
  if (code == 4011) return ErrorCode::kDatabaseAlreadyExists;
  if (code == 5000) return ErrorCode::kStorageUnavailable;
  if (code == 6000) return ErrorCode::kResourceExhausted;
  if (code == 6001) return ErrorCode::kRateLimited;
  if (code >= 7000 && code <= 7006) return ErrorCode::kAuthFailed;
  return ErrorCode::kOther;
}

Error::Error(ErrorKind kind, std::string message) noexcept : kind_(kind), message_(std::move(message)) {}

[[gnu::cold, gnu::noinline]] Error Error::InvalidConfig(std::string message) noexcept {
  return Error(ErrorKind::kInvalidConfig, std::move(message));
}

[[gnu::cold, gnu::noinline]] Error Error::Transport(std::string message) noexcept {
  return Error(ErrorKind::kTransport, std::move(message));
}

Error Error::ChannelStatus(grpcxx::Status status) noexcept {
  auto result = Grpc(std::move(status));
  switch (result.grpc_code_) {
  case grpcxx::StatusCode::kDeadlineExceeded: result.kind_ = ErrorKind::kTimeout; break;
  case grpcxx::StatusCode::kUnavailable: result.kind_ = ErrorKind::kTransport; break;
  case grpcxx::StatusCode::kInternal:
  case grpcxx::StatusCode::kDataLoss: result.kind_ = ErrorKind::kProtocol; break;
  default: break;
  }
  return result;
}

Error Error::Grpc(grpcxx::Status status) noexcept {
  Error result(ErrorKind::kGrpc, std::string(status.Message()));
  result.grpc_code_ = status.Code();
  return result;
}

[[gnu::cold, gnu::noinline]] Error Error::ServerStatus(std::uint32_t code, std::string message,
                                                       bool retry_hint) noexcept {
  Error result(ErrorKind::kServerStatus, std::move(message));
  result.raw_code_ = code;
  result.code_ = ClassifyErrorCode(code);
  result.retry_hint_ = retry_hint;
  return result;
}

[[gnu::cold, gnu::noinline]] Error Error::AllEndpointsFailed(std::size_t attempts, const Error &last) noexcept {
  Error result = last;
  result.kind_ = ErrorKind::kAllEndpointsFailed;
  result.attempts_ = attempts;
  return result;
}

Result<void> Error::FromResponseHeader(const ::greptime::v1::ResponseHeader *header) noexcept {
  if (header == nullptr || !header->has_status() || header->status().status_code() == 0) return {};
  const auto &status = header->status();
  return std::unexpected(ServerStatus(status.status_code(), status.err_msg(), ParseHint(status.retry_hint()) == true));
}

Error Error::FromGrpcStatusAndTrailers(grpcxx::Status status, const grpcxx::MetadataMap *trailers) noexcept {
  if (trailers == nullptr) return Grpc(std::move(status));
  auto diagnostic = Inspect(*trailers);
  const bool retry = diagnostic.has_hint && diagnostic.allow_retry;
  if (diagnostic.inner) {
    return ServerStatus(diagnostic.inner->code, std::move(diagnostic.inner->message), retry);
  }
  const auto message = status.Message().empty() ? diagnostic.message.value_or("") : status.Message();
  if (diagnostic.numeric_code && !diagnostic.invalid_code) {
    return ServerStatus(*diagnostic.numeric_code, std::string(message), retry);
  }
  if (diagnostic.has_hint || diagnostic.message) {
    return ServerStatus(1000, std::string(message), retry);
  }
  return Grpc(std::move(status));
}

Result<void> Error::CheckOkTrailers(const grpcxx::MetadataMap &trailers) noexcept {
  const auto diagnostic = Inspect(trailers);
  if (diagnostic.invalid_inner || (!diagnostic.inner && diagnostic.numeric_code && diagnostic.invalid_code)) {
    return std::unexpected(
        Error(ErrorKind::kProtocol, "conflicting or malformed application diagnostics in OK trailers"));
  }
  if (diagnostic.inner || (diagnostic.numeric_code && !diagnostic.invalid_code) || diagnostic.message) {
    return std::unexpected(FromGrpcStatusAndTrailers(grpcxx::Status::OkStatus(), trailers));
  }
  return {};
}

} // namespace greptimedb
