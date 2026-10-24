#include "greptimedb/client/client.h"

#include <cstdlib>
#include <limits>
#include <new>
#include <utility>

#include <greptime/v1/health.pb.h>

#include "greptimedb/batch/columnar_batch.h"
#include "greptimedb/batch/row_batch.h"
#include "greptimedb/client/ddl.h"

namespace greptimedb {
namespace {
constexpr std::string_view kDatabasePath = "/greptime.v1.GreptimeDatabase/Handle";
constexpr std::string_view kHealthPath = "/greptime.v1.HealthCheck/HealthCheck";
constexpr std::string_view kPromPath = "/greptime.v1.PrometheusGateway/Handle";

const void *ThreadToken() noexcept {
  static thread_local const char token{};
  return &token;
}

std::string EndpointName(const Endpoint &endpoint) {
  switch (endpoint.Scheme()) {
  case TransportScheme::kGrpc: return "grpc://" + endpoint.SocketAddress();
  case TransportScheme::kGrpcs: return "grpcs://" + endpoint.SocketAddress();
  case TransportScheme::kUnix: return "unix://" + endpoint.SocketAddress();
  }
  std::abort();
}

template <class Channel>
auto ConnectChannel(const ClientOptions &options, const TlsOptions &tls) {
  if constexpr (requires { Channel::Connect(options.endpoints.front(), options, tls); })
    return Channel::Connect(options.endpoints.front(), options, tls);
  else
    return Channel::Connect(EndpointName(options.endpoints.front()), tls);
}

grpcxx::CompressionAlgorithm WireCompression(Compression compression) noexcept {
  switch (compression) {
  case Compression::kIdentity: return grpcxx::CompressionAlgorithm::kIdentity;
  case Compression::kGzip: return grpcxx::CompressionAlgorithm::kGzip;
  case Compression::kDeflate: return grpcxx::CompressionAlgorithm::kDeflate;
  case Compression::kSnappy: return grpcxx::CompressionAlgorithm::kSnappy;
  case Compression::kZstd: return grpcxx::CompressionAlgorithm::kZstd;
  case Compression::kLz4: return grpcxx::CompressionAlgorithm::kLz4;
  }
  std::abort();
}

template <class T> Result<T> Decode(std::span<const std::byte> bytes) {
  if (bytes.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(Error{ErrorKind::kProtocol, "response exceeds protobuf size limit"});
  T message;
  if (!message.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())))
    return std::unexpected(Error{ErrorKind::kDecode, "invalid protobuf response"});
  return message;
}

Result<grpcxx::MetadataMap> ErrorMetadata(const RpcResponse &response) {
  grpcxx::MetadataMap metadata = response.trailing_metadata;
  for (const auto &entry : response.initial_metadata.Entries()) {
    if (entry.binary) continue;
    if (entry.key != "inner_error_msg" && entry.key != kGreptimeRetryHintHeader &&
        entry.key != kGreptimeErrorCodeHeader && entry.key != kGreptimeErrorMessageHeader)
      continue;
    if (auto added = metadata.Add(entry.key, entry.value); !added)
      return std::unexpected(Error{ErrorKind::kProtocol, "invalid response error metadata"});
  }
  return metadata;
}
Result<void> ValidateClientOptions(const ClientOptions &options) {
  if (auto metadata = options.BuildCallMetadata(); !metadata)
    return std::unexpected(metadata.error());
  if (!client::detail::ValidScope(
          {options.catalog, options.schema, options.timezone.value_or("")}))
    return std::unexpected(Error::InvalidConfig("invalid client scope"));
  return {};
}

Result<void> CheckResponseHeader(const ::greptime::v1::ResponseHeader *header,
                                 const RpcResponse &response) {
  auto result = Error::FromResponseHeader(header);
  if (result || !result.error().IsRetryable()) return result;
  auto metadata = ErrorMetadata(response);
  if (!metadata) return std::unexpected(metadata.error());
  for (const auto &entry : metadata->Entries()) {
    if (!entry.binary && entry.key == kGreptimeRetryHintHeader &&
        entry.value != "true" && entry.value != "retryable")
      return std::unexpected(Error::ServerStatus(result.error().RawCode(),
                                                std::string(result.error().Message()), false));
  }
  return result;
}
} // namespace

Error client_detail::ColumnError(ColumnBufError error) {
  switch (error) {
  case ColumnBufError::kAllocationFailure:
    return Error{ErrorKind::kEncode, "column buffer allocation failed"};
  case ColumnBufError::kNullTimestamp:
    return Error{ErrorKind::kSchemaViolation, "timestamp cannot be null"};
  case ColumnBufError::kInvalidDecimal:
    return Error{ErrorKind::kSchemaViolation, "invalid decimal precision, scale or coefficient"};
  case ColumnBufError::kTypeMismatch:
    return Error{ErrorKind::kSchemaViolation, "value does not match column type"};
  case ColumnBufError::kInvalidSchema:
    return Error{ErrorKind::kSchemaViolation, "invalid column schema"};
  }
  return Error{ErrorKind::kSchemaViolation, "invalid column buffer"};
}

Client::Client(ClientOptions options, std::unique_ptr<SynchronousChannel> channel)
    : options_(std::move(options)), channel_(std::move(channel)), owner_(ThreadToken()) {}
Client::~Client() { CheckThread(); }
void Client::CheckThread() const noexcept {
  if (owner_ != ThreadToken()) std::abort();
}
Result<void> Client::ValidateDestination(std::string_view table) {
  bool nonblank = false;
  for (const unsigned char ch : table) {
    if (ch == 0)
      return std::unexpected(Error{ErrorKind::kSchemaViolation, "table name contains NUL"});
    if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' && ch != '\f' && ch != '\v')
      nonblank = true;
  }
  if (!nonblank)
    return std::unexpected(Error{ErrorKind::kSchemaViolation, "table name must be nonempty"});
  return {};
}
const ClientOptions &Client::Options() const noexcept { CheckThread(); return options_; }
SynchronousChannel &Client::Channel() noexcept { CheckThread(); return *channel_; }

Result<std::unique_ptr<Client>> Client::Connect(ClientOptions options, const TlsOptions &tls) {
  if (auto valid = ValidateClientOptions(options); !valid)
    return std::unexpected(valid.error());
  auto channel = ConnectChannel<SynchronousChannel>(options, tls);
  if (!channel) return std::unexpected(Error::Grpc(channel.error()));
  return FromChannel(std::move(options), std::move(*channel));
}

Result<std::unique_ptr<Client>>
Client::FromChannel(ClientOptions options, std::unique_ptr<SynchronousChannel> channel) {
  if (auto valid = ValidateClientOptions(options); !valid)
    return std::unexpected(valid.error());
  if (!channel) return std::unexpected(Error::InvalidConfig("client requires a channel"));
  std::unique_ptr<Client> client{new (std::nothrow) Client(std::move(options), std::move(channel))};
  if (!client)
    return std::unexpected(Error{ErrorKind::kEncode, "client allocation failed"});
  return client;
}

Result<std::unique_ptr<Client>>
Client::FromMemoryPipe(ClientOptions options, std::unique_ptr<EndpointTransport> pipe,
                       void *context, SynchronousChannel::ProgressHook progress) {
  if (auto valid = ValidateClientOptions(options); !valid)
    return std::unexpected(valid.error());
  auto channel = SynchronousChannel::Create(std::move(pipe), options.endpoints.front().Authority());
  if (!channel) return std::unexpected(Error::Grpc(channel.error()));
  if (progress) (*channel)->SetProgressHook(context, progress);
  return FromChannel(std::move(options), std::move(*channel));
}

Result<RpcResponse> Client::Unary(std::string_view path,
                                const google::protobuf::MessageLite &request,
                                const WriteHints *hints) {
  auto call = BuildCallOptions(hints);
  if (!call) return std::unexpected(call.error());
  const auto size = request.ByteSizeLong();
  if (size > 4U * 1024U * 1024U)
    return std::unexpected(Error{ErrorKind::kEncode, "request exceeds channel 4 MiB message limit"});
  std::unique_ptr<std::byte[]> bytes{new (std::nothrow) std::byte[size == 0 ? 1 : size]};
  if (!bytes) return std::unexpected(Error{ErrorKind::kEncode, "request allocation failed"});
  if (!request.SerializeToArray(bytes.get(), static_cast<int>(size)))
    return std::unexpected(Error{ErrorKind::kEncode, "protobuf serialization failed"});
  auto response = channel_->Unary(path, {bytes.get(), size}, std::move(*call));
  if (!response) return std::unexpected(Error::Grpc(response.error()));
  auto diagnostics = ErrorMetadata(*response);
  if (!diagnostics) return std::unexpected(diagnostics.error());
  if (!response->status.Ok())
    return std::unexpected(Error::FromGrpcStatusAndTrailers(response->status, *diagnostics));
  if (auto valid = Error::CheckOkTrailers(*diagnostics); !valid)
    return std::unexpected(valid.error());
  return std::move(*response);
}

Result<CallOptions> Client::BuildCallOptions(const WriteHints *hints) {
  CheckThread();
  auto metadata = options_.BuildCallMetadata(hints);
  if (!metadata) return std::unexpected(metadata.error());
  CallOptions call;
  call.metadata = std::move(*metadata);
  call.timeout = options_.request_timeout;
  call.compression = WireCompression(options_.compression);
  return call;
}

Result<std::unique_ptr<::greptimedb::StreamInserter>>
Client::StreamInserter(const WriteHints *hints) {
  auto call = BuildCallOptions(hints);
  if (!call) return std::unexpected(call.error());
  return ::greptimedb::StreamInserter::Open(*channel_, options_.BuildRequestHeader(),
                                          std::move(*call));
}

Result<std::unique_ptr<flight::BulkStreamWriter>> Client::BulkWriter(
    std::string_view table, const flight::FlightSchema &schema,
    std::optional<flight::BulkWriteOptions> options, const WriteHints *hints) {
  auto call = BuildCallOptions(hints);
  if (!call) return std::unexpected(call.error());
  if (auto valid = ValidateDestination(table); !valid)
    return std::unexpected(valid.error());
  if (!options) {
    options.emplace();
    options->max_in_flight = options_.max_in_flight;
    options->timeout = options_.request_timeout;
  }
  return flight::BulkStreamWriter::Open(*channel_, table, schema, *options, std::move(*call));
}

Result<std::unique_ptr<flight::QueryStream>>
Client::Query(const ::greptime::v1::QueryRequest &query) {
  auto call = BuildCallOptions();
  if (!call) return std::unexpected(call.error());
  return flight::QueryStream::Open(*channel_, options_.BuildRequestHeader(), query,
                                   std::move(*call));
}

Result<std::unique_ptr<flight::QueryStream>> Client::Query(std::string_view sql) {
  auto call = BuildCallOptions();
  if (!call) return std::unexpected(call.error());
  return flight::QueryStream::Sql(*channel_, options_.BuildRequestHeader(), sql,
                                  std::move(*call));
}

Result<void> Client::Ping() {
  auto response = Unary(kHealthPath, ::greptime::v1::HealthCheckRequest{});
  if (!response) return std::unexpected(response.error());
  auto decoded = Decode<::greptime::v1::HealthCheckResponse>(response->payload);
  if (!decoded) return std::unexpected(decoded.error());
  return {};
}

Result<HealthReport> Client::ProbeHealth() {
  CheckThread();
  const auto started = std::chrono::steady_clock::now();
  if (auto health = Ping(); !health) return std::unexpected(health.error());
  return HealthReport{EndpointName(options_.endpoints.front()),
                      std::chrono::steady_clock::now() - started, true};
}

Result<std::uint32_t> Client::HandleRequest(::greptime::v1::GreptimeRequest request,
                                          const WriteHints *hints) {
  CheckThread();
  *request.mutable_header() = options_.BuildRequestHeader();
  auto response = Unary(kDatabasePath, request, hints);
  if (!response) return std::unexpected(response.error());
  auto decoded = Decode<::greptime::v1::GreptimeResponse>(response->payload);
  if (!decoded) return std::unexpected(decoded.error());
  if (auto valid = CheckResponseHeader(decoded->has_header() ? &decoded->header() : nullptr, *response);
      !valid) return std::unexpected(valid.error());
  if (!decoded->has_affected_rows())
    return std::unexpected(Error{ErrorKind::kProtocol, "GreptimeDatabase returned no affected-row result"});
  return decoded->affected_rows().value();
}

Result<std::uint32_t> Client::InsertBatch(::greptime::v1::InsertRequest insert,
                                        const WriteHints *hints) {
  CheckThread();
  if (auto valid = ValidateDestination(insert.table_name()); !valid)
    return std::unexpected(valid.error());
  if (insert.row_count() == 0) return std::uint32_t{0};
  ::greptime::v1::InsertRequests requests;
  *requests.add_inserts() = std::move(insert);
  return InsertBatches(std::move(requests), hints);
}
Result<std::uint32_t> Client::InsertBatch(const ColumnarBatch &batch,
                                        const WriteHints *hints) {
  CheckThread();
  if (auto valid = ValidateDestination(batch.TableName()); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeInsertRequest();
  if (!encoded) return std::unexpected(encoded.error());
  return InsertBatch(std::move(*encoded), hints);
}
Result<std::uint32_t> Client::InsertBatchTo(std::string_view table, const ColumnarBatch &batch,
                                          const WriteHints *hints) {
  CheckThread();
  if (auto valid = ValidateDestination(table); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeInsertRequestFor(table);
  if (!encoded) return std::unexpected(encoded.error());
  return InsertBatch(std::move(*encoded), hints);
}
Result<std::uint32_t> Client::InsertBatches(std::span<const ColumnarBatch *const> batches,
                                          const WriteHints *hints) {
  CheckThread();
  ::greptime::v1::InsertRequests requests;
  for (const auto *batch : batches) {
    if (!batch)
      return std::unexpected(Error{ErrorKind::kSchemaViolation, "null insert batch"});
    if (auto valid = ValidateDestination(batch->TableName()); !valid)
      return std::unexpected(valid.error());
    auto encoded = batch->EncodeInsertRequest();
    if (!encoded) return std::unexpected(encoded.error());
    *requests.add_inserts() = std::move(*encoded);
  }
  return InsertBatches(std::move(requests), hints);
}
Result<std::uint32_t> Client::InsertMultiBatch(const MultiTableBatch &batch,
                                              const WriteHints *hints) {
  CheckThread();
  return InsertBatches(batch.ToInsertRequests(), hints);
}
Result<std::uint32_t> Client::InsertBatches(::greptime::v1::InsertRequests inserts,
                                          const WriteHints *hints) {
  CheckThread();
  for (const auto &insert : inserts.inserts()) {
    if (auto valid = ValidateDestination(insert.table_name()); !valid)
      return std::unexpected(valid.error());
  }
  auto *items = inserts.mutable_inserts();
  for (int i = items->size(); i > 0; --i)
    if (items->Get(i - 1).row_count() == 0) items->DeleteSubrange(i - 1, 1);
  if (items->empty()) return std::uint32_t{0};
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_inserts() = std::move(inserts);
  return HandleRequest(std::move(request), hints);
}
Result<std::uint32_t> Client::InsertRows(::greptime::v1::RowInsertRequest insert,
                                       const WriteHints *hints) {
  CheckThread();
  if (auto valid = ValidateDestination(insert.table_name()); !valid)
    return std::unexpected(valid.error());
  if (insert.rows().rows_size() == 0) return std::uint32_t{0};
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_row_inserts()->add_inserts() = std::move(insert);
  return HandleRequest(std::move(request), hints);
}
Result<std::uint32_t> Client::InsertRows(const RowBatch &batch, const WriteHints *hints) {
  CheckThread();
  if (auto valid = ValidateDestination(batch.TableName()); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeRowInsertRequest();
  if (!encoded) return std::unexpected(encoded.error());
  return InsertRows(std::move(*encoded), hints);
}
Result<std::uint32_t> Client::InsertRowsTo(std::string_view table, const RowBatch &batch,
                                         const WriteHints *hints) {
  CheckThread();
  if (auto valid = ValidateDestination(table); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeRowInsertRequestFor(table);
  if (!encoded) return std::unexpected(encoded.error());
  return InsertRows(std::move(*encoded), hints);
}
Result<std::uint32_t> Client::DeleteBatch(::greptime::v1::DeleteRequest deletion) {
  CheckThread();
  if (auto valid = ValidateDestination(deletion.table_name()); !valid)
    return std::unexpected(valid.error());
  if (deletion.row_count() == 0) return std::uint32_t{0};
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_deletes()->add_deletes() = std::move(deletion);
  return HandleRequest(std::move(request));
}
Result<std::uint32_t> Client::DeleteBatch(const ColumnarBatch &batch) {
  CheckThread();
  if (auto valid = ValidateDestination(batch.TableName()); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeDeleteRequest();
  if (!encoded) return std::unexpected(encoded.error());
  return DeleteBatch(std::move(*encoded));
}
Result<std::uint32_t> Client::DeleteBatchFrom(std::string_view table, const ColumnarBatch &batch) {
  CheckThread();
  if (auto valid = ValidateDestination(table); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeDeleteRequestFor(table);
  if (!encoded) return std::unexpected(encoded.error());
  return DeleteBatch(std::move(*encoded));
}
Result<std::uint32_t> Client::DeleteRows(::greptime::v1::RowDeleteRequest deletion) {
  CheckThread();
  if (auto valid = ValidateDestination(deletion.table_name()); !valid)
    return std::unexpected(valid.error());
  if (deletion.rows().rows_size() == 0) return std::uint32_t{0};
  ::greptime::v1::GreptimeRequest request;
  *request.mutable_row_deletes()->add_deletes() = std::move(deletion);
  return HandleRequest(std::move(request));
}
Result<std::uint32_t> Client::DeleteRows(const RowBatch &batch) {
  CheckThread();
  if (auto valid = ValidateDestination(batch.TableName()); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeRowDeleteRequest();
  if (!encoded) return std::unexpected(encoded.error());
  return DeleteRows(std::move(*encoded));
}
Result<std::uint32_t> Client::DeleteRowsFrom(std::string_view table, const RowBatch &batch) {
  CheckThread();
  if (auto valid = ValidateDestination(table); !valid)
    return std::unexpected(valid.error());
  auto encoded = batch.EncodeRowDeleteRequestFor(table);
  if (!encoded) return std::unexpected(encoded.error());
  return DeleteRows(std::move(*encoded));
}
Result<std::uint32_t> Client::ExecuteDdl(::greptime::v1::DdlRequest ddl) {
  CheckThread();
  auto request = client::BuildDdlRequest(
      {options_.catalog, options_.schema, options_.timezone.value_or("")}, ddl);
  if (!request)
    return std::unexpected(Error{ErrorKind::kSchemaViolation, "invalid DDL expression"});
  return HandleRequest(std::move(*request));
}

Result<std::uint32_t> Client::CreateDatabase(
    std::string_view schema, bool if_not_exists,
    const std::unordered_map<std::string, std::string> &options) {
  CheckThread();
  ::greptime::v1::CreateDatabaseExpr expression;
  expression.set_catalog_name(options_.catalog);
  expression.set_schema_name(std::string(schema));
  expression.set_create_if_not_exists(if_not_exists);
  for (const auto &[key, value] : options) (*expression.mutable_options())[key] = value;
  return CreateDatabase(std::move(expression));
}
Result<std::uint32_t> Client::CreateDatabase(::greptime::v1::CreateDatabaseExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_create_database() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::AlterDatabase(::greptime::v1::AlterDatabaseExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_alter_database() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::CreateTable(::greptime::v1::CreateTableExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_create_table() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::AlterTable(::greptime::v1::AlterTableExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_alter_table() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::DropTable(std::string_view table, bool if_exists) {
  CheckThread();
  ::greptime::v1::DropTableExpr expression;
  expression.set_catalog_name(options_.catalog);
  expression.set_schema_name(options_.schema);
  expression.set_table_name(std::string(table));
  expression.set_drop_if_exists(if_exists);
  return DropTable(std::move(expression));
}
Result<std::uint32_t> Client::DropTable(::greptime::v1::DropTableExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_drop_table() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::TruncateTable(std::string_view table) {
  CheckThread();
  ::greptime::v1::TruncateTableExpr expression;
  expression.set_catalog_name(options_.catalog);
  expression.set_schema_name(options_.schema);
  expression.set_table_name(std::string(table));
  return TruncateTable(std::move(expression));
}
Result<std::uint32_t> Client::TruncateTable(::greptime::v1::TruncateTableExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_truncate_table() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::CreateView(::greptime::v1::CreateViewExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_create_view() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::DropView(std::string_view view, bool if_exists) {
  CheckThread();
  ::greptime::v1::DropViewExpr expression;
  expression.set_catalog_name(options_.catalog);
  expression.set_schema_name(options_.schema);
  expression.set_view_name(std::string(view));
  expression.set_drop_if_exists(if_exists);
  return DropView(std::move(expression));
}
Result<std::uint32_t> Client::DropView(::greptime::v1::DropViewExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_drop_view() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::CreateFlow(::greptime::v1::CreateFlowExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_create_flow() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::DropFlow(std::string_view flow, bool if_exists) {
  CheckThread();
  ::greptime::v1::DropFlowExpr expression;
  expression.set_catalog_name(options_.catalog);
  expression.set_flow_name(std::string(flow));
  expression.set_drop_if_exists(if_exists);
  return DropFlow(std::move(expression));
}
Result<std::uint32_t> Client::DropFlow(::greptime::v1::DropFlowExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_drop_flow() = std::move(expression);
  return ExecuteDdl(std::move(request));
}
Result<std::uint32_t> Client::CommentOn(::greptime::v1::CommentOnExpr expression) {
  ::greptime::v1::DdlRequest request;
  *request.mutable_comment_on() = std::move(expression);
  return ExecuteDdl(std::move(request));
}

Result<PromQueryResult> Client::PromQuery(::greptime::v1::PromqlRequest request) {
  CheckThread();
  *request.mutable_header() = options_.BuildRequestHeader();
  auto response = Unary(kPromPath, request);
  if (!response) return std::unexpected(response.error());
  auto decoded = Decode<::greptime::v1::PromqlResponse>(response->payload);
  if (!decoded) return std::unexpected(decoded.error());
  if (auto valid = CheckResponseHeader(decoded->has_header() ? &decoded->header() : nullptr, *response);
      !valid) return std::unexpected(valid.error());
  PromQueryResult result;
  result.body = std::move(*decoded->mutable_body());
  if (decoded->has_header()) result.header = std::move(*decoded->mutable_header());
  return result;
}

Result<PromQueryResult> Client::PromInstantQuery(std::string_view query, std::string_view time,
                                               std::string_view lookback) {
  CheckThread();
  auto request = client::BuildPromInstantRequest(
      {options_.catalog, options_.schema, options_.timezone.value_or("")}, {query, time, lookback});
  if (!request) return std::unexpected(Error::InvalidConfig("invalid PromQL request text or scope"));
  return PromQuery(std::move(*request));
}
Result<PromQueryResult> Client::PromRangeQuery(std::string_view query, std::string_view start,
                                             std::string_view end, std::string_view step,
                                             std::string_view lookback) {
  CheckThread();
  auto request = client::BuildPromRangeRequest(
      {options_.catalog, options_.schema, options_.timezone.value_or("")},
      {query, start, end, step, lookback});
  if (!request) return std::unexpected(Error::InvalidConfig("invalid PromQL request text or scope"));
  return PromQuery(std::move(*request));
}

} // namespace greptimedb
