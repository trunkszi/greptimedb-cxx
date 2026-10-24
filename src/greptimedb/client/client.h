#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include <greptime/v1/database.pb.h>
#include <greptime/v1/prom.pb.h>

#include "greptimedb/client/prom.h"
#include "greptimedb/client/stream_inserter.h"
#include "greptimedb/core/config.h"
#include "greptimedb/flight/bulk_writer.h"
#include "greptimedb/flight/query_stream.h"
#include "greptimedb/row/reflected_row.h"
#include "greptimedb/transport/synchronous_channel.h"

namespace greptimedb {

using PromQueryResult = client::PromQueryResult;

namespace client_detail {
/** Preserves schema failures separately from buffer allocation failures. */
[[nodiscard]] Error ColumnError(ColumnBufError error);
} // namespace client_detail

class ColumnarBatch;
class MultiTableBatch;
class RowBatch;

/** A completed health-check exchange with the configured endpoint. */
struct HealthReport final {
  std::string endpoint;
  std::chrono::nanoseconds latency{};
  bool healthy{false};
};

/** Synchronous RPCs confined to the creating caller thread. Channel progress
 * hooks run inline and their contexts must outlive all client operations. */
class Client final {
public:
  /** Connects to the first configured endpoint using the transport's supported
   * connection options. Hostname resolution occurs on the caller thread. */
  [[nodiscard]] static Result<std::unique_ptr<Client>>
  Connect(ClientOptions options, const TlsOptions &tls = {});
  [[nodiscard]] static Result<std::unique_ptr<Client>>
  FromChannel(ClientOptions options, std::unique_ptr<SynchronousChannel> channel);
  [[nodiscard]] static Result<std::unique_ptr<Client>>
  FromMemoryPipe(ClientOptions options, std::unique_ptr<EndpointTransport> pipe,
                 void *progress_context = nullptr,
                 SynchronousChannel::ProgressHook progress = nullptr);
  ~Client();
  Client(const Client &) = delete;
  Client &operator=(const Client &) = delete;
  Client(Client &&) = delete;
  Client &operator=(Client &&) = delete;

  [[nodiscard]] const ClientOptions &Options() const noexcept;
  /** The client retains ownership; streams must finish before client teardown. */
  [[nodiscard]] SynchronousChannel &Channel() noexcept;
  [[nodiscard]] Result<void> Ping();
  [[nodiscard]] Result<HealthReport> ProbeHealth();

  /** Sessions borrow this client's channel and must be destroyed before it. */
  [[nodiscard]] Result<std::unique_ptr<::greptimedb::StreamInserter>>
  StreamInserter(const WriteHints *hints = nullptr);
  /** Unspecified bulk options inherit max_in_flight and request_timeout from
   * ClientOptions. Explicit options control the complete bulk session. */
  [[nodiscard]] Result<std::unique_ptr<flight::BulkStreamWriter>>
  BulkWriter(std::string_view table, const flight::FlightSchema &schema,
             std::optional<flight::BulkWriteOptions> options = std::nullopt,
             const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::unique_ptr<flight::QueryStream>>
  Query(const ::greptime::v1::QueryRequest &query);
  [[nodiscard]] Result<std::unique_ptr<flight::QueryStream>> Query(std::string_view sql);

  /** Builds a temporary columnar batch. Valid empty writes return zero without
   * opening an RPC; destinations and builder schemas are still validated. */
  template <RowConcept T>
  [[nodiscard]] Result<std::uint32_t> Insert(std::span<const T> rows) {
    return InsertTo<T>(TypedColumnarBatch<T>::TableName(), rows);
  }
  template <RowConcept T>
  [[nodiscard]] Result<std::uint32_t>
  InsertTo(std::string_view table, std::span<const T> rows) {
    CheckThread();
    if (auto valid = ValidateDestination(table); !valid)
      return std::unexpected(valid.error());
    if (rows.empty()) return std::uint32_t{0};
    TypedColumnarBatch<T> batch;
    if (auto reserved = batch.ReserveRows(rows.size()); !reserved)
      return std::unexpected(client_detail::ColumnError(reserved.error()));
    for (const auto &row : rows) {
      if (auto pushed = batch.Push(row); !pushed)
        return std::unexpected(client_detail::ColumnError(pushed.error()));
    }
    auto request = batch.ToInsertRequest();
    if (!request)
      return std::unexpected(client_detail::ColumnError(request.error()));
    request->set_table_name(std::string(table));
    return InsertBatch(std::move(*request));
  }

  template <RowConcept T>
  [[nodiscard]] Result<std::uint32_t>
  InsertBatch(const TypedColumnarBatch<T> &batch, const WriteHints *hints = nullptr) {
    return InsertBatchTo(batch.TableName(), batch, hints);
  }
  template <RowConcept T>
  [[nodiscard]] Result<std::uint32_t>
  InsertBatchTo(std::string_view table, const TypedColumnarBatch<T> &batch,
                const WriteHints *hints = nullptr) {
    CheckThread();
    if (auto valid = ValidateDestination(table); !valid)
      return std::unexpected(valid.error());
    auto request = batch.ToInsertRequest();
    if (!request)
      return std::unexpected(client_detail::ColumnError(request.error()));
    request->set_table_name(std::string(table));
    return InsertBatch(std::move(*request), hints);
  }
  template <RowConcept T>
  [[nodiscard]] Result<std::uint32_t> DeleteBatch(const TypedColumnarBatch<T> &batch) {
    return DeleteBatchFrom(batch.TableName(), batch);
  }
  template <RowConcept T>
  [[nodiscard]] Result<std::uint32_t>
  DeleteBatchFrom(std::string_view table, const TypedColumnarBatch<T> &batch) {
    CheckThread();
    if (auto valid = ValidateDestination(table); !valid)
      return std::unexpected(valid.error());
    auto request = batch.ToDeleteRequest();
    if (!request)
      return std::unexpected(client_detail::ColumnError(request.error()));
    request->set_table_name(std::string(table));
    return DeleteBatch(std::move(*request));
  }

  [[nodiscard]] Result<std::uint32_t>
  InsertBatch(::greptime::v1::InsertRequest request, const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  InsertBatches(::greptime::v1::InsertRequests requests, const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  InsertRows(::greptime::v1::RowInsertRequest request, const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t> DeleteBatch(::greptime::v1::DeleteRequest request);
  [[nodiscard]] Result<std::uint32_t> DeleteRows(::greptime::v1::RowDeleteRequest request);

  [[nodiscard]] Result<std::uint32_t>
  InsertBatch(const ColumnarBatch &batch, const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  InsertBatchTo(std::string_view table, const ColumnarBatch &batch,
                const WriteHints *hints = nullptr);
  /** All batches are encoded before the single InsertRequests RPC is opened. */
  [[nodiscard]] Result<std::uint32_t>
  InsertBatches(std::span<const ColumnarBatch *const> batches,
                const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  InsertMultiBatch(const MultiTableBatch &batch, const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  InsertRows(const RowBatch &batch, const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  InsertRowsTo(std::string_view table, const RowBatch &batch,
               const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t> DeleteBatch(const ColumnarBatch &batch);
  [[nodiscard]] Result<std::uint32_t>
  DeleteBatchFrom(std::string_view table, const ColumnarBatch &batch);
  [[nodiscard]] Result<std::uint32_t> DeleteRows(const RowBatch &batch);
  [[nodiscard]] Result<std::uint32_t>
  DeleteRowsFrom(std::string_view table, const RowBatch &batch);

  [[nodiscard]] Result<std::uint32_t> ExecuteDdl(::greptime::v1::DdlRequest ddl);
  [[nodiscard]] Result<std::uint32_t>
  CreateDatabase(std::string_view schema, bool if_not_exists,
                 const std::unordered_map<std::string, std::string> &options = {});
  [[nodiscard]] Result<std::uint32_t>
  CreateDatabase(::greptime::v1::CreateDatabaseExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  AlterDatabase(::greptime::v1::AlterDatabaseExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  CreateTable(::greptime::v1::CreateTableExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  AlterTable(::greptime::v1::AlterTableExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  DropTable(std::string_view table, bool if_exists);
  [[nodiscard]] Result<std::uint32_t>
  DropTable(::greptime::v1::DropTableExpr expression);
  [[nodiscard]] Result<std::uint32_t> TruncateTable(std::string_view table);
  [[nodiscard]] Result<std::uint32_t>
  TruncateTable(::greptime::v1::TruncateTableExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  CreateView(::greptime::v1::CreateViewExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  DropView(std::string_view view, bool if_exists);
  [[nodiscard]] Result<std::uint32_t>
  DropView(::greptime::v1::DropViewExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  CreateFlow(::greptime::v1::CreateFlowExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  DropFlow(std::string_view flow, bool if_exists);
  [[nodiscard]] Result<std::uint32_t>
  DropFlow(::greptime::v1::DropFlowExpr expression);
  [[nodiscard]] Result<std::uint32_t>
  CommentOn(::greptime::v1::CommentOnExpr expression);

  [[nodiscard]] Result<PromQueryResult>
  PromInstantQuery(std::string_view query, std::string_view time = {},
                   std::string_view lookback = {});
  [[nodiscard]] Result<PromQueryResult>
  PromRangeQuery(std::string_view query, std::string_view start,
                 std::string_view end, std::string_view step,
                 std::string_view lookback = {});

private:
  Client(ClientOptions options, std::unique_ptr<SynchronousChannel> channel);
  void CheckThread() const noexcept;
  [[nodiscard]] static Result<void> ValidateDestination(std::string_view table);
  [[nodiscard]] Result<CallOptions> BuildCallOptions(const WriteHints *hints = nullptr);
  [[nodiscard]] Result<RpcResponse>
  Unary(std::string_view path, const google::protobuf::MessageLite &request,
        const WriteHints *hints = nullptr);
  [[nodiscard]] Result<std::uint32_t>
  HandleRequest(::greptime::v1::GreptimeRequest request,
                const WriteHints *hints = nullptr);
  [[nodiscard]] Result<PromQueryResult>
  PromQuery(::greptime::v1::PromqlRequest request);

  ClientOptions options_;
  std::unique_ptr<SynchronousChannel> channel_;
  const void *owner_;
};

} // namespace greptimedb
